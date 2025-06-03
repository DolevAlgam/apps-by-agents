package e2e

import (
   "bytes"
   "context"
   "flag"
   "fmt"
   "io"
   "io/ioutil"
   "os"
   "os/exec"
   "path/filepath"
   "testing"
   "time"

   "github.com/go-resty/resty/v2"
   corev1 "k8s.io/api/core/v1"
   appsv1 "k8s.io/api/apps/v1"
   metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
   apierrors "k8s.io/apimachinery/pkg/api/errors"
   meta "k8s.io/apimachinery/pkg/api/meta"
   "k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
   "k8s.io/apimachinery/pkg/runtime"
   yamlserializer "k8s.io/apimachinery/pkg/runtime/serializer/yaml"
   "k8s.io/client-go/discovery"
   "k8s.io/client-go/discovery/cached/memory"
   "k8s.io/client-go/dynamic"
   "k8s.io/client-go/kubernetes"
   "k8s.io/client-go/restmapper"
   "k8s.io/client-go/tools/clientcmd"
)

var kubeconfig = flag.String("kubeconfig", filepath.Join(os.Getenv("HOME"), ".kube", "config"), "path to kubeconfig file")

func TestE2E(t *testing.T) {
   flag.Parse()

   cfg, err := clientcmd.BuildConfigFromFlags("", *kubeconfig)
   if err != nil {
       t.Fatalf("failed to build kubeconfig: %v", err)
   }
   clientset, err := kubernetes.NewForConfig(cfg)
   if err != nil {
       t.Fatalf("failed to create clientset: %v", err)
   }
   dynClient, err := dynamic.NewForConfig(cfg)
   if err != nil {
       t.Fatalf("failed to create dynamic client: %v", err)
   }
   disco, err := discovery.NewDiscoveryClientForConfig(cfg)
   if err != nil {
       t.Fatalf("failed to create discovery client: %v", err)
   }
   mapper := restmapper.NewDeferredDiscoveryRESTMapper(memory.NewMemCacheClient(disco))

   // Deploy manifests
   baseDir := filepath.Dir(os.Args[0])
   files := []string{
       "../deploy/crds.yaml",
       "../deploy/rbac.yaml",
       "../deploy/operator-deployment.yaml",
       "../deploy/operator-service.yaml",
       "../deploy/ui-deployment.yaml",
       "../deploy/ui-service.yaml",
   }
   for _, f := range files {
       applyYAML(t, mapper, dynClient, filepath.Join(baseDir, f))
   }

   // Wait for deployments
   waitForDeployment(t, clientset, "default", "hpc-operator", 2*time.Minute)
   waitForDeployment(t, clientset, "default", "hpc-ui", 2*time.Minute)

   // Port-forward operator REST API
   stopCh := make(chan struct{})
   go portForward(t, stopCh, "default", "svc/hpc-operator", "8081", "8081")
   defer close(stopCh)
   time.Sleep(5 * time.Second)

   restClient := resty.New().SetHostURL("http://localhost:8081")
   spec := map[string]interface{}{ // submit HPCJob via REST API
       "jobName":   "e2e-test-job",
       "image":     "busybox",
       "command":   []string{"echo", "hello world"},
       "resources": map[string]string{"cpu": "0.1", "memory": "64Mi"},
   }
   resp, err := restClient.R().SetBody(spec).Post("/jobs")
   if err != nil {
       t.Fatalf("failed to submit job: %v", err)
   }
   if resp.StatusCode() != 201 {
       t.Fatalf("unexpected status code: %d", resp.StatusCode())
   }

   // Poll for job completion
   type jobItem struct {
       Metadata struct{
           Name string `json:"name"`
       } `json:"metadata"`
       Status struct{
           JobStatus string `json:"jobStatus"`
       } `json:"status"`
   }
   var status struct{ Items []jobItem `json:"items"` }
   deadline := time.Now().Add(2 * time.Minute)
   for time.Now().Before(deadline) {
       _, err := restClient.R().SetResult(&status).Get("/status")
       if err != nil {
           t.Fatalf("failed to get status: %v", err)
       }
       if len(status.Items) > 0 && status.Items[0].Status.JobStatus == string(corev1.PodSucceeded) {
           break
       }
       time.Sleep(5 * time.Second)
   }

   // Verify job output in pod logs
   pods, err := clientset.CoreV1().Pods("default").List(context.Background(), metav1.ListOptions{LabelSelector: fmt.Sprintf("job-name=%s", status.Items[0].Metadata.Name)})
   if err != nil || len(pods.Items) == 0 {
       t.Fatalf("failed to find job pod: %v", err)
   }
   pod := pods.Items[0]
   logReq := clientset.CoreV1().Pods("default").GetLogs(pod.Name, &corev1.PodLogOptions{})
   logs, err := logReq.Stream(context.Background())
   if err != nil {
       t.Fatalf("failed to stream logs: %v", err)
   }
   defer logs.Close()
   buf := new(bytes.Buffer)
   if _, err := io.Copy(buf, logs); err != nil {
       t.Fatalf("failed to read logs: %v", err)
   }
   if !bytes.Contains(buf.Bytes(), []byte("hello world")) {
       t.Fatalf("expected logs to contain 'hello world'; got: %s", buf.String())
   }
}

// applyYAML reads a YAML file and creates the resources in the cluster
func applyYAML(t *testing.T, mapper *restmapper.DeferredDiscoveryRESTMapper, dynClient dynamic.Interface, path string) {
   data, err := ioutil.ReadFile(path)
   if err != nil {
       t.Fatalf("failed to read %s: %v", path, err)
   }
   docs := bytes.Split(data, []byte("---"))
   for _, doc := range docs {
       if len(bytes.TrimSpace(doc)) == 0 {
           continue
       }
       obj := &unstructured.Unstructured{}
       dec := yamlserializer.NewDecodingSerializer(unstructured.UnstructuredJSONScheme)
       _, gvk, err := dec.Decode(doc, nil, obj)
       if err != nil {
           t.Fatalf("failed to decode YAML: %v", err)
       }
       mapping, err := mapper.RESTMapping(gvk.GroupKind(), gvk.Version)
       if err != nil {
           t.Fatalf("failed to get REST mapping: %v", err)
       }
       var dr dynamic.ResourceInterface
       if mapping.Scope.Name() == meta.RESTScopeNameNamespace {
           if obj.GetNamespace() == "" {
               obj.SetNamespace("default")
           }
           dr = dynClient.Resource(mapping.Resource).Namespace(obj.GetNamespace())
       } else {
           dr = dynClient.Resource(mapping.Resource)
       }
       obj.SetManagedFields(nil)
       _, err = dr.Create(context.Background(), obj, metav1.CreateOptions{})
       if err != nil && !apierrors.IsAlreadyExists(err) {
           t.Fatalf("failed to create resource: %v", err)
       }
   }
}

// waitForDeployment waits until the specified deployment has all replicas ready
func waitForDeployment(t *testing.T, clientset *kubernetes.Clientset, namespace, name string, timeout time.Duration) {
   deadline := time.Now().Add(timeout)
   for time.Now().Before(deadline) {
       dep, err := clientset.AppsV1().Deployments(namespace).Get(context.Background(), name, metav1.GetOptions{})
       if err != nil {
           t.Logf("waiting for deployment %s: %v", name, err)
       } else if dep.Status.ReadyReplicas == *dep.Spec.Replicas {
           return
       }
       time.Sleep(3 * time.Second)
   }
   t.Fatalf("timed out waiting for deployment %s to be ready", name)
}

// portForward starts kubectl port-forward for the given resource
func portForward(t *testing.T, stopCh chan struct{}, namespace, resource, localPort, remotePort string) {
   cmd := exec.Command("kubectl", "port-forward", fmt.Sprintf("-n%s", namespace), resource, fmt.Sprintf("%s:%s", localPort, remotePort))
   cmd.Stdout = os.Stdout
   cmd.Stderr = os.Stderr
   if err := cmd.Start(); err != nil {
       t.Fatalf("failed to start port-forward: %v", err)
   }
   go func() {
       <-stopCh
       cmd.Process.Kill()
   }()
}