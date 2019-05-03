package controller

import (
	"encoding/json"
	"log"
	"reflect"
	"regexp"
	"strconv"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/version"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/pkg/api/v1"
	"k8s.io/client-go/tools/cache"
)

const resyncPeriod = time.Second * 30

// PodController watches the kubernetes api for changes to Pods and
// delete completed Pods without specific annotation
type PodController struct {
	podInformer cache.SharedIndexInformer
	kclient     *kubernetes.Clientset

	keepSuccess time.Duration
	keepFailed  time.Duration
	keepPending time.Duration
	dryRun           bool
	isLegacySystem   bool
}

// CreatedByAnnotation type used to match pods created by job
type CreatedByAnnotation struct {
	Kind       string
	ApiVersion string
	Reference  struct {
		Kind            string
		Namespace       string
		Name            string
		Uid             string
		ApiVersion      string
		ResourceVersion string
	}
}

func isLegacySystem(v version.Info) bool {
	oldVersion := false

	major, _ := strconv.Atoi(v.Major)

	var minor int
	re := regexp.MustCompile("[0-9]+")
	m := re.FindAllString(v.Minor, 1)
	if len(m) != 0 {
		minor, _ = strconv.Atoi(m[0])
	} else {
		log.Printf("failed to parse minor version %s", v.Minor)
		minor = 0
	}

	if major < 2 && minor < 8 {
		oldVersion = true
	}

	return oldVersion
}

// NewPodController creates a new NewPodController
func NewPodController(kclient *kubernetes.Clientset, namespace string, dryRun bool, opts map[string]time.Duration) *PodController {
	serverVersion, err := kclient.ServerVersion()
	if err != nil {
		log.Fatalf("Failed to retrieve server serverVersion %v", err)
	}

	podWatcher := &PodController{
		keepSuccess: opts["keepSuccess"],
		keepFailed:  opts["keepFailed"],
		keepPending: opts["keepPending"],
		dryRun:           dryRun,
		isLegacySystem:   isLegacySystem(*serverVersion),
	}
	// Create informer for watching Namespaces
	podInformer := cache.NewSharedIndexInformer(
		&cache.ListWatch{
			ListFunc: func(options metav1.ListOptions) (runtime.Object, error) {
				return kclient.CoreV1().Pods(namespace).List(options)
			},
			WatchFunc: func(options metav1.ListOptions) (watch.Interface, error) {
				return kclient.CoreV1().Pods(namespace).Watch(options)
			},
		},
		&v1.Pod{},
		resyncPeriod,
		cache.Indexers{},
	)
	podInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			podWatcher.Process(obj)
		},
		UpdateFunc: func(old, new interface{}) {
			if !reflect.DeepEqual(old, new) {
				podWatcher.Process(new)
			}
		},
	})

	podWatcher.kclient = kclient
	podWatcher.podInformer = podInformer

	return podWatcher
}

func (c *PodController) periodicCacheCheck() {
	for {
		for _, obj := range c.podInformer.GetStore().List() {
			c.Process(obj)
		}
		time.Sleep(2 * resyncPeriod)
	}
}

// Run starts the process for listening for pod changes and acting upon those changes.
func (c *PodController) Run(stopCh <-chan struct{}) {
	log.Printf("Listening for changes...")

	go c.podInformer.Run(stopCh)
	go c.periodicCacheCheck()

	<-stopCh
}

func (c *PodController) Process(obj interface{}) {
	podObj := obj.(*v1.Pod)
	parentJobName := c.getParentJobName(podObj)
	// if we couldn't find a prent job name, ignore this pod
	if parentJobName == "" {
		return
	}

	timeSinceCompletion := c.getTimeSinceCompletion(podObj)
	switch podObj.Status.Phase {
	case v1.PodSucceeded:
		if c.keepSuccess >= 0 && timeSinceCompletion >= c.keepSuccess {
			c.deleteObjects(podObj, parentJobName)
		}
	case v1.PodFailed:
		if c.keepFailed >= 0 && timeSinceCompletion >= c.keepFailed {
			c.deleteObjects(podObj, parentJobName)
		}
	case v1.PodPending:
		if c.keepPending >= 0 && timeSinceCompletion >= c.keepPending {
			c.deleteObjects(podObj, parentJobName)
		}
	default:
		return
	}
}

// method to calculate the hours that passed since the pod's execution end time
func (c *PodController) getTimeSinceCompletion(podObj *v1.Pod) time.Duration {
	currentUnixTime := time.Now()
	for _, pc := range podObj.Status.Conditions {
		// Looking for the time when pod's condition "Ready" became "false" (equals end of execution)
		if pc.Type == v1.PodReady && pc.Status == v1.ConditionFalse {
			return currentUnixTime.Sub(pc.LastTransitionTime.Time)
		}
	}
	return 0
}

func (c *PodController) deleteObjects(podObj *v1.Pod, parentJobName string) {
	// Delete Pod
	if !c.dryRun {
		log.Printf("Deleting pod '%s'", podObj.Name)
		var po metav1.DeleteOptions
		err := c.kclient.CoreV1().Pods(podObj.Namespace).Delete(podObj.Name, &po)
		if err != nil {
			log.Printf("failed to delete job %s: %v", parentJobName, err)
		}
	} else {
		log.Printf("dry-run: Pod '%s' would have been deleted", podObj.Name)
	}
	// Delete Job itself
	if !c.dryRun {
		log.Printf("Deleting job '%s'", parentJobName)
		var jo metav1.DeleteOptions
		err := c.kclient.BatchV1Client.Jobs(podObj.Namespace).Delete(parentJobName, &jo)
		if err != nil {
			log.Printf("failed to delete job %s: %v", parentJobName, err)
		}
	} else {
		log.Printf("dry-run: Job '%s' would have been deleted", parentJobName)
	}
	return
}

func (c *PodController) getParentJobName(podObj *v1.Pod) (parentJobName string) {

	if c.isLegacySystem {
		var createdMeta CreatedByAnnotation
		err := json.Unmarshal([]byte(podObj.ObjectMeta.Annotations["kubernetes.io/created-by"]), &createdMeta)
		if err != nil {
			log.Printf("failed to unmarshal annotations for pod %s. %v", podObj.Name, err)
			return
		}
		if createdMeta.Reference.Kind == "Job" {
			parentJobName = createdMeta.Reference.Name
		}
	} else {
		// Going all over the owners, looking for a job, usually there is only one owner
		for _, ow := range podObj.OwnerReferences {
			if ow.Kind == "Job" {
				parentJobName = ow.Name
			}
		}
	}
	return
}
