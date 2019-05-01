package main

import (
	"flag"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"sync"
	"time"
	"syscall"

	"github.com/lwolf/kube-cleanup-operator/pkg/controller"
	"k8s.io/client-go/kubernetes"
	_ "k8s.io/client-go/plugin/pkg/client/auth/gcp" // TODO: Add all auth providers
	_ "k8s.io/client-go/plugin/pkg/client/auth/oidc"
	"k8s.io/client-go/tools/clientcmd"
)

func main() {
	// Set logging output to standard console out
	log.SetOutput(os.Stdout)

	sigs := make(chan os.Signal, 1) // Create channel to receive OS signals
	stop := make(chan struct{})     // Create channel to receive stop signal

	signal.Notify(sigs, os.Interrupt, syscall.SIGTERM, syscall.SIGINT) // Register the sigs channel to receieve SIGTERM

	wg := &sync.WaitGroup{} // Goroutines can add themselves to this to be waited on so that they finish

	runOutsideCluster := flag.Bool("run-outside-cluster", false, "Set this flag when running outside of the cluster.")
	namespace := flag.String("namespace", "", "Watch only this namespaces")
	keepSuccess := flag.Duration("keep-successful", 15*time.Minute, "Duration to keep successful jobs, forever if negative, e.g. 1h15m")
	keepFailed := flag.Duration("keep-failures", 15*time.Minute, "Duration to keep failed jobs, forever if negative, e.g. 1h15m")
	keepPending := flag.Duration("keep-pending", 15*time.Minute, "Duration to keep pending jobs, forever if negative, e.g. 1h15m")
	dryRun := flag.Bool("dry-run", false, "Print only, do not delete anything.")
	flag.Parse()

	// Create clientset for interacting with the kubernetes cluster
	clientset, err := newClientSet(*runOutsideCluster)

	if err != nil {
		log.Fatal(err.Error())
	}

	options := map[string]time.Duration{
		"keepSuccess": *keepSuccess,
		"keepFailed":  *keepFailed,
		"keepPending": *keepPending,
	}
	if *dryRun {
		log.Println("Performing dry run...")
	}
	log.Printf(
		"Provided settings: namespace=%s, dryRun=%t, keepSuccess: %s, keepFailed: %s, keepPending: %s",
		*namespace, *dryRun, *keepSuccess, *keepFailed, *keepPending,
	)

	go func() {
		wg.Add(1)
		defer wg.Done()
		controller.NewPodController(clientset, *namespace, *dryRun, options).Run(stop)
	}()
	log.Printf("Controller started...")

	<-sigs // Wait for signals (this hangs until a signal arrives)
	log.Printf("Shutting down...")

	close(stop) // Tell goroutines to stop themselves
	wg.Wait()   // Wait for all to be stopped
}

func newClientSet(runOutsideCluster bool) (*kubernetes.Clientset, error) {
	kubeConfigLocation := ""

	if runOutsideCluster == true {
		if os.Getenv("KUBECONFIG") != "" {
			kubeConfigLocation = filepath.Join(os.Getenv("KUBECONFIG"))
		} else {
			homeDir := os.Getenv("HOME")
			kubeConfigLocation = filepath.Join(homeDir, ".kube", "config")
		}
	}

	// use the current context in kubeconfig
	config, err := clientcmd.BuildConfigFromFlags("", kubeConfigLocation)

	if err != nil {
		return nil, err
	}

	return kubernetes.NewForConfig(config)
}
