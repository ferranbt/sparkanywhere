package sparkanywhere

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	"github.com/labstack/echo"
	v1 "k8s.io/api/core/v1"
)

type K8S struct {
	config   *Config
	pods     []v1.Pod
	handles  []*taskHandle
	updateCh []chan Event

	provider provider

	createLock      sync.Mutex
	resourceVersion uint64
}

type Config struct {
	ControlPlaneAddr string
	EcsEnabled       bool
	DockerEnabled    bool
	EcsConfig        *ECSConfig
	Instances        uint64
}

func New(config *Config) (*K8S, error) {
	if config.EcsEnabled && config.DockerEnabled {
		return nil, fmt.Errorf("only one provider can be enabled")
	}

	var (
		provider provider
		err      error
	)
	if config.EcsEnabled {
		provider, err = newEcsProvider(config.EcsConfig)
	} else {
		provider, err = newDockerProvider()
	}
	if err != nil {
		return nil, err
	}

	k := &K8S{
		config:   config,
		handles:  []*taskHandle{},
		provider: provider,
	}
	return k, nil
}

func (k *K8S) Run() error {
	k.initServer()
	return k.deploy()
}

func (k *K8S) addHandle(handle *taskHandle) {
	k.handles = append(k.handles, handle)
}

func (k *K8S) GatherLogs() error {
	slog.Info("Gathering logs...")

	// create log directory
	logDir := filepath.Join("logs", fmt.Sprintf("%d", time.Now().UTC().UnixMilli()))
	if err := os.MkdirAll(logDir, 0755); err != nil {
		return err
	}

	// get logs from all the handles
	for _, handle := range k.handles {
		logs, err := k.provider.GetLogs(handle)
		if err != nil {
			return err
		}

		// write logs to file
		if err := os.WriteFile(filepath.Join(logDir, handle.Name+".log"), []byte(logs), 0644); err != nil {
			return err
		}
	}

	return nil
}

func (k *K8S) deploy() error {
	if k.config.DockerEnabled {
		k.config.ControlPlaneAddr = "host.docker.internal"
	}
	if k.config.ControlPlaneAddr == "" {
		return fmt.Errorf("control plane public address is required")
	}

	slog.Info("Using control plane address", "control-plane-addr", k.config.ControlPlaneAddr)

	task := &Task{
		Name:  "spark-pi",
		Image: "apache/spark",
		Args: []string{
			"/bin/bash",
			"-c",
			"cd .. && ./bin/spark-submit --master k8s://http://" + k.config.ControlPlaneAddr + ":1323 --deploy-mode client --name spark-pi --class org.apache.spark.examples.SparkPi --conf spark.executor.instances=" + strconv.Itoa(int(k.config.Instances)) + " --conf spark.kubernetes.container.image=apache/spark:latest ./examples/jars/spark-examples_2.12-3.5.0.jar",
		},
	}

	handle, err := k.provider.CreateTask(task)
	if err != nil {
		return err
	}

	handle.Name = "spark-pi"
	k.addHandle(handle)

	slog.Info("deploy task created", "name", handle.Name, "id", handle.Id)

	if err := k.provider.WaitForTask(handle); err != nil {
		return err
	}

	return nil
}

func (k *K8S) initServer() {
	e := echo.New()
	e.HideBanner = true

	logger := slog.With("theme", "k8s-server")

	e.Use(func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c echo.Context) error {
			logger.Info("request", "method", c.Request().Method, "path", c.Path(), "query", c.QueryString())
			return next(c)
		}
	})

	e.GET("/", func(c echo.Context) error {
		return c.String(http.StatusOK, "Hello, World!")
	})

	// pod namespace
	e.GET("/api/v1/namespaces/:namespace/pods", k.getPods)
	e.POST("/api/v1/namespaces/:namespace/pods", k.postPods)
	e.DELETE("/api/v1/namespaces/:namespace/pods", func(c echo.Context) error {
		// this one is called at the end of the spark job
		return c.NoContent(http.StatusOK)
	})

	// config map namespace
	e.POST("/api/v1/namespaces/:namespace/configmaps", k.postConfigMaps)
	e.GET("/api/v1/namespaces/:namespace/configmaps", k.getConfigMap)
	e.DELETE("/api/v1/namespaces/:namespace/configmaps", func(c echo.Context) error {
		// this one is called at the end of the spark job
		return c.NoContent(http.StatusOK)
	})

	// services
	e.DELETE("/api/v1/namespaces/:namespace/services", func(c echo.Context) error {
		// this one is called at the end of the spark job
		return c.NoContent(http.StatusOK)
	})

	// persistent volume claims
	e.DELETE("/api/v1/namespaces/:namespace/persistentvolumeclaims", func(c echo.Context) error {
		// this one is called at the end of the spark job
		return c.NoContent(http.StatusOK)
	})

	go func() {
		e.Start("0.0.0.0:1323")
	}()
}

type Event struct {
	Type   string      `json:"type"`
	Object interface{} `json:"object"`
}

func (k *K8S) getPods(c echo.Context) error {
	if c.QueryParam("watch") == "true" {
		// check the index of the last event
		updateCh := make(chan Event, 1000)

		c.Response().Header().Set("Content-Type", "application/json")

		k.createLock.Lock()
		k.updateCh = append(k.updateCh, updateCh)
		k.createLock.Unlock()

		for event := range updateCh {
			slog.Info("sending event")

			data, err := json.Marshal(event)
			if err != nil {
				return err
			}
			c.Response().Write(data)
			c.Response().Flush()

			// check if the connection is closed
			select {
			case <-c.Request().Context().Done():
				return nil
			default:
			}
		}
	} else {
		c.JSON(http.StatusOK, v1.PodList{
			Items: k.pods,
		})
	}

	return nil
}

func (k *K8S) getConfigMap(c echo.Context) error {
	c.JSON(http.StatusOK, v1.ConfigMapList{})

	return nil
}

func (k *K8S) postPods(c echo.Context) error {
	var pod v1.Pod
	if err := c.Bind(&pod); err != nil {
		return err
	}
	go func() {
		if err := k.createPod(pod); err != nil {
			slog.Error("error creating pod", "err", err)
		}
	}()

	return c.JSON(http.StatusOK, pod)
}

func (k *K8S) Close() {
}

func (k *K8S) createPod(pod v1.Pod) error {
	k.createLock.Lock()
	defer k.createLock.Unlock()

	k.resourceVersion++

	// assume only one container per pod, otherwise it requires special
	// networking protocols
	if len(pod.Spec.Containers) != 1 {
		panic("only one container per pod is supported")
	}
	cc := pod.Spec.Containers[0]

	// convert pod to task
	task := &Task{
		Name:  pod.ObjectMeta.Name,
		Image: cc.Image,
		Args:  cc.Args,
		Env:   make(map[string]string),
	}
	for _, kv := range cc.Env {
		// override the SPARK_LOCAL_DIRS to point to /tmp
		if kv.Name == "SPARK_LOCAL_DIRS" {
			task.Env[kv.Name] = "/tmp"
			continue
		}

		task.Env[kv.Name] = kv.Value
	}

	handle, err := k.provider.CreateTask(task)
	if err != nil {
		return err
	}

	slog.Info("task created", "name", handle.Name, "id", handle.Id)

	handle.Name = pod.ObjectMeta.Name
	k.addHandle(handle)

	// just put already as running
	pod.Status.Phase = v1.PodRunning

	k.pods = append(k.pods, pod)

	// add an update with increasing resourceVersion
	pod = *pod.DeepCopy()
	pod.ObjectMeta.ResourceVersion = fmt.Sprintf("%d", k.resourceVersion)

	event := Event{
		Type:   "ADDED",
		Object: pod,
	}
	for _, ch := range k.updateCh {
		ch <- event
	}

	return nil
}

func (k *K8S) postConfigMaps(c echo.Context) error {
	var configMap v1.ConfigMap
	if err := c.Bind(&configMap); err != nil {
		return err
	}
	return c.JSON(http.StatusOK, configMap)
}

type Task struct {
	Name  string
	Image string
	Args  []string
	Env   map[string]string
}

type provider interface {
	CreateTask(task *Task) (*taskHandle, error)
	WaitForTask(handle *taskHandle) error
	GetLogs(handle *taskHandle) (string, error)
}

type taskHandle struct {
	Name string
	Id   string
}
