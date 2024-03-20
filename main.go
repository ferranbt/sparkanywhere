package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/ferranbt/sparkanywhere/sparkanywhere"
)

func main() {
	cfg := &sparkanywhere.Config{
		EcsConfig: &sparkanywhere.ECSConfig{},
	}

	flag.BoolVar(&cfg.EcsEnabled, "ecs", false, "Use ECS as the provider")
	flag.BoolVar(&cfg.DockerEnabled, "docker", false, "Use Docker as the provider")
	flag.StringVar(&cfg.EcsConfig.ClusterName, "ecs-cluster-name", "", "")
	flag.StringVar(&cfg.EcsConfig.SecurityGroup, "ecs-security-group", "", "")
	flag.StringVar(&cfg.EcsConfig.SubnetId, "ecs-subnet-id", "", "")
	flag.StringVar(&cfg.ControlPlaneAddr, "control-plane-addr", "", "")
	flag.Uint64Var(&cfg.Instances, "instances", 1, "")
	flag.Parse()

	var (
		doneCh = make(chan struct{})
	)

	sChan := make(chan os.Signal, 1)
	signal.Notify(sChan, syscall.SIGTERM, syscall.SIGINT)

	core, err := sparkanywhere.New(cfg)
	if err != nil {
		fmt.Printf("Error creating sparkanywhere: %v\n", err)
		os.Exit(1)
	}

	go func() {
		if err := core.Run(); err != nil {
			fmt.Printf("Error running sparkanywhere: %v\n", err)
		}

		close(doneCh)
	}()

	select {
	case <-doneCh:
	case <-sChan:
		fmt.Printf("Shutting down...\n")
	}

	core.GatherLogs()
}
