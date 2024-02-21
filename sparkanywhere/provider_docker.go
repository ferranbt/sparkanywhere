package sparkanywhere

import (
	"bytes"
	"context"
	"log/slog"

	"github.com/docker/docker/api/types"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/network"
	"github.com/docker/docker/api/types/strslice"
	"github.com/docker/docker/client"
	"github.com/docker/docker/pkg/stdcopy"
)

type dockerProvider struct {
	logger *slog.Logger
	cli    *client.Client
}

var dockerNetworkName = "spark-network"

var _ provider = &dockerProvider{}

func newDockerProvider() (*dockerProvider, error) {
	cli, err := client.NewClientWithOpts(client.FromEnv)
	if err != nil {
		return nil, err
	}
	cli.NegotiateAPIVersion(context.Background())

	p := &dockerProvider{
		logger: slog.With("dockerProvider"),
		cli:    cli,
	}

	// create a network if there is none yet
	if _, err := cli.NetworkInspect(context.Background(), dockerNetworkName, types.NetworkInspectOptions{}); err != nil {
		slog.Info("creating network", "name", dockerNetworkName)
		if _, err = cli.NetworkCreate(context.Background(), dockerNetworkName, types.NetworkCreate{}); err != nil {
			return nil, err
		}
	}
	return p, nil
}

func (d *dockerProvider) GetLogs(handle *taskHandle) (string, error) {
	logs, err := d.cli.ContainerLogs(context.Background(), handle.Id, container.LogsOptions{ShowStdout: true, ShowStderr: true})
	if err != nil {
		return "", err
	}
	defer logs.Close()

	var buf bytes.Buffer
	if _, err = stdcopy.StdCopy(&buf, &buf, logs); err != nil {
		return "", err
	}

	return buf.String(), nil
}

func (d *dockerProvider) WaitForTask(handle *taskHandle) error {
	waitCh, errCh := d.cli.ContainerWait(context.Background(), handle.Id, container.WaitConditionNotRunning)

	select {
	case err := <-errCh:
		return err
	case <-waitCh:
	}
	return nil
}

func (d *dockerProvider) CreateTask(task *Task) (*taskHandle, error) {
	config := &container.Config{
		Image: task.Image,
		Cmd:   strslice.StrSlice(task.Args),
	}
	for name, value := range task.Env {
		config.Env = append(config.Env, name+"="+value)
	}

	hostConfig := &container.HostConfig{
		NetworkMode: container.NetworkMode(dockerNetworkName),
	}

	body, err := d.cli.ContainerCreate(context.Background(), config, hostConfig, &network.NetworkingConfig{}, nil, "")
	if err != nil {
		return nil, err
	}
	if err := d.cli.ContainerStart(context.Background(), body.ID, container.StartOptions{}); err != nil {
		return nil, err
	}

	handle := &taskHandle{
		Id: body.ID,
	}
	return handle, nil
}
