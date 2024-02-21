package sparkanywhere

import (
	"fmt"
	"strings"
	"time"

	"log/slog"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/ec2"
	"github.com/aws/aws-sdk-go/service/ecs"
)

type ecsProvider struct {
	log *slog.Logger

	config *ECSConfig
	svc    *ecs.ECS

	// taskDefinitionName is the full name for the task definition with the revision
	taskDefinitionName string

	// taskDefinitionContainerName is the name of apache/spark container of the task definition
	taskDefinitionContainerName string
}

type ECSConfig struct {
	ClusterName   string
	SubnetId      string
	SecurityGroup string
}

func newEcsProvider(config *ECSConfig) (provider, error) {
	sess, err := session.NewSession(&aws.Config{
		Credentials: credentials.NewSharedCredentials("", ""),
	})
	if err != nil {
		return nil, err
	}
	svc := ecs.New(sess)

	p := &ecsProvider{
		log:    slog.With("provider", "ecs"),
		config: config,
		svc:    svc,
	}

	// query the cluster name and figure out the task definition, revision and container name.
	output, err := svc.DescribeClusters(&ecs.DescribeClustersInput{Clusters: []*string{aws.String(config.ClusterName)}})
	if err != nil {
		return nil, err
	}
	if len(output.Clusters) == 0 {
		return nil, fmt.Errorf("cluster not found: %s", config.ClusterName)
	}

	taskDefs, err := svc.ListTaskDefinitionFamilies(&ecs.ListTaskDefinitionFamiliesInput{})
	if err != nil {
		return nil, err
	}

	sparkAnywhereTaskDefs := []string{}
	for _, x := range taskDefs.Families {
		if strings.Contains(*x, "sparkanywhere") {
			sparkAnywhereTaskDefs = append(sparkAnywhereTaskDefs, *x)
		}
	}
	if len(sparkAnywhereTaskDefs) == 0 {
		return nil, fmt.Errorf("no task definition found")
	}
	if len(sparkAnywhereTaskDefs) > 1 {
		return nil, fmt.Errorf("more than one task definition found")
	}

	taskDef := sparkAnywhereTaskDefs[0]
	out2, err := svc.DescribeTaskDefinition(&ecs.DescribeTaskDefinitionInput{TaskDefinition: aws.String(taskDef)})
	if err != nil {
		return nil, err
	}

	p.taskDefinitionName = fmt.Sprintf("%s:%d", taskDef, *out2.TaskDefinition.Revision)
	p.taskDefinitionContainerName = *out2.TaskDefinition.ContainerDefinitions[0].Name

	p.log.Info("Using task definitione", "name", p.taskDefinitionName)
	p.log.Info("Detected task primary container", "name", p.taskDefinitionContainerName)

	// describe the VPN and get the subnet ids and security group.
	svcEc2 := ec2.New(sess)

	// check that the subnet exists
	if _, err = svcEc2.DescribeSubnets(&ec2.DescribeSubnetsInput{SubnetIds: []*string{aws.String(config.SubnetId)}}); err != nil {
		return nil, fmt.Errorf("subnet not found: %s", config.SubnetId)
	}

	// check that the security group exists
	if _, err = svcEc2.DescribeSecurityGroups(&ec2.DescribeSecurityGroupsInput{GroupIds: []*string{aws.String(config.SecurityGroup)}}); err != nil {
		return nil, fmt.Errorf("security group not found: %s", config.SecurityGroup)
	}

	return p, nil
}

func (e *ecsProvider) CreateTask(task *Task) (*taskHandle, error) {
	e.log.Info("Creating task", "task", task.Name)

	envOverride := []*ecs.KeyValuePair{}
	for name, value := range task.Env {
		envOverride = append(envOverride, &ecs.KeyValuePair{
			Name:  aws.String(name),
			Value: aws.String(value),
		})
	}

	input := &ecs.RunTaskInput{
		Cluster:        aws.String(e.config.ClusterName),
		TaskDefinition: aws.String(e.taskDefinitionName),
		LaunchType:     aws.String("FARGATE"),
		Count:          aws.Int64(1),
		NetworkConfiguration: &ecs.NetworkConfiguration{
			AwsvpcConfiguration: &ecs.AwsVpcConfiguration{
				AssignPublicIp: aws.String("ENABLED"),
				SecurityGroups: []*string{
					aws.String(e.config.SecurityGroup),
				},
				Subnets: []*string{
					aws.String(e.config.SubnetId),
				},
			},
		},
		Overrides: &ecs.TaskOverride{
			ContainerOverrides: []*ecs.ContainerOverride{
				{
					Name:        aws.String(e.taskDefinitionContainerName),
					Command:     aws.StringSlice(task.Args),
					Environment: envOverride,
				},
			},
		},
	}

	result, err := e.svc.RunTask(input)
	if err != nil {
		return nil, err
	}

	handle := &taskHandle{
		Id: *result.Tasks[0].TaskArn,
	}

	// block until it changes state
	for {
		describeTasksOutput, err := e.svc.DescribeTasks(&ecs.DescribeTasksInput{
			Cluster: aws.String(e.config.ClusterName),
			Tasks:   []*string{aws.String(handle.Id)},
		})
		if err != nil {
			return nil, err
		}
		if *describeTasksOutput.Tasks[0].LastStatus == "RUNNING" {
			e.log.Info("task is running", "taskArn", *result.Tasks[0].TaskArn)
			break
		}
		time.Sleep(5 * time.Second)
	}

	return handle, nil
}

func (e *ecsProvider) WaitForTask(handle *taskHandle) error {
	for {
		describeTasksOutput, err := e.svc.DescribeTasks(&ecs.DescribeTasksInput{
			Cluster: aws.String(e.config.ClusterName),
			Tasks:   []*string{aws.String(handle.Id)},
		})
		if err != nil {
			return err
		}

		if describeTasksOutput.Tasks[0].LastStatus == aws.String("STOPPED") {
			break
		}
		time.Sleep(1 * time.Second)
	}
	return nil
}

func (e *ecsProvider) GetLogs(handle *taskHandle) (string, error) {
	return "TODO", nil
}
