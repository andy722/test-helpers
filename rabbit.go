package containers

import (
	"context"
	"fmt"
	"github.com/stretchr/testify/suite"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

type RabbitContainerOptions struct {
	Image string
}

func NewRabbitContainerOptions() *RabbitContainerOptions {
	return &RabbitContainerOptions{
		Image: "rabbitmq:3-management",
	}
}

type RabbitContainer struct {
	containerBase

	amqpUri  string
	adminUri string
	admin    *RabbitAdmin
}

func NewRabbitContainer(opts *RabbitContainerOptions, s *suite.Suite, ctx context.Context) *RabbitContainer {
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        opts.Image,
			ExposedPorts: []string{"5672/tcp", "15672/tcp"},
			WaitingFor:   wait.ForListeningPort("15672/tcp"),
		},
		Started: true,
	})
	s.Require().NoError(err)

	rabbit := &RabbitContainer{
		containerBase: newContainerBase(s, container, ctx),
	}

	amqpPort, err := container.MappedPort(ctx, "5672/tcp")
	s.Require().NoError(err)
	rabbit.amqpUri = fmt.Sprintf("amqp://guest:guest@%s:%d", rabbit.host, amqpPort.Int())

	adminPort, err := rabbit.MappedPort(rabbit.ctx, "15672/tcp")
	rabbit.Require().NoError(err)
	rabbit.adminUri = fmt.Sprintf("http://guest:guest@%s:%d", rabbit.host, adminPort.Int())

	admin, err := NewRabbitAdmin(rabbit.GetAdminUri())
	s.Require().NoError(err)
	rabbit.admin = admin

	return rabbit
}

func (rabbit *RabbitContainer) GetUri() string {
	return rabbit.amqpUri
}

func (rabbit *RabbitContainer) GetAdminUri() string {
	return rabbit.adminUri
}

func (rabbit *RabbitContainer) DumpAll(queue string) (result []map[string]interface{}) {
	const batchSize = 100

	req := NewGetMessagesRequest()
	req.Count = 100

	defer rabbit.T().Log("Consumed", result)

	for {
		rc, err := rabbit.admin.Consume(queue, req)
		if !rabbit.NoError(err) {
			return
		}

		result = append(result, rc...)
		if len(rc) < batchSize {
			return
		}
	}
}

func (rabbit *RabbitContainer) Purge(queue string) {
	rabbit.NoError(rabbit.admin.Purge(queue, "/"))
}
