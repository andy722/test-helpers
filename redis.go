package containers

import (
	"context"
	"fmt"
	"github.com/stretchr/testify/suite"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

type RedisContainerOptions struct {
	Image string
}

func NewRedisContainerOptions() *RedisContainerOptions {
	return &RedisContainerOptions{
		Image: "redis",
	}
}

type RedisContainer struct {
	containerBase
}

func NewRedisContainer(opts *RedisContainerOptions, s *suite.Suite, ctx context.Context) *RedisContainer {
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        opts.Image,
			ExposedPorts: []string{"6379/tcp"},
			WaitingFor:   wait.ForListeningPort("6379/tcp"),
		},
		Started: true,
	})
	s.Require().NoError(err)

	return &RedisContainer{
		newContainerBase(s, container, ctx),
	}
}

func (redis *RedisContainer) GetUri() string {
	port, err := redis.MappedPort(redis.ctx, "6379/tcp")
	redis.Require().NoError(err)
	return fmt.Sprintf("redis://%s:%d", redis.host, port.Int())
}
