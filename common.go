package containers

import (
	"context"
	"github.com/stretchr/testify/suite"
	"github.com/testcontainers/testcontainers-go"
)

type containerBase struct {
	*suite.Suite
	testcontainers.Container
	ctx context.Context

	host string
}

func newContainerBase(suite *suite.Suite, container testcontainers.Container, ctx context.Context) containerBase {
	host, err := container.Host(ctx)
	suite.Require().NoError(err)

	return containerBase{
		Suite:     suite,
		Container: container,
		ctx:       ctx,
		host:      host,
	}
}
