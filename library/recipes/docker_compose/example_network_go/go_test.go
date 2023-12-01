package example

import (
	"context"
	"testing"

	"github.com/go-redis/redis/v8"
	"github.com/stretchr/testify/require"
)

func TestFoo(t *testing.T) {
	c := redis.NewUniversalClient(
		&redis.UniversalOptions{
			Addrs: []string{"redis:6379"},
		},
	)

	sc := c.Ping(context.Background())
	require.NoError(t, sc.Err())
	t.Log(sc)
}
