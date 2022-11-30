package cache_test

import (
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"a.yandex-team.ru/library/go/yandex/tvm"
	"a.yandex-team.ru/library/go/yandex/tvm/tvmtool/internal/cache"
)

var (
	testDst      = "test_dst"
	testDstAlias = "test_dst_alias"
	testDstID    = tvm.ClientID(1)
	testValue    = "test_val"
)

func TestNewAtHour(t *testing.T) {
	c := cache.New(time.Hour, 11*time.Hour)
	assert.NotNil(t, c, "failed to create cache")
}

func TestCache_Load(t *testing.T) {

	c := cache.New(time.Second, time.Hour)
	c.Store(testDstID, testDst, &testValue)
	// checking before
	{
		r, hit := c.Load(testDstID)
		assert.Equal(t, cache.Hit, hit, "failed to get '%d' from cache before deadline", testDstID)
		assert.NotNil(t, r, "failed to get '%d' from cache before deadline", testDstID)
		assert.Equal(t, testValue, *r)

		r, hit = c.LoadByAlias(testDst)
		assert.Equal(t, cache.Hit, hit, "failed to get '%s' from cache before deadline", testDst)
		assert.NotNil(t, r, "failed to get %q from tickets before deadline", testDst)
		assert.Equal(t, testValue, *r)
	}
	{
		r, hit := c.Load(999833321)
		assert.Equal(t, cache.Miss, hit, "got tickets for '999833321', but that key must be never existed")
		assert.Nil(t, r, "got tickets for '999833321', but that key must be never existed")

		r, hit = c.LoadByAlias("kek")
		assert.Equal(t, cache.Miss, hit, "got tickets for 'kek', but that key must be never existed")
		assert.Nil(t, r, "got tickets for 'kek', but that key must be never existed")
	}

	time.Sleep(3 * time.Second)
	// checking after
	{
		r, hit := c.Load(testDstID)
		assert.Equal(t, cache.GonnaMissy, hit)
		assert.Equal(t, testValue, *r)

		r, hit = c.LoadByAlias(testDst)
		assert.Equal(t, cache.GonnaMissy, hit)
		assert.Equal(t, testValue, *r)
	}
}

func TestCache_Keys(t *testing.T) {
	c := cache.New(time.Second, time.Hour)
	c.Store(testDstID, testDst, &testValue)
	c.Store(testDstID, testDstAlias, &testValue)

	t.Run("aliases", func(t *testing.T) {
		aliases := c.Aliases()
		sort.Strings(aliases)
		require.Equal(t, 2, len(aliases), "not correct length of aliases")
		require.EqualValues(t, []string{testDst, testDstAlias}, aliases)
	})

	t.Run("client_ids", func(t *testing.T) {
		ids := c.ClientIDs()
		require.Equal(t, 1, len(ids), "not correct length of client ids")
		require.EqualValues(t, []tvm.ClientID{testDstID}, ids)
	})
}

func TestCache_ExpiredKeys(t *testing.T) {
	c := cache.New(time.Second, 10*time.Second)
	c.Store(testDstID, testDst, &testValue)
	c.Store(testDstID, testDstAlias, &testValue)

	time.Sleep(3 * time.Second)
	c.Gc()

	var (
		newDst   = "new_dst"
		newDstID = tvm.ClientID(2)
	)
	c.Store(newDstID, newDst, &testValue)

	t.Run("aliases", func(t *testing.T) {
		aliases := c.Aliases()
		require.Equal(t, 3, len(aliases), "not correct length of aliases")
		require.ElementsMatch(t, []string{testDst, testDstAlias, newDst}, aliases)
	})

	t.Run("client_ids", func(t *testing.T) {
		ids := c.ClientIDs()
		require.Equal(t, 2, len(ids), "not correct length of client ids")
		require.ElementsMatch(t, []tvm.ClientID{testDstID, newDstID}, ids)
	})

	time.Sleep(8 * time.Second)
	c.Gc()

	t.Run("aliases", func(t *testing.T) {
		aliases := c.Aliases()
		require.Equal(t, 1, len(aliases), "not correct length of aliases")
		require.ElementsMatch(t, []string{newDst}, aliases)
	})

	t.Run("client_ids", func(t *testing.T) {
		ids := c.ClientIDs()
		require.Equal(t, 1, len(ids), "not correct length of client ids")
		require.ElementsMatch(t, []tvm.ClientID{newDstID}, ids)
	})
}
