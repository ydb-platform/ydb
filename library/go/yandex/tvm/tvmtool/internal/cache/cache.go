package cache

import (
	"sync"
	"time"

	"a.yandex-team.ru/library/go/yandex/tvm"
)

const (
	Hit Status = iota
	Miss
	GonnaMissy
)

type (
	Status int

	Cache struct {
		ttl     time.Duration
		maxTTL  time.Duration
		tickets map[tvm.ClientID]entry
		aliases map[string]tvm.ClientID
		lock    sync.RWMutex
	}

	entry struct {
		value *string
		born  time.Time
	}
)

func New(ttl, maxTTL time.Duration) *Cache {
	return &Cache{
		ttl:     ttl,
		maxTTL:  maxTTL,
		tickets: make(map[tvm.ClientID]entry, 1),
		aliases: make(map[string]tvm.ClientID, 1),
	}
}

func (c *Cache) Gc() {
	now := time.Now()

	c.lock.Lock()
	defer c.lock.Unlock()
	for clientID, ticket := range c.tickets {
		if ticket.born.Add(c.maxTTL).After(now) {
			continue
		}

		delete(c.tickets, clientID)
		for alias, aClientID := range c.aliases {
			if clientID == aClientID {
				delete(c.aliases, alias)
			}
		}
	}
}

func (c *Cache) ClientIDs() []tvm.ClientID {
	c.lock.RLock()
	defer c.lock.RUnlock()

	clientIDs := make([]tvm.ClientID, 0, len(c.tickets))
	for clientID := range c.tickets {
		clientIDs = append(clientIDs, clientID)
	}
	return clientIDs
}

func (c *Cache) Aliases() []string {
	c.lock.RLock()
	defer c.lock.RUnlock()

	aliases := make([]string, 0, len(c.aliases))
	for alias := range c.aliases {
		aliases = append(aliases, alias)
	}
	return aliases
}

func (c *Cache) Load(clientID tvm.ClientID) (*string, Status) {
	c.lock.RLock()
	e, ok := c.tickets[clientID]
	c.lock.RUnlock()
	if !ok {
		return nil, Miss
	}

	now := time.Now()
	exp := e.born.Add(c.ttl)
	if exp.After(now) {
		return e.value, Hit
	}

	exp = e.born.Add(c.maxTTL)
	if exp.After(now) {
		return e.value, GonnaMissy
	}

	c.lock.Lock()
	delete(c.tickets, clientID)
	c.lock.Unlock()
	return nil, Miss
}

func (c *Cache) LoadByAlias(alias string) (*string, Status) {
	c.lock.RLock()
	clientID, ok := c.aliases[alias]
	c.lock.RUnlock()
	if !ok {
		return nil, Miss
	}

	return c.Load(clientID)
}

func (c *Cache) Store(clientID tvm.ClientID, alias string, value *string) {
	c.lock.Lock()
	defer c.lock.Unlock()

	c.aliases[alias] = clientID
	c.tickets[clientID] = entry{
		value: value,
		born:  time.Now(),
	}
}
