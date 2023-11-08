package tvm

import (
	"time"
)

type Roles struct {
	tvmRoles  map[ClientID]*ConsumerRoles
	userRoles map[UID]*ConsumerRoles
	raw       []byte
	meta      Meta
}

type Meta struct {
	Revision string
	BornTime time.Time
	Applied  time.Time
}

type ConsumerRoles struct {
	roles EntitiesByRoles
}

type EntitiesByRoles = map[string]*Entities

type Entities struct {
	subtree subTree
}

type Entity = map[string]string
