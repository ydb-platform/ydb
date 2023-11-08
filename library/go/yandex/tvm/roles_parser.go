package tvm

import (
	"encoding/json"
	"strconv"
	"time"

	"github.com/ydb-platform/ydb/library/go/core/xerrors"
)

type rawRoles struct {
	Revision string       `json:"revision"`
	BornDate int64        `json:"born_date"`
	Tvm      rawConsumers `json:"tvm"`
	User     rawConsumers `json:"user"`
}

type rawConsumers = map[string]rawConsumerRoles
type rawConsumerRoles = map[string][]Entity

func NewRoles(buf []byte) (*Roles, error) {
	return NewRolesWithOpts(buf)
}

func NewRolesWithOpts(buf []byte, opts ...RoleParserOption) (*Roles, error) {
	options := newRolesParserOptions(opts...)

	var raw rawRoles
	if err := json.Unmarshal(buf, &raw); err != nil {
		return nil, xerrors.Errorf("failed to parse roles: invalid json: %w", err)
	}

	tvmRoles := map[ClientID]*ConsumerRoles{}
	for key, value := range raw.Tvm {
		id, err := strconv.ParseUint(key, 10, 32)
		if err != nil {
			return nil, xerrors.Errorf("failed to parse roles: invalid tvmid '%s': %w", key, err)
		}
		tvmRoles[ClientID(id)] = buildConsumerRoles(value, options)
	}

	userRoles := map[UID]*ConsumerRoles{}
	for key, value := range raw.User {
		id, err := strconv.ParseUint(key, 10, 64)
		if err != nil {
			return nil, xerrors.Errorf("failed to parse roles: invalid UID '%s': %w", key, err)
		}
		userRoles[UID(id)] = buildConsumerRoles(value, options)
	}

	return &Roles{
		tvmRoles:  tvmRoles,
		userRoles: userRoles,
		raw:       buf,
		meta: Meta{
			Revision: raw.Revision,
			BornTime: time.Unix(raw.BornDate, 0),
			Applied:  time.Now(),
		},
	}, nil
}

func buildConsumerRoles(rawConsumerRoles rawConsumerRoles, opts *rolesParserOptions) *ConsumerRoles {
	roles := &ConsumerRoles{
		roles: make(EntitiesByRoles, len(rawConsumerRoles)),
	}

	for r, ents := range rawConsumerRoles {
		if opts.UseLightIndex {
			roles.roles[r] = buildLightEntities(ents)
		} else {
			roles.roles[r] = buildEntities(ents)
		}
	}

	return roles
}
