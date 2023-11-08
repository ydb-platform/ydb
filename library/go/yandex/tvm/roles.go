package tvm

import (
	"encoding/json"

	"github.com/ydb-platform/ydb/library/go/core/xerrors"
)

func (r *Roles) GetRolesForService(t *CheckedServiceTicket) *ConsumerRoles {
	return r.tvmRoles[t.SrcID]
}

func (r *Roles) GetRolesForUser(t *CheckedUserTicket, uid *UID) (*ConsumerRoles, error) {
	if t.Env != BlackboxProdYateam {
		return nil, xerrors.Errorf("user ticket must be from ProdYateam, got from %s", t.Env)
	}

	if uid == nil {
		if t.DefaultUID == 0 {
			return nil, xerrors.Errorf("default uid is 0 - it cannot have any role")
		}
		uid = &t.DefaultUID
	} else {
		found := false
		for _, u := range t.UIDs {
			if u == *uid {
				found = true
				break
			}
		}
		if !found {
			return nil, xerrors.Errorf("'uid' must be in user ticket but it is not: %d", *uid)
		}
	}

	return r.userRoles[*uid], nil
}

func (r *Roles) GetRaw() []byte {
	return r.raw
}

func (r *Roles) GetMeta() Meta {
	return r.meta
}

func (r *Roles) CheckServiceRole(t *CheckedServiceTicket, roleName string, opts *CheckServiceOptions) bool {
	roles := r.GetRolesForService(t)

	if !roles.HasRole(roleName) {
		return false
	}

	if opts != nil && opts.Entity != nil {
		e := roles.GetEntitiesForRole(roleName)
		if e == nil {
			return false
		}

		if !e.ContainsExactEntity(opts.Entity) {
			return false
		}
	}

	return true
}

func (r *Roles) CheckUserRole(t *CheckedUserTicket, roleName string, opts *CheckUserOptions) (bool, error) {
	var uid *UID
	if opts != nil && opts.UID != 0 {
		uid = &opts.UID
	}

	roles, err := r.GetRolesForUser(t, uid)
	if err != nil {
		return false, err
	}

	if !roles.HasRole(roleName) {
		return false, nil
	}

	if opts != nil && opts.Entity != nil {
		e := roles.GetEntitiesForRole(roleName)
		if e == nil {
			return false, nil
		}

		if !e.ContainsExactEntity(opts.Entity) {
			return false, nil
		}
	}

	return true, nil
}

func (r *ConsumerRoles) HasRole(roleName string) bool {
	if r == nil {
		return false
	}

	_, ok := r.roles[roleName]
	return ok
}

func (r *ConsumerRoles) GetRoles() EntitiesByRoles {
	if r == nil {
		return nil
	}
	return r.roles
}

func (r *ConsumerRoles) GetEntitiesForRole(roleName string) *Entities {
	if r == nil {
		return nil
	}
	return r.roles[roleName]
}

func (r *ConsumerRoles) DebugPrint() string {
	tmp := make(map[string][]Entity)

	for k, v := range r.roles {
		if v != nil {
			tmp[k] = v.subtree.entities
		} else {
			tmp[k] = nil
		}
	}

	res, err := json.MarshalIndent(tmp, "", "    ")
	if err != nil {
		panic(err)
	}
	return string(res)
}

func (e *Entities) ContainsExactEntity(entity Entity) bool {
	if e == nil {
		return false
	}
	return e.subtree.containsExactEntity(entity)
}

func (e *Entities) GetEntitiesWithAttrs(entityPart Entity) []Entity {
	if e == nil {
		return nil
	}
	return e.subtree.getEntitiesWithAttrs(entityPart)
}
