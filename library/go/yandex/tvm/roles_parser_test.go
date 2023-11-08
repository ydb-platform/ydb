package tvm

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestNewRolesWithOpts(t *testing.T) {
	type TestCase struct {
		buf   string
		opts  []RoleParserOption
		roles Roles
		err   string
	}

	cases := []TestCase{
		{
			buf:  `{"revision":100500}`,
			opts: []RoleParserOption{},
			err:  "failed to parse roles: invalid json",
		},
		{
			buf:  `{"born_date":1612791978.42}`,
			opts: []RoleParserOption{},
			err:  "failed to parse roles: invalid json",
		},
		{
			buf:  `{"tvm":{"asd":{}}}`,
			opts: []RoleParserOption{},
			err:  "failed to parse roles: invalid tvmid 'asd'",
		},
		{
			buf:  `{"user":{"asd":{}}}`,
			opts: []RoleParserOption{},
			err:  "failed to parse roles: invalid UID 'asd'",
		},
		{
			buf:  `{"tvm":{"1120000000000493":{}}}`,
			opts: []RoleParserOption{},
			err:  "failed to parse roles: invalid tvmid '1120000000000493'",
		},
		{
			buf:  `{"revision":"GYYDEMJUGBQWC","born_date":1612791978,"tvm":{"2012192":{"/group/system/system_on/abc/role/impersonator/":[{"scope":"/"}],"/group/system/system_on/abc/role/tree_edit/":[{"scope":"/"}]}},"user":{"1120000000000493":{"/group/system/system_on/abc/role/roles_manage/":[{"scope":"/services/meta_infra/tools/jobjira/"},{"scope":"/services/meta_edu/infrastructure/"}]}}}`,
			opts: []RoleParserOption{},
			roles: Roles{
				tvmRoles: map[ClientID]*ConsumerRoles{
					ClientID(2012192): {
						roles: EntitiesByRoles{
							"/group/system/system_on/abc/role/impersonator/": {},
							"/group/system/system_on/abc/role/tree_edit/":    {},
						},
					},
				},
				userRoles: map[UID]*ConsumerRoles{
					UID(1120000000000493): {
						roles: EntitiesByRoles{
							"/group/system/system_on/abc/role/roles_manage/": {},
						},
					},
				},
				raw: []byte(`{"revision":"GYYDEMJUGBQWC","born_date":1612791978,"tvm":{"2012192":{"/group/system/system_on/abc/role/impersonator/":[{"scope":"/"}],"/group/system/system_on/abc/role/tree_edit/":[{"scope":"/"}]}},"user":{"1120000000000493":{"/group/system/system_on/abc/role/roles_manage/":[{"scope":"/services/meta_infra/tools/jobjira/"},{"scope":"/services/meta_edu/infrastructure/"}]}}}`),
				meta: Meta{
					Revision: "GYYDEMJUGBQWC",
					BornTime: time.Unix(1612791978, 0),
				},
			},
		},
		{
			buf:  `{"revision":"GYYDEMJUGBQWC","born_date":1612791978,"tvm":{"2012192":{"/group/system/system_on/abc/role/impersonator/":[{"scope":"/"}],"/group/system/system_on/abc/role/tree_edit/":[]}},"user":{"1120000000000493":{"/group/system/system_on/abc/role/roles_manage/":[{"scope":"/services/meta_infra/tools/jobjira/"},{"scope":"/services/meta_edu/infrastructure/","blank":""}],"/group/system/system_on/abc/role/admin/":[{}]}}}`,
			opts: []RoleParserOption{WithLightIndex()},
			roles: Roles{
				tvmRoles: map[ClientID]*ConsumerRoles{
					ClientID(2012192): {
						roles: EntitiesByRoles{
							"/group/system/system_on/abc/role/impersonator/": {},
							"/group/system/system_on/abc/role/tree_edit/":    nil,
						},
					},
				},
				userRoles: map[UID]*ConsumerRoles{
					UID(1120000000000493): {
						roles: EntitiesByRoles{
							"/group/system/system_on/abc/role/roles_manage/": {},
							"/group/system/system_on/abc/role/admin/":        nil,
						},
					},
				},
				raw: []byte(`{"revision":"GYYDEMJUGBQWC","born_date":1612791978,"tvm":{"2012192":{"/group/system/system_on/abc/role/impersonator/":[{"scope":"/"}],"/group/system/system_on/abc/role/tree_edit/":[]}},"user":{"1120000000000493":{"/group/system/system_on/abc/role/roles_manage/":[{"scope":"/services/meta_infra/tools/jobjira/"},{"scope":"/services/meta_edu/infrastructure/","blank":""}],"/group/system/system_on/abc/role/admin/":[{}]}}}`),
				meta: Meta{
					Revision: "GYYDEMJUGBQWC",
					BornTime: time.Unix(1612791978, 0),
				},
			},
		},
	}

	for idx, c := range cases {
		r, err := NewRolesWithOpts([]byte(c.buf), c.opts...)
		if c.err == "" {
			require.NoError(t, err, idx)

			r.meta.Applied = time.Time{}
			for _, roles := range r.tvmRoles {
				for _, v := range roles.roles {
					if v != nil {
						v.subtree = subTree{}
					}
				}
			}
			for _, roles := range r.userRoles {
				for _, v := range roles.roles {
					if v != nil {
						v.subtree = subTree{}
					}
				}
			}

			require.Equal(t, c.roles, *r, idx)
		} else {
			require.Error(t, err, idx)
			require.Contains(t, err.Error(), c.err, idx)
		}
	}
}
