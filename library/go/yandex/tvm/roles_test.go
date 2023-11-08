package tvm

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRolesPublicServiceTicket(t *testing.T) {
	roles, err := NewRoles([]byte(`{"revision":"GYYDEMJUGBQWC","born_date":1612791978,"tvm":{"2012192":{"/group/system/system_on/abc/role/impersonator/":[{"scope":"/"},{"blank":""}],"/group/system/system_on/abc/role/tree_edit/":[{"scope":"/"}],"/group/system/system_on/abc/role/admin/":[]}},"user":{"1120000000000493":{"/group/system/system_on/abc/role/roles_manage/":[{"scope":"/services/meta_infra/tools/jobjira/"},{"scope":"/services/meta_edu/infrastructure/"}]}}}`))
	require.NoError(t, err)

	st := &CheckedServiceTicket{SrcID: 42}
	require.Nil(t, roles.GetRolesForService(st))
	require.False(t, roles.CheckServiceRole(st, "/group/system/system_on/abc/role/impersonator/", nil))
	require.False(t, roles.CheckServiceRole(st, "/group/system/system_on/abc/role/admin/", nil))
	require.False(t, roles.CheckServiceRole(st, "/group/system/system_on/abc/role/impersonator/", &CheckServiceOptions{Entity: Entity{"scope": "/"}}))

	st = &CheckedServiceTicket{SrcID: 2012192}
	r := roles.GetRolesForService(st)
	require.NotNil(t, r)
	require.EqualValues(t,
		`{
    "/group/system/system_on/abc/role/admin/": [],
    "/group/system/system_on/abc/role/impersonator/": [
        {
            "scope": "/"
        },
        {
            "blank": ""
        }
    ],
    "/group/system/system_on/abc/role/tree_edit/": [
        {
            "scope": "/"
        }
    ]
}`,
		r.DebugPrint(),
	)
	require.Equal(t, 3, len(r.GetRoles()))
	require.False(t, r.HasRole("/"))
	require.True(t, r.HasRole("/group/system/system_on/abc/role/impersonator/"))
	require.True(t, r.HasRole("/group/system/system_on/abc/role/admin/"))
	require.False(t, roles.CheckServiceRole(st, "/", nil))
	require.True(t, roles.CheckServiceRole(st, "/group/system/system_on/abc/role/impersonator/", nil))
	require.False(t, roles.CheckServiceRole(st, "/group/system/system_on/abc/role/impersonator/", &CheckServiceOptions{Entity: Entity{"scope": "kek"}}))
	require.True(t, roles.CheckServiceRole(st, "/group/system/system_on/abc/role/impersonator/", &CheckServiceOptions{Entity{"scope": "/"}}))
	require.False(t, roles.CheckServiceRole(st, "/group/system/system_on/abc/role/impersonator/", &CheckServiceOptions{Entity{"blank": "/"}}))
	require.True(t, roles.CheckServiceRole(st, "/group/system/system_on/abc/role/impersonator/", &CheckServiceOptions{Entity{"blank": ""}}))
	require.False(t, roles.CheckServiceRole(st, "/group/system/system_on/abc/role/admin/", &CheckServiceOptions{Entity{"scope": "/"}}))
	require.Nil(t, r.GetEntitiesForRole("/"))

	en := r.GetEntitiesForRole("/group/system/system_on/abc/role/impersonator/")
	require.NotNil(t, en)
	require.False(t, en.ContainsExactEntity(Entity{"scope": "kek"}))
	require.True(t, en.ContainsExactEntity(Entity{"scope": "/"}))
	require.False(t, en.ContainsExactEntity(Entity{"blank": "/"}))
	require.True(t, en.ContainsExactEntity(Entity{"blank": ""}))

	require.Nil(t, en.GetEntitiesWithAttrs(Entity{"scope": "kek"}))
	require.Equal(t, []Entity{{"scope": "/"}}, en.GetEntitiesWithAttrs(Entity{"scope": "/"}))
	require.Nil(t, en.GetEntitiesWithAttrs(Entity{"blank": "kek"}))
	require.Equal(t, []Entity{{"blank": ""}}, en.GetEntitiesWithAttrs(Entity{"blank": ""}))
	require.ElementsMatch(t, []Entity{{"scope": "/"}, {"blank": ""}}, en.GetEntitiesWithAttrs(nil))

	en = r.GetEntitiesForRole("/group/system/system_on/abc/role/admin/")
	require.NotNil(t, en)
	require.False(t, en.ContainsExactEntity(Entity{"scope": "/"}))

	require.Nil(t, en.GetEntitiesWithAttrs(Entity{"scope": "/"}))
}

func TestRolesPublicServiceTicketWithNilEntities(t *testing.T) {
	roles, err := NewRolesWithOpts(
		[]byte(`{"revision":"GYYDEMJUGBQWC","born_date":1612791978,"tvm":{"2012192":{"/group/system/system_on/abc/role/impersonator/":[{"scope":"/"},{"blank":""}],"/group/system/system_on/abc/role/tree_edit/":[{"scope":"/"}],"/group/system/system_on/abc/role/admin/":[{}]}},"user":{"1120000000000493":{"/group/system/system_on/abc/role/roles_manage/":[{"scope":"/services/meta_infra/tools/jobjira/"},{"scope":"/services/meta_edu/infrastructure/"}]}}}`),
		WithLightIndex(),
	)
	require.NoError(t, err)

	st := &CheckedServiceTicket{SrcID: 42}
	require.Nil(t, roles.GetRolesForService(st))
	require.False(t, roles.CheckServiceRole(st, "/group/system/system_on/abc/role/impersonator/", nil))
	require.False(t, roles.CheckServiceRole(st, "/group/system/system_on/abc/role/admin/", nil))
	require.False(t, roles.CheckServiceRole(st, "/group/system/system_on/abc/role/impersonator/", &CheckServiceOptions{Entity: Entity{"scope": "/"}}))

	st = &CheckedServiceTicket{SrcID: 2012192}
	r := roles.GetRolesForService(st)
	require.NotNil(t, r)
	require.EqualValues(t,
		`{
    "/group/system/system_on/abc/role/admin/": null,
    "/group/system/system_on/abc/role/impersonator/": [
        {
            "scope": "/"
        },
        {
            "blank": ""
        }
    ],
    "/group/system/system_on/abc/role/tree_edit/": [
        {
            "scope": "/"
        }
    ]
}`,
		r.DebugPrint(),
	)
	require.Equal(t, 3, len(r.GetRoles()))
	require.False(t, r.HasRole("/"))
	require.True(t, r.HasRole("/group/system/system_on/abc/role/impersonator/"))
	require.True(t, r.HasRole("/group/system/system_on/abc/role/admin/"))
	require.False(t, roles.CheckServiceRole(st, "/", nil))
	require.True(t, roles.CheckServiceRole(st, "/group/system/system_on/abc/role/impersonator/", nil))
	require.False(t, roles.CheckServiceRole(st, "/group/system/system_on/abc/role/impersonator/", &CheckServiceOptions{Entity: Entity{"scope": "kek"}}))
	require.False(t, roles.CheckServiceRole(st, "/group/system/system_on/abc/role/impersonator/", &CheckServiceOptions{Entity{"scope": "/"}}))
	require.False(t, roles.CheckServiceRole(st, "/group/system/system_on/abc/role/impersonator/", &CheckServiceOptions{Entity{"blank": "/"}}))
	require.False(t, roles.CheckServiceRole(st, "/group/system/system_on/abc/role/impersonator/", &CheckServiceOptions{Entity{"blank": ""}}))
	require.False(t, roles.CheckServiceRole(st, "/group/system/system_on/abc/role/admin/", &CheckServiceOptions{Entity{"scope": "/"}}))
	require.Nil(t, r.GetEntitiesForRole("/"))

	en := r.GetEntitiesForRole("/group/system/system_on/abc/role/impersonator/")
	require.NotNil(t, en)
	require.False(t, en.ContainsExactEntity(Entity{"scope": "kek"}))
	require.False(t, en.ContainsExactEntity(Entity{"scope": "/"}))
	require.False(t, en.ContainsExactEntity(Entity{"blank": "/"}))
	require.False(t, en.ContainsExactEntity(Entity{"blank": ""}))

	require.Nil(t, en.GetEntitiesWithAttrs(Entity{"scope": "kek"}))
	require.Nil(t, en.GetEntitiesWithAttrs(Entity{"scope": "/"}))
	require.Nil(t, en.GetEntitiesWithAttrs(Entity{"blank": "kek"}))
	require.Nil(t, en.GetEntitiesWithAttrs(Entity{"blank": ""}))
	require.ElementsMatch(t, []Entity{{"scope": "/"}, {"blank": ""}}, en.GetEntitiesWithAttrs(nil))

	en = r.GetEntitiesForRole("/group/system/system_on/abc/role/admin/")
	require.Nil(t, en)
	require.False(t, en.ContainsExactEntity(Entity{"scope": "/"}))

	require.Nil(t, en.GetEntitiesWithAttrs(Entity{"scope": "/"}))
}

func TestRolesPublicUserTicket(t *testing.T) {
	roles, err := NewRoles([]byte(`{"revision":"GYYDEMJUGBQWC","born_date":1612791978,"tvm":{"2012192":{"/group/system/system_on/abc/role/impersonator/":[{"scope":"/"},{"blank":""}],"/group/system/system_on/abc/role/tree_edit/":[{"scope":"/"}]}},"user":{"1120000000000493":{"/group/system/system_on/abc/role/roles_manage/":[{"scope":"/services/meta_infra/tools/jobjira/"},{"scope":"/services/meta_edu/infrastructure/"}],"/group/system/system_on/abc/role/roles_admin/":[]}}}`))
	require.NoError(t, err)

	ut := &CheckedUserTicket{DefaultUID: 42}
	_, err = roles.GetRolesForUser(ut, nil)
	require.EqualError(t, err, "user ticket must be from ProdYateam, got from Prod")
	ut.Env = BlackboxProdYateam

	r, err := roles.GetRolesForUser(ut, nil)
	require.NoError(t, err)
	require.Nil(t, r)
	ok, err := roles.CheckUserRole(ut, "/group/system/system_on/abc/role/impersonator/", nil)
	require.NoError(t, err)
	require.False(t, ok)
	ok, err = roles.CheckUserRole(ut, "/group/system/system_on/abc/role/impersonator/", &CheckUserOptions{Entity: Entity{"scope": "/"}})
	require.NoError(t, err)
	require.False(t, ok)
	ok, err = roles.CheckUserRole(ut, "/group/system/system_on/abc/role/admin/", nil)
	require.NoError(t, err)
	require.False(t, ok)
	ok, err = roles.CheckUserRole(ut, "/group/system/system_on/abc/role/roles_admin/", &CheckUserOptions{Entity: Entity{"scope": "/"}})
	require.NoError(t, err)
	require.False(t, ok)

	ut = &CheckedUserTicket{DefaultUID: 1120000000000493, UIDs: []UID{42}, Env: BlackboxProdYateam}
	r, err = roles.GetRolesForUser(ut, nil)
	require.NoError(t, err)
	require.NotNil(t, r)
	require.EqualValues(t,
		`{
    "/group/system/system_on/abc/role/roles_admin/": [],
    "/group/system/system_on/abc/role/roles_manage/": [
        {
            "scope": "/services/meta_infra/tools/jobjira/"
        },
        {
            "scope": "/services/meta_edu/infrastructure/"
        }
    ]
}`,
		r.DebugPrint(),
	)
	require.Equal(t, 2, len(r.GetRoles()))
	require.False(t, r.HasRole("/"))
	require.True(t, r.HasRole("/group/system/system_on/abc/role/roles_manage/"))
	ok, err = roles.CheckUserRole(ut, "/", nil)
	require.NoError(t, err)
	require.False(t, ok)
	ok, err = roles.CheckUserRole(ut, "/group/system/system_on/abc/role/roles_manage/", nil)
	require.NoError(t, err)
	require.True(t, ok)
	ok, err = roles.CheckUserRole(ut, "/group/system/system_on/abc/role/roles_manage/", &CheckUserOptions{Entity: Entity{"scope": "kek"}})
	require.NoError(t, err)
	require.False(t, ok)
	ok, err = roles.CheckUserRole(ut, "/group/system/system_on/abc/role/roles_manage/", &CheckUserOptions{Entity: Entity{"scope": "/services/meta_infra/tools/jobjira/"}})
	require.NoError(t, err)
	require.True(t, ok)
	ok, err = roles.CheckUserRole(ut, "/group/system/system_on/abc/role/roles_admin/", nil)
	require.NoError(t, err)
	require.True(t, ok)
	ok, err = roles.CheckUserRole(ut, "/group/system/system_on/abc/role/roles_admin/", &CheckUserOptions{Entity: Entity{"scope": "/"}})
	require.NoError(t, err)
	require.False(t, ok)

	ok, err = roles.CheckUserRole(ut, "/group/system/system_on/abc/role/roles_manage/", &CheckUserOptions{UID: UID(42)})
	require.NoError(t, err)
	require.False(t, ok)

	ut = &CheckedUserTicket{DefaultUID: 0, UIDs: []UID{42}, Env: BlackboxProdYateam}
	_, err = roles.GetRolesForUser(ut, nil)
	require.EqualError(t, err, "default uid is 0 - it cannot have any role")
	uid := UID(83)
	_, err = roles.GetRolesForUser(ut, &uid)
	require.EqualError(t, err, "'uid' must be in user ticket but it is not: 83")
}

func TestRolesPublicUserTicketWithNilEntities(t *testing.T) {
	roles, err := NewRolesWithOpts(
		[]byte(`{"revision":"GYYDEMJUGBQWC","born_date":1612791978,"tvm":{"2012192":{"/group/system/system_on/abc/role/impersonator/":[{"scope":"/"},{"blank":""}],"/group/system/system_on/abc/role/tree_edit/":[{"scope":"/"}]}},"user":{"1120000000000493":{"/group/system/system_on/abc/role/roles_manage/":[{"scope":"/services/meta_infra/tools/jobjira/"},{"scope":"/services/meta_edu/infrastructure/"}],"/group/system/system_on/abc/role/roles_admin/":[{}]}}}`),
		WithLightIndex(),
	)
	require.NoError(t, err)

	ut := &CheckedUserTicket{DefaultUID: 42}
	_, err = roles.GetRolesForUser(ut, nil)
	require.EqualError(t, err, "user ticket must be from ProdYateam, got from Prod")
	ut.Env = BlackboxProdYateam

	r, err := roles.GetRolesForUser(ut, nil)
	require.NoError(t, err)
	require.Nil(t, r)
	ok, err := roles.CheckUserRole(ut, "/group/system/system_on/abc/role/impersonator/", nil)
	require.NoError(t, err)
	require.False(t, ok)
	ok, err = roles.CheckUserRole(ut, "/group/system/system_on/abc/role/impersonator/", &CheckUserOptions{Entity: Entity{"scope": "/"}})
	require.NoError(t, err)
	require.False(t, ok)
	ok, err = roles.CheckUserRole(ut, "/group/system/system_on/abc/role/admin/", nil)
	require.NoError(t, err)
	require.False(t, ok)
	ok, err = roles.CheckUserRole(ut, "/group/system/system_on/abc/role/roles_admin/", &CheckUserOptions{Entity: Entity{"scope": "/"}})
	require.NoError(t, err)
	require.False(t, ok)

	ut = &CheckedUserTicket{DefaultUID: 1120000000000493, UIDs: []UID{42}, Env: BlackboxProdYateam}
	r, err = roles.GetRolesForUser(ut, nil)
	require.NoError(t, err)
	require.NotNil(t, r)
	require.EqualValues(t,
		`{
    "/group/system/system_on/abc/role/roles_admin/": null,
    "/group/system/system_on/abc/role/roles_manage/": [
        {
            "scope": "/services/meta_infra/tools/jobjira/"
        },
        {
            "scope": "/services/meta_edu/infrastructure/"
        }
    ]
}`,
		r.DebugPrint(),
	)
	require.Equal(t, 2, len(r.GetRoles()))
	require.False(t, r.HasRole("/"))
	require.True(t, r.HasRole("/group/system/system_on/abc/role/roles_manage/"))
	ok, err = roles.CheckUserRole(ut, "/", nil)
	require.NoError(t, err)
	require.False(t, ok)
	ok, err = roles.CheckUserRole(ut, "/group/system/system_on/abc/role/roles_manage/", nil)
	require.NoError(t, err)
	require.True(t, ok)
	ok, err = roles.CheckUserRole(ut, "/group/system/system_on/abc/role/roles_manage/", &CheckUserOptions{Entity: Entity{"scope": "kek"}})
	require.NoError(t, err)
	require.False(t, ok)
	ok, err = roles.CheckUserRole(ut, "/group/system/system_on/abc/role/roles_manage/", &CheckUserOptions{Entity: Entity{"scope": "/services/meta_infra/tools/jobjira/"}})
	require.NoError(t, err)
	require.False(t, ok)
	ok, err = roles.CheckUserRole(ut, "/group/system/system_on/abc/role/roles_admin/", nil)
	require.NoError(t, err)
	require.True(t, ok)
	ok, err = roles.CheckUserRole(ut, "/group/system/system_on/abc/role/roles_admin/", &CheckUserOptions{Entity: Entity{"scope": "/"}})
	require.NoError(t, err)
	require.False(t, ok)

	ok, err = roles.CheckUserRole(ut, "/group/system/system_on/abc/role/roles_manage/", &CheckUserOptions{UID: UID(42)})
	require.NoError(t, err)
	require.False(t, ok)

	ut = &CheckedUserTicket{DefaultUID: 0, UIDs: []UID{42}, Env: BlackboxProdYateam}
	_, err = roles.GetRolesForUser(ut, nil)
	require.EqualError(t, err, "default uid is 0 - it cannot have any role")
	uid := UID(83)
	_, err = roles.GetRolesForUser(ut, &uid)
	require.EqualError(t, err, "'uid' must be in user ticket but it is not: 83")
}
