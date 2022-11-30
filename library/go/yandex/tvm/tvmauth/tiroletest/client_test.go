package tiroletest

import (
	"context"
	"io/ioutil"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"

	"a.yandex-team.ru/library/go/core/log/nop"
	"a.yandex-team.ru/library/go/yandex/tvm"
	"a.yandex-team.ru/library/go/yandex/tvm/tvmauth"
)

func getPort(t *testing.T, filename string) int {
	body, err := ioutil.ReadFile(filename)
	require.NoError(t, err)

	res, err := strconv.Atoi(string(body))
	require.NoError(t, err, "port is invalid: ", filename)

	return res
}

func createClientWithTirole(t *testing.T, disableSrcCheck bool, disableDefaultUIDCheck bool) *tvmauth.Client {
	env := tvm.BlackboxProdYateam
	client, err := tvmauth.NewAPIClient(
		tvmauth.TvmAPISettings{
			SelfID:                      1000502,
			ServiceTicketOptions:        tvmauth.NewIDsOptions("e5kL0vM3nP-nPf-388Hi6Q", nil),
			DiskCacheDir:                "./",
			FetchRolesForIdmSystemSlug:  "some_slug_2",
			EnableServiceTicketChecking: true,
			DisableSrcCheck:             disableSrcCheck,
			DisableDefaultUIDCheck:      disableDefaultUIDCheck,
			BlackboxEnv:                 &env,
			TVMHost:                     "http://localhost",
			TVMPort:                     getPort(t, "tvmapi.port"),
			TiroleHost:                  "http://localhost",
			TirolePort:                  getPort(t, "tirole.port"),
			TiroleTvmID:                 1000001,
		},
		&nop.Logger{},
	)
	require.NoError(t, err)

	return client
}

func createClientWithTvmtool(t *testing.T, disableSrcCheck bool, disableDefaultUIDCheck bool) *tvmauth.Client {
	token, err := ioutil.ReadFile("tvmtool.authtoken")
	require.NoError(t, err)

	client, err := tvmauth.NewToolClient(
		tvmauth.TvmToolSettings{
			Alias:                  "me",
			AuthToken:              string(token),
			DisableSrcCheck:        disableSrcCheck,
			DisableDefaultUIDCheck: disableDefaultUIDCheck,
			Port:                   getPort(t, "tvmtool.port"),
		},
		&nop.Logger{},
	)
	require.NoError(t, err)

	return client
}

func checkServiceNoRoles(t *testing.T, clientsWithAutoCheck, clientsWithoutAutoCheck []tvm.Client) {
	// src=1000000000: tvmknife unittest service -s 1000000000 -d 1000502
	stWithoutRoles := "3:serv:CBAQ__________9_IgoIgJTr3AMQtog9:Sv3SKuDQ4p-2419PKqc1vo9EC128K6Iv7LKck5SyliJZn5gTAqMDAwb9aYWHhf49HTR-Qmsjw4i_Lh-sNhge-JHWi5PTGFJm03CZHOCJG9Y0_G1pcgTfodtAsvDykMxLhiXGB4N84cGhVVqn1pFWz6SPmMeKUPulTt7qH1ifVtQ"

	ctx := context.Background()

	for _, cl := range clientsWithAutoCheck {
		_, err := cl.CheckServiceTicket(ctx, stWithoutRoles)
		require.EqualValues(t,
			&tvm.TicketError{Status: tvm.TicketNoRoles},
			err,
		)
	}

	for _, cl := range clientsWithoutAutoCheck {
		st, err := cl.CheckServiceTicket(ctx, stWithoutRoles)
		require.NoError(t, err)

		roles, err := cl.GetRoles(ctx)
		require.NoError(t, err)

		res := roles.GetRolesForService(st)
		require.Nil(t, res)
	}
}

func checkServiceHasRoles(t *testing.T, clientsWithAutoCheck, clientsWithoutAutoCheck []tvm.Client) {
	// src=1000000001: tvmknife unittest service -s 1000000001 -d 1000502
	stWithRoles := "3:serv:CBAQ__________9_IgoIgZTr3AMQtog9:EyPympmoLBM6jyiQLcK8ummNmL5IUAdTvKM1do8ppuEgY6yHfto3s_WAKmP9Pf9EiNqPBe18HR7yKmVS7gvdFJY4gP4Ut51ejS-iBPlsbsApJOYTgodQPhkmjHVKIT0ub0pT3fWHQtapb8uimKpGcO6jCfopFQSVG04Ehj7a0jw"

	ctx := context.Background()

	check := func(cl tvm.Client) {
		checked, err := cl.CheckServiceTicket(ctx, stWithRoles)
		require.NoError(t, err)

		clientRoles, err := cl.GetRoles(ctx)
		require.NoError(t, err)

		require.EqualValues(t,
			`{
    "/role/service/read/": [],
    "/role/service/write/": [
        {
            "foo": "bar",
            "kek": "lol"
        }
    ]
}`,
			clientRoles.GetRolesForService(checked).DebugPrint(),
		)

		require.True(t, clientRoles.CheckServiceRole(checked, "/role/service/read/", nil))
		require.True(t, clientRoles.CheckServiceRole(checked, "/role/service/write/", nil))
		require.False(t, clientRoles.CheckServiceRole(checked, "/role/foo/", nil))

		require.False(t, clientRoles.CheckServiceRole(checked, "/role/service/read/", &tvm.CheckServiceOptions{
			Entity: tvm.Entity{"foo": "bar", "kek": "lol"},
		}))
		require.False(t, clientRoles.CheckServiceRole(checked, "/role/service/write/", &tvm.CheckServiceOptions{
			Entity: tvm.Entity{"kek": "lol"},
		}))
		require.True(t, clientRoles.CheckServiceRole(checked, "/role/service/write/", &tvm.CheckServiceOptions{
			Entity: tvm.Entity{"foo": "bar", "kek": "lol"},
		}))
	}

	for _, cl := range clientsWithAutoCheck {
		check(cl)
	}
	for _, cl := range clientsWithoutAutoCheck {
		check(cl)
	}
}

func checkUserNoRoles(t *testing.T, clientsWithAutoCheck, clientsWithoutAutoCheck []tvm.Client) {
	// default_uid=1000000000: tvmknife unittest user -d 1000000000 --env prod_yateam
	utWithoutRoles := "3:user:CAwQ__________9_GhYKBgiAlOvcAxCAlOvcAyDShdjMBCgC:LloRDlCZ4vd0IUTOj6MD1mxBPgGhS6EevnnWvHgyXmxc--2CVVkAtNKNZJqCJ6GtDY4nknEnYmWvEu6-MInibD-Uk6saI1DN-2Y3C1Wdsz2SJCq2OYgaqQsrM5PagdyP9PLrftkuV_ZluS_FUYebMXPzjJb0L0ALKByMPkCVWuk"

	ctx := context.Background()

	for _, cl := range clientsWithAutoCheck {
		_, err := cl.CheckUserTicket(ctx, utWithoutRoles)
		require.EqualValues(t,
			&tvm.TicketError{Status: tvm.TicketNoRoles},
			err,
		)
	}

	for _, cl := range clientsWithoutAutoCheck {
		ut, err := cl.CheckUserTicket(ctx, utWithoutRoles)
		require.NoError(t, err)

		roles, err := cl.GetRoles(ctx)
		require.NoError(t, err)

		res, err := roles.GetRolesForUser(ut, nil)
		require.NoError(t, err)
		require.Nil(t, res)
	}
}

func checkUserHasRoles(t *testing.T, clientsWithAutoCheck, clientsWithoutAutoCheck []tvm.Client) {
	// default_uid=1120000000000001: tvmknife unittest user -d 1120000000000001 --env prod_yateam
	utWithRoles := "3:user:CAwQ__________9_GhwKCQiBgJiRpdT-ARCBgJiRpdT-ASDShdjMBCgC:SQV7Z9hDpZ_F62XGkSF6yr8PoZHezRp0ZxCINf_iAbT2rlEiO6j4UfLjzwn3EnRXkAOJxuAtTDCnHlrzdh3JgSKK7gciwPstdRT5GGTixBoUU9kI_UlxEbfGBX1DfuDsw_GFQ2eCLu4Svq6jC3ynuqQ41D2RKopYL8Bx8PDZKQc"

	ctx := context.Background()

	check := func(cl tvm.Client) {
		checked, err := cl.CheckUserTicket(ctx, utWithRoles)
		require.NoError(t, err)

		clientRoles, err := cl.GetRoles(ctx)
		require.NoError(t, err)

		ut, err := clientRoles.GetRolesForUser(checked, nil)
		require.NoError(t, err)
		require.EqualValues(t,
			`{
    "/role/user/read/": [
        {
            "foo": "bar",
            "kek": "lol"
        }
    ],
    "/role/user/write/": []
}`,
			ut.DebugPrint(),
		)

		res, err := clientRoles.CheckUserRole(checked, "/role/user/write/", nil)
		require.NoError(t, err)
		require.True(t, res)
		res, err = clientRoles.CheckUserRole(checked, "/role/user/read/", nil)
		require.NoError(t, err)
		require.True(t, res)
		res, err = clientRoles.CheckUserRole(checked, "/role/foo/", nil)
		require.NoError(t, err)
		require.False(t, res)

		res, err = clientRoles.CheckUserRole(checked, "/role/user/write/", &tvm.CheckUserOptions{
			Entity: tvm.Entity{"foo": "bar", "kek": "lol"},
		})
		require.NoError(t, err)
		require.False(t, res)
		res, err = clientRoles.CheckUserRole(checked, "/role/user/read/", &tvm.CheckUserOptions{
			Entity: tvm.Entity{"kek": "lol"},
		})
		require.NoError(t, err)
		require.False(t, res)
		res, err = clientRoles.CheckUserRole(checked, "/role/user/read/", &tvm.CheckUserOptions{
			Entity: tvm.Entity{"foo": "bar", "kek": "lol"},
		})
		require.NoError(t, err)
		require.True(t, res)
	}

	for _, cl := range clientsWithAutoCheck {
		check(cl)
	}
	for _, cl := range clientsWithoutAutoCheck {
		check(cl)
	}

}

func TestRolesFromTiroleCheckSrc_noRoles(t *testing.T) {
	clientWithAutoCheck := createClientWithTirole(t, false, true)
	clientWithoutAutoCheck := createClientWithTirole(t, true, true)

	checkServiceNoRoles(t,
		[]tvm.Client{clientWithAutoCheck},
		[]tvm.Client{clientWithoutAutoCheck},
	)

	clientWithAutoCheck.Destroy()
	clientWithoutAutoCheck.Destroy()
}

func TestRolesFromTiroleCheckSrc_HasRoles(t *testing.T) {
	clientWithAutoCheck := createClientWithTirole(t, false, true)
	clientWithoutAutoCheck := createClientWithTirole(t, true, true)

	checkServiceHasRoles(t,
		[]tvm.Client{clientWithAutoCheck},
		[]tvm.Client{clientWithoutAutoCheck},
	)

	clientWithAutoCheck.Destroy()
	clientWithoutAutoCheck.Destroy()
}

func TestRolesFromTiroleCheckDefaultUid_noRoles(t *testing.T) {
	clientWithAutoCheck := createClientWithTirole(t, true, false)
	clientWithoutAutoCheck := createClientWithTirole(t, true, true)

	checkUserNoRoles(t,
		[]tvm.Client{clientWithAutoCheck},
		[]tvm.Client{clientWithoutAutoCheck},
	)

	clientWithAutoCheck.Destroy()
	clientWithoutAutoCheck.Destroy()
}

func TestRolesFromTiroleCheckDefaultUid_HasRoles(t *testing.T) {
	clientWithAutoCheck := createClientWithTirole(t, true, false)
	clientWithoutAutoCheck := createClientWithTirole(t, true, true)

	checkUserHasRoles(t,
		[]tvm.Client{clientWithAutoCheck},
		[]tvm.Client{clientWithoutAutoCheck},
	)

	clientWithAutoCheck.Destroy()
	clientWithoutAutoCheck.Destroy()
}

func TestRolesFromTvmtoolCheckSrc_noRoles(t *testing.T) {
	clientWithAutoCheck := createClientWithTvmtool(t, false, true)
	clientWithoutAutoCheck := createClientWithTvmtool(t, true, true)

	checkServiceNoRoles(t,
		[]tvm.Client{clientWithAutoCheck},
		[]tvm.Client{clientWithoutAutoCheck},
	)

	clientWithAutoCheck.Destroy()
	clientWithoutAutoCheck.Destroy()
}

func TestRolesFromTvmtoolCheckSrc_HasRoles(t *testing.T) {
	clientWithAutoCheck := createClientWithTvmtool(t, false, true)
	clientWithoutAutoCheck := createClientWithTvmtool(t, true, true)

	checkServiceHasRoles(t,
		[]tvm.Client{clientWithAutoCheck},
		[]tvm.Client{clientWithoutAutoCheck},
	)

	clientWithAutoCheck.Destroy()
	clientWithoutAutoCheck.Destroy()
}

func TestRolesFromTvmtoolCheckDefaultUid_noRoles(t *testing.T) {
	clientWithAutoCheck := createClientWithTvmtool(t, true, false)
	clientWithoutAutoCheck := createClientWithTvmtool(t, true, true)

	checkUserNoRoles(t,
		[]tvm.Client{clientWithAutoCheck},
		[]tvm.Client{clientWithoutAutoCheck},
	)

	clientWithAutoCheck.Destroy()
	clientWithoutAutoCheck.Destroy()
}

func TestRolesFromTvmtoolCheckDefaultUid_HasRoles(t *testing.T) {
	clientWithAutoCheck := createClientWithTvmtool(t, true, false)
	clientWithoutAutoCheck := createClientWithTvmtool(t, true, true)

	checkUserHasRoles(t,
		[]tvm.Client{clientWithAutoCheck},
		[]tvm.Client{clientWithoutAutoCheck},
	)

	clientWithAutoCheck.Destroy()
	clientWithoutAutoCheck.Destroy()
}
