package tvm_test

import (
	"context"
	"fmt"

	"github.com/ydb-platform/ydb/library/go/core/log/nop"
	"github.com/ydb-platform/ydb/library/go/yandex/tvm"
	"github.com/ydb-platform/ydb/library/go/yandex/tvm/tvmauth"
)

func ExampleClient_alias() {
	blackboxAlias := "blackbox"

	settings := tvmauth.TvmAPISettings{
		SelfID: 1000502,
		ServiceTicketOptions: tvmauth.NewAliasesOptions(
			"...",
			map[string]tvm.ClientID{
				blackboxAlias: 1000501,
			}),
	}

	c, err := tvmauth.NewAPIClient(settings, &nop.Logger{})
	if err != nil {
		panic(err)
	}

	// ...

	serviceTicket, _ := c.GetServiceTicketForAlias(context.Background(), blackboxAlias)
	fmt.Printf("Service ticket for visiting backend: %s", serviceTicket)
}

func ExampleClient_roles() {
	settings := tvmauth.TvmAPISettings{
		SelfID:                      1000502,
		ServiceTicketOptions:        tvmauth.NewIDsOptions("...", nil),
		FetchRolesForIdmSystemSlug:  "some_idm_system",
		DiskCacheDir:                "...",
		EnableServiceTicketChecking: true,
	}

	c, err := tvmauth.NewAPIClient(settings, &nop.Logger{})
	if err != nil {
		panic(err)
	}

	// ...

	serviceTicket, _ := c.CheckServiceTicket(context.Background(), "3:serv:...")
	fmt.Printf("Service ticket for visiting backend: %s", serviceTicket)

	r, err := c.GetRoles(context.Background())
	if err != nil {
		panic(err)
	}
	fmt.Println(r.GetMeta().Revision)
}
