package tvmauth_test

import (
	"context"
	"fmt"

	"a.yandex-team.ru/library/go/core/log/nop"
	"a.yandex-team.ru/library/go/yandex/tvm"
	"a.yandex-team.ru/library/go/yandex/tvm/tvmauth"
)

func ExampleNewAPIClient_getServiceTicketsWithAliases() {
	blackboxAlias := "blackbox"
	datasyncAlias := "datasync"

	settings := tvmauth.TvmAPISettings{
		SelfID: 1000501,
		ServiceTicketOptions: tvmauth.NewAliasesOptions(
			"bAicxJVa5uVY7MjDlapthw",
			map[string]tvm.ClientID{
				blackboxAlias: 1000502,
				datasyncAlias: 1000503,
			}),
		DiskCacheDir: "/var/tmp/cache/tvm/",
	}

	c, err := tvmauth.NewAPIClient(settings, &nop.Logger{})
	if err != nil {
		panic(err)
	}

	// ...

	serviceTicket, _ := c.GetServiceTicketForAlias(context.Background(), blackboxAlias)
	fmt.Printf("Service ticket for visiting backend: %s", serviceTicket)
}

func ExampleNewAPIClient_getServiceTicketsWithID() {
	blackboxID := tvm.ClientID(1000502)
	datasyncID := tvm.ClientID(1000503)

	settings := tvmauth.TvmAPISettings{
		SelfID: 1000501,
		ServiceTicketOptions: tvmauth.NewIDsOptions(
			"bAicxJVa5uVY7MjDlapthw",
			[]tvm.ClientID{
				blackboxID,
				datasyncID,
			}),
		DiskCacheDir: "/var/tmp/cache/tvm/",
	}

	c, err := tvmauth.NewAPIClient(settings, &nop.Logger{})
	if err != nil {
		panic(err)
	}

	// ...

	serviceTicket, _ := c.GetServiceTicketForID(context.Background(), blackboxID)
	fmt.Printf("Service ticket for visiting backend: %s", serviceTicket)
}

func ExampleNewAPIClient_checkServiceTicket() {
	// allowed tvm consumers for your service
	acl := map[tvm.ClientID]interface{}{}

	settings := tvmauth.TvmAPISettings{
		SelfID:                      1000501,
		EnableServiceTicketChecking: true,
		DiskCacheDir:                "/var/tmp/cache/tvm/",
	}

	c, err := tvmauth.NewAPIClient(settings, &nop.Logger{})
	if err != nil {
		panic(err)
	}

	// ...
	serviceTicketFromRequest := "kek"

	serviceTicketStruct, err := c.CheckServiceTicket(context.Background(), serviceTicketFromRequest)
	if err != nil {
		response := map[string]string{
			"error":  "service ticket is invalid",
			"desc":   err.Error(),
			"status": err.(*tvm.TicketError).Status.String(),
		}
		if serviceTicketStruct != nil {
			response["debug_info"] = serviceTicketStruct.DbgInfo
		}
		panic(response) // return 403
	}
	if _, ok := acl[serviceTicketStruct.SrcID]; !ok {
		response := map[string]string{
			"error": fmt.Sprintf("tvm client id is not allowed: %d", serviceTicketStruct.SrcID),
		}
		panic(response) // return 403
	}

	// proceed...
}

func ExampleNewAPIClient_checkUserTicket() {
	env := tvm.BlackboxTest
	settings := tvmauth.TvmAPISettings{
		SelfID:                      1000501,
		EnableServiceTicketChecking: true,
		BlackboxEnv:                 &env,
		DiskCacheDir:                "/var/tmp/cache/tvm/",
	}

	c, err := tvmauth.NewAPIClient(settings, &nop.Logger{})
	if err != nil {
		panic(err)
	}

	// ...
	serviceTicketFromRequest := "kek"
	userTicketFromRequest := "lol"

	_, _ = c.CheckServiceTicket(context.Background(), serviceTicketFromRequest) // See example for this method

	userTicketStruct, err := c.CheckUserTicket(context.Background(), userTicketFromRequest)
	if err != nil {
		response := map[string]string{
			"error":  "user ticket is invalid",
			"desc":   err.Error(),
			"status": err.(*tvm.TicketError).Status.String(),
		}
		if userTicketStruct != nil {
			response["debug_info"] = userTicketStruct.DbgInfo
		}
		panic(response) // return 403
	}

	fmt.Printf("Got user in request: %d", userTicketStruct.DefaultUID)
	// proceed...
}

func ExampleNewAPIClient_createClientWithAllSettings() {
	blackboxAlias := "blackbox"
	datasyncAlias := "datasync"

	env := tvm.BlackboxTest
	settings := tvmauth.TvmAPISettings{
		SelfID: 1000501,
		ServiceTicketOptions: tvmauth.NewAliasesOptions(
			"bAicxJVa5uVY7MjDlapthw",
			map[string]tvm.ClientID{
				blackboxAlias: 1000502,
				datasyncAlias: 1000503,
			}),
		EnableServiceTicketChecking: true,
		BlackboxEnv:                 &env,
		DiskCacheDir:                "/var/tmp/cache/tvm/",
	}

	_, _ = tvmauth.NewAPIClient(settings, &nop.Logger{})
}

func ExampleNewToolClient_getServiceTicketsWithAliases() {
	// should be configured in tvmtool
	blackboxAlias := "blackbox"

	settings := tvmauth.TvmToolSettings{
		Alias:     "my_service",
		Port:      18000,
		AuthToken: "kek",
	}

	c, err := tvmauth.NewToolClient(settings, &nop.Logger{})
	if err != nil {
		panic(err)
	}

	// ...

	serviceTicket, _ := c.GetServiceTicketForAlias(context.Background(), blackboxAlias)
	fmt.Printf("Service ticket for visiting backend: %s", serviceTicket)
	// please extrapolate other methods for this way of construction
}
