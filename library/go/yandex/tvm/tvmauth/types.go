package tvmauth

import (
	"sync"
	"unsafe"

	"a.yandex-team.ru/library/go/yandex/tvm"
)

// TvmAPISettings may be used to fetch data from tvm-api
type TvmAPISettings struct {
	// SelfID is required for ServiceTicketOptions and EnableServiceTicketChecking
	SelfID tvm.ClientID

	// ServiceTicketOptions provides info for fetching Service Tickets from tvm-api
	//  to allow you send them to your backends.
	//
	// WARNING: It is not way to provide authorization for incoming ServiceTickets!
	//          It is way only to send your ServiceTickets to your backend!
	ServiceTicketOptions *TVMAPIOptions

	// EnableServiceTicketChecking enables fetching of public keys for signature checking
	EnableServiceTicketChecking bool

	// BlackboxEnv with not nil value enables UserTicket checking
	// and enables fetching of public keys for signature checking
	BlackboxEnv *tvm.BlackboxEnv

	fetchRolesForIdmSystemSlug []byte
	// Non-empty FetchRolesForIdmSystemSlug enables roles fetching from tirole
	FetchRolesForIdmSystemSlug string
	// By default, client checks src from ServiceTicket or default uid from UserTicket -
	//   to prevent you from forgetting to check it yourself.
	// It does binary checks only:
	//   ticket gets status NoRoles, if there is no role for src or default uid.
	// You need to check roles on your own if you have a non-binary role system or
	// 	you have switched DisableSrcCheck/DisableDefaultUIDCheck
	//
	// You may need to disable this check in the following cases:
	//   - You use GetRoles() to provide verbose message (with revision).
	// 	Double check may be inconsistent:
	// 	  binary check inside client uses revision of roles X - i.e. src 100500 has no role,
	// 	  exact check in your code uses revision of roles Y -  i.e. src 100500 has some roles.
	DisableSrcCheck bool
	// See comment for DisableSrcCheck
	DisableDefaultUIDCheck bool

	tvmHost []byte
	// TVMHost should be used only in tests
	TVMHost string
	// TVMPort should be used only in tests
	TVMPort int

	tiroleHost []byte
	// TiroleHost should be used only in tests or for tirole-api-test.yandex.net
	TiroleHost string
	// TirolePort should be used only in tests
	TirolePort int
	// TiroleTvmID should be used only in tests or for tirole-api-test.yandex.net
	TiroleTvmID tvm.ClientID

	// Directory for disk cache.
	// Requires read/write permissions. Permissions will be checked before start.
	// WARNING: The same directory can be used only:
	//     - for TVM clients with the same settings
	//   OR
	//     - for new client replacing previous - with another config.
	// System user must be the same for processes with these clients inside.
	// Implementation doesn't provide other scenarios.
	DiskCacheDir string
	diskCacheDir []byte
}

// TVMAPIOptions is part of TvmAPISettings: allows to enable fetching of ServiceTickets
type TVMAPIOptions struct {
	selfSecret  string
	selfSecretB []byte
	dstAliases  []byte
}

// TvmToolSettings may be used to fetch data from tvmtool
type TvmToolSettings struct {
	// Alias is required: self alias of your tvm ClientID
	Alias string
	alias []byte

	// By default, client checks src from ServiceTicket or default uid from UserTicket -
	//   to prevent you from forgetting to check it yourself.
	// It does binary checks only:
	//   ticket gets status NoRoles, if there is no role for src or default uid.
	// You need to check roles on your own if you have a non-binary role system or
	// 	you have switched DisableSrcCheck/DisableDefaultUIDCheck
	//
	// You may need to disable this check in the following cases:
	//   - You use GetRoles() to provide verbose message (with revision).
	// 	Double check may be inconsistent:
	// 	  binary check inside client uses revision of roles X - i.e. src 100500 has no role,
	// 	  exact check in your code uses revision of roles Y -  i.e. src 100500 has some roles.
	DisableSrcCheck bool
	// See comment for DisableSrcCheck
	DisableDefaultUIDCheck bool

	// Port will be detected with env["DEPLOY_TVM_TOOL_URL"] (provided with Yandex.Deploy),
	//    otherwise port == 1 (it is ok for Qloud)
	Port int
	// Hostname == "localhost" by default
	Hostname string
	hostname []byte

	// AuthToken is protection from SSRF.
	// By default it is fetched from env:
	//  * TVMTOOL_LOCAL_AUTHTOKEN (provided with Yandex.Deploy)
	//  * QLOUD_TVM_TOKEN (provided with Qloud)
	AuthToken string
	authToken []byte
}

type TvmUnittestSettings struct {
	// SelfID is required for service ticket checking
	SelfID tvm.ClientID

	// Service ticket checking is enabled by default

	// User ticket checking is enabled by default: choose required environment
	BlackboxEnv tvm.BlackboxEnv

	// Other features are not supported yet
}

// Client contains raw pointer for C++ object
type Client struct {
	handle unsafe.Pointer
	logger *int

	roles *tvm.Roles
	mutex *sync.RWMutex
}

var _ tvm.Client = (*Client)(nil)
