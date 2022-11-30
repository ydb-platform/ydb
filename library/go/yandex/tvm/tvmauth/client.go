//go:build cgo
// +build cgo

package tvmauth

// #include <stdlib.h>
//
// #include "tvm.h"
import "C"
import (
	"context"
	"encoding/json"
	"fmt"
	"runtime"
	"sync"
	"unsafe"

	"a.yandex-team.ru/library/go/cgosem"
	"a.yandex-team.ru/library/go/core/log"
	"a.yandex-team.ru/library/go/yandex/tvm"
)

// NewIDsOptions creates options for fetching CheckedServiceTicket's with ClientID
func NewIDsOptions(secret string, dsts []tvm.ClientID) *TVMAPIOptions {
	tmp := make(map[string]tvm.ClientID)
	for _, dst := range dsts {
		tmp[fmt.Sprintf("%d", dst)] = dst
	}

	res, err := json.Marshal(tmp)
	if err != nil {
		panic(err)
	}

	return &TVMAPIOptions{
		selfSecret: secret,
		dstAliases: res,
	}
}

// NewAliasesOptions creates options for fetching CheckedServiceTicket's with alias+ClientID
func NewAliasesOptions(secret string, dsts map[string]tvm.ClientID) *TVMAPIOptions {
	if dsts == nil {
		dsts = make(map[string]tvm.ClientID)
	}

	res, err := json.Marshal(dsts)
	if err != nil {
		panic(err)
	}

	return &TVMAPIOptions{
		selfSecret: secret,
		dstAliases: res,
	}
}

func (o *TvmAPISettings) pack(out *C.TVM_ApiSettings) {
	out.SelfId = C.uint32_t(o.SelfID)

	if o.EnableServiceTicketChecking {
		out.EnableServiceTicketChecking = 1
	}

	if o.BlackboxEnv != nil {
		out.EnableUserTicketChecking = 1
		out.BlackboxEnv = C.int(*o.BlackboxEnv)
	}

	if o.FetchRolesForIdmSystemSlug != "" {
		o.fetchRolesForIdmSystemSlug = []byte(o.FetchRolesForIdmSystemSlug)
		out.IdmSystemSlug = (*C.uchar)(&o.fetchRolesForIdmSystemSlug[0])
		out.IdmSystemSlugSize = C.int(len(o.fetchRolesForIdmSystemSlug))
	}
	if o.DisableSrcCheck {
		out.DisableSrcCheck = 1
	}
	if o.DisableDefaultUIDCheck {
		out.DisableDefaultUIDCheck = 1
	}

	if o.TVMHost != "" {
		o.tvmHost = []byte(o.TVMHost)
		out.TVMHost = (*C.uchar)(&o.tvmHost[0])
		out.TVMHostSize = C.int(len(o.tvmHost))
	}
	out.TVMPort = C.int(o.TVMPort)

	if o.TiroleHost != "" {
		o.tiroleHost = []byte(o.TiroleHost)
		out.TiroleHost = (*C.uchar)(&o.tiroleHost[0])
		out.TiroleHostSize = C.int(len(o.tiroleHost))
	}
	out.TirolePort = C.int(o.TirolePort)
	out.TiroleTvmId = C.uint32_t(o.TiroleTvmID)

	if o.ServiceTicketOptions != nil {
		if (o.ServiceTicketOptions.selfSecret != "") {
			o.ServiceTicketOptions.selfSecretB = []byte(o.ServiceTicketOptions.selfSecret)
			out.SelfSecret = (*C.uchar)(&o.ServiceTicketOptions.selfSecretB[0])
			out.SelfSecretSize = C.int(len(o.ServiceTicketOptions.selfSecretB))
		}

		if (len(o.ServiceTicketOptions.dstAliases) != 0) {
			out.DstAliases = (*C.uchar)(&o.ServiceTicketOptions.dstAliases[0])
			out.DstAliasesSize = C.int(len(o.ServiceTicketOptions.dstAliases))
		}
	}

	if o.DiskCacheDir != "" {
		o.diskCacheDir = []byte(o.DiskCacheDir)

		out.DiskCacheDir = (*C.uchar)(&o.diskCacheDir[0])
		out.DiskCacheDirSize = C.int(len(o.diskCacheDir))
	}
}

func (o *TvmToolSettings) pack(out *C.TVM_ToolSettings) {
	if o.Alias != "" {
		o.alias = []byte(o.Alias)

		out.Alias = (*C.uchar)(&o.alias[0])
		out.AliasSize = C.int(len(o.alias))
	}

	out.Port = C.int(o.Port)

	if o.Hostname != "" {
		o.hostname = []byte(o.Hostname)
		out.Hostname = (*C.uchar)(&o.hostname[0])
		out.HostnameSize = C.int(len(o.hostname))
	}

	if o.AuthToken != "" {
		o.authToken = []byte(o.AuthToken)
		out.AuthToken = (*C.uchar)(&o.authToken[0])
		out.AuthTokenSize = C.int(len(o.authToken))
	}

	if o.DisableSrcCheck {
		out.DisableSrcCheck = 1
	}
	if o.DisableDefaultUIDCheck {
		out.DisableDefaultUIDCheck = 1
	}
}

func (o *TvmUnittestSettings) pack(out *C.TVM_UnittestSettings) {
	out.SelfId = C.uint32_t(o.SelfID)
	out.BlackboxEnv = C.int(o.BlackboxEnv)
}

// Destroy stops client and delete it from memory.
// Do not try to use client after destroying it
func (c *Client) Destroy() {
	if c.handle == nil {
		return
	}

	C.TVM_DestroyClient(c.handle)
	c.handle = nil

	if c.logger != nil {
		unregisterLogger(*c.logger)
	}
}

func unpackString(s *C.TVM_String) string {
	if s.Data == nil {
		return ""
	}

	return C.GoStringN(s.Data, s.Size)
}

func unpackErr(err *C.TVM_Error) error {
	msg := unpackString(&err.Message)
	code := tvm.ErrorCode(err.Code)

	if code != 0 {
		return &tvm.Error{Code: code, Retriable: err.Retriable != 0, Msg: msg}
	}

	return nil
}

func unpackScopes(scopes *C.TVM_String, scopeSize C.int) (s []string) {
	if scopeSize == 0 {
		return
	}

	s = make([]string, int(scopeSize))
	scopesArr := (*[1 << 30]C.TVM_String)(unsafe.Pointer(scopes))

	for i := 0; i < int(scopeSize); i++ {
		s[i] = C.GoStringN(scopesArr[i].Data, scopesArr[i].Size)
	}

	return
}

func unpackStatus(status C.int) error {
	if status == 0 {
		return nil
	}

	return &tvm.TicketError{Status: tvm.TicketStatus(status)}
}

func unpackServiceTicket(t *C.TVM_ServiceTicket) (*tvm.CheckedServiceTicket, error) {
	ticket := &tvm.CheckedServiceTicket{}
	ticket.SrcID = tvm.ClientID(t.SrcId)
	ticket.IssuerUID = tvm.UID(t.IssuerUid)
	ticket.DbgInfo = unpackString(&t.DbgInfo)
	ticket.LogInfo = unpackString(&t.LogInfo)
	return ticket, unpackStatus(t.Status)
}

func unpackUserTicket(t *C.TVM_UserTicket) (*tvm.CheckedUserTicket, error) {
	ticket := &tvm.CheckedUserTicket{}
	ticket.DefaultUID = tvm.UID(t.DefaultUid)
	if t.UidsSize != 0 {
		ticket.UIDs = make([]tvm.UID, int(t.UidsSize))
		uids := (*[1 << 30]C.uint64_t)(unsafe.Pointer(t.Uids))
		for i := 0; i < int(t.UidsSize); i++ {
			ticket.UIDs[i] = tvm.UID(uids[i])
		}
	}

	ticket.Env = tvm.BlackboxEnv(t.Env)

	ticket.Scopes = unpackScopes(t.Scopes, t.ScopesSize)
	ticket.DbgInfo = unpackString(&t.DbgInfo)
	ticket.LogInfo = unpackString(&t.LogInfo)
	return ticket, unpackStatus(t.Status)
}

func unpackClientStatus(s *C.TVM_ClientStatus) (status tvm.ClientStatusInfo) {
	status.Status = tvm.ClientStatus(s.Status)
	status.LastError = C.GoStringN(s.LastError.Data, s.LastError.Size)

	return
}

// NewAPIClient creates client which uses https://tvm-api.yandex.net to get state
func NewAPIClient(options TvmAPISettings, log log.Logger) (*Client, error) {
	var settings C.TVM_ApiSettings
	options.pack(&settings)

	client := &Client{
		mutex: &sync.RWMutex{},
	}

	var pool C.TVM_MemPool
	defer C.TVM_DestroyMemPool(&pool)

	loggerId := registerLogger(log)
	client.logger = &loggerId

	var tvmErr C.TVM_Error
	C.TVM_NewApiClient(settings, C.int(loggerId), &client.handle, &tvmErr, &pool)

	if err := unpackErr(&tvmErr); err != nil {
		unregisterLogger(loggerId)
		return nil, err
	}

	runtime.SetFinalizer(client, (*Client).Destroy)
	return client, nil
}

// NewToolClient creates client uses local http-interface to get state: http://localhost/tvm/.
// Details: https://wiki.yandex-team.ru/passport/tvm2/tvm-daemon/.
func NewToolClient(options TvmToolSettings, log log.Logger) (*Client, error) {
	var settings C.TVM_ToolSettings
	options.pack(&settings)

	client := &Client{
		mutex: &sync.RWMutex{},
	}

	var pool C.TVM_MemPool
	defer C.TVM_DestroyMemPool(&pool)

	loggerId := registerLogger(log)
	client.logger = &loggerId

	var tvmErr C.TVM_Error
	C.TVM_NewToolClient(settings, C.int(loggerId), &client.handle, &tvmErr, &pool)

	if err := unpackErr(&tvmErr); err != nil {
		unregisterLogger(loggerId)
		return nil, err
	}

	runtime.SetFinalizer(client, (*Client).Destroy)
	return client, nil
}

// NewUnittestClient creates client with mocked state.
func NewUnittestClient(options TvmUnittestSettings) (*Client, error) {
	var settings C.TVM_UnittestSettings
	options.pack(&settings)

	client := &Client{
		mutex: &sync.RWMutex{},
	}

	var pool C.TVM_MemPool
	defer C.TVM_DestroyMemPool(&pool)

	var tvmErr C.TVM_Error
	C.TVM_NewUnittestClient(settings, &client.handle, &tvmErr, &pool)

	if err := unpackErr(&tvmErr); err != nil {
		return nil, err
	}

	runtime.SetFinalizer(client, (*Client).Destroy)
	return client, nil
}

// CheckServiceTicket always checks ticket with keys from memory
func (c *Client) CheckServiceTicket(ctx context.Context, ticketStr string) (*tvm.CheckedServiceTicket, error) {
	defer cgosem.S.Acquire().Release()

	ticketBytes := []byte(ticketStr)

	var pool C.TVM_MemPool
	defer C.TVM_DestroyMemPool(&pool)

	var ticket C.TVM_ServiceTicket
	var tvmErr C.TVM_Error
	C.TVM_CheckServiceTicket(
		c.handle,
		(*C.uchar)(&ticketBytes[0]), C.int(len(ticketBytes)),
		&ticket,
		&tvmErr,
		&pool)
	runtime.KeepAlive(c)

	if err := unpackErr(&tvmErr); err != nil {
		return nil, err
	}

	return unpackServiceTicket(&ticket)
}

// CheckUserTicket always checks ticket with keys from memory
func (c *Client) CheckUserTicket(ctx context.Context, ticketStr string, opts ...tvm.CheckUserTicketOption) (*tvm.CheckedUserTicket, error) {
	defer cgosem.S.Acquire().Release()

	var options tvm.CheckUserTicketOptions
	for _, opt := range opts {
		opt(&options)
	}

	ticketBytes := []byte(ticketStr)

	var pool C.TVM_MemPool
	defer C.TVM_DestroyMemPool(&pool)

	var bbEnv *C.int
	var bbEnvOverrided C.int
	if options.EnvOverride != nil {
		bbEnvOverrided = C.int(*options.EnvOverride)
		bbEnv = &bbEnvOverrided
	}

	var ticket C.TVM_UserTicket
	var tvmErr C.TVM_Error
	C.TVM_CheckUserTicket(
		c.handle,
		(*C.uchar)(&ticketBytes[0]), C.int(len(ticketBytes)),
		bbEnv,
		&ticket,
		&tvmErr,
		&pool)
	runtime.KeepAlive(c)

	if err := unpackErr(&tvmErr); err != nil {
		return nil, err
	}

	return unpackUserTicket(&ticket)
}

// GetServiceTicketForAlias always returns ticket from memory
func (c *Client) GetServiceTicketForAlias(ctx context.Context, alias string) (string, error) {
	defer cgosem.S.Acquire().Release()

	aliasBytes := []byte(alias)

	var pool C.TVM_MemPool
	defer C.TVM_DestroyMemPool(&pool)

	var ticket *C.char
	var tvmErr C.TVM_Error
	C.TVM_GetServiceTicketForAlias(
		c.handle,
		(*C.uchar)(&aliasBytes[0]), C.int(len(aliasBytes)),
		&ticket,
		&tvmErr,
		&pool)
	runtime.KeepAlive(c)

	if err := unpackErr(&tvmErr); err != nil {
		return "", err
	}

	return C.GoString(ticket), nil
}

// GetServiceTicketForID always returns ticket from memory
func (c *Client) GetServiceTicketForID(ctx context.Context, dstID tvm.ClientID) (string, error) {
	defer cgosem.S.Acquire().Release()

	var pool C.TVM_MemPool
	defer C.TVM_DestroyMemPool(&pool)

	var ticket *C.char
	var tvmErr C.TVM_Error
	C.TVM_GetServiceTicket(
		c.handle,
		C.uint32_t(dstID),
		&ticket,
		&tvmErr,
		&pool)
	runtime.KeepAlive(c)

	if err := unpackErr(&tvmErr); err != nil {
		return "", err
	}

	return C.GoString(ticket), nil
}

// GetStatus returns current status of client.
// See detials: https://godoc.yandex-team.ru/pkg/a.yandex-team.ru/library/go/yandex/tvm/#Client
func (c *Client) GetStatus(ctx context.Context) (tvm.ClientStatusInfo, error) {
	var pool C.TVM_MemPool
	defer C.TVM_DestroyMemPool(&pool)

	var status C.TVM_ClientStatus
	var tvmErr C.TVM_Error
	C.TVM_GetStatus(c.handle, &status, &tvmErr, &pool)
	runtime.KeepAlive(c)

	if err := unpackErr(&tvmErr); err != nil {
		return tvm.ClientStatusInfo{}, err
	}

	return unpackClientStatus(&status), nil
}

func (c *Client) GetRoles(ctx context.Context) (*tvm.Roles, error) {
	defer cgosem.S.Acquire().Release()

	var pool C.TVM_MemPool
	defer C.TVM_DestroyMemPool(&pool)

	currentRoles := c.getCurrentRoles()
	var currentRevision []byte
	var currentRevisionPtr *C.uchar
	if currentRoles != nil {
		currentRevision = []byte(currentRoles.GetMeta().Revision)
		currentRevisionPtr = (*C.uchar)(&currentRevision[0])
	}

	var raw *C.char
	var rawSize C.int
	var tvmErr C.TVM_Error
	C.TVM_GetRoles(
		c.handle,
		currentRevisionPtr, C.int(len(currentRevision)),
		&raw,
		&rawSize,
		&tvmErr,
		&pool)
	runtime.KeepAlive(c)

	if err := unpackErr(&tvmErr); err != nil {
		return nil, err
	}
	if raw == nil {
		return currentRoles, nil
	}

	c.mutex.Lock()
	defer c.mutex.Unlock()

	if currentRoles != c.roles {
		return c.roles, nil
	}

	roles, err := tvm.NewRoles(C.GoBytes(unsafe.Pointer(raw), rawSize))
	if err != nil {
		return nil, err
	}

	c.roles = roles
	return c.roles, nil
}

func (c *Client) getCurrentRoles() *tvm.Roles {
	c.mutex.RLock()
	defer c.mutex.RUnlock()
	return c.roles
}
