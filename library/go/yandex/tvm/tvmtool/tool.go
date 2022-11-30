package tvmtool

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"a.yandex-team.ru/library/go/core/log"
	"a.yandex-team.ru/library/go/core/log/nop"
	"a.yandex-team.ru/library/go/core/xerrors"
	"a.yandex-team.ru/library/go/yandex/tvm"
	"a.yandex-team.ru/library/go/yandex/tvm/tvmtool/internal/cache"
)

const (
	dialTimeout    = 100 * time.Millisecond
	requestTimeout = 500 * time.Millisecond
	keepAlive      = 60 * time.Second
	cacheTTL       = 10 * time.Minute
	cacheMaxTTL    = 11 * time.Hour
)

var _ tvm.Client = (*Client)(nil)

type (
	Client struct {
		lastSync        int64
		baseURI         string
		src             string
		authToken       string
		bbEnv           string
		refreshFreq     int64
		bgCtx           context.Context
		bgCancel        context.CancelFunc
		inFlightRefresh uint32
		cache           *cache.Cache
		pingRequest     *http.Request
		ownHTTPClient   bool
		httpClient      *http.Client
		l               log.Structured
	}

	ticketsResponse map[string]struct {
		Error  string       `json:"error"`
		Ticket string       `json:"ticket"`
		TvmID  tvm.ClientID `json:"tvm_id"`
	}

	checkSrvResponse struct {
		SrcID   tvm.ClientID `json:"src"`
		Error   string       `json:"error"`
		DbgInfo string       `json:"debug_string"`
		LogInfo string       `json:"logging_string"`
	}

	checkUserResponse struct {
		DefaultUID tvm.UID   `json:"default_uid"`
		UIDs       []tvm.UID `json:"uids"`
		Scopes     []string  `json:"scopes"`
		Error      string    `json:"error"`
		DbgInfo    string    `json:"debug_string"`
		LogInfo    string    `json:"logging_string"`
	}
)

// NewClient method creates a new tvmtool client.
// You must reuse it to prevent connection/goroutines leakage.
func NewClient(apiURI string, opts ...Option) (*Client, error) {
	baseURI := strings.TrimRight(apiURI, "/") + "/tvm"
	pingRequest, err := http.NewRequest("GET", baseURI+"/ping", nil)
	if err != nil {
		return nil, xerrors.Errorf("tvmtool: failed to configure client: %w", err)
	}

	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.DialContext = (&net.Dialer{
		Timeout:   dialTimeout,
		KeepAlive: keepAlive,
	}).DialContext

	tool := &Client{
		baseURI:       baseURI,
		refreshFreq:   8 * 60,
		cache:         cache.New(cacheTTL, cacheMaxTTL),
		pingRequest:   pingRequest,
		l:             &nop.Logger{},
		ownHTTPClient: true,
		httpClient: &http.Client{
			Transport: transport,
			Timeout:   requestTimeout,
			CheckRedirect: func(req *http.Request, via []*http.Request) error {
				return http.ErrUseLastResponse
			},
		},
	}

	for _, opt := range opts {
		if err := opt(tool); err != nil {
			return nil, xerrors.Errorf("tvmtool: failed to configure client: %w", err)
		}
	}

	if tool.bgCtx != nil {
		go tool.serviceTicketsRefreshLoop()
	}

	return tool, nil
}

// GetServiceTicketForAlias returns TVM service ticket for alias
//
// WARNING: alias must be configured in tvmtool
//
// TVMTool documentation: https://wiki.yandex-team.ru/passport/tvm2/tvm-daemon/#/tvm/tickets
func (c *Client) GetServiceTicketForAlias(ctx context.Context, alias string) (string, error) {
	var (
		cachedTicket *string
		cacheStatus  = cache.Miss
	)

	if c.cache != nil {
		c.refreshServiceTickets()

		if cachedTicket, cacheStatus = c.cache.LoadByAlias(alias); cacheStatus == cache.Hit {
			return *cachedTicket, nil
		}
	}

	tickets, err := c.getServiceTickets(ctx, alias)
	if err != nil {
		if cachedTicket != nil && cacheStatus == cache.GonnaMissy {
			return *cachedTicket, nil
		}
		return "", err
	}

	entry, ok := tickets[alias]
	if !ok {
		return "", xerrors.Errorf("tvmtool: alias %q was not found in response", alias)
	}

	if entry.Error != "" {
		return "", &Error{Code: ErrorOther, Msg: entry.Error}
	}

	ticket := entry.Ticket
	if c.cache != nil {
		c.cache.Store(entry.TvmID, alias, &ticket)
	}
	return ticket, nil
}

// GetServiceTicketForID returns TVM service ticket for destination application id
//
// WARNING: id must be configured in tvmtool
//
// TVMTool documentation: https://wiki.yandex-team.ru/passport/tvm2/tvm-daemon/#/tvm/tickets
func (c *Client) GetServiceTicketForID(ctx context.Context, dstID tvm.ClientID) (string, error) {
	var (
		cachedTicket *string
		cacheStatus  = cache.Miss
	)

	if c.cache != nil {
		c.refreshServiceTickets()

		if cachedTicket, cacheStatus = c.cache.Load(dstID); cacheStatus == cache.Hit {
			return *cachedTicket, nil
		}
	}

	alias := strconv.FormatUint(uint64(dstID), 10)
	tickets, err := c.getServiceTickets(ctx, alias)
	if err != nil {
		if cachedTicket != nil && cacheStatus == cache.GonnaMissy {
			return *cachedTicket, nil
		}
		return "", err
	}

	entry, ok := tickets[alias]
	if !ok {
		// ok, let's find him
		for candidateAlias, candidate := range tickets {
			if candidate.TvmID == dstID {
				entry = candidate
				alias = candidateAlias
				ok = true
				break
			}
		}

		if !ok {
			return "", xerrors.Errorf("tvmtool: dst %q was not found in response", alias)
		}
	}

	if entry.Error != "" {
		return "", &Error{Code: ErrorOther, Msg: entry.Error}
	}

	ticket := entry.Ticket
	if c.cache != nil {
		c.cache.Store(dstID, alias, &ticket)
	}
	return ticket, nil
}

// Close stops background ticket updates (if configured) and closes idle connections.
func (c *Client) Close() {
	if c.bgCancel != nil {
		c.bgCancel()
	}

	if c.ownHTTPClient {
		c.httpClient.CloseIdleConnections()
	}
}

func (c *Client) refreshServiceTickets() {
	if c.bgCtx != nil {
		// service tickets will be updated at background in the separated goroutine
		return
	}

	now := time.Now().Unix()
	if now-atomic.LoadInt64(&c.lastSync) > c.refreshFreq {
		atomic.StoreInt64(&c.lastSync, now)
		if atomic.CompareAndSwapUint32(&c.inFlightRefresh, 0, 1) {
			go c.doServiceTicketsRefresh(context.Background())
		}
	}
}

func (c *Client) serviceTicketsRefreshLoop() {
	var ticker = time.NewTicker(time.Duration(c.refreshFreq) * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-c.bgCtx.Done():
			return
		case <-ticker.C:
			c.doServiceTicketsRefresh(c.bgCtx)
		}
	}
}

func (c *Client) doServiceTicketsRefresh(ctx context.Context) {
	defer atomic.CompareAndSwapUint32(&c.inFlightRefresh, 1, 0)

	c.cache.Gc()
	aliases := c.cache.Aliases()
	if len(aliases) == 0 {
		return
	}

	c.l.Debug("tvmtool: service ticket update started")
	defer c.l.Debug("tvmtool: service ticket update finished")

	// fast path: batch update, must work most of time
	err := c.refreshServiceTicket(ctx, aliases...)
	if err == nil {
		return
	}

	if tvmErr, ok := err.(*Error); ok && tvmErr.Code != ErrorBadRequest {
		c.l.Error(
			"tvmtool: failed to refresh all service tickets at background",
			log.Strings("dsts", aliases),
			log.Error(err),
		)

		// if we have non "bad request" error - something really terrible happens, nothing to do with it :(
		// TODO(buglloc): implement adaptive refreshFreq based on errors?
		return
	}

	// slow path: trying to update service tickets one by one
	c.l.Error(
		"tvmtool: failed to refresh all service tickets at background, switched to slow path",
		log.Strings("dsts", aliases),
		log.Error(err),
	)

	for _, dst := range aliases {
		if err := c.refreshServiceTicket(ctx, dst); err != nil {
			c.l.Error(
				"tvmtool: failed to refresh service ticket at background",
				log.String("dst", dst),
				log.Error(err),
			)
		}
	}
}

func (c *Client) refreshServiceTicket(ctx context.Context, dsts ...string) error {
	tickets, err := c.getServiceTickets(ctx, strings.Join(dsts, ","))
	if err != nil {
		return err
	}

	for _, dst := range dsts {
		entry, ok := tickets[dst]
		if !ok {
			c.l.Error(
				"tvmtool: destination was not found in tvmtool response",
				log.String("dst", dst),
			)
			continue
		}

		if entry.Error != "" {
			c.l.Error(
				"tvmtool: failed to get service ticket for destination",
				log.String("dst", dst),
				log.String("err", entry.Error),
			)
			continue
		}

		c.cache.Store(entry.TvmID, dst, &entry.Ticket)
	}
	return nil
}

func (c *Client) getServiceTickets(ctx context.Context, dst string) (ticketsResponse, error) {
	params := url.Values{
		"dsts": {dst},
	}
	if c.src != "" {
		params.Set("src", c.src)
	}

	req, err := http.NewRequest("GET", c.baseURI+"/tickets?"+params.Encode(), nil)
	if err != nil {
		return nil, xerrors.Errorf("tvmtool: failed to call tvmtool: %w", err)
	}
	req.Header.Set("Authorization", c.authToken)

	req = req.WithContext(ctx)
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, xerrors.Errorf("tvmtool: failed to call tvmtool: %w", err)
	}

	var result ticketsResponse
	err = readResponse(resp, &result)
	return result, err
}

// Check TVM service ticket
//
// TVMTool documentation: https://wiki.yandex-team.ru/passport/tvm2/tvm-daemon/#/tvm/checksrv
func (c *Client) CheckServiceTicket(ctx context.Context, ticket string) (*tvm.CheckedServiceTicket, error) {
	req, err := http.NewRequest("GET", c.baseURI+"/checksrv", nil)
	if err != nil {
		return nil, xerrors.Errorf("tvmtool: failed to call tvmtool: %w", err)
	}

	if c.src != "" {
		req.URL.RawQuery += "dst=" + url.QueryEscape(c.src)
	}
	req.Header.Set("Authorization", c.authToken)
	req.Header.Set("X-Ya-Service-Ticket", ticket)

	req = req.WithContext(ctx)
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, xerrors.Errorf("tvmtool: failed to call tvmtool: %w", err)
	}

	var result checkSrvResponse
	if err = readResponse(resp, &result); err != nil {
		return nil, err
	}

	ticketInfo := &tvm.CheckedServiceTicket{
		SrcID:   result.SrcID,
		DbgInfo: result.DbgInfo,
		LogInfo: result.LogInfo,
	}

	if resp.StatusCode == http.StatusForbidden {
		err = &TicketError{Status: TicketErrorOther, Msg: result.Error}
	}

	return ticketInfo, err
}

// Check TVM user ticket
//
// TVMTool documentation: https://wiki.yandex-team.ru/passport/tvm2/tvm-daemon/#/tvm/checkusr
func (c *Client) CheckUserTicket(ctx context.Context, ticket string, opts ...tvm.CheckUserTicketOption) (*tvm.CheckedUserTicket, error) {
	for range opts {
		panic("implement me")
	}

	req, err := http.NewRequest("GET", c.baseURI+"/checkusr", nil)
	if err != nil {
		return nil, xerrors.Errorf("tvmtool: failed to call tvmtool: %w", err)
	}
	if c.bbEnv != "" {
		req.URL.RawQuery += "override_env=" + url.QueryEscape(c.bbEnv)
	}
	req.Header.Set("Authorization", c.authToken)
	req.Header.Set("X-Ya-User-Ticket", ticket)

	req = req.WithContext(ctx)
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, xerrors.Errorf("tvmtool: failed to call tvmtool: %w", err)
	}

	var result checkUserResponse
	if err = readResponse(resp, &result); err != nil {
		return nil, err
	}

	ticketInfo := &tvm.CheckedUserTicket{
		DefaultUID: result.DefaultUID,
		UIDs:       result.UIDs,
		Scopes:     result.Scopes,
		DbgInfo:    result.DbgInfo,
		LogInfo:    result.LogInfo,
	}

	if resp.StatusCode == http.StatusForbidden {
		err = &TicketError{Status: TicketErrorOther, Msg: result.Error}
	}

	return ticketInfo, err
}

// Checks TVMTool liveness
//
// TVMTool documentation: https://wiki.yandex-team.ru/passport/tvm2/tvm-daemon/#/tvm/ping
func (c *Client) GetStatus(ctx context.Context) (tvm.ClientStatusInfo, error) {
	req := c.pingRequest.WithContext(ctx)
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return tvm.ClientStatusInfo{Status: tvm.ClientError},
			&PingError{Code: PingCodeDie, Err: err}
	}
	defer func() { _ = resp.Body.Close() }()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return tvm.ClientStatusInfo{Status: tvm.ClientError},
			&PingError{Code: PingCodeDie, Err: err}
	}

	var status tvm.ClientStatusInfo
	switch resp.StatusCode {
	case http.StatusOK:
		// OK!
		status = tvm.ClientStatusInfo{Status: tvm.ClientOK}
		err = nil
	case http.StatusPartialContent:
		status = tvm.ClientStatusInfo{Status: tvm.ClientWarning}
		err = &PingError{Code: PingCodeWarning, Err: xerrors.New(string(body))}
	case http.StatusInternalServerError:
		status = tvm.ClientStatusInfo{Status: tvm.ClientError}
		err = &PingError{Code: PingCodeError, Err: xerrors.New(string(body))}
	default:
		status = tvm.ClientStatusInfo{Status: tvm.ClientError}
		err = &PingError{Code: PingCodeOther, Err: xerrors.Errorf("tvmtool: unexpected status: %d", resp.StatusCode)}
	}
	return status, err
}

// Returns TVMTool version
func (c *Client) Version(ctx context.Context) (string, error) {
	req := c.pingRequest.WithContext(ctx)
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return "", xerrors.Errorf("tvmtool: failed to call tmvtool: %w", err)
	}
	_, _ = ioutil.ReadAll(resp.Body)
	_ = resp.Body.Close()

	return resp.Header.Get("Server"), nil
}

func (c *Client) GetRoles(ctx context.Context) (*tvm.Roles, error) {
	return nil, errors.New("not implemented")
}

func readResponse(resp *http.Response, dst interface{}) error {
	body, err := ioutil.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if err != nil {
		return xerrors.Errorf("tvmtool: failed to read response: %w", err)
	}

	switch resp.StatusCode {
	case http.StatusOK, http.StatusForbidden:
		// ok
		return json.Unmarshal(body, dst)
	case http.StatusUnauthorized:
		return &Error{
			Code: ErrorAuthFail,
			Msg:  string(body),
		}
	case http.StatusBadRequest:
		return &Error{
			Code: ErrorBadRequest,
			Msg:  string(body),
		}
	case http.StatusInternalServerError:
		return &Error{
			Code:      ErrorOther,
			Msg:       string(body),
			Retriable: true,
		}
	default:
		return &Error{
			Code: ErrorOther,
			Msg:  fmt.Sprintf("tvmtool: unexpected status: %d, msg: %s", resp.StatusCode, string(body)),
		}
	}
}
