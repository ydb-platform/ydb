package tvm_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"a.yandex-team.ru/library/go/yandex/tvm"
)

func TestUserTicketCheckScopes(t *testing.T) {
	cases := map[string]struct {
		ticketScopes   []string
		requiredScopes []string
		err            bool
	}{
		"wo_required_scopes": {
			ticketScopes:   []string{"bb:sessionid"},
			requiredScopes: nil,
			err:            false,
		},
		"multiple_scopes_0": {
			ticketScopes:   []string{"bb:sessionid", "test:test"},
			requiredScopes: []string{"bb:sessionid", "test:test"},
			err:            false,
		},
		"multiple_scopes_1": {
			ticketScopes:   []string{"bb:sessionid", "test:test"},
			requiredScopes: []string{"test:test", "bb:sessionid"},
			err:            false,
		},
		"wo_scopes": {
			ticketScopes:   nil,
			requiredScopes: []string{"bb:sessionid"},
			err:            true,
		},
		"invalid_scopes": {
			ticketScopes:   []string{"bb:sessionid"},
			requiredScopes: []string{"test:test"},
			err:            true,
		},
		"not_all_scopes": {
			ticketScopes:   []string{"bb:sessionid", "test:test1"},
			requiredScopes: []string{"bb:sessionid", "test:test"},
			err:            true,
		},
	}

	for name, testCase := range cases {
		t.Run(name, func(t *testing.T) {
			ticket := tvm.CheckedUserTicket{
				Scopes: testCase.ticketScopes,
			}
			err := ticket.CheckScopes(testCase.requiredScopes...)
			if testCase.err {
				require.Error(t, err)
				require.IsType(t, &tvm.TicketError{}, err)
				ticketErr := err.(*tvm.TicketError)
				require.Equal(t, tvm.TicketInvalidScopes, ticketErr.Status)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestUserTicketCheckScopesAny(t *testing.T) {
	cases := map[string]struct {
		ticketScopes   []string
		requiredScopes []string
		err            bool
	}{
		"wo_required_scopes": {
			ticketScopes:   []string{"bb:sessionid"},
			requiredScopes: nil,
			err:            false,
		},
		"multiple_scopes_0": {
			ticketScopes:   []string{"bb:sessionid", "test:test"},
			requiredScopes: []string{"bb:sessionid"},
			err:            false,
		},
		"multiple_scopes_1": {
			ticketScopes:   []string{"bb:sessionid", "test:test"},
			requiredScopes: []string{"test:test"},
			err:            false,
		},
		"multiple_scopes_2": {
			ticketScopes:   []string{"bb:sessionid", "test:test"},
			requiredScopes: []string{"bb:sessionid", "test:test"},
			err:            false,
		},
		"multiple_scopes_3": {
			ticketScopes:   []string{"bb:sessionid", "test:test"},
			requiredScopes: []string{"test:test", "bb:sessionid"},
			err:            false,
		},
		"wo_scopes": {
			ticketScopes:   nil,
			requiredScopes: []string{"bb:sessionid"},
			err:            true,
		},
		"invalid_scopes": {
			ticketScopes:   []string{"bb:sessionid"},
			requiredScopes: []string{"test:test"},
			err:            true,
		},
	}

	for name, testCase := range cases {
		t.Run(name, func(t *testing.T) {
			ticket := tvm.CheckedUserTicket{
				Scopes: testCase.ticketScopes,
			}
			err := ticket.CheckScopes(testCase.requiredScopes...)
			if testCase.err {
				require.Error(t, err)
				require.IsType(t, &tvm.TicketError{}, err)
				ticketErr := err.(*tvm.TicketError)
				require.Equal(t, tvm.TicketInvalidScopes, ticketErr.Status)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestServiceTicketAllowedSrcIDs(t *testing.T) {
	cases := map[string]struct {
		srcID         uint32
		allowedSrcIDs []uint32
		err           bool
	}{
		"empty_allow_list_allows_any_srcID": {srcID: 162, allowedSrcIDs: []uint32{}, err: false},
		"known_src_id_is_allowed":           {srcID: 42, allowedSrcIDs: []uint32{42, 100500}, err: false},
		"unknown_src_id_is_not_allowed":     {srcID: 404, allowedSrcIDs: []uint32{42, 100500}, err: true},
	}

	for name, testCase := range cases {
		t.Run(name, func(t *testing.T) {
			ticket := tvm.CheckedServiceTicket{
				SrcID: tvm.ClientID(testCase.srcID),
			}
			allowedSrcIDsMap := make(map[uint32]struct{}, len(testCase.allowedSrcIDs))
			for _, allowedSrcID := range testCase.allowedSrcIDs {
				allowedSrcIDsMap[allowedSrcID] = struct{}{}
			}
			err := ticket.CheckSrcID(allowedSrcIDsMap)
			if testCase.err {
				require.Error(t, err)
				require.IsType(t, &tvm.TicketError{}, err)
				ticketErr := err.(*tvm.TicketError)
				require.Equal(t, tvm.TicketInvalidSrcID, ticketErr.Status)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestTicketError_Is(t *testing.T) {
	err1 := &tvm.TicketError{
		Status: tvm.TicketInvalidSrcID,
		Msg:    "uh oh",
	}
	err2 := &tvm.TicketError{
		Status: tvm.TicketInvalidSrcID,
		Msg:    "uh oh",
	}
	err3 := &tvm.TicketError{
		Status: tvm.TicketInvalidSrcID,
		Msg:    "other uh oh message",
	}
	err4 := &tvm.TicketError{
		Status: tvm.TicketExpired,
		Msg:    "uh oh",
	}
	err5 := &tvm.TicketError{
		Status: tvm.TicketMalformed,
		Msg:    "i am completely different",
	}
	var nilErr *tvm.TicketError = nil

	// ticketErrors are equal to themselves
	require.True(t, err1.Is(err1))
	require.True(t, err2.Is(err2))
	require.True(t, nilErr.Is(nilErr))

	// equal value ticketErrors are equal
	require.True(t, err1.Is(err2))
	require.True(t, err2.Is(err1))
	// equal status ticketErrors are equal
	require.True(t, err1.Is(err3))
	require.True(t, err1.Is(tvm.ErrTicketInvalidSrcID))
	require.True(t, err2.Is(tvm.ErrTicketInvalidSrcID))
	require.True(t, err3.Is(tvm.ErrTicketInvalidSrcID))
	require.True(t, err4.Is(tvm.ErrTicketExpired))
	require.True(t, err5.Is(tvm.ErrTicketMalformed))

	// different status ticketErrors are not equal
	require.False(t, err1.Is(err4))

	// completely different ticketErrors are not equal
	require.False(t, err1.Is(err5))

	// non-nil ticketErrors are not equal to nil errors
	require.False(t, err1.Is(nil))
	require.False(t, err2.Is(nil))

	// non-nil ticketErrors are not equal to nil ticketErrors
	require.False(t, err1.Is(nilErr))
	require.False(t, err2.Is(nilErr))
}

func TestBbEnvFromString(t *testing.T) {
	type Case struct {
		in  string
		env tvm.BlackboxEnv
		err string
	}
	cases := []Case{
		{in: "prod", env: tvm.BlackboxProd},
		{in: "Prod", env: tvm.BlackboxProd},
		{in: "ProD", env: tvm.BlackboxProd},
		{in: "PROD", env: tvm.BlackboxProd},
		{in: "test", env: tvm.BlackboxTest},
		{in: "prod_yateam", env: tvm.BlackboxProdYateam},
		{in: "ProdYateam", env: tvm.BlackboxProdYateam},
		{in: "test_yateam", env: tvm.BlackboxTestYateam},
		{in: "TestYateam", env: tvm.BlackboxTestYateam},
		{in: "stress", env: tvm.BlackboxStress},
		{in: "", err: "blackbox env is unknown: ''"},
		{in: "kek", err: "blackbox env is unknown: 'kek'"},
	}

	for idx, c := range cases {
		res, err := tvm.BlackboxEnvFromString(c.in)

		if c.err == "" {
			require.NoError(t, err, idx)
			require.Equal(t, c.env, res, idx)
		} else {
			require.EqualError(t, err, c.err, idx)
		}
	}
}
