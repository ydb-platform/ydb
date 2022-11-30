package tvm

import (
	"fmt"
)

// CheckedUserTicket is short-lived user credential.
//
// CheckedUserTicket contains only valid users.
// Details: https://wiki.yandex-team.ru/passport/tvm2/user-ticket/#chtoestvusertickete
type CheckedUserTicket struct {
	// DefaultUID is default user - maybe 0
	DefaultUID UID
	// UIDs is array of valid users - never empty
	UIDs []UID
	// Env is blackbox environment which created this UserTicket - provides only tvmauth now
	Env BlackboxEnv
	// Scopes is array of scopes inherited from credential - never empty
	Scopes []string
	// DbgInfo is human readable data for debug purposes
	DbgInfo string
	// LogInfo is safe for logging part of ticket - it can be parsed later with `tvmknife parse_ticket -t ...`
	LogInfo string
}

func (t CheckedUserTicket) String() string {
	return fmt.Sprintf("%s (%s)", t.LogInfo, t.DbgInfo)
}

// CheckScopes verify that ALL needed scopes presents in the user ticket
func (t *CheckedUserTicket) CheckScopes(scopes ...string) error {
	switch {
	case len(scopes) == 0:
		// ok, no scopes. no checks. no rules
		return nil
	case len(t.Scopes) == 0:
		msg := fmt.Sprintf("user ticket doesn't contain expected scopes: %s (actual: nil)", scopes)
		return &TicketError{Status: TicketInvalidScopes, Msg: msg}
	default:
		actualScopes := make(map[string]struct{}, len(t.Scopes))
		for _, s := range t.Scopes {
			actualScopes[s] = struct{}{}
		}

		for _, s := range scopes {
			if _, found := actualScopes[s]; !found {
				// exit on first nonexistent scope
				msg := fmt.Sprintf(
					"user ticket doesn't contain one of expected scopes: %s (actual: %s)",
					scopes, t.Scopes,
				)

				return &TicketError{Status: TicketInvalidScopes, Msg: msg}
			}
		}

		return nil
	}
}

// CheckScopesAny verify that ANY of needed scopes presents in the user ticket
func (t *CheckedUserTicket) CheckScopesAny(scopes ...string) error {
	switch {
	case len(scopes) == 0:
		// ok, no scopes. no checks. no rules
		return nil
	case len(t.Scopes) == 0:
		msg := fmt.Sprintf("user ticket doesn't contain any of expected scopes: %s (actual: nil)", scopes)
		return &TicketError{Status: TicketInvalidScopes, Msg: msg}
	default:
		actualScopes := make(map[string]struct{}, len(t.Scopes))
		for _, s := range t.Scopes {
			actualScopes[s] = struct{}{}
		}

		for _, s := range scopes {
			if _, found := actualScopes[s]; found {
				// exit on first valid scope
				return nil
			}
		}

		msg := fmt.Sprintf(
			"user ticket doesn't contain any of expected scopes: %s (actual: %s)",
			scopes, t.Scopes,
		)

		return &TicketError{Status: TicketInvalidScopes, Msg: msg}
	}
}

type CheckUserTicketOptions struct {
	EnvOverride *BlackboxEnv
}

type CheckUserTicketOption func(*CheckUserTicketOptions)

func WithBlackboxOverride(env BlackboxEnv) CheckUserTicketOption {
	return func(opts *CheckUserTicketOptions) {
		opts.EnvOverride = &env
	}
}

type UserTicketACL func(ticket *CheckedUserTicket) error

func AllowAllUserTickets() UserTicketACL {
	return func(ticket *CheckedUserTicket) error {
		return nil
	}
}

func CheckAllUserTicketScopesPresent(scopes []string) UserTicketACL {
	return func(ticket *CheckedUserTicket) error {
		return ticket.CheckScopes(scopes...)
	}
}

func CheckAnyUserTicketScopesPresent(scopes []string) UserTicketACL {
	return func(ticket *CheckedUserTicket) error {
		return ticket.CheckScopesAny(scopes...)
	}
}
