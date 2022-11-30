package tvm

//go:generate ya tool mockgen -source=$GOFILE -destination=mocks/tvm.gen.go Client

import (
	"context"
	"fmt"
)

type ClientStatus int

// This constants must be in sync with EStatus from library/cpp/tvmauth/client/client_status.h
const (
	ClientOK ClientStatus = iota
	ClientWarning
	ClientError
)

func (s ClientStatus) String() string {
	switch s {
	case ClientOK:
		return "OK"
	case ClientWarning:
		return "Warning"
	case ClientError:
		return "Error"
	default:
		return fmt.Sprintf("Unknown%d", s)
	}
}

type ClientStatusInfo struct {
	Status ClientStatus

	// This message allows to trigger alert with useful message
	// It returns "OK" if Status==Ok
	LastError string
}

// Client allows to use aliases for ClientID.
//
// Alias is local label for ClientID which can be used to avoid this number in every checking case in code.
type Client interface {
	GetServiceTicketForAlias(ctx context.Context, alias string) (string, error)
	GetServiceTicketForID(ctx context.Context, dstID ClientID) (string, error)

	// CheckServiceTicket returns struct with SrcID: you should check it by yourself with ACL
	CheckServiceTicket(ctx context.Context, ticket string) (*CheckedServiceTicket, error)
	CheckUserTicket(ctx context.Context, ticket string, opts ...CheckUserTicketOption) (*CheckedUserTicket, error)
	GetRoles(ctx context.Context) (*Roles, error)

	// GetStatus returns current status of client:
	//  * you should trigger your monitoring if status is not Ok
	//  * it will be unable to operate if status is Invalid
	GetStatus(ctx context.Context) (ClientStatusInfo, error)
}
