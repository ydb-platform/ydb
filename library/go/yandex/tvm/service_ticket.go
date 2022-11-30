package tvm

import (
	"fmt"
)

// CheckedServiceTicket is service credential
type CheckedServiceTicket struct {
	// SrcID is ID of request source service. You should check SrcID by yourself with your ACL.
	SrcID ClientID
	// IssuerUID is UID of developer who is debuging something, so he(she) issued CheckedServiceTicket with his(her) ssh-sign:
	// it is grant_type=sshkey in tvm-api
	// https://wiki.yandex-team.ru/passport/tvm2/debug/#sxoditvapizakrytoeserviceticketami.
	IssuerUID UID
	// DbgInfo is human readable data for debug purposes
	DbgInfo string
	// LogInfo is safe for logging part of ticket - it can be parsed later with `tvmknife parse_ticket -t ...`
	LogInfo string
}

func (t *CheckedServiceTicket) CheckSrcID(allowedSrcIDsMap map[uint32]struct{}) error {
	if len(allowedSrcIDsMap) == 0 {
		return nil
	}
	if _, allowed := allowedSrcIDsMap[uint32(t.SrcID)]; !allowed {
		return &TicketError{
			Status: TicketInvalidSrcID,
			Msg:    fmt.Sprintf("service ticket srcID is not in allowed srcIDs: %v (actual: %v)", allowedSrcIDsMap, t.SrcID),
		}
	}
	return nil
}

func (t CheckedServiceTicket) String() string {
	return fmt.Sprintf("%s (%s)", t.LogInfo, t.DbgInfo)
}

type ServiceTicketACL func(ticket *CheckedServiceTicket) error

func AllowAllServiceTickets() ServiceTicketACL {
	return func(ticket *CheckedServiceTicket) error {
		return nil
	}
}

func CheckServiceTicketSrcID(allowedSrcIDs map[uint32]struct{}) ServiceTicketACL {
	return func(ticket *CheckedServiceTicket) error {
		return ticket.CheckSrcID(allowedSrcIDs)
	}
}
