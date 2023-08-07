#include "tvm_base.h"

namespace NYT::NAuth {

////////////////////////////////////////////////////////////////////////////////

TServiceTicketFixedAuth::TServiceTicketFixedAuth(TString ticket)
    : Ticket_(std::move(ticket))
{ }

TString TServiceTicketFixedAuth::IssueServiceTicket()
{
    return Ticket_;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NAuth
