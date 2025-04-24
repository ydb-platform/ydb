#include "tvm_base.h"

namespace NYT::NAuth {

////////////////////////////////////////////////////////////////////////////////

TServiceTicketFixedAuth::TServiceTicketFixedAuth(std::string ticket)
    : Ticket_(std::move(ticket))
{ }

std::string TServiceTicketFixedAuth::IssueServiceTicket()
{
    return Ticket_;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NAuth
