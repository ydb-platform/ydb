#pragma once

#include "public.h"

#include <util/generic/string.h>

namespace NYT::NAuth {

////////////////////////////////////////////////////////////////////////////////

struct IServiceTicketAuth
    : public virtual TRefCounted
{
    virtual std::string IssueServiceTicket() = 0;
};

DEFINE_REFCOUNTED_TYPE(IServiceTicketAuth)

////////////////////////////////////////////////////////////////////////////////

class TServiceTicketFixedAuth
    : public IServiceTicketAuth
{
public:
    explicit TServiceTicketFixedAuth(std::string ticket);

    std::string IssueServiceTicket() override;

private:
    const std::string Ticket_;
};

DEFINE_REFCOUNTED_TYPE(TServiceTicketFixedAuth)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NAuth
