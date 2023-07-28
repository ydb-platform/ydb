#include "pollable_detail.h"

namespace NYT::NConcurrency {

////////////////////////////////////////////////////////////////////////////////

void TPollableBase::SetCookie(TCookiePtr cookie)
{
    Cookie_ = std::move(cookie);
}

void* TPollableBase::GetCookie() const
{
    return Cookie_.Get();
}

EPollablePriority TPollableBase::GetPriority() const
{
    return EPollablePriority::Normal;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NConcurrency
