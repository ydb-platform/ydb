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

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NConcurrency
