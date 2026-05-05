#include "pollable_detail.h"

namespace NYT::NConcurrency {

////////////////////////////////////////////////////////////////////////////////

void TPollableBase::SetCookie(TCookiePtr cookie)
{
    YT_VERIFY(!Cookie_);
    Cookie_ = std::move(cookie);
}

void* TPollableBase::GetCookie() const
{
    return Cookie_.Get();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NConcurrency
