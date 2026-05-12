#pragma once

#include "poller.h"

namespace NYT::NConcurrency {

////////////////////////////////////////////////////////////////////////////////

//! A base class for implementing IPollable.
class TPollableBase
    : public IPollable
{
public:
    void SetCookie(TCookiePtr cookie) override;
    void* GetCookie() const override;

private:
    TCookiePtr Cookie_;
};

//! Make a pollable that calls body in OnEvent.
template <std::invocable<NConcurrency::IPollable&, NConcurrency::EPollControl> T>
NConcurrency::IPollablePtr MakeSimplePollable(T body, std::string loggingTag);


////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NConcurrency

#define POLLABLE_DETAIL_INL_H_
#include "pollable_detail-inl.h"
#undef POLLABLE_DETAIL_INL_H_
