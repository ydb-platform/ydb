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

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NConcurrency
