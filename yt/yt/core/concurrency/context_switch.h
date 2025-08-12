#pragma once

#include "public.h"

namespace NYT::NConcurrency {

////////////////////////////////////////////////////////////////////////////////

//! Returns |true| if fiber context switch is currently forbidden.
bool IsContextSwitchForbidden();

class TForbidContextSwitchGuard
{
public:
    TForbidContextSwitchGuard();
    TForbidContextSwitchGuard(const TForbidContextSwitchGuard&) = delete;

    ~TForbidContextSwitchGuard();

private:
    const bool OldValue_;
};

////////////////////////////////////////////////////////////////////////////////

// NB: Use function pointer to minimize the overhead.
using TGlobalContextSwitchHandler = void(*)();

void InstallGlobalContextSwitchHandlers(
    TGlobalContextSwitchHandler outHandler,
    TGlobalContextSwitchHandler inHandler);

////////////////////////////////////////////////////////////////////////////////

using TContextSwitchHandler = std::function<void()>;

class TContextSwitchGuard
{
public:
    TContextSwitchGuard(
        TContextSwitchHandler outHandler,
        TContextSwitchHandler inHandler);

    TContextSwitchGuard(const TContextSwitchGuard& other) = delete;
    ~TContextSwitchGuard();
};

class TOneShotContextSwitchGuard
    : public TContextSwitchGuard
{
public:
    explicit TOneShotContextSwitchGuard(TContextSwitchHandler outHandler);

private:
    bool Active_;
};

////////////////////////////////////////////////////////////////////////////////

} //namespace NYT::NConcurrency
