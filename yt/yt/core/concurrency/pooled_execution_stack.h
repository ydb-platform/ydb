#pragma once

#include "public.h"

#include <library/cpp/yt/threading/public.h>

namespace NYT::NConcurrency {

////////////////////////////////////////////////////////////////////////////////

std::shared_ptr<NThreading::TExecutionStack> GetPooledExecutionStack(EExecutionStackKind kind);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NConcurrency
