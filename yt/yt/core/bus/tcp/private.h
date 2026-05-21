#pragma once

#include "public.h"

#include <yt/yt/core/bus/private.h>

namespace NYT::NBus::NTcp {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TConnection)

DECLARE_REFCOUNTED_STRUCT(ILocalMessageHandler)

constexpr ui32 HandshakeMessageSignature = 0x68737562;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NBus::NTcp

