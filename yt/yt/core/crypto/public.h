#pragma once

#include <yt/yt/core/misc/public.h>

#include <library/cpp/yt/memory/intrusive_ptr.h>

namespace NYT::NCrypto {

////////////////////////////////////////////////////////////////////////////////

using TCertificatePathResolver = std::function<std::string(const std::string&)>;

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(TPemBlobConfig)
DECLARE_REFCOUNTED_STRUCT(TSslContextCommand)
DECLARE_REFCOUNTED_STRUCT(TSslContextConfig)
DECLARE_REFCOUNTED_CLASS(TSslContext)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCrypto
