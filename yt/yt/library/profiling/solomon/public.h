#pragma once

#include <library/cpp/yt/memory/ref_counted.h>

#include <library/cpp/yt/misc/enum.h>
#include <library/cpp/yt/misc/typeid.h>

namespace NYT::NProfiling {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(TShardConfig)
DECLARE_REFCOUNTED_STRUCT(TSolomonExporterConfig)
DECLARE_REFCOUNTED_STRUCT(TSolomonProxyConfig)

DECLARE_REFCOUNTED_CLASS(TSolomonExporter)
YT_DECLARE_TYPEID(TSolomonExporter)
DECLARE_REFCOUNTED_CLASS(TSolomonRegistry)
DECLARE_REFCOUNTED_CLASS(TSolomonProxy)

DECLARE_REFCOUNTED_STRUCT(IEndpointProvider)

////////////////////////////////////////////////////////////////////////////////

static constexpr int MaxSolomonLabelSize = 200;

DEFINE_ENUM(ELabelSanitizationPolicy,
    ((None)   (0))
    ((Weak)   (1)) // Escape only zero symbol and trim label to 200 symbols
    ((Strong) (2)) // Escape all forbidden symbols and trim label to 200 symbols
);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NProfiling
