#pragma once

#include "public.h"

#include <yt/yt/core/ytree/yson_struct.h>

namespace NYT::NDns {

////////////////////////////////////////////////////////////////////////////////

class TAresDnsResolverConfig
    : public virtual NYTree::TYsonStruct
{
public:
    int Retries;
    TDuration RetryDelay;
    TDuration ResolveTimeout;
    TDuration MaxResolveTimeout;
    std::optional<double> Jitter;
    TDuration WarningTimeout;

    //! If set, Ares forcefully uses TCP for DNS queries.
    //! See ARES_FLAG_USEVC.
    bool ForceTcp;

    //! If set, Ares keeps socket open even if there are no active queries.
    //! See ARES_FLAG_STAYOPEN.
    bool KeepSocket;

    REGISTER_YSON_STRUCT(TAresDnsResolverConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TAresDnsResolverConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDns
