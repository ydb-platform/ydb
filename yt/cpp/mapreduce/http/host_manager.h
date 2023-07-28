#pragma once

#include "fwd.h"

#include <util/generic/string.h>
#include <util/generic/hash.h>
#include <util/system/spinlock.h>


namespace NYT::NPrivate {

////////////////////////////////////////////////////////////////////////////////

class THostManager
{
public:
    static THostManager& Get();

    TString GetProxyForHeavyRequest(const TClientContext& context);

    // For testing purposes only.
    void Reset();

private:
    class TClusterHostList;

private:
    TAdaptiveLock Lock_;
    THashMap<TString, TClusterHostList> ClusterHosts_;

private:
    static TClusterHostList GetHosts(const TClientContext& context);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NPrivate
