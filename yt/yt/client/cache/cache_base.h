#pragma once

#include "cache.h"

#include <library/cpp/yt/threading/rw_spin_lock.h>

namespace NYT::NClient::NCache {

////////////////////////////////////////////////////////////////////////////////

class TClientsCacheBase
    : public IClientsCache
{
public:
    NApi::IClientPtr GetClient(TStringBuf clusterUrl) override;

protected:
    virtual NApi::IClientPtr CreateClient(TStringBuf clusterUrl) = 0;

private:
    YT_DECLARE_SPIN_LOCK(NThreading::TReaderWriterSpinLock, Lock_);
    THashMap<TString, NApi::IClientPtr> Clients_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClient::NCache
