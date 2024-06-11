#pragma once

#include "test_memory_tracker.h"

#include <yt/yt/core/bus/public.h>

#include <yt/yt/core/rpc/service_detail.h>

#include <library/cpp/testing/common/network.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class TTestServerHost
    : public TRefCounted
{
public:
    TTestServerHost(
        NTesting::TPortHolder port,
        NRpc::IServerPtr server,
        std::vector<NRpc::IServicePtr> services,
        TTestNodeMemoryTrackerPtr memoryUsageTracker);

    void TearDown();

    TTestNodeMemoryTrackerPtr GetMemoryUsageTracker() const;
    TString GetAddress() const;
    std::vector<NRpc::IServicePtr> GetServices() const;
    NRpc::IServerPtr GetServer() const;

protected:
    NTesting::TPortHolder Port_;
    NRpc::IServerPtr Server_;
    const std::vector<NRpc::IServicePtr> Services_;
    const TTestNodeMemoryTrackerPtr MemoryUsageTracker_;

    void InitializeServer();
};

DECLARE_REFCOUNTED_CLASS(TTestServerHost)
DEFINE_REFCOUNTED_TYPE(TTestServerHost)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpc
