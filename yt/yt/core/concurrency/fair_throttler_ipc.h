#pragma once

#include "public.h"

#include <library/cpp/yt/logging/logger.h>

#include <atomic>

namespace NYT::NConcurrency {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(IFairThrottlerIpcBucket);

struct IFairThrottlerIpcBucket
    : public TRefCounted
{
    // NB: This struct is shared between processes. All changes must be backward compatible.
#pragma pack(push, 1)
    struct TState
    {
        std::atomic<double> Weight;
        std::atomic<i64> Limit;
        std::atomic<i64> Demand;
        std::atomic<i64> InFlow;
        std::atomic<i64> OutFlow;
        std::atomic<i64> GuaranteedQuota;
    };
#pragma pack(pop)

    virtual TState* GetState() = 0;
};

DEFINE_REFCOUNTED_TYPE(IFairThrottlerIpcBucket);

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(IFairThrottlerIpc);

struct IFairThrottlerIpc
    : public TRefCounted
{
    // NB: This struct is shared between processes. All changes must be backward compatible.
#pragma pack(push, 1)
    struct TSharedState
    {
        std::atomic<i64> Value;
    };
#pragma pack(pop)

    virtual bool TryLock() = 0;
    virtual TSharedState* GetState() = 0;
    virtual std::vector<IFairThrottlerIpcBucketPtr> ListBuckets() = 0;
    virtual IFairThrottlerIpcBucketPtr CreateBucket() = 0;
};

DEFINE_REFCOUNTED_TYPE(IFairThrottlerIpc);

////////////////////////////////////////////////////////////////////////////////

IFairThrottlerIpcPtr CreateFairThrottlerFileIpc(
    const std::string& rootPath,
    bool useShmem,
    NLogging::TLogger logger);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NConcurrency
