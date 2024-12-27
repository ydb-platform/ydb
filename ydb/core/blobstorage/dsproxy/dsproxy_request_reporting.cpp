#include "dsproxy_request_reporting.h"

namespace NKikimr {

struct TReportLeakBucket {
    std::atomic<i64> Level;
    TInstant LastUpdate;
};

static std::array<TReportLeakBucket, NKikimrBlobStorage::EPutHandleClass_MAX + 1> ReportPutPermissions;
static std::array<TReportLeakBucket, NKikimrBlobStorage::EGetHandleClass_MAX + 1> ReportGetPermissions;

bool GetFromBucket(TReportLeakBucket& bucket) {
    auto level = bucket.Level.fetch_sub(1);
    if (level < 1) {
        bucket.Level++;
        return false;
    }
    return true;
}

bool AllowToReport(NKikimrBlobStorage::EPutHandleClass handleClass) {
    return GetFromBucket(ReportPutPermissions[(ui32)handleClass]);
}

bool AllowToReport(NKikimrBlobStorage::EGetHandleClass handleClass) {
    return GetFromBucket(ReportGetPermissions[(ui32)handleClass]);
}

class TRequestReportingThrottler : public TActorBootstrapped<TRequestReportingThrottler> {
public:
    TRequestReportingThrottler(const TControlWrapper& bucketSize, const TControlWrapper& leakDurationMs,
            const TControlWrapper& leakRate)
        : BucketSize(bucketSize)
        , LeakDurationMs(leakDurationMs)
        , LeakRate(leakRate)
    {
        for (auto& permission : ReportPutPermissions) {
            permission.Level.store(BucketSize);
        }
        for (auto& permission : ReportGetPermissions) {
            permission.Level.store(BucketSize);
        }
    }

    void Bootstrap(const TActorContext &ctx) {
        for (auto& permission : ReportPutPermissions) {
            permission.LastUpdate = ctx.Now();
        }
        for (auto& permission : ReportGetPermissions) {
            permission.LastUpdate = ctx.Now();
        }
        Become(&TThis::StateFunc);
        HandleWakeup(ctx);
    }

    STRICT_STFUNC(StateFunc,
        CFunc(TEvents::TEvWakeup::EventType, HandleWakeup);
    )

private:
    void Update(const TInstant& now, TInstant& lastUpdate, std::atomic<i64>& bucketLevel) {
        i64 bucketSize = BucketSize.Update(now);
        i64 leakRate = LeakRate.Update(now);
        i64 leakDurationMs = LeakDurationMs.Update(now);

        auto level = bucketLevel.load();
        if (level >= bucketSize) {
            lastUpdate = now;
            return;
        }
        i64 msSinceLastUpdate = (now - lastUpdate).MilliSeconds();
        i64 intervalsCount = msSinceLastUpdate / leakDurationMs;
        bucketLevel += std::min(leakRate * intervalsCount, bucketSize - level);
        lastUpdate += TDuration::MilliSeconds(intervalsCount * leakDurationMs);
    }

    void HandleWakeup(const TActorContext& ctx) {
        TInstant now = ctx.Now();
        for (auto& permission : ReportPutPermissions) {
            Update(now, permission.LastUpdate, permission.Level);
        }
        for (auto& permission : ReportGetPermissions) {
            Update(now, permission.LastUpdate, permission.Level);
        }
        Schedule(TDuration::MilliSeconds(LeakDurationMs.Update(now)), new TEvents::TEvWakeup);
    }   

private:
    TMemorizableControlWrapper BucketSize;
    TMemorizableControlWrapper LeakDurationMs;
    TMemorizableControlWrapper LeakRate;
};

IActor* CreateRequestReportingThrottler(const TControlWrapper& bucketSize, const TControlWrapper& leakDurationMs,
        const TControlWrapper& leakRate) {
    return new TRequestReportingThrottler(bucketSize, leakDurationMs, leakRate);
}

} // namespace NKikimr
