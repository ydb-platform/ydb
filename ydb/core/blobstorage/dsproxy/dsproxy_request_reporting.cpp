#include "dsproxy_request_reporting.h"

namespace NKikimr {

struct TReportLeakBucket {
    std::atomic<ui64> Level;
    TInstant LastUpdate;
};

static std::array<TReportLeakBucket, NKikimrBlobStorage::EPutHandleClass_MAX + 1> ReportPutPermissions;
static std::array<TReportLeakBucket, NKikimrBlobStorage::EGetHandleClass_MAX + 1> ReportGetPermissions;

bool AllowToReport(NKikimrBlobStorage::EPutHandleClass handleClass) {
    auto& permission = ReportPutPermissions[(ui32)handleClass];
    auto level = permission.Level.fetch_sub(1);
    if (level < 1) {
        permission.Level++;
        return false;
    }
    return true;
}

bool AllowToReport(NKikimrBlobStorage::EGetHandleClass handleClass) {
    auto& permission = ReportGetPermissions[(ui32)handleClass];
    auto level = permission.Level.fetch_sub(1);
    if (level < 1) {
        permission.Level++;
        return false;
    }
    return true;
}

class TRequestReportingThrottler : public TActorBootstrapped<TRequestReportingThrottler> {
public:
    TRequestReportingThrottler(const TControlWrapper& bucketSize, const TControlWrapper& leakDurationMs,
            const TControlWrapper& leakRate, const TControlWrapper& updatingDurationMs)
        : BucketSize(bucketSize)
        , LeakDurationMs(leakDurationMs)
        , LeakRate(leakRate)
        , UpdatingDurationMs(updatingDurationMs)
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
    void Update(const TInstant& now, TInstant& lastUpdate, std::atomic<ui64>& bucketLevel) {
        ui64 bucketSize = BucketSize.Update(now);
        ui64 leakRate = LeakRate.Update(now);
        ui64 leakDurationMs = LeakDurationMs.Update(now);

        ui64 msSinceLastUpdate = (now - lastUpdate).MilliSeconds();
        ui64 intervalsCount = msSinceLastUpdate / leakDurationMs;
        auto level = bucketLevel.load();
        if (level >= bucketSize) {
            lastUpdate = now;
            return;
        }
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
        Schedule(TDuration::MilliSeconds(UpdatingDurationMs.Update(now)), new TEvents::TEvWakeup);
    }   

private:
    TMemorizableControlWrapper BucketSize;
    TMemorizableControlWrapper LeakDurationMs;
    TMemorizableControlWrapper LeakRate;
    TMemorizableControlWrapper UpdatingDurationMs;
};

IActor* CreateRequestReportingThrottler(const TControlWrapper& bucketSize, const TControlWrapper& leakDurationMs,
        const TControlWrapper& leakRate, const TControlWrapper& updatingDurationMs) {
    return new TRequestReportingThrottler(bucketSize, leakDurationMs, leakRate, updatingDurationMs);
}

} // namespace NKikimr
