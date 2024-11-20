#include "dsproxy_request_reporting.h"

namespace NKikimr {

struct TReportLeakBucket {
    ui64 Level;
    TInstant LastUpdate;
    TMutex Lock;
};

static std::array<TReportLeakBucket, NKikimrBlobStorage::EPutHandleClass_MAX + 1> ReportPutPermissions;
static std::array<TReportLeakBucket, NKikimrBlobStorage::EGetHandleClass_MAX + 1> ReportGetPermissions;

bool AllowToReport(NKikimrBlobStorage::EPutHandleClass handleClass) {
    auto& permission = ReportPutPermissions[(ui32)handleClass];
    TGuard<TMutex> guard(permission.Lock);
    auto level = permission.Level;
    permission.Level = std::max((i64)0, (i64)level - 1);
    return level > 0;
}

bool AllowToReport(NKikimrBlobStorage::EGetHandleClass handleClass) {
    auto& permission = ReportGetPermissions[(ui32)handleClass];
    TGuard<TMutex> guard(permission.Lock);
    auto level = permission.Level;
    permission.Level = std::max((i64)0, (i64)level - 1);
    return level > 0;
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
            permission.Level = BucketSize;
        }
        for (auto& permission : ReportGetPermissions) {
            permission.Level = BucketSize;
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
    void Update(const TInstant& now, TInstant& lastUpdate, ui64& bucketLevel) {
        ui64 bucketSize = BucketSize.Update(now);
        ui64 leakRate = LeakRate.Update(now);
        ui64 leakDurationMs = LeakDurationMs.Update(now);

        ui64 msSinceLastUpdate = (now - lastUpdate).MilliSeconds();
        ui64 intervalsCount = msSinceLastUpdate / leakDurationMs;
        if (bucketLevel == bucketSize) {
            lastUpdate = now;
            return;
        }
        lastUpdate += TDuration::MilliSeconds(intervalsCount * leakDurationMs);
        bucketLevel = std::min(bucketLevel + leakRate * intervalsCount, bucketSize);
    }

    void HandleWakeup(const TActorContext& ctx) {
        TInstant now = ctx.Now();
        for (auto& permission : ReportPutPermissions) {
            TGuard<TMutex> guard(permission.Lock);
            Update(now, permission.LastUpdate, permission.Level);
        }
        for (auto& permission : ReportGetPermissions) {
            TGuard<TMutex> guard(permission.Lock);
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
