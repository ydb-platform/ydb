#include "dsproxy_request_reporting.h"

namespace NKikimr {

static std::array<std::atomic<bool>, NKikimrBlobStorage::EPutHandleClass_MAX> ReportPutPermissions;
static std::array<std::atomic<bool>, NKikimrBlobStorage::EGetHandleClass_MAX> ReportGetPermissions;

bool AllowToReport(NKikimrBlobStorage::EPutHandleClass handleClass) {
    return ReportPutPermissions[(ui32)handleClass - 1].exchange(false);
}

bool AllowToReport(NKikimrBlobStorage::EGetHandleClass handleClass) {
    return ReportGetPermissions[(ui32)handleClass - 1].exchange(false);
}

class TRequestReportingThrottler : public TActorBootstrapped<TRequestReportingThrottler> {
public:
    TRequestReportingThrottler(TDuration updatePermissionsDelay)
        : UpdatePermissionsDelay(updatePermissionsDelay)
    {}

    void Bootstrap() {
        Become(&TThis::StateFunc);
        HandleWakeup();
    }

    STRICT_STFUNC(StateFunc,
        cFunc(TEvents::TEvWakeup::EventType, HandleWakeup);
    )

private:
    void HandleWakeup() {
        for (std::atomic<bool>& permission : ReportPutPermissions) {
            permission.store(true);
        }
        for (std::atomic<bool>& permission : ReportGetPermissions) {
            permission.store(true);
        }
        Schedule(UpdatePermissionsDelay, new TEvents::TEvWakeup);
    }   

private:
    TDuration UpdatePermissionsDelay;
};

IActor* CreateRequestReportingThrottler(TDuration updatePermissionsDelay) {
    return new TRequestReportingThrottler(updatePermissionsDelay);
}

} // namespace NKikimr
