#include "dsproxy_request_reporting.h"

namespace NKikimr {

static std::array<std::atomic<bool>, 7> ReportPermissions;

bool AllowToReport(NKikimrBlobStorage::EPutHandleClass handleClass) {
    return ReportPermissions[(ui32)handleClass].exchange(false);
}

bool AllowToReport(NKikimrBlobStorage::EGetHandleClass handleClass) {
    return ReportPermissions[(ui32)handleClass + 3].exchange(false);
}

class TRequestReportingThrottler : public TActorBootstrapped<TRequestReportingThrottler> {
public:
    TRequestReportingThrottler(TDuration updatePermissionsDelay)
        : UpdatePermissionsDelay(updatePermissionsDelay)
    {}

    void Bootstrap() {
        Schedule(UpdatePermissionsDelay, new TEvents::TEvWakeup);
        Become(&TThis::StateFunc);
    }

    STRICT_STFUNC(StateFunc,
        cFunc(TEvents::TEvWakeup::EventType, HandleWakeup);
    )

private:
    void HandleWakeup() {
        for (auto& permission : ReportPermissions) {
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
