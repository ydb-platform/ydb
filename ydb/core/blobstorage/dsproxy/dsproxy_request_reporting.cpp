#include "dsproxy_request_reporting.h"

namespace NKikimr {

static std::array<std::atomic<bool>, 7> ReportPermissions;

bool AllowToReport(NKikimrBlobStorage::EPutHandleClass handleClass) {
    return ReportPermissions[(ui32)handleClass - 1].exchange(false);
}

bool AllowToReport(NKikimrBlobStorage::EGetHandleClass handleClass) {
    return ReportPermissions[(ui32)handleClass - 1 + 3].exchange(false);
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
        for (std::atomic<bool>& permission : ReportPermissions) {
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
