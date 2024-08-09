#include "dsproxy_request_reporting.h"

namespace NKikimr {

ui32 GetPermissionIdx(NKikimrBlobStorage::EPutHandleClass& handleClass) {
    switch (handleClass) {
    case NKikimrBlobStorage::TabletLog:
        return 0;
    case NKikimrBlobStorage::AsyncBlob:
        return 1;
    case NKikimrBlobStorage::UserData:
        return 2;
    default:
        Y_FAIL_S("Unexpected Put handleClass# " << (ui32)handleClass);
    }
}

ui32 GetPermissionIdx(NKikimrBlobStorage::EGetHandleClass& handleClass) {
    switch (handleClass) {
    case NKikimrBlobStorage::AsyncRead:
        return 3;
    case NKikimrBlobStorage::FastRead:
        return 4;
    case NKikimrBlobStorage::Discover:
        return 5;
    case NKikimrBlobStorage::LowRead:
        return 6;
    default:
        Y_FAIL_S("Unexpected Get handleClass# " << (ui32)handleClass);
    }
}

static std::array<std::atomic<bool>, 7> ReportPermissions;

bool AllowToReport(NKikimrBlobStorage::EPutHandleClass& handleClass) {
    return ReportPermissions[GetPermissionIdx(handleClass)].exchange(false);
}

bool AllowToReport(NKikimrBlobStorage::EGetHandleClass& handleClass) {
    return ReportPermissions[GetPermissionIdx(handleClass)].exchange(false);
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
