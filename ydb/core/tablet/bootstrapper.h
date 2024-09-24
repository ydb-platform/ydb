#pragma once
#include "defs.h"
#include <ydb/core/tablet/tablet_setup.h>
#include <ydb/core/base/tablet.h>
#include <ydb/core/base/blobstorage.h>
#include <ydb/library/actors/core/event_simple_non_local.h>

namespace NKikimr {

struct TEvBootstrapper {
    enum EEv {
        EvActivate = EventSpaceBegin(TKikimrEvents::ES_BOOTSTRAPPER),
        EvStandBy,
        EvWatch,
        EvWatchResult,
        EvNotify,

        EvEnd,
    };

    static_assert(EvEnd < EventSpaceEnd(TKikimrEvents::ES_BOOTSTRAPPER), "event space overrun");

    struct TEvActivate : public TEventSimpleNonLocal<TEvActivate, EvActivate> {
    };

    struct TEvStandBy : public TEventSimpleNonLocal<TEvStandBy, EvStandBy> {
    };

    struct TEvWatch;
    struct TEvWatchResult;
    struct TEvNotify;
};

struct TBootstrapperInfo : public TThrRefBase {
    TIntrusivePtr<TTabletSetupInfo> SetupInfo;
    TVector<ui32> OtherNodes;
    TDuration WatchThreshold;
    TDuration OfflineDelay;
    bool StartFollowers;

    TBootstrapperInfo(TTabletSetupInfo* setupInfo)
        : SetupInfo(setupInfo)
        , WatchThreshold(TDuration::MilliSeconds(200))
        , OfflineDelay(TDuration::Seconds(3))
        , StartFollowers(false)
    {}
};

IActor* CreateBootstrapper(TTabletStorageInfo* tabletInfo, TBootstrapperInfo* bootstrapperInfo, bool standby = false);
TActorId MakeBootstrapperID(ui64 tablet, ui32 node);

}
