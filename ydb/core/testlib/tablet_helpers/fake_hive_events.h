#pragma once

#include <ydb/core/base/hive.h>

namespace NKikimr {
    struct TEvFakeHiveRuntime {
        enum EEv {
            EvSubscribeToTabletDeletion = TEvHive::EvEnd + 1,
            EvNotifyTabletDeleted,
            EvRequestDomainInfo,
            EvRequestDomainInfoReply
        };

        struct TEvSubscribeToTabletDeletion : public TEventLocal<TEvSubscribeToTabletDeletion, EvSubscribeToTabletDeletion> {
            ui64 TabletId;

            explicit TEvSubscribeToTabletDeletion(ui64 tabletId)
                : TabletId(tabletId)
            {}
        };

        struct TEvNotifyTabletDeleted : public TEventLocal<TEvNotifyTabletDeleted, EvNotifyTabletDeleted> {
            ui64 TabletId;

            explicit TEvNotifyTabletDeleted(ui64 tabletId)
                : TabletId(tabletId)
            {}
        };

    };
}
