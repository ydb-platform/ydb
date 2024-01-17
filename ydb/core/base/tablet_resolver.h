#pragma once
#include "defs.h"
#include "events.h"

#include <ydb/core/protos/base.pb.h>
#include <ydb/library/actors/core/event_local.h>
#include <util/stream/str.h>
#include <util/string/builder.h>

namespace NKikimr {

struct TEvTabletResolver {
    enum EEv {
        EvForward = EventSpaceBegin(TKikimrEvents::ES_TABLETRESOLVER),
        EvTabletProblem,
        EvNodeProblem,

        EvForwardResult = EvForward + 1 * 512,

        EvEnd
    };

    static_assert(EvEnd < EventSpaceEnd(TKikimrEvents::ES_TABLETRESOLVER), "expect EvEnd < EventSpaceEnd(TKikimrEvents::ES_TABLETRESOLVER)");

    struct TEvForward : public TEventLocal<TEvForward, EvForward> {
        ///
        enum class EResolvePrio : ui8 {
            ResPrioDisallow,
            ResPrioAllow,
            ResPrioPrefer,
            ResPrioForce,
        };

        ///
        struct TResolveFlags {
            EResolvePrio LocalNodePrio;
            EResolvePrio LocalDcPrio;
            EResolvePrio FollowerPrio;

            TResolveFlags()
                : LocalNodePrio(EResolvePrio::ResPrioAllow)
                , LocalDcPrio(EResolvePrio::ResPrioPrefer)
                , FollowerPrio(EResolvePrio::ResPrioDisallow)
            {}

            static constexpr ui32 MaxTabletPriority() { return 4; }

            ui32 GetTabletPriority(bool isLocalNode, bool isLocalDc, bool isFollower) const {
                if (isLocalNode && (LocalNodePrio == EResolvePrio::ResPrioDisallow) ||
                    isLocalDc && (LocalDcPrio == EResolvePrio::ResPrioDisallow) ||
                    isFollower && (FollowerPrio == EResolvePrio::ResPrioDisallow) ||
                    !isLocalNode && (LocalNodePrio == EResolvePrio::ResPrioForce) ||
                    !isLocalDc && (LocalDcPrio == EResolvePrio::ResPrioForce) ||
                    !isFollower && (FollowerPrio == EResolvePrio::ResPrioForce))
                {
                    return 0;
                }

                ui32 prio = 1;
                if (isLocalNode && (LocalNodePrio == EResolvePrio::ResPrioPrefer))
                    prio |= 2;
                if (isLocalDc && (LocalDcPrio == EResolvePrio::ResPrioPrefer))
                    prio |= 8;
                if (isFollower && (FollowerPrio == EResolvePrio::ResPrioPrefer))
                    prio |= 4;
                return prio;
            }

            bool AllowFollower() const { return FollowerPrio != EResolvePrio::ResPrioDisallow; }

            void SetAllowFollower(bool flag, bool allowMeansPrefer = true) {
                FollowerPrio = EResolvePrio::ResPrioDisallow;
                if (flag)
                    FollowerPrio = (allowMeansPrefer ? EResolvePrio::ResPrioPrefer : EResolvePrio::ResPrioAllow);
            }

            void SetForceFollower(bool flag) {
                if (flag)
                    FollowerPrio = EResolvePrio::ResPrioForce;
            }

            void SetPreferLocal(bool flag) {
                if (flag && LocalNodePrio < EResolvePrio::ResPrioPrefer)
                    LocalNodePrio = EResolvePrio::ResPrioPrefer;
            }

            void SetForceLocal(bool flag) {
                if (flag)
                    LocalNodePrio = EResolvePrio::ResPrioForce;
            }

            TString ToString() const {
                TStringStream str;
                str << (ui32) LocalNodePrio << ':'
                    << (ui32) LocalDcPrio << ':'
                    << (ui32) FollowerPrio;
                return str.Str();
            }
        };

        ///
        enum class EActor : ui8 {
            Tablet,
            SysTablet,
        };

        const ui64 TabletID;
        THolder<IEventHandle> Ev;
        TResolveFlags ResolveFlags;
        EActor Actor;

        TEvForward(ui64 tabletId, IEventHandle *ev, TResolveFlags flags = TResolveFlags(), EActor actor = EActor::Tablet)
            : TabletID(tabletId)
            , Ev(ev)
            , ResolveFlags(flags)
            , Actor(actor)
        {}

        TString ToString() const {
            TStringStream str;
            str << "{EvForward TabletID: " << TabletID;
            str << " Ev: " << (Ev ? Ev->ToString() : "nullptr");
            str << " Flags: " << ResolveFlags.ToString();
            str << "}";
            return str.Str();
        }

        TActorId SelectActor(const TActorId& tablet, const TActorId& sysTablet) const {
            switch (Actor) {
                case EActor::Tablet:
                    return tablet;
                case EActor::SysTablet:
                    return sysTablet;
            }
        }
    };

    struct TEvTabletProblem : public TEventLocal<TEvTabletProblem, EvTabletProblem> {
        const ui64 TabletID;
        const TActorId TabletActor;

        TEvTabletProblem(ui64 tabletId, const TActorId &tabletActor)
            : TabletID(tabletId)
            , TabletActor(tabletActor)
        {}

        TString ToString() const {
            TStringStream str;
            str << "{EvTabletProblem TabletID: " << TabletID;
            str << " TabletActor: " << TabletActor.ToString();
            str << "}";
            return str.Str();
        }
    };

    struct TEvNodeProblem : public TEventLocal<TEvNodeProblem, EvNodeProblem> {
        const ui32 NodeId;
        const ui64 CacheEpoch;

        TEvNodeProblem(ui32 nodeId, ui64 cacheEpoch)
            : NodeId(nodeId)
            , CacheEpoch(cacheEpoch)
        {}

        TString ToString() const {
            return TStringBuilder()
                << "{EvNodeProblem NodeId: " << NodeId
                << " CacheEpoch: " << CacheEpoch
                << "}";
        }
    };

    struct TEvForwardResult : public TEventLocal<TEvForwardResult, EvForwardResult> {
        const NKikimrProto::EReplyStatus Status;

        ui64 TabletID;
        TActorId TabletActor;
        TActorId Tablet;
        ui64 CacheEpoch;

        TEvForwardResult(NKikimrProto::EReplyStatus status, ui64 tabletId)
            : Status(status)
            , TabletID(tabletId)
            , CacheEpoch(0)
        {}

        TEvForwardResult(ui64 tabletId, const TActorId &tabletActor, const TActorId &tablet, ui64 cacheEpoch)
            : Status(NKikimrProto::OK)
            , TabletID(tabletId)
            , TabletActor(tabletActor)
            , Tablet(tablet)
            , CacheEpoch(cacheEpoch)
        {}

        TString ToString() const {
            TStringStream str;
            str << "{EvForwardResult Status: " << (ui32)Status;
            str << " TabletID: " << TabletID;
            str << " TabletActor: " << TabletActor.ToString();
            str << " Tablet: " << Tablet.ToString();
            str << " CacheEpoch: " << CacheEpoch;
            str << "}";
            return str.Str();
        }
    };
};

struct TTabletResolverConfig : public TThrRefBase {
    ui64 TabletCacheLimit;

    TTabletResolverConfig()
        : TabletCacheLimit(500000)
    {}
};

IActor* CreateTabletResolver(const TIntrusivePtr<TTabletResolverConfig> &config);
TActorId MakeTabletResolverID();

}
