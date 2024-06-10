#pragma once

#include <ydb/core/base/defs.h>
#include <ydb/core/base/events.h>
#include <ydb/core/protos/replication.pb.h>

namespace NKikimr::NReplication {

struct TEvService {
    enum EEv {
        EvBegin = EventSpaceBegin(TKikimrEvents::ES_REPLICATION_SERVICE),

        EvHandshake,
        EvStatus,

        EvEnd,
    };

    static_assert(EvEnd < EventSpaceEnd(TKikimrEvents::ES_REPLICATION_SERVICE));

    struct TEvHandshake: public TEventPB<TEvHandshake, NKikimrReplication::TEvHandshake, EvHandshake> {
        TEvHandshake() = default;

        explicit TEvHandshake(ui64 tabletId, ui64 generation) {
            Record.SetControllerId(tabletId);
            Record.SetGeneration(generation);
        }
    };

    struct TEvStatus: public TEventPB<TEvStatus, NKikimrReplication::TEvStatus, EvStatus> {
        TEvStatus() = default;
    };
};

namespace NService {

inline TString MakeDiscoveryPath(const TString& tenant) {
    return "rs+" + tenant;
}

} // NService

inline TActorId MakeReplicationServiceId(ui32 nodeId) {
    return TActorId(nodeId, TStringBuf("ReplictnSvc"));
}

IActor* CreateReplicationService();

}
