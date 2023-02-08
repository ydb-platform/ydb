#pragma once

#include <ydb/core/base/defs.h>
#include <ydb/core/base/events.h>
#include <ydb/core/protos/replication.pb.h>

namespace NKikimr::NReplication {

struct TEvController {
    enum EEv {
        EvCreateReplication = EventSpaceBegin(TKikimrEvents::ES_REPLICATION_CONTROLLER),
        EvCreateReplicationResult,
        EvAlterReplication,
        EvAlterReplicationResult,
        EvDropReplication,
        EvDropReplicationResult,

        EvEnd,
    };

    static_assert(EvEnd < EventSpaceEnd(TKikimrEvents::ES_REPLICATION_CONTROLLER), 
        "expect EvEnd < EventSpaceEnd(TKikimrEvents::ES_REPLICATION_CONTROLLER)");

    struct TEvCreateReplication
        : public TEventPB<TEvCreateReplication, NKikimrReplication::TEvCreateReplication, EvCreateReplication>
    {};

    struct TEvCreateReplicationResult
        : public TEventPB<TEvCreateReplicationResult, NKikimrReplication::TEvCreateReplicationResult, EvCreateReplicationResult>
    {};

    struct TEvDropReplication
        : public TEventPB<TEvDropReplication, NKikimrReplication::TEvDropReplication, EvDropReplication>
    {};

    struct TEvDropReplicationResult
        : public TEventPB<TEvDropReplicationResult, NKikimrReplication::TEvDropReplicationResult, EvDropReplicationResult>
    {};

}; // TEvController

}
