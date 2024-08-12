#pragma once

#include <ydb/core/base/defs.h>
#include <ydb/core/base/events.h>
#include <ydb/core/protos/replication.pb.h>
#include <ydb/core/tx/replication/common/sensitive_event_pb.h>

namespace NKikimr::NReplication {

struct TEvController {
    enum EEv {
        EvCreateReplication = EventSpaceBegin(TKikimrEvents::ES_REPLICATION_CONTROLLER),
        EvCreateReplicationResult,
        EvAlterReplication,
        EvAlterReplicationResult,
        EvDropReplication,
        EvDropReplicationResult,
        EvDescribeReplication,
        EvDescribeReplicationResult,

        EvEnd,
    };

    static_assert(EvEnd < EventSpaceEnd(TKikimrEvents::ES_REPLICATION_CONTROLLER), 
        "expect EvEnd < EventSpaceEnd(TKikimrEvents::ES_REPLICATION_CONTROLLER)");

    struct TEvCreateReplication
        : public TSensitiveEventPB<TEvCreateReplication, NKikimrReplication::TEvCreateReplication, EvCreateReplication>
    {};

    struct TEvCreateReplicationResult
        : public TEventPB<TEvCreateReplicationResult, NKikimrReplication::TEvCreateReplicationResult, EvCreateReplicationResult>
    {};

    struct TEvAlterReplication
        : public TSensitiveEventPB<TEvAlterReplication, NKikimrReplication::TEvAlterReplication, EvAlterReplication>
    {};

    struct TEvAlterReplicationResult
        : public TEventPB<TEvAlterReplicationResult, NKikimrReplication::TEvAlterReplicationResult, EvAlterReplicationResult>
    {};

    struct TEvDropReplication
        : public TEventPB<TEvDropReplication, NKikimrReplication::TEvDropReplication, EvDropReplication>
    {};

    struct TEvDropReplicationResult
        : public TEventPB<TEvDropReplicationResult, NKikimrReplication::TEvDropReplicationResult, EvDropReplicationResult>
    {};

    struct TEvDescribeReplication
        : public TEventPB<TEvDescribeReplication, NKikimrReplication::TEvDescribeReplication, EvDescribeReplication>
    {};

    struct TEvDescribeReplicationResult
        : public TEventPB<TEvDescribeReplicationResult, NKikimrReplication::TEvDescribeReplicationResult, EvDescribeReplicationResult>
    {};

}; // TEvController

}
