#pragma once

#include <ydb/core/base/events.h>
#include <ydb/core/protos/tx_columnshard.pb.h>
#include <ydb/core/tx/columnshard/columnshard_private_events.h>
#include <ydb/core/tx/columnshard/flow_control_manager/flow_control_manager_types.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>

#include <ydb/library/aclib/user_context.h>
#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/event_pb.h>

namespace NKikimr::NColumnShard::NFlowControl {

enum EEvFlowControl {
    EvLongTxWrite = EventSpaceBegin(TKikimrEvents::ES_FLOW_CONTROL_MANAGER),
    EvNodeOverloadStatus,
    EvTabletLocationUpdated,
    EvTabletLocationInvalidated,

    EvEnd
};

static_assert(EvEnd < EventSpaceEnd(TKikimrEvents::ES_FLOW_CONTROL_MANAGER), "expect EvEnd < EventSpaceEnd(ES_FLOW_CONTROL_MANAGER)");

class TEvLongTxWrite: public NActors::TEventLocal<TEvLongTxWrite, EvLongTxWrite> {
private:
    std::optional<TLongTxWrite> LongTxWrite;

public:
    explicit TEvLongTxWrite(TLongTxWrite&& longTxWrite)
        : LongTxWrite(std::move(longTxWrite))
    {
    }

    TLongTxWrite DetachLongTxWrite() {
        Y_ABORT_UNLESS(LongTxWrite.has_value(), "LongTxWrite already detached");
        TLongTxWrite result = std::move(*LongTxWrite);
        LongTxWrite.reset();
        return result;
    }
};

struct TEvNodeOverloadStatus
    : public NActors::TEventPB<TEvNodeOverloadStatus, NKikimrTxColumnShard::TEvNodeOverloadStatus, EvNodeOverloadStatus> {
    TEvNodeOverloadStatus() = default;

    TEvNodeOverloadStatus(ui32 nodeId, NKikimrTxColumnShard::TEvNodeOverloadStatus::EStatus status, ui64 generation) {
        Record.SetNodeId(nodeId);
        Record.SetStatus(status);
        Record.SetGeneration(generation);
    }
};

class TEvTabletLocationUpdated: public NActors::TEventLocal<TEvTabletLocationUpdated, EvTabletLocationUpdated> {
    YDB_READONLY(ui64, TabletId, 0);
    YDB_READONLY(ui32, NodeId, 0);

public:
    TEvTabletLocationUpdated(ui64 tabletId, ui32 nodeId)
        : TabletId(tabletId)
        , NodeId(nodeId)
    {
    }
};

class TEvTabletLocationInvalidated: public NActors::TEventLocal<TEvTabletLocationInvalidated, EvTabletLocationInvalidated> {
    YDB_READONLY(ui64, TabletId, 0);

public:
    explicit TEvTabletLocationInvalidated(ui64 tabletId)
        : TabletId(tabletId)
    {
    }
};

}   // namespace NKikimr::NColumnShard::NFlowControl
