#pragma once

#include <ydb/core/base/events.h>
#include <ydb/core/tx/columnshard/columnshard_private_events.h>
#include <ydb/core/tx/columnshard/flow_control_manager/flow_control_manager_types.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>

#include <ydb/library/aclib/user_context.h>
#include <ydb/library/actors/core/actor.h>

namespace NKikimr::NColumnShard::NFlowControl {

enum EEvFlowControl {
    EvLongTxWrite = EventSpaceBegin(TKikimrEvents::ES_FLOW_CONTROL_MANAGER),

    EvEnd
};

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

}   // namespace NKikimr::NColumnShard::NFlowControl
