#pragma once

#include <ydb/core/base/events.h>
#include <ydb/core/scheme/scheme_pathid.h>
#include <ydb/core/tablet_flat/flat_scan_iface.h>
#include <ydb/core/tx/datashard/datashard_user_table.h>
#include <ydb/core/tx/datashard/stream_scan_common.h>
#include <ydb/library/actors/core/actor.h>

#include <functional>

namespace NKikimr::NDataShard {

using namespace NActors;

struct TEvIncrementalRestoreScan {
    enum EEv {
        EvNoMoreData = EventSpaceBegin(TKikimrEvents::ES_INCREMENTAL_RESTORE_SCAN),
        EvFinished,

        EvEnd,
    };

    static_assert(EvEnd < EventSpaceEnd(TKikimrEvents::ES_INCREMENTAL_RESTORE_SCAN));

    struct TEvNoMoreData: public TEventLocal<TEvNoMoreData, EvNoMoreData> {};
    struct TEvFinished: public TEventLocal<TEvFinished, EvFinished> {};
};

THolder<NTable::IScan> CreateIncrementalRestoreScan(
        NActors::TActorId parent,
        std::function<TActorId(const TActorContext& ctx)> changeSenderFactory,
        const TPathId& sourcePathId,
        TUserTable::TCPtr table,
        const TPathId& targetPathId,
        ui64 txId,
        NStreamScan::TLimits limits);

} // namespace NKikimr::NDataShard
