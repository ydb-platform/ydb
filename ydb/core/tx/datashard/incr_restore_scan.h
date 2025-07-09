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
        EvServe = EventSpaceBegin(TKikimrEvents::ES_INCREMENTAL_RESTORE_SCAN),
        EvNoMoreData,
        EvFinished,

        EvEnd,
    };

    static_assert(EvEnd < EventSpaceEnd(TKikimrEvents::ES_INCREMENTAL_RESTORE_SCAN));

    struct TEvServe: public TEventLocal<TEvServe, EvServe> {};
    struct TEvNoMoreData: public TEventLocal<TEvNoMoreData, EvNoMoreData> {};
    struct TEvFinished: public TEventLocal<TEvFinished, EvFinished> {
        TEvFinished() = default;
        TEvFinished(ui64 txId) : TxId(txId) {}
        ui64 TxId;
    };
};

THolder<NTable::IScan> CreateIncrementalRestoreScan(
        NActors::TActorId parent,
        std::function<TActorId(const TActorContext& ctx, TActorId parent)> changeSenderFactory,
        const TPathId& sourcePathId,
        TUserTable::TCPtr table,
        const TPathId& targetPathId,
        ui64 txId,
        ui64 schemeShardTabletId,
        NStreamScan::TLimits limits);

} // namespace NKikimr::NDataShard
