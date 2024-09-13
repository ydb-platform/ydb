#pragma once

#include <ydb/core/scheme/scheme_pathid.h>
#include <ydb/core/tablet_flat/flat_scan_iface.h>
#include <ydb/library/actors/core/actor.h>
#include <ydb/core/base/events.h>

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
        std::function<NActors::IActor*()> changeSenderFactory,
        TPathId tablePathId,
        const TPathId& targetPathId,
        ui64 txId);

} // namespace NKikimr::NDataShard
