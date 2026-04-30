#pragma once

#include <ydb/core/base/events.h>
#include <ydb/core/protos/tx_datashard.pb.h>
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
        TEvFinished(ui64 txId, bool success = true, const TString& error = "",
                    NKikimrTxDataShard::TShardOpResult::EOpEndStatus endStatus =
                        NKikimrTxDataShard::TShardOpResult::END_UNSPECIFIED)
            : TxId(txId)
            , Success(success)
            , Error(error)
            , EndStatus(endStatus)
        {}
        ui64 TxId = 0;
        bool Success = true;
        TString Error;
        NKikimrTxDataShard::TShardOpResult::EOpEndStatus EndStatus =
            NKikimrTxDataShard::TShardOpResult::END_UNSPECIFIED;
    };
};

inline bool IsScanSuccess(NTable::EStatus status) {
    return status == NTable::EStatus::Done;
}

// DS-side classifier: maps a scan termination cause to the wire-level enum.
// SS owns the policy (see schemeshard_incremental_restore_classify.h).
inline NKikimrTxDataShard::TShardOpResult::EOpEndStatus MapScanStatus(NTable::EStatus s) {
    switch (s) {
        case NTable::EStatus::Done:
            return NKikimrTxDataShard::TShardOpResult::END_SUCCESS;
        case NTable::EStatus::Term:
            return NKikimrTxDataShard::TShardOpResult::END_TRANSIENT_FAILURE;
        case NTable::EStatus::Lost:
        case NTable::EStatus::StorageError:
        case NTable::EStatus::Exception:
            return NKikimrTxDataShard::TShardOpResult::END_FATAL_FAILURE;
        default:
            return NKikimrTxDataShard::TShardOpResult::END_UNSPECIFIED;
    }
}

THolder<NTable::IScan> CreateIncrementalRestoreScan(
        NActors::TActorId parent,
        std::function<TActorId(const TActorContext& ctx, TActorId parent)> changeSenderFactory,
        const TPathId& sourcePathId,
        TUserTable::TCPtr table,
        const TPathId& targetPathId,
        ui64 txId,
        NStreamScan::TLimits limits);

} // namespace NKikimr::NDataShard
