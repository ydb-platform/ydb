#pragma once

#include "../datashard_user_table.h"
#include "../scan_common.h"
#include <ydb/core/protos/tx_datashard.pb.h>
#include <ydb/core/tablet_flat/flat_scan_iface.h>

namespace NKikimr::NDataShard {

// Column IDs are the binding. Key inputs are read from the scan key, and value
// inputs from the minimal projection, in the descriptor's dependency order.
struct TGeneratedColumnsProjection {
    struct TInput {
        bool Key;
        ui32 Position;
    };
    TTags Tags;
    TVector<TInput> Inputs;
    ui32 TargetPosition = 0;

    TVector<TCell> GetInputs(TConstArrayRef<TCell> key, const NTable::TRowState& row) const;
};

TGeneratedColumnsProjection BuildGeneratedColumnsProjection(
    const TUserTable& table, const NKikimrIndexBuilder::TGeneratedColumnBuild& descriptor, ui32 targetColumnId);

bool ValidateGeneratedColumnsBuild(const NKikimrTxDataShard::TEvBuildIndexCreateRequest& request,
    const TUserTable& table, NKikimrTxDataShard::TGeneratedColumnFillContext& context, TString& error);

TAutoPtr<NTable::IScan> CreateGeneratedColumnsScan(ui64 buildIndexId,
    const TString& databaseName, const TString& target, const TScanRecord::TSeqNo& seqNo,
    ui64 dataShardId, const TActorId& progressActorId, const TSerializedTableRange& range,
    const NKikimrIndexBuilder::TGeneratedColumnBuild& descriptor,
    const NKikimrTxDataShard::TGeneratedColumnFillContext& fillContext,
    const TUserTable& tableInfo, const TIndexBuildScanSettings& scanSettings);

} // namespace NKikimr::NDataShard
