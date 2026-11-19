#pragma once

#include <ydb/core/backup/query/table_checksum_snapshot.h>
#include <ydb/core/backup/query/table_checksum.h>
#include <ydb/core/tx/datashard/datashard.h>

#include <ydb/core/base/appdata.h>
#include <ydb/core/testlib/actors/test_runtime.h>
#include <ydb/public/api/protos/ydb_value.pb.h>

namespace NSchemeShardUT_Private {

inline NKikimr::NBackup::TTableChecksum MakeTableChecksum(
    ui64 count = 0, const TString& hash = "0000000000000000", const TString& algorithm = "CityHash64") {
    return { .Count = count, .Hash = hash, .Algorithm = algorithm };
}

inline void FillTableChecksumResult(Ydb::ResultSet& result, const NKikimr::NBackup::TTableChecksum& checksum) {
    const std::pair<const char*, Ydb::Type::PrimitiveTypeId> columns[] = {
        { "version", Ydb::Type::UINT32 },
        { "algorithm", Ydb::Type::UTF8 },
        { "count", Ydb::Type::UINT64 },
        { "hash", Ydb::Type::STRING },
    };
    for (const auto& [name, type] : columns) {
        auto* column = result.add_columns();
        column->set_name(name);
        column->mutable_type()->set_type_id(type);
    }
    auto* row = result.add_rows();
    row->add_items()->set_uint32_value(NKikimr::NBackup::TTableChecksum::Version);
    row->add_items()->set_text_value(checksum.Algorithm);
    row->add_items()->set_uint64_value(checksum.Count);
    row->add_items()->set_bytes_value(checksum.Hash);
}

inline ui64 CountTableChecksumSnapshotRows(NActors::TTestActorRuntime& runtime, const NKikimr::TTableId& table,
    const TVector<ui64>& shards, NKikimr::TRowVersion snapshot) {
    ui64 rows = 0;
    for (ui64 shard : shards) {
        const auto edge = runtime.AllocateEdgeActor();
        auto request = MakeHolder<NKikimr::TEvDataShard::TEvRead>();
        request->Record.SetReadId(1);
        request->Record.MutableTableId()->SetOwnerId(table.PathId.OwnerId);
        request->Record.MutableTableId()->SetTableId(table.PathId.LocalPathId);
        if (!snapshot.IsMax()) {
            snapshot.ToProto(request->Record.MutableSnapshot());
        }
        request->Record.AddColumns(1);
        request->Record.SetResultFormat(NKikimrDataEvents::FORMAT_CELLVEC);
        request->Ranges.emplace_back(NKikimr::TOwnedCellVec(), true, NKikimr::TOwnedCellVec(), true);
        runtime.SendToPipe(shard, edge, request.Release());
        auto result = runtime.GrabEdgeEventRethrow<NKikimr::TEvDataShard::TEvReadResult>(edge);
        UNIT_ASSERT_VALUES_EQUAL_C(
            result->Get()->Record.GetStatus().GetCode(), Ydb::StatusIds::SUCCESS, result->Get()->Record.DebugString());
        UNIT_ASSERT(result->Get()->Record.GetFinished());
        rows += result->Get()->GetRowsCount();
    }
    return rows;
}

inline NKikimr::NBackup::TEvTableChecksum::TEvSnapshotConfirmed::TPtr ConfirmTableChecksumSnapshot(
    NActors::TTestActorRuntime& runtime, const NKikimr::TTableId& table, ui64 schemeShardId,
    NKikimr::TRowVersion snapshot) {
    const auto edge = runtime.AllocateEdgeActor();
    runtime.Register(NKikimr::NBackup::CreateTableChecksumSnapshotReader(edge, table, schemeShardId, snapshot));
    return runtime.GrabEdgeEventRethrow<NKikimr::NBackup::TEvTableChecksum::TEvSnapshotConfirmed>(edge);
}

} // namespace NSchemeShardUT_Private
