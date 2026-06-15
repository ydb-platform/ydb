#pragma once

#include "helper.h"

#include <ydb/core/formats/arrow/arrow_helpers.h>
#include <ydb/core/tx/columnshard/engines/portions/constructor_portion.h>
#include <ydb/core/tx/columnshard/engines/protos/portion_info.pb.h>
#include <ydb/core/tx/columnshard/engines/scheme/index_info.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/array/builder_primitive.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/record_batch.h>

#include <optional>

namespace NKikimr::NOlap::NTest {

inline TIndexInfo MakePortionTestIndexInfo() {
    THashMap<ui32, NTable::TColumn> columns = { { 0, NTable::TColumn("pk", 0, NScheme::TTypeInfo(NScheme::NTypeIds::Uint64), "") } };
    std::vector<ui32> pkIds = { 0 };
    for (ui64 i = 0; i < pkIds.size(); ++i) {
        TValidator::CheckNotNull(columns.FindPtr(pkIds[i]))->KeyOrder = i;
    }
    return TIndexInfo::BuildDefault(1, TTestStoragesManager::GetInstance(), columns, pkIds);
}

inline std::shared_ptr<arrow::RecordBatch> MakePortionTestPKBatch(const ui64 startKey, const ui64 endKey) {
    auto schema = arrow::schema({ arrow::field("pk", arrow::uint64()) });
    arrow::UInt64Builder builder;
    AFL_VERIFY(builder.AppendValues({ startKey, endKey }).ok());
    auto array = builder.Finish().ValueOrDie();
    return arrow::RecordBatch::Make(schema, 2, { array });
}

inline std::shared_ptr<TPortionInfo> MakeTestCompactedPortion(const TInternalPathId pathId, const ui64 portionId, const ui64 startKey,
    const ui64 endKey, const ui32 recordsCount, const TSnapshot& appearanceSnapshot, const std::optional<TSnapshot>& removeSnapshot) {
    TString serialized = NArrow::SerializeBatchNoCompression(MakePortionTestPKBatch(startKey, endKey));
    TIndexInfo indexInfo = MakePortionTestIndexInfo();

    NKikimrTxColumnShard::TIndexPortionMeta metaProto;
    metaProto.SetIsCompacted(true);
    metaProto.SetPrimaryKeyBorders(serialized);
    metaProto.MutableRecordSnapshotMin()->SetPlanStep(appearanceSnapshot.GetPlanStep());
    metaProto.MutableRecordSnapshotMin()->SetTxId(appearanceSnapshot.GetTxId());
    metaProto.MutableRecordSnapshotMax()->SetPlanStep(appearanceSnapshot.GetPlanStep());
    metaProto.MutableRecordSnapshotMax()->SetTxId(appearanceSnapshot.GetTxId());
    metaProto.SetDeletionsCount(0);
    metaProto.SetCompactionLevel(0);
    metaProto.SetRecordsCount(recordsCount);
    metaProto.SetColumnRawBytes(100);
    metaProto.SetColumnBlobBytes(100);
    metaProto.SetIndexRawBytes(0);
    metaProto.SetIndexBlobBytes(0);
    metaProto.SetNumSlices(1);
    metaProto.MutableCompactedPortion()->MutableAppearanceSnapshot()->SetPlanStep(appearanceSnapshot.GetPlanStep());
    metaProto.MutableCompactedPortion()->MutableAppearanceSnapshot()->SetTxId(appearanceSnapshot.GetTxId());

    TPortionMetaConstructor metaConstructor;
    TFakeGroupSelector groupSelector;
    AFL_VERIFY(metaConstructor.LoadMetadata(metaProto, indexInfo, groupSelector));

    TCompactedPortionInfoConstructor constructor(pathId, portionId);
    constructor.SetSchemaVersion(1);
    constructor.SetAppearanceSnapshot(appearanceSnapshot);
    constructor.MutableMeta() = metaConstructor;
    if (removeSnapshot) {
        constructor.SetRemoveSnapshot(*removeSnapshot);
    }

    return constructor.Build();
}

}   // namespace NKikimr::NOlap::NTest
