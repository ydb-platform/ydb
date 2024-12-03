#include "constructor_portion.h"

#include <ydb/core/tx/columnshard/columnshard_schema.h>
#include <ydb/core/tx/columnshard/common/limits.h>
#include <ydb/core/tx/columnshard/engines/scheme/index_info.h>
#include <ydb/core/tx/columnshard/engines/scheme/versions/abstract_scheme.h>
#include <ydb/core/tx/columnshard/engines/scheme/versions/versioned_index.h>
#include <ydb/core/tx/columnshard/hooks/abstract/abstract.h>

namespace NKikimr::NOlap {

std::shared_ptr<TPortionInfo> TPortionInfoConstructor::Build() {
    AFL_VERIFY(!Constructed);
    Constructed = true;

    std::shared_ptr<TPortionInfo> result(new TPortionInfo(MetaConstructor.Build()));
    AFL_VERIFY(PathId);
    result->PathId = PathId;
    result->PortionId = GetPortionIdVerified();

    AFL_VERIFY(MinSnapshotDeprecated);
    AFL_VERIFY(MinSnapshotDeprecated->Valid());
    result->MinSnapshotDeprecated = *MinSnapshotDeprecated;
    if (RemoveSnapshot) {
        AFL_VERIFY(RemoveSnapshot->Valid());
        result->RemoveSnapshot = *RemoveSnapshot;
    }
    result->SchemaVersion = SchemaVersion;
    result->ShardingVersion = ShardingVersion;
    result->CommitSnapshot = CommitSnapshot;
    result->InsertWriteId = InsertWriteId;
    AFL_VERIFY(!CommitSnapshot || !!InsertWriteId);

    if (result->GetMeta().GetProduced() == NPortion::EProduced::INSERTED) {
//        AFL_VERIFY(!!InsertWriteId);
    } else {
        AFL_VERIFY(!CommitSnapshot);
        AFL_VERIFY(!InsertWriteId);
    }

    return result;
}

ISnapshotSchema::TPtr TPortionInfoConstructor::GetSchema(const TVersionedIndex& index) const {
    if (SchemaVersion) {
        auto schema = index.GetSchemaVerified(SchemaVersion.value());
        AFL_VERIFY(!!schema)("details", TStringBuilder() << "cannot find schema for version " << SchemaVersion.value());
        return schema;
    } else {
        AFL_VERIFY(MinSnapshotDeprecated);
        return index.GetSchemaVerified(*MinSnapshotDeprecated);
    }
}

void TPortionInfoConstructor::AddMetadata(const ISnapshotSchema& snapshotSchema, const std::shared_ptr<arrow::RecordBatch>& batch) {
    MetaConstructor.FillMetaInfo(NArrow::TFirstLastSpecialKeys(batch), IIndexInfo::CalcDeletions(batch, false),
        NArrow::TMinMaxSpecialKeys(batch, TIndexInfo::ArrowSchemaSnapshot()), snapshotSchema.GetIndexInfo());
}

}   // namespace NKikimr::NOlap
