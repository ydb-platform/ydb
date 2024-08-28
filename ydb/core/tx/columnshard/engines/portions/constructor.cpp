#include "constructor.h"

#include <ydb/core/tx/columnshard/columnshard_schema.h>
#include <ydb/core/tx/columnshard/common/limits.h>
#include <ydb/core/tx/columnshard/engines/scheme/index_info.h>
#include <ydb/core/tx/columnshard/engines/scheme/versions/abstract_scheme.h>
#include <ydb/core/tx/columnshard/engines/scheme/versions/versioned_index.h>
#include <ydb/core/tx/columnshard/hooks/abstract/abstract.h>

namespace NKikimr::NOlap {

TPortionInfo TPortionInfoConstructor::Build(const bool needChunksNormalization) {
    TPortionInfo result(MetaConstructor.Build());
    AFL_VERIFY(PathId);
    result.PathId = PathId;
    result.Portion = GetPortionIdVerified();

    AFL_VERIFY(MinSnapshotDeprecated);
    AFL_VERIFY(MinSnapshotDeprecated->Valid());
    result.MinSnapshotDeprecated = *MinSnapshotDeprecated;
    if (RemoveSnapshot) {
        AFL_VERIFY(RemoveSnapshot->Valid());
        result.RemoveSnapshot = *RemoveSnapshot;
    }
    result.SchemaVersion = SchemaVersion;
    result.ShardingVersion = ShardingVersion;

    if (needChunksNormalization) {
        ReorderChunks();
    }
    NActors::TLogContextGuard lGuard = NActors::TLogContextBuilder::Build()("portion_id", GetPortionIdVerified());
    FullValidation();

    result.Indexes = Indexes;
    result.Records = Records;
    result.BlobIds = BlobIds;
    return result;
}

ISnapshotSchema::TPtr TPortionInfoConstructor::GetSchema(const TVersionedIndex& index) const {
    if (SchemaVersion) {
        auto schema = index.GetSchema(SchemaVersion.value());
        AFL_VERIFY(!!schema)("details", TStringBuilder() << "cannot find schema for version " << SchemaVersion.value());
        return schema;
    }
    AFL_VERIFY(MinSnapshotDeprecated);
    return index.GetSchema(*MinSnapshotDeprecated);
}

void TPortionInfoConstructor::LoadRecord(const TIndexInfo& indexInfo, const TColumnChunkLoadContext& loadContext) {
    TColumnRecord rec(RegisterBlobId(loadContext.GetBlobRange().GetBlobId()), loadContext, indexInfo.GetColumnFeaturesVerified(loadContext.GetAddress().GetColumnId()));
    Records.push_back(std::move(rec));

    if (loadContext.GetPortionMeta()) {
        AFL_VERIFY(MetaConstructor.LoadMetadata(*loadContext.GetPortionMeta(), indexInfo));
    }
}

void TPortionInfoConstructor::LoadIndex(const TIndexChunkLoadContext& loadContext) {
    if (loadContext.GetBlobRange()) {
        const TBlobRangeLink16::TLinkId linkBlobId = RegisterBlobId(loadContext.GetBlobRange()->GetBlobId());
        AddIndex(loadContext.BuildIndexChunk(linkBlobId));
    } else {
        AddIndex(loadContext.BuildIndexChunk());
    }
}

const NKikimr::NOlap::TColumnRecord& TPortionInfoConstructor::AppendOneChunkColumn(TColumnRecord&& record) {
    Y_ABORT_UNLESS(record.ColumnId);
    std::optional<ui32> maxChunk;
    for (auto&& i : Records) {
        if (i.ColumnId == record.ColumnId) {
            if (!maxChunk) {
                maxChunk = i.Chunk;
            } else {
                Y_ABORT_UNLESS(*maxChunk + 1 == i.Chunk);
                maxChunk = i.Chunk;
            }
        }
    }
    if (maxChunk) {
        AFL_VERIFY(*maxChunk + 1 == record.Chunk)("max", *maxChunk)("record", record.Chunk);
    } else {
        AFL_VERIFY(0 == record.Chunk)("record", record.Chunk);
    }
    Records.emplace_back(std::move(record));
    return Records.back();
}

void TPortionInfoConstructor::AddMetadata(const ISnapshotSchema& snapshotSchema, const std::shared_ptr<arrow::RecordBatch>& batch) {
    Y_ABORT_UNLESS(batch->num_rows() == GetRecordsCount());
    MetaConstructor.FillMetaInfo(NArrow::TFirstLastSpecialKeys(batch), IIndexInfo::CalcDeletions(batch, false),
        NArrow::TMinMaxSpecialKeys(batch, TIndexInfo::ArrowSchemaSnapshot()), snapshotSchema.GetIndexInfo());
}

}   // namespace NKikimr::NOlap
