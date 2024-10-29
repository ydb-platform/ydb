#include "constructor.h"

#include <ydb/core/tx/columnshard/columnshard_schema.h>
#include <ydb/core/tx/columnshard/common/limits.h>
#include <ydb/core/tx/columnshard/engines/scheme/index_info.h>
#include <ydb/core/tx/columnshard/engines/scheme/versions/abstract_scheme.h>
#include <ydb/core/tx/columnshard/engines/scheme/versions/versioned_index.h>
#include <ydb/core/tx/columnshard/hooks/abstract/abstract.h>

namespace NKikimr::NOlap {

TPortionInfo TPortionInfoConstructor::Build(const bool needChunksNormalization) {
    AFL_VERIFY(!Constructed);
    Constructed = true;
    TPortionInfo result(MetaConstructor.Build());
    AFL_VERIFY(PathId);
    result.PathId = PathId;
    result.PortionId = GetPortionIdVerified();

    AFL_VERIFY(MinSnapshotDeprecated);
    AFL_VERIFY(MinSnapshotDeprecated->Valid());
    result.MinSnapshotDeprecated = *MinSnapshotDeprecated;
    if (RemoveSnapshot) {
        AFL_VERIFY(RemoveSnapshot->Valid());
        result.RemoveSnapshot = *RemoveSnapshot;
    }
    result.SchemaVersion = SchemaVersion;
    result.ShardingVersion = ShardingVersion;
    result.CommitSnapshot = CommitSnapshot;
    result.InsertWriteId = InsertWriteId;
    AFL_VERIFY(!CommitSnapshot || !!InsertWriteId);

    if (result.GetMeta().GetProduced() == NPortion::EProduced::INSERTED) {
//        AFL_VERIFY(!!InsertWriteId);
    } else {
        AFL_VERIFY(!CommitSnapshot);
        AFL_VERIFY(!InsertWriteId);
    }

    if (needChunksNormalization) {
        ReorderChunks();
    }
    NActors::TLogContextGuard lGuard = NActors::TLogContextBuilder::Build()("portion_id", GetPortionIdVerified());
    FullValidation();

    if (BlobIdxs.size()) {
        auto itRecord = Records.begin();
        auto itIndex = Indexes.begin();
        auto itBlobIdx = BlobIdxs.begin();
        while (itRecord != Records.end() && itIndex != Indexes.end() && itBlobIdx != BlobIdxs.end()) {
            if (itRecord->GetAddress() < itIndex->GetAddress()) {
                AFL_VERIFY(itRecord->GetAddress() == itBlobIdx->GetAddress());
                itRecord->RegisterBlobIdx(itBlobIdx->GetBlobIdx());
                ++itRecord;
                ++itBlobIdx;
            } else if (itIndex->GetAddress() < itRecord->GetAddress()) {
                if (itIndex->HasBlobData()) {
                    ++itIndex;
                    continue;
                }
                AFL_VERIFY(itIndex->GetAddress() == itBlobIdx->GetAddress());
                itIndex->RegisterBlobIdx(itBlobIdx->GetBlobIdx());
                ++itIndex;
                ++itBlobIdx;
            } else {
                AFL_VERIFY(false);
            }
        }
        for (; itRecord != Records.end() && itBlobIdx != BlobIdxs.end(); ++itRecord, ++itBlobIdx) {
            AFL_VERIFY(itRecord->GetAddress() == itBlobIdx->GetAddress());
            itRecord->RegisterBlobIdx(itBlobIdx->GetBlobIdx());
        }
        for (; itIndex != Indexes.end() && itBlobIdx != BlobIdxs.end(); ++itIndex) {
            if (itIndex->HasBlobData()) {
                continue;
            }
            AFL_VERIFY(itIndex->GetAddress() == itBlobIdx->GetAddress());
            itIndex->RegisterBlobIdx(itBlobIdx->GetBlobIdx());
            ++itBlobIdx;
        }
        AFL_VERIFY(itRecord == Records.end());
        AFL_VERIFY(itBlobIdx == BlobIdxs.end());
    }

    result.Indexes = std::move(Indexes);
    result.Indexes.shrink_to_fit();
    result.Records = std::move(Records);
    result.Records.shrink_to_fit();
    result.BlobIds = std::move(BlobIds);
    result.BlobIds.shrink_to_fit();
    result.Precalculate();
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
    TColumnRecord rec(RegisterBlobId(loadContext.GetBlobRange().GetBlobId()), loadContext);
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
    Records.emplace_back(std::move(record));
    return Records.back();
}

void TPortionInfoConstructor::AddMetadata(const ISnapshotSchema& snapshotSchema, const std::shared_ptr<arrow::RecordBatch>& batch) {
    Y_ABORT_UNLESS(batch->num_rows() == GetRecordsCount());
    MetaConstructor.FillMetaInfo(NArrow::TFirstLastSpecialKeys(batch), IIndexInfo::CalcDeletions(batch, false),
        NArrow::TMinMaxSpecialKeys(batch, TIndexInfo::ArrowSchemaSnapshot()), snapshotSchema.GetIndexInfo());
}

}   // namespace NKikimr::NOlap
