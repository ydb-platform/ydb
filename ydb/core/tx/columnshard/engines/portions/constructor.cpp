#include "constructor.h"

#include <ydb/core/tx/columnshard/columnshard_schema.h>
#include <ydb/core/tx/columnshard/common/limits.h>
#include <ydb/core/tx/columnshard/engines/scheme/index_info.h>
#include <ydb/core/tx/columnshard/engines/scheme/versions/abstract_scheme.h>
#include <ydb/core/tx/columnshard/engines/scheme/versions/versioned_index.h>
#include <ydb/core/tx/columnshard/hooks/abstract/abstract.h>

namespace NKikimr::NOlap {

TPortionDataAccessor TPortionInfoConstructor::Build(const bool needChunksNormalization) {
    AFL_VERIFY(!Constructed);
    Constructed = true;

    MetaConstructor.ColumnRawBytes = 0;
    MetaConstructor.ColumnBlobBytes = 0;
    MetaConstructor.IndexRawBytes = 0;
    MetaConstructor.IndexBlobBytes = 0;

    MetaConstructor.RecordsCount = GetRecordsCount();
    for (auto&& r : Records) {
        *MetaConstructor.ColumnRawBytes += r.GetMeta().GetRawBytes();
        *MetaConstructor.ColumnBlobBytes += r.GetBlobRange().GetSize();
    }
    for (auto&& r : Indexes) {
        *MetaConstructor.IndexRawBytes += r.GetRawBytes();
        *MetaConstructor.IndexBlobBytes += r.GetDataSize();
    }

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

    if (needChunksNormalization) {
        ReorderChunks();
    }
    NActors::TLogContextGuard lGuard = NActors::TLogContextBuilder::Build()("portion_id", GetPortionIdVerified());
    if (MetaConstructor.BlobIdxs.size()) {
        auto itRecord = Records.begin();
        auto itIndex = Indexes.begin();
        auto itBlobIdx = MetaConstructor.BlobIdxs.begin();
        while (itRecord != Records.end() && itIndex != Indexes.end() && itBlobIdx != MetaConstructor.BlobIdxs.end()) {
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
        for (; itRecord != Records.end() && itBlobIdx != MetaConstructor.BlobIdxs.end(); ++itRecord, ++itBlobIdx) {
            AFL_VERIFY(itRecord->GetAddress() == itBlobIdx->GetAddress());
            itRecord->RegisterBlobIdx(itBlobIdx->GetBlobIdx());
        }
        for (; itIndex != Indexes.end() && itBlobIdx != MetaConstructor.BlobIdxs.end(); ++itIndex) {
            if (itIndex->HasBlobData()) {
                continue;
            }
            AFL_VERIFY(itIndex->GetAddress() == itBlobIdx->GetAddress());
            itIndex->RegisterBlobIdx(itBlobIdx->GetBlobIdx());
            ++itBlobIdx;
        }
        AFL_VERIFY(itRecord == Records.end());
        AFL_VERIFY(itBlobIdx == MetaConstructor.BlobIdxs.end());
    } else {
        for (auto&& i : Records) {
            AFL_VERIFY(i.BlobRange.GetBlobIdxVerified() < MetaConstructor.BlobIds.size());
        }
        for (auto&& i : Indexes) {
            if (auto* blobId = i.GetBlobRangeOptional()) {
                AFL_VERIFY(blobId->GetBlobIdxVerified() < MetaConstructor.BlobIds.size());
            }
        }
    }
    FullValidation();

    result->Indexes = std::move(Indexes);
    result->Indexes.shrink_to_fit();
    result->Records = std::move(Records);
    result->Records.shrink_to_fit();
    return TPortionDataAccessor(result);
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

void TPortionInfoConstructor::LoadRecord(const TColumnChunkLoadContextV1& loadContext) {
    AFL_VERIFY(loadContext.GetBlobRange().GetBlobIdxVerified() < MetaConstructor.BlobIds.size());
    AFL_VERIFY(loadContext.GetBlobRange().CheckBlob(MetaConstructor.BlobIds[loadContext.GetBlobRange().GetBlobIdxVerified()]))(
        "blobs", JoinSeq(",", MetaConstructor.BlobIds))("range", loadContext.GetBlobRange().ToString());
    TColumnRecord rec(loadContext);
    Records.push_back(std::move(rec));
}

void TPortionInfoConstructor::LoadIndex(const TIndexChunkLoadContext& loadContext) {
    if (loadContext.GetBlobRange()) {
        const TBlobRangeLink16::TLinkId linkBlobId = RegisterBlobId(loadContext.GetBlobRange()->GetBlobId());
        AddIndex(loadContext.BuildIndexChunk(linkBlobId));
    } else {
        AddIndex(loadContext.BuildIndexChunk());
    }
}

const TColumnRecord& TPortionInfoConstructor::AppendOneChunkColumn(TColumnRecord&& record) {
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
