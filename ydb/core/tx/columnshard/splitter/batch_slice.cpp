#include "batch_slice.h"

#include <ydb/library/accessor/validator.h>
#include <ydb/library/formats/arrow/splitter/similar_packer.h>

#include <util/string/join.h>

namespace NKikimr::NOlap {

bool TGeneralSerializedSlice::GroupBlobsImpl(const NSplitter::TGroupFeatures& features, std::vector<TSplittedBlob>& blobs) {
    AFL_VERIFY(features.GetSplitSettings().GetMaxBlobSize() >= 2 * features.GetSplitSettings().GetMinBlobSize())(
                                        "event", "we need be sure that 2 * small < big");
    const auto pred = [](const TSplittedEntity* l, const TSplittedEntity* r) {
        return r->GetSize() < l->GetSize();
    };
    std::vector<TSplittedEntity*> dataPtr;
    for (auto&& i : Data) {
        if (!features.Contains(i.GetEntityId())) {
            continue;
        }
        dataPtr.emplace_back(&i);
    }
    std::sort(dataPtr.begin(), dataPtr.end(), pred);
    ui32 count = 0;
    THashMap<ui32, TSplittedEntity::TNormalizedBlobChunks> chunksPreparation;
    for (auto&& i : dataPtr) {
        count += i->GetChunks().size() * 2;
        TSplittedEntity::TNormalizedBlobChunks normalizedChunks(features.GetSplitSettings().GetMinBlobSize(),
            features.GetSplitSettings().GetMaxBlobSize(), features.GetSplitSettings().GetBlobSizeTolerance(), Schema, Counters, InternalSplitsCount);
        normalizedChunks.Reserve(i->GetChunks().size() * 2);
        auto chunksInit = i->BuildBlobChunks(features.GetSplitSettings().GetMaxBlobSize(), Schema, Counters, InternalSplitsCount);
        for (auto&& c : chunksInit) {
            normalizedChunks.AddChunk(std::move(c));
        }
        chunksPreparation.emplace(i->GetEntityId(), normalizedChunks.Normalize());
    }
    TSplittedEntity::TNormalizedBlobChunks result(features.GetSplitSettings().GetMinBlobSize(), features.GetSplitSettings().GetMaxBlobSize(),
        features.GetSplitSettings().GetBlobSizeTolerance(), Schema, Counters, InternalSplitsCount);
    result.Reserve(count);
    for (auto&& i : chunksPreparation) {
        result.Merge(std::move(i.second));
    }
    result = result.Normalize();
    result.ForceMergeSmall();
    blobs = result.Finish(features.GetName());
    return true;
}

TGeneralSerializedSlice::TGeneralSerializedSlice(const THashMap<ui32, std::vector<std::shared_ptr<IPortionDataChunk>>>& data,
    NArrow::NSplitter::ISchemaDetailInfo::TPtr schema, std::shared_ptr<NColumnShard::TSplitterCounters> counters)
    : Schema(schema)
    , Counters(counters) {
    std::optional<ui32> recordsCount;
    for (auto&& [entityId, chunks] : data) {
        TSplittedEntity entity(entityId);
        entity.SetChunks(chunks);
        if (!!entity.GetRecordsCount()) {
            if (!recordsCount) {
                recordsCount = entity.GetRecordsCount();
            } else {
                AFL_VERIFY(*recordsCount == entity.GetRecordsCount())("records_count", *recordsCount)("column", entity.GetRecordsCount())(
                                              "chunks", chunks.size());
            }
        }
        Size += entity.GetSize();
        Data.emplace_back(std::move(entity));
    }
    Y_ABORT_UNLESS(recordsCount);
    RecordsCount = *recordsCount;
}

TGeneralSerializedSlice::TGeneralSerializedSlice(
    const ui32 recordsCount, NArrow::NSplitter::ISchemaDetailInfo::TPtr schema, std::shared_ptr<NColumnShard::TSplitterCounters> counters)
    : RecordsCount(recordsCount)
    , Schema(schema)
    , Counters(counters) {
}

void TGeneralSerializedSlice::MergeSlice(TGeneralSerializedSlice&& slice) {
    Y_ABORT_UNLESS(Data.size() == slice.Data.size());
    RecordsCount += slice.GetRecordsCount();
    for (ui32 i = 0; i < Data.size(); ++i) {
        Size += slice.Data[i].GetSize();
        Data[i].Merge(std::move(slice.Data[i]));
    }
}

bool TGeneralSerializedSlice::GroupBlobs(std::vector<TSplittedBlob>& blobs, const NSplitter::TEntityGroups& groups) {
    if (groups.IsEmpty()) {
        return GroupBlobsImpl(groups.GetDefaultGroupFeatures(), blobs);
    } else {
        std::vector<TSplittedBlob> result;
        for (auto&& i : groups) {
            std::vector<TSplittedBlob> blobsLocal;
            if (!GroupBlobsImpl(i.second, blobsLocal)) {
                return false;
            }
            result.insert(result.end(), blobsLocal.begin(), blobsLocal.end());
        }
        std::swap(result, blobs);
        return true;
    }
}

}   // namespace NKikimr::NOlap
