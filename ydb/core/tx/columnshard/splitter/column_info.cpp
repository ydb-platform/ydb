#include "blob_info.h"
#include "column_info.h"

namespace NKikimr::NOlap {

std::vector<TSplittedBlob> TSplittedEntity::TNormalizedBlobChunks::Finish(const TString& groupName) {
    AFL_VERIFY(!Finished);
    AFL_VERIFY(Small.empty());
    Finished = true;
    std::vector<TSplittedBlob> result;
    for (auto&& i : Normal) {
        std::vector<std::shared_ptr<IPortionDataChunk>> blobChunks;
        for (auto&& [_, chunks] : i.GetEntities()) {
            for (auto&& c : chunks.GetChunks()) {
                blobChunks.emplace_back(c);
            }
        }
        TSplittedBlob sb(groupName, std::move(blobChunks));
        AFL_VERIFY(sb.GetSize() < MaxSize);
        AFL_VERIFY(sb.GetSize() >= MinSize || Normal.size() == 1)("size", sb.GetSize())("min", MinSize)("size", Normal.size());
        result.emplace_back(std::move(sb));
    }
    return result;
}

bool TSplittedEntity::TBlobChunk::TakeEntityPartFrom(TBlobChunk& sourceNormal, const ui32 minSize, const ui32 maxSize, const ui32 tolerance,
    const NArrow::NSplitter::ISchemaDetailInfo::TPtr& schema, const std::shared_ptr<NColumnShard::TSplitterCounters>& counters,
    ui32& internalSplitsCount) {
    AFL_VERIFY(sourceNormal.GetSize() + GetSize() >= maxSize);
    std::vector<TEntityChunk*> chunks;
    for (auto&& i : sourceNormal.Entities) {
        chunks.emplace_back(&i.second);
    }
    const auto pred = [](const TEntityChunk* l, const TEntityChunk* r) {
        return l->GetSize() < r->GetSize();
    };
    std::sort(chunks.begin(), chunks.end(), pred);
    for (auto&& i : chunks) {
        AFL_VERIFY(GetSize() + i->GetSize() >= maxSize || sourceNormal.GetSize() - i->GetSize() < minSize);
        {
            const auto chunks = i->GetChunks();
            for (auto&& c : chunks) {
                const ui64 cSize = c->GetPackedSize();
                if (GetSize() + cSize < maxSize && sourceNormal.GetSize() >= minSize + cSize) {
                    AddChunk(i->DetachEntityChunkVerified(c));
                    if (GetSize() + tolerance >= minSize) {
                        return true;
                    }
                }
            }
        }
        AFL_VERIFY(GetSize() + tolerance < minSize);
        AFL_VERIFY(sourceNormal.GetSize() >= minSize);
        {
            const auto chunks = i->GetChunks();
            for (auto&& c : chunks) {
                if (!c->IsSplittable()) {
                    continue;
                }
                const ui64 cSize = c->GetPackedSize();
                const ui64 deltaSource = sourceNormal.GetSize() - minSize;
                const ui64 deltaSelf = minSize - GetSize();
                AFL_VERIFY(deltaSelf <= deltaSource);
                AFL_VERIFY(deltaSelf <= cSize);
                std::vector<std::shared_ptr<IPortionDataChunk>> chunks =
                    i->SplitChunk(c, deltaSelf * 1.1, schema->GetColumnSaver(i->GetEntityId()), counters);
                ++internalSplitsCount;
                AFL_VERIFY(chunks.size() == 2);
                AddChunk(i->DetachEntityChunkVerified(chunks.front()));
                return true;
            }
        }
    }
    return false;
}

}   // namespace NKikimr::NOlap
