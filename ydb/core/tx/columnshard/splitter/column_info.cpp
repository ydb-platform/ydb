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
                Counters->BySizeSplitter.OnCorrectSerialized(c->GetPackedSize());
                blobChunks.emplace_back(c);
            }
        }
        TSplittedBlob sb(groupName, std::move(blobChunks));
        AFL_VERIFY(sb.GetSize() < MaxSize);
        if (sb.GetSize() + Tolerance < MinSize) {
            if (Normal.size() != 1) {
                Counters->BySizeSplitter.OnSmallSerialized(sb.GetSize());
            }
            AFL_VERIFY_DEBUG(Normal.size() == 1)("size", sb.GetSize())("min", MinSize)("size", Normal.size());
        }
        result.emplace_back(std::move(sb));
    }
    return result;
}

bool TSplittedEntity::TBlobChunk::TakeEntityPartFrom(TBlobChunk& sourceNormal, const ui32 minSize, const ui32 maxSize,
    const NArrow::NSplitter::ISchemaDetailInfo::TPtr& schema, const std::shared_ptr<NColumnShard::TSplitterCounters>& counters,
    ui32& internalSplitsCount) {
    AFL_VERIFY(sourceNormal.GetSize() + GetSize() >= maxSize);
    std::vector<TEntityChunk*> eChunks;
    for (auto&& i : sourceNormal.Entities) {
        eChunks.emplace_back(&i.second);
    }
    const auto pred = [](const TEntityChunk* l, const TEntityChunk* r) {
        return l->GetSize() < r->GetSize();
    };
    std::sort(eChunks.begin(), eChunks.end(), pred);
    for (auto&& i : eChunks) {
        AFL_VERIFY(GetSize() + i->GetSize() >= maxSize || sourceNormal.GetSize() - i->GetSize() < minSize);
        {
            const auto chunks = i->GetChunks();
            for (auto&& c : chunks) {
                const ui64 cSize = c->GetPackedSize();
                if (GetSize() + cSize < maxSize && sourceNormal.GetSize() - cSize >= minSize) {
                    AddChunk(i->DetachEntityChunkVerified(c));
                    if (GetSize() >= minSize) {
                        return true;
                    }
                }
            }
        }
        AFL_VERIFY(GetSize() < minSize);
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
                // sourceNormal.GetSize() + GetSize() >= maxSize > 2 * minSize
                // -> sourceNormal.GetSize() - minSize >= minSize - GetSize()
                // -> deltaSource >= deltaSelf
                AFL_VERIFY(deltaSelf <= deltaSource);
                if (GetSize() + cSize >= maxSize) {
                    //GetSize() + cSize >= maxSize
                    // -> cSize >= maxSize - GetSize() > minSize - GetSize() = deltaSelf
                    AFL_VERIFY(deltaSelf <= cSize);
                } else {
                    AFL_VERIFY(sourceNormal.GetSize() - cSize < minSize);
                    //sourceNormal.GetSize() - cSize < minSize
                    // -> cSize > sourceNormal.GetSize() - minSize = deltaSource >= deltaSelf
                    AFL_VERIFY(deltaSource <= cSize);
                }

                counters->BySizeSplitter.OnTrashSerialized(c->GetPackedSize());
                auto splitParts = c->InternalSplit(schema->GetColumnSaver(i->GetEntityId()), counters, { (ui64)(0.5 * (deltaSelf + std::min(deltaSource, cSize))) });
                AFL_VERIFY(splitParts.size() == 2)("size", splitParts.size());
                if (i->GetSize() - c->GetPackedSize() + splitParts.back()->GetPackedSize() < maxSize &&
                    GetSize() + splitParts.front()->GetPackedSize() < maxSize) {
                    i->Exchange(c, splitParts);
                    AddChunk(i->DetachEntityChunkVerified(splitParts.front()));
                } else {
                    AFL_WARN(NKikimrServices::TX_COLUMNSHARD)("event", "grow_size_after_split")("start", c->GetPackedSize())(
                        "s1", splitParts.front()->GetPackedSize())("s2", splitParts.back()->GetPackedSize())("normal", i->GetSize())(
                        "self", GetSize());
                }

                ++internalSplitsCount;
                return true;
            }
        }
    }
    return false;
}

}   // namespace NKikimr::NOlap
