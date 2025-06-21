#pragma once
#include "chunks.h"

#include <ydb/core/formats/arrow/splitter/scheme_info.h>

#include <ydb/library/accessor/accessor.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/type.h>

namespace NKikimr::NOlap {

class TSplittedBlob;

class TSplittedEntity {
private:
    YDB_READONLY(ui32, EntityId, 0);
    YDB_READONLY(ui64, Size, 0);
    YDB_READONLY_DEF(std::vector<std::shared_ptr<IPortionDataChunk>>, Chunks);
    YDB_READONLY_DEF(std::optional<ui32>, RecordsCount);

protected:
    template <class T>
    const T& GetChunkAs(const ui32 idx) const {
        AFL_VERIFY(idx < Chunks.size());
        auto result = std::dynamic_pointer_cast<T>(Chunks[idx]);
        AFL_VERIFY(result);
        return *result;
    }

public:
    TSplittedEntity(const ui32 entityId)
        : EntityId(entityId) {
        AFL_VERIFY(EntityId);
    }

    ui64 GetPackedSize() const {
        ui64 result = 0;
        for (auto&& i : Chunks) {
            result += i->GetPackedSize();
        }
        AFL_VERIFY(Size == result)("size", Size)("result", result);
        return result;
    }

    class TEntityChunk {
    private:
        YDB_READONLY_DEF(TPositiveControlInteger, Size);
        std::vector<std::shared_ptr<IPortionDataChunk>> Chunks;
        TSplittedEntity* Entity;

    public:
        void Exchange(const std::shared_ptr<IPortionDataChunk>& from, const std::vector<std::shared_ptr<IPortionDataChunk>>& to) {
            DetachEntityChunkVerified(from);
            Entity->SwitchChunk(from, to);
            for (auto&& i : to) {
                AddChunk(i);
            }
        }

        TEntityChunk DetachEntityChunkVerified(const std::shared_ptr<IPortionDataChunk>& chunk) {
            for (ui32 idx = 0; idx < Chunks.size(); ++idx) {
                if (Chunks[idx]->GetChunkIdxVerified() == chunk->GetChunkIdxVerified()) {
                    AFL_VERIFY((ui64)Chunks[idx].get() == (ui64)chunk.get());
                    std::swap(Chunks[idx], Chunks[Chunks.size() - 1]);
                    Chunks.pop_back();
                    TEntityChunk result(Entity);
                    result.AddChunk(chunk);
                    Size.Sub(chunk->GetPackedSize());
                    return result;
                }
            }
            AFL_VERIFY(false);
            return TEntityChunk(Entity);
        }

        TEntityChunk(TSplittedEntity* entity)
            : Entity(entity) {
        }

        ui32 GetEntityId() const {
            return Entity->GetEntityId();
        }

        const std::vector<std::shared_ptr<IPortionDataChunk>>& GetChunks() const {
            return Chunks;
        }

        ui32 GetChunksCount() const {
            return Chunks.size();
        }

        const std::shared_ptr<IPortionDataChunk>& FindChunkVerified(const ui32 idx) const {
            for (auto&& i : Chunks) {
                if (i->GetChunkIdxVerified() == idx) {
                    return i;
                }
            }
            AFL_VERIFY(false);
            return Chunks[0];
        }

        void AddChunk(const std::shared_ptr<IPortionDataChunk>& chunk) {
            AFL_VERIFY(chunk);
            AFL_VERIFY(chunk->GetPackedSize());
            for (auto&& i : Chunks) {
                AFL_VERIFY(i->GetChunkIdxVerified() != chunk->GetChunkIdxVerified());
            }
            Chunks.emplace_back(chunk);
            Size.Add(chunk->GetPackedSize());
        }

        void Merge(const TEntityChunk& item) {
            AFL_VERIFY((ui64)Entity == (ui64)item.Entity);
            for (auto&& i : item.Chunks) {
                AddChunk(i);
            }
        }

        static std::vector<std::shared_ptr<IPortionDataChunk>> SplitToSize(const std::shared_ptr<IPortionDataChunk>& bigChunk,
            const ui32 sizeLimit, const NArrow::NSplitter::ISchemaDetailInfo::TPtr& schema,
            const std::shared_ptr<NColumnShard::TSplitterCounters>& counters) {
            const ui32 entityId = bigChunk->GetEntityId();
            NActors::TLogContextGuard lGuard = NActors::TLogContextBuilder::Build(NKikimrServices::TX_COLUMNSHARD)("entity_id", entityId)(
                "size", bigChunk->GetPackedSize())("limit", sizeLimit)("r_count", bigChunk->GetRecordsCountVerified());
            const auto predSplit = [sizeLimit, entityId, schema, counters](const std::shared_ptr<IPortionDataChunk>& chunkToSplit) {
                AFL_VERIFY(chunkToSplit->IsSplittable());
                counters->BySizeSplitter.OnTrashSerialized(chunkToSplit->GetPackedSize());
                const ui32 countSplit = chunkToSplit->GetPackedSize() / sizeLimit + 1;
                const ui32 sizeSplit = chunkToSplit->GetPackedSize() / countSplit;
                const std::vector<i64> sizes = NArrow::NSplitter::TSimilarPacker::SplitWithExpected(chunkToSplit->GetPackedSize(), sizeSplit);
                const std::vector<ui64> sizesUI64(sizes.begin(), sizes.end());
                auto result = chunkToSplit->InternalSplit(schema->GetColumnSaver(entityId), counters, sizesUI64);
                std::vector<ui32> splittedSizes;
                std::vector<ui32> splittedRecords;
                for (auto&& i : result) {
                    splittedSizes.emplace_back(i->GetPackedSize());
                    splittedRecords.emplace_back(i->GetRecordsCountVerified());
                }
                AFL_WARN(NKikimrServices::TX_COLUMNSHARD)("sizes", JoinSeq(",", sizes))("s_splitted", JoinSeq(",", splittedSizes))(
                    "r_splitted", JoinSeq(",", splittedRecords));
                return result;
            };
            std::deque<std::shared_ptr<IPortionDataChunk>> dqParts;
            {
                auto parts = predSplit(bigChunk);
                dqParts.insert(dqParts.end(), parts.begin(), parts.end());
            }
            std::vector<std::shared_ptr<IPortionDataChunk>> result;
            while (dqParts.size()) {
                auto checkImpl = dqParts.front();
                dqParts.pop_front();
                if (checkImpl->GetPackedSize() < sizeLimit) {
                    result.emplace_back(checkImpl);
                } else {
                    AFL_VERIFY(checkImpl->IsSplittable())("p_size", checkImpl->GetPackedSize())("c_impl", checkImpl->GetRecordsCountVerified());
                    auto parts = predSplit(checkImpl);
                    dqParts.insert(dqParts.begin(), parts.begin(), parts.end());
                }
            }
            return result;
        }
    };

    class TBlobChunk {
    private:
        YDB_READONLY_DEF(TPositiveControlInteger, Size);
        THashMap<ui32, TEntityChunk> Entities;

    public:
        TBlobChunk() = default;
        TBlobChunk(std::vector<TBlobChunk>&& chunks) {
            for (auto&& i : chunks) {
                Merge(std::move(i));
            }
        }
        TBlobChunk(TEntityChunk&& chunk) {
            AddChunk(std::move(chunk));
        }
        TEntityChunk ExtractChunk(const ui32 id) {
            auto it = Entities.find(id);
            AFL_VERIFY(it != Entities.end());
            Size.Sub(it->second.GetSize());
            auto result = std::move(it->second);
            Entities.erase(it);
            return result;
        }

        bool TakeEntityFrom(TBlobChunk& sourceNormal, const ui32 minSize, const ui32 maxSize) {
            std::vector<TEntityChunk*> chunks;
            for (auto&& i : sourceNormal.Entities) {
                chunks.emplace_back(&i.second);
            }
            const auto pred = [](const TEntityChunk* l, const TEntityChunk* r) {
                return l->GetSize() < r->GetSize();
            };
            std::sort(chunks.begin(), chunks.end(), pred);
            for (auto&& i : chunks) {
                if (GetSize() + i->GetSize() < maxSize && sourceNormal.GetSize() >= minSize + i->GetSize()) {
                    AddChunk(sourceNormal.ExtractChunk(i->GetEntityId()));
                    if (GetSize() >= minSize) {
                        return true;
                    }
                }
            }
            return false;
        }

        bool TakeEntityPartFrom(TBlobChunk& sourceNormal, const ui32 minSize, const ui32 maxSize,
            const NArrow::NSplitter::ISchemaDetailInfo::TPtr& schema, const std::shared_ptr<NColumnShard::TSplitterCounters>& counters, ui32& internalSplitsCount);

        const THashMap<ui32, TEntityChunk>& GetEntities() const {
            return Entities;
        }

        void AddChunk(TEntityChunk&& chunk) {
            Size.Add(chunk.GetSize());
            const ui32 entityId = chunk.GetEntityId();
            auto it = Entities.find(chunk.GetEntityId());
            if (it == Entities.end()) {
                it = Entities.emplace(entityId, std::move(chunk)).first;
            } else {
                it->second.Merge(std::move(chunk));
            }
        }

        void Merge(TBlobChunk&& chunk) {
            if (Entities.empty() && Size == 0) {
                Entities = std::move(chunk.Entities);
                Size = chunk.Size;
            } else {
                for (auto&& i : chunk.Entities) {
                    AddChunk(std::move(i.second));
                }
            }
        }
    };

    class TNormalizedBlobChunks {
    private:
        YDB_READONLY_DEF(TPositiveControlInteger, Size);
        std::vector<TBlobChunk> Normal;
        std::vector<TBlobChunk> Small;
        ui32 MinSize;
        ui32 MaxSize;
        ui32 Tolerance;
        bool Finished = false;
        NArrow::NSplitter::ISchemaDetailInfo::TPtr Schema;
        std::shared_ptr<NColumnShard::TSplitterCounters> Counters;
        ui32* InternalSplitsCount = nullptr;

    public:
        void Reserve(const ui32 count) {
            Normal.reserve(count);
            Small.reserve(count);
        }

        TNormalizedBlobChunks(const ui32 minSize, const ui32 maxSize, const ui32 tolerance, const NArrow::NSplitter::ISchemaDetailInfo::TPtr& schema, const std::shared_ptr<NColumnShard::TSplitterCounters>& counters,
            ui32& internalSplitsCount)
            : MinSize(minSize)
            , MaxSize(maxSize)
            , Tolerance(tolerance)
            , Schema(schema)
            , Counters(counters)
            , InternalSplitsCount(&internalSplitsCount)
        {
            AFL_VERIFY(MinSize * 2 < MaxSize);
        }

        std::vector<TSplittedBlob> Finish(const TString& groupName);

        void AddChunk(TBlobChunk&& chunk) {
            Size.Add(chunk.GetSize());
            AFL_VERIFY(chunk.GetSize() < MaxSize)("size", chunk.GetSize())("max", MaxSize);
            if (chunk.GetSize() < MinSize) {
                Small.emplace_back(std::move(chunk));
            } else {
                Normal.emplace_back(std::move(chunk));
            }
        }

        void Merge(TNormalizedBlobChunks&& normalizer) {
            for (auto&& i : normalizer.Normal) {
                AddChunk(std::move(i));
            }
            for (auto&& i : normalizer.Small) {
                AddChunk(std::move(i));
            }
        }

        [[nodiscard]] TNormalizedBlobChunks Normalize() {
            if (Small.empty()) {
                return *this;
            }
            TNormalizedBlobChunks result(MinSize, MaxSize, Tolerance, Schema, Counters, *InternalSplitsCount);
            for (auto&& i : Normal) {
                result.AddChunk(std::move(i));
            }
            std::vector<TBlobChunk*> smallPtr;
            for (auto&& i : Small) {
                smallPtr.emplace_back(&i);
            }
            const auto pred = [](const TBlobChunk* l, const TBlobChunk* r) {
                return r->GetSize() < l->GetSize();
            };
            std::sort(smallPtr.begin(), smallPtr.end(), pred);
            std::vector<TBlobChunk> normal;
            ui32 sumSize = 0;
            for (auto&& i : smallPtr) {
                if (sumSize + i->GetSize() >= MaxSize) {
                    AFL_VERIFY(normal.size() > 1);
                    result.AddChunk(TBlobChunk(std::move(normal)));
                    normal.clear();
                    sumSize = 0;
                }
                normal.emplace_back(std::move(*i));
                sumSize += i->GetSize();
            }
            if (normal.size()) {
                result.AddChunk(TBlobChunk(std::move(normal)));
            }
            return result;
        }

        void ForceMergeSmall() {
            if (Small.empty()) {
                return;
            }
            AFL_VERIFY(Small.size() == 1);
            TBlobChunk smallBlob = std::move(Small.front());
            Small.clear();
            if (Normal.empty()) {
                Normal.emplace_back(std::move(smallBlob));
                return;
            }
            for (auto&& i : Normal) {
                if (i.GetSize() + smallBlob.GetSize() < MaxSize) {
                    i.Merge(std::move(smallBlob));
                    return;
                }
            }
            for (auto&& i : Normal) {
                if (smallBlob.TakeEntityFrom(i, MinSize, MaxSize)) {
                    AFL_VERIFY(smallBlob.GetSize() < MaxSize);
                    Normal.emplace_back(std::move(smallBlob));
                    return;
                }
            }
            for (auto&& i : Normal) {
                Size.Sub(smallBlob.GetSize());
                Size.Sub(i.GetSize());
                if (smallBlob.TakeEntityPartFrom(i, MinSize, MaxSize, Schema, Counters, *InternalSplitsCount)) {
                    Size.Add(smallBlob.GetSize());
                    Size.Add(i.GetSize());
                    AFL_VERIFY(smallBlob.GetSize() < MaxSize);
                    Normal.emplace_back(std::move(smallBlob));
                    return;
                }
                Size.Add(smallBlob.GetSize());
                Size.Add(i.GetSize());
            }
            Normal.emplace_back(std::move(smallBlob));
        }
    };

    std::vector<TBlobChunk> BuildBlobChunks(const ui32 maxSize, const NArrow::NSplitter::ISchemaDetailInfo::TPtr& schema,
        const std::shared_ptr<NColumnShard::TSplitterCounters>& counters, ui32& internalSplitsCount) {
        for (ui32 idx = 0; idx < Chunks.size(); ++idx) {
            Chunks[idx]->SetChunkIdx(idx);
        }
        {
            auto eChunks = Chunks;
            for (auto&& i : eChunks) {
                if (maxSize <= i->GetPackedSize()) {
                    SwitchChunk(i, TEntityChunk::SplitToSize(i, maxSize, schema, counters));
                    ++internalSplitsCount;
                }
            }
        }
        TEntityChunk eChunk(this);
        std::vector<TBlobChunk> bChunks;
        bChunks.reserve(Chunks.size());
        for (ui32 idx = 0; idx < Chunks.size(); ++idx) {
            AFL_VERIFY(idx == Chunks[idx]->GetChunkIdxVerified());
            if (eChunk.GetSize() + Chunks[idx]->GetPackedSize() >= maxSize) {
                bChunks.emplace_back(std::move(eChunk));
                eChunk = TEntityChunk(this);
            }
            eChunk.AddChunk(Chunks[idx]);
        }
        if (eChunk.GetSize()) {
            bChunks.emplace_back(std::move(eChunk));
        }
        return bChunks;
    }

    void SwitchChunk(const std::shared_ptr<IPortionDataChunk>& from, const std::vector<std::shared_ptr<IPortionDataChunk>>& to) {
        AFL_VERIFY(to.size() > 1);
        const ui32 startSize = Chunks.size();
        AFL_VERIFY(startSize);
        AFL_VERIFY(from->GetChunkIdxVerified() < startSize);
        AFL_VERIFY((ui64)Chunks[from->GetChunkIdxVerified()].get() == (ui64)from.get());
        std::vector<std::shared_ptr<IPortionDataChunk>> result;
        result.insert(result.end(), Chunks.begin(), Chunks.begin() + from->GetChunkIdxVerified());
        result.insert(result.end(), to.begin(), to.end());
        result.insert(result.end(), Chunks.begin() + from->GetChunkIdxVerified() + 1, Chunks.end());
        ui32 idx = 0;
        for (auto&& i : result) {
            i->SetChunkIdx(idx++);
        }
        Chunks = std::move(result);
    }

    std::shared_ptr<arrow::Scalar> GetFirstScalar() const {
        Y_ABORT_UNLESS(Chunks.size());
        return Chunks.front()->GetFirstScalar();
    }

    std::shared_ptr<arrow::Scalar> GetLastScalar() const {
        Y_ABORT_UNLESS(Chunks.size());
        return Chunks.back()->GetLastScalar();
    }

    void Merge(TSplittedEntity&& c) {
        Size += c.Size;
        AFL_VERIFY(!!RecordsCount == !!c.RecordsCount);
        if (RecordsCount) {
            *RecordsCount += *c.RecordsCount;
        }
        AFL_VERIFY(EntityId == c.EntityId)("self", EntityId)("c", c.EntityId);
        Y_ABORT_UNLESS(c.EntityId);
        for (auto&& i : c.Chunks) {
            Chunks.emplace_back(std::move(i));
        }
    }

    void SetChunks(const std::vector<std::shared_ptr<IPortionDataChunk>>& data) {
        Y_ABORT_UNLESS(Chunks.empty());
        std::optional<bool> hasRecords;
        for (auto&& i : data) {
            Y_ABORT_UNLESS(i->GetEntityId() == EntityId);
            Size += i->GetPackedSize();
            Chunks.emplace_back(i);
            auto rc = i->GetRecordsCount();
            if (!hasRecords) {
                hasRecords = !!rc;
            }
            AFL_VERIFY(*hasRecords == !!rc);
            if (!rc) {
                continue;
            }
            if (!RecordsCount) {
                RecordsCount = rc;
            } else {
                *RecordsCount += *rc;
            }
        }
    }
};
}   // namespace NKikimr::NOlap
