#pragma once
#include "chunks.h"

#include <ydb/library/accessor/accessor.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/type.h>

namespace NKikimr::NOlap {

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

    bool operator<(const TSplittedEntity& item) const {
        return Size > item.Size;
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
}
