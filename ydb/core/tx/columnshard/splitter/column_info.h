#pragma once
#include "chunks.h"

#include <ydb/library/accessor/accessor.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/type.h>

namespace NKikimr::NOlap {

class TSplittedColumn {
private:
    YDB_READONLY(ui64, Size, 0);
    YDB_READONLY(ui32, ColumnId, 0);
    YDB_READONLY(ui32, RecordsCount, 0);
    YDB_READONLY_DEF(std::shared_ptr<arrow::Field>, Field);
    YDB_READONLY_DEF(std::vector<IPortionColumnChunk::TPtr>, Chunks);
public:
    void Merge(TSplittedColumn&& c) {
        Size += c.Size;
        Y_VERIFY(Field->name() == c.Field->name());
        Y_VERIFY_DEBUG(Field->Equals(c.Field));
        Y_VERIFY(ColumnId == c.ColumnId);
        Y_VERIFY(ColumnId);
        RecordsCount += c.RecordsCount;
        for (auto&& i : c.Chunks) {
            Chunks.emplace_back(std::move(i));
        }
    }

    void SetChunks(const std::vector<IPortionColumnChunk::TPtr>& data) {
        Y_VERIFY(Chunks.empty());
        for (auto&& i : data) {
            Y_VERIFY(i->GetColumnId() == ColumnId);
            Size += i->GetPackedSize();
            RecordsCount += i->GetRecordsCount();
            Chunks.emplace_back(i);
        }
    }

    void AddChunks(const std::vector<IPortionColumnChunk::TPtr>& data) {
        for (auto&& i : data) {
            Y_VERIFY(i->GetColumnId() == ColumnId);
            Size += i->GetPackedSize();
            RecordsCount += i->GetRecordsCount();
            Chunks.emplace_back(i);
        }
    }

    TSplittedColumn(std::shared_ptr<arrow::Field> f, const ui32 id)
        : ColumnId(id)
        , Field(f)
    {

    }

    bool operator<(const TSplittedColumn& item) const {
        return Size > item.Size;
    }
};
}
