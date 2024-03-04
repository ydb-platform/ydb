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

    std::shared_ptr<arrow::Scalar> GetFirstScalar() const {
        Y_ABORT_UNLESS(Chunks.size());
        return Chunks.front()->GetFirstScalar();
    }

    std::shared_ptr<arrow::Scalar> GetLastScalar() const {
        Y_ABORT_UNLESS(Chunks.size());
        return Chunks.back()->GetLastScalar();
    }

    void Merge(TSplittedColumn&& c) {
        Size += c.Size;
        Y_ABORT_UNLESS(Field->name() == c.Field->name());
        Y_DEBUG_ABORT_UNLESS(Field->Equals(c.Field));
        Y_ABORT_UNLESS(ColumnId == c.ColumnId);
        Y_ABORT_UNLESS(ColumnId);
        RecordsCount += c.RecordsCount;
        for (auto&& i : c.Chunks) {
            Chunks.emplace_back(std::move(i));
        }
    }

    void SetChunks(const std::vector<IPortionColumnChunk::TPtr>& data) {
        Y_ABORT_UNLESS(Chunks.empty());
        for (auto&& i : data) {
            Y_ABORT_UNLESS(i->GetColumnId() == ColumnId);
            Size += i->GetPackedSize();
            RecordsCount += i->GetRecordsCount();
            Chunks.emplace_back(i);
        }
    }

    void AddChunks(const std::vector<IPortionColumnChunk::TPtr>& data) {
        for (auto&& i : data) {
            Y_ABORT_UNLESS(i->GetColumnId() == ColumnId);
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
