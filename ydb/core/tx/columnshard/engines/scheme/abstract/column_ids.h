#pragma once

#include <ydb/library/actors/core/log.h>
#include <ydb/library/formats/arrow/common/iterator.h>

#include <util/generic/noncopyable.h>
#include <util/system/types.h>

#include <span>

namespace NKikimr::NOlap {

class TColumnIdsView: private TNonCopyable {
private:
    std::span<const ui32> ColumnIds;

    class TIterator: public NArrow::NUtil::TRandomAccessIteratorClone<std::span<const ui32>::iterator, TIterator> {
        using TBase = NArrow::NUtil::TRandomAccessIteratorClone<std::span<const ui32>::iterator, TIterator>;

    public:
        using TBase::TRandomAccessIteratorClone;
    };

public:
    template <typename It>
    TColumnIdsView(const It begin, const It end)
        : ColumnIds(begin, end) {
    }

    TIterator begin() const {
        return ColumnIds.begin();
    }

    TIterator end() const {
        return ColumnIds.end();
    }

    ui32 operator[](size_t idx) const {
        AFL_VERIFY(idx < ColumnIds.size())("idx", idx)("size", ColumnIds.size());
        return ColumnIds[idx];
    }

    ui64 size() const {
        return ColumnIds.size();
    }
};

}   // namespace NKikimr::NOlap
