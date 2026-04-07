#pragma once

#include <ydb/core/formats/arrow/reader/position.h>
#include <ydb/core/tx/columnshard/engines/portions/portion_info.h>

#include <ydb/library/accessor/accessor.h>

namespace NKikimr::NOlap::NReader::NSimple::NDuplicateFiltering {

class TPortionStore;

class TPortionStore: TMoveOnly {
private:
    THashMap<ui64, TPortionInfo::TConstPtr> Portions;

public:
    TPortionStore(THashMap<ui64, TPortionInfo::TConstPtr>&& portions);

    TPortionInfo::TConstPtr GetPortionVerified(const ui64 portionId) const;
};

class TBorder {
private:
    YDB_READONLY_DEF(std::shared_ptr<NArrow::NMerger::TSortableBatchPosition>, Key);
    YDB_READONLY_DEF(std::vector<ui64>, PortionIds);

public:
    TBorder(const std::shared_ptr<NArrow::NMerger::TSortableBatchPosition>& key, const std::vector<ui64>& portionIds);

    TString DebugString() const;
};

class TBordersBatch {
private:
    YDB_READONLY_DEF(std::vector<TBorder>, Borders);
    YDB_READONLY_DEF(THashSet<ui64>, PortionIds);

public:
    void AddBorder(const TBorder& border);
};

class TBordersIterator {
    friend class TBordersIteratorBuilder;
private:
    YDB_READONLY_DEF(std::vector<TBorder>, Borders);
    ui64 NextBorder = 0;
    const ui64 PortionsCountSoftLimit;

private:
    TBordersIterator(std::vector<TBorder>&& borders, const ui64 portionsCountSoftLimit);

public:
    TBordersBatch Next();
    bool IsDone() const;
};

class TBordersIteratorBuilder {
private:
    inline static const ui64 BATCH_PORTIONS_COUNT_SOFT_LIMIT = 10;
    std::vector<TBorder> Borders;

public:
    void AppendBorder(const TBorder& border);
    TBordersIterator Build();
    ui64 NumBorders() const;
};

}   // namespace NKikimr::NOlap::NReader::NSimple::NDuplicateFiltering
