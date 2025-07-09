#pragma once
#include "schema.h"
#include "source.h"

#include <ydb/core/tx/columnshard/engines/reader/common_reader/constructor/read_metadata.h>
#include <ydb/core/tx/columnshard/engines/reader/simple_reader/iterator/context.h>
#include <ydb/core/tx/columnshard/engines/reader/simple_reader/iterator/source.h>
#include <ydb/core/tx/columnshard/engines/reader/simple_reader/iterator/sys_view/abstract/constructor.h>

namespace NKikimr::NOlap::NReader::NSimple::NSysView::NPortions {
class TPortionDataConstructor {
private:
    NColumnShard::TUnifiedPathId PathId;
    ui64 TabletId;
    YDB_READONLY_DEF(std::vector<TPortionInfo::TConstPtr>, Portions);
    NArrow::TSimpleRow Start;
    NArrow::TSimpleRow Finish;
    ui32 SourceId = 0;
    ui32 SourceIdx = 0;

public:
    void SetIndex(const ui32 index) {
        AFL_VERIFY(!SourceId);
        SourceIdx = index;
        SourceId = index + 1;
    }

    TPortionDataConstructor(
        const NColumnShard::TUnifiedPathId& pathId, const ui64 tabletId, const std::vector<TPortionInfo::TConstPtr>& portions)
        : PathId(pathId)
        , TabletId(tabletId)
        , Portions(portions)
        , Start(TSchemaAdapter::GetPKSimpleRow(PathId, TabletId, Portions.front()->GetPortionId()))
        , Finish(TSchemaAdapter::GetPKSimpleRow(PathId, TabletId, Portions.back()->GetPortionId())) {
    }

    const NArrow::TSimpleRow& GetStart() const {
        return Start;
    }
    const NArrow::TSimpleRow& GetFinish() const {
        return Finish;
    }

    struct TComparator {
    private:
        const bool IsReverse;

    public:
        TComparator(const bool isReverse)
            : IsReverse(isReverse) {
        }

        bool operator()(const TPortionDataConstructor& l, const TPortionDataConstructor& r) const {
            if (IsReverse) {
                return r.Finish < l.Finish;
            } else {
                return l.Start < r.Start;
            }
        }
    };

    std::shared_ptr<NReader::NSimple::IDataSource> Construct(const std::shared_ptr<NReader::NSimple::TSpecialReadContext>& context) {
        AFL_VERIFY(SourceId);
        return std::make_shared<TSourceData>(
            SourceId, SourceIdx, PathId, TabletId, std::move(Portions), std::move(Start), std::move(Finish), context);
    }
};

class TConstructor: public NAbstract::ISourcesConstructor {
private:
    std::deque<TPortionDataConstructor> Constructors;
    const ui64 TabletId;
    ui32 CurrentSourceIdx = 1;
    const ERequestSorting Sorting;

    virtual void DoClear() override {
        Constructors.clear();
    }
    virtual void DoAbort() override {
        Constructors.clear();
    }
    virtual bool DoIsFinished() const override {
        return Constructors.empty();
    }
    virtual std::shared_ptr<NReader::NCommon::IDataSource> DoExtractNext(
        const std::shared_ptr<NReader::NCommon::TSpecialReadContext>& context) override;
    virtual void DoInitCursor(const std::shared_ptr<IScanCursor>& /*cursor*/) override {
    }
    virtual TString DoDebugString() const override {
        return Default<TString>();
    }

    void AddConstructors(const NOlap::IPathIdTranslator& pathIdTranslator, const NColumnShard::TInternalPathId& pathId,
        std::vector<TPortionInfo::TConstPtr>&& portions, const std::shared_ptr<NOlap::TPKRangesFilter>& pkFilter) {
        Constructors.emplace_back(pathIdTranslator.GetUnifiedByInternalVerified(pathId), TabletId, std::move(portions));
        if (!pkFilter->IsUsed(Constructors.back().GetStart(), Constructors.back().GetFinish())) {
            Constructors.pop_back();
        }
    }

public:
    TConstructor(const NOlap::IPathIdTranslator& pathIdTranslator, const IColumnEngine& engine, const ui64 tabletId,
        const std::optional<NOlap::TInternalPathId> internalPathId, const TSnapshot reqSnapshot,
        const std::shared_ptr<NOlap::TPKRangesFilter>& pkFilter, const ERequestSorting sorting);
};
}   // namespace NKikimr::NOlap::NReader::NSimple::NSysView::NPortions
