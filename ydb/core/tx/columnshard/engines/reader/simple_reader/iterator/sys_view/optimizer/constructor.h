#pragma once
#include "schema.h"
#include "source.h"

#include <ydb/core/tx/columnshard/engines/reader/common_reader/constructor/read_metadata.h>
#include <ydb/core/tx/columnshard/engines/reader/simple_reader/iterator/context.h>
#include <ydb/core/tx/columnshard/engines/reader/simple_reader/iterator/source.h>
#include <ydb/core/tx/columnshard/engines/reader/simple_reader/iterator/sys_view/abstract/constructor.h>
#include <ydb/core/tx/columnshard/engines/storage/granule/granule.h>

namespace NKikimr::NOlap::NReader::NSimple::NSysView::NOptimizer {
class TGranuleDataConstructor {
private:
    ui64 TabletId;
    std::shared_ptr<const TGranuleMeta> Granule;
    NColumnShard::TSchemeShardLocalPathId ExternalPathId;
    ui32 PortionsCount;
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

    TGranuleDataConstructor(const IPathIdTranslator& translator, const ui64 tabletId, const std::shared_ptr<const TGranuleMeta>& granule)
        : TabletId(tabletId)
        , Granule(std::move(granule))
        , ExternalPathId(translator.ResolveSchemeShardLocalPathIdVerified(Granule->GetPathId()))
        , PortionsCount(Granule->GetPortions().size())
        , Start(TSchemaAdapter::GetPKSimpleRow(ExternalPathId, TabletId, 0))
        , Finish(TSchemaAdapter::GetPKSimpleRow(ExternalPathId, TabletId, Max<ui64>())) {
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

        bool operator()(const TGranuleDataConstructor& l, const TGranuleDataConstructor& r) const {
            if (IsReverse) {
                return r.Finish < l.Finish;
            } else {
                return l.Start < r.Start;
            }
        }
    };

    std::shared_ptr<NReader::NSimple::IDataSource> Construct(const std::shared_ptr<NReader::NSimple::TSpecialReadContext>& context) {
        AFL_VERIFY(SourceId);
        auto tasks = Granule->GetOptimizerPlanner().GetTasksDescription();
        return std::make_shared<TSourceData>(
            SourceId, SourceIdx, TabletId, Granule, std::move(tasks), ExternalPathId, std::move(Start), std::move(Finish), context);
    }
};

class TConstructor: public NAbstract::ISourcesConstructor {
private:
    std::deque<TGranuleDataConstructor> Constructors;
    const ui64 TabletId;

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
        const std::shared_ptr<NReader::NCommon::TSpecialReadContext>& context) override {
        AFL_VERIFY(Constructors.size());
        std::shared_ptr<NReader::NCommon::IDataSource> result =
            Constructors.front().Construct(std::static_pointer_cast<NReader::NSimple::TSpecialReadContext>(context));
        Constructors.pop_front();
        return result;
    }
    virtual void DoInitCursor(const std::shared_ptr<IScanCursor>& /*cursor*/) override {
    }
    virtual TString DoDebugString() const override {
        return Default<TString>();
    }

    void AddConstructors(const IPathIdTranslator& pathIdTranslator, const std::shared_ptr<const TGranuleMeta>& granule,
        const std::shared_ptr<NOlap::TPKRangesFilter>& pkFilter) {
        Constructors.emplace_back(pathIdTranslator, TabletId, granule);
        if (!pkFilter->IsUsed(Constructors.back().GetStart(), Constructors.back().GetFinish())) {
            Constructors.pop_back();
        }
    }

public:
    TConstructor(const NOlap::IPathIdTranslator& pathIdTranslator, const IColumnEngine& engine, const ui64 tabletId,
        const std::optional<NOlap::TInternalPathId> internalPathId, const std::shared_ptr<NOlap::TPKRangesFilter>& pkFilter,
        const bool isReverseSort);
};
}   // namespace NKikimr::NOlap::NReader::NSimple::NSysView::NOptimizer
