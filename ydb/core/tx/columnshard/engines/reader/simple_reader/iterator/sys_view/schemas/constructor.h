#pragma once
#include "schema.h"
#include "source.h"

#include <ydb/core/tx/columnshard/engines/reader/common_reader/constructor/read_metadata.h>
#include <ydb/core/tx/columnshard/engines/reader/simple_reader/iterator/context.h>
#include <ydb/core/tx/columnshard/engines/reader/simple_reader/iterator/source.h>
#include <ydb/core/tx/columnshard/engines/reader/simple_reader/iterator/sys_view/abstract/constructor.h>

namespace NKikimr::NOlap::NReader::NSimple::NSysView::NSchemas {
class TDataConstructor {
private:
    ui64 TabletId;
    std::vector<ISnapshotSchema::TPtr> Schemas;
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

    TDataConstructor(const ui64 tabletId, std::vector<ISnapshotSchema::TPtr>&& schemas)
        : TabletId(tabletId)
        , Schemas(std::move(schemas))
        , Start(TSchemaAdapter::GetPKSimpleRow(TabletId, Schemas.front()->GetIndexInfo().GetPresetId(), Schemas.front()->GetVersion()))
        , Finish(TSchemaAdapter::GetPKSimpleRow(TabletId, Schemas.back()->GetIndexInfo().GetPresetId(), Schemas.back()->GetVersion())) {
        if (Schemas.size() > 1) {
            AFL_VERIFY(Start < Finish)("start", Start.DebugString())("finish", Finish.DebugString());
        }
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

        bool operator()(const TDataConstructor& l, const TDataConstructor& r) const {
            if (IsReverse) {
                return r.Finish < l.Finish;
            } else {
                return l.Start < r.Start;
            }
        }
    };

    std::shared_ptr<NReader::NSimple::IDataSource> Construct(const std::shared_ptr<NReader::NCommon::TSpecialReadContext>& context) {
        AFL_VERIFY(SourceId);
        return std::make_shared<TSourceData>(SourceId, SourceIdx, TabletId, std::move(Schemas), std::move(Start), std::move(Finish), context);
    }
};

class TConstructor: public NCommon::ISourcesConstructor {
private:
    const ui64 TabletId;
    std::deque<TDataConstructor> Constructors;

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
        const std::shared_ptr<NReader::NCommon::TSpecialReadContext>& context, const ui32 /*inFlightCurrentLimit*/) override {
        AFL_VERIFY(Constructors.size());
        std::shared_ptr<NReader::NCommon::IDataSource> result = Constructors.front().Construct(context);
        Constructors.pop_front();
        return result;
    }
    virtual void DoInitCursor(const std::shared_ptr<IScanCursor>& /*cursor*/) override {
    }
    virtual TString DoDebugString() const override {
        return Default<TString>();
    }

    void AddConstructors(std::vector<ISnapshotSchema::TPtr>&& schemas, const std::shared_ptr<NOlap::TPKRangesFilter>& pkFilter) {
        Constructors.emplace_back(TabletId, std::move(schemas));
        if (!pkFilter->IsUsed(Constructors.back().GetStart(), Constructors.back().GetFinish())) {
            Constructors.pop_back();
        }
    }

public:
    TConstructor(
        const IColumnEngine& engine, const ui64 tabletId, const std::shared_ptr<NOlap::TPKRangesFilter>& pkFilter, const bool isReverseSort);
};
}   // namespace NKikimr::NOlap::NReader::NSimple::NSysView::NSchemas
