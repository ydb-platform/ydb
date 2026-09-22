#pragma once
#include "source.h"

#include <ydb/core/tx/columnshard/engines/reader/common_reader/common/accessors_ordering.h>
#include <ydb/core/tx/columnshard/engines/reader/common_reader/constructor/read_metadata.h>
#include <ydb/core/tx/columnshard/engines/reader/simple_reader/iterator/context.h>
#include <ydb/core/tx/columnshard/engines/reader/simple_reader/iterator/source.h>

namespace NKikimr::NOlap::NReader::NSimple::NSysView::NAbstract {

class TDataSourceConstructor: public NCommon::TDataSourceConstructor {
private:
    YDB_READONLY_DEF(ui64, TabletId);
    ESourcesSorting SourcesSorting;

    virtual ui64 DoGetSourceRecordsCount() const override {
        return 0;
    }

    virtual ui64 DoGetSourceId() const override {
        return TabletId;
    }

public:
    // DESC orders by the finish key in reverse, like the portion constructor, so sources are extracted in scan direction
    TDataSourceConstructor(const ui64 tabletId, NArrow::TSimpleRow&& start, NArrow::TSimpleRow&& finish, const ESourcesSorting sourcesSorting)
        : NCommon::TDataSourceConstructor(
              TReplaceKeyAdapter((sourcesSorting == ESourcesSorting::LastPkDesc) ? std::move(finish) : std::move(start),
                  sourcesSorting == ESourcesSorting::LastPkDesc),
              TReplaceKeyAdapter((sourcesSorting == ESourcesSorting::LastPkDesc) ? std::move(start) : std::move(finish),
                  sourcesSorting == ESourcesSorting::LastPkDesc), false)
        , TabletId(tabletId)
        , SourcesSorting(sourcesSorting)
    {
    }

    // the PK filter wants the range in key order; Start/Finish are swapped for DESC, so undo that here
    bool IsUsedBy(const NOlap::TPKRangesFilter& filter) const {
        const auto& lo = (SourcesSorting == ESourcesSorting::LastPkDesc) ? GetFinish() : GetStart();
        const auto& hi = (SourcesSorting == ESourcesSorting::LastPkDesc) ? GetStart() : GetFinish();
        return filter.IsUsed(lo.GetValue().BuildSortablePosition(), hi.GetValue().BuildSortablePosition());
    }
};

template <class TDataSourceConstructorImpl>
class TConstructor: public NCommon::ISourcesConstructor {
private:
    virtual void DoClear() override {
        Constructors.Clear();
    }

    virtual void DoAbort() override {
        Constructors.Clear();
    }

    virtual bool DoIsFinished() const override {
        return Constructors.IsEmpty();
    }

    virtual std::shared_ptr<NCommon::IDataSource> DoTryExtractNext(
        const std::shared_ptr<NCommon::TSpecialReadContext>& context, const ui32 /*inFlightCurrentLimit*/) override final {
        auto constructor = Constructors.PopFront();
        return constructor.Construct(context);
    }

    virtual void DoInitCursor(const std::shared_ptr<IScanCursor>& cursor) override {
        while (Constructors.GetSize()) {
            bool usage = false;
            if (!cursor->CheckEntityIsBorder(Constructors.MutableNextObject(), usage)) {
                Constructors.PopFront();
                continue;
            }
            AFL_VERIFY(!usage);
            Constructors.PopFront();
            break;
        }
    }

    virtual TString DoDebugString() const override {
        return Default<TString>();
    }

protected:
    NCommon::TOrderedObjects<TDataSourceConstructorImpl> Constructors;
    const ui64 TabletId;

public:
    TConstructor(const ESourcesSorting sourcesSorting, const ui64 tabletId)
        : Constructors(sourcesSorting)
        , TabletId(tabletId)
    {
    }
};

}   // namespace NKikimr::NOlap::NReader::NSimple::NSysView::NAbstract
