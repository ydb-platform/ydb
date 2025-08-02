#pragma once
#include "source.h"

#include <ydb/core/tx/columnshard/engines/reader/common_reader/constructor/read_metadata.h>
#include <ydb/core/tx/columnshard/engines/reader/simple_reader/iterator/context.h>
#include <ydb/core/tx/columnshard/engines/reader/simple_reader/iterator/source.h>

namespace NKikimr::NOlap::NReader::NSimple::NSysView::NAbstract {

class TDataSourceConstructor: public ICursorEntity {
private:
    const ui64 TabletId;
    NArrow::TSimpleRow Start;
    NArrow::TSimpleRow Finish;
    const ui32 SourceId = 0;
    ui32 SourceIdx = 0;
    bool SourceIdxInitialized = false;

    virtual ui64 DoGetEntityId() const override {
        return SourceId;
    }
    virtual ui64 DoGetEntityRecordsCount() const override {
        return 0;
    }

public:
    ui32 GetSourceId() const {
        return SourceId;
    }

    void SetIndex(const ui32 index) {
        AFL_VERIFY(!SourceIdxInitialized);
        SourceIdxInitialized = true;
        SourceIdx = index;
    }

    ui64 GetTabletId() const {
        return TabletId;
    }
    ui32 GetSourceIdx() const {
        AFL_VERIFY(SourceIdxInitialized);
        return SourceIdx;
    }

    NArrow::TSimpleRow ExtractStart() {
        return std::move(Start);
    }

    NArrow::TSimpleRow ExtractFinish() {
        return std::move(Finish);
    }

    TDataSourceConstructor(const ui64 tabletId, const ui32 sourceId, NArrow::TSimpleRow&& start, NArrow::TSimpleRow&& finish)
        : TabletId(tabletId)
        , SourceId(sourceId)
        , Start(std::move(start))
        , Finish(std::move(finish)) {
        AFL_VERIFY(SourceId);
    }

    const NArrow::TSimpleRow& GetStart() const {
        return Start;
    }
    const NArrow::TSimpleRow& GetFinish() const {
        return Finish;
    }

    class TComparator {
    private:
        const ERequestSorting Sorting;

    public:
        TComparator(const ERequestSorting sorting)
            : Sorting(sorting) {
            AFL_VERIFY(Sorting != ERequestSorting::NONE);
        }

        bool operator()(const TPortionDataConstructor& l, const TPortionDataConstructor& r) const {
            if (Sorting == ERequestSorting::DESC) {
                return l.Finish < r.Finish;
            } else {
                return r.Start < l.Start;
            }
        }
    };
};

template <class TDataSourceConstructorImpl>
class TConstructor: public NCommon::ISourcesConstructor {
private:
    ui32 CurrentSourceIdx = 0;
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
        constructor.SetIndex(CurrentSourceIdx++);
        return constructor.Construct(context);
    }
    virtual void DoInitCursor(const std::shared_ptr<IScanCursor>& /*cursor*/) override {
        while (Constructors.GetSize()) {
            bool usage = false;
            if (!cursor->CheckEntityIsBorder(Constructors.MutableNextObject(), usage)) {
                Constructors.DropNextConstructor();
                continue;
            }
            AFL_VERIFY(!usage);
            Constructors.DropNextConstructor();
            break;
        }
    }
    virtual TString DoDebugString() const override {
        return Default<TString>();
    }

protected:
    TOrderedObjects<TDataSourceConstructorImpl> Constructors;
    const ui64 TabletId;

public:
    TConstructor(const ERequestSorting sorting, const ui64 tabletId)
        : Constructors(sorting)
        , TabletId(tabletId)
    {
    }
};

}   // namespace NKikimr::NOlap::NReader::NSimple::NSysView::NAbstract
