#pragma once
#include "source.h"

#include <ydb/core/tx/columnshard/engines/reader/common_reader/common/accessors_ordering.h>
#include <ydb/core/tx/columnshard/engines/reader/common_reader/constructor/read_metadata.h>
#include <ydb/core/tx/columnshard/engines/reader/simple_reader/iterator/context.h>
#include <ydb/core/tx/columnshard/engines/reader/simple_reader/iterator/source.h>

namespace NKikimr::NOlap::NReader::NSimple::NSysView::NAbstract {

class TDataSourceConstructor: public ICursorEntity, public TMoveOnly {
private:
    ui64 TabletId;
    ui32 SourceId = 0;
    NArrow::TSimpleRowContent Start;
    NArrow::TSimpleRowContent Finish;
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

    NArrow::TSimpleRowContent ExtractStart() {
        return std::move(Start);
    }

    NArrow::TSimpleRowContent ExtractFinish() {
        return std::move(Finish);
    }

    TDataSourceConstructor(const ui64 tabletId, const ui32 sourceId, NArrow::TSimpleRowContent&& start, NArrow::TSimpleRowContent&& finish)
        : TabletId(tabletId)
        , SourceId(sourceId)
        , Start(std::move(start))
        , Finish(std::move(finish)) {
        AFL_VERIFY(SourceId);
    }

    const NArrow::TSimpleRowContent& GetStart() const {
        return Start;
    }
    const NArrow::TSimpleRowContent& GetFinish() const {
        return Finish;
    }

    class TComparator {
    private:
        const ERequestSorting Sorting;
        const arrow::Schema* Schema;
    public:
        TComparator(const ERequestSorting sorting, const arrow::Schema* schema)
            : Sorting(sorting)
            , Schema(schema)
        {
            AFL_VERIFY(schema);
            AFL_VERIFY(Sorting != ERequestSorting::NONE);
        }

        bool operator()(const TDataSourceConstructor& l, const TDataSourceConstructor& r) const {
            if (Sorting == ERequestSorting::DESC) {
                return l.Finish.GetView(*Schema) < r.Finish.GetView(*Schema);
            } else {
                return r.Start.GetView(*Schema) < l.Start.GetView(*Schema);
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
    TConstructor(const ERequestSorting sorting, const ui64 tabletId, const std::shared_ptr<arrow::Schema>& pkSchema)
        : Constructors(sorting, pkSchema)
        , TabletId(tabletId) {
    }
};

}   // namespace NKikimr::NOlap::NReader::NSimple::NSysView::NAbstract
