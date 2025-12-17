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

    virtual ui64 DoGetEntityRecordsCount() const override {
        return 0;
    }

public:
    TDataSourceConstructor(const ui64 tabletId, NArrow::TSimpleRow&& start, NArrow::TSimpleRow&& finish)
        : NCommon::TDataSourceConstructor(TReplaceKeyAdapter(std::move(start), false), TReplaceKeyAdapter(std::move(finish), false))
        , TabletId(tabletId)
    {
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
    TConstructor(const ERequestSorting sorting, const ui64 tabletId)
        : Constructors(sorting)
        , TabletId(tabletId) {
    }
};

}   // namespace NKikimr::NOlap::NReader::NSimple::NSysView::NAbstract
