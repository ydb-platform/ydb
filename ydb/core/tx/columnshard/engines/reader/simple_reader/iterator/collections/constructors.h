#pragma once
#include "abstract.h"

#include <ydb/core/tx/columnshard/engines/reader/common_reader/common/accessors_ordering.h>
#include <ydb/core/tx/columnshard/engines/reader/common_reader/constructor/read_metadata.h>
#include <ydb/core/tx/columnshard/engines/reader/simple_reader/duplicates/common.h>

#include <ydb/library/accessor/positive_integer.h>

namespace NKikimr::NOlap {
class TPortionInfo;
}

namespace NKikimr::NOlap::NReader::NSimple {

class TSourceConstructor: public ICursorEntity, public TMoveOnly {
private:
    TCompareKeyForScanSequence Start;
    YDB_READONLY(ui32, SourceId, 0);
    YDB_READONLY_DEF(std::shared_ptr<TPortionInfo>, Portion);
    ui32 RecordsCount = 0;
    bool IsStartedByCursorFlag = false;

    virtual ui64 DoGetEntityId() const override {
        return SourceId;
    }
    virtual ui64 DoGetEntityRecordsCount() const override {
        return RecordsCount;
    }
    std::optional<ui32> SourceIdx;

public:
    void SetIndex(const ui32 index) {
        SourceIdx = index;
    }

    void SetIsStartedByCursor() {
        IsStartedByCursorFlag = true;
    }
    bool GetIsStartedByCursor() const {
        return IsStartedByCursorFlag;
    }

    const TCompareKeyForScanSequence& GetStart() const {
        return Start;
    }

    TSourceConstructor(const std::shared_ptr<TPortionInfo>&& portion, const NReader::ERequestSorting sorting)
        : Start(TReplaceKeyAdapter((sorting == NReader::ERequestSorting::DESC) ? portion->IndexKeyEnd() : portion->IndexKeyStart(),
                    sorting == NReader::ERequestSorting::DESC),
              portion->GetPortionId())
        , SourceId(portion->GetPortionId())
        , Portion(std::move(portion))
        , RecordsCount(portion->GetRecordsCount()) {
    }

    class TComparator {
    private:
        const ERequestSorting Sorting;

    public:
        TComparator(const ERequestSorting sorting)
            : Sorting(sorting) {
            AFL_VERIFY(Sorting != ERequestSorting::NONE);
        }

        bool operator()(const TSourceConstructor& l, const TSourceConstructor& r) const {
            return r.Start < l.Start;
        }
    };

    std::shared_ptr<TPortionDataSource> Construct(const std::shared_ptr<NCommon::TSpecialReadContext>& context, std::shared_ptr<TPortionDataAccessor>&& accessor) const;
};

class TPortionsSources: public NCommon::TSourcesConstructorWithAccessors<TSourceConstructor> {
private:
    using TBase = NCommon::TSourcesConstructorWithAccessors<TSourceConstructor>;
    ui32 CurrentSourceIdx = 0;
    std::vector<TInsertWriteId> Uncommitted;    

    virtual void DoFillReadStats(TReadStats& stats) const override {
        ui64 compactedPortionsBytes = 0;
        ui64 insertedPortionsBytes = 0;
        ui64 committedPortionsBytes = 0;
        for (auto&& i : TBase::GetConstructors()) {
            if (i.GetPortion()->GetPortionType() == EPortionType::Compacted) {
                compactedPortionsBytes += i.GetPortion()->GetTotalBlobBytes();
            } else if (i.GetPortion()->GetProduced() == NPortion::EProduced::INSERTED) {
                insertedPortionsBytes += i.GetPortion()->GetTotalBlobBytes();
            } else {
                committedPortionsBytes += i.GetPortion()->GetTotalBlobBytes();
            }
        }
        stats.IndexPortions = TBase::GetConstructorsCount();
        stats.InsertedPortionsBytes = insertedPortionsBytes;
        stats.CompactedPortionsBytes = compactedPortionsBytes;
        stats.CommittedPortionsBytes = committedPortionsBytes;
    }

    virtual void DoInitCursor(const std::shared_ptr<IScanCursor>& cursor) override;

    virtual NCommon::TPortionIntervalTree GetPortionIntervals() const override {
        NCommon::TPortionIntervalTree result;
        for (const auto& portion : Sources) {
            result.AddRange(NCommon::TPortionIntervalTree::TOwnedRange(portion.GetPortion()->IndexKeyStart(), true, portion.GetPortion()->IndexKeyEnd(), true),
                portion.GetPortion());
        }
        return result;
    }

    virtual std::vector<TInsertWriteId> GetUncommittedWriteIds() const override;

    virtual std::shared_ptr<NCommon::IDataSource> DoTryExtractNextImpl(const std::shared_ptr<NCommon::TSpecialReadContext>& context) override {
        auto constructor = TBase::PopObjectWithAccessor();
        constructor.MutableObject().SetIndex(CurrentSourceIdx);
        ++CurrentSourceIdx;
        return constructor.MutableObject().Construct(context, constructor.DetachAccessor());
    }
    virtual NCommon::TPortionIntervalTree  GetPortionIntervals() const override {
        NCommon::TPortionIntervalTree  result;
        for (const auto& portion : HeapSources) {
            result.AddRange(NCommon::TPortionIntervalTree ::TOwnedRange(portion.GetPortion()->IndexKeyStart(), true, portion.GetPortion()->IndexKeyEnd(), true),
                portion.GetPortion());
        }
        return result;
    }

public:
    TPortionsSources(std::deque<TSourceConstructor>&& sources, const ERequestSorting sorting, std::vector<TInsertWriteId>&& uncommitted)
        : TBase(sorting)
        , Uncommitted(std::move(uncommitted))
    {
        InitializeConstructors(std::move(sources));
    }

    static std::unique_ptr<TPortionsSources> BuildEmpty() {
        return std::make_unique<TPortionsSources>(std::deque<TSourceConstructor>{}, ERequestSorting::NONE, std::vector<TInsertWriteId>{});
    }
};

}   // namespace NKikimr::NOlap::NReader::NSimple
