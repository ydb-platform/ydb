#pragma once
#include "abstract.h"

namespace NKikimr::NOlap::NReader::NSimple {

class TScanWithLimitCollection;

class TSyncPointLimitControl: public ISyncPoint {
private:
    using TBase = ISyncPoint;

    const ui32 Limit;
    std::shared_ptr<TScanWithLimitCollection> Collection;
    ui32 FetchedCount = 0;
    std::optional<ui32> PKPrefixSize;

    virtual bool IsSourcePrepared(const std::shared_ptr<NCommon::IDataSource>& source) const override {
        if (source->IsSyncSection() && source->HasStageResult()) {
            AFL_VERIFY(!source->GetStageResult().HasResultChunk());
            return true;
        }
        return false;
    }
    class TSourceIterator {
    private:
        std::shared_ptr<NCommon::IDataSource> Source;
        bool Reverse;
        int Delta = 0;
        i64 Start = 0;
        i64 Finish = 0;
        std::shared_ptr<NArrow::NMerger::TRWSortableBatchPosition> SortableRecord;
        std::shared_ptr<NArrow::TColumnFilter> Filter;
        std::shared_ptr<NArrow::TColumnFilter::TIterator> FilterIterator;
        bool IsValidFlag = true;

        bool ShiftWithFilter() const {
            AFL_VERIFY(IsValidFlag);
            while (!FilterIterator->GetCurrentAcceptance()) {
                if (!FilterIterator->Next(1)) {
                    AFL_VERIFY(!SortableRecord->NextPosition(Delta));
                    return false;
                } else {
                    AFL_VERIFY(SortableRecord->NextPosition(Delta));
                }
            }
            return true;
        }

    public:
        TString DebugString() const;

        const std::shared_ptr<NCommon::IDataSource>& GetSource() const {
            AFL_VERIFY(Source);
            return Source;
        }

        TSourceIterator(const std::shared_ptr<NCommon::IDataSource>& source)
            : Source(source)
            , Reverse(Source->GetContext()->GetReadMetadata()->IsDescSorted())
            , Delta(Reverse ? -1 : 1) {
            AFL_VERIFY(Source);
            AFL_VERIFY(Source->GetType() == IDataSource::EType::SimplePortion)("type", Source->GetType());
            auto batch = Source->GetAs<TPortionDataSource>()->GetStart().GetValue().ToBatch();
            SortableRecord = std::make_shared<NArrow::NMerger::TRWSortableBatchPosition>(batch, 0, Reverse);
        }

        TSourceIterator(const std::vector<std::shared_ptr<NArrow::NAccessor::IChunkedArray>>& arrs,
            const std::shared_ptr<NArrow::TColumnFilter>& filter, const std::shared_ptr<NCommon::IDataSource>& source)
            : Source(source)
            , Reverse(Source->GetContext()->GetReadMetadata()->IsDescSorted())
            , Delta(Reverse ? -1 : 1)
            , Start(Reverse ? (arrs.front()->GetRecordsCount() - 1) : 0)
            , Finish(Reverse ? 0 : (arrs.front()->GetRecordsCount() - 1))
            , Filter(filter ? filter : std::make_shared<NArrow::TColumnFilter>(NArrow::TColumnFilter::BuildAllowFilter())) {
            AFL_VERIFY(arrs.size());
            AFL_VERIFY(arrs.front()->GetRecordsCount());
            FilterIterator = std::make_shared<NArrow::TColumnFilter::TIterator>(Filter->GetBegin(Reverse, arrs.front()->GetRecordsCount()));
            auto prefixSchema = Source->GetSourceSchema()->GetIndexInfo().GetReplaceKeyPrefix(arrs.size());
            auto copyArrs = arrs;
            auto batch = std::make_shared<NArrow::TGeneralContainer>(prefixSchema->fields(), std::move(copyArrs));
            SortableRecord = std::make_shared<NArrow::NMerger::TRWSortableBatchPosition>(batch, Start, Reverse);
            IsValidFlag = ShiftWithFilter();
        }

        ui64 GetSourceId() const {
            AFL_VERIFY(Source);
            return Source->GetSourceId();
        }

        bool IsFilled() const {
            return !!Filter;
        }

        bool IsValid() const {
            return IsValidFlag;
        }

        bool Next() {
            AFL_VERIFY(IsValidFlag);
            AFL_VERIFY(!!SortableRecord);
            AFL_VERIFY(!!Filter);
            IsValidFlag = SortableRecord->NextPosition(Delta);
            AFL_VERIFY(FilterIterator->Next(1) == IsValidFlag);
            if (IsValidFlag) {
                IsValidFlag = ShiftWithFilter();
            }
            return IsValidFlag;
        }

        bool operator<(const TSourceIterator& item) const {
            const auto cmp = SortableRecord->ComparePartial(*item.SortableRecord);
            if (cmp == std::partial_ordering::equivalent) {
                return item.Source->GetSourceId() < Source->GetSourceId();
            }
            return cmp == std::partial_ordering::greater;
        }
    };

    std::vector<TSourceIterator> Iterators;

    virtual std::shared_ptr<NCommon::IDataSource> OnAddSource(const std::shared_ptr<NCommon::IDataSource>& source) override {
        AFL_VERIFY(FetchedCount < Limit);
        Iterators.emplace_back(TSourceIterator(source));
        std::push_heap(Iterators.begin(), Iterators.end());
        return TBase::OnAddSource(source);
    }

    virtual void DoAbort() override {
        Iterators.clear();
    }

    virtual ESourceAction OnSourceReady(const std::shared_ptr<NCommon::IDataSource>& source, TPlainReadData& reader) override;

    bool DrainToLimit();

public:
    TSyncPointLimitControl(const ui32 limit, const ui32 pointIndex, const std::shared_ptr<TSpecialReadContext>& context,
        const std::shared_ptr<TScanWithLimitCollection>& collection);
};

}   // namespace NKikimr::NOlap::NReader::NSimple
