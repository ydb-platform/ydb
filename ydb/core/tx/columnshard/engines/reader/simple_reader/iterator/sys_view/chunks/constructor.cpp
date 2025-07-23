#include "constructor.h"
#include "source.h"

#include <ydb/core/tx/columnshard/engines/reader/simple_reader/iterator/plain_read_data.h>

namespace NKikimr::NOlap::NReader::NSimple::NSysView::NChunks {

std::shared_ptr<IDataSource> TPortionDataConstructor::Construct(const std::shared_ptr<NReader::NCommon::TSpecialReadContext>& context) {
    AFL_VERIFY(SourceId);
    return std::make_shared<TSourceData>(
        SourceId, SourceIdx, PathId, TabletId, std::move(Portion), std::move(Start), std::move(Finish), context, std::move(Schema));
}

std::shared_ptr<IDataSource> TPortionDataConstructor::Construct(
    const std::shared_ptr<NReader::NCommon::TSpecialReadContext>& context, std::shared_ptr<TPortionDataAccessor>&& accessor) {
    auto result = Construct(context);
    result->SetPortionAccessor(std::move(accessor));
    return result;
}

namespace {
class TApplySourceResult: public IApplyAction {
private:
    using TBase = IDataTasksProcessor::ITask;
    TDataAccessorsResult Accessors;

public:
    TApplySourceResult(TDataAccessorsResult&& result)
        : Accessors(std::move(result)) {
    }

    virtual bool DoApply(IDataReader& indexedDataRead) override {
        auto* plainReader = static_cast<TPlainReadData*>(&indexedDataRead);
        TConstructor& constructor = plainReader->MutableScanner().MutableSourcesCollection().MutableConstructorsAs<TConstructor>();
        constructor.AddAccessors(std::move(Accessors));
        return true;
    }
};

class TLocalPortionAccessorFetchingSubscriber: public IDataAccessorRequestsSubscriber {
private:
    const NActors::TActorId ScanActorId;
    NColumnShard::TCounterGuard Guard;
    const std::shared_ptr<const TAtomicCounter> AbortFlag;
    virtual const std::shared_ptr<const TAtomicCounter>& DoGetAbortionFlag() const override {
        return AbortFlag;
    }

    virtual void DoOnRequestsFinished(TDataAccessorsResult&& result) override {
        NActors::TActivationContext::AsActorContext().Send(ScanActorId,
            new NColumnShard::TEvPrivate::TEvTaskProcessedResult(std::make_shared<TApplySourceResult>(std::move(result)), std::move(Guard)));
    }

public:
    TLocalPortionAccessorFetchingSubscriber(const std::shared_ptr<NReader::NCommon::TSpecialReadContext>& context)
        : ScanActorId(context->GetCommonContext()->GetScanActorId())
        , Guard(context->GetCommonContext()->GetCounters().GetAccessorsForConstructionGuard())
        , AbortFlag(context->GetCommonContext()->GetAbortionFlag()) {
    }
};

}   // namespace

void TConstructor::AddAccessors(TDataAccessorsResult&& accessors) {
    AFL_VERIFY(InFlightRequests);
    if (Accessors.empty()) {
        Accessors = std::move(accessors.ExtractPortions());
    } else {
        for (auto&& i : accessors.ExtractPortions()) {
            AFL_VERIFY(Accessors.emplace(i.first, std::move(i.second)).second);
        }
    }
    AFL_VERIFY(InFlightRequests);
    --InFlightRequests;
}

std::shared_ptr<NCommon::IDataSource> TConstructor::DoExtractNext(
    const std::shared_ptr<NReader::NCommon::TSpecialReadContext>& context, const ui32 inFlightCurrentLimit) {
    AFL_VERIFY(Constructors.size());
    if (Sorting == ERequestSorting::NONE) {
        if (InFlightRequests && !Accessors.size()) {
            return nullptr;
        }
        if (!InFlightRequests && (!Accessors.size() || (Accessors.size() < Constructors.size() && Accessors.size() < inFlightCurrentLimit))) {
            std::shared_ptr<TDataAccessorsRequest> request =
                std::make_shared<TDataAccessorsRequest>(NGeneralCache::TPortionsMetadataCachePolicy::EConsumer::SCAN);
            for (ui32 idx = Accessors.size(); idx < Constructors.size(); ++idx) {
                request->AddPortion(Constructors[idx].GetPortion());
                if (request->GetSize() + Accessors.size() == inFlightCurrentLimit * 2) {
                    break;
                }
            }
            request->SetColumnIds(context->GetAllUsageColumns()->GetColumnIds());
            request->RegisterSubscriber(std::make_shared<TLocalPortionAccessorFetchingSubscriber>(context));
            context->GetCommonContext()->GetDataAccessorsManager()->AskData(std::move(request));
            AFL_VERIFY(++InFlightRequests == 1);
        }
        if (!Accessors.size()) {
            return nullptr;
        }
        Constructors.front().SetIndex(CurrentSourceIdx);
        ++CurrentSourceIdx;
        auto it = Accessors.find(Constructors.front().GetPortion()->GetPortionId());
        AFL_VERIFY(it != Accessors.end());
        std::shared_ptr<NReader::NCommon::IDataSource> result = Constructors.front().Construct(context, std::move(it->second));
        Accessors.erase(it);
        Constructors.pop_front();
        return result;
    } else {
        Constructors.front().SetIndex(CurrentSourceIdx);
        ++CurrentSourceIdx;
        std::pop_heap(Constructors.begin(), Constructors.end(), TPortionDataConstructor::TComparator(Sorting == ERequestSorting::DESC));
        std::shared_ptr<NReader::NCommon::IDataSource> result = Constructors.back().Construct(context);
        Constructors.pop_back();
        return result;
    }
}

TConstructor::TConstructor(const NOlap::IPathIdTranslator& pathIdTranslator, const IColumnEngine& engine, const ui64 tabletId,
    const std::optional<NOlap::TInternalPathId> internalPathId, const TSnapshot reqSnapshot,
    const std::shared_ptr<NOlap::TPKRangesFilter>& pkFilter, const ERequestSorting sorting)
    : Sorting(sorting) {
    const TColumnEngineForLogs* engineImpl = dynamic_cast<const TColumnEngineForLogs*>(&engine);
    const TVersionedIndex& originalSchemaInfo = engineImpl->GetVersionedIndex();
    for (auto&& i : engineImpl->GetTables()) {
        if (internalPathId && *internalPathId != i.first) {
            continue;
        }
        for (auto&& [_, p] : i.second->GetPortions()) {
            if (reqSnapshot < p->RecordSnapshotMin()) {
                continue;
            }
            if (p->IsRemovedFor(reqSnapshot)) {
                continue;
            }
            Constructors.emplace_back(
                pathIdTranslator.GetUnifiedByInternalVerified(p->GetPathId()), tabletId, p, p->GetSchema(originalSchemaInfo));
            if (!pkFilter->IsUsed(Constructors.back().GetStart(), Constructors.back().GetFinish())) {
                Constructors.pop_back();
            }
        }
    }
    if (Sorting != ERequestSorting::NONE) {
        std::make_heap(Constructors.begin(), Constructors.end(), TPortionDataConstructor::TComparator(Sorting == ERequestSorting::DESC));
    }
}

}   // namespace NKikimr::NOlap::NReader::NSimple::NSysView::NChunks
