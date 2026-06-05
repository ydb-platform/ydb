#include "accessors_ordering.h"

#include <ydb/core/tx/columnshard/data_accessor/request.h>
#include <ydb/core/tx/columnshard/engines/reader/common/conveyor_task.h>
#include <ydb/core/tx/columnshard/engines/reader/simple_reader/iterator/plain_read_data.h>
#include <ydb/core/tx/columnshard/engines/reader/tracing/probes.h>

namespace NKikimr::NOlap::NReader::NCommon {

LWTRACE_USING(YDB_CS_SCAN);

namespace {

TString BuildTopSourceIds(const std::vector<ui64>& ids) {
    TStringBuilder sb;
    ui32 count = 0;
    for (const ui64 id : ids) {
        if (count >= 10) {
            sb << " ...";
            break;
        }
        if (count > 0) {
            sb << ",";
        }
        sb << id;
        ++count;
    }
    return sb;
}

class TApplySourceResult: public IApplyAction {
private:
    using TBase = IDataTasksProcessor::ITask;
    TDataAccessorsResult Accessors;
    const TMonotonic StartTime;

    std::vector<ui64> GetAccessorPortionIds() const {
        std::vector<ui64> ids;
        ids.reserve(Accessors.GetPortions().size());
        for (auto&& [id, _] : Accessors.GetPortions()) {
            ids.emplace_back(id);
        }
        return ids;
    }

public:
    TApplySourceResult(TDataAccessorsResult&& result, const TMonotonic startTime)
        : Accessors(std::move(result))
        , StartTime(startTime)
    {
    }

    virtual bool DoApply(IDataReader& indexedDataRead) override {
        auto* plainReader = static_cast<NSimple::TPlainReadData*>(&indexedDataRead);
        const auto& commonContext = *plainReader->GetSpecialReadContext()->GetCommonContext();
        if (commonContext.GetScanOrbit() &&
            (NLWTrace::HasShuttles(*commonContext.GetScanOrbit()) || LWPROBE_ENABLED(ScanFinishFetchingAccessor))) {
            const TDuration fetchDuration = TMonotonic::Now() - StartTime;
            LWTRACK(ScanFinishFetchingAccessor, *commonContext.GetScanOrbit(),
                commonContext.GetReadMetadataPtrVerifiedAs<NCommon::TReadMetadata>()
                    ->TableMetadataAccessor->GetPathIdVerified()
                    .GetInternalPathId()
                    .GetRawValue(), commonContext.GetReadMetadata()->GetTabletId(), commonContext.GetReadMetadata()->GetTxId(),
                commonContext.GetScanId(), BuildTopSourceIds(GetAccessorPortionIds()), fetchDuration);
        }
        auto& collection = plainReader->MutableScanner().MutableSourcesCollection();
        collection.MutableConstructorsAs<TSourcesConstructorWithAccessorsImpl>().AddAccessors(std::move(Accessors));
        return true;
    }
};

class TLocalPortionAccessorFetchingSubscriber: public IDataAccessorRequestsSubscriber {
private:
    const NActors::TActorId ScanActorId;
    NColumnShard::TCounterGuard Guard;
    const std::shared_ptr<const TAtomicCounter> AbortFlag;
    const TMonotonic StartTime;

    virtual const std::shared_ptr<const TAtomicCounter>& DoGetAbortionFlag() const override {
        return AbortFlag;
    }

    virtual void DoOnRequestsFinished(TDataAccessorsResult&& result) override {
        NActors::TActivationContext::AsActorContext().Send(
            ScanActorId, new NColumnShard::TEvPrivate::TEvTaskProcessedResult(
                             std::make_shared<TApplySourceResult>(std::move(result), StartTime), std::move(Guard)));
    }

public:
    TLocalPortionAccessorFetchingSubscriber(const std::shared_ptr<NReader::NCommon::TSpecialReadContext>& context)
        : ScanActorId(context->GetCommonContext()->GetScanActorId())
        , Guard(context->GetCommonContext()->GetCounters().GetAccessorsForConstructionGuard())
        , AbortFlag(context->GetCommonContext()->GetAbortionFlag())
        , StartTime(TMonotonic::Now())
    {
    }
};

}   // namespace

void TAccessorsFetcherImpl::StartRequest(
    std::shared_ptr<TDataAccessorsRequest>&& request, const std::shared_ptr<NReader::NCommon::TSpecialReadContext>& context) {
    AFL_VERIFY(!InFlightRequests);
    const auto& commonContext = *context->GetCommonContext();
    if (commonContext.GetScanOrbit() && (NLWTrace::HasShuttles(*commonContext.GetScanOrbit()) || LWPROBE_ENABLED(ScanStartFetchingAccessor))) {
        LWTRACK(ScanStartFetchingAccessor, *commonContext.GetScanOrbit(),
            commonContext.GetReadMetadataPtrVerifiedAs<NCommon::TReadMetadata>()
                ->TableMetadataAccessor->GetPathIdVerified()
                .GetInternalPathId()
                .GetRawValue(), commonContext.GetReadMetadata()->GetTabletId(), commonContext.GetReadMetadata()->GetTxId(),
            commonContext.GetScanId(), BuildTopSourceIds(request->GetPortionIds()));
    }
    request->RegisterSubscriber(std::make_shared<TLocalPortionAccessorFetchingSubscriber>(context));
    context->GetCommonContext()->GetDataAccessorsManager()->AskData(std::move(request));
    AFL_VERIFY(++InFlightRequests == 1);
}

}   // namespace NKikimr::NOlap::NReader::NCommon
