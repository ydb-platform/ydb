#include "accessors_ordering.h"

#include <ydb/core/tx/columnshard/data_accessor/request.h>
#include <ydb/core/tx/columnshard/engines/reader/common/conveyor_task.h>
#include <ydb/core/tx/columnshard/engines/reader/simple_reader/iterator/plain_read_data.h>

namespace NKikimr::NOlap::NReader::NCommon {

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
        auto* plainReader = static_cast<NSimple::TPlainReadData*>(&indexedDataRead);
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

void TAccessorsFetcherImpl::StartRequest(
    std::shared_ptr<TDataAccessorsRequest>&& request, const std::shared_ptr<NReader::NCommon::TSpecialReadContext>& context) {
    AFL_VERIFY(!InFlightRequests);
    request->RegisterSubscriber(std::make_shared<TLocalPortionAccessorFetchingSubscriber>(context));
    context->GetCommonContext()->GetDataAccessorsManager()->AskData(std::move(request));
    AFL_VERIFY(++InFlightRequests == 1);
}

}   // namespace NKikimr::NOlap::NReader::NCommon
