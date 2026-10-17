#include "accessor_callback.h"

#include <ydb/core/tx/columnshard/engines/reader/common_reader/iterator/fetching.h>
#include <ydb/core/tx/columnshard/engines/reader/common_reader/iterator/source.h>
#include <ydb/core/tx/conveyor_composite/usage/service.h>

namespace NKikimr::NOlap::NReader::NCommon {

void TPortionAccessorFetchingSubscriber::TStartJob::Start(std::unique_ptr<TDataSourceLease> sourceLease) {
    Request->RegisterSubscriber(std::make_shared<TPortionAccessorFetchingSubscriber>(Step, std::move(sourceLease)));
    Manager->AskData(Request);
}

void TPortionAccessorFetchingSubscriber::DoOnRequestsFinished(TDataAccessorsResult&& result) {
    auto& source = SourceLease->GetSource();
    FOR_DEBUG_LOG(NKikimrServices::COLUMNSHARD_SCAN_EVLOG, source.AddEvent("facc"));
    if (result.HasErrors()) {
        source.GetContext()->GetCommonContext()->AbortWithError("has errors on portion accessors restore");
        return;
    }

    if (result.HasRemovedData()) {
        source.GetContext()->GetCommonContext()->AbortWithError(
            TStringBuilder{} << "there is a removed accessors restore, count: " << result.GetRemovedData().size());
        return;
    }

    AFL_VERIFY(result.GetPortions().size() == 1)("count", result.GetPortions().size());
    source.SetPortionAccessor(std::move(result.ExtractPortions().begin()->second));
    auto context = source.GetContext()->GetCommonContext();
    auto task = std::make_shared<NReader::NCommon::TStepAction>(std::move(SourceLease), std::move(Step), ScanActorId, false);
    context->SendTaskToExecute(task);
}

TPortionAccessorFetchingSubscriber::TPortionAccessorFetchingSubscriber(
    const TFetchingScriptCursor& step, std::unique_ptr<TDataSourceLease> sourceLease)
    : Step(step)
    , SourceLease(std::move(sourceLease))
    , Guard(SourceLease->GetSource().GetContext()->GetCommonContext()->GetCounters().GetFetcherAcessorsGuard())
    , ScanActorId(SourceLease->GetSource().GetContext()->GetCommonContext()->GetScanActorId())
{
}

const std::shared_ptr<const TAtomicCounter>& TPortionAccessorFetchingSubscriber::DoGetAbortionFlag() const {
    return SourceLease->GetSource().GetContext()->GetCommonContext()->GetAbortionFlag();
}

}   // namespace NKikimr::NOlap::NReader::NCommon
