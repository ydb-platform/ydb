#include "accessor_callback.h"

#include <ydb/core/tx/columnshard/engines/reader/common_reader/iterator/source.h>
#include <ydb/core/tx/conveyor_composite/usage/service.h>

namespace NKikimr::NOlap::NReader::NCommon {

void TPortionAccessorFetchingSubscriber::DoOnRequestsFinished(TDataAccessorsResult&& result) {
    FOR_DEBUG_LOG(NKikimrServices::COLUMNSHARD_SCAN_EVLOG, Source->AddEvent("facc"));
    if (result.HasErrors()) {
        Source->GetContext()->GetCommonContext()->AbortWithError("has errors on portion accessors restore");
        return;
    }
    AFL_VERIFY(result.GetPortions().size() == 1)("count", result.GetPortions().size());
    Source->SetPortionAccessor(std::move(result.ExtractPortions().begin()->second));
    auto task = std::make_shared<NReader::NCommon::TStepAction>(std::move(Source), std::move(Step), ScanActorId, false);
    NConveyorComposite::TScanServiceOperator::SendTaskToExecute(task, ConveyorProcessId);
}

TPortionAccessorFetchingSubscriber::TPortionAccessorFetchingSubscriber(
    const TFetchingScriptCursor& step, const std::shared_ptr<IDataSource>& source)
    : Step(step)
    , Source(source)
    , Guard(Source->GetContext()->GetCommonContext()->GetCounters().GetFetcherAcessorsGuard()) {
    const auto& commonContext = *Source->GetContext()->GetCommonContext();
    ConveyorProcessId = commonContext.GetConveyorProcessId();
    ScanActorId = commonContext.GetScanActorId();
}

const std::shared_ptr<const TAtomicCounter>& TPortionAccessorFetchingSubscriber::DoGetAbortionFlag() const {
    return Source->GetContext()->GetCommonContext()->GetAbortionFlag();
}

}   // namespace NKikimr::NOlap::NReader::NCommon
