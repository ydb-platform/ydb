#include "constructor.h"
#include "default_fetching.h"
#include "fetch_steps.h"
#include "fetching.h"
#include "source.h"
#include "sub_columns_fetching.h"

#include <ydb/core/kqp/runtime/scheduler/new/kqp_schedulable_actor.h>
#include <ydb/core/tx/columnshard/blobs_reader/actor.h>

#include <util/string/builder.h>
#include <yql/essentials/minikql/mkql_terminator.h>

namespace NKikimr::NOlap::NReader::NCommon {

bool TStepAction::DoApply(IDataReader& owner) {
    AFL_VERIFY(FinishedFlag);
    AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "apply");
    Source->StartSyncSection();
    Source->OnSourceFetchingFinishedSafe(owner, Source);
    return true;
}

TConclusion<bool> TStepAction::DoExecuteImpl() {
    FOR_DEBUG_LOG(NKikimrServices::COLUMNSHARD_SCAN_EVLOG, Source->AddEvent("step_action"));
    if (Source->GetContext()->IsAborted()) {
        AFL_VERIFY(!FinishedFlag);
        FinishedFlag = true;
        return true;
    }
    auto executeResult = Cursor.Execute(Source);
    if (executeResult.IsFail()) {
        AFL_VERIFY(!FinishedFlag);
        FinishedFlag = true;
        return executeResult;
    }
    if (*executeResult) {
        AFL_VERIFY(!FinishedFlag);
        FinishedFlag = true;
    }
    return FinishedFlag;
}

TStepAction::TStepAction(
    std::shared_ptr<IDataSource>&& source, TFetchingScriptCursor&& cursor, const NActors::TActorId& ownerActorId, const bool changeSyncSection)
    : TBase(ownerActorId, source->GetContext()->GetCommonContext()->GetCounters().GetAssembleTasksGuard())
    , Source(std::move(source))
    , Cursor(std::move(cursor)) {
    if (changeSyncSection) {
        Source->StartAsyncSection();
    } else {
        Source->CheckAsyncSection();
    }
}

TConclusion<bool> TProgramStep::DoExecuteInplace(const std::shared_ptr<IDataSource>& source, const TFetchingScriptCursor& step) const {
    const bool started = !source->GetExecutionContext().HasProgramIterator();
    if (!source->GetExecutionContext().HasProgramIterator()) {
        source->MutableExecutionContext().Start(source, Program, step);
    }
    auto iterator = source->GetExecutionContext().GetProgramIteratorVerified();
    if (!started) {
        iterator->Next();
        source->MutableExecutionContext().OnFinishProgramStepExecution();
    }
    while (iterator->IsValid()) {
        {
            auto conclusion = iterator->Next();
            if (conclusion.IsFail()) {
                return conclusion;
            }
        }
        if (!source->GetExecutionContext().GetExecutionVisitorVerified()->GetExecutionNode()) {
            if (iterator->IsValid()) {
                GetSignals(iterator->GetCurrentNodeId())->OnSkipGraphNode(source->GetRecordsCount());
                source->GetContext()->GetCommonContext()->GetCounters().OnSkipGraphNode(iterator->GetCurrentNode().GetIdentifier());
            }
            continue;
        }
        AFL_VERIFY(source->GetExecutionContext().GetExecutionVisitorVerified()->GetExecutionNode()->GetIdentifier() == iterator->GetCurrentNodeId());
        source->MutableExecutionContext().OnStartProgramStepExecution(iterator->GetCurrentNodeId(), GetSignals(iterator->GetCurrentNodeId()));
        auto signals = GetSignals(iterator->GetCurrentNodeId());
        const TMonotonic start = TMonotonic::Now();
        auto conclusion = source->GetExecutionContext().GetExecutionVisitorVerified()->Execute();
        source->GetContext()->GetCommonContext()->GetCounters().AddExecutionDuration(TMonotonic::Now() - start);
        signals->AddExecutionDuration(TMonotonic::Now() - start);
        if (conclusion.IsFail()) {
            source->MutableExecutionContext().OnFailedProgramStepExecution();
            return conclusion;
        } else if (*conclusion == NArrow::NSSA::IResourceProcessor::EExecutionResult::InBackground) {
            return false;
        }
        source->MutableExecutionContext().OnFinishProgramStepExecution();
        GetSignals(iterator->GetCurrentNodeId())->OnExecuteGraphNode(source->GetRecordsCount());
        source->GetContext()->GetCommonContext()->GetCounters().OnExecuteGraphNode(iterator->GetCurrentNode().GetIdentifier());
        if (source->GetExecutionContext().GetExecutionVisitorVerified()->MutableContext().GetResources().GetRecordsCountActualOptional() == 0) {
            source->GetExecutionContext().GetExecutionVisitorVerified()->MutableContext().MutableResources().Clear();
            break;
        }
    }
    FOR_DEBUG_LOG(NKikimrServices::COLUMNSHARD_SCAN_EVLOG, source->AddEvent("fgraph"));
    AFL_DEBUG(NKikimrServices::SSA_GRAPH_EXECUTION)(
        "graph_constructed", Program->DebugDOT(source->GetExecutionContext().GetExecutionVisitorVerified()->GetExecutedIds()));
    source->MutableStageData().ReturnTable(source->GetExecutionContext().GetExecutionVisitorVerified()->MutableContext().ExtractResources());

    return true;
}

const std::shared_ptr<TFetchingStepSignals>& TProgramStep::GetSignals(const ui32 nodeId) const {
    auto it = Signals.find(nodeId);
    AFL_VERIFY(it != Signals.end())("node_id", nodeId);
    return it->second;
}

}   // namespace NKikimr::NOlap::NReader::NCommon
