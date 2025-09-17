#pragma once

#include <ydb/core/tx/columnshard/counters/scan.h>
#include <ydb/core/tx/columnshard/engines/reader/common/conveyor_task.h>
#include <ydb/core/tx/columnshard/engines/reader/common_reader/common/columns_set.h>
#include <ydb/core/tx/columnshard/engines/reader/common_reader/common/script_cursor.h>

#include <ydb/library/accessor/accessor.h>
#include <ydb/library/actors/core/log.h>
#include <ydb/library/actors/core/monotonic.h>
#include <ydb/library/conclusion/result.h>
#include <ydb/library/signals/owner.h>

#include <util/datetime/base.h>

namespace NKikimr::NOlap::NReader::NCommon {

class TStepAction: public IDataTasksProcessor::ITask {
private:
    using TBase = IDataTasksProcessor::ITask;
    std::shared_ptr<IDataSource> Source;
    TFetchingScriptCursor Cursor;
    bool FinishedFlag = false;

protected:
    virtual bool DoApply(IDataReader& owner) override;
    virtual TConclusion<bool> DoExecuteImpl() override;

public:
    virtual TString GetTaskClassIdentifier() const override {
        return "STEP_ACTION";
    }

    template <class T>
    TStepAction(std::shared_ptr<T>&& source, TFetchingScriptCursor&& cursor, const NActors::TActorId& ownerActorId, const bool changeSyncSection)
        : TStepAction(std::static_pointer_cast<IDataSource>(source), std::move(cursor), ownerActorId, changeSyncSection) {
    }
    TStepAction(std::shared_ptr<IDataSource>&& source, TFetchingScriptCursor&& cursor, const NActors::TActorId& ownerActorId,
        const bool changeSyncSection);
};

class TProgramStep: public IFetchingStep {
private:
    using TBase = IFetchingStep;
    const std::shared_ptr<NArrow::NSSA::NGraph::NExecution::TCompiledGraph> Program;
    std::vector<std::shared_ptr<TFetchingStepSignals>> Signals;
    const std::shared_ptr<TFetchingStepSignals>& GetSignals(const ui32 nodeId) const;

public:
    virtual TConclusion<bool> DoExecuteInplace(const std::shared_ptr<IDataSource>& source, const TFetchingScriptCursor& step) const override;

    TProgramStep(const std::shared_ptr<NArrow::NSSA::NGraph::NExecution::TCompiledGraph>& program)
        : TBase("PROGRAM_EXECUTION")
        , Program(program) {
        Signals.resize(Program->GetNodesCountReserve());
        for (auto&& i : Program->GetNodes()) {
            AFL_VERIFY(i.first < Signals.size());
            Signals[i.first] = TFetchingStepsSignalsCollection::GetSignals(i.second->GetSignalCategoryName());
        }
    }
};

}   // namespace NKikimr::NOlap::NReader::NCommon
