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
    ui64 CachedSourceId = 0;
    ui64 CachedBlobBytes = 0;
    ui64 CachedRawBytes = 0;
    ui32 CachedFilteredRows = 0;
    ui32 CachedTotalRows = 0;
    ui64 CachedTotalReservedBytes = 0;

    void CacheSourceStats();

protected:
    virtual bool DoApply(IDataReader& owner) override;
    virtual TConclusion<bool> DoExecuteImpl() override;

public:
    virtual TString GetTaskClassIdentifier() const override {
        return "STEP_ACTION";
    }

    virtual ui64 GetSourceId() const override {
        return CachedSourceId;
    }

    virtual ui64 GetBlobBytes() const override {
        return CachedBlobBytes;
    }

    virtual ui64 GetRawBytes() const override {
        return CachedRawBytes;
    }

    virtual ui32 GetFilteredRows() const override {
        return CachedFilteredRows;
    }

    virtual ui32 GetTotalRows() const override {
        return CachedTotalRows;
    }

    virtual ui64 GetTotalReservedBytes() const override {
        return CachedTotalReservedBytes;
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
    THashMap<ui32, std::shared_ptr<TFetchingStepSignals>> Signals;
    const std::shared_ptr<TFetchingStepSignals>& GetSignals(const ui32 nodeId) const;
    void ReportTracing(const std::shared_ptr<IDataSource>& source, const TDuration executionDurationMs, const TString& currentExecutionResult) const;

public:
    virtual TConclusion<bool> DoExecuteInplace(const std::shared_ptr<IDataSource>& source, const TFetchingScriptCursor& step) const override;

    TProgramStep(const std::shared_ptr<NArrow::NSSA::NGraph::NExecution::TCompiledGraph>& program)
        : TBase("PROGRAM_EXECUTION")
        , Program(program) {
        for (auto&& i : Program->GetNodes()) {
            Signals.emplace(i.first, TFetchingStepsSignalsCollection::GetSignals(i.second->GetSignalCategoryName()));
        }
    }
};

}   // namespace NKikimr::NOlap::NReader::NCommon
