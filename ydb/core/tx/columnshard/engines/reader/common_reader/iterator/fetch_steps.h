#pragma once
#include "fetching.h"

#include <ydb/core/tx/limiter/grouped_memory/usage/abstract.h>

namespace NKikimr::NOlap::NReader::NCommon {

class TAllocateMemoryStep: public IFetchingStep {
private:
    using TBase = IFetchingStep;
    class TColumnsPack {
    private:
        YDB_READONLY_DEF(TColumnsSetIds, Columns);
        YDB_READONLY(EMemType, MemType, EMemType::Blob);

    public:
        TColumnsPack(const TColumnsSetIds& columns, const EMemType memType)
            : Columns(columns)
            , MemType(memType) {
        }
    };
    std::vector<TColumnsPack> Packs;
    THashMap<ui32, THashSet<EMemType>> Control;
    const EStageFeaturesIndexes StageIndex;
    const std::optional<ui64> PredefinedSize;

protected:
    class TFetchingStepAllocation: public NGroupedMemoryManager::IAllocation {
    private:
        using TBase = NGroupedMemoryManager::IAllocation;
        std::weak_ptr<IDataSource> Source;
        TFetchingScriptCursor Step;
        NColumnShard::TCounterGuard TasksGuard;
        const EStageFeaturesIndexes StageIndex;
        virtual bool DoOnAllocated(std::shared_ptr<NGroupedMemoryManager::TAllocationGuard>&& guard,
            const std::shared_ptr<NGroupedMemoryManager::IAllocation>& allocation) override;
        virtual void DoOnAllocationImpossible(const TString& errorMessage) override;

    public:
        TFetchingStepAllocation(const std::shared_ptr<IDataSource>& source, const ui64 mem, const TFetchingScriptCursor& step,
            const EStageFeaturesIndexes stageIndex);
    };
    virtual TConclusion<bool> DoExecuteInplace(const std::shared_ptr<IDataSource>& source, const TFetchingScriptCursor& step) const override;
    virtual ui64 GetProcessingDataSize(const std::shared_ptr<IDataSource>& source) const override;
    virtual TString DoDebugString() const override {
        return TStringBuilder() << "stage=" << StageIndex << ";";
    }

public:
    void AddAllocation(const TColumnsSetIds& ids, const EMemType memType) {
        if (!ids.GetColumnsCount()) {
            return;
        }
        for (auto&& i : ids.GetColumnIds()) {
            AFL_VERIFY(Control[i].emplace(memType).second);
        }
        Packs.emplace_back(ids, memType);
    }
    EStageFeaturesIndexes GetStage() const {
        return StageIndex;
    }

    TAllocateMemoryStep(const TColumnsSetIds& columns, const EMemType memType, const EStageFeaturesIndexes stageIndex)
        : TBase("ALLOCATE_MEMORY::" + ::ToString(stageIndex))
        , StageIndex(stageIndex) {
        AddAllocation(columns, memType);
    }

    TAllocateMemoryStep(const ui64 memSize, const EStageFeaturesIndexes stageIndex)
        : TBase("ALLOCATE_MEMORY::" + ::ToString(stageIndex))
        , StageIndex(stageIndex)
        , PredefinedSize(memSize) {
    }
};

class TAssemblerStep: public IFetchingStep {
private:
    using TBase = IFetchingStep;
    YDB_READONLY_DEF(std::shared_ptr<TColumnsSet>, Columns);
    virtual TString DoDebugString() const override {
        return TStringBuilder() << "columns=" << Columns->DebugString() << ";";
    }

public:
    virtual ui64 GetProcessingDataSize(const std::shared_ptr<IDataSource>& source) const override;
    virtual TConclusion<bool> DoExecuteInplace(const std::shared_ptr<IDataSource>& source, const TFetchingScriptCursor& step) const override;
    TAssemblerStep(const std::shared_ptr<TColumnsSet>& columns, const TString& specName = Default<TString>())
        : TBase("ASSEMBLER" + (specName ? "::" + specName : ""))
        , Columns(columns) {
        AFL_VERIFY(Columns);
        AFL_VERIFY(Columns->GetColumnsCount());
    }
};

class TBuildStageResultStep: public IFetchingStep {
private:
    using TBase = IFetchingStep;

public:
    virtual TConclusion<bool> DoExecuteInplace(const std::shared_ptr<IDataSource>& source, const TFetchingScriptCursor& /*step*/) const override;
    TBuildStageResultStep()
        : TBase("BUILD_STAGE_RESULT") {
    }
};

class TOptionalAssemblerStep: public IFetchingStep {
private:
    using TBase = IFetchingStep;
    YDB_READONLY_DEF(std::shared_ptr<TColumnsSet>, Columns);
    virtual TString DoDebugString() const override {
        return TStringBuilder() << "columns=" << Columns->DebugString() << ";";
    }

public:
    virtual ui64 GetProcessingDataSize(const std::shared_ptr<IDataSource>& source) const override;

    virtual TConclusion<bool> DoExecuteInplace(const std::shared_ptr<IDataSource>& source, const TFetchingScriptCursor& step) const override;
    TOptionalAssemblerStep(const std::shared_ptr<TColumnsSet>& columns, const TString& specName = Default<TString>())
        : TBase("OPTIONAL_ASSEMBLER" + (specName ? "::" + specName : ""))
        , Columns(columns) {
        AFL_VERIFY(Columns);
        AFL_VERIFY(Columns->GetColumnsCount());
    }
};

class TColumnBlobsFetchingStep: public IFetchingStep {
private:
    using TBase = IFetchingStep;
    YDB_READONLY_DEF(TColumnsSetIds, Columns);

protected:
    virtual TConclusion<bool> DoExecuteInplace(const std::shared_ptr<IDataSource>& source, const TFetchingScriptCursor& step) const override;
    virtual TString DoDebugString() const override {
        return TStringBuilder() << "columns=" << Columns.DebugString() << ";";
    }

public:
    virtual ui64 GetProcessingDataSize(const std::shared_ptr<IDataSource>& source) const override;
    TColumnBlobsFetchingStep(const TColumnsSetIds& columns)
        : TBase("FETCHING_COLUMNS")
        , Columns(columns) {
        AFL_VERIFY(Columns.GetColumnsCount());
    }
};

}   // namespace NKikimr::NOlap::NReader::NCommon
