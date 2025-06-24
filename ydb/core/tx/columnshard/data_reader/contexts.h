#pragma once
#include <ydb/core/tx/columnshard/blobs_reader/task.h>
#include <ydb/core/tx/columnshard/data_accessor/manager.h>
#include <ydb/core/tx/columnshard/engines/portions/data_accessor.h>
#include <ydb/core/tx/columnshard/engines/reader/common_reader/iterator/columns_set.h>
#include <ydb/core/tx/limiter/grouped_memory/usage/abstract.h>
#include <ydb/core/tx/limiter/grouped_memory/usage/service.h>

#include <ydb/library/accessor/accessor.h>

namespace NKikimr::NOlap::NDataFetcher {

enum class EFetchingStage : ui32 {
    Created = 0,
    AskAccessorResources,
    AskDataResources,
    AskGeneralResources,
    AskAccessors,
    ReadBlobs,
    Finished,
    Error
};

class TCurrentContext: TMoveOnly {
private:
    std::optional<std::vector<TPortionDataAccessor>> Accessors;
    YDB_READONLY_DEF(std::vector<std::shared_ptr<NGroupedMemoryManager::TAllocationGuard>>, ResourceGuards);
    std::shared_ptr<NGroupedMemoryManager::TProcessGuard> MemoryProcessGuard;
    std::shared_ptr<NGroupedMemoryManager::TScopeGuard> MemoryProcessScopeGuard;
    std::shared_ptr<NGroupedMemoryManager::TGroupGuard> MemoryProcessGroupGuard;
    static inline TAtomicCounter Counter = 0;
    const ui64 MemoryProcessId = Counter.Inc();
    std::optional<NBlobOperations::NRead::TCompositeReadBlobs> Blobs;

public:
    ui64 GetMemoryProcessId() const {
        return MemoryProcessId;
    }

    ui64 GetMemoryScopeId() const {
        return MemoryProcessScopeGuard->GetScopeId();
    }

    ui64 GetMemoryGroupId() const {
        return MemoryProcessGroupGuard->GetGroupId();
    }

    void SetBlobs(NBlobOperations::NRead::TCompositeReadBlobs&& blobs) {
        AFL_VERIFY(!Blobs);
        Blobs = std::move(blobs);
    }

    NBlobOperations::NRead::TCompositeReadBlobs& MutableBlobs() {
        AFL_VERIFY(!!Blobs);
        return *Blobs;
    }

    NBlobOperations::NRead::TCompositeReadBlobs ExtractBlobs() {
        AFL_VERIFY(!!Blobs);
        auto result = std::move(*Blobs);
        Blobs.reset();
        return result;
    }

    void ResetBlobs() {
        Blobs.reset();
    }

    TCurrentContext() {
        static std::shared_ptr<NGroupedMemoryManager::TStageFeatures> stageFeatures =
            NGroupedMemoryManager::TCompMemoryLimiterOperator::BuildStageFeatures("DEFAULT", 1000000000);

        MemoryProcessGuard = NGroupedMemoryManager::TCompMemoryLimiterOperator::BuildProcessGuard(MemoryProcessId, { stageFeatures });
        MemoryProcessScopeGuard = NGroupedMemoryManager::TCompMemoryLimiterOperator::BuildScopeGuard(MemoryProcessId, 1);
        MemoryProcessGroupGuard = NGroupedMemoryManager::TCompMemoryLimiterOperator::BuildGroupGuard(MemoryProcessId, 1);
    }

    void SetPortionAccessors(std::vector<TPortionDataAccessor>&& acc) {
        AFL_VERIFY(!Accessors);
        Accessors = std::move(acc);
    }

    const std::vector<TPortionDataAccessor> GetPortionAccessors() const {
        AFL_VERIFY(Accessors);
        return *Accessors;
    }

    std::vector<TPortionDataAccessor> ExtractPortionAccessors() {
        AFL_VERIFY(Accessors);
        auto result = std::move(*Accessors);
        Accessors.reset();
        return result;
    }

    void RegisterResourcesGuard(std::shared_ptr<NGroupedMemoryManager::TAllocationGuard>&& g) {
        ResourceGuards.emplace_back(std::move(g));
    }
};

class IFetchCallback {
private:
    virtual void DoOnFinished(TCurrentContext&& context) = 0;
    virtual void DoOnError(const TString& errorMessage) = 0;
    bool IsFinished = false;

public:
    virtual ~IFetchCallback() = default;

    virtual ui64 GetNecessaryDataMemory(
        const std::shared_ptr<NReader::NCommon::TColumnsSetIds>& columnIds, const std::vector<TPortionDataAccessor>& acc) const {
        ui64 memory = 0;
        for (auto&& a : acc) {
            if (columnIds) {
                memory += a.GetColumnBlobBytes(columnIds->GetColumnIds());
            } else {
                memory += a.GetPortionInfo().GetTotalBlobBytes();
            }
        }
        return memory;
    }

    virtual std::optional<ui64> GetMemoryForUsage() const {
        return std::nullopt;
    }

    virtual bool IsAborted() const = 0;
    virtual TString GetClassName() const = 0;

    virtual void OnStageStarting(const EFetchingStage /*stage*/) {
    }

    void OnFinished(TCurrentContext&& context) {
        AFL_VERIFY(!IsFinished);
        IsFinished = true;
        return DoOnFinished(std::move(context));
    }

    void OnError(const TString& errorMessage) {
        AFL_VERIFY(!IsFinished);
        IsFinished = true;
        return DoOnError(errorMessage);
    }
};

class TEnvironment {
private:
    YDB_READONLY_DEF(std::shared_ptr<NDataAccessorControl::IDataAccessorsManager>, DataAccessorsManager);
    YDB_READONLY_DEF(std::shared_ptr<IStoragesManager>, StoragesManager);

public:
    TEnvironment(const std::shared_ptr<NDataAccessorControl::IDataAccessorsManager>& accessorsManager,
        const std::shared_ptr<IStoragesManager>& storagesManager)
        : DataAccessorsManager(accessorsManager)
        , StoragesManager(storagesManager) {
    }
};

class TPortionsDataFetcher;

class IFetchingStep {
public:
    enum class EStepResult {
        Continue,
        Detached,
        Error
    };

private:
    virtual EStepResult DoExecute(const std::shared_ptr<TPortionsDataFetcher>& fetchingContext) const = 0;

public:
    virtual ~IFetchingStep() = default;

    [[nodiscard]] EStepResult Execute(const std::shared_ptr<TPortionsDataFetcher>& fetchingContext) const;
};

class TScript {
private:
    const TString ClassName;
    std::vector<std::shared_ptr<IFetchingStep>> Steps;

public:
    const TString& GetClassName() const {
        return ClassName;
    }

    TScript(std::vector<std::shared_ptr<IFetchingStep>>&& steps, const TString& className)
        : ClassName(className)
        , Steps(std::move(steps)) {
    }

    const std::shared_ptr<IFetchingStep>& GetStep(const ui32 index) const {
        AFL_VERIFY(index < Steps.size());
        return Steps[index];
    }

    ui32 GetStepsCount() const {
        return Steps.size();
    }
};

class TScriptExecution {
private:
    std::shared_ptr<TScript> Script;
    ui32 StepIndex = 0;

public:
    TScriptExecution(const std::shared_ptr<TScript>& script)
        : Script(script) {
        AFL_VERIFY(Script);
    }

    const TString& GetScriptClassName() const {
        return Script->GetClassName();
    }

    const std::shared_ptr<IFetchingStep>& GetCurrentStep() const {
        return Script->GetStep(StepIndex);
    }

    bool IsFinished() const {
        return StepIndex == Script->GetStepsCount();
    }

    void Next() {
        AFL_VERIFY(!IsFinished());
        ++StepIndex;
    }
};

class TFullPortionInfo {
private:
    YDB_READONLY_DEF(TPortionInfo::TConstPtr, PortionInfo);
    YDB_READONLY_DEF(ISnapshotSchema::TPtr, Schema);

public:
    TFullPortionInfo(const TPortionInfo::TConstPtr& portionInfo, const ISnapshotSchema::TPtr& schema)
        : PortionInfo(portionInfo)
        , Schema(schema) {
    }
};

class TRequestInput {
private:
    YDB_READONLY_DEF(std::vector<std::shared_ptr<TFullPortionInfo>>, Portions);
    YDB_READONLY_DEF(std::shared_ptr<ISnapshotSchema>, ActualSchema);
    YDB_READONLY(NBlobOperations::EConsumer, Consumer, NBlobOperations::EConsumer::UNDEFINED);
    YDB_READONLY_DEF(TString, ExternalTaskId);

public:
    TRequestInput(const std::vector<TPortionInfo::TConstPtr>& portions, const std::shared_ptr<TVersionedIndex>& versions,
        const NBlobOperations::EConsumer consumer, const TString& externalTaskId)
        : Consumer(consumer)
        , ExternalTaskId(externalTaskId) {
        AFL_VERIFY(portions.size());
        ActualSchema = versions->GetLastSchema();
        for (auto&& i : portions) {
            Portions.emplace_back(std::make_shared<TFullPortionInfo>(i, versions->GetSchemaVerified(i->GetSchemaVersionVerified())));
        }
    }
};

}   // namespace NKikimr::NOlap::NDataFetcher
