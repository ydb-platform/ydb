#pragma once

#include <ydb/core/tx/columnshard/normalizer/abstract/abstract.h>
#include <ydb/core/tx/columnshard/engines/scheme/abstract_scheme.h>
#include <ydb/core/tx/columnshard/blobs_action/counters/storage.h>

#include <ydb/core/tx/conveyor/usage/abstract.h>
#include <ydb/core/tx/conveyor/usage/service.h>
#include <ydb/core/tx/columnshard/blobs_reader/task.h>
#include <ydb/core/tx/columnshard/columnshard_private_events.h>

#include <ydb/core/tx/columnshard/defs.h>


namespace NKikimr::NColumnShard {
    class TTablesManager;
}
namespace NKikimr::NOlap {

template <class TConveyorTask>
class TReadPortionsTask: public NOlap::NBlobOperations::NRead::ITask {
private:
    using TBase = NOlap::NBlobOperations::NRead::ITask;
    typename TConveyorTask::TDataContainer Data;
    std::shared_ptr<THashMap<ui64, ISnapshotSchema::TPtr>> Schemas;
    TNormalizationContext NormContext;

public:
     TReadPortionsTask(const TNormalizationContext& nCtx, const std::vector<std::shared_ptr<IBlobsReadingAction>>& actions, typename TConveyorTask::TDataContainer&& data, std::shared_ptr<THashMap<ui64, ISnapshotSchema::TPtr>> schemas)
        : TBase(actions, "CS::NORMALIZER")
        , Data(std::move(data))
        , Schemas(std::move(schemas))
        , NormContext(nCtx)
    {
    }

protected:
    virtual void DoOnDataReady(const std::shared_ptr<NOlap::NResourceBroker::NSubscribe::TResourcesGuard>& resourcesGuard) override {
        NormContext.SetResourcesGuard(resourcesGuard);
        std::shared_ptr<NConveyor::ITask> task = std::make_shared<TConveyorTask>(std::move(ExtractBlobsData()), NormContext, std::move(Data), Schemas);
        NConveyor::TCompServiceOperator::SendTaskToExecute(task);
    }

    virtual bool DoOnError(const TString& storageId, const TBlobRange& range, const IBlobsReadingAction::TErrorStatus& status) override {
        Y_UNUSED(status, range, storageId);
        return false;
    }

public:
    using TBase::TBase;
};

template <class TConveyorTask>
class TPortionsNormalizerTask : public INormalizerTask {
    typename TConveyorTask::TDataContainer Package;
    std::shared_ptr<THashMap<ui64, ISnapshotSchema::TPtr>> Schemas;
public:
    TPortionsNormalizerTask(typename TConveyorTask::TDataContainer&& package)
        : Package(std::move(package))
    {}

    TPortionsNormalizerTask(typename TConveyorTask::TDataContainer&& package, const std::shared_ptr<THashMap<ui64, ISnapshotSchema::TPtr>> schemas)
        : Package(std::move(package))
        , Schemas(schemas)
    {}

    void Start(const TNormalizationController& controller, const TNormalizationContext& nCtx) override {
        controller.GetCounters().CountObjects(Package.size());
        auto readingAction = controller.GetStoragesManager()->GetInsertOperator()->StartReadingAction(NBlobOperations::EConsumer::NORMALIZER);
        ui64 memSize = 0;
        for (auto&& data : Package) {
            TConveyorTask::FillBlobRanges(readingAction, data);
            memSize += TConveyorTask::GetMemSize(data);
        }
        std::vector<std::shared_ptr<IBlobsReadingAction>> actions = {readingAction};
        NOlap::NResourceBroker::NSubscribe::ITask::StartResourceSubscription(
            nCtx.GetResourceSubscribeActor(),std::make_shared<NOlap::NBlobOperations::NRead::ITask::TReadSubscriber>(
                    std::make_shared<TReadPortionsTask<TConveyorTask>>(nCtx, actions, std::move(Package), Schemas), 1, memSize, "CS::NORMALIZER", controller.GetTaskSubscription()));
    }
};

class TPortionsNormalizerBase : public TNormalizationController::INormalizerComponent {
public:
    TPortionsNormalizerBase(const TNormalizationController::TInitContext& info)
        : DsGroupSelector(info.GetStorageInfo())
    {}

    TConclusionStatus InitColumns(
        const NColumnShard::TTablesManager& tablesManager, NIceDb::TNiceDb& db, THashMap<ui64, TPortionInfoConstructor>& portions);
    TConclusionStatus InitIndexes(NIceDb::TNiceDb& db, THashMap<ui64, TPortionInfoConstructor>& portions);

    virtual TConclusion<std::vector<INormalizerTask::TPtr>> DoInit(
        const TNormalizationController& controller, NTabletFlatExecutor::TTransactionContext& txc) override final;

protected:
    virtual INormalizerTask::TPtr BuildTask(std::vector<std::shared_ptr<TPortionInfo>>&& portions, std::shared_ptr<THashMap<ui64, ISnapshotSchema::TPtr>> schemas) const = 0;
    virtual TConclusion<bool> DoInitImpl(const TNormalizationController& controller, NTabletFlatExecutor::TTransactionContext& txc)  = 0;

    virtual bool CheckPortion(const NColumnShard::TTablesManager& tablesManager, const TPortionInfo& /*portionInfo*/) const = 0;

    virtual std::set<ui32> GetColumnsFilter(const ISnapshotSchema::TPtr& schema) const {
        return schema->GetPkColumnsIds();
    }

private:
    NColumnShard::TBlobGroupSelector DsGroupSelector;
};

}
