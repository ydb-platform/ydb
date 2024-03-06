#pragma once

#include <ydb/core/tablet_flat/tablet_flat_executor.h>
#include <ydb/library/accessor/accessor.h>

#include <ydb/core/tx/columnshard/blobs_action/abstract/storages_manager.h>
#include <ydb/core/tx/columnshard/resource_subscriber/task.h>

#include <ydb/library/conclusion/result.h>

namespace NKikimr::NOlap {

    class TNormalizerCounters: public NColumnShard::TCommonCountersOwner {
        using TBase = NColumnShard::TCommonCountersOwner;

        NMonitoring::TDynamicCounters::TCounterPtr ObjectsCount;
        NMonitoring::TDynamicCounters::TCounterPtr StartedCount;
        NMonitoring::TDynamicCounters::TCounterPtr FinishedCount;
        NMonitoring::TDynamicCounters::TCounterPtr FailedCount;
    public:
        TNormalizerCounters(const TString& normalizerName)
            : TBase("Normalizer")
        {
            DeepSubGroup("normalizer", normalizerName);

            ObjectsCount = TBase::GetDeriviative("Objects/Count");
            StartedCount = TBase::GetDeriviative("Started");
            FinishedCount = TBase::GetDeriviative("Finished");
            FailedCount = TBase::GetDeriviative("Failed");
        }

        void CountObjects(const ui64 objectsCount) const {
            ObjectsCount->Add(objectsCount);
        }

        void OnNormalizerStart() const {
            StartedCount->Add(1);
        }

        void OnNormalizerFinish() const {
            FinishedCount->Add(1);
        }

        void OnNormalizerFails() const {
            FailedCount->Add(1);
        }
    };

    class TNormalizationContext {
        YDB_ACCESSOR_DEF(TActorId, ResourceSubscribeActor);
        YDB_ACCESSOR_DEF(TActorId, ColumnshardActor);
        std::shared_ptr<NOlap::NResourceBroker::NSubscribe::TResourcesGuard> ResourcesGuard;
    public:
        void SetResourcesGuard(std::shared_ptr<NOlap::NResourceBroker::NSubscribe::TResourcesGuard> rg) {
            ResourcesGuard = rg;
        }
    };

    class TNormalizationController;

    class INormalizerTask {
    public:
        using TPtr = std::shared_ptr<INormalizerTask>;
        virtual ~INormalizerTask() {}

        virtual void Start(const TNormalizationController& controller, const TNormalizationContext& nCtx) = 0;
    };

    class INormalizerChanges {
    public:
        using TPtr = std::shared_ptr<INormalizerChanges>;
        virtual ~INormalizerChanges() {}

        virtual bool Apply(NTabletFlatExecutor::TTransactionContext& txc, const TNormalizationController& normalizationContext) const = 0;
    };

    class INormalizerComponent {
    public:
        using TPtr = std::shared_ptr<INormalizerComponent>;

        virtual ~INormalizerComponent() {}

        bool WaitResult() const {
            return AtomicGet(ActiveTasksCount) > 0;
        }

        void OnResultReady() {
            AFL_VERIFY(ActiveTasksCount > 0);
            AtomicDecrement(ActiveTasksCount);
        }

        virtual const TString& GetName() const = 0;
        virtual TConclusion<std::vector<INormalizerTask::TPtr>> Init(const TNormalizationController& controller, NTabletFlatExecutor::TTransactionContext& txc) = 0;
    protected:
        TAtomic ActiveTasksCount = 0;
    };

    class TNormalizationController {
        std::shared_ptr<IStoragesManager> StoragesManager;
        NOlap::NResourceBroker::NSubscribe::TTaskContext TaskSubscription;

        std::vector<NOlap::INormalizerComponent::TPtr> Normalizers;
        ui64 CurrentNormalizerIndex = 0;
        std::vector<TNormalizerCounters> Counters;

    public:
        TNormalizationController(std::shared_ptr<IStoragesManager> storagesManager, const std::shared_ptr<NOlap::NResourceBroker::NSubscribe::TSubscriberCounters>& counters)
            : StoragesManager(storagesManager)
            , TaskSubscription("CS::NORMALIZER", counters) {}

        const NOlap::NResourceBroker::NSubscribe::TTaskContext& GetTaskSubscription() const {
            return TaskSubscription;
        }

        void RegisterNormalizer(INormalizerComponent::TPtr normalizer);

        std::shared_ptr<IStoragesManager> GetStoragesManager() const {
            AFL_VERIFY(!!StoragesManager);
            return StoragesManager;
        }

        TString DebugString() const {
            return TStringBuilder() << "normalizers_count=" << Normalizers.size()
                                    << ";current_normalizer_idx=" << CurrentNormalizerIndex
                                    << ";current_normalizer=" << (CurrentNormalizerIndex < Normalizers.size() ? Normalizers[CurrentNormalizerIndex]->GetName() : "");
        }

        const INormalizerComponent::TPtr& GetNormalizer() const;
        bool IsNormalizationFinished() const;
        bool SwitchNormalizer();
        const TNormalizerCounters& GetCounters() const;
    };
}
