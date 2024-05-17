#pragma once

#include <ydb/core/tablet_flat/tablet_flat_executor.h>
#include <ydb/library/accessor/accessor.h>

#include <ydb/core/tx/columnshard/blobs_action/abstract/storages_manager.h>
#include <ydb/core/tx/columnshard/resource_subscriber/task.h>
#include <ydb/library/conclusion/result.h>
#include <library/cpp/object_factory/object_factory.h>

namespace NKikimr::NIceDb {
    class TNiceDb;
}

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

    enum class ENormalizerSequentialId : ui32 {
        Granules = 1,
        Chunks,
        PortionsCleaner,
        TablesCleaner,
        PortionsMetadata,
    };

    class TNormalizationContext {
        YDB_ACCESSOR_DEF(TActorId, ResourceSubscribeActor);
        YDB_ACCESSOR_DEF(TActorId, ShardActor);
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

        virtual bool ApplyOnExecute(NTabletFlatExecutor::TTransactionContext& txc, const TNormalizationController& normalizationContext) const = 0;
        virtual void ApplyOnComplete(const TNormalizationController& normalizationContext) const {
            Y_UNUSED(normalizationContext);
        }

        virtual ui64 GetSize() const = 0;
    };

    class TTrivialNormalizerTask : public INormalizerTask {
        INormalizerChanges::TPtr Changes;
    public:
        TTrivialNormalizerTask(const INormalizerChanges::TPtr& changes)
            : Changes(changes)
        {
            AFL_VERIFY(Changes);
        }

        void Start(const TNormalizationController& /* controller */, const TNormalizationContext& /*nCtx*/) override;
    };

    class TNormalizationController {
    public:
        class TInitContext {
            TIntrusiveConstPtr<TTabletStorageInfo> StorageInfo;
        public:
            TInitContext(TTabletStorageInfo* info)
                : StorageInfo(info)
            {}

            TIntrusiveConstPtr<TTabletStorageInfo> GetStorageInfo() const {
                return StorageInfo;
            }
        };

        class INormalizerComponent {
        public:
            using TPtr = std::shared_ptr<INormalizerComponent>;
            using TFactory = NObjectFactory::TParametrizedObjectFactory<INormalizerComponent, ENormalizerSequentialId, TInitContext>;

            virtual ~INormalizerComponent() {}

            bool HasActiveTasks() const {
                return AtomicGet(ActiveTasksCount) > 0;
            }

            void OnResultReady() {
                AFL_VERIFY(ActiveTasksCount > 0);
                AtomicDecrement(ActiveTasksCount);
            }

            i64 GetActiveTasksCount() const {
                return AtomicGet(ActiveTasksCount);
            }

            virtual ENormalizerSequentialId GetType() const = 0;

            TString GetName() const {
                return ToString(GetType());
            }

            ui32 GetSequentialId() const {
                return (ui32) GetType();
            }

            TConclusion<std::vector<INormalizerTask::TPtr>> Init(const TNormalizationController& controller, NTabletFlatExecutor::TTransactionContext& txc) {
                if (controller.HasLastAppliedNormalizerId() && controller.GetLastAppliedNormalizerIdUnsafe() >= GetSequentialId()) {
                    return std::vector<INormalizerTask::TPtr>();
                }
                auto result = DoInit(controller, txc);
                if (!result.IsSuccess()) {
                    return result;
                }
                AtomicSet(ActiveTasksCount, result.GetResult().size());
                return result;
            }

        private:
            virtual TConclusion<std::vector<INormalizerTask::TPtr>> DoInit(const TNormalizationController& controller, NTabletFlatExecutor::TTransactionContext& txc) = 0;

            TAtomic ActiveTasksCount = 0;
        };
        using TPtr = std::shared_ptr<INormalizerComponent>;

    private:
        std::shared_ptr<IStoragesManager> StoragesManager;
        NOlap::NResourceBroker::NSubscribe::TTaskContext TaskSubscription;

        std::vector<INormalizerComponent::TPtr> Normalizers;
        ui64 CurrentNormalizerIndex = 0;
        std::vector<TNormalizerCounters> Counters;
        YDB_READONLY_OPT(ui32, LastAppliedNormalizerId);

    private:
        void RegisterNormalizer(INormalizerComponent::TPtr normalizer);

    public:
        TNormalizationController(std::shared_ptr<IStoragesManager> storagesManager, const std::shared_ptr<NOlap::NResourceBroker::NSubscribe::TSubscriberCounters>& counters)
            : StoragesManager(storagesManager)
            , TaskSubscription("CS::NORMALIZER", counters) {}

        const NOlap::NResourceBroker::NSubscribe::TTaskContext& GetTaskSubscription() const {
            return TaskSubscription;
        }

        void InitNormalizers(const TInitContext& ctx);
        void UpdateControllerState(NIceDb::TNiceDb& db);
        void InitControllerState(NIceDb::TNiceDb& db);

        ui32 GetLastNormalizerSequentialId() {
            AFL_VERIFY(!Normalizers.empty());
            return Normalizers.back()->GetSequentialId();
        }

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
