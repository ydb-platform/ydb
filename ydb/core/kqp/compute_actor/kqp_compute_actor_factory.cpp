#include "kqp_compute_actor_factory.h"
#include "kqp_compute_actor.h"

#include <ydb/core/kqp/common/kqp_resolve.h>
#include <ydb/core/kqp/rm_service/kqp_resource_estimation.h>

namespace NKikimr::NKqp::NComputeActor {


struct TMemoryQuotaManager : public NYql::NDq::TGuaranteeQuotaManager {

    TMemoryQuotaManager(std::shared_ptr<NRm::IKqpResourceManager> resourceManager
        , NRm::EKqpMemoryPool memoryPool
        , std::shared_ptr<IKqpNodeState> state
        , TIntrusivePtr<NRm::TTxState> tx
        , TIntrusivePtr<NRm::TTaskState> task
        , ui64 limit
        , ui64 reasonableSpillingTreshold)
    : NYql::NDq::TGuaranteeQuotaManager(limit, limit)
    , ResourceManager(std::move(resourceManager))
    , MemoryPool(memoryPool)
    , State(std::move(state))
    , Tx(std::move(tx))
    , Task(std::move(task))
    , ReasonableSpillingTreshold(reasonableSpillingTreshold)
    {
    }

    ~TMemoryQuotaManager() override {
        if (State) {
            State->OnTaskTerminate(Tx->TxId, Task->TaskId, Success);
        }

        ResourceManager->FreeResources(Tx, Task);
    }

    bool AllocateExtraQuota(ui64 extraSize) override {
        auto result = ResourceManager->AllocateResources(Tx, Task,
            NRm::TKqpResourcesRequest{.MemoryPool = MemoryPool, .Memory = extraSize});

        if (!result) {
            AFL_WARN(NKikimrServices::KQP_COMPUTE)
                ("problem", "cannot_allocate_memory")
                ("tx_id", Tx->TxId)
                ("task_id", Task->TaskId)
                ("memory", extraSize);

            return false;
        }

        return true;
    }

    void FreeExtraQuota(ui64 extraSize) override {
        NRm::TKqpResourcesRequest request = NRm::TKqpResourcesRequest{.MemoryPool = MemoryPool, .Memory = extraSize};
        ResourceManager->FreeResources(Tx, Task, Task->FitRequest(request));
    }

    bool IsReasonableToUseSpilling() const override {
        return Tx->GetExtraMemoryAllocatedSize() >= ReasonableSpillingTreshold;
    }

    TString MemoryConsumptionDetails() const override {
        return Tx->ToString();
    }

    void TerminateHandler(bool success, const NYql::TIssues& issues) {
        AFL_DEBUG(NKikimrServices::KQP_COMPUTE)
            ("problem", "finish_compute_actor")
            ("tx_id", Tx->TxId)("task_id", Task->TaskId)("success", success)("message", issues.ToOneLineString());
        Success = success;
    }

    std::shared_ptr<NRm::IKqpResourceManager> ResourceManager;
    NRm::EKqpMemoryPool MemoryPool;
    std::shared_ptr<IKqpNodeState> State;
    TIntrusivePtr<NRm::TTxState> Tx;
    TIntrusivePtr<NRm::TTaskState> Task;
    bool Success = true;
    ui64 ReasonableSpillingTreshold = 0;
};

class TKqpCaFactory : public IKqpNodeComputeActorFactory {
    std::shared_ptr<NRm::IKqpResourceManager> ResourceManager_;
    NYql::NDq::IDqAsyncIoFactory::TPtr AsyncIoFactory;
    const std::optional<TKqpFederatedQuerySetup> FederatedQuerySetup;

    std::atomic<ui64> MkqlLightProgramMemoryLimit = 0;
    std::atomic<ui64> MkqlHeavyProgramMemoryLimit = 0;
    std::atomic<ui64> MinChannelBufferSize = 0;
    std::atomic<ui64> ReasonableSpillingTreshold = 0;

public:
    TKqpCaFactory(const NKikimrConfig::TTableServiceConfig::TResourceManager& config,
        std::shared_ptr<NRm::IKqpResourceManager> resourceManager,
        NYql::NDq::IDqAsyncIoFactory::TPtr asyncIoFactory,
        const std::optional<TKqpFederatedQuerySetup> federatedQuerySetup)
        : ResourceManager_(resourceManager)
        , AsyncIoFactory(asyncIoFactory)
        , FederatedQuerySetup(federatedQuerySetup)
    {
        ApplyConfig(config);
    }

    void ApplyConfig(const NKikimrConfig::TTableServiceConfig::TResourceManager& config)
    {
        MkqlLightProgramMemoryLimit.store(config.GetMkqlLightProgramMemoryLimit());
        MkqlHeavyProgramMemoryLimit.store(config.GetMkqlHeavyProgramMemoryLimit());
        MinChannelBufferSize.store(config.GetMinChannelBufferSize());
        ReasonableSpillingTreshold.store(config.GetReasonableSpillingTreshold());
    }

    TActorStartResult CreateKqpComputeActor(TCreateArgs&& args) {
        NYql::NDq::TComputeMemoryLimits memoryLimits;
        memoryLimits.ChannelBufferSize = 0;
        memoryLimits.MkqlLightProgramMemoryLimit = MkqlLightProgramMemoryLimit.load();
        memoryLimits.MkqlHeavyProgramMemoryLimit = MkqlHeavyProgramMemoryLimit.load();

        auto estimation = ResourceManager_->EstimateTaskResources(*args.Task, args.NumberOfTasks);
        NRm::TKqpResourcesRequest resourcesRequest;
        resourcesRequest.MemoryPool = args.MemoryPool;
        resourcesRequest.ExecutionUnits = 1;
        resourcesRequest.Memory =  memoryLimits.MkqlLightProgramMemoryLimit;

        TIntrusivePtr<NRm::TTaskState> task = MakeIntrusive<NRm::TTaskState>(args.Task->GetId(), args.TxInfo->CreatedAt);

        auto rmResult = ResourceManager_->AllocateResources(
            args.TxInfo, task, resourcesRequest);

        if (!rmResult) {
            return NRm::TKqpRMAllocateResult{rmResult};
        }

        {
            ui32 inputChannelsCount = 0;
            for (auto&& i : args.Task->GetInputs()) {
                inputChannelsCount += i.ChannelsSize();
            }

            memoryLimits.ChannelBufferSize = std::max<ui32>(estimation.ChannelBufferMemoryLimit / std::max<ui32>(1, inputChannelsCount), MinChannelBufferSize.load());
            memoryLimits.OutputChunkMaxSize = args.OutputChunkMaxSize;
            AFL_DEBUG(NKikimrServices::KQP_COMPUTE)("event", "channel_info")
                ("ch_size", estimation.ChannelBufferMemoryLimit)
                ("ch_count", estimation.ChannelBuffersCount)
                ("ch_limit", memoryLimits.ChannelBufferSize)
                ("inputs", args.Task->InputsSize())
                ("input_channels_count", inputChannelsCount);
        }

        auto& taskOpts = args.Task->GetProgram().GetSettings();
        auto limit = taskOpts.GetHasMapJoin() || taskOpts.GetHasStateAggregation()
            ? memoryLimits.MkqlHeavyProgramMemoryLimit
            : memoryLimits.MkqlLightProgramMemoryLimit;

        memoryLimits.MemoryQuotaManager = std::make_shared<TMemoryQuotaManager>(
            ResourceManager_,
            args.MemoryPool,
            std::move(args.State),
            std::move(args.TxInfo),
            std::move(task),
            limit,
            ReasonableSpillingTreshold.load());

        auto runtimeSettings = args.RuntimeSettings;
        runtimeSettings.ExtraMemoryAllocationPool = args.MemoryPool;
        runtimeSettings.UseSpilling = args.WithSpilling;
        runtimeSettings.StatsMode = args.StatsMode;

        if (args.Deadline) {
            runtimeSettings.Timeout = args.Deadline - TAppData::TimeProvider->Now();
        }

        if (args.RlPath) {
            runtimeSettings.RlPath = args.RlPath;
        }

        NYql::NDq::IMemoryQuotaManager::TWeakPtr memoryQuotaManager = memoryLimits.MemoryQuotaManager;
        runtimeSettings.TerminateHandler = [memoryQuotaManager]
            (bool success, const NYql::TIssues& issues) {
                auto manager = memoryQuotaManager.lock();
                if (manager) {
                    static_cast<TMemoryQuotaManager*>(manager.get())->TerminateHandler(success, issues);
                }
            };

        NKikimrTxDataShard::TKqpTransaction::TScanTaskMeta meta;
        const auto tableKindExtract = [](const NKikimrTxDataShard::TKqpTransaction::TScanTaskMeta& meta) {
            ETableKind result = (ETableKind)meta.GetTable().GetTableKind();
            if (result == ETableKind::Unknown) {
                // For backward compatibility
                result = meta.GetTable().GetSysViewInfo().empty() ? ETableKind::Datashard : ETableKind::SysView;
            }
            return result;
        };

        ETableKind tableKind = ETableKind::Unknown;
        if (args.Task->HasMetaId()) {
            YQL_ENSURE(args.ComputesByStages);
            YQL_ENSURE(args.ComputesByStages->GetMetaById(*args.Task, meta) || args.Task->GetMeta().UnpackTo(&meta), "cannot take meta on MetaId exists in tasks");
            tableKind = tableKindExtract(meta);
        } else if (args.Task->GetMeta().UnpackTo(&meta)) {
            tableKind = tableKindExtract(meta);
        }

        if (tableKind == ETableKind::Datashard || tableKind == ETableKind::Olap) {
            YQL_ENSURE(args.ComputesByStages);
            auto& info = args.ComputesByStages->UpsertTaskWithScan(*args.Task, meta, !AppData()->FeatureFlags.GetEnableSeparationComputeActorsFromRead());
            IActor* computeActor = CreateKqpScanComputeActor(args.ExecuterId, args.TxId, args.Task,
                AsyncIoFactory, runtimeSettings, memoryLimits,
                std::move(args.TraceId), std::move(args.Arena));
            TActorId result = TlsActivationContext->Register(computeActor);
            info.MutableActorIds().emplace_back(result);
            return result;
        } else {
            std::shared_ptr<TGUCSettings> GUCSettings;
            if (!args.SerializedGUCSettings.empty()) {
                GUCSettings = std::make_shared<TGUCSettings>(args.SerializedGUCSettings);
            }
            IActor* computeActor = ::NKikimr::NKqp::CreateKqpComputeActor(args.ExecuterId, args.TxId, args.Task, AsyncIoFactory,
                runtimeSettings, memoryLimits, std::move(args.TraceId), std::move(args.Arena), FederatedQuerySetup, GUCSettings);
            return args.ShareMailbox ? TlsActivationContext->AsActorContext().RegisterWithSameMailbox(computeActor) :
                TlsActivationContext->AsActorContext().Register(computeActor);
        }
    }
};

std::shared_ptr<IKqpNodeComputeActorFactory> MakeKqpCaFactory(const NKikimrConfig::TTableServiceConfig::TResourceManager& config,
        std::shared_ptr<NRm::IKqpResourceManager> resourceManager,
        NYql::NDq::IDqAsyncIoFactory::TPtr asyncIoFactory,
        const std::optional<TKqpFederatedQuerySetup> federatedQuerySetup)
{
    return std::make_shared<TKqpCaFactory>(config, resourceManager, asyncIoFactory, federatedQuerySetup);
}

}