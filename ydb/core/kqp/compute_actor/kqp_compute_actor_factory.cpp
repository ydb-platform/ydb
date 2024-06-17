#include "kqp_compute_actor_factory.h"
#include "kqp_compute_actor.h"

#include <ydb/core/kqp/common/kqp_resolve.h>
#include <ydb/core/kqp/rm_service/kqp_resource_estimation.h>

namespace NKikimr::NKqp::NComputeActor {

struct TMemoryQuotaManager : public NYql::NDq::TGuaranteeQuotaManager {

    TMemoryQuotaManager(std::shared_ptr<NRm::IKqpResourceManager> resourceManager
        , NRm::EKqpMemoryPool memoryPool
        , std::shared_ptr<IKqpNodeState> state
        , ui64 txId
        , ui64 taskId
        , ui64 limit)
    : NYql::NDq::TGuaranteeQuotaManager(limit, limit)
    , ResourceManager(std::move(resourceManager))
    , MemoryPool(memoryPool)
    , State(std::move(state))
    , TxId(txId)
    , TaskId(taskId)
    {
    }

    ~TMemoryQuotaManager() override {
        State->OnTaskTerminate(TxId, TaskId, Success);
        ResourceManager->FreeResources(TxId, TaskId);
    }

    bool AllocateExtraQuota(ui64 extraSize) override {
        auto result = ResourceManager->AllocateResources(TxId, TaskId,
            NRm::TKqpResourcesRequest{.MemoryPool = MemoryPool, .Memory = extraSize});

        if (!result) {
            AFL_WARN(NKikimrServices::KQP_COMPUTE)
                ("problem", "cannot_allocate_memory")
                ("tx_id", TxId)
                ("task_id", TaskId)
                ("memory", extraSize);

            return false;
        }

        return true;
    }

    void FreeExtraQuota(ui64 extraSize) override {
        ResourceManager->FreeResources(TxId, TaskId,
            NRm::TKqpResourcesRequest{.MemoryPool = MemoryPool, .Memory = extraSize}
        );
    }

    void TerminateHandler(bool success, const NYql::TIssues& issues) {
        AFL_DEBUG(NKikimrServices::KQP_COMPUTE)
            ("problem", "finish_compute_actor")
            ("tx_id", TxId)("task_id", TaskId)("success", success)("message", issues.ToOneLineString());
        Success = success;
    }

    std::shared_ptr<NRm::IKqpResourceManager> ResourceManager;
    NRm::EKqpMemoryPool MemoryPool;
    std::shared_ptr<IKqpNodeState> State;
    ui64 TxId;
    ui64 TaskId;
    bool Success = true;
};

class TKqpCaFactory : public IKqpNodeComputeActorFactory {
    NKikimrConfig::TTableServiceConfig::TResourceManager Config;
    std::shared_ptr<NRm::IKqpResourceManager> ResourceManager_;
    NYql::NDq::IDqAsyncIoFactory::TPtr AsyncIoFactory;
    const std::optional<TKqpFederatedQuerySetup> FederatedQuerySetup;

public:
    TKqpCaFactory(const NKikimrConfig::TTableServiceConfig::TResourceManager& config,
        std::shared_ptr<NRm::IKqpResourceManager> resourceManager,
        NYql::NDq::IDqAsyncIoFactory::TPtr asyncIoFactory,
        const std::optional<TKqpFederatedQuerySetup> federatedQuerySetup)
        : Config(config)
        , ResourceManager_(resourceManager)
        , AsyncIoFactory(asyncIoFactory)
        , FederatedQuerySetup(federatedQuerySetup)
    {}

    TActorId CreateKqpComputeActor(const TActorId& executerId, ui64 txId, NYql::NDqProto::TDqTask* dqTask,
        const NYql::NDq::TComputeRuntimeSettings& settings,
        NWilson::TTraceId traceId, TIntrusivePtr<NActors::TProtoArenaHolder> arena, const TString& serializedGUCSettings,
        TComputeStagesWithScan& computesByStage, ui64 outputChunkMaxSize, std::shared_ptr<IKqpNodeState> state,
        NRm::EKqpMemoryPool memoryPool, ui32 numberOfTasks)
    {
        NYql::NDq::TComputeMemoryLimits memoryLimits;
        memoryLimits.ChannelBufferSize = 0;
        memoryLimits.MkqlLightProgramMemoryLimit = Config.GetMkqlLightProgramMemoryLimit();
        memoryLimits.MkqlHeavyProgramMemoryLimit = Config.GetMkqlHeavyProgramMemoryLimit();

        auto estimation = EstimateTaskResources(*dqTask, Config, numberOfTasks);

        {
            ui32 inputChannelsCount = 0;
            for (auto&& i : dqTask->GetInputs()) {
                inputChannelsCount += i.ChannelsSize();
            }

            memoryLimits.ChannelBufferSize = std::max<ui32>(estimation.ChannelBufferMemoryLimit / std::max<ui32>(1, inputChannelsCount), Config.GetMinChannelBufferSize());
            memoryLimits.OutputChunkMaxSize = outputChunkMaxSize;
            AFL_DEBUG(NKikimrServices::KQP_COMPUTE)("event", "channel_info")
                ("ch_size", estimation.ChannelBufferMemoryLimit)
                ("ch_count", estimation.ChannelBuffersCount)
                ("ch_limit", memoryLimits.ChannelBufferSize)
                ("inputs", dqTask->InputsSize())
                ("input_channels_count", inputChannelsCount);
        }

        auto& taskOpts = dqTask->GetProgram().GetSettings();
        auto limit = taskOpts.GetHasMapJoin() || taskOpts.GetHasStateAggregation()
            ? memoryLimits.MkqlHeavyProgramMemoryLimit
            : memoryLimits.MkqlLightProgramMemoryLimit;

        memoryLimits.MemoryQuotaManager = std::make_shared<TMemoryQuotaManager>(
            ResourceManager_,
            memoryPool,
            std::move(state),
            txId,
            dqTask->GetId(),
            limit);

        auto runtimeSettings = settings;
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
        if (dqTask->HasMetaId()) {
            YQL_ENSURE(computesByStage.GetMetaById(*dqTask, meta) || dqTask->GetMeta().UnpackTo(&meta), "cannot take meta on MetaId exists in tasks");
            tableKind = tableKindExtract(meta);
        } else if (dqTask->GetMeta().UnpackTo(&meta)) {
            tableKind = tableKindExtract(meta);
        }

        if (tableKind == ETableKind::Datashard || tableKind == ETableKind::Olap) {
            auto& info = computesByStage.UpsertTaskWithScan(*dqTask, meta, !AppData()->FeatureFlags.GetEnableSeparationComputeActorsFromRead());
            IActor* computeActor = CreateKqpScanComputeActor(executerId, txId, dqTask,
                AsyncIoFactory, runtimeSettings, memoryLimits,
                std::move(traceId), std::move(arena));
            TActorId result = TlsActivationContext->Register(computeActor);
            info.MutableActorIds().emplace_back(result);
            return result;
        } else {
            std::shared_ptr<TGUCSettings> GUCSettings;
            if (!serializedGUCSettings.empty()) {
                GUCSettings = std::make_shared<TGUCSettings>(serializedGUCSettings);
            }
            IActor* computeActor = ::NKikimr::NKqp::CreateKqpComputeActor(executerId, txId, dqTask, AsyncIoFactory,
                runtimeSettings, memoryLimits, std::move(traceId), std::move(arena), FederatedQuerySetup, GUCSettings);
            return TlsActivationContext->Register(computeActor);
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