#include "storage_proxy.h"

#include "gc.h"
#include <ydb/core/fq/libs/config/protos/storage.pb.h>
#include <ydb/core/fq/libs/control_plane_storage/util.h>
#include "ydb_checkpoint_storage.h"
#include "ydb_state_storage.h"

#include <ydb/core/fq/libs/checkpointing_common/defs.h>
#include <ydb/core/fq/libs/checkpoint_storage/events/events.h>

#include <ydb/core/fq/libs/actors/logging/log.h>
#include <ydb/core/fq/libs/ydb/util.h>

#include <ydb/library/yql/dq/actors/compute/dq_compute_actor.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>

#include <util/stream/file.h>
#include <util/string/join.h>
#include <util/string/strip.h>

namespace NFq {

using namespace NActors;

namespace {

////////////////////////////////////////////////////////////////////////////////

class TStorageProxy : public TActorBootstrapped<TStorageProxy> {
    NConfig::TCheckpointCoordinatorConfig Config;
    NConfig::TCommonConfig CommonConfig;
    NConfig::TYdbStorageConfig StorageConfig;
    TCheckpointStoragePtr CheckpointStorage;
    TStateStoragePtr StateStorage;
    TActorId ActorGC;
    NKikimr::TYdbCredentialsProviderFactory CredentialsProviderFactory;
    TYqSharedResources::TPtr YqSharedResources;

public:
    explicit TStorageProxy(
        const NConfig::TCheckpointCoordinatorConfig& config,
        const NConfig::TCommonConfig& commonConfig,
        const NKikimr::TYdbCredentialsProviderFactory& credentialsProviderFactory,
        const TYqSharedResources::TPtr& yqSharedResources);

    void Bootstrap();

    static constexpr char ActorName[] = "YQ_STORAGE_PROXY";

private:
    STRICT_STFUNC(StateFunc,
        hFunc(TEvCheckpointStorage::TEvRegisterCoordinatorRequest, Handle);
        hFunc(TEvCheckpointStorage::TEvCreateCheckpointRequest, Handle);
        hFunc(TEvCheckpointStorage::TEvSetCheckpointPendingCommitStatusRequest, Handle);
        hFunc(TEvCheckpointStorage::TEvCompleteCheckpointRequest, Handle);
        hFunc(TEvCheckpointStorage::TEvAbortCheckpointRequest, Handle);
        hFunc(TEvCheckpointStorage::TEvGetCheckpointsMetadataRequest, Handle);

        hFunc(NYql::NDq::TEvDqCompute::TEvSaveTaskState, Handle);
        hFunc(NYql::NDq::TEvDqCompute::TEvGetTaskState, Handle);
    )

    void Handle(TEvCheckpointStorage::TEvRegisterCoordinatorRequest::TPtr& ev);

    void Handle(TEvCheckpointStorage::TEvCreateCheckpointRequest::TPtr& ev);
    void Handle(TEvCheckpointStorage::TEvSetCheckpointPendingCommitStatusRequest::TPtr& ev);
    void Handle(TEvCheckpointStorage::TEvCompleteCheckpointRequest::TPtr& ev);
    void Handle(TEvCheckpointStorage::TEvAbortCheckpointRequest::TPtr& ev);

    void Handle(TEvCheckpointStorage::TEvGetCheckpointsMetadataRequest::TPtr& ev);

    void Handle(NYql::NDq::TEvDqCompute::TEvSaveTaskState::TPtr& ev);
    void Handle(NYql::NDq::TEvDqCompute::TEvGetTaskState::TPtr& ev);
};

static void FillDefaultParameters(NConfig::TCheckpointCoordinatorConfig& checkpointCoordinatorConfig, NConfig::TYdbStorageConfig& ydbStorageConfig) {
    auto& limits = *checkpointCoordinatorConfig.MutableStateStorageLimits();
    if (!limits.GetMaxGraphCheckpointsSizeBytes()) {
        limits.SetMaxGraphCheckpointsSizeBytes(1099511627776);
    }

    if (!limits.GetMaxTaskStateSizeBytes()) {
        limits.SetMaxTaskStateSizeBytes(1099511627776);
    }

    if (!limits.GetMaxRowSizeBytes()) {
        limits.SetMaxRowSizeBytes(16000000);
    }

    if (!checkpointCoordinatorConfig.GetStorage().GetToken() && checkpointCoordinatorConfig.GetStorage().GetOAuthFile()) {
        checkpointCoordinatorConfig.MutableStorage()->SetToken(StripString(TFileInput(checkpointCoordinatorConfig.GetStorage().GetOAuthFile()).ReadAll()));
    }

    if (!ydbStorageConfig.GetToken() && ydbStorageConfig.GetOAuthFile()) {
        ydbStorageConfig.SetToken(StripString(TFileInput(ydbStorageConfig.GetOAuthFile()).ReadAll()));
    }
}

TStorageProxy::TStorageProxy(
    const NConfig::TCheckpointCoordinatorConfig& config,
    const NConfig::TCommonConfig& commonConfig,
    const NKikimr::TYdbCredentialsProviderFactory& credentialsProviderFactory,
    const TYqSharedResources::TPtr& yqSharedResources)
    : Config(config)
    , CommonConfig(commonConfig)
    , StorageConfig(Config.GetStorage())
    , CredentialsProviderFactory(credentialsProviderFactory)
    , YqSharedResources(yqSharedResources) {
    FillDefaultParameters(Config, StorageConfig);
}

void TStorageProxy::Bootstrap() {
    CheckpointStorage = NewYdbCheckpointStorage(StorageConfig, CredentialsProviderFactory, CreateEntityIdGenerator(CommonConfig.GetIdsPrefix()), YqSharedResources);
    auto issues = CheckpointStorage->Init().GetValueSync();
    if (!issues.Empty()) {
        LOG_STREAMS_STORAGE_SERVICE_ERROR("Failed to init checkpoint storage: " << issues.ToOneLineString());
    }

    StateStorage = NewYdbStateStorage(Config, CredentialsProviderFactory, YqSharedResources);
    issues = StateStorage->Init().GetValueSync();
    if (!issues.Empty()) {
        LOG_STREAMS_STORAGE_SERVICE_ERROR("Failed to init checkpoint state storage: " << issues.ToOneLineString());
    }

    if (Config.GetCheckpointGarbageConfig().GetEnabled()) {
        const auto& gcConfig = Config.GetCheckpointGarbageConfig();
        ActorGC = Register(NewGC(gcConfig, CheckpointStorage, StateStorage).release());
    }

    Become(&TStorageProxy::StateFunc);

    LOG_STREAMS_STORAGE_SERVICE_INFO("Successfully bootstrapped TStorageProxy " << SelfId() << " with connection to "
        << StorageConfig.GetEndpoint().data()
        << ":" << StorageConfig.GetDatabase().data())
}

void TStorageProxy::Handle(TEvCheckpointStorage::TEvRegisterCoordinatorRequest::TPtr& ev) {
    const auto* event = ev->Get();
    LOG_STREAMS_STORAGE_SERVICE_DEBUG("[" << event->CoordinatorId << "] Got TEvRegisterCoordinatorRequest")

    CheckpointStorage->RegisterGraphCoordinator(event->CoordinatorId)
        .Apply([coordinatorId = event->CoordinatorId,
                cookie = ev->Cookie,
                sender = ev->Sender,
                context = TActivationContext::AsActorContext()] (const NThreading::TFuture<NYql::TIssues>& issuesFuture) {
            auto response = std::make_unique<TEvCheckpointStorage::TEvRegisterCoordinatorResponse>();
            response->Issues = issuesFuture.GetValue();
            if (response->Issues) {
                LOG_STREAMS_STORAGE_SERVICE_AS_WARN(context, "[" << coordinatorId << "] Failed to register graph: " << response->Issues.ToString())
            } else {
                LOG_STREAMS_STORAGE_SERVICE_AS_INFO(context, "[" << coordinatorId << "] Graph registered")
            }
            LOG_STREAMS_STORAGE_SERVICE_AS_DEBUG(context, "[" << coordinatorId << "] Send TEvRegisterCoordinatorResponse")
            context.Send(sender, response.release(), 0, cookie);
        });
}

void TStorageProxy::Handle(TEvCheckpointStorage::TEvCreateCheckpointRequest::TPtr& ev) {
    const auto* event = ev->Get();
    LOG_STREAMS_STORAGE_SERVICE_DEBUG("[" << event->CoordinatorId << "] [" << event->CheckpointId << "] Got TEvCreateCheckpointRequest")

    CheckpointStorage->GetTotalCheckpointsStateSize(event->CoordinatorId.GraphId)
        .Apply([checkpointId = event->CheckpointId,
                coordinatorId = event->CoordinatorId,
                cookie = ev->Cookie,
                sender = ev->Sender,
                totalGraphCheckpointsSizeLimit = Config.GetStateStorageLimits().GetMaxGraphCheckpointsSizeBytes(),
                context = TActivationContext::AsActorContext()]
               (const NThreading::TFuture<ICheckpointStorage::TGetTotalCheckpointsStateSizeResult>& resultFuture) {
            auto result = resultFuture.GetValue();
            auto issues = result.second;

            if (issues) {
                LOG_STREAMS_STORAGE_SERVICE_AS_WARN(context, "[" << coordinatorId << "] [" << checkpointId << "] Failed to fetch total graph checkpoints size: " << issues.ToString());
                context.Send(sender, new TEvCheckpointStorage::TEvCreateCheckpointResponse(checkpointId, std::move(issues), TString()), 0, cookie);
                return false;
            }

            auto totalGraphCheckpointsSize = result.first;

            if (totalGraphCheckpointsSize > totalGraphCheckpointsSizeLimit) {
                TStringStream ss;
                ss << "[" << coordinatorId << "] [" << checkpointId << "] Graph checkpoints size limit exceeded: limit " << totalGraphCheckpointsSizeLimit << ", current checkpoints size: " << totalGraphCheckpointsSize;
                auto message = ss.Str();
                LOG_STREAMS_STORAGE_SERVICE_AS_WARN(context, message)
                issues.AddIssue(message);
                LOG_STREAMS_STORAGE_SERVICE_AS_DEBUG(context, "[" << coordinatorId << "] [" << checkpointId << "] Send TEvCreateCheckpointResponse");
                context.Send(sender, new TEvCheckpointStorage::TEvCreateCheckpointResponse(checkpointId, std::move(issues), TString()), 0, cookie);
                return false;
            }
            return true;
        })
        .Apply([checkpointId = event->CheckpointId,
                coordinatorId = event->CoordinatorId,
                cookie = ev->Cookie,
                sender = ev->Sender,
                graphDesc = event->GraphDescription,
                storage = CheckpointStorage]
                   (const NThreading::TFuture<bool>& passedSizeLimitCheckFuture) {
            if (!passedSizeLimitCheckFuture.GetValue()) {
                return NThreading::TFuture<ICheckpointStorage::TCreateCheckpointResult>();
            }
            if (std::holds_alternative<TString>(graphDesc)) {
                return storage->CreateCheckpoint(coordinatorId, checkpointId, std::get<TString>(graphDesc), ECheckpointStatus::Pending);
            } else {
                return storage->CreateCheckpoint(coordinatorId, checkpointId, std::get<NProto::TCheckpointGraphDescription>(graphDesc), ECheckpointStatus::Pending);
            }
        })
        .Apply([checkpointId = event->CheckpointId,
                coordinatorId = event->CoordinatorId,
                cookie = ev->Cookie,
                sender = ev->Sender,
                context = TActivationContext::AsActorContext()]
               (const NThreading::TFuture<ICheckpointStorage::TCreateCheckpointResult>& resultFuture) {
            if (!resultFuture.Initialized()) { // didn't pass the size limit check
                return;
            }
            auto result = resultFuture.GetValue();
            auto issues = result.second;
            auto response = std::make_unique<TEvCheckpointStorage::TEvCreateCheckpointResponse>(checkpointId, std::move(issues), result.first);
            if (response->Issues) {
                LOG_STREAMS_STORAGE_SERVICE_AS_WARN(context, "[" << coordinatorId << "] [" << checkpointId << "] Failed to create checkpoint: " << response->Issues.ToString());
            } else {
                LOG_STREAMS_STORAGE_SERVICE_AS_INFO(context, "[" << coordinatorId << "] [" << checkpointId << "] Checkpoint created");
            }
            LOG_STREAMS_STORAGE_SERVICE_AS_DEBUG(context, "[" << coordinatorId << "] [" << checkpointId << "] Send TEvCreateCheckpointResponse");
            context.Send(sender, response.release(), 0, cookie);
        });
}

void TStorageProxy::Handle(TEvCheckpointStorage::TEvSetCheckpointPendingCommitStatusRequest::TPtr& ev) {
    const auto* event = ev->Get();
    LOG_STREAMS_STORAGE_SERVICE_DEBUG("[" << event->CoordinatorId << "] [" << event->CheckpointId << "] Got TEvSetCheckpointPendingCommitStatusRequest")
    CheckpointStorage->UpdateCheckpointStatus(event->CoordinatorId, event->CheckpointId, ECheckpointStatus::PendingCommit, ECheckpointStatus::Pending, event->StateSizeBytes)
        .Apply([checkpointId = event->CheckpointId,
                coordinatorId = event->CoordinatorId,
                cookie = ev->Cookie,
                sender = ev->Sender,
                context = TActivationContext::AsActorContext()]
               (const NThreading::TFuture<NYql::TIssues>& issuesFuture) {
            auto issues = issuesFuture.GetValue();
            auto response = std::make_unique<TEvCheckpointStorage::TEvSetCheckpointPendingCommitStatusResponse>(checkpointId, std::move(issues));
            if (response->Issues) {
                LOG_STREAMS_STORAGE_SERVICE_AS_WARN(context, "[" << coordinatorId << "] [" << checkpointId << "] Failed to set 'PendingCommit' status: " << response->Issues.ToString())
            } else {
                LOG_STREAMS_STORAGE_SERVICE_AS_INFO(context, "[" << coordinatorId << "] [" << checkpointId << "] Status updated to 'PendingCommit'")
            }
            LOG_STREAMS_STORAGE_SERVICE_AS_DEBUG(context, "[" << coordinatorId << "] [" << checkpointId << "] Send TEvSetCheckpointPendingCommitStatusResponse")
            context.Send(sender, response.release(), 0, cookie);
        });
}

void TStorageProxy::Handle(TEvCheckpointStorage::TEvCompleteCheckpointRequest::TPtr& ev) {
    const auto* event = ev->Get();
    LOG_STREAMS_STORAGE_SERVICE_DEBUG("[" << event->CoordinatorId << "] [" << event->CheckpointId << "] Got TEvCompleteCheckpointRequest")
    CheckpointStorage->UpdateCheckpointStatus(event->CoordinatorId, event->CheckpointId, ECheckpointStatus::Completed, ECheckpointStatus::PendingCommit, event->StateSizeBytes)
        .Apply([checkpointId = event->CheckpointId,
                coordinatorId = event->CoordinatorId,
                cookie = ev->Cookie,
                sender = ev->Sender,
                type = event->Type,
                gcEnabled = Config.GetCheckpointGarbageConfig().GetEnabled(),
                actorGC = ActorGC,
                context = TActivationContext::AsActorContext()]
               (const NThreading::TFuture<NYql::TIssues>& issuesFuture) {
            auto issues = issuesFuture.GetValue();
            auto response = std::make_unique<TEvCheckpointStorage::TEvCompleteCheckpointResponse>(checkpointId, std::move(issues));
            if (response->Issues) {
                LOG_STREAMS_STORAGE_SERVICE_AS_DEBUG(context, "[" << coordinatorId << "] [" << checkpointId << "] Failed to set 'Completed' status: " << response->Issues.ToString())
            } else {
                LOG_STREAMS_STORAGE_SERVICE_AS_INFO(context, "[" << coordinatorId << "] [" << checkpointId << "] Status updated to 'Completed'")
                if (gcEnabled) {
                    auto request = std::make_unique<TEvCheckpointStorage::TEvNewCheckpointSucceeded>(coordinatorId, checkpointId, type);
                    LOG_STREAMS_STORAGE_SERVICE_AS_DEBUG(context, "[" << coordinatorId << "] [" << checkpointId << "] Send TEvNewCheckpointSucceeded")
                    context.Send(actorGC, request.release(), 0);
                }
            }
            LOG_STREAMS_STORAGE_SERVICE_AS_DEBUG(context, "[" << coordinatorId << "] [" << checkpointId << "] Send TEvCompleteCheckpointResponse")
            context.Send(sender, response.release(), 0, cookie);
        });
}

void TStorageProxy::Handle(TEvCheckpointStorage::TEvAbortCheckpointRequest::TPtr& ev) {
    const auto* event = ev->Get();
    LOG_STREAMS_STORAGE_SERVICE_DEBUG("[" << event->CoordinatorId << "] [" << event->CheckpointId << "] Got TEvAbortCheckpointRequest")
    CheckpointStorage->AbortCheckpoint(event->CoordinatorId,event->CheckpointId)
        .Apply([checkpointId = event->CheckpointId,
                coordinatorId = event->CoordinatorId,
                cookie = ev->Cookie,
                sender = ev->Sender,
                context = TActivationContext::AsActorContext()] (const NThreading::TFuture<NYql::TIssues>& issuesFuture) {
            auto issues = issuesFuture.GetValue();
            auto response = std::make_unique<TEvCheckpointStorage::TEvAbortCheckpointResponse>(checkpointId, std::move(issues));
            if (response->Issues) {
                LOG_STREAMS_STORAGE_SERVICE_AS_WARN(context, "[" << coordinatorId << "] [" << checkpointId << "] Failed to abort checkpoint: " << response->Issues.ToString())
            } else {
                LOG_STREAMS_STORAGE_SERVICE_AS_INFO(context, "[" << coordinatorId << "] [" << checkpointId << "] Checkpoint aborted")
            }
            LOG_STREAMS_STORAGE_SERVICE_AS_DEBUG(context, "[" << coordinatorId << "] [" << checkpointId << "] Send TEvAbortCheckpointResponse")
            context.Send(sender, response.release(), 0, cookie);
        });
}

void TStorageProxy::Handle(TEvCheckpointStorage::TEvGetCheckpointsMetadataRequest::TPtr& ev) {
    const auto* event = ev->Get();
    LOG_STREAMS_STORAGE_SERVICE_DEBUG("[" << event->GraphId << "] Got TEvGetCheckpointsMetadataRequest");
    CheckpointStorage->GetCheckpoints(event->GraphId, event->Statuses, event->Limit, event->LoadGraphDescription)
        .Apply([graphId = event->GraphId,
                cookie = ev->Cookie,
                sender = ev->Sender,
                context = TActivationContext::AsActorContext()] (const NThreading::TFuture<ICheckpointStorage::TGetCheckpointsResult>& futureResult) {
            auto result = futureResult.GetValue();
            auto response = std::make_unique<TEvCheckpointStorage::TEvGetCheckpointsMetadataResponse>(result.first, result.second);
            if (response->Issues) {
                LOG_STREAMS_STORAGE_SERVICE_AS_WARN(context, "[" << graphId << "] Failed to get checkpoints: " << response->Issues.ToString())
            }
            LOG_STREAMS_STORAGE_SERVICE_AS_DEBUG(context, "[" << graphId << "] Send TEvGetCheckpointsMetadataResponse")
            context.Send(sender, response.release(), 0, cookie);
        });
}

void TStorageProxy::Handle(NYql::NDq::TEvDqCompute::TEvSaveTaskState::TPtr& ev) {
    auto* event = ev->Get();
    const auto checkpointId = TCheckpointId(event->Checkpoint.GetGeneration(), event->Checkpoint.GetId());
    LOG_STREAMS_STORAGE_SERVICE_DEBUG("[" << event->GraphId << "-" << checkpointId << "] Got TEvSaveTaskState: task " << event->TaskId);

    const size_t stateSize = event->State.ByteSizeLong();
    if (stateSize > Config.GetStateStorageLimits().GetMaxTaskStateSizeBytes()) {
        LOG_STREAMS_STORAGE_SERVICE_WARN("[" << checkpointId << "] Won't save task state because it's too big: task: " << event->TaskId
            << ", state size: " << stateSize << "/" << Config.GetStateStorageLimits().GetMaxTaskStateSizeBytes());
        auto response = std::make_unique<NYql::NDq::TEvDqCompute::TEvSaveTaskStateResult>();
        response->Record.MutableCheckpoint()->SetGeneration(checkpointId.CoordinatorGeneration);
        response->Record.MutableCheckpoint()->SetId(checkpointId.SeqNo);
        response->Record.SetStateSizeBytes(0);
        response->Record.SetTaskId(event->TaskId);
        response->Record.SetStatus(NYql::NDqProto::TEvSaveTaskStateResult::STATE_TOO_BIG);
        Send(ev->Sender, response.release());
        return;
    }

    StateStorage->SaveState(event->TaskId, event->GraphId, checkpointId, event->State)
        .Apply([graphId = event->GraphId,
                checkpointId,
                taskId = event->TaskId,
                cookie = ev->Cookie,
                sender = ev->Sender,
                stateSize = stateSize,
                context = TActivationContext::AsActorContext()](const NThreading::TFuture<NYql::TIssues>& futureResult) {
            const auto& issues = futureResult.GetValue();
            auto response = std::make_unique<NYql::NDq::TEvDqCompute::TEvSaveTaskStateResult>();
            response->Record.MutableCheckpoint()->SetGeneration(checkpointId.CoordinatorGeneration);
            response->Record.MutableCheckpoint()->SetId(checkpointId.SeqNo);
            response->Record.SetStateSizeBytes(stateSize);
            response->Record.SetTaskId(taskId);

            if (issues) {
                LOG_STREAMS_STORAGE_SERVICE_AS_WARN(context, "[" << graphId << "-" << checkpointId << "] Failed to save task state: task: " << taskId << ", issues: " << issues.ToString())
                response->Record.SetStatus(NYql::NDqProto::TEvSaveTaskStateResult::STORAGE_ERROR);
            } else {
                response->Record.SetStatus(NYql::NDqProto::TEvSaveTaskStateResult::OK);
            }
            LOG_STREAMS_STORAGE_SERVICE_AS_DEBUG(context, "[" << checkpointId << "] Send TEvSaveTaskStateResult")
            context.Send(sender, response.release(), 0, cookie);
        });
}

void TStorageProxy::Handle(NYql::NDq::TEvDqCompute::TEvGetTaskState::TPtr& ev) {
    const auto* event = ev->Get();
    const auto checkpointId = TCheckpointId(event->Checkpoint.GetGeneration(), event->Checkpoint.GetId());
    LOG_STREAMS_STORAGE_SERVICE_DEBUG("[" << checkpointId << "] Got TEvGetTaskState: tasks {" << JoinSeq(", ", event->TaskIds) << "}");

    StateStorage->GetState(event->TaskIds, event->GraphId, checkpointId)
        .Apply([checkpointId = event->Checkpoint,
                generation = event->Generation,
                taskIds = event->TaskIds,
                cookie = ev->Cookie,
                sender = ev->Sender,
                context = TActivationContext::AsActorContext()](const NThreading::TFuture<IStateStorage::TGetStateResult>& resultFuture) {
            auto result = resultFuture.GetValue();

            auto response = std::make_unique<NYql::NDq::TEvDqCompute::TEvGetTaskStateResult>(checkpointId, result.second, generation);
            std::swap(response->States, result.first);
            if (response->Issues) {
                LOG_STREAMS_STORAGE_SERVICE_AS_WARN(context, "[" << checkpointId << "] Failed to get task state: taskIds: {" << JoinSeq(", ", taskIds) << "}, issues: " << response->Issues.ToString());
            }
            LOG_STREAMS_STORAGE_SERVICE_AS_DEBUG(context, "[" << checkpointId << "] Send TEvGetTaskStateResult, taskIds: {" << JoinSeq(", ", taskIds) << "}");
            context.Send(sender, response.release(), 0, cookie);
        });
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<NActors::IActor> NewStorageProxy(
    const NConfig::TCheckpointCoordinatorConfig& config,
    const NConfig::TCommonConfig& commonConfig,
    const NKikimr::TYdbCredentialsProviderFactory& credentialsProviderFactory,
    const TYqSharedResources::TPtr& yqSharedResources)
{
    return std::unique_ptr<NActors::IActor>(new TStorageProxy(config, commonConfig, credentialsProviderFactory, yqSharedResources));
}

} // namespace NFq
