#pragma once

#include "distconf.h"

#include <ydb/core/blobstorage/base/blobstorage_events.h>
#include <ydb/core/blobstorage/groupinfo/blobstorage_groupinfo_sets.h>

namespace NKikimr::NStorage {

    // Bump generation of the bridge proxy group after the groups of its piles got their generations bumped; inline pile
    // is its own pile, so it is not bumped twice. bumpedAsPile tells whether the group was bumped as the pile.
    inline void BumpBridgeProxyGeneration(NKikimrBlobStorage::TGroupInfo *proxyGroup,
            const std::function<bool(TGroupId)>& bumpedAsPile) {
        if (!bumpedAsPile(TGroupId::FromProto(proxyGroup, &NKikimrBlobStorage::TGroupInfo::GetGroupID))) {
            proxyGroup->SetGroupGeneration(proxyGroup->GetGroupGeneration() + 1);
        }
    }

    class TDistributedConfigKeeper::TInvokeRequestHandlerActor : public TActor<TInvokeRequestHandlerActor> {
        friend class TDistributedConfigKeeper;

        TDistributedConfigKeeper* const Self;
        const std::weak_ptr<TLifetimeToken> LifetimeToken;
        const ui64 InvokePipelineGeneration;
        TInvokeQuery Query;
        const std::shared_ptr<TFreshConfigQueryGroup> FreshConfigQueryGroup;

        ui32 WaitingReplyFromNode = 0;

        using TQuery = NKikimrBlobStorage::TEvNodeConfigInvokeOnRoot;
        using TResult = NKikimrBlobStorage::TEvNodeConfigInvokeOnRootResult;

        using TGatherCallback = std::function<void(TEvGather*)>;
        ui64 NextScatterCookie = 1;
        THashMap<ui64, TGatherCallback> ScatterTasks;

        std::shared_ptr<TLifetimeToken> RequestHandlerToken = std::make_shared<TLifetimeToken>();

        bool InvokedWithoutScepter = false;
        bool EnablingDistconf = false;
        bool Detached = false;
        bool AdvancesConflictingGeneration = false;
        bool RecoveryProposition = false;

    public: // Error handling
        struct TExError : yexception {
            bool IsCritical = false; // the one what would case Y_ABORT if in debug mode
            TResult::EStatus Status = TResult::ERROR;
        };

        struct TExCriticalError : TExError {
            TExCriticalError() { IsCritical = true; }
        };

        struct TExRace : TExError {
            TExRace() { Status = TResult::RACE; }
        };

        struct TExNoQuorum : TExError {
            TExNoQuorum() { Status = TResult::NO_QUORUM; }
        };

    public:
        TInvokeRequestHandlerActor(TDistributedConfigKeeper *self, TInvokeQuery&& query,
                                  std::shared_ptr<TFreshConfigQueryGroup> freshConfigQueryGroup = {});
        TInvokeRequestHandlerActor(TDistributedConfigKeeper *self, TInvokeExternalOperation&& query, ui32 hopNodeId);

        void HandleExecuteQuery();
        void Handle(TEvPrivate::TEvAbortQuery::TPtr ev);

        void Handle(TEvNodeConfigInvokeOnRootResult::TPtr ev);

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // Interconnect machinery

        THashMap<ui32, TActorId> Subscriptions;

        void Handle(TEvInterconnect::TEvNodeConnected::TPtr ev);
        void Handle(TEvInterconnect::TEvNodeDisconnected::TPtr ev);
        void UnsubscribeInterconnect();

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // Query execution logic

        void ExecuteQuery();
        void IssueScatterTask(TEvScatter&& task, TGatherCallback callback);
        void Handle(TEvNodeConfigGather::TPtr ev);

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // Configuration update

        void UpdateConfig(TQuery::TUpdateConfig *request);
        void DescendCommittedStorageConfig(const TQuery::TDescendCommittedStorageConfig& request);

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // Reassign group disk logic

        THashMultiMap<ui32, TVDiskID> NodeToVDisk;
        THashMap<TActorId, TVDiskID> ActorToVDisk;
        THashMap<ui32, TVector<std::pair<TActorId, TVDiskID>>> VStatusQueriesAwaitingConnection;
        std::optional<NKikimrBlobStorage::TBaseConfig> BaseConfig;
        THashSet<TVDiskID> PendingVDiskIds;
        TIntrusivePtr<TBlobStorageGroupInfo> GroupInfo;
        std::optional<TBlobStorageGroupInfo::TGroupVDisks> SuccessfulVDisks;
        std::optional<TResult::TReassignGroupDisk> ReassignGroupDiskResult;

        void ReassignGroupDisk(const TQuery::TReassignGroupDisk& cmd);
        void IssueVStatusQueries(const NKikimrBlobStorage::TGroupInfo& group);
        void SendVStatusQuery(TActorId actorId, TVDiskID vdiskId, TActorId sessionId = {});
        void SendPendingVStatusQueries(ui32 nodeId, TActorId sessionId);
        void Handle(TEvBlobStorage::TEvVStatusResult::TPtr ev);
        void Handle(TEvents::TEvUndelivered::TPtr ev);
        void OnVStatusError(TVDiskID vdiskId);
        void Handle(TEvNodeWardenBaseConfig::TPtr ev);
        void CheckReassignGroupDisk();
        void ReassignGroupDiskExecute();
        void CheckBridgedPartDataRestorable(const NKikimrBlobStorage::TStorageConfig& config,
            const NKikimrBlobStorage::TGroupInfo& group, TVDiskID vdiskId);
        void UpdateBridgeGroupInfo(const TQuery::TUpdateBridgeGroupInfo& cmd);

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // VDiskSlain/DropDonor logic

        void StaticVDiskSlain(const TQuery::TStaticVDiskSlain& cmd);
        void DropDonor(const TQuery::TDropDonor& cmd);
        void HandleDropDonorAndSlain(TVDiskID vdiskId, const NKikimrBlobStorage::TVSlotId& vslotId, bool isDropDonor);

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // State Storage operation

        void ReassignStateStorageNode(const TQuery::TReassignStateStorageNode& cmd);
        void ReconfigStateStorage(const NKikimrBlobStorage::TStateStorageConfig& cmd);
        void SelfHealStateStorage(const TQuery::TSelfHealStateStorage& cmd);
        void SelfHealStateStorage(ui32 waitForConfigStep, bool forceHeal, bool pileupReplicas, ui32 overrideReplicasInRingCount,
            ui32 overrideRingsCount, ui32 replicasSpecificVolume);
        void SelfHealNodesStateUpdate(const TQuery::TSelfHealNodesStateUpdate& cmd);
        void GetStateStorageConfig(const TQuery::TGetStateStorageConfig& cmd);

        void GetCurrentStateStorageConfig(NKikimrBlobStorage::TStateStorageConfig* currentConfig, bool getNodesState);
        bool GetRecommendedStateStorageConfig(NKikimrBlobStorage::TStateStorageConfig* currentConfig, bool pileupReplicas,
            ui32 overrideReplicasInRingCount, ui32 overrideRingsCount, ui32 replicasSpecificVolume);
        void AdjustRingGroupActorIdOffsetInRecommendedStateStorageConfig(NKikimrBlobStorage::TStateStorageConfig* currentConfig);

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // Storage configuration YAML manipulation

        NKikimrBlobStorage::TStorageConfig ProposedStorageConfig;

        std::optional<TString> NewYaml;
        std::optional<TString> NewStorageYaml;
        std::optional<ui64> MainYamlVersion;
        std::optional<ui64> StorageYamlVersion;
        bool IsDryRun = false;

        TStorageConfigPtr ConfigAwaitingCommit;
        TMonotonic CommitConfirmationDeadline;
        bool FullCommitConfirmationNeeded = false;
        static constexpr ui64 ConfigValidationTimeoutCookie = 1;
        static constexpr ui64 VStatusTimeoutCookie = 2;
        bool WaitingForConfigValidation = false;

        TActorId ControllerPipeId;

        enum class EControllerOp {
            UNSET,
            ENABLE_DISTCONF,
            DISABLE_DISTCONF,
            OTHER,
            ADD_BRIDGE_PILE, // BSC validates addition of the pile, then ProposedStorageConfig gets proposed
            REMOVE_BRIDGE_PILE, // BSC validates removal of the pile, then ProposedStorageConfig gets proposed
        } ControllerOp = EControllerOp::UNSET;

        void FetchStorageConfig(const TQuery::TFetchStorageConfig& request);
        void Handle(TEvNodeWardenStorageConfig::TPtr ev);
        void HandleWakeup();
        void HandleWakeup(STFUNC_SIG);
        void ReplyToFetchStorageConfig(const std::optional<TString>& mainConfigYaml,
            const std::optional<TString>& storageConfigYaml, bool transient);
        void ReplaceStorageConfig(const TQuery::TReplaceStorageConfig& request);
        void ValidateReplaceStorageConfig(const std::optional<TString>& storageConfigYaml, ui64 expectedMainYamlVersion,
                ui64 expectedStorageYamlVersion);
        void ReplaceStorageConfigResume(const std::optional<TString>& storageConfigYaml, ui64 expectedMainYamlVersion,
                ui64 expectedStorageYamlVersion, bool enablingDistconf);
        void ReplaceStorageConfigExecute();
        void StartCommitConfirmation();
        void CollectCommittedConfig();
        void TryEnableDistconf();
        void ConnectToController();
        void Handle(TEvTabletPipe::TEvClientConnected::TPtr ev);
        void Handle(TEvTabletPipe::TEvClientDestroyed::TPtr ev);
        void Handle(TEvBlobStorage::TEvControllerConfigResponse::TPtr ev);
        void Handle(TEvBlobStorage::TEvControllerDistconfResponse::TPtr ev);
        void Handle(TEvBlobStorage::TEvControllerValidateConfigResponse::TPtr ev);
        void BootstrapCluster(const TQuery::TBootstrapCluster& request);

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // Bridge mode

        void NeedBridgeMode();
        void CheckPileNotification(TBridgePileId bridgePileId, ui64 generation);

        void SwitchBridgeClusterState(const TQuery::TSwitchBridgeClusterState& cmd);

        void NotifyBridgeSyncFinished(const TQuery::TNotifyBridgeSyncFinished& cmd);
        void NotifyBridgeSuspended(const TQuery::TNotifyBridgeSuspended& cmd);

        void ConvertToBridge(const TQuery::TConvertToBridge& cmd);

        // check that every node runs with BridgeConfig (and supports bridge features of minFeatureLevel, when it is set),
        // then invoke the callback; every static node listed in requiredNodeIds must report it, while nodes listed in
        // ignoredNodeIds may not report anything (they are leaving the cluster)
        void RequireBridgeCapability(ui32 minFeatureLevel, THashSet<ui32> requiredNodeIds,
            THashSet<ui32> ignoredNodeIds, std::function<void()> callback);
        void NotifyBscConversionFinished(const TQuery::TNotifyBscConversionFinished& cmd);

        TBridgePileId BridgePileOpPileId; // the pile being added or removed, see ControllerOp

        // common checks of the operations changing the set of piles: the pile is listed in BridgeConfig and it is in the
        // expected state, no other operation is in progress; returns the id of the pile
        TBridgePileId CheckPileOperation(const TString& pileName, NKikimrBridge::TClusterState::EPileState expectedState,
            bool adding);
        // every node remaining in the cluster has to support pile operations (see RequireBridgeCapability)
        void RequirePileOperationCapability(TBridgePileId leavingPileId, std::function<void()> callback);

        void AddBridgePile(const TQuery::TAddBridgePile& cmd);
        void AddBridgePileWithBaseConfig();
        void RemoveBridgePile(const TQuery::TRemoveBridgePile& cmd);
        void NotifyBridgePileOperation(const TQuery::TNotifyBridgePileOperation& cmd);

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // Retro trace collection

        void DemandRetroTrace(const TQuery::TDemandRetroTrace& cmd);

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // Configuration proposition

        void AdvanceGeneration();
        void QueryConfig(const TQuery::TQueryConfig& request);
        void CollectConfig(std::function<void(TProcessCollectConfigsResult&&)> callback, bool allowConflictingGenerationAdvance = false);
        void StartRecoveryProposition(TProcessCollectConfigsResult& result);
        void CollectBridgeConfig(const NKikimrBlobStorage::TStorageConfig& config, std::function<void()> callback);
        void StartProposition(NKikimrBlobStorage::TStorageConfig *config, bool mindPrev = true,
            const NKikimrBlobStorage::TStorageConfig *propositionBase = nullptr, bool fromBootstrap = false);
        void StartPropositionImpl(NKikimrBlobStorage::TStorageConfig *config, bool mindPrev,
                                  const NKikimrBlobStorage::TStorageConfig *propositionBase, bool fromBootstrap);
        void Handle(TEvPrivate::TEvConfigProposed::TPtr ev);

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // Query termination and result delivery

        void RunCommonChecks(bool requireScepter = true);

        void Finish(TResult::EStatus status, std::optional<TStringBuf> errorReason,
            const std::function<void(TResult*)>& callback = {}, bool sendResult = true);

        void DetachQuery();

        void PassAway() override;

        STFUNC(StateFunc);
    };

} // NKikimr::NStorage
