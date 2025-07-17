#pragma once

#include "distconf.h"

namespace NKikimr::NStorage {

    class TDistributedConfigKeeper::TInvokeRequestHandlerActor : public TActorBootstrapped<TInvokeRequestHandlerActor> {
        friend class TDistributedConfigKeeper;

        TDistributedConfigKeeper* const Self;
        const std::weak_ptr<TLifetimeToken> LifetimeToken;
        const ui64 InvokeActorQueueGeneration;
        std::unique_ptr<TEventHandle<TEvNodeConfigInvokeOnRoot>> Event;
        const TActorId Sender;
        const ui64 Cookie;
        const TActorId RequestSessionId;

        bool BeginRegistered = false;

        bool CheckSyncersAfterCommit = false;

        TActorId ParentId;
        ui32 WaitingReplyFromNode = 0;

        using TQuery = NKikimrBlobStorage::TEvNodeConfigInvokeOnRoot;
        using TResult = NKikimrBlobStorage::TEvNodeConfigInvokeOnRootResult;

        using TGatherCallback = std::function<void(TEvGather*)>;
        ui64 NextScatterCookie = 1;
        THashMap<ui64, TGatherCallback> ScatterTasks;

        std::shared_ptr<TLifetimeToken> RequestHandlerToken = std::make_shared<TLifetimeToken>();

        THashSet<TBridgePileId> SpecificBridgePileIds;
        std::optional<NKikimrBlobStorage::TStorageConfig> SwitchBridgeNewConfig;

        std::optional<NKikimrBlobStorage::TStorageConfig> MergedConfig;

        std::optional<NKikimrBlobStorage::TStorageConfig> ReplaceConfig;

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
        TInvokeRequestHandlerActor(TDistributedConfigKeeper *self, std::unique_ptr<TEventHandle<TEvNodeConfigInvokeOnRoot>>&& ev);
        TInvokeRequestHandlerActor(TDistributedConfigKeeper *self);
        TInvokeRequestHandlerActor(TDistributedConfigKeeper *self, NKikimrBlobStorage::TStorageConfig&& config);

        void Bootstrap(TActorId parentId);

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
        void ExecuteInitialRootAction();
        void IssueScatterTask(TEvScatter&& task, TGatherCallback callback);
        void Handle(TEvNodeConfigGather::TPtr ev);

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // Configuration update

        void UpdateConfig(TQuery::TUpdateConfig *request);

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // Reassign group disk logic

        THashMultiMap<ui32, TVDiskID> NodeToVDisk;
        THashMap<TActorId, TVDiskID> ActorToVDisk;
        std::optional<NKikimrBlobStorage::TBaseConfig> BaseConfig;
        THashSet<TVDiskID> PendingVDiskIds;
        TIntrusivePtr<TBlobStorageGroupInfo> GroupInfo;
        std::optional<TBlobStorageGroupInfo::TGroupVDisks> SuccessfulVDisks;

        void ReassignGroupDisk(const TQuery::TReassignGroupDisk& cmd);
        void IssueVStatusQueries(const NKikimrBlobStorage::TGroupInfo& group);
        void Handle(TEvBlobStorage::TEvVStatusResult::TPtr ev);
        void Handle(TEvents::TEvUndelivered::TPtr ev);
        void OnVStatusError(TVDiskID vdiskId);
        void Handle(TEvNodeWardenBaseConfig::TPtr ev);
        void CheckReassignGroupDisk();
        void ReassignGroupDiskExecute();

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // VDiskSlain/DropDonor logic

        void StaticVDiskSlain(const TQuery::TStaticVDiskSlain& cmd);
        void DropDonor(const TQuery::TDropDonor& cmd);
        void HandleDropDonorAndSlain(TVDiskID vdiskId, const NKikimrBlobStorage::TVSlotId& vslotId, bool isDropDonor);

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // State Storage operation

        void ReassignStateStorageNode(const TQuery::TReassignStateStorageNode& cmd);
        void ReconfigStateStorage(const NKikimrBlobStorage::TStateStorageConfig& cmd);
        void SelfHealStateStorage(bool waitForConfigStep);
        void SelfHealBadNodesListUpdate(const TQuery::TSelfHealBadNodesListUpdate& cmd);
        void GetStateStorageConfig(const TQuery::TGetStateStorageConfig& cmd);

        void GetCurrentStateStorageConfig(NKikimrBlobStorage::TStateStorageConfig* currentConfig);
        void GetRecommendedStateStorageConfig(NKikimrBlobStorage::TStateStorageConfig* currentConfig);
        void AdjustRingGroupActorIdOffsetInRecommendedStateStorageConfig(NKikimrBlobStorage::TStateStorageConfig* currentConfig);
        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // Storage configuration YAML manipulation

        NKikimrBlobStorage::TStorageConfig ProposedStorageConfig;

        std::optional<TString> NewYaml;
        std::optional<TString> NewStorageYaml;
        std::optional<ui64> MainYamlVersion;
        std::optional<ui64> StorageYamlVersion;

        TActorId ControllerPipeId;

        enum class EControllerOp {
            UNSET,
            ENABLE_DISTCONF,
            DISABLE_DISTCONF,
            OTHER,
        } ControllerOp = EControllerOp::UNSET;

        void FetchStorageConfig(bool fetchMain, bool fetchStorage, bool addExplicitMgmtSections, bool addV1);
        void ReplaceStorageConfig(const TQuery::TReplaceStorageConfig& request);
        void ReplaceStorageConfigResume(const std::optional<TString>& storageConfigYaml, ui64 expectedMainYamlVersion,
                ui64 expectedStorageYamlVersion, bool enablingDistconf);
        void TryEnableDistconf();
        void ConnectToController();
        void Handle(TEvTabletPipe::TEvClientConnected::TPtr ev);
        void Handle(TEvTabletPipe::TEvClientDestroyed::TPtr ev);
        void Handle(TEvBlobStorage::TEvControllerConfigResponse::TPtr ev);
        void Handle(TEvBlobStorage::TEvControllerDistconfResponse::TPtr ev);
        void Handle(TEvBlobStorage::TEvControllerValidateConfigResponse::TPtr ev);
        void BootstrapCluster(const TString& selfAssemblyUUID);

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // Bridge mode

        void NeedBridgeMode();

        void PrepareSwitchBridgeClusterState(const TQuery::TSwitchBridgeClusterState& cmd);
        void SwitchBridgeClusterState();

        NKikimrBlobStorage::TStorageConfig GetSwitchBridgeNewConfig(const NKikimrBridge::TClusterState& newClusterState);

        void NotifyBridgeSyncFinished(const TQuery::TNotifyBridgeSyncFinished& cmd);

        void PrepareMergeUnsyncedPileConfig(const TQuery::TMergeUnsyncedPileConfig& cmd);
        void MergeUnsyncedPileConfig();

        void NegotiateUnsyncedConnection(const TQuery::TNegotiateUnsyncedConnection& cmd);

        void PrepareAdvanceClusterStateGeneration(const TQuery::TAdvanceClusterStateGeneration& cmd);
        void AdvanceClusterStateGeneration();

        void GenerateSpecificBridgePileIds();

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // Configuration proposition

        void AdvanceGeneration();
        void StartProposition(NKikimrBlobStorage::TStorageConfig *config, bool forceGeneration = false,
            const NKikimrBlobStorage::TStorageConfig *propositionBase = nullptr);

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // Query termination and result delivery

        void RunCommonChecks();

        std::unique_ptr<TEvNodeConfigInvokeOnRootResult> PrepareResult(TResult::EStatus status, std::optional<TStringBuf> errorReason);
        void FinishWithError(TResult::EStatus status, const TString& errorReason);

        template<typename T>
        void FinishWithSuccess(T&& callback, TResult::EStatus status = TResult::OK,
                std::optional<TStringBuf> errorReason = std::nullopt) {
            auto ev = PrepareResult(status, errorReason);
            callback(&ev->Record);
            Finish(Sender, SelfId(), ev.release(), Cookie);
        }

        void FinishWithSuccess() {
            FinishWithSuccess([&](auto* /*record*/) {});
        }

        template<typename... TArgs>
        void Finish(TArgs&&... args) {
            auto handle = std::make_unique<IEventHandle>(std::forward<TArgs>(args)...);
            if (RequestSessionId) { // deliver response through interconnection session the request arrived from
                handle->Rewrite(TEvInterconnect::EvForward, RequestSessionId);
            }
            TActivationContext::Send(handle.release());
            PassAway();
        }

        void PassAway() override;

        STFUNC(StateFunc);

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // Notifications from distconf

        void OnNoQuorum();
        void OnError(const TString& errorReason);
        void OnBeginOperation();
        void OnConfigProposed(const std::optional<TString>& errorReason);

        template<typename T>
        void Wrap(T&& callback) {
            try {
                callback();
            } catch (const TExError& error) {
                FinishWithError(error.Status, error.what());
                if (error.IsCritical) {
                    Y_DEBUG_ABORT("critical error during query processing: %s", error.what());
                }
            }
        }
    };

} // NKikimr::NStorage
