#pragma once

#include "distconf.h"

namespace NKikimr::NStorage {

    class TDistributedConfigKeeper::TInvokeRequestHandlerActor : public TActor<TInvokeRequestHandlerActor> {
        friend class TDistributedConfigKeeper;

        TDistributedConfigKeeper* const Self;
        const std::weak_ptr<TLifetimeToken> LifetimeToken;
        const ui64 InvokePipelineGeneration;
        TInvokeQuery Query;

        ui32 WaitingReplyFromNode = 0;

        using TQuery = NKikimrBlobStorage::TEvNodeConfigInvokeOnRoot;
        using TResult = NKikimrBlobStorage::TEvNodeConfigInvokeOnRootResult;

        using TGatherCallback = std::function<void(TEvGather*)>;
        ui64 NextScatterCookie = 1;
        THashMap<ui64, TGatherCallback> ScatterTasks;

        std::shared_ptr<TLifetimeToken> RequestHandlerToken = std::make_shared<TLifetimeToken>();

        bool InvokedWithoutScepter = false;

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
        TInvokeRequestHandlerActor(TDistributedConfigKeeper *self, TInvokeQuery&& query);
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
        void SelfHealStateStorage(ui32 waitForConfigStep, bool forceHeal, bool pileupReplicas);
        void SelfHealNodesStateUpdate(const TQuery::TSelfHealNodesStateUpdate& cmd);
        void GetStateStorageConfig(const TQuery::TGetStateStorageConfig& cmd);

        void GetCurrentStateStorageConfig(NKikimrBlobStorage::TStateStorageConfig* currentConfig);
        bool GetRecommendedStateStorageConfig(NKikimrBlobStorage::TStateStorageConfig* currentConfig, bool pileupReplicas);
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

        void SwitchBridgeClusterState(const TQuery::TSwitchBridgeClusterState& cmd);

        void NotifyBridgeSyncFinished(const TQuery::TNotifyBridgeSyncFinished& cmd);

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // Configuration proposition

        void AdvanceGeneration();
        void StartProposition(NKikimrBlobStorage::TStorageConfig *config, bool acceptLocalQuorum = false,
            bool requireScepter = true, bool mindPrev = true,
            const NKikimrBlobStorage::TStorageConfig *propositionBase = nullptr);
        void Handle(TEvPrivate::TEvConfigProposed::TPtr ev);

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // Query termination and result delivery

        void RunCommonChecks(bool requireScepter = true);

        void Finish(TResult::EStatus status, std::optional<TStringBuf> errorReason,
            const std::function<void(TResult*)>& callback = {});

        void PassAway() override;

        STFUNC(StateFunc);
    };

} // NKikimr::NStorage
