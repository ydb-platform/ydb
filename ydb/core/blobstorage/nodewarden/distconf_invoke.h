#pragma once

#include "distconf.h"

namespace NKikimr::NStorage {

    class TDistributedConfigKeeper::TInvokeRequestHandlerActor : public TActorBootstrapped<TInvokeRequestHandlerActor> {
        TDistributedConfigKeeper* const Self;
        const std::weak_ptr<TLifetimeToken> LifetimeToken;
        const std::weak_ptr<TScepter> Scepter;
        std::unique_ptr<TEventHandle<TEvNodeConfigInvokeOnRoot>> Event;
        const TActorId Sender;
        const ui64 Cookie;
        const TActorId RequestSessionId;

        TActorId ParentId;
        ui32 WaitingReplyFromNode = 0;

        using TQuery = NKikimrBlobStorage::TEvNodeConfigInvokeOnRoot;
        using TResult = NKikimrBlobStorage::TEvNodeConfigInvokeOnRootResult;

        using TGatherCallback = std::function<std::optional<TString>(TEvGather*)>;
        ui64 NextScatterCookie = 1;
        THashMap<ui64, TGatherCallback> ScatterTasks;

        std::shared_ptr<TLifetimeToken> RequestHandlerToken = std::make_shared<TLifetimeToken>();

    public:
        TInvokeRequestHandlerActor(TDistributedConfigKeeper *self, std::unique_ptr<TEventHandle<TEvNodeConfigInvokeOnRoot>>&& ev);

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

        void FetchStorageConfig(bool manual, bool fetchMain, bool fetchStorage);
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
        // Configuration proposition

        void AdvanceGeneration();
        void StartProposition(NKikimrBlobStorage::TStorageConfig *config, bool updateFields = true);

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // Query termination and result delivery

        bool RunCommonChecks();

        std::unique_ptr<TEvNodeConfigInvokeOnRootResult> PrepareResult(TResult::EStatus status, std::optional<TStringBuf> errorReason);
        void FinishWithError(TResult::EStatus status, const TString& errorReason);

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
    };

} // NKikimr::NStorage
