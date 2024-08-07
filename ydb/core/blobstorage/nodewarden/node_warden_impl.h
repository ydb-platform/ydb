#pragma once

#include "defs.h"
#include "node_warden.h"
#include "node_warden_events.h"

#include <ydb/core/base/statestorage.h>
#include <ydb/core/blobstorage/dsproxy/group_sessions.h>
#include <ydb/core/node_whiteboard/node_whiteboard.h>

namespace NKikimr::NStorage {

    constexpr ui32 ProxyConfigurationTimeoutMilliseconds = 200;
    constexpr TDuration BackoffMin = TDuration::MilliSeconds(20);
    constexpr TDuration BackoffMax = TDuration::Seconds(5);
    constexpr const char *MockDevicesPath = "/Berkanavt/kikimr/testing/mock_devices.txt";

    template<typename T, typename TPred>
    T *FindOrCreateProtoItem(google::protobuf::RepeatedPtrField<T> *collection, TPred&& pred) {
        for (int i = 0; i < collection->size(); ++i) {
            if (pred(collection->Get(i))) {
                return collection->Mutable(i);
            }
        }
        return collection->Add();
    }

    struct TPDiskKey {
        ui32 NodeId;
        ui32 PDiskId;

        TPDiskKey(ui32 nodeId, ui32 pdiskId)
            : NodeId(nodeId)
            , PDiskId(pdiskId)
        {}

        TPDiskKey(const NKikimrBlobStorage::TNodeWardenServiceSet::TPDisk& pdisk)
            : NodeId(pdisk.GetNodeID())
            , PDiskId(pdisk.GetPDiskID())
        {}

        size_t Hash() const {
            return MultiHash(NodeId, PDiskId);
        }

        friend bool operator <(const TPDiskKey& x, const TPDiskKey& y) {
            return std::make_tuple(x.NodeId, x.PDiskId) < std::make_tuple(y.NodeId, y.PDiskId);
        }

        friend bool operator ==(const TPDiskKey& x, const TPDiskKey& y) {
            return x.NodeId == y.NodeId && x.PDiskId == y.PDiskId;
        }
    };

    struct TUnreportedMetricTag {};

    struct TPDiskRecord
        : TIntrusiveListItem<TPDiskRecord, TUnreportedMetricTag>
    {
        NKikimrBlobStorage::TNodeWardenServiceSet::TPDisk Record;

        std::optional<NKikimrBlobStorage::TPDiskMetrics> PDiskMetrics;

        TReplQuoter::TPtr ReplPDiskReadQuoter;
        TReplQuoter::TPtr ReplPDiskWriteQuoter;

        ui32 RefCount = 0;
        bool Temporary = false;

        TPDiskRecord(NKikimrBlobStorage::TNodeWardenServiceSet::TPDisk record)
            : Record(std::move(record))
        {}
    };

    struct TDrivePathCounters {
        ::NMonitoring::TDynamicCounters::TCounterPtr BadSerialsRead;

        TDrivePathCounters(const TIntrusivePtr<::NMonitoring::TDynamicCounters>& counters, const TString& path) {
            auto driveGroup = GetServiceCounters(counters, "pdisks")->GetSubgroup("path", path);

            BadSerialsRead = driveGroup->GetCounter("BadSerialsRead");
        }
    };

    class TNodeWarden : public TActorBootstrapped<TNodeWarden> {
        TIntrusivePtr<TNodeWardenConfig> Cfg;
        TIntrusivePtr<TDsProxyNodeMon> DsProxyNodeMon;
        TActorId DsProxyNodeMonActor;
        TIntrusivePtr<TDsProxyPerPoolCounters> DsProxyPerPoolCounters;

        // Counters for drives by drive path.
        TMap<TString, TDrivePathCounters> ByPathDriveCounters;

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////

        ui32 LocalNodeId; // NodeId for local node
        TActorId WhiteboardId;

        NKikimrBlobStorage::TNodeWardenServiceSet StaticServices; // these are obtained on start
        NKikimrBlobStorage::TNodeWardenServiceSet DynamicServices; // these are controlled by BSC

        std::map<TPDiskKey, TPDiskRecord> LocalPDisks;
        TIntrusiveList<TPDiskRecord, TUnreportedMetricTag> PDisksWithUnreportedMetrics;
        std::map<ui64, ui32> PDiskRestartRequests;

        struct TPDiskByPathInfo {
            TPDiskKey RunningPDiskId; // currently running PDiskId
            std::optional<NKikimrBlobStorage::TNodeWardenServiceSet::TPDisk> Pending; // pending
        };
        THashMap<TString, TPDiskByPathInfo> PDiskByPath;
        THashSet<ui32> PDisksWaitingToStart;

        ui64 LastScrubCookie = RandomNumber<ui64>();

        ui32 AvailDomainId;
        std::optional<TString> InstanceId; // instance ID of BS_CONTROLLER running this node
        TActorId PipeClientId;

        ui64 NextConfigCookie = 1;
        std::unordered_map<ui64, std::function<void(TEvBlobStorage::TEvControllerConfigResponse*)>> ConfigInFlight;

        TVector<NPDisk::TDriveData> WorkingLocalDrives;

        NPDisk::TOwnerRound LocalPDiskInitOwnerRound = 1;

        bool IgnoreCache = false;

        bool EnableProxyMock = false;
        NKikimrBlobStorage::TMockDevicesConfig MockDevicesConfig;

        struct TEvPrivate {
            enum EEv {
                EvSendDiskMetrics = EventSpaceBegin(TEvents::ES_PRIVATE),
                EvUpdateNodeDrives,
                EvReadCache,
                EvGetGroup,
                EvGroupPendingQueueTick,
                EvDereferencePDisk,
            };

            struct TEvSendDiskMetrics : TEventLocal<TEvSendDiskMetrics, EvSendDiskMetrics> {};
            struct TEvUpdateNodeDrives : TEventLocal<TEvUpdateNodeDrives, EvUpdateNodeDrives> {};

            struct TEvDereferencePDisk : TEventLocal<TEvDereferencePDisk, EvDereferencePDisk> {
                TPDiskKey PDiskKey;
                TEvDereferencePDisk(TPDiskKey pdiskKey) : PDiskKey(pdiskKey) {}
            };
        };

        TControlWrapper EnablePutBatching;
        TControlWrapper EnableVPatch;

        TControlWrapper EnableLocalSyncLogDataCutting;
        TControlWrapper EnableSyncLogChunkCompressionHDD;
        TControlWrapper EnableSyncLogChunkCompressionSSD;
        TControlWrapper MaxSyncLogChunksInFlightHDD;
        TControlWrapper MaxSyncLogChunksInFlightSSD;
        TControlWrapper DefaultHugeGarbagePerMille;

        TReplQuoter::TPtr ReplNodeRequestQuoter;
        TReplQuoter::TPtr ReplNodeResponseQuoter;

        TCostMetricsParametersByMedia CostMetricsParametersByMedia;

        class TPDiskMetadataInteractionActor;

        TControlWrapper SlowDiskThreshold;
        TControlWrapper PredictedDelayMultiplier;

    public:
        struct TGroupRecord;

    public:
        static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
            return NKikimrServices::TActivity::NODE_WARDEN;
        }

        TNodeWarden(const TIntrusivePtr<TNodeWardenConfig> &cfg)
            : Cfg(cfg)
            , EnablePutBatching(Cfg->FeatureFlags.GetEnablePutBatchingForBlobStorage(), false, true)
            , EnableVPatch(Cfg->FeatureFlags.GetEnableVPatch(), false, true)
            , EnableLocalSyncLogDataCutting(0, 0, 1)
            , EnableSyncLogChunkCompressionHDD(1, 0, 1)
            , EnableSyncLogChunkCompressionSSD(0, 0, 1)
            , MaxSyncLogChunksInFlightHDD(10, 1, 1024)
            , MaxSyncLogChunksInFlightSSD(10, 1, 1024)
            , DefaultHugeGarbagePerMille(300, 1, 1000)
            , CostMetricsParametersByMedia({
                TCostMetricsParameters{200},
                TCostMetricsParameters{50},
                TCostMetricsParameters{32},
            })
            , SlowDiskThreshold(2000, 1, 1000000)
            , PredictedDelayMultiplier(1000, 1, 1000)
        {
            Y_ABORT_UNLESS(Cfg->BlobStorageConfig.GetServiceSet().AvailabilityDomainsSize() <= 1);
            AvailDomainId = 1;
            for (const auto& domain : Cfg->BlobStorageConfig.GetServiceSet().GetAvailabilityDomains()) {
                AvailDomainId = domain;
            }
            if (Cfg->DomainsConfig) {
                for (const auto& ssconf : Cfg->DomainsConfig->GetStateStorage()) {
                    BuildStateStorageInfos(ssconf, StateStorageInfo, BoardInfo, SchemeBoardInfo);
                    StateStorageProxyConfigured = true;
                }
            }
        }

        NPDisk::TOwnerRound NextLocalPDiskInitOwnerRound() {
            LocalPDiskInitOwnerRound++;
            return LocalPDiskInitOwnerRound;
        }

        TIntrusivePtr<TPDiskConfig> CreatePDiskConfig(const NKikimrBlobStorage::TNodeWardenServiceSet::TPDisk& pdisk);
        void StartLocalPDisk(const NKikimrBlobStorage::TNodeWardenServiceSet::TPDisk& pdisk, bool temporary);
        void AskBSCToRestartPDisk(ui32 pdiskId, ui64 requestCookie);
        void OnPDiskRestartFinished(ui32 pdiskId, NKikimrProto::EReplyStatus status);
        void DestroyLocalPDisk(ui32 pdiskId);

        void DoRestartLocalPDisk(const NKikimrBlobStorage::TNodeWardenServiceSet::TPDisk& pdisk);
        void OnUnableToRestartPDisk(ui32 pdiskId, TString error);
        void ApplyServiceSetPDisks(const NKikimrBlobStorage::TNodeWardenServiceSet& serviceSet);

        using TServiceSetPDisk = NKikimrBlobStorage::TNodeWardenServiceSet::TPDisk;

        void MergeServiceSetPDisks(NProtoBuf::RepeatedPtrField<TServiceSetPDisk> *to,
            const NProtoBuf::RepeatedPtrField<TServiceSetPDisk>& from);

        void ApplyServiceSetPDisks();

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // VDisks

        void ApplyServiceSet(const NKikimrBlobStorage::TNodeWardenServiceSet &serviceSet,
            bool isStatic, bool comprehensive, bool updateCache, const char *origin);

        void ConfigureLocalProxy(TIntrusivePtr<TBlobStorageGroupInfo> bsInfo);
        TActorId StartEjectedProxy(ui32 groupId);
        void StartInvalidGroupProxy();
        void StopInvalidGroupProxy();
        void StartLocalProxy(ui32 groupId);
        void StartVirtualGroupAgent(ui32 groupId);
        void StartStaticProxies();

        /**
         * Removes drives with bad serial numbers and reports them to monitoring.
         *
         * This method scans a vector of drive data and checks for drives with bad serial numbers.
         * Drives with bad serial numbers are removed from the vector and reported to the monitoring.
         *
         * @param drives A vector of data representing disk drives.
         * @param details A string stream with details about drives.
         */
        void RemoveDrivesWithBadSerialsAndReport(TVector<NPDisk::TDriveData>& drives, TStringStream& details);
        TVector<NPDisk::TDriveData> ListLocalDrives();

        TVector<TString> DrivePathCounterKeys() const {
            TVector<TString> keys;

            for (const auto& [key, _] : ByPathDriveCounters) {
                keys.push_back(key);
            }

            return keys;
        }

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // Pipe management

        void SendToController(std::unique_ptr<IEventBase> ev, ui64 cookie = 0, TActorId sender = {});

        void EstablishPipe();

        void Handle(TEvTabletPipe::TEvClientConnected::TPtr ev);
        void Handle(TEvTabletPipe::TEvClientDestroyed::TPtr ev);
        void OnPipeError();

        void SendRegisterNode();
        void SendInitialGroupRequests();

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // Actor methods
        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

        void PassAway() override;

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // Group statistics reporting
        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

        struct TAggregatorInfo {
            ui32 GroupId;
            TGroupStat Stat;
        };

        TSet<TActorId> RunningVDiskServiceIds;
        TMap<TActorId, TAggregatorInfo> PerAggregatorInfo;

        void ReportLatencies();
        void Handle(TEvGroupStatReport::TPtr ev);
        void StartAggregator(const TActorId& vdiskServiceId, ui32 groupId);
        void StopAggregator(const TActorId& vdiskServiceId);

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // VDisk management code
        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////

        struct TVSlotId {
            const ui32 NodeId;
            const ui32 PDiskId;
            const ui32 VDiskSlotId;

            TVSlotId(ui32 nodeId, ui32 pdiskId, ui32 vdiskSlotId)
                : NodeId(nodeId)
                , PDiskId(pdiskId)
                , VDiskSlotId(vdiskSlotId)
            {}

            TVSlotId(const NKikimrBlobStorage::TVDiskLocation& proto)
                : TVSlotId(proto.GetNodeID(), proto.GetPDiskID(), proto.GetVDiskSlotID())
            {}

            TVSlotId(const NKikimrBlobStorage::TVSlotId& proto)
                : TVSlotId(proto.GetNodeId(), proto.GetPDiskId(), proto.GetVSlotId())
            {}

            TActorId GetVDiskServiceId() const {
                return MakeBlobStorageVDiskID(NodeId, PDiskId, VDiskSlotId);
            }

            void Serialize(NKikimrBlobStorage::TVSlotId *proto) const {
                proto->SetNodeId(NodeId);
                proto->SetPDiskId(PDiskId);
                proto->SetVSlotId(VDiskSlotId);
            }

            auto AsTuple() const { return std::make_tuple(NodeId, PDiskId, VDiskSlotId); }
            friend bool operator <(const TVSlotId& x, const TVSlotId& y) { return x.AsTuple() < y.AsTuple(); }
            friend bool operator <=(const TVSlotId& x, const TVSlotId& y) { return x.AsTuple() <= y.AsTuple(); }
            friend bool operator ==(const TVSlotId& x, const TVSlotId& y) { return x.AsTuple() == y.AsTuple(); }
        };

        struct TGroupRelationTag {};

        struct TVDiskRecord
            : TIntrusiveListItem<TVDiskRecord, TGroupRelationTag>
            , TIntrusiveListItem<TVDiskRecord, TUnreportedMetricTag>
        {
            // Configuration of VDisk never changes since VDisk is created. The only possible actions are:
            // 1. Wiping the disk.
            // 2. Incrementing generation.
            // 3. Becoming a donor.
            // 4. Deleting disk.
            NKikimrBlobStorage::TNodeWardenServiceSet::TVDisk Config;

            // Runtime configuration of VDisk.
            struct TRuntimeData {
                TIntrusivePtr<TBlobStorageGroupInfo> GroupInfo;
                ui32 OrderNumber;
                bool DonorMode;
                bool ReadOnly;
            };
            std::optional<TRuntimeData> RuntimeData;
            bool ShutdownPending = false;
            bool RestartAfterShutdown = false;

            // Last VDiskId reported to Node Whiteboard.
            std::optional<TVDiskID> WhiteboardVDiskId;
            ui64 WhiteboardInstanceGuid;

            NKikimrBlobStorage::EVDiskStatus Status = NKikimrBlobStorage::EVDiskStatus::INIT_PENDING;
            bool OnlyPhantomsRemain = false;
            std::optional<NKikimrBlobStorage::EVDiskStatus> ReportedVDiskStatus; // last reported to BSC
            std::optional<bool> ReportedOnlyPhantomsRemain;

            enum EScrubState : ui32 {
                IDLE,
                QUERY_START_QUANTUM,
                IN_PROGRESS,
                QUANTUM_FINISHED,
                QUANTUM_FINISHED_AND_WAITING_FOR_NEXT_ONE,
            } ScrubState = EScrubState::IDLE;

            NKikimrBlobStorage::TEvControllerScrubQuantumFinished QuantumFinished; // message to be sent
            ui64 ScrubCookie = 0; // cookie used to match QueryStartQuantum requests with StartQuantum responses
            ui64 ScrubCookieForController = 0; // cookie used to communicate with BS_CONTROLLER

            std::optional<NKikimrBlobStorage::TVDiskMetrics> VDiskMetrics;

            // this flag is only used to cooperate between PDisk and VDisk code while processing service set update;
            // it should never escape the ApplyServiceSet() function
            bool UnderlyingPDiskDestroyed = false;

            ui32 GetGroupId() const {
                return Config.GetVDiskID().GetGroupID();
            }

            TVSlotId GetVSlotId() const {
                const auto& loc = Config.GetVDiskLocation();
                return {loc.GetNodeID(), loc.GetPDiskID(), loc.GetVDiskSlotID()};
            }

            TVDiskID GetVDiskId() const {
                const auto& vdiskId = VDiskIDFromVDiskID(Config.GetVDiskID());
                const ui32 generation = RuntimeData
                    ? RuntimeData->GroupInfo->GroupGeneration
                    : vdiskId.GroupGeneration;
                return TVDiskID(vdiskId.GroupID, generation, vdiskId);
            }

            TActorId GetVDiskServiceId() const {
                return GetVSlotId().GetVDiskServiceId();
            }
        };

        std::map<TVSlotId, TVDiskRecord> LocalVDisks;
        THashMap<TActorId, TVSlotId> VDiskIdByActor;
        std::map<TVSlotId, ui64> SlayInFlight;
        std::set<ui32> PDiskRestartInFlight;
        TIntrusiveList<TVDiskRecord, TUnreportedMetricTag> VDisksWithUnreportedMetrics;

        void DestroyLocalVDisk(TVDiskRecord& vdisk);
        void PoisonLocalVDisk(TVDiskRecord& vdisk);
        void StartLocalVDiskActor(TVDiskRecord& vdisk);
        void HandleGone(STATEFN_SIG);
        void ApplyServiceSetVDisks(const NKikimrBlobStorage::TNodeWardenServiceSet& serviceSet);

        // process VDisk configuration
        void ApplyLocalVDiskInfo(const NKikimrBlobStorage::TNodeWardenServiceSet::TVDisk& vdisk);

        void Slay(TVDiskRecord& vdisk);

        void UpdateGroupInfoForDisk(TVDiskRecord& vdisk, const TIntrusivePtr<TBlobStorageGroupInfo>& newInfo);

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // Sync operation queue
        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////

        std::queue<std::unique_ptr<IActor>> SyncOpQ;
        TActorId SyncActorId;

        void InvokeSyncOp(std::unique_ptr<IActor> actor);
        void Handle(TEvents::TEvInvokeResult::TPtr ev);
        void EnqueueSyncOp(std::function<std::function<void()>(const TActorContext&)> callback);

        using TWrappedCacheOp = std::function<std::function<void()>(NKikimrBlobStorage::TNodeWardenServiceSet*)>;
        std::function<std::function<void()>(const TActorContext&)> WrapCacheOp(TWrappedCacheOp operation);

        TWrappedCacheOp UpdateGroupInCache(const NKikimrBlobStorage::TGroupInfo& group);
        TWrappedCacheOp UpdateServiceSet(const NKikimrBlobStorage::TNodeWardenServiceSet& newServices, bool comprehensive,
            std::function<void()> tail);

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // NW group handling code
        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////

        enum class EGroupInfoOrigin {
            BSC,
            GROUP_RESOLVER,
            DSPROXY,
            VDISK,
        };

        struct TGroupRecord {
            TIntrusivePtr<TBlobStorageGroupInfo> Info; // current group info
            ui32 MaxKnownGeneration = 0; // maximum seen generation
            std::optional<NKikimrBlobStorage::TGroupInfo> Group; // group info as a protobuf
            NKikimrBlobStorage::TGroupInfo EncryptionParams; // latest encryption parameters; set only when encryption enabled; overlay in respect to Group
            TActorId ProxyId; // actor id of running DS proxy or agent
            bool AgentProxy = false; // was the group started as an BlobDepot agent proxy?
            bool GetGroupRequestPending = false; // if true, then we are waiting for GetGroup response for this group
            bool ProposeRequestPending = false; // if true, then we have sent ProposeKey request and waiting for the group
            TActorId GroupResolver; // resolver actor id
            TIntrusiveList<TVDiskRecord, TGroupRelationTag> VDisksOfGroup;
        };

        std::unordered_map<ui32, TGroupRecord> Groups;
        std::unordered_set<ui32> EjectedGroups;
        using TGroupPendingQueue = THashMap<ui32, std::deque<std::tuple<TMonotonic, std::unique_ptr<IEventHandle>>>>;
        TGroupPendingQueue GroupPendingQueue;
        std::set<std::tuple<TMonotonic, TGroupPendingQueue::value_type*>> TimeoutToQueue;

        // this function returns group info if possible, or otherwise starts requesting group info and/or proposing key
        // if needed
        TIntrusivePtr<TBlobStorageGroupInfo> NeedGroupInfo(ui32 groupId);

        // propose group key
        void ProposeKey(ui32 groupId, const TEncryptionKey& mainKey, const NKikimrBlobStorage::TGroupInfo& encryptionParams);

        // get encryption key for the group
        TEncryptionKey& GetGroupMainKey(ui32 groupId);

        // process group information structure
        void ApplyGroupInfo(ui32 groupId, ui32 generation, const NKikimrBlobStorage::TGroupInfo *newGroup, bool fromController,
            bool fromResolver);

        // issue GetGroup request to BSC/GroupResolver actor
        void RequestGroupConfig(ui32 groupId, TGroupRecord& group);

        // process group information from the configuration message
        void ApplyGroupInfoFromServiceSet(const NKikimrBlobStorage::TNodeWardenServiceSet& serviceSet);

        // process group information obtained by one of managed entities
        void Handle(TEvBlobStorage::TEvUpdateGroupInfo::TPtr ev);

        void HandleGetGroup(TAutoPtr<IEventHandle> ev);

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////

        std::map<TActorId, std::deque<std::unique_ptr<IEventHandle>>> PendingMessageQ;

        void RegisterPendingActor(const TActorId& actorId);
        void EnqueuePendingMessage(TAutoPtr<IEventHandle> ev);
        void IssuePendingMessages(const TActorId& actorId);

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////

        void Bootstrap();
        void HandleReadCache();
        void Handle(TEvInterconnect::TEvNodeInfo::TPtr ev);
        void Handle(NPDisk::TEvSlayResult::TPtr ev);
        void Handle(TEvRegisterPDiskLoadActor::TPtr ev);
        void Handle(TEvBlobStorage::TEvControllerNodeServiceSetUpdate::TPtr ev);

        void SendDropDonorQuery(ui32 nodeId, ui32 pdiskId, ui32 vslotId, const TVDiskID& vdiskId, TDuration backoff = {});

        void SendVDiskReport(TVSlotId vslotId, const TVDiskID& vdiskId,
            NKikimrBlobStorage::TEvControllerNodeReport::EVDiskPhase phase, TDuration backoff = {});

        void SendPDiskReport(ui32 pdiskId, NKikimrBlobStorage::TEvControllerNodeReport::EPDiskPhase phase);

        void Handle(TEvBlobStorage::TEvControllerUpdateDiskStatus::TPtr ev);
        void Handle(TEvBlobStorage::TEvControllerGroupMetricsExchange::TPtr ev);
        void Handle(TEvPrivate::TEvSendDiskMetrics::TPtr&);
        void Handle(TEvPrivate::TEvUpdateNodeDrives ::TPtr&);
        void Handle(NMon::TEvHttpInfo::TPtr&);
        void RenderJsonGroupInfo(IOutputStream& out, const std::set<ui32>& groupIds);
        void RenderWholePage(IOutputStream&);
        void RenderLocalDrives(IOutputStream&);
        void RenderDSProxies(IOutputStream& out);

        void SendDiskMetrics(bool reportMetrics);
        void Handle(TEvStatusUpdate::TPtr ev);

        void Handle(TEvBlobStorage::TEvDropDonor::TPtr ev);
        void Handle(TEvBlobStorage::TEvAskRestartVDisk::TPtr ev);
        void Handle(TEvBlobStorage::TEvAskWardenRestartPDisk::TPtr ev);
        void Handle(TEvBlobStorage::TEvNotifyWardenPDiskRestarted::TPtr ev);
        void Handle(TEvBlobStorage::TEvControllerConfigResponse::TPtr ev);

        void FillInVDiskStatus(google::protobuf::RepeatedPtrField<NKikimrBlobStorage::TVDiskStatus> *pb, bool initial);

        void HandleForwarded(TAutoPtr<::NActors::IEventHandle> &ev);
        void HandleGroupPendingQueueTick();
        void HandleIncrHugeInit(NIncrHuge::TEvIncrHugeInit::TPtr ev);

        void Handle(TEvBlobStorage::TEvControllerScrubQueryStartQuantum::TPtr ev); // from VDisk
        void Handle(TEvBlobStorage::TEvControllerScrubStartQuantum::TPtr ev); // from BSC
        void Handle(TEvBlobStorage::TEvControllerScrubQuantumFinished::TPtr ev); // from VDisk
        void SendScrubRequests();

        void Handle(NNodeWhiteboard::TEvWhiteboard::TEvBSGroupStateUpdate::TPtr ev);

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////

        TActorId DistributedConfigKeeperId;
        bool StateStorageProxyConfigured = false;
        TIntrusivePtr<TStateStorageInfo> StateStorageInfo;
        TIntrusivePtr<TStateStorageInfo> BoardInfo;
        TIntrusivePtr<TStateStorageInfo> SchemeBoardInfo;

        void StartDistributedConfigKeeper();
        void ForwardToDistributedConfigKeeper(STATEFN_SIG);

        NKikimrBlobStorage::TStorageConfig StorageConfig;
        THashSet<TActorId> StorageConfigSubscribers;

        void Handle(TEvNodeWardenQueryStorageConfig::TPtr ev);
        void Handle(TEvNodeWardenStorageConfig::TPtr ev);
        void HandleUnsubscribe(STATEFN_SIG);
        void ApplyStorageConfig(const NKikimrBlobStorage::TNodeWardenServiceSet& current,
                const NKikimrBlobStorage::TNodeWardenServiceSet *proposed);
        void ApplyStateStorageConfig(const NKikimrBlobStorage::TStorageConfig *proposed);
        void ApplyStaticServiceSet(const NKikimrBlobStorage::TNodeWardenServiceSet& ss);

        void Handle(TEvNodeWardenQueryBaseConfig::TPtr ev);

        void Handle(TEvNodeWardenReadMetadata::TPtr ev);
        void Handle(TEvNodeWardenWriteMetadata::TPtr ev);
        TPDiskKey GetPDiskForMetadata(const TString& path);
        void Handle(TEvPrivate::TEvDereferencePDisk::TPtr ev);

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////

        ui64 NextInvokeCookie = 1;
        std::unordered_map<ui64, std::function<void(TEvNodeConfigInvokeOnRootResult&)>> InvokeCallbacks;

        void Handle(TEvNodeConfigInvokeOnRootResult::TPtr ev);

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////

        struct TGroupResolverContext : TThrRefBase {
            struct TImpl;
            std::unique_ptr<TImpl> Impl;
            TGroupResolverContext();
            ~TGroupResolverContext();
        };
        TIntrusivePtr<TGroupResolverContext> GroupResolverContext = MakeIntrusive<TGroupResolverContext>();

        class TGroupResolverActor;

        IActor *CreateGroupResolverActor(ui32 groupId);
        void Handle(TEvNodeWardenQueryGroupInfo::TPtr ev);

        bool VDiskStatusChanged = false;

        STATEFN(StateOnline) {
            switch (ev->GetTypeRewrite()) {
                fFunc(TEvBlobStorage::TEvPut::EventType, HandleForwarded);
                fFunc(TEvBlobStorage::TEvGet::EventType, HandleForwarded);
                fFunc(TEvBlobStorage::TEvBlock::EventType, HandleForwarded);
                fFunc(TEvBlobStorage::TEvPatch::EventType, HandleForwarded);
                fFunc(TEvBlobStorage::TEvDiscover::EventType, HandleForwarded);
                fFunc(TEvBlobStorage::TEvRange::EventType, HandleForwarded);
                fFunc(TEvBlobStorage::TEvCollectGarbage::EventType, HandleForwarded);
                fFunc(TEvBlobStorage::TEvStatus::EventType, HandleForwarded);
                fFunc(TEvBlobStorage::TEvAssimilate::EventType, HandleForwarded);
                fFunc(TEvBlobStorage::TEvBunchOfEvents::EventType, HandleForwarded);
                fFunc(TEvRequestProxySessionsState::EventType, HandleForwarded);

                cFunc(TEvPrivate::EvGroupPendingQueueTick, HandleGroupPendingQueueTick);

                hFunc(NIncrHuge::TEvIncrHugeInit, HandleIncrHugeInit);

                hFunc(TEvInterconnect::TEvNodeInfo, Handle);

                hFunc(TEvTabletPipe::TEvClientConnected, Handle);
                hFunc(TEvTabletPipe::TEvClientDestroyed, Handle);

                hFunc(NPDisk::TEvSlayResult, Handle);

                hFunc(TEvRegisterPDiskLoadActor, Handle);

                hFunc(TEvStatusUpdate, Handle);
                hFunc(TEvBlobStorage::TEvDropDonor, Handle);
                hFunc(TEvBlobStorage::TEvAskRestartVDisk, Handle);
                hFunc(TEvBlobStorage::TEvAskWardenRestartPDisk, Handle);
                hFunc(TEvBlobStorage::TEvNotifyWardenPDiskRestarted, Handle);

                hFunc(TEvGroupStatReport, Handle);

                hFunc(TEvBlobStorage::TEvControllerNodeServiceSetUpdate, Handle);
                hFunc(TEvBlobStorage::TEvUpdateGroupInfo, Handle);
                hFunc(TEvBlobStorage::TEvControllerUpdateDiskStatus, Handle);
                hFunc(TEvBlobStorage::TEvControllerGroupMetricsExchange, Handle);
                hFunc(TEvPrivate::TEvSendDiskMetrics, Handle);
                hFunc(TEvPrivate::TEvUpdateNodeDrives, Handle);
                hFunc(NMon::TEvHttpInfo, Handle);
                cFunc(NActors::TEvents::TSystem::Poison, PassAway);

                hFunc(TEvBlobStorage::TEvControllerScrubQueryStartQuantum, Handle);
                hFunc(TEvBlobStorage::TEvControllerScrubStartQuantum, Handle);
                hFunc(TEvBlobStorage::TEvControllerScrubQuantumFinished, Handle);

                hFunc(TEvents::TEvInvokeResult, Handle);

                hFunc(TEvNodeWardenQueryGroupInfo, Handle);
                hFunc(TEvNodeWardenQueryStorageConfig, Handle);
                hFunc(TEvNodeWardenStorageConfig, Handle);
                fFunc(TEvents::TSystem::Unsubscribe, HandleUnsubscribe);

                // proxy requests for the NodeWhiteboard to prevent races
                hFunc(NNodeWhiteboard::TEvWhiteboard::TEvBSGroupStateUpdate, Handle);

                hFunc(TEvBlobStorage::TEvControllerConfigResponse, Handle);

                cFunc(TEvPrivate::EvReadCache, HandleReadCache);
                fFunc(TEvPrivate::EvGetGroup, HandleGetGroup);

                fFunc(TEvBlobStorage::EvNodeConfigPush, ForwardToDistributedConfigKeeper);
                fFunc(TEvBlobStorage::EvNodeConfigReversePush, ForwardToDistributedConfigKeeper);
                fFunc(TEvBlobStorage::EvNodeConfigUnbind, ForwardToDistributedConfigKeeper);
                fFunc(TEvBlobStorage::EvNodeConfigScatter, ForwardToDistributedConfigKeeper);
                fFunc(TEvBlobStorage::EvNodeConfigGather, ForwardToDistributedConfigKeeper);
                fFunc(TEvBlobStorage::EvNodeConfigInvokeOnRoot, ForwardToDistributedConfigKeeper);
                fFunc(TEvBlobStorage::EvNodeWardenDynamicConfigSubscribe, ForwardToDistributedConfigKeeper);
                fFunc(TEvBlobStorage::EvNodeWardenDynamicConfigPush, ForwardToDistributedConfigKeeper);

                hFunc(TEvNodeWardenQueryBaseConfig, Handle);
                hFunc(TEvNodeConfigInvokeOnRootResult, Handle);

                fFunc(TEvents::TSystem::Gone, HandleGone);

                hFunc(TEvNodeWardenReadMetadata, Handle);
                hFunc(TEvNodeWardenWriteMetadata, Handle);
                hFunc(TEvPrivate::TEvDereferencePDisk, Handle);

                default:
                    EnqueuePendingMessage(ev);
                    break;
            }

            if (VDiskStatusChanged) {
                SendDiskMetrics(false);
                VDiskStatusChanged = false;
            }
        }
    };

}

template<>
inline void Out<NKikimr::NStorage::TNodeWarden::TVSlotId>(IOutputStream& o, const NKikimr::NStorage::TNodeWarden::TVSlotId& x) {
    o << x.NodeId << ":" << x.PDiskId << ":" << x.VDiskSlotId;
}

template<>
struct THash<NKikimr::NStorage::TPDiskKey> {
    size_t operator ()(const NKikimr::NStorage::TPDiskKey& x) const { return x.Hash(); }
};
