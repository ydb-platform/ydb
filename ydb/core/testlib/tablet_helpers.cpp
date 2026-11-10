#include "tablet_helpers.h"
#include <ydb/core/base/appdata.h>
#include <ydb/core/base/hive.h>
#include <ydb/core/base/statestorage.h>
#include <ydb/core/base/statestorage_impl.h>
#include <ydb/core/base/tablet_pipe.h>
#include <ydb/core/base/tablet_resolver.h>
#include <ydb/core/blobstorage/pdisk/blobstorage_pdisk_tools.h>
#include <ydb/core/blobstorage/base/blobstorage_events.h>
#include <ydb/core/tablet/bootstrapper.h>
#include <ydb/core/tablet/resource_broker.h>
#include <ydb/core/tablet_flat/tablet_flat_executed.h>
#include <ydb/core/tablet/tablet_counters_aggregator.h>
#include <ydb/core/tablet_flat/shared_sausagecache.h>
#include <ydb/core/engine/minikql/flat_local_tx_factory.h>
#include <ydb/core/mind/local.h>
#include <ydb/core/scheme/tablet_scheme.h>
#include <ydb/core/tx/datashard/datashard.h>
#include <ydb/core/tx/columnshard/columnshard.h>
#include <ydb/core/tx/tx_allocator/txallocator.h>
#include <ydb/core/tx/coordinator/coordinator.h>
#include <ydb/core/tx/mediator/mediator.h>
#include <ydb/core/tx/replication/controller/controller.h>
#include <ydb/core/tx/schemeshard/schemeshard.h>
#include <ydb/core/tx/sequenceshard/sequenceshard.h>
#include <ydb/core/tx/time_cast/time_cast.h>
#include <ydb/core/persqueue/pqtablet/cache/pq_l2_service.h>
#include <ydb/core/util/console.h>

#include <google/protobuf/text_format.h>

#include <util/folder/dirut.h>
#include <util/random/mersenne.h>
#include <library/cpp/regex/pcre/regexp.h>
#include <util/string/printf.h>
#include <util/string/subst.h>
#include <util/system/env.h>
#include <util/system/sanitizers.h>
#include <ydb/library/actors/interconnect/interconnect.h>

#include <library/cpp/testing/unittest/registar.h>
#include <ydb/core/kesus/tablet/tablet.h>
#include <ydb/core/keyvalue/keyvalue.h>
#include <ydb/core/persqueue/pq.h>
#include <ydb/core/sys_view/processor/processor.h>
#include <ydb/core/statistics/aggregator/aggregator.h>
#include <ydb/core/graph/api/shard.h>
#include <ydb/services/udf_store/compile_controller/compile_controller.h>

#include <ydb/core/testlib/basics/storage.h>
#include <ydb/core/testlib/basics/appdata.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::HIVE

const ui64 PQ_CACHE_MAX_SIZE_MB = 32;
const TDuration PQ_CACHE_KEEP_TIMEOUT = TDuration::Seconds(10);

namespace NKikimr {

    class TFakeMediatorTimecastProxy : public TActor<TFakeMediatorTimecastProxy> {
    public:
        static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
            return NKikimrServices::TActivity::TX_MEDIATOR_TIMECAST_ACTOR;
        }

        TFakeMediatorTimecastProxy()
            : TActor(&TFakeMediatorTimecastProxy::StateFunc)
        {}

        STFUNC(StateFunc) {
            switch (ev->GetTypeRewrite()) {
                hFunc(TEvMediatorTimecast::TEvRegisterTablet, Handle);
                hFunc(TEvMediatorTimecast::TEvSubscribeReadStep, Handle);
            }
        }

        void Handle(TEvMediatorTimecast::TEvRegisterTablet::TPtr& ev) {
            const ui64 tabletId = ev->Get()->TabletId;
            auto& entry = Entries[tabletId];
            if (!entry) {
                entry = new TMediatorTimecastSharedEntry();
            }

            Send(ev->Sender, new TEvMediatorTimecast::TEvRegisterTabletResult(tabletId, new TMediatorTimecastEntry(entry, entry)));
        }

        void Handle(TEvMediatorTimecast::TEvSubscribeReadStep::TPtr& ev) {
            const ui64 coordinatorId = ev->Get()->CoordinatorId;
            auto& entry = ReadSteps[coordinatorId];
            if (!entry) {
                entry = new TMediatorTimecastReadStep();
            }

            Send(ev->Sender, new TEvMediatorTimecast::TEvSubscribeReadStepResult(coordinatorId, 0, entry->Get(), entry));
        }

    private:
        THashMap<ui64, TIntrusivePtr<TMediatorTimecastSharedEntry>> Entries;
        THashMap<ui64, TIntrusivePtr<TMediatorTimecastReadStep>> ReadSteps;
    };

    void SetupMediatorTimecastProxy(TTestActorRuntime& runtime, ui32 nodeIndex, bool useFake = false)
    {
        runtime.AddLocalService(
            MakeMediatorTimecastProxyID()
            , TActorSetupCmd(useFake ? new TFakeMediatorTimecastProxy() : CreateMediatorTimecastProxy()
                            , TMailboxType::Revolving, 0)
            , nodeIndex);
    }

    void SetupTabletCountersAggregator(TTestActorRuntime& runtime, ui32 nodeIndex)
    {
        runtime.AddLocalService(MakeTabletCountersAggregatorID(runtime.GetNodeId(nodeIndex)),
            TActorSetupCmd(CreateTabletCountersAggregator(false), TMailboxType::Revolving, 0), nodeIndex);
    }

    void SetupPQNodeCache(TTestActorRuntime& runtime, ui32 nodeIndex)
    {
        struct NPQ::TCacheL2Parameters l2Params = {PQ_CACHE_MAX_SIZE_MB, PQ_CACHE_KEEP_TIMEOUT};
        runtime.AddLocalService(NPQ::MakePersQueueL2CacheID(),
            TActorSetupCmd(
                NPQ::CreateNodePersQueueL2Cache(l2Params, runtime.GetDynamicCounters(0)),
                TMailboxType::Simple, 0),
            nodeIndex);
    }

    struct TUltimateNodes : public NFake::INode {
        TUltimateNodes(TTestActorRuntime &runtime, const TAppPrepare *app)
            : Runtime(runtime)
        {

            if (runtime.IsRealThreads()) {
                return;
            }

            const auto& domainsInfo = app->Domains;
            if (!domainsInfo || !domainsInfo->Domain) {
                return;
            }

            const TDomainsInfo::TDomain *domain = domainsInfo->GetDomain();
            UseFakeTimeCast |= domain->Mediators.size() == 0;
        }

        void Birth(ui32 node) noexcept override
        {
            SetupMediatorTimecastProxy(Runtime, node, UseFakeTimeCast);
            SetupMonitoringProxy(Runtime, node);
            SetupTabletCountersAggregator(Runtime, node);
            SetupGRpcProxyStatus(Runtime, node);
            SetupNodeWhiteboard(Runtime, node);
            SetupNodeTabletMonitor(Runtime, node);
            SetupPQNodeCache(Runtime, node);
        }

        TTestActorRuntime &Runtime;
        bool UseFakeTimeCast = false;
    };

    TActorId FollowerTablet(TTestActorRuntime &runtime, const TActorId &launcher, TTabletStorageInfo *info, std::function<IActor * (const TActorId &, TTabletStorageInfo *)> op) {
        return runtime.Register(CreateTabletFollower(launcher, info, new TTabletSetupInfo(op, TMailboxType::Simple, 0, TMailboxType::Simple, 0), 0, new TResourceProfiles));
    }

    void SetupTabletServices(TTestActorRuntime &runtime, TAppPrepare *app, bool mockDisk, NFake::TStorage storage,
                            const NSharedCache::TSharedCacheConfig* sharedCacheConfig, bool forceFollowers,
                            TVector<TIntrusivePtr<NFake::TProxyDS>> dsProxies) {
        TAutoPtr<TAppPrepare> dummy;
        if (app == nullptr) {
            dummy = app = new TAppPrepare;
        }
        TUltimateNodes nodes(runtime, app);
        SetupBasicServices(runtime, *app, mockDisk, &nodes, storage, sharedCacheConfig, forceFollowers, dsProxies);
    }

    TDomainsInfo::TDomain::TStoragePoolKinds DefaultPoolKinds(ui32 count) {
        TDomainsInfo::TDomain::TStoragePoolKinds storagePoolKinds;

        for (ui32 poolNum = 1; poolNum <= count; ++poolNum) {
            TString poolKind = "pool-kind-" + ToString(poolNum);
            NKikimrBlobStorage::TDefineStoragePool& hddPool = storagePoolKinds[poolKind];
            hddPool.SetBoxId(1);
            hddPool.SetErasureSpecies("none");
            hddPool.SetVDiskKind("Default");
            hddPool.AddPDiskFilter()->AddProperty()->SetType(NKikimrBlobStorage::ROT);
            hddPool.SetKind(poolKind);
            hddPool.SetStoragePoolId(poolNum);
            hddPool.SetName("pool-" + ToString(poolNum));
        }

        return storagePoolKinds;
    }

    void SetSplitMergePartCountLimit(TTestActorRuntime* runtime, i64 val) {
        TControlBoard::SetValue(val, runtime->GetAppData().Icb->SchemeShardControls.SplitMergePartCountLimit);
    }

    void SetAllowServerlessStorageBilling(TTestActorRuntime* runtime, bool isAllow) {
        TControlBoard::SetValue(isAllow, runtime->GetAppData().Icb->SchemeShardControls.AllowServerlessStorageBilling);
    }

    void SetupChannelProfiles(TAppPrepare &app, ui32 nchannels) {
        auto& poolKinds = app.Domains->GetDomain()->StoragePoolTypes;
        Y_ABORT_UNLESS(!poolKinds.empty());

        TIntrusivePtr<TChannelProfiles> channelProfiles = new TChannelProfiles;

        {//Set unexisted pool type for default profile # 0
            channelProfiles->Profiles.emplace_back();
            auto& profile = channelProfiles->Profiles.back();
            for (ui32 channelIdx = 0; channelIdx < nchannels; ++channelIdx) {
                profile.Channels.emplace_back(TBlobStorageGroupType::ErasureNone, 0, NKikimrBlobStorage::TVDiskKind::Default, poolKinds.begin()->first);
            }
        }

        //add mixed pool profile # 1
        if (poolKinds) {
            channelProfiles->Profiles.emplace_back();
            TChannelProfiles::TProfile &profile = channelProfiles->Profiles.back();
            auto poolIt = poolKinds.begin();

            profile.Channels.emplace_back(TBlobStorageGroupType::ErasureNone, 0, NKikimrBlobStorage::TVDiskKind::Default, poolIt->first);

            if (poolKinds.size() > 1) {
                ++poolIt;
            }
            profile.Channels.emplace_back(TBlobStorageGroupType::ErasureNone, 0, NKikimrBlobStorage::TVDiskKind::Default, poolIt->first);

            if (poolKinds.size() > 2) {
                ++poolIt;

                profile.Channels.emplace_back(TBlobStorageGroupType::ErasureNone, 0, NKikimrBlobStorage::TVDiskKind::Default, poolIt->first);
            }
        }

        //add one pool profile for each pool # 2 .. poolKinds + 2
        for (auto& kind: poolKinds) {
            channelProfiles->Profiles.emplace_back();
            auto& profile = channelProfiles->Profiles.back();
            for (ui32 channelIdx = 0; channelIdx < nchannels; ++channelIdx) {
                profile.Channels.emplace_back(TBlobStorageGroupType::ErasureNone, 0, NKikimrBlobStorage::TVDiskKind::Default, kind.first);
            }
        }

        app.SetChannels(std::move(channelProfiles));
    }

    void SetupBoxAndStoragePool(TTestActorRuntime &runtime, const TActorId& sender, ui32 nGroups) {
        NTabletPipe::TClientConfig pipeConfig;
        pipeConfig.RetryPolicy = NTabletPipe::TClientRetryPolicy::WithRetries();

        //get NodesInfo, nodes hostname and port are interested
        runtime.Send(new IEventHandle(GetNameserviceActorId(), sender, new TEvInterconnect::TEvListNodes));
        TAutoPtr<IEventHandle> handleNodesInfo;
        auto nodesInfo = runtime.GrabEdgeEventRethrow<TEvInterconnect::TEvNodesInfo>(handleNodesInfo);
        auto bsConfigureRequest = MakeHolder<TEvBlobStorage::TEvControllerConfigRequest>();

        NKikimrBlobStorage::TDefineBox boxConfig;
        boxConfig.SetBoxId(1);

        ui32 nodeId = runtime.GetNodeId(0);
        Y_ABORT_UNLESS(nodesInfo->Nodes[0].NodeId == nodeId);
        auto& nodeInfo = nodesInfo->Nodes[0];

        NKikimrBlobStorage::TDefineHostConfig hostConfig;
        hostConfig.SetHostConfigId(nodeId);
        TString path = TStringBuilder() << runtime.GetTempDir() << "pdisk_1.dat";
        hostConfig.AddDrive()->SetPath(path);
        Cerr << "tablet_helpers.cpp: SetPath # " << path << Endl;
        bsConfigureRequest->Record.MutableRequest()->AddCommand()->MutableDefineHostConfig()->CopyFrom(hostConfig);

        auto &host = *boxConfig.AddHost();
        host.MutableKey()->SetFqdn(nodeInfo.Host);
        host.MutableKey()->SetIcPort(nodeInfo.Port);
        host.SetHostConfigId(hostConfig.GetHostConfigId());
        bsConfigureRequest->Record.MutableRequest()->AddCommand()->MutableDefineBox()->CopyFrom(boxConfig);

        for (const auto& [kind, pool] : runtime.GetAppData().DomainsInfo->GetDomain()->StoragePoolTypes) {
            NKikimrBlobStorage::TDefineStoragePool storagePool(pool);
            storagePool.SetNumGroups(nGroups);
            bsConfigureRequest->Record.MutableRequest()->AddCommand()->MutableDefineStoragePool()->CopyFrom(storagePool);
        }

        runtime.SendToPipe(MakeBSControllerID(), sender, bsConfigureRequest.Release(), 0, GetPipeConfigWithRetries());

        TAutoPtr<IEventHandle> handleConfigureResponse;
        auto configureResponse = runtime.GrabEdgeEventRethrow<TEvBlobStorage::TEvControllerConfigResponse>(handleConfigureResponse);
        if (!configureResponse->Record.GetResponse().GetSuccess()) {
            Cerr << "\n\n configResponse is #" << configureResponse->Record.DebugString() << "\n\n";
        }
        UNIT_ASSERT(configureResponse->Record.GetResponse().GetSuccess());
    }

    ui64 GetFreePDiskSize(TTestActorRuntime& runtime, const TActorId& sender) {
        TActorId pdiskServiceId = MakeBlobStoragePDiskID(runtime.GetNodeId(0), 0);
        runtime.Send(new IEventHandle(pdiskServiceId, sender, nullptr));
        TAutoPtr<IEventHandle> handle;
        auto event = runtime.GrabEdgeEvent<NMon::TEvHttpInfoRes>(handle);
        UNIT_ASSERT(event);
        //Cout << event->Answer << "\n";
        ui64 totalFreeSize = 0;
        for (ui32 i = 0; i < 2; ++i) {
            TString regex = Sprintf(".*sensor=%s:\\s(\\d+).*", i == 0 ? "FreeChunks" : "UntrimmedFreeChunks");
            TRegExBase matcher(regex);
            regmatch_t groups[2] = {};
            matcher.Exec(event->Answer.data(), groups, 0, 2);
            const ui64 freeSize = IntFromString<ui64, 10>(event->Answer.data() + groups[1].rm_so, groups[1].rm_eo - groups[1].rm_so);
            totalFreeSize += freeSize;
        }

        return totalFreeSize;
    };

    class TFollowerLauncher : public TActorBootstrapped<TFollowerLauncher> {
    private:
        ui64 TabletId;
        ui32 FollowerId;
        TActorId FollowerActorId;

    public:
        TFollowerLauncher(ui64 tabletId, ui32 follewerId)
            : TabletId(tabletId)
            , FollowerId(follewerId)
        {
        }

        void Bootstrap(const TActorContext& ctx) {
            CreateFollower();

            YDB_LOG_INFO_CTX(ctx, "Follower launcher created follower for tablet",
                {"selfId", SelfId()},
                {"followerId", FollowerId},
                {"tabletId", TabletId},
                {"followerActorId", FollowerActorId});

            Become(&TThis::StateWork);
        }

        STFUNC(StateWork) {
            switch (ev->GetTypeRewrite()) {
                HFunc(TEvTablet::TEvTabletDead, Handle);
                HFunc(TEvents::TEvPoison, Handle);
            }
        }

        void Handle(TEvTablet::TEvTabletDead::TPtr& ev, const TActorContext& ctx) {
            if (ev->Sender != FollowerActorId) {
                YDB_LOG_INFO_CTX(ctx, "Follower launcher received TEvTabletDead for an unknown actor",
                    {"selfId", SelfId()},
                    {"tabletId", ev->Get()->TabletID},
                    {"ignored", FollowerActorId});

                return;
            }

            LOG_INFO_S(
                ctx,
                NKikimrServices::HIVE,
                "[Follower launcher " << SelfId()
                    << "] Received EvTabletDead from follower ID " << FollowerId
                    << " for tabletId " << TabletId
                    << ": " << FollowerActorId
            );

            // The follower has died, start a new one
            FollowerActorId = {};
            CreateFollower();

            YDB_LOG_INFO_CTX(ctx, "Follower launcher restarted for tablet",
                {"selfId", SelfId()},
                {"followerId", FollowerId},
                {"tabletId", TabletId},
                {"followerActorId", FollowerActorId});
        }

        void Handle(TEvents::TEvPoison::TPtr& /* ev */, const TActorContext& ctx) {
            if (FollowerActorId) {
                YDB_LOG_INFO_CTX(ctx, "Follower launcher destroying follower for tablet",
                    {"selfId", SelfId()},
                    {"followerId", FollowerId},
                    {"tabletId", TabletId},
                    {"followerActorId", FollowerActorId});

                ctx.Send(FollowerActorId, new TEvents::TEvPoisonPill());
                FollowerActorId = {};
            };

            Die(ctx);
        }

    private:
        void CreateFollower() {
            FollowerActorId = Register(
                CreateTabletFollower(
                    SelfId(),
                    CreateTestTabletInfo(
                        TabletId,
                        TTabletTypes::DataShard,
                        DataGroupErasure
                    ),
                    new TTabletSetupInfo(
                        &CreateDataShard,
                        TMailboxType::Simple,
                        0,
                        TMailboxType::Simple,
                        0
                    ),
                    FollowerId
                )
            );
        }
    };

    class TFakeHive : public TActor<TFakeHive>, public NTabletFlatExecutor::TTabletExecutedFlat {
    public:
        static std::function<IActor* (const TActorId &, TTabletStorageInfo*)> DefaultGetTabletCreationFunc(ui32 type) {
            Y_UNUSED(type);
            return nullptr;
        }

        using TTabletInfo = TFakeHiveTabletInfo;
        using TState = TFakeHiveState;

        static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
            return NKikimrServices::TActivity::HIVE_ACTOR;
        }

        TFakeHive(const TActorId &tablet, TTabletStorageInfo *info, TState::TPtr state,
                  TGetTabletCreationFunc getTabletCreationFunc)
            : TActor<TFakeHive>(&TFakeHive::StateInit)
            , NTabletFlatExecutor::TTabletExecutedFlat(info, tablet, new NMiniKQL::TMiniKQLFactory)
            , State(state)
            , GetTabletCreationFunc(getTabletCreationFunc)
        {
        }

        void DefaultSignalTabletActive(const TActorContext &) override {
            // must be empty
        }

        void OnActivateExecutor(const TActorContext &ctx) final {
            Become(&TFakeHive::StateWork);
            SignalTabletActive(ctx);

            YDB_LOG_INFO_CTX(ctx, "Started, primary subdomain",
                {"tabletId", TabletID()},
                {"primarySubDomainKey", PrimarySubDomainKey});
        }

        void OnDetach(const TActorContext &ctx) override {
            Die(ctx);
        }

        void OnTabletDead(TEvTablet::TEvTabletDead::TPtr &ev, const TActorContext &ctx) override {
            Y_UNUSED(ev);
            Die(ctx);
        }

        void StateInit(STFUNC_SIG) {
            StateInitImpl(ev, SelfId());
        }

        void StateWork(STFUNC_SIG) {
            switch (ev->GetTypeRewrite()) {
                HFunc(TEvTablet::TEvTabletDead, HandleTabletDead);
                HFunc(TEvHive::TEvConfigureHive, Handle);
                HFunc(TEvHive::TEvCreateTablet, Handle);
                HFunc(TEvHive::TEvAdoptTablet, Handle);
                HFunc(TEvHive::TEvDeleteTablet, Handle);
                HFunc(TEvHive::TEvDeleteOwnerTablets, Handle);
                HFunc(TEvHive::TEvStopTablet, Handle);
                HFunc(TEvHive::TEvRequestHiveInfo, Handle);
                HFunc(TEvHive::TEvInitiateTabletExternalBoot, Handle);
                HFunc(TEvHive::TEvUpdateTabletsObject, Handle);
                HFunc(TEvFakeHive::TEvSubscribeToTabletDeletion, Handle);
                HFunc(TEvHive::TEvUpdateDomain, Handle);
                HFunc(TEvFakeHive::TEvRequestDomainInfo, Handle);
                HFunc(TEvents::TEvPoisonPill, Handle);
            }
        }

        void BrokenState(STFUNC_SIG) {
            switch (ev->GetTypeRewrite()) {
                HFunc(TEvTablet::TEvTabletDead, HandleTabletDead);
            }
        }

        void Handle(TEvHive::TEvConfigureHive::TPtr& ev, const TActorContext& ctx) {
            YDB_LOG_INFO_CTX(ctx, "TEvConfigureHive",
                {"tabletId", TabletID()},
                {"msg", ev->Get()->Record});

            const auto& subdomainKey(ev->Get()->Record.GetDomain());
            PrimarySubDomainKey = TSubDomainKey(subdomainKey);

            YDB_LOG_INFO_CTX(ctx, "TEvConfigureHive, subdomain set",
                {"tabletId", TabletID()},
                {"subdomainKey", subdomainKey});
            ctx.Send(ev->Sender, new TEvSubDomain::TEvConfigureStatus(NKikimrTx::TEvSubDomainConfigurationAck::SUCCESS, TabletID()));
        }

        void Handle(TEvHive::TEvCreateTablet::TPtr& ev, const TActorContext& ctx) {
            YDB_LOG_INFO_CTX(ctx, "TEvCreateTablet",
                {"tabletId", TabletID()},
                {"msg", ev->Get()->Record});
            Cerr << "FAKEHIVE " << TabletID() << " TEvCreateTablet " << ev->Get()->Record.ShortDebugString() << Endl;
            NKikimrProto::EReplyStatus status = NKikimrProto::OK;
            const std::pair<ui64, ui64> key(ev->Get()->Record.GetOwner(), ev->Get()->Record.GetOwnerIdx());
            const auto type = ev->Get()->Record.GetTabletType();
            const auto bootMode = ev->Get()->Record.GetTabletBootMode();

            YDB_LOG_CREATE_CONTEXT({"tabletId", TabletID()},
                {"owner", ev->Get()->Record.GetOwner()},
                {"ownerIdx", ev->Get()->Record.GetOwnerIdx()},
                {"type", type});

            auto it = State->Tablets.find(key);
            TActorId bootstrapperActorId;
            if (it == State->Tablets.end()) {
                if (bootMode == NKikimrHive::TABLET_BOOT_MODE_EXTERNAL) {
                    // don't boot anything
                    YDB_LOG_INFO_CTX(ctx, "TEvCreateTablet. External boot mode requested");
                } else if (auto x = GetTabletCreationFunc(type)) {
                    bootstrapperActorId = Boot(ctx, type, x, DataGroupErasure);
                } else if (type == TTabletTypes::DataShard) {
                    bootstrapperActorId = Boot(ctx, type, &CreateDataShard, DataGroupErasure);
                } else if (type == TTabletTypes::KeyValue) {
                    bootstrapperActorId = Boot(ctx, type, &CreateKeyValueFlat, DataGroupErasure);
                } else if (type == TTabletTypes::ColumnShard) {
                    bootstrapperActorId = Boot(ctx, type, &CreateColumnShard, DataGroupErasure);
                } else if (type == TTabletTypes::PersQueue) {
                    bootstrapperActorId = Boot(ctx, type, &CreatePersQueue, DataGroupErasure);
                } else if (type == TTabletTypes::PersQueueReadBalancer) {
                    bootstrapperActorId = Boot(ctx, type, &CreatePersQueueReadBalancer, DataGroupErasure);
                } else if (type == TTabletTypes::Coordinator) {
                    bootstrapperActorId = Boot(ctx, type, &CreateFlatTxCoordinator, DataGroupErasure);
                } else if (type == TTabletTypes::Mediator) {
                    bootstrapperActorId = Boot(ctx, type, &CreateTxMediator, DataGroupErasure);
                } else if (type == TTabletTypes::SchemeShard) {
                    bootstrapperActorId = Boot(ctx, type, &CreateFlatTxSchemeShard, DataGroupErasure);
                } else if (type == TTabletTypes::Kesus) {
                    bootstrapperActorId = Boot(ctx, type, &NKesus::CreateKesusTablet, DataGroupErasure);
                } else if (type == TTabletTypes::Hive) {
                    TFakeHiveState::TPtr state = State->AllocateSubHive();
                    bootstrapperActorId = Boot(ctx, type, [=](const TActorId& tablet, TTabletStorageInfo* info) {
                                                   return new TFakeHive(tablet, info, state, &TFakeHive::DefaultGetTabletCreationFunc);
                                               }, DataGroupErasure);
                } else if (type == TTabletTypes::SysViewProcessor) {
                    bootstrapperActorId = Boot(ctx, type, &NSysView::CreateSysViewProcessor, DataGroupErasure);
                } else if (type == TTabletTypes::SequenceShard) {
                    bootstrapperActorId = Boot(ctx, type, &NSequenceShard::CreateSequenceShard, DataGroupErasure);
                } else if (type == TTabletTypes::ReplicationController) {
                    bootstrapperActorId = Boot(ctx, type, &NReplication::CreateController, DataGroupErasure);
                } else if (type == TTabletTypes::PersQueue) {
                    bootstrapperActorId = Boot(ctx, type, &CreatePersQueue, DataGroupErasure);
                } else if (type == TTabletTypes::StatisticsAggregator) {
                    bootstrapperActorId = Boot(ctx, type, &NStat::CreateStatisticsAggregator, DataGroupErasure);
                } else if (type == TTabletTypes::GraphShard) {
                    bootstrapperActorId = Boot(ctx, type, &NGraph::CreateGraphShard, DataGroupErasure);
                } else if (type == TTabletTypes::WasmCompileController) {
                    bootstrapperActorId = Boot(ctx, type, &NUdfStore::CreateWasmCompileController, DataGroupErasure);
                } else {
                    status = NKikimrProto::ERROR;
                }

                if (status == NKikimrProto::OK) {
                    ui64 tabletId = State->AllocateTabletId();
                    it = State->Tablets.insert(std::make_pair(key, TTabletInfo(type, tabletId, bootstrapperActorId))).first;
                    State->TabletIdToOwner[tabletId] = key;

                    YDB_LOG_INFO_CTX(ctx, "TEvCreateTablet. Boot OK",
                       {"tabletId", tabletId});

                    // After a successful creation of a data shard, need to create
                    // the given number of followers (if requested)
                    //
                    // NOTE: Only the simplest PartitionConfig -> FollowerCount option
                    //       is supported here. More complex options (for example,
                    //       FollowerCountPerDataCenter and FollowerGroups options)
                    //       are completely ignored.
                    if (type == TTabletTypes::DataShard) {
                        const ui32 followerCount = ev->Get()->Record.GetFollowerCount();

                        if (followerCount) {
                            YDB_LOG_INFO_CTX(ctx, "TEvCreateTablet. DataShard created successfully, creating followers",
                                {"tabletId", tabletId},
                                {"followerCount", followerCount});

                            for (ui32 i = 0; i < followerCount; ++i) {
                                const ui32 followerId = i + 1;

                                it->second.FollowerLaunchers[followerId] = ctx.Register(
                                    new TFollowerLauncher(tabletId, followerId)
                                );
                            }
                        }
                    }
                } else {
                    YDB_LOG_ERROR_CTX(ctx, "TEvCreateTablet. Boot failed",
                        {"status", status});
                }
            } else {
                if (it->second.Type != type) {
                    status = NKikimrProto::ERROR;
                }
            }

            if (status == NKikimrProto::OK) {
                auto& boundChannels = ev->Get()->Record.GetBindedChannels();
                it->second.BoundChannels.assign(boundChannels.begin(), boundChannels.end());
                it->second.ChannelsProfile = ev->Get()->Record.GetChannelsProfile();

                it->second.State = ETabletState::ReadyToWork;
                it->second.ObjectDomain = TSubDomainKey(ev->Get()->Record.GetObjectDomain());
            }

            ctx.Send(ev->Sender, new TEvHive::TEvCreateTabletReply(status, key.first,
                key.second, it->second.TabletId, TabletID()), 0, ev->Cookie);
        }

        void TraceAdoptingCases(const std::pair<ui64, ui64> prevKey,
                                const std::pair<ui64, ui64> newKey,
                                const TTabletTypes::EType type,
                                const ui64 tabletID,
                                TString& explain,
                                NKikimrProto::EReplyStatus& status)
        {
            auto it = State->Tablets.find(newKey);
            if (it !=  State->Tablets.end()) {
                if (it->second.TabletId != tabletID) {
                    explain = "there is another tablet associated with the (owner; ownerIdx)";
                    status = NKikimrProto::EReplyStatus::RACE;
                    return;
                }

                if (it->second.Type != type) {
                    explain = "there is the tablet with different type associated with the (owner; ownerIdx)";
                    status = NKikimrProto::EReplyStatus::RACE;
                    return;
                }

                explain = "it seems like the tablet already adopted";
                status = NKikimrProto::EReplyStatus::ALREADY;
                return;
            }

            it = State->Tablets.find(prevKey);
            if (it == State->Tablets.end()) {
                explain = "the tablet isn't found";
                status = NKikimrProto::EReplyStatus::NODATA;
                return;
            }

            if (it->second.TabletId != tabletID) {
                explain = "there is another tablet associated with the (prevOwner; prevOwnerIdx)";
                status = NKikimrProto::EReplyStatus::ERROR;
                return;
            }

            if (it->second.Type != type) { // tablet is the same
                explain = "there is the tablet with different type associated with the (preOwner; prevOwnerIdx)";
                status = NKikimrProto::EReplyStatus::ERROR;
                return;
            }

            State->Tablets.emplace(newKey, it->second);
            State->Tablets.erase(prevKey);
            State->TabletIdToOwner[tabletID] = newKey;

            explain = "we did it";
            status = NKikimrProto::OK;
            return;
        }

        void Handle(TEvHive::TEvAdoptTablet::TPtr& ev, const TActorContext& ctx) {
            YDB_LOG_INFO_CTX(ctx, "TEvAdoptTablet",
                {"tabletId", TabletID()},
                {"msg", ev->Get()->Record});
            const std::pair<ui64, ui64> prevKey(ev->Get()->Record.GetPrevOwner(), ev->Get()->Record.GetPrevOwnerIdx());
            const std::pair<ui64, ui64> newKey(ev->Get()->Record.GetOwner(), ev->Get()->Record.GetOwnerIdx());
            const TTabletTypes::EType type = ev->Get()->Record.GetTabletType();
            const ui64 tabletID = ev->Get()->Record.GetTabletID();

            TString explain;
            NKikimrProto::EReplyStatus status = NKikimrProto::OK;

            TraceAdoptingCases(prevKey, newKey, type, tabletID, explain, status);

            ctx.Send(ev->Sender, new TEvHive::TEvAdoptTabletReply(status, tabletID, newKey.first,
                newKey.second, explain, TabletID()), 0, ev->Cookie);
        }

        void DeleteTablet(const std::pair<ui64, ui64>& id, const TActorContext &ctx) {
            auto it = State->Tablets.find(id);
            if (it == State->Tablets.end()) {
                return;
            }

            TFakeHiveTabletInfo& tabletInfo = it->second;
            ctx.Send(ctx.SelfID, new TEvFakeHive::TEvNotifyTabletDeleted(tabletInfo.TabletId));

            // Destroy all follower actors, if any
            for (const auto& [followerId, launcherActorId] : it->second.FollowerLaunchers) {
                Cerr << "FAKEHIVE " << TabletID()
                    << " Destroying launcher for the followerId " << followerId
                    << " for tabletId " << it->second.TabletId
                    << ": " << launcherActorId
                    << Endl;

                ctx.Send(launcherActorId, new TEvents::TEvPoison());
            }

            // Kill the tablet and don't restart it
            TActorId bootstrapperActorId = tabletInfo.BootstrapperActorId;
            ctx.Send(bootstrapperActorId, new TEvBootstrapper::TEvStandBy());

            for (TActorId waiter : tabletInfo.DeletionWaiters) {
                SendDeletionNotification(it->second.TabletId, waiter, ctx);
            }
            State->TabletIdToOwner.erase(it->second.TabletId);
            State->Tablets.erase(it);
        }

        void Handle(TEvHive::TEvDeleteTablet::TPtr &ev, const TActorContext &ctx) {
            YDB_LOG_INFO_CTX(ctx, "TEvDeleteTablet",
                {"tabletId", TabletID()},
                {"msg", ev->Get()->Record});
            NKikimrHive::TEvDeleteTablet& rec = ev->Get()->Record;
            Cerr << "FAKEHIVE " << TabletID() << " TEvDeleteTablet " << rec.ShortDebugString() << Endl;
            TVector<ui64> deletedIdx;
            for (size_t i = 0; i < rec.ShardLocalIdxSize(); ++i) {
                auto id = std::make_pair<ui64, ui64>(rec.GetShardOwnerId(), rec.GetShardLocalIdx(i));
                deletedIdx.push_back(rec.GetShardLocalIdx(i));
                DeleteTablet(id, ctx);
            }
            ctx.Send(ev->Sender, new TEvHive::TEvDeleteTabletReply(NKikimrProto::OK, TabletID(), rec.GetTxId_Deprecated(), rec.GetShardOwnerId(), deletedIdx));
        }

        void Handle(TEvHive::TEvDeleteOwnerTablets::TPtr &ev, const TActorContext &ctx) {
            YDB_LOG_INFO_CTX(ctx, "TEvDeleteOwnerTablets",
                {"tabletId", TabletID()},
                {"msg", ev->Get()->Record});
            NKikimrHive::TEvDeleteOwnerTablets& rec = ev->Get()->Record;
            Cerr << "FAKEHIVE " << TabletID() << " TEvDeleteOwnerTablets " << rec.ShortDebugString() << Endl;
            auto ownerId = rec.GetOwner();
            TVector<ui64> toDelete;

            if (ownerId == 0) {
                ctx.Send(ev->Sender, new TEvHive::TEvDeleteOwnerTabletsReply(NKikimrProto::ERROR, TabletID(), ownerId, rec.GetTxId()));
                return;
            }

            for (auto& item: State->Tablets) {
                auto& id = item.first;

                if (id.first != ownerId) {
                    continue;
                }

                toDelete.push_back(id.second);
            }

            if (toDelete.empty()) {
                ctx.Send(ev->Sender, new TEvHive::TEvDeleteOwnerTabletsReply(NKikimrProto::ALREADY, TabletID(), ownerId, rec.GetTxId()));
                return;
            }

            for (auto& idx: toDelete) {
                std::pair<ui64, ui64> id(ownerId, idx);
                DeleteTablet(id, ctx);
            }

            ctx.Send(ev->Sender, new TEvHive::TEvDeleteOwnerTabletsReply(NKikimrProto::OK, TabletID(), ownerId, rec.GetTxId()));
        }

        void StopTablet(const ui64& tabletId, const TActorContext &ctx) {
            auto ownerIt = State->TabletIdToOwner.find(tabletId);
            if (ownerIt == State->TabletIdToOwner.end()) {
                return;
            }
            auto it = State->Tablets.find(ownerIt->second);
            if (it == State->Tablets.end()) {
                return;
            }

            TFakeHiveTabletInfo& tabletInfo = it->second;

            // Very similar to DeleteTablet but don't actually removes tablet
            // Kill the tablet and don't restart it
            TActorId bootstrapperActorId = tabletInfo.BootstrapperActorId;
            ctx.Send(bootstrapperActorId, new TEvBootstrapper::TEvStandBy());

            tabletInfo.State = ETabletState::Stopped;
        }

        void Handle(TEvHive::TEvStopTablet::TPtr &ev, const TActorContext &ctx) {
            YDB_LOG_INFO_CTX(ctx, "TEvStopTablet",
                {"tabletId", TabletID()},
                {"msg", ev->Get()->Record});
            NKikimrHive::TEvStopTablet& rec = ev->Get()->Record;
            Cerr << "FAKEHIVE " << TabletID() << " TEvStopTablet " << rec.ShortDebugString() << Endl;
            StopTablet(rec.GetTabletID(), ctx);
            ctx.Send(ev->Sender, new TEvHive::TEvStopTabletResult(NKikimrProto::OK, rec.GetTabletID()));
        }

        void Handle(TEvHive::TEvRequestHiveInfo::TPtr &ev, const TActorContext &ctx) {
            YDB_LOG_INFO_CTX(ctx, "TEvRequestHiveInfo",
                {"tabletId", TabletID()},
                {"msg", ev->Get()->Record});
            const auto& record = ev->Get()->Record;
            TAutoPtr<TEvHive::TEvResponseHiveInfo> response = new TEvHive::TEvResponseHiveInfo();

            if (record.HasTabletID()) {
                auto it = State->TabletIdToOwner.find(record.GetTabletID());
                FillTabletInfo(response->Record, record.GetTabletID(), it == State->TabletIdToOwner.end() ? nullptr : State->Tablets.FindPtr(it->second));
            } else {
                response->Record.MutableTablets()->Reserve(State->Tablets.size());
                for (auto it = State->Tablets.begin(); it != State->Tablets.end(); ++it) {
                    if (record.HasTabletType() && record.GetTabletType() != it->second.Type)
                        continue;
                    FillTabletInfo(response->Record, it->second.TabletId, &it->second);
                }
            }

            ctx.Send(ev->Sender, response.Release());
        }

        void Handle(TEvHive::TEvInitiateTabletExternalBoot::TPtr &ev, const TActorContext &ctx) {
            YDB_LOG_INFO_CTX(ctx, "TEvInitiateTabletExternalBoot",
                {"tabletId", TabletID()},
                {"msg", ev->Get()->Record});

            ui64 tabletId = ev->Get()->Record.GetTabletID();
            if (!State->TabletIdToOwner.contains(tabletId)) {
                ctx.Send(ev->Sender, new TEvHive::TEvBootTabletReply(NKikimrProto::EReplyStatus::ERROR), 0, ev->Cookie);
                return;
            }

            auto key = State->TabletIdToOwner[tabletId];
            auto it = State->Tablets.find(key);
            Y_ABORT_UNLESS(it != State->Tablets.end());

            THolder<TTabletStorageInfo> tabletInfo(CreateTestTabletInfo(tabletId, it->second.Type));
            ctx.Send(ev->Sender, new TEvLocal::TEvBootTablet(*tabletInfo.Get(), 0), 0, ev->Cookie);
        }

        void Handle(TEvHive::TEvUpdateTabletsObject::TPtr &ev, const TActorContext &ctx) {
            YDB_LOG_INFO_CTX(ctx, "TEvUpdateTabletsObject",
                {"tabletId", TabletID()},
                {"msg", ev->Get()->Record});

            // Fake Hive does not care about objects, do nothing


            auto response = std::make_unique<TEvHive::TEvUpdateTabletsObjectReply>(NKikimrProto::OK);
            response->Record.SetTxId(ev->Get()->Record.GetTxId());
            response->Record.SetTxPartId(ev->Get()->Record.GetTxPartId());
            ctx.Send(ev->Sender, response.release(), 0, ev->Cookie);
        }

        void Handle(TEvHive::TEvUpdateDomain::TPtr &ev, const TActorContext &ctx) {
            YDB_LOG_INFO_CTX(ctx, "TEvUpdateDomain",
                {"tabletId", TabletID()},
                {"msg", ev->Get()->Record});

            const TSubDomainKey subdomainKey(ev->Get()->Record.GetDomainKey());
            NHive::TDomainInfo& domainInfo = State->Domains[subdomainKey];
            if (ev->Get()->Record.HasServerlessComputeResourcesMode()) {
                domainInfo.ServerlessComputeResourcesMode = ev->Get()->Record.GetServerlessComputeResourcesMode();
            } else {
                domainInfo.ServerlessComputeResourcesMode.Clear();
            }

            auto response = std::make_unique<TEvHive::TEvUpdateDomainReply>();
            response->Record.SetTxId(ev->Get()->Record.GetTxId());
            response->Record.SetOrigin(TabletID());
            ctx.Send(ev->Sender, response.release(), 0, ev->Cookie);
        }

        void Handle(TEvFakeHive::TEvRequestDomainInfo::TPtr &ev, const TActorContext &ctx) {
            YDB_LOG_INFO_CTX(ctx, "TEvRequestDomainInfo",
                {"tabletId", TabletID()},
                {"domainKey", ev->Get()->DomainKey});
            auto response = std::make_unique<TEvFakeHive::TEvRequestDomainInfoReply>(State->Domains[ev->Get()->DomainKey]);
            ctx.Send(ev->Sender, response.release());
        }

        void Handle(TEvFakeHive::TEvSubscribeToTabletDeletion::TPtr &ev, const TActorContext &ctx) {
            YDB_LOG_INFO_CTX(ctx, "TEvSubscribeToTabletDeletion",
                {"tabletId", TabletID()},
                {"eventTabletId", ev->Get()->TabletId});

            ui64 tabletId = ev->Get()->TabletId;
            auto it = State->TabletIdToOwner.find(tabletId);
            if (it == State->TabletIdToOwner.end()) {
                SendDeletionNotification(tabletId, ev->Sender, ctx);
            } else {
                State->Tablets.FindPtr(it->second)->DeletionWaiters.insert(ev->Sender);
            }
        }

        void SendDeletionNotification(ui64 tabletId, TActorId waiter, const TActorContext& ctx) {
            TAutoPtr<TEvHive::TEvResponseHiveInfo> response = new TEvHive::TEvResponseHiveInfo();
            FillTabletInfo(response->Record, tabletId, nullptr);
            ctx.Send(waiter, response.Release());
        }

        void Handle(TEvents::TEvPoisonPill::TPtr &ev, const TActorContext &ctx) {
            YDB_LOG_INFO_CTX(ctx, "TEvPoisonPill",
                {"tabletId", TabletID()});
            Y_UNUSED(ev);
            Become(&TThis::BrokenState);
            ctx.Send(Tablet(), new TEvents::TEvPoisonPill);
        }

    private:
        TActorId Boot(const TActorContext& ctx, TTabletTypes::EType tabletType, std::function<IActor* (const TActorId &, TTabletStorageInfo *)> op,
            TBlobStorageGroupType::EErasureSpecies erasure) {
            TIntrusivePtr<TBootstrapperInfo> bi(new TBootstrapperInfo(new TTabletSetupInfo(op, TMailboxType::Simple, 0,
                TMailboxType::Simple, 0)));
            return ctx.Register(CreateBootstrapper(
                CreateTestTabletInfo(State->NextTabletId, tabletType, erasure), bi.Get()));
        }

        void FillTabletInfo(NKikimrHive::TEvResponseHiveInfo& response, ui64 tabletId, const TFakeHiveTabletInfo *info) {
            auto& tabletInfo = *response.AddTablets();
            tabletInfo.SetTabletID(tabletId);
            if (info) {
                tabletInfo.SetTabletType(info->Type);
                tabletInfo.SetState(ui32(info->State)); // THive::ETabletState::*
                tabletInfo.MutableObjectDomain()->CopyFrom(info->ObjectDomain);

                // TODO: fill other fields when needed
            }
        }

    private:
        TState::TPtr State;
        TGetTabletCreationFunc GetTabletCreationFunc;
        TSubDomainKey PrimarySubDomainKey;
    };

    void BootFakeHive(TTestActorRuntime& runtime, ui64 tabletId, TFakeHiveState::TPtr state,
                      TGetTabletCreationFunc getTabletCreationFunc)
    {
        CreateTestBootstrapper(runtime, CreateTestTabletInfo(tabletId, TTabletTypes::Hive), [=](const TActorId & tablet, TTabletStorageInfo* info) {
            return new TFakeHive(tablet, info, state,
                                 (getTabletCreationFunc == nullptr) ? &TFakeHive::DefaultGetTabletCreationFunc : getTabletCreationFunc);
        });

        {
            TDispatchOptions options;
            options.FinalEvents.push_back(TDispatchOptions::TFinalEventCondition(TEvTablet::EvBoot, 1));
            runtime.DispatchEvents(options);
        }
    }

}
