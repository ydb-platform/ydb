#include <ydb/core/blobstorage/ut_blobstorage/lib/env.h>
#include <ydb/core/blobstorage/pdisk/blobstorage_pdisk_tools.h>
#include <ydb/library/pdisk_io/aio.h>

namespace {

    struct TSlowPDiskState {
        ui32 Starts = 0;
        ui32 CompletedStarts = 0;
        ui32 Stops = 0;
        ui32 Reads = 0;
        std::map<std::pair<ui32, TString>, TRcBuf> Metadata;
    };

    // Model a PDisk that cannot service metadata until its startup scan finishes.
    // Destroying this actor loses its pending startup, just as stopping the worker does.
    class TSlowMetadataPDisk : public TActorBootstrapped<TSlowMetadataPDisk> {
        TSlowPDiskState& State;
        const std::optional<ui64> Guid;
        TRcBuf& Metadata;
        const TActorId Backend;
        const bool Slow;
        bool Ready = false;
        std::vector<std::unique_ptr<IEventHandle>> Pending;

        void Reply(IEventHandle& ev) {
            if (ev.GetTypeRewrite() == NPDisk::TEvReadMetadata::EventType) {
                ++State.Reads;
                Send(ev.Sender, new NPDisk::TEvReadMetadataResult(TRcBuf(Metadata), Guid), 0, ev.Cookie);
            } else {
                Metadata = std::move(ev.Get<NPDisk::TEvWriteMetadata>()->Metadata);
                Send(ev.Sender, new NPDisk::TEvWriteMetadataResult(NPDisk::EPDiskMetadataOutcome::OK, Guid),
                    0, ev.Cookie);
            }
        }

        void HandleMetadata(TAutoPtr<IEventHandle>& ev) {
            if (Ready) {
                Reply(*ev);
            } else {
                Pending.emplace_back(ev.Release());
            }
        }

        void FinishStartup() {
            Ready = true;
            ++State.CompletedStarts;
            for (const auto& ev : Pending) {
                Reply(*ev);
            }
            Pending.clear();
        }

        void Stop(TAutoPtr<IEventHandle>& ev) {
            if (Slow) {
                ++State.Stops;
            }
            if (Backend) {
                Forward(ev, Backend);
            } else if (ev->Cookie) {
                Send(ev->Sender, new TEvents::TEvPoisonTaken, 0, ev->Cookie);
            }
            PassAway();
        }

    public:
        TSlowMetadataPDisk(TSlowPDiskState& state, TRcBuf& metadata, std::optional<ui64> guid,
                TActorId backend, bool slow)
            : State(state)
            , Guid(guid)
            , Metadata(metadata)
            , Backend(backend)
            , Slow(slow)
        {}

        void Bootstrap() {
            Become(&TThis::StateFunc);
            if (Slow) {
                ++State.Starts;
                Schedule(TDuration::Seconds(90), new TEvents::TEvWakeup);
            } else {
                Ready = true;
            }
        }

        STFUNC(StateFunc) {
            switch (ev->GetTypeRewrite()) {
                fFunc(NPDisk::TEvReadMetadata::EventType, HandleMetadata);
                fFunc(NPDisk::TEvWriteMetadata::EventType, HandleMetadata);
                cFunc(TEvents::TSystem::Wakeup, FinishStartup);
                fFunc(TEvents::TSystem::Poison, Stop);
                default:
                    UNIT_ASSERT(Backend);
                    Forward(ev, Backend);
            }
        }
    };

    class TSlowPDiskSubsystem : public IPDiskSubsystem {
        TSlowPDiskState& State;
        std::unique_ptr<ISubSystem> Delegate;

    public:
        explicit TSlowPDiskSubsystem(TSlowPDiskState& state, std::unique_ptr<ISubSystem> delegate = {})
            : State(state)
            , Delegate(std::move(delegate))
        {}

        void Start(const TActorContext& ctx, ui32 pdiskId, const TIntrusivePtr<TPDiskConfig>& config,
                const NPDisk::TMainKey& mainKey, ui32 poolId, ui32 nodeId) override {
            TActorId backend;
            if (Delegate) {
                static_cast<IPDiskSubsystem&>(*Delegate).Start(ctx, pdiskId, config, mainKey, poolId, nodeId);
                backend = ctx.ActorSystem()->LookupLocalService(MakeBlobStoragePDiskID(nodeId, pdiskId));
            }
            const auto guid = Delegate ? (config->PDiskGuid ? std::make_optional(config->PDiskGuid) : std::nullopt)
                : std::optional<ui64>(42);
            const auto actor = ctx.Register(new TSlowMetadataPDisk(State, State.Metadata[{nodeId, config->Path}],
                guid, backend, config->MetadataOnly), TMailboxType::HTSwap, poolId);
            ctx.ActorSystem()->RegisterLocalService(MakeBlobStoragePDiskID(nodeId, pdiskId), actor);
        }
    };

    template<bool Write>
    void CheckSlowMetadataStartup() {
        TSlowPDiskState state;
        TEnvironmentSetup env({.NodeCount = 1, .PrepareRuntime = [&](TTestActorSystem& runtime) {
            runtime.SetupNodeSubSystems = [&](ui32, TActorSystemSetup* setup) {
                setup->RegisterSubSystem<IPDiskSubsystem>(std::make_unique<TSlowPDiskSubsystem>(state));
            };
        }});
        auto& runtime = env.Runtime;
        const ui32 nodeId = *runtime->GetNodes().begin();
        const auto edge = runtime->AllocateEdgeActor(nodeId);
        const auto warden = MakeBlobStorageNodeWardenID(nodeId);
        const TString path = "/mock/slow-starting-pdisk";
        constexpr ui64 cookie = 817;
        using TResult = std::conditional_t<Write, NStorage::TEvNodeWardenWriteMetadataResult,
            NStorage::TEvNodeWardenReadMetadataResult>;
        ui32 replies = 0;
        bool deliverReplies = false;
        runtime->FilterFunction = [&](ui32, std::unique_ptr<IEventHandle>& ev) {
            if (ev->Recipient == edge && ev->GetTypeRewrite() == TResult::EventType) {
                ++replies;
                return deliverReplies;
            }
            return true;
        };
        if constexpr (Write) {
            runtime->Send(new IEventHandle(warden, edge,
                new NStorage::TEvNodeWardenWriteMetadata(path, {}), 0, cookie), nodeId);
        } else {
            runtime->Send(new IEventHandle(warden, edge,
                new NStorage::TEvNodeWardenReadMetadata(path), 0, cookie), nodeId);
        }
        env.Sim(TDuration::Seconds(65));
        UNIT_ASSERT_VALUES_EQUAL(state.Starts, 1);
        UNIT_ASSERT_VALUES_EQUAL(state.CompletedStarts, 0);
        UNIT_ASSERT_VALUES_EQUAL_C(state.Stops, 0, "Metadata timeout destroyed a PDisk during startup");
        UNIT_ASSERT_VALUES_EQUAL(replies, 0);
        deliverReplies = true;
        auto response = env.WaitForEdgeActorEvent<TResult>(edge, false,
            runtime->GetClock() + TDuration::Seconds(30));
        UNIT_ASSERT_C(response, "The original startup did not finish");
        UNIT_ASSERT_VALUES_EQUAL(response->Cookie, cookie);
        UNIT_ASSERT(response->Get()->Outcome == (Write ? NPDisk::EPDiskMetadataOutcome::OK
            : NPDisk::EPDiskMetadataOutcome::NO_METADATA));
        UNIT_ASSERT(response->Get()->Guid == 42);
        UNIT_ASSERT_VALUES_EQUAL(state.Starts, 1);
        UNIT_ASSERT_VALUES_EQUAL(state.CompletedStarts, 1);
        env.Sim(TDuration::Seconds(1));
        UNIT_ASSERT_VALUES_EQUAL(replies, 1);
        UNIT_ASSERT_VALUES_EQUAL(state.Stops, 0);
        env.Sim(TDuration::Seconds(60));
        UNIT_ASSERT_VALUES_EQUAL(state.Stops, 1);
        runtime->FilterFunction = nullptr;
    }

    void CheckSlowBootstrap(bool bridge) {
        const ui32 nodeCount = bridge ? 2 : 1;
        TSlowPDiskState state;
        TEnvironmentSetup env({
            .NodeCount = nodeCount,
            .ConfigPreprocessor = [=](ui32, TNodeWardenConfig& config) {
                auto& sm = *config.SelfManagementConfig;
                sm.SetErasureSpecies("none");
                sm.SetPDiskType(NKikimrBlobStorage::EPDiskType::NVME);
                auto* hostConfig = config.BlobStorageConfig->AddDefineHostConfig();
                hostConfig->SetHostConfigId(1);
                auto* drive = hostConfig->AddDrive();
                drive->SetPath("/mock/slow-bootstrap-pdisk");
                drive->SetType(NKikimrBlobStorage::EPDiskType::NVME);
                for (ui32 nodeId = 1; nodeId <= nodeCount; ++nodeId) {
                    auto* node = config.NameserviceConfig->AddNode();
                    node->SetNodeId(nodeId);
                    node->SetHost(TStringBuilder() << "host_" << nodeId);
                    node->SetInterconnectHost(node->GetHost());
                    node->SetPort(19001);
                    auto* location = node->MutableLocation();
                    location->SetDataCenter("dc");
                    location->SetRack(TStringBuilder() << "rack_" << nodeId);
                    if (bridge) {
                        location->SetBridgePileName(TStringBuilder() << "pile_" << nodeId);
                    }
                    auto* host = config.BlobStorageConfig->MutableDefineBox()->AddHost();
                    host->SetEnforcedNodeId(nodeId);
                    host->SetHostConfigId(1);
                }
                config.StartupConfigYaml = "metadata: {kind: MainConfig, version: 0, cluster: ''}\nconfig: {}\n";
            },
            .PrepareRuntime = [&](TTestActorSystem& runtime) {
                auto configure = std::move(runtime.SetupNodeSubSystems);
                runtime.SetupNodeSubSystems = [&, configure = std::move(configure)](ui32 nodeId, TActorSystemSetup* setup) {
                    configure(nodeId, setup);
                    auto& subsystem = setup->SubSystems[TSubSystemRegistry::TItem<IPDiskSubsystem>::Index()];
                    setup->RegisterSubSystem<IPDiskSubsystem>(std::make_unique<TSlowPDiskSubsystem>(state,
                        std::move(subsystem)));
                };
            },
            .LocationGenerator = [=](ui32 nodeId) {
                NActorsInterconnect::TNodeLocation location;
                location.SetDataCenter("dc");
                location.SetRack(TStringBuilder() << "rack_" << nodeId);
                if (bridge) {
                    location.SetBridgePileName(TStringBuilder() << "pile_" << nodeId);
                }
                return TNodeLocation(location);
            },
            .SelfManagementConfig = true,
            .NumPiles = bridge ? 2u : 0u,
        });
        auto& runtime = env.Runtime;
        env.Sim(TDuration::Seconds(151));
        UNIT_ASSERT(state.Stops >= nodeCount);
        const ui32 initialReads = state.Reads;
        const auto edge = runtime->AllocateEdgeActor(1);
        const auto deadline = runtime->GetClock() + TDuration::Minutes(4);
        bool bootstrapped = false;
        ui32 failures = 0;
        while (!bootstrapped && runtime->GetClock() < deadline) {
            auto request = std::make_unique<NStorage::TEvNodeConfigInvokeOnRoot>();
            request->Record.MutableBootstrapCluster()->SetSelfAssemblyUUID("slow-pdisk-bootstrap");
            runtime->Send(new IEventHandle(MakeBlobStorageNodeWardenID(1), edge, request.release()), 1);
            auto response = env.WaitForEdgeActorEvent<NStorage::TEvNodeConfigInvokeOnRootResult>(edge, false, deadline);
            UNIT_ASSERT_C(response, "Bootstrap did not finish after the PDisk startup completed");
            bootstrapped = response->Get()->Record.GetStatus() == NKikimrBlobStorage::TEvNodeConfigInvokeOnRootResult::OK;
            if (!bootstrapped) {
                ++failures;
                env.Sim(TDuration::Seconds(1));
            }
        }
        UNIT_ASSERT_C(bootstrapped, "Every collection restarted the slow temporary PDisk");
        UNIT_ASSERT(failures);
        UNIT_ASSERT_VALUES_EQUAL(state.Starts, 2 * nodeCount);
        UNIT_ASSERT_VALUES_EQUAL(state.CompletedStarts, 2 * nodeCount);
        UNIT_ASSERT_C(state.Reads > initialReads + nodeCount, "Bootstrap did not perform a fresh read after its reader expired");
        runtime->Send(new IEventHandle(MakeBlobStorageNodeWardenID(1), edge,
            new TEvNodeWardenQueryStorageConfig(false)), 1);
        const auto config = env.WaitForEdgeActorEvent<TEvNodeWardenStorageConfig>(edge, false, deadline);
        UNIT_ASSERT(config);
        UNIT_ASSERT_VALUES_EQUAL(config->Get()->Config->GetGeneration(), 1);
        UNIT_ASSERT_VALUES_EQUAL(config->Get()->Config->GetSelfAssemblyUUID(), "slow-pdisk-bootstrap");
        for (const auto delay : {TDuration::Seconds(1), TDuration::Seconds(60)}) {
            env.Sim(delay);
            UNIT_ASSERT_VALUES_EQUAL(state.Stops, 2 * nodeCount);
            runtime->Send(new IEventHandle(MakeBlobStorageNodeWardenID(1), edge,
                new NStorage::TEvNodeWardenReadMetadata("/mock/slow-bootstrap-pdisk")), 1);
            const auto read = env.WaitForEdgeActorEvent<NStorage::TEvNodeWardenReadMetadataResult>(edge, false,
                runtime->GetClock() + TDuration::Seconds(1));
            UNIT_ASSERT_C(read, "The permanent PDisk did not remain available");
            UNIT_ASSERT(read->Get()->Guid);
            UNIT_ASSERT(read->Get()->Outcome == NPDisk::EPDiskMetadataOutcome::OK);
            UNIT_ASSERT_VALUES_EQUAL(read->Get()->Record.GetCommittedStorageConfig().GetGeneration(), 1);
            UNIT_ASSERT_VALUES_EQUAL(state.Starts, 2 * nodeCount);
        }
    }

    class TObservedRealPDiskSubsystem : public IPDiskSubsystem {
        std::unique_ptr<IPDiskSubsystem> Delegate = CreatePDiskSubsystem();
        const std::function<void(bool, TActorId)> OnStart;

    public:
        explicit TObservedRealPDiskSubsystem(std::function<void(bool, TActorId)> onStart)
            : OnStart(std::move(onStart))
        {}

        void Start(const TActorContext& ctx, ui32 pdiskId, const TIntrusivePtr<TPDiskConfig>& config,
                const NPDisk::TMainKey& mainKey, ui32 poolId, ui32 nodeId) override {
            Delegate->Start(ctx, pdiskId, config, mainKey, poolId, nodeId);
            OnStart(config->MetadataOnly,
                ctx.ActorSystem()->LookupLocalService(MakeBlobStoragePDiskID(nodeId, pdiskId)));
        }
    };

    struct TTemporaryPDiskPromotionFixture {
        static constexpr ui32 NodeId = 1;
        static constexpr ui32 PDiskId = 1;
        static constexpr ui64 Guid = 12345;
        const TString Path = "SectorMap:temporary-pdisk-promotion:32";
        std::shared_ptr<NPDisk::IIoContextFactory> IoFactory = std::make_shared<NPDisk::TIoContextFactoryOSS>();
        TIntrusivePtr<NPDisk::TSectorMap> SectorMap = MakeIntrusive<NPDisk::TSectorMap>(32ull << 30);
        TTestActorSystem Runtime{1, NLog::PRI_ERROR, MakeIntrusive<TDomainsInfo>()};
        TActorId Warden;
        TActorId Requester;
        TActorId TemporaryPDisk;
        TActorId PermanentPDisk;
        ui32 PermanentStarts = 0;

        TTemporaryPDiskPromotionFixture() {
            TFormatOptions options;
            options.SectorMap = SectorMap;
            FormatPDisk(Path, 0, 4096, 32u << 20, Guid, 1, 2, 3,
                NPDisk::YdbDefaultPDiskSequence, "", options);

            Runtime.SetupNodeSubSystems = [this](ui32, TActorSystemSetup* setup) {
                setup->RegisterSubSystem<IPDiskSubsystem>(std::make_unique<TObservedRealPDiskSubsystem>(
                    [this](bool temporary, TActorId actor) {
                        if (temporary) {
                            UNIT_ASSERT(!TemporaryPDisk);
                            TemporaryPDisk = actor;
                        } else {
                            ++PermanentStarts;
                            PermanentPDisk = actor;
                        }
                    }));
            };
            Runtime.Start();
            auto& appData = *Runtime.GetNode(NodeId)->AppData;
            appData.IoContextFactory = IoFactory.get();
            appData.DomainsInfo->AddDomain(TDomainsInfo::TDomain::ConstructEmptyDomain("dom", 1).Release());
            appData.DynamicNameserviceConfig = new TDynamicNameserviceConfig;

            auto config = MakeIntrusive<TNodeWardenConfig>();
            config->PDiskKey = {.Keys = {NPDisk::YdbDefaultPDiskSequence}, .IsInitialized = true};
            config->FeatureFlags->SetEnableSmallDiskOptimization(true);
            config->SectorMaps[Path] = SectorMap;
            Warden = Runtime.Register(CreateBSNodeWarden(config), NodeId);
            Runtime.RegisterService(MakeBlobStorageNodeWardenID(NodeId), Warden);
            Requester = Runtime.AllocateEdgeActor(NodeId);
            Barrier();
        }

        ~TTemporaryPDiskPromotionFixture() {
            Runtime.FilterFunction = {};
            Runtime.Stop();
        }

        void Barrier() {
            const auto edge = Runtime.AllocateEdgeActor(NodeId);
            Runtime.Send(new IEventHandle(Warden, edge, new TEvNodeWardenQueryStorageConfig(false)), NodeId);
            Runtime.WaitForEdgeActorEvent<TEvNodeWardenStorageConfig>(edge);
        }

        void Promote() {
            auto update = std::make_unique<TEvBlobStorage::TEvControllerNodeServiceSetUpdate>(NKikimrProto::OK, NodeId);
            auto* pdisk = update->Record.MutableServiceSet()->AddPDisks();
            pdisk->SetNodeID(NodeId);
            pdisk->SetPDiskID(PDiskId);
            pdisk->SetPath(Path);
            pdisk->SetPDiskGuid(Guid);
            pdisk->SetPDiskCategory(TPDiskCategory(NPDisk::DEVICE_TYPE_ROT, 0).GetRaw());
            pdisk->SetEntityStatus(NKikimrBlobStorage::CREATE);
            Runtime.Send(new IEventHandle(Warden, {}, update.release()), NodeId);
            Barrier();
        }

        void CheckPermanentMetadata() {
            Runtime.Sim([&] { return !PermanentPDisk; });
            UNIT_ASSERT_VALUES_EQUAL(PermanentStarts, 1);
            NKikimrBlobStorage::TPDiskMetadataRecord record;
            record.MutableCommittedStorageConfig()->SetGeneration(42);
            Runtime.Send(new IEventHandle(Warden, Requester,
                new NStorage::TEvNodeWardenWriteMetadata(Path, record), 0, 73), NodeId);
            auto written = Runtime.WaitForEdgeActorEvent<NStorage::TEvNodeWardenWriteMetadataResult>(Requester, false);
            UNIT_ASSERT(written->Get()->Outcome == NPDisk::EPDiskMetadataOutcome::OK);
            UNIT_ASSERT(written->Get()->Guid == Guid);
            UNIT_ASSERT_VALUES_EQUAL(written->Cookie, 73);
            Runtime.Send(new IEventHandle(Warden, Requester,
                new NStorage::TEvNodeWardenReadMetadata(Path), 0, 74), NodeId);
            auto read = Runtime.WaitForEdgeActorEvent<NStorage::TEvNodeWardenReadMetadataResult>(Requester, false);
            UNIT_ASSERT(read->Get()->Outcome == NPDisk::EPDiskMetadataOutcome::OK);
            UNIT_ASSERT(read->Get()->Guid == Guid);
            UNIT_ASSERT_VALUES_EQUAL(read->Get()->Record.SerializeAsString(), record.SerializeAsString());
            UNIT_ASSERT_VALUES_EQUAL(read->Cookie, 74);
            UNIT_ASSERT_VALUES_EQUAL(PermanentStarts, 1);
        }
    };

    void CheckTemporaryPDiskPromotion(bool promoteBeforeCompletion, bool failStartup) {
        TTemporaryPDiskPromotionFixture fixture;
        auto& runtime = fixture.Runtime;
        std::unique_ptr<IEventHandle> heldRead;
        std::unique_ptr<IEventHandle> heldPoison;
        std::unique_ptr<IEventHandle> readResult;
        TActorId metadataRequester;
        bool holdRead = promoteBeforeCompletion;
        bool holdPoison = true;
        runtime.FilterFunction = [&](ui32, std::unique_ptr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == NPDisk::TEvReadMetadata::EventType
                    && ev->GetRecipientRewrite() == fixture.TemporaryPDisk) {
                metadataRequester = ev->Sender;
            }
            if (holdRead && ev->GetTypeRewrite() == NPDisk::TEvReadMetadataResult::EventType
                    && ev->GetRecipientRewrite() == metadataRequester) {
                UNIT_ASSERT(!heldRead);
                heldRead = std::move(ev);
                return false;
            }
            // Barriers and controlled Sim calls can run while this reply arrives.
            // Capture it here instead of delivering it to an unarmed edge actor.
            if (ev->GetTypeRewrite() == NStorage::TEvNodeWardenReadMetadataResult::EventType
                    && ev->GetRecipientRewrite() == fixture.Requester && ev->Cookie == 71) {
                UNIT_ASSERT(!readResult);
                readResult = std::move(ev);
                return false;
            }
            if (holdPoison && ev->GetTypeRewrite() == TEvents::TSystem::Poison
                    && (ev->GetRecipientRewrite() == fixture.TemporaryPDisk
                        || ev->GetRecipientRewrite() == MakeBlobStoragePDiskID(fixture.NodeId, Max<ui32>()))) {
                UNIT_ASSERT(!heldPoison);
                heldPoison = std::move(ev);
                return false;
            }
            return true;
        };

        // A lock held by the test forces the real temporary PDisk into StateError.
        if (failStartup) {
            UNIT_ASSERT(fixture.SectorMap->Lock());
        }
        runtime.Send(new IEventHandle(fixture.Warden, fixture.Requester,
            new NStorage::TEvNodeWardenReadMetadata(fixture.Path), 0, 71), fixture.NodeId);
        if (promoteBeforeCompletion) {
            runtime.Sim([&] { return !heldRead; });
            fixture.Promote();
            UNIT_ASSERT(!heldPoison);
            UNIT_ASSERT_VALUES_EQUAL(fixture.PermanentStarts, 0);
            holdRead = false;
            runtime.Send(heldRead.release(), fixture.NodeId);
        }
        if (!promoteBeforeCompletion && !failStartup) {
            // An idle successful temporary disk returns its read before promotion.
            runtime.Sim([&] { return !readResult; });
            fixture.Promote();
        }
        runtime.Sim([&] { return !heldPoison; });
        if (failStartup) {
            // The failed PDisk never acquired this lock. Only the test may release it.
            fixture.SectorMap->Unlock();
            fixture.Promote();
        }
        UNIT_ASSERT_C(heldPoison->Cookie, "Temporary PDisk shutdown must request an acknowledgment");
        UNIT_ASSERT_VALUES_EQUAL_C(fixture.PermanentStarts, 0,
            "The permanent PDisk started before the temporary actor released the device");
        UNIT_ASSERT(!fixture.PermanentPDisk);
        if (!failStartup) {
            UNIT_ASSERT_C(!fixture.SectorMap->Lock(), "The held temporary PDisk must still own the device lock");
        }
        if (promoteBeforeCompletion || failStartup) {
            UNIT_ASSERT_C(!readResult, "The final metadata reply must wait for temporary PDisk shutdown");
        }

        const auto closingRequester = runtime.AllocateEdgeActor(fixture.NodeId);
        runtime.Send(new IEventHandle(fixture.Warden, closingRequester,
            new NStorage::TEvNodeWardenReadMetadata(fixture.Path), 0, 72), fixture.NodeId);
        const auto closingRead = runtime.WaitForEdgeActorEvent<NStorage::TEvNodeWardenReadMetadataResult>(closingRequester);
        UNIT_ASSERT(closingRead->Get()->Outcome == NPDisk::EPDiskMetadataOutcome::ERROR);
        UNIT_ASSERT_VALUES_EQUAL(closingRead->Cookie, 72);
        UNIT_ASSERT_VALUES_EQUAL(fixture.PermanentStarts, 0);

        holdPoison = false;
        runtime.Send(heldPoison.release(), fixture.NodeId);
        runtime.Sim([&] { return !readResult; });
        UNIT_ASSERT_VALUES_EQUAL(readResult->Cookie, 71);
        UNIT_ASSERT(readResult->Get<NStorage::TEvNodeWardenReadMetadataResult>()->Outcome
            == (failStartup ? NPDisk::EPDiskMetadataOutcome::ERROR
            : NPDisk::EPDiskMetadataOutcome::NO_METADATA));
        fixture.CheckPermanentMetadata();
        runtime.FilterFunction = {};
    }

} // namespace

Y_UNIT_TEST_SUITE(PDiskSubsystem) {

    Y_UNIT_TEST(SharedMockMetadataPreservesSnapshotIsolation) {
        TEnvironmentSetup env({.NodeCount = 1});
        auto& runtime = env.Runtime;
        const auto edge = runtime->AllocateEdgeActor(1);
        auto metadata = std::make_shared<TPDiskMockState::TMetadata>();
        auto original = MakeIntrusive<TPDiskMockState>(1u, 1u, 12345ull, 32ull << 30, 128u << 20,
            false, NPDisk::DEVICE_TYPE_NVME, TPDiskMockState::ESpaceColorPolicy::None, metadata);
        auto promoted = MakeIntrusive<TPDiskMockState>(1u, 2u, 67890ull, 32ull << 30, 128u << 20,
            false, NPDisk::DEVICE_TYPE_NVME, TPDiskMockState::ESpaceColorPolicy::None, metadata);
        const auto first = runtime->Register(CreatePDiskMockActor(original), 1);
        const auto second = runtime->Register(CreatePDiskMockActor(promoted), 1);
        auto write = [&](TActorId actor, TString data) {
            runtime->Send(new IEventHandle(actor, edge,
                new NPDisk::TEvWriteMetadata(TRcBuf(std::move(data)))), 1);
            const auto result = env.WaitForEdgeActorEvent<NPDisk::TEvWriteMetadataResult>(edge, false);
            UNIT_ASSERT(result->Get()->Outcome == NPDisk::EPDiskMetadataOutcome::OK);
        };
        auto check = [&](TActorId actor, ui64 guid, const TString& expected) {
            runtime->Send(new IEventHandle(actor, edge, new NPDisk::TEvReadMetadata), 1);
            auto result = env.WaitForEdgeActorEvent<NPDisk::TEvReadMetadataResult>(edge, false);
            UNIT_ASSERT(result->Get()->Outcome == NPDisk::EPDiskMetadataOutcome::OK);
            UNIT_ASSERT(result->Get()->PDiskGuid == guid);
            UNIT_ASSERT_VALUES_EQUAL(TRope(std::move(result->Get()->Metadata)).ConvertToString(), expected);
        };
        write(first, "before promotion");
        const auto snapshot = runtime->Register(CreatePDiskMockActor(original->Snapshot()), 1);
        write(second, "after promotion");
        check(first, 12345, "after promotion");
        check(second, 67890, "after promotion");
        check(snapshot, 12345, "before promotion");
        write(snapshot, "snapshot only");
        check(first, 12345, "after promotion");
        check(second, 67890, "after promotion");
        check(snapshot, 12345, "snapshot only");
    }

    Y_UNIT_TEST(TemporaryPDiskPromotionWaitsForDeviceShutdown) {
        for (const bool promoteBeforeCompletion : {false, true}) {
            CheckTemporaryPDiskPromotion(promoteBeforeCompletion, false);
        }
        CheckTemporaryPDiskPromotion(false, true);
    }

    Y_UNIT_TEST(ReadMetadataPreservesSlowPDiskStartup) {
        CheckSlowMetadataStartup<false>();
    }

    Y_UNIT_TEST(WriteMetadataPreservesSlowPDiskStartup) {
        CheckSlowMetadataStartup<true>();
    }

    Y_UNIT_TEST(BootstrapPreservesSlowPDiskStartup) {
        CheckSlowBootstrap(false);
    }

    Y_UNIT_TEST(BridgeBootstrapPreservesSlowPDiskStartup) {
        CheckSlowBootstrap(true);
    }


    template<bool Write>
    void TestMetadataTimeout() {
        TEnvironmentSetup env({.NodeCount = 1});
        auto& runtime = env.Runtime;
        const ui32 nodeId = *runtime->GetNodes().begin();
        const auto edge = runtime->AllocateEdgeActor(nodeId);
        const auto warden = MakeBlobStorageNodeWardenID(nodeId);
        const TString path = "/mock/pdisk-metadata-timeout";
        constexpr ui64 cookie = 741;
        constexpr ui32 pdiskResultType = Write ? NPDisk::TEvWriteMetadataResult::EventType
                                              : NPDisk::TEvReadMetadataResult::EventType;
        constexpr ui32 resultType = Write ? NStorage::TEvNodeWardenWriteMetadataResult::EventType
                                         : NStorage::TEvNodeWardenReadMetadataResult::EventType;
        std::unique_ptr<IEventHandle> delayed;
        bool delayReply = true;
        ui32 replies = 0;
        bool deliverReplies = false;
        runtime->FilterFunction = [&](ui32, std::unique_ptr<IEventHandle>& event) {
            if (delayReply && event->GetTypeRewrite() == pdiskResultType) {
                UNIT_ASSERT(!delayed);
                delayed = std::move(event);
                return false;
            }
            if (event->GetTypeRewrite() == resultType && event->Recipient == edge) {
                ++replies;
                return deliverReplies;
            }
            return true;
        };
        NKikimrBlobStorage::TPDiskMetadataRecord record;
        record.MutableCommittedStorageConfig()->SetGeneration(42);
        if constexpr (Write) {
            runtime->Send(new IEventHandle(warden, edge,
                new NStorage::TEvNodeWardenWriteMetadata(path, record), 0, cookie), nodeId);
        } else {
            runtime->Send(new IEventHandle(warden, edge,
                new NStorage::TEvNodeWardenReadMetadata(path), 0, cookie), nodeId);
        }
        using TResult = std::conditional_t<Write, NStorage::TEvNodeWardenWriteMetadataResult,
            NStorage::TEvNodeWardenReadMetadataResult>;
        env.Sim(TDuration::Seconds(65));
        UNIT_ASSERT(delayed);
        UNIT_ASSERT_VALUES_EQUAL_C(replies, 0, "A slow metadata operation must remain pending");

        using TPDiskResult = std::conditional_t<Write, NPDisk::TEvWriteMetadataResult,
            NPDisk::TEvReadMetadataResult>;
        const auto expectedGuid = delayed->Get<TPDiskResult>()->PDiskGuid;
        delayReply = false;
        runtime->Send(delayed.release(), nodeId);
        deliverReplies = true;
        auto response = env.WaitForEdgeActorEvent<TResult>(edge, false,
            runtime->GetClock() + TDuration::Seconds(5));
        UNIT_ASSERT(response);
        UNIT_ASSERT_VALUES_EQUAL(response->Cookie, cookie);
        UNIT_ASSERT(response->Get()->Outcome == (Write ? NPDisk::EPDiskMetadataOutcome::OK
            : NPDisk::EPDiskMetadataOutcome::NO_METADATA));
        UNIT_ASSERT(response->Get()->Guid == expectedGuid);
        env.Sim(TDuration::Seconds(1));
        UNIT_ASSERT_VALUES_EQUAL(replies, 1);
        runtime->FilterFunction = nullptr;

        const size_t disksCreated = env.PDiskActors.size();
        record.MutableCommittedStorageConfig()->SetGeneration(43);
        runtime->Send(new IEventHandle(warden, edge,
            new NStorage::TEvNodeWardenWriteMetadata(path, record)), nodeId);
        auto written = env.WaitForEdgeActorEvent<NStorage::TEvNodeWardenWriteMetadataResult>(edge, false,
            runtime->GetClock() + TDuration::Seconds(5));
        UNIT_ASSERT(written);
        UNIT_ASSERT(written->Get()->Outcome == NPDisk::EPDiskMetadataOutcome::OK);
        UNIT_ASSERT_VALUES_EQUAL(env.PDiskActors.size(), disksCreated);
        runtime->Send(new IEventHandle(warden, edge, new NStorage::TEvNodeWardenReadMetadata(path)), nodeId);
        auto read = env.WaitForEdgeActorEvent<NStorage::TEvNodeWardenReadMetadataResult>(edge, false,
            runtime->GetClock() + TDuration::Seconds(5));
        UNIT_ASSERT(read);
        UNIT_ASSERT(read->Get()->Outcome == NPDisk::EPDiskMetadataOutcome::OK);
        UNIT_ASSERT_VALUES_EQUAL(read->Get()->Record.SerializeAsString(), record.SerializeAsString());
    }

    Y_UNIT_TEST(ReadMetadataTimeoutWaitsForCompletion) {
        TestMetadataTimeout<false>();
    }

    Y_UNIT_TEST(WriteMetadataTimeoutWaitsForCompletion) {
        TestMetadataTimeout<true>();
    }

    Y_UNIT_TEST(MetadataFailureReleasesTemporaryPDisk) {
        for (const bool write : {false, true}) {
            for (const bool undelivered : {false, true}) {
                TEnvironmentSetup env({.NodeCount = 1});
                auto& runtime = env.Runtime;
                const auto edge = runtime->AllocateEdgeActor(1);
                const auto warden = MakeBlobStorageNodeWardenID(1);
                const TString path = "/mock/failed-metadata";
                bool fail = true;
                runtime->FilterFunction = [&](ui32, std::unique_ptr<IEventHandle>& ev) {
                    const ui32 type = write ? NPDisk::TEvWriteMetadataResult::EventType
                        : NPDisk::TEvReadMetadataResult::EventType;
                    if (fail && ev->GetTypeRewrite() == type) {
                        fail = false;
                        if (undelivered) {
                            ev = std::make_unique<IEventHandle>(ev->Recipient, ev->Sender,
                                new TEvents::TEvUndelivered(write ? NPDisk::TEvWriteMetadata::EventType
                                    : NPDisk::TEvReadMetadata::EventType, TEvents::TEvUndelivered::ReasonActorUnknown));
                        } else if (write) {
                            ev->Get<NPDisk::TEvWriteMetadataResult>()->Outcome = NPDisk::EPDiskMetadataOutcome::ERROR;
                        } else {
                            ev->Get<NPDisk::TEvReadMetadataResult>()->Outcome = NPDisk::EPDiskMetadataOutcome::ERROR;
                        }
                    }
                    return true;
                };
                if (write) {
                    runtime->Send(new IEventHandle(warden, edge,
                        new NStorage::TEvNodeWardenWriteMetadata(path, {})), 1);
                    auto response = env.WaitForEdgeActorEvent<NStorage::TEvNodeWardenWriteMetadataResult>(edge, false);
                    UNIT_ASSERT(response->Get()->Outcome == (undelivered ? NPDisk::EPDiskMetadataOutcome::OK
                        : NPDisk::EPDiskMetadataOutcome::ERROR));
                } else {
                    runtime->Send(new IEventHandle(warden, edge, new NStorage::TEvNodeWardenReadMetadata(path)), 1);
                    auto response = env.WaitForEdgeActorEvent<NStorage::TEvNodeWardenReadMetadataResult>(edge, false);
                    UNIT_ASSERT(response->Get()->Outcome == (undelivered ? NPDisk::EPDiskMetadataOutcome::NO_METADATA
                        : NPDisk::EPDiskMetadataOutcome::ERROR));
                }
                runtime->FilterFunction = nullptr;
                runtime->Send(new IEventHandle(warden, edge, new NStorage::TEvNodeWardenReadMetadata(path)), 1);
                const auto read = env.WaitForEdgeActorEvent<NStorage::TEvNodeWardenReadMetadataResult>(edge, false,
                    runtime->GetClock() + TDuration::Seconds(1));
                UNIT_ASSERT(read);
                UNIT_ASSERT(read->Get()->Outcome != NPDisk::EPDiskMetadataOutcome::ERROR);
                UNIT_ASSERT_VALUES_EQUAL(env.PDiskActors.size(), 2);
            }
        }
    }

    Y_UNIT_TEST(MetadataSurvivesNodeRestart) {
        ui32 registrations = 0;
        TEnvironmentSetup env({
            .NodeCount = 1,
            .PrepareRuntime = [&registrations](TTestActorSystem& runtime) {
                auto configure = std::move(runtime.SetupNodeSubSystems);
                runtime.SetupNodeSubSystems = [configure = std::move(configure), &registrations](
                        ui32 nodeId, TActorSystemSetup* setup) {
                    ++registrations;
                    configure(nodeId, setup);
                };
            },
        });
        UNIT_ASSERT_VALUES_EQUAL(registrations, 1);
        const ui32 nodeId = *env.Runtime->GetNodes().begin();
        const TString path = "/mock/pdisk-subsystem-metadata";
        const TActorId wardenId = MakeBlobStorageNodeWardenID(nodeId);
        NKikimrBlobStorage::TPDiskMetadataRecord record;
        record.MutableCommittedStorageConfig()->SetGeneration(42);

        auto edge = env.Runtime->AllocateEdgeActor(nodeId);
        env.Runtime->Send(new IEventHandle(wardenId, edge,
            new NStorage::TEvNodeWardenWriteMetadata(path, record)), nodeId);
        auto written = env.Runtime->WaitForEdgeActorEvent<NStorage::TEvNodeWardenWriteMetadataResult>(edge);
        UNIT_ASSERT(written->Get()->Outcome == NPDisk::EPDiskMetadataOutcome::OK);

        const auto states = env.PDiskMockStates;
        UNIT_ASSERT(!states.empty());
        env.RestartNode(nodeId);
        UNIT_ASSERT_VALUES_EQUAL(registrations, 2);

        edge = env.Runtime->AllocateEdgeActor(nodeId);
        env.Runtime->Send(new IEventHandle(wardenId, edge,
            new NStorage::TEvNodeWardenReadMetadata(path)), nodeId);
        auto read = env.Runtime->WaitForEdgeActorEvent<NStorage::TEvNodeWardenReadMetadataResult>(edge);
        UNIT_ASSERT(read->Get()->Outcome == NPDisk::EPDiskMetadataOutcome::OK);
        UNIT_ASSERT_VALUES_EQUAL(read->Get()->Record.SerializeAsString(), record.SerializeAsString());
        for (const auto& [key, state] : states) {
            UNIT_ASSERT(env.PDiskMockStates.at(key) == state);
        }
    }
}
