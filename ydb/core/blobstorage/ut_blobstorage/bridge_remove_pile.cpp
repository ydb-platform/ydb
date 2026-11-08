#include "bridge_pile_ops.h"

#include <ydb/core/blobstorage/nodewarden/distconf.h>
#include <ydb/core/blobstorage/ut_blobstorage/lib/env.h>

using namespace NBridgeTest;

namespace {

    using TResult = NKikimrBlobStorage::TEvNodeConfigInvokeOnRootResult;

    constexpr ui32 PileOpsLevel = NStorage::BridgeFeatureLevelPileOps;

    size_t NumStaticVDisks(TEnvironmentSetup& env) {
        return QueryStorageConfig(env).GetBlobStorageConfig().GetServiceSet().VDisksSize();
    }

} // anonymous

Y_UNIT_TEST_SUITE(BridgeRemovePile) {
    enum class EReprovisionMode { InspectConfig, FormatDisks, KeepDisks };
    enum class ECacheReplay { Disabled, BeforeProvision, AfterProvision, BeforeComprehensive };

    class TReprovisionFixture {
        using TPDisk = NKikimrBlobStorage::TNodeWardenServiceSet::TPDisk;
        static constexpr ui32 PileNodeCount = LastThirdPileNodeId - FirstThirdPileNodeId + 1;
        std::vector<TPDisk> OldPDisks;
        std::optional<NKikimrBlobStorage::TBaseConfig> ProvisionBaseConfig;
        std::vector<std::unique_ptr<IEventHandle>> ControllerUpdates;
        std::vector<std::unique_ptr<IEventHandle>> PipeFailures;
        std::set<ui32> ComprehensiveReplies;
        std::set<ui32> CachedDynamicStarts;
        std::set<ui32> RestoredOldConfig;
        bool HoldControllerUpdates = false;
        bool HoldPipeFailures = false;
        bool ObserveStartup = false;
        bool Isolated = false;
        bool BlockPeerConfigs = false;
        size_t RejectedInitializations = 0;

        static bool InPile(ui32 nodeId) {
            return FirstThirdPileNodeId <= nodeId && nodeId <= LastThirdPileNodeId;
        }

        bool HoldControllerUpdate(ui32 nodeId, std::unique_ptr<IEventHandle>& ev) {
            if (HoldControllerUpdates && InPile(nodeId)
                    && ev->GetTypeRewrite() == TEvBlobStorage::TEvControllerNodeServiceSetUpdate::EventType) {
                if (ev->Get<TEvBlobStorage::TEvControllerNodeServiceSetUpdate>()->Record.GetComprehensive()) {
                    ComprehensiveReplies.insert(nodeId);
                }
                ControllerUpdates.push_back(std::move(ev));
                return true;
            }
            return false;
        }

        bool CheckMediaGuid(ui32 nodeId, std::unique_ptr<IEventHandle>& ev) {
            if (!RejectNewGuids || !InPile(nodeId) || ev->GetTypeRewrite() != NPDisk::TEvYardInit::EventType) {
                return true;
            }
            const auto& init = *ev->Get<NPDisk::TEvYardInit>();
            const bool oldGuid = std::ranges::any_of(OldPDisks, [&](const auto& pdisk) {
                return pdisk.GetNodeID() == nodeId && pdisk.GetPDiskGuid() == init.PDiskGuid;
            });
            if (oldGuid && HoldControllerUpdates
                    && TGroupID(init.VDisk.GroupID).ConfigurationType() == EGroupConfigurationType::Dynamic) {
                CachedDynamicStarts.insert(nodeId);
            }
            if (oldGuid) {
                return true;
            }
            // The mock keys data by PDisk ID. Model the real device's retained GUID across ID changes.
            ++RejectedInitializations;
            Env.Runtime->Send(new IEventHandle(ev->Sender, ev->Recipient,
                new NPDisk::TEvYardInitResult(NKikimrProto::CORRUPTED, "PDisk GUID mismatch on reused media"),
                0, ev->Cookie), nodeId);
            return false;
        }

        bool Filter(ui32 nodeId, std::unique_ptr<IEventHandle>& ev) {
            const ui32 type = ev->GetTypeRewrite();
            if (HoldPipeFailures && InPile(nodeId) && type == TEvTabletPipe::TEvClientDestroyed::EventType) {
                PipeFailures.push_back(std::move(ev));
                return false;
            }
            if (Isolated) {
                if (BlockPeerConfigs && InPile(nodeId) && type == NStorage::TEvNodeWardenUpdateConfigFromPeer::EventType) {
                    return false;
                }
                if (ev->InterconnectSession && InPile(nodeId) != InPile(ev->Sender.NodeId())) {
                    return false;
                }
            }
            if (HoldControllerUpdate(nodeId, ev)) {
                return false;
            }
            if (type == NStorage::TEvNodeWardenBaseConfig::EventType) {
                const auto& msg = *ev->Get<NStorage::TEvNodeWardenBaseConfig>();
                UNIT_ASSERT(!msg.ErrorReason);
                ProvisionBaseConfig = msg.BaseConfig;
            }
            if (ObserveStartup && InPile(nodeId) && type == TEvNodeWardenStorageConfig::EventType) {
                const auto& config = *ev->Get<TEvNodeWardenStorageConfig>()->Config;
                for (const auto& pdisk : config.GetBlobStorageConfig().GetServiceSet().GetPDisks()) {
                    for (const auto& old : OldPDisks) {
                        if (pdisk.GetNodeID() == nodeId && pdisk.GetNodeID() == old.GetNodeID()
                                && pdisk.GetPDiskID() == old.GetPDiskID() && pdisk.GetPDiskGuid() == old.GetPDiskGuid()) {
                            RestoredOldConfig.insert(nodeId);
                        }
                    }
                }
            }
            return CheckMediaGuid(nodeId, ev);
        }

        void CheckOldPDisks(const NKikimrBlobStorage::TBaseConfig& baseConfig, bool present) {
            for (const auto& old : OldPDisks) {
                const bool found = std::ranges::any_of(baseConfig.GetPDisk(), [&](const auto& pdisk) {
                    return pdisk.GetNodeId() == old.GetNodeID() && pdisk.GetPDiskId() == old.GetPDiskID()
                        && pdisk.GetGuid() == old.GetPDiskGuid();
                });
                UNIT_ASSERT_VALUES_EQUAL_C(found, present, old.DebugString());
            }
        }

    public:
        TEnvironmentSetup Env;
        const ui32 GroupId;
        TDataChecker Checker;
        bool RejectNewGuids = false;

        explicit TReprovisionFixture(bool cache = false)
            : Env({
                .NodeCount = 24,
                .Erasure = TBlobStorageGroupType::Erasure4Plus2Block,
                .Cache = cache,
                .LocationGenerator = GetBridgedLocation,
                .SelfManagementConfig = true,
                .NumPiles = 3,
                .AutomaticBootstrap = true,
            })
            , GroupId(CreatePoolAndGetGroup(Env))
            , Checker(Env)
        {
            Checker.Write(GroupId);
            const auto initial = QueryStorageConfig(Env);
            for (const auto& pdisk : initial.GetBlobStorageConfig().GetServiceSet().GetPDisks()) {
                if (InPile(pdisk.GetNodeID())) {
                    OldPDisks.push_back(pdisk);
                }
            }
            UNIT_ASSERT(!OldPDisks.empty());
            Env.Runtime->FilterFunction = [this](ui32 nodeId, std::unique_ptr<IEventHandle>& ev) {
                return Filter(nodeId, ev);
            };
        }

        ~TReprovisionFixture() {
            Env.Runtime->FilterFunction = {};
        }

        void StopPile() {
            for (ui32 nodeId = FirstThirdPileNodeId; nodeId <= LastThirdPileNodeId; ++nodeId) {
                Env.StopNode(nodeId);
            }
        }

        void ClearPileDisks() {
            std::erase_if(Env.PDiskMockStates, [&](const auto& item) { return InPile(item.first.first); });
            std::erase_if(Env.PDiskMetadataByPath, [&](const auto& item) { return InPile(item.first.first); });
            std::erase_if(Env.Cache, [&](const auto& item) { return InPile(item.first); });
        }

        void DisconnectAndRemove(bool removeHosts, bool keepRunning = false) {
            HoldPipeFailures = keepRunning;
            SwitchPileState(Env, 2, NKikimrBridge::TClusterState::DISCONNECTED);
            Env.Sim(TDuration::Seconds(10));
            if (keepRunning) {
                // Keep old services alive while all cross-pile traffic and peer-config catchup is isolated.
                // Also delay pipe-loss notifications to exercise config arriving before OnPipeError.
                Isolated = BlockPeerConfigs = true;
                for (ui32 nodeId = FirstThirdPileNodeId; nodeId <= LastThirdPileNodeId; ++nodeId) {
                    const auto edge = Env.Runtime->AllocateEdgeActor(nodeId);
                    UNIT_ASSERT(Env.Runtime->WrapInActorContext(edge, [&](IActor*) {
                        for (ui32 peer = 1; peer < FirstThirdPileNodeId; ++peer) {
                            TActivationContext::Send(new IEventHandle(TEvInterconnect::EvDisconnect, 0,
                                TActivationContext::InterconnectProxy(peer), {}, nullptr, 0));
                        }
                    }));
                }
            }
            if (!keepRunning) {
                StopPile();
            }
            Env.Sim(TDuration::Seconds(30));
            if (keepRunning) {
                WaitUntil(Env, [&] {
                    const auto response = InvokeDistconf(Env, [](auto& request) {
                        request.MutableQueryConfig();
                    }, 1, Env.Runtime->GetClock() + TDuration::Seconds(5));
                    return response.GetStatus() == TResult::OK && response.HasScepter();
                }, "distconf leadership after isolation", TDuration::Seconds(1), 60);
            }
            RemovePileAndWait(Env, "pile_3");
            CheckOldPDisks(Env.FetchBaseConfig(), true);
            if (removeHosts) {
                auto res = ReplaceMainConfig(Env, [&](YAML::Node& config) {
                    YAML::Node hosts(YAML::NodeType::Sequence);
                    for (const YAML::Node& host : config["hosts"]) {
                        if (host["location"]["bridge_pile_name"].as<TString>() != "pile_3") {
                            hosts.push_back(host);
                        }
                    }
                    config["hosts"] = hosts;
                });
                UNIT_ASSERT_EQUAL_C(res.GetStatus(), TResult::OK, res.GetErrorReason());
                Env.Sim(TDuration::Seconds(10));
                CheckOldPDisks(Env.FetchBaseConfig(), false);
                res = ReplaceMainConfig(Env, [&](YAML::Node& config) {
                    AddPileHosts(config, "pile_3", FirstThirdPileNodeId, LastThirdPileNodeId);
                });
                UNIT_ASSERT_EQUAL_C(res.GetStatus(), TResult::OK, res.GetErrorReason());
                Env.Sim(TDuration::Seconds(10));
            }
            const auto before = QueryStorageConfig(Env);
            size_t count = 0;
            for (const auto& pdisk : before.GetBlobStorageConfig().GetServiceSet().GetPDisks()) {
                count += InPile(pdisk.GetNodeID());
            }
            UNIT_ASSERT_VALUES_EQUAL(count, removeHosts ? 0 : OldPDisks.size());
        }

        void StartPile(bool clearMedia, std::optional<bool> expectCachedStarts = std::nullopt) {
            UNIT_ASSERT(std::ranges::any_of(Env.PDiskMetadataByPath, [&](const auto& item) {
                return InPile(item.first.first) && item.second && item.second->has_value();
            }));
            if (clearMedia) {
                ClearPileDisks();
            }
            HoldControllerUpdates = expectCachedStarts.has_value();
            if (HoldControllerUpdates && !clearMedia) {
                for (ui32 nodeId = FirstThirdPileNodeId; nodeId <= LastThirdPileNodeId; ++nodeId) {
                    NKikimrBlobStorage::TNodeWardenCache cache;
                    UNIT_ASSERT(google::protobuf::TextFormat::ParseFromString(Env.Cache.at(nodeId), &cache));
                    UNIT_ASSERT(cache.GetServiceSet().PDisksSize());
                    UNIT_ASSERT(cache.GetServiceSet().VDisksSize());
                }
            }
            Checker.Write(GroupId);
            ObserveStartup = true;
            for (ui32 nodeId = FirstThirdPileNodeId; nodeId <= LastThirdPileNodeId; ++nodeId) {
                Env.StartNode(nodeId);
            }
            Env.Sim(TDuration::Seconds(30));
            ObserveStartup = false;
            UNIT_ASSERT_VALUES_EQUAL(RestoredOldConfig.size(), clearMedia ? 0 : PileNodeCount);
            if (expectCachedStarts) {
                UNIT_ASSERT_VALUES_EQUAL(CachedDynamicStarts.size(), *expectCachedStarts ? PileNodeCount : 0);
            }
        }

        void ReleaseControllerUpdates() {
            UNIT_ASSERT_VALUES_EQUAL(ComprehensiveReplies.size(), PileNodeCount);
            HoldControllerUpdates = false;
            for (auto& ev : ControllerUpdates) {
                const ui32 nodeId = ev->Recipient.NodeId();
                Env.Runtime->Send(ev.release(), nodeId);
            }
            ControllerUpdates.clear();
            Env.Sim(TDuration::Seconds(30));
        }

        void Provision(bool removedHosts) {
            ProvisionBaseConfig.reset();
            const auto res = AddPile(Env, "pile_3");
            UNIT_ASSERT_EQUAL_C(res.GetStatus(), TResult::OK, res.GetErrorReason());
            UNIT_ASSERT(ProvisionBaseConfig);
            CheckOldPDisks(*ProvisionBaseConfig, !removedHosts);
            size_t count = 0;
            const auto config = QueryStorageConfig(Env);
            for (const auto& pdisk : config.GetBlobStorageConfig().GetServiceSet().GetPDisks()) {
                if (InPile(pdisk.GetNodeID())) {
                    ++count;
                    UNIT_ASSERT_C(std::ranges::any_of(ProvisionBaseConfig->GetPDisk(), [&](const auto& base) {
                        return base.GetNodeId() == pdisk.GetNodeID() && base.GetPDiskId() == pdisk.GetPDiskID()
                            && base.GetGuid() == pdisk.GetPDiskGuid() && base.GetPath() == pdisk.GetPath();
                    }), pdisk.DebugString());
                    if (removedHosts) {
                        UNIT_ASSERT_GE(pdisk.GetPDiskID(), 1000);
                    }
                }
            }
            UNIT_ASSERT(count);
        }

        void DeliverConfigToRunningPile() {
            UNIT_ASSERT(!PipeFailures.empty());
            const auto config = QueryStorageConfig(Env);
            BlockPeerConfigs = false;
            for (ui32 nodeId = FirstThirdPileNodeId; nodeId <= LastThirdPileNodeId; ++nodeId) {
                // This is the normal distconf input from a rejected handshake; no process restarts.
                Env.Runtime->Send(new IEventHandle(MakeBlobStorageNodeWardenID(nodeId), {},
                    new NStorage::TEvNodeWardenUpdateConfigFromPeer(config, config)), nodeId);
            }
            Env.Sim(TDuration::Seconds(30));
        }

        void DestroyDynamicGroup(const TIntrusivePtr<TBlobStorageGroupInfo>& info) {
            std::vector<TActorId> actors;
            for (ui32 i = 0; i < info->GetTotalVDisksNum(); ++i) {
                const auto service = info->GetActorId(i);
                UNIT_ASSERT(InPile(service.NodeId()));
                const auto actor = Env.Runtime->GetNode(service.NodeId())->ActorSystem->LookupLocalService(service);
                UNIT_ASSERT(actor && Env.Runtime->GetActor(actor));
                actors.push_back(actor);
            }
            UNIT_ASSERT(!actors.empty());
            for (ui32 nodeId = FirstThirdPileNodeId; nodeId <= LastThirdPileNodeId; ++nodeId) {
                auto update = std::make_unique<TEvBlobStorage::TEvControllerNodeServiceSetUpdate>(NKikimrProto::OK, nodeId);
                auto* group = update->Record.MutableServiceSet()->AddGroups();
                group->SetGroupID(info->GroupID.GetRawId());
                group->SetEntityStatus(NKikimrBlobStorage::DESTROY);
                Env.Runtime->Send(new IEventHandle(MakeBlobStorageNodeWardenID(nodeId), {}, update.release()), nodeId);
            }
            WaitUntil(Env, [&] {
                return std::ranges::none_of(actors, [&](const auto& actor) { return Env.Runtime->GetActor(actor); });
            }, "destroyed component VDisk actors", TDuration::Seconds(1), 30);
        }

        void CheckStaticVDisks(NKikimrProto::EReplyStatus expected) {
            Env.Sim(TDuration::Seconds(30));
            if (expected == NKikimrProto::VDISK_ERROR_STATE) {
                UNIT_ASSERT(RejectedInitializations);
            }
            const auto config = QueryStorageConfig(Env);
            size_t count = 0;
            for (const auto& vdisk : config.GetBlobStorageConfig().GetServiceSet().GetVDisks()) {
                const auto& loc = vdisk.GetVDiskLocation();
                if (!InPile(loc.GetNodeID()) || vdisk.GetDoDestroy()
                        || vdisk.GetEntityStatus() == NKikimrBlobStorage::DESTROY) {
                    continue;
                }
                const auto edge = Env.Runtime->AllocateEdgeActor(loc.GetNodeID());
                Env.Runtime->Send(new IEventHandle(MakeBlobStorageVDiskID(loc.GetNodeID(), loc.GetPDiskID(),
                    loc.GetVDiskSlotID()), edge, new TEvBlobStorage::TEvVStatus(VDiskIDFromVDiskID(vdisk.GetVDiskID()))),
                    loc.GetNodeID());
                const auto response = Env.WaitForEdgeActorEvent<TEvBlobStorage::TEvVStatusResult>(edge, false,
                    Env.Runtime->GetClock() + TDuration::Seconds(10));
                UNIT_ASSERT(response);
                UNIT_ASSERT_VALUES_EQUAL(response->Get()->Record.GetStatus(), expected);
                ++count;
            }
            UNIT_ASSERT(count);
        }

        void CheckSurvivingData() {
            Checker.Write(GroupId);
            Checker.CheckAll();
        }

        void ClearAndRestart() {
            StopPile();
            ClearPileDisks();
            RejectNewGuids = false;
            for (ui32 nodeId = FirstThirdPileNodeId; nodeId <= LastThirdPileNodeId; ++nodeId) {
                Env.StartNode(nodeId);
            }
            Env.Sim(TDuration::Seconds(30));
        }

        void RejoinAndCheck() {
            WaitPileOperationFinished(Env);
            RejoinPile(Env, 2);
            Checker.CheckAll();
            CheckPileData(Env, Checker, 2, FirstThirdPileNodeId);
            CheckSurvivingData();
        }
    };

    void CheckReprovision(bool removeHosts, EReprovisionMode mode = EReprovisionMode::InspectConfig,
            ECacheReplay cacheReplay = ECacheReplay::Disabled) {
        TReprovisionFixture test(cacheReplay != ECacheReplay::Disabled);
        const bool restart = mode != EReprovisionMode::InspectConfig;
        const bool keepDisks = mode == EReprovisionMode::KeepDisks;
        test.RejectNewGuids = keepDisks;
        test.DisconnectAndRemove(removeHosts);
        if (restart && cacheReplay != ECacheReplay::AfterProvision) {
            test.StartPile(!keepDisks, cacheReplay == ECacheReplay::Disabled ? std::nullopt : std::make_optional(true));
        }
        if (cacheReplay == ECacheReplay::BeforeComprehensive) {
            test.ReleaseControllerUpdates();
        }
        test.Provision(removeHosts);
        if (restart && cacheReplay == ECacheReplay::AfterProvision) {
            test.StartPile(!keepDisks, false);
        }
        if (keepDisks) {
            test.CheckStaticVDisks(NKikimrProto::VDISK_ERROR_STATE);
            if (cacheReplay == ECacheReplay::BeforeProvision || cacheReplay == ECacheReplay::AfterProvision) {
                test.ReleaseControllerUpdates();
            }
            test.CheckSurvivingData();
            test.ClearAndRestart();
        }
        if (restart) {
            test.RejoinAndCheck();
        }
    }

    Y_UNIT_TEST(BaseConfigAtReprovisionAfterHostRemoval) {
        for (const bool removeHosts : {false, true}) {
            CheckReprovision(removeHosts);
        }
    }

    Y_UNIT_TEST(RestartReprovisionedPileFormatted) {
        CheckReprovision(true, EReprovisionMode::FormatDisks);
    }

    Y_UNIT_TEST(RestartReprovisionedPileUnformatted) {
        CheckReprovision(true, EReprovisionMode::KeepDisks);
    }

    Y_UNIT_TEST(RestartReprovisionedPileWithStaleCache) {
        CheckReprovision(true, EReprovisionMode::KeepDisks, ECacheReplay::BeforeProvision);
    }

    Y_UNIT_TEST(RestartReprovisionedPileBeforeStaleCacheReplay) {
        CheckReprovision(true, EReprovisionMode::KeepDisks, ECacheReplay::AfterProvision);
    }

    Y_UNIT_TEST(RestartReprovisionedPileAfterCacheReconciliation) {
        CheckReprovision(true, EReprovisionMode::KeepDisks, ECacheReplay::BeforeComprehensive);
    }

    Y_UNIT_TEST(ReprovisionRunningPileBeforePipeFailureNotification) {
        TReprovisionFixture test;
        test.RejectNewGuids = true;
        test.DisconnectAndRemove(true, true);
        test.Provision(true);
        test.DeliverConfigToRunningPile();
        test.CheckStaticVDisks(NKikimrProto::VDISK_ERROR_STATE);
        test.CheckSurvivingData();
    }

    Y_UNIT_TEST(ReprovisionRunningPileAfterDynamicGroupDestruction) {
        TReprovisionFixture test;
        test.RejectNewGuids = true;
        const auto component = test.Env.GetGroupInfo(GetPileGroups(test.Env, test.GroupId)[2]);
        test.DisconnectAndRemove(true, true);
        test.DestroyDynamicGroup(component);
        test.Provision(true);
        test.DeliverConfigToRunningPile();
        test.CheckStaticVDisks(NKikimrProto::VDISK_ERROR_STATE);
        test.CheckSurvivingData();
    }

    Y_UNIT_TEST(ClearedCacheCannotRestoreOldPDiskBeforeProvisioning) {
        TReprovisionFixture test(true);
        test.DisconnectAndRemove(true);
        test.StartPile(true, false); // clear media and cache; BSC replies stay held for over five seconds
        test.Provision(true);
        test.CheckStaticVDisks(NKikimrProto::OK);
        test.ReleaseControllerUpdates();
        test.CheckSurvivingData();
    }

    // the pile is added and removed repeatedly: data stays available, nothing of the removed pile remains in the
    // cluster and the lists of piles don't grow; then the removed pile is dropped from the config along with its hosts
    Y_UNIT_TEST(AddRemoveCycles) {
        TEnvironmentSetup env(MakeExpandableSettings());
        const ui32 groupId = CreatePoolAndGetGroup(env);
        TDataChecker checker(env);
        checker.Write(groupId);

        auto res = AddThirdPileToConfig(env);
        UNIT_ASSERT_EQUAL_C(res.GetStatus(), TResult::OK, res.GetErrorReason());
        env.Sim(TDuration::Seconds(10));
        StartPileNodes(env, "pile_3", FirstThirdPileNodeId, LastThirdPileNodeId);

        std::optional<size_t> numStaticVDisks;
        THashSet<ui32> staticPartIds; // nodes know the static groups removed with the pile, so their ids are not reused
        ui64 prevAddedGeneration = 0;
        for (ui32 cycle = 0; cycle < 3; ++cycle) {
            AddPileAndWait(env, "pile_3");
            const auto config = QueryStorageConfig(env);
            // the generation of the addition tells the nodes of the previous incarnation of the pile from the current ones
            const ui64 addedGeneration = config.GetClusterStateDetails().GetPileRegistry().GetPiles(2).GetAddedGeneration();
            UNIT_ASSERT_LT(prevAddedGeneration, addedGeneration);
            UNIT_ASSERT_LE(addedGeneration, config.GetClusterState().GetGeneration());
            prevAddedGeneration = addedGeneration;
            UNIT_ASSERT_VALUES_EQUAL(GetPileStates(config), "pile_1:SYNCHRONIZED pile_2:SYNCHRONIZED pile_3:DISCONNECTED");
            // nodes of older versions can't join the cluster after pile operations
            UNIT_ASSERT(NStorage::IsPileListChanged(config));
            UNIT_ASSERT_VALUES_EQUAL(NStorage::GetRequiredBridgeFeatureLevel(config), PileOpsLevel);
            for (const auto& group : config.GetBlobStorageConfig().GetServiceSet().GetGroups()) {
                if (group.HasBridgeGroupState()) {
                    const auto *part = NBridge::GetGroupPile(group.GetBridgeGroupState(), TBridgePileId::FromPileIndex(2));
                    UNIT_ASSERT(part);
                    UNIT_ASSERT(staticPartIds.insert(part->GetGroupId()).second);
                }
            }
            UNIT_ASSERT_VALUES_EQUAL(CheckPileParts(env, 2, FirstThirdPileNodeId, LastThirdPileNodeId), 1);
            RejoinPile(env, 2);
            checker.Write(groupId);
            checker.CheckAll();
            CheckPileData(env, checker, 2, FirstThirdPileNodeId);

            // static VDisks of the previous incarnation of the pile get destroyed by its nodes once they rejoin
            if (!numStaticVDisks) {
                numStaticVDisks = NumStaticVDisks(env);
            } else {
                WaitUntil(env, [&] { return NumStaticVDisks(env) == *numStaticVDisks; }, "destroyed VDisks slain");
            }

            SwitchPileState(env, 2, NKikimrBridge::TClusterState::DISCONNECTED);
            RemovePileAndWait(env, "pile_3");
            UNIT_ASSERT_VALUES_EQUAL(GetPileStates(QueryStorageConfig(env)),
                "pile_1:SYNCHRONIZED pile_2:SYNCHRONIZED pile_3:INACTIVE");
            CheckPileGone(env, 2, FirstThirdPileNodeId, LastThirdPileNodeId);
            checker.Write(groupId);
            checker.CheckAll();
        }

        // the hosts of the pile are decommissioned, the slot of the pile gets free and trimmed
        for (ui32 nodeId = FirstThirdPileNodeId; nodeId <= LastThirdPileNodeId; ++nodeId) {
            env.StopNode(nodeId);
        }
        res = DropPileFromConfig(env, "pile_3");
        UNIT_ASSERT_EQUAL_C(res.GetStatus(), TResult::OK, res.GetErrorReason());
        env.Sim(TDuration::Seconds(10));
        const auto config = QueryStorageConfig(env);
        UNIT_ASSERT_VALUES_EQUAL(GetPileStates(config), "pile_1:SYNCHRONIZED pile_2:SYNCHRONIZED");
        UNIT_ASSERT_VALUES_EQUAL(NStorage::GetRequiredBridgeFeatureLevel(config), PileOpsLevel); // kept by config changes
        const auto& ss = config.GetBlobStorageConfig().GetServiceSet();
        for (const auto& vdisk : ss.GetVDisks()) {
            UNIT_ASSERT_LT_C(vdisk.GetVDiskLocation().GetNodeID(), FirstThirdPileNodeId, vdisk.DebugString());
        }
        for (const auto& pdisk : ss.GetPDisks()) {
            UNIT_ASSERT_LT_C(pdisk.GetNodeID(), FirstThirdPileNodeId, pdisk.DebugString());
        }
        const auto baseConfig = env.FetchBaseConfig();
        for (const auto& pdisk : baseConfig.GetPDisk()) {
            UNIT_ASSERT_LT_C(pdisk.GetNodeId(), FirstThirdPileNodeId, pdisk.DebugString());
        }
        checker.Write(groupId);
        checker.CheckAll();
    }

    // the pile added to the converted cluster is removed and added again; inline piles stay the only parts of their
    // groups meanwhile
    Y_UNIT_TEST(ConvertedAddRemove) {
        const auto named = std::make_shared<bool>(false);
        TEnvironmentSetup env(MakeConvertedClusterSettings(named));
        TDataChecker checker(env);
        const ui32 groupId = ConvertCluster(env, checker);
        NameConvertedClusterNodes(env, *named);
        auto res = AddPileToConfig(env, "pile_2", 10, 17);
        UNIT_ASSERT_EQUAL_C(res.GetStatus(), TResult::OK, res.GetErrorReason());
        env.Sim(TDuration::Seconds(10));
        StartPileNodes(env, "pile_2", 10, 17);

        for (ui32 cycle = 0; cycle < 2; ++cycle) {
            AddPileAndWait(env, "pile_2");
            UNIT_ASSERT_VALUES_EQUAL(CheckPileParts(env, 1, 10, 17), 1);
            RejoinPile(env, 1);
            checker.Write(groupId);
            checker.CheckAll();
            CheckPileData(env, checker, 1, 10);

            SwitchPileState(env, 1, NKikimrBridge::TClusterState::DISCONNECTED);
            RemovePileAndWait(env, "pile_2");
            CheckPileGone(env, 1, 10, 17);
            const auto config = QueryStorageConfig(env);
            UNIT_ASSERT_VALUES_EQUAL(GetPileStates(config), "pile_1:SYNCHRONIZED pile_2:INACTIVE");
            UNIT_ASSERT_VALUES_EQUAL(config.GetClusterStateDetails().GetPileRegistry().GetImplicitPileId(), 1);

            // the converted group is the inline pile only again
            const auto info = GetGroupInfo(env, groupId);
            UNIT_ASSERT_VALUES_EQUAL_C(info.GetBridgeGroupState().PileSize(), 1, info.DebugString());
            UNIT_ASSERT_VALUES_EQUAL_C(info.GetBridgeGroupState().GetPile(0).GetGroupId(), groupId, info.DebugString());
            checker.Write(groupId);
            checker.CheckAll();
            CheckPileData(env, checker, 0, 1);
        }

        // nodes keep working after restarts
        env.RestartNode(2);
        env.Sim(TDuration::Seconds(30));
        checker.Write(groupId);
        checker.CheckAll();
    }

    // the original pile of the converted cluster is removed when the added one has taken over: groups of the cluster lose
    // their inline piles and keep serving as bridge proxy groups only, the implicit pile goes away
    Y_UNIT_TEST(ConvertedRemoveOriginalPile) {
        const auto named = std::make_shared<bool>(false);
        TEnvironmentSetup env(MakeConvertedClusterSettings(named));
        TDataChecker checker(env);
        const ui32 groupId = ConvertCluster(env, checker);
        NameConvertedClusterNodes(env, *named);
        auto res = AddPileToConfig(env, "pile_2", 10, 17);
        UNIT_ASSERT_EQUAL_C(res.GetStatus(), TResult::OK, res.GetErrorReason());
        env.Sim(TDuration::Seconds(10));
        StartPileNodes(env, "pile_2", 10, 17);
        AddPileAndWait(env, "pile_2");
        RejoinPile(env, 1);
        checker.Write(groupId);
        checker.CheckAll();

        // the added pile becomes the primary one, the original pile is disconnected; distconf is asked through the
        // nodes of the added pile from now on
        constexpr ui32 nodeId = 10;

        // the read of the blob present in the added pile only gets NODATA from the inline pile, while the response of
        // the added pile is held until the inline pile is removed; then the blob is restored into the inline pile gone
        const TLogoBlobID blobId(100500, 1, 1, 0, 6, 0);
        const TActorId edge = env.Runtime->AllocateEdgeActor(nodeId, __FILE__, __LINE__);
        const ui32 addedPartId = NBridge::GetGroupPile(GetGroupInfo(env, groupId).GetBridgeGroupState(),
            TBridgePileId::FromPileIndex(1))->GetGroupId();
        const ui32 addedPartGeneration = GetGroupInfo(env, addedPartId).GetGroupGeneration();
        env.Runtime->WrapInActorContext(edge, [&] {
            auto *put = new TEvBlobStorage::TEvPut(blobId, TString("hello!"), TInstant::Max());
            put->ForceGroupGeneration = addedPartGeneration;
            SendToBSProxy(edge, addedPartId, put);
        });
        {
            const auto putResult = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvPutResult>(edge, false);
            UNIT_ASSERT_VALUES_EQUAL_C(putResult->Get()->Status, NKikimrProto::OK, putResult->Get()->ToString());
        }
        std::unique_ptr<IEventHandle> heldResult;
        env.Runtime->FilterFunction = [&](ui32 /*nodeId*/, std::unique_ptr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == TEvBlobStorage::EvGetResult && !heldResult) {
                const auto *msg = ev->Get<TEvBlobStorage::TEvGetResult>();
                if (msg->GroupId == addedPartId && msg->ResponseSz == 1 && msg->Responses[0].Id == blobId) {
                    heldResult = std::move(ev);
                    return false;
                }
            }
            return true;
        };
        env.Runtime->WrapInActorContext(edge, [&] {
            SendToBSProxy(edge, groupId, new TEvBlobStorage::TEvGet(blobId, 0, 0, TInstant::Max(),
                NKikimrBlobStorage::FastRead, true /*mustRestoreFirst*/));
        });
        WaitUntil(env, [&] { return heldResult != nullptr; }, "response of the added pile");
        env.Runtime->FilterFunction = {};

        // Retain an already-resolved physical request, as a phantom check from an old VDisk can be in flight when
        // its inline pile is removed. Replaying the original envelope must not reinterpret it as a logical read.
        const auto physicalEdge = env.Runtime->AllocateEdgeActor(nodeId, __FILE__, __LINE__);
        std::unique_ptr<IEventHandle> physicalRequest;
        env.Runtime->FilterFunction = [&](ui32, std::unique_ptr<IEventHandle>& ev) {
            if (ev->Sender == physicalEdge && ev->GetTypeRewrite() == TEvBlobStorage::EvGet) {
                physicalRequest = std::move(ev);
                return false;
            }
            return true;
        };
        const ui32 inlineGeneration = GetGroupInfo(env, groupId).GetGroupGeneration();
        env.Runtime->WrapInActorContext(physicalEdge, [&] {
            auto request = std::make_unique<TEvBlobStorage::TEvGet>(blobId, 0, 0, TInstant::Max(),
                NKikimrBlobStorage::AsyncRead);
            request->PhantomCheck = true;
            request->ForceGroupGeneration = inlineGeneration;
            SendToBSProxy(physicalEdge, groupId, request.release());
        });
        WaitUntil(env, [&] { return bool(physicalRequest); }, "physical request before pile removal");
        env.Runtime->FilterFunction = {};

        auto clusterState = QueryStorageConfig(env).GetClusterState();
        clusterState.SetGeneration(clusterState.GetGeneration() + 1);
        TBridgePileId::FromPileIndex(1).CopyToProto(&clusterState, &NKikimrBridge::TClusterState::SetPrimaryPile);
        TBridgePileId::FromPileIndex(1).CopyToProto(&clusterState, &NKikimrBridge::TClusterState::SetPromotedPile);
        clusterState.SetPerPileState(0, NKikimrBridge::TClusterState::DISCONNECTED);
        res = InvokeDistconf(env, [&](auto& record) {
            record.MutableSwitchBridgeClusterState()->MutableNewClusterState()->CopyFrom(clusterState);
        }, nodeId);
        UNIT_ASSERT_EQUAL_C(res.GetStatus(), TResult::OK, res.GetErrorReason());
        env.Sim(TDuration::Seconds(30));

        res = RemovePile(env, "pile_1", nodeId);
        UNIT_ASSERT_EQUAL_C(res.GetStatus(), TResult::OK, res.GetErrorReason());
        const auto config = WaitPileOperationFinished(env, nodeId);
        UNIT_ASSERT_VALUES_EQUAL(GetPileStates(config), "pile_1:INACTIVE pile_2:SYNCHRONIZED");
        UNIT_ASSERT_VALUES_EQUAL(config.GetClusterStateDetails().GetPileRegistry().GetImplicitPileId(), 0);
        CheckPileGone(env, 0, 1, 9, nodeId);

        // the restore write into the inline pile gone gets error as the subrequest of the inline pile, the read is
        // answered and the node keeps running
        env.Runtime->Send(heldResult.release(), nodeId);
        env.WaitForEdgeActorEvent<TEvBlobStorage::TEvGetResult>(edge, false);

        env.Runtime->Send(physicalRequest.release(), nodeId);
        const auto physicalResult = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvGetResult>(physicalEdge);
        UNIT_ASSERT_VALUES_EQUAL(physicalResult->Get()->Status, NKikimrProto::ERROR);
        UNIT_ASSERT_STRING_CONTAINS(physicalResult->Get()->ErrorReason, "physical pile");

        // the groups of the converted cluster are pure bridge proxy groups now
        for (const auto& group : config.GetBlobStorageConfig().GetServiceSet().GetGroups()) {
            if (group.HasBridgeGroupState()) {
                UNIT_ASSERT_C(!group.RingsSize() && !group.HasBridgeProxyGroupId(), group.DebugString());
            }
        }
        const auto info = GetGroupInfo(env, groupId);
        UNIT_ASSERT_C(!info.RingsSize() && !info.HasBridgeProxyGroupId(), info.DebugString());
        UNIT_ASSERT_C(NBridge::GetGroupPile(info.GetBridgeGroupState(), TBridgePileId::FromPileIndex(1)),
            info.DebugString());
        checker.Write(groupId, nodeId);
        checker.CheckAll(nodeId);
        CheckPileData(env, checker, 1, nodeId);

        // nodes of the remaining pile keep working after restarts
        env.RestartNode(11);
        env.Sim(TDuration::Seconds(30));
        checker.Write(groupId, nodeId);
        checker.CheckAll(nodeId);
    }

    // notification of BSC delivered late (e.g. retried after BSC restart) can't finish another operation on the same pile
    Y_UNIT_TEST(StaleNotificationRefused) {
        TEnvironmentSetup env(MakeExpandableSettings());
        CreatePoolAndGetGroup(env);
        auto res = AddThirdPileToConfig(env);
        UNIT_ASSERT_EQUAL_C(res.GetStatus(), TResult::OK, res.GetErrorReason());
        env.Sim(TDuration::Seconds(10));

        std::optional<NKikimrBlobStorage::TEvNodeConfigInvokeOnRoot> notification;
        bool hold = false;
        env.Runtime->FilterFunction = [&](ui32 /*nodeId*/, std::unique_ptr<IEventHandle>& ev) {
            if (const auto *record = GetBridgePileOperationNotification(*ev)) {
                if (!notification) {
                    notification.emplace(*record);
                }
                return !hold;
            }
            return true;
        };
        AddPileAndWait(env, "pile_3");
        UNIT_ASSERT(notification);
        RemovePileAndWait(env, "pile_3");

        // the same pile is being added again, while the notification of the first addition comes once more
        hold = true;
        res = AddPile(env, "pile_3");
        UNIT_ASSERT_EQUAL_C(res.GetStatus(), TResult::OK, res.GetErrorReason());
        env.Sim(TDuration::Seconds(10));
        hold = false; // BSC doesn't repeat the dropped notification until restarted
        ExpectError(InvokeDistconf(env, [&](auto& record) { record.CopyFrom(*notification); }),
            "Pending pile operation is another one");
        const auto config = QueryStorageConfig(env);
        const auto& details = config.GetClusterStateDetails();
        UNIT_ASSERT(details.HasPendingPileOperation());
        UNIT_ASSERT_UNEQUAL(details.GetPendingPileOperation().GetOperationId(),
            notification->GetNotifyBridgePileOperation().GetOperationId());

        // BSC (residing on node 1) performs its part again after restart, as the notification has been dropped
        env.RestartNode(1);
        WaitPileOperationFinished(env);
        UNIT_ASSERT_VALUES_EQUAL(CheckPileParts(env, 2, FirstThirdPileNodeId, LastThirdPileNodeId), 1);
    }

    // two piles of three fail at once and the remaining one removes them one by one, though the nodes of the failed
    // piles can't report anything (they wouldn't be let in by the remaining pile even if they were running)
    Y_UNIT_TEST(RemoveTwoFailedPiles) {
        TEnvironmentSetup env{{
            .NodeCount = 24,
            .Erasure = TBlobStorageGroupType::Erasure4Plus2Block,
            .LocationGenerator = GetBridgedLocation,
            .SelfManagementConfig = true,
            .NumPiles = 3,
            .AutomaticBootstrap = true,
        }};
        const ui32 groupId = CreatePoolAndGetGroup(env);
        TDataChecker checker(env);
        checker.Write(groupId);

        auto clusterState = QueryStorageConfig(env).GetClusterState();
        UNIT_ASSERT_VALUES_EQUAL(TBridgePileId::FromProto(&clusterState,
            &NKikimrBridge::TClusterState::GetPrimaryPile).GetPileIndex(), 0);
        for (ui32 nodeId = 9; nodeId <= 24; ++nodeId) {
            env.StopNode(nodeId);
        }
        clusterState.SetGeneration(clusterState.GetGeneration() + 1);
        clusterState.SetPerPileState(1, NKikimrBridge::TClusterState::DISCONNECTED);
        clusterState.SetPerPileState(2, NKikimrBridge::TClusterState::DISCONNECTED);
        // the nodes of the remaining pile may be still rebinding after losing their parents
        const TInstant deadline = env.Runtime->GetClock() + TDuration::Minutes(2);
        for (;;) {
            const auto res = InvokeDistconf(env, [&](auto& record) {
                record.MutableSwitchBridgeClusterState()->MutableNewClusterState()->CopyFrom(clusterState);
            });
            if (res.GetStatus() == TResult::OK) {
                break;
            }
            UNIT_ASSERT_C((res.GetStatus() == TResult::NO_QUORUM || res.GetStatus() == TResult::RACE) &&
                env.Runtime->GetClock() < deadline, res.DebugString());
            env.Sim(TDuration::Seconds(1));
        }
        env.Sim(TDuration::Seconds(30));

        RemovePileAndWait(env, "pile_2");
        CheckPileGone(env, 1, 9, 16);
        RemovePileAndWait(env, "pile_3");
        CheckPileGone(env, 2, 17, 24);
        UNIT_ASSERT_VALUES_EQUAL(GetPileStates(QueryStorageConfig(env)),
            "pile_1:SYNCHRONIZED pile_2:INACTIVE pile_3:INACTIVE");
        checker.Write(groupId);
        checker.CheckAll();
    }

    // only DISCONNECTED pile without pending operation can be removed
    Y_UNIT_TEST(Refusals) {
        TEnvironmentSetup env(MakeExpandableSettings());
        const ui32 groupId = CreatePoolAndGetGroup(env);
        TDataChecker checker(env);
        checker.Write(groupId);
        auto res = AddThirdPileToConfig(env);
        UNIT_ASSERT_EQUAL_C(res.GetStatus(), TResult::OK, res.GetErrorReason());
        env.Sim(TDuration::Seconds(10));

        ExpectError(RemovePile(env, "pile_4"), "is not listed in BridgeConfig");
        ExpectError(RemovePile(env, "pile_1"), "has to be DISCONNECTED to be removed from the cluster, its state is"
            " SYNCHRONIZED");
        ExpectError(RemovePile(env, "pile_3"), "its state is INACTIVE");

        // other piles can't be removed until BSC finishes adding the pile (its own removal cancels the addition, see
        // BridgeAddPile::StuckAdditionCancelled)
        bool hold = true;
        env.Runtime->FilterFunction = [&](ui32 /*nodeId*/, std::unique_ptr<IEventHandle>& ev) {
            return !hold || !GetBridgePileOperationNotification(*ev);
        };
        res = AddPile(env, "pile_3");
        UNIT_ASSERT_EQUAL_C(res.GetStatus(), TResult::OK, res.GetErrorReason());
        env.Sim(TDuration::Seconds(10));
        SwitchPileState(env, 1, NKikimrBridge::TClusterState::DISCONNECTED);
        ExpectError(RemovePile(env, "pile_2"), "Operation on pile 'pile_3' is in progress");
        // the notification has been dropped, BSC (residing on node 1) performs its part again after restart
        hold = false;
        env.RestartNode(1);
        WaitPileOperationFinished(env);

        // nothing has been removed by the refused commands
        UNIT_ASSERT_VALUES_EQUAL(GetPileStates(QueryStorageConfig(env)),
            "pile_1:SYNCHRONIZED pile_2:DISCONNECTED pile_3:DISCONNECTED");
        SwitchPileState(env, 1, NKikimrBridge::TClusterState::NOT_SYNCHRONIZED_1);
        WaitForPileState(env, 1, NKikimrBridge::TClusterState::SYNCHRONIZED);
        checker.Write(groupId);
        checker.CheckAll();
        CheckPileData(env, checker, 1, 9);
    }

    // distconf finishes removal only when BSC has deleted parts of its groups; the pile can't change its state meanwhile,
    // new groups get no parts in it, and BSC repeats its part after restart
    Y_UNIT_TEST(RemovalWaitsForBsc) {
        TEnvironmentSetup env(MakeExpandableSettings());
        const ui32 groupId = CreatePoolAndGetGroup(env);
        TDataChecker checker(env);
        checker.Write(groupId);
        auto res = AddThirdPileToConfig(env);
        UNIT_ASSERT_EQUAL_C(res.GetStatus(), TResult::OK, res.GetErrorReason());
        env.Sim(TDuration::Seconds(10));
        AddPileAndWait(env, "pile_3");

        bool hold = true;
        ui32 numNotifications = 0;
        env.Runtime->FilterFunction = [&](ui32 /*nodeId*/, std::unique_ptr<IEventHandle>& ev) {
            if (GetBridgePileOperationNotification(*ev)) {
                ++numNotifications;
                return !hold;
            }
            return true;
        };
        res = RemovePile(env, "pile_3");
        UNIT_ASSERT_EQUAL_C(res.GetStatus(), TResult::OK, res.GetErrorReason());
        env.Sim(TDuration::Seconds(30));
        UNIT_ASSERT(numNotifications);
        auto config = QueryStorageConfig(env);
        UNIT_ASSERT_VALUES_EQUAL(GetPileStates(config), "pile_1:SYNCHRONIZED pile_2:SYNCHRONIZED pile_3:DISCONNECTED");
        UNIT_ASSERT_EQUAL(config.GetClusterStateDetails().GetPendingPileOperation().GetKind(),
            NKikimrBridge::TClusterStateDetails::TPendingPileOperation::REMOVE);
        // nodes of older versions can't work with the cluster removing piles
        UNIT_ASSERT(config.GetClusterStateDetails().GetPileRegistry().GetPileListChanged());
        UNIT_ASSERT_VALUES_EQUAL(config.GetClusterStateDetails().GetPileRegistry().GetRequiredFeatureLevel(),
            static_cast<ui32>(NStorage::BridgeFeatureLevelPileOps));
        // BSC has already deleted its parts, while static ones are still there
        const auto pileId = TBridgePileId::FromPileIndex(2);
        UNIT_ASSERT(!NBridge::GetGroupPile(GetGroupInfo(env, groupId).GetBridgeGroupState(), pileId));
        for (const auto& group : config.GetBlobStorageConfig().GetServiceSet().GetGroups()) {
            if (group.HasBridgeGroupState()) {
                UNIT_ASSERT(NBridge::GetGroupPile(group.GetBridgeGroupState(), pileId));
            }
        }

        ExpectError(TrySwitchPileState(env, 2, NKikimrBridge::TClusterState::NOT_SYNCHRONIZED_1),
            "is being removed from the cluster");
        ExpectError(AddPile(env, "pile_3"), "Operation on pile 'pile_3' is in progress");

        // new groups get no parts in the pile being removed
        CreateAnotherPool(env, 2);
        env.Sim(TDuration::Seconds(30));
        const auto baseConfig = env.FetchBaseConfig(); // groups are referenced by pointers
        const auto poolGroups = GetPoolGroups(baseConfig, 2);
        UNIT_ASSERT(!poolGroups.empty());
        for (const auto *group : poolGroups) {
            if (group->GetIsProxyGroup()) {
                const auto info = GetGroupInfo(env, group->GetGroupId());
                UNIT_ASSERT_VALUES_EQUAL_C(info.GetBridgeGroupState().PileSize(), 2, info.DebugString());
                checker.Write(group->GetGroupId());
            }
        }
        checker.Write(groupId);
        checker.CheckAll();

        // BSC (residing on node 1) is restarted, it performs its part again finding everything done already
        hold = false;
        env.RestartNode(1);
        WaitPileOperationFinished(env);
        CheckPileGone(env, 2, FirstThirdPileNodeId, LastThirdPileNodeId);
        checker.Write(groupId);
        checker.CheckAll();
    }

    // drives of the removed pile get ACTIVE status back, as nobody can tell how they are until the pile is added again;
    // INACTIVE ones (CMS sentinel makes them so when their nodes go away) would keep the pile from being added
    Y_UNIT_TEST(RemovedPileDrivesActiveAgain) {
        TEnvironmentSetup env(MakeExpandableSettings());
        const ui32 groupId = CreatePoolAndGetGroup(env);
        TDataChecker checker(env);
        checker.Write(groupId);
        auto res = AddThirdPileToConfig(env);
        UNIT_ASSERT_EQUAL_C(res.GetStatus(), TResult::OK, res.GetErrorReason());
        env.Sim(TDuration::Seconds(10));
        StartPileNodes(env, "pile_3", FirstThirdPileNodeId, LastThirdPileNodeId);
        AddPileAndWait(env, "pile_3");
        RejoinPile(env, 2);

        auto forEachPileDrive = [&](const std::function<void(const NKikimrBlobStorage::TBaseConfig::TPDisk&)>& callback) {
            size_t numDrives = 0;
            const auto baseConfig = env.FetchBaseConfig();
            for (const auto& pdisk : baseConfig.GetPDisk()) {
                if (FirstThirdPileNodeId <= pdisk.GetNodeId() && pdisk.GetNodeId() <= LastThirdPileNodeId) {
                    callback(pdisk);
                    ++numDrives;
                }
            }
            UNIT_ASSERT(numDrives);
        };

        SwitchPileState(env, 2, NKikimrBridge::TClusterState::DISCONNECTED);
        forEachPileDrive([&](const auto& pdisk) {
            env.UpdateDriveStatus(pdisk.GetNodeId(), pdisk.GetPDiskId(), NKikimrBlobStorage::EDriveStatus::INACTIVE, {},
                true);
        });
        RemovePileAndWait(env, "pile_3");
        CheckPileGone(env, 2, FirstThirdPileNodeId, LastThirdPileNodeId);
        forEachPileDrive([&](const auto& pdisk) {
            UNIT_ASSERT_EQUAL_C(pdisk.GetDriveStatus(), NKikimrBlobStorage::EDriveStatus::ACTIVE, pdisk.DebugString());
        });

        // status change as CMS sentinel makes it, judging the drive by what it sees of it
        auto updateObservedStatus = [&](ui32 nodeId, ui32 pdiskId, NKikimrBlobStorage::EDriveStatus status) {
            NKikimrBlobStorage::TConfigRequest request;
            request.SetIgnoreGroupFailModelChecks(true);
            request.SetIgnoreDegradedGroupsChecks(true);
            request.SetIgnoreDisintegratedGroupsChecks(true);
            auto *cmd = request.AddCommand()->MutableUpdateDriveStatus();
            cmd->MutableHostKey()->SetNodeId(nodeId);
            cmd->SetPDiskId(pdiskId);
            cmd->SetStatus(status);
            cmd->SetSkipIfPileCutOff(true);
            const auto response = env.Invoke(request);
            UNIT_ASSERT_C(response.GetSuccess(), response.GetErrorDescription());
        };

        // the stale change comes after the removal (e.g. the sentinel retries the one judged before the pile went
        // away): the drives of the pile cut off from the cluster keep their status
        forEachPileDrive([&](const auto& pdisk) {
            updateObservedStatus(pdisk.GetNodeId(), pdisk.GetPDiskId(), NKikimrBlobStorage::EDriveStatus::INACTIVE);
        });
        forEachPileDrive([&](const auto& pdisk) {
            UNIT_ASSERT_EQUAL_C(pdisk.GetDriveStatus(), NKikimrBlobStorage::EDriveStatus::ACTIVE, pdisk.DebugString());
        });

        // while the drives of connected piles get it
        std::optional<std::tuple<ui32, ui32>> connectedDrive;
        const auto baseConfig = env.FetchBaseConfig();
        for (const auto& pdisk : baseConfig.GetPDisk()) {
            if (pdisk.GetNodeId() == 1) {
                connectedDrive.emplace(pdisk.GetNodeId(), pdisk.GetPDiskId());
            }
        }
        UNIT_ASSERT(connectedDrive);
        const auto& [connectedNodeId, connectedPDiskId] = *connectedDrive;
        updateObservedStatus(connectedNodeId, connectedPDiskId, NKikimrBlobStorage::EDriveStatus::INACTIVE);
        bool found = false;
        const auto updatedBaseConfig = env.FetchBaseConfig();
        for (const auto& pdisk : updatedBaseConfig.GetPDisk()) {
            if (pdisk.GetNodeId() == connectedNodeId && pdisk.GetPDiskId() == connectedPDiskId) {
                UNIT_ASSERT_EQUAL_C(pdisk.GetDriveStatus(), NKikimrBlobStorage::EDriveStatus::INACTIVE, pdisk.DebugString());
                found = true;
            }
        }
        UNIT_ASSERT(found);
        env.UpdateDriveStatus(connectedNodeId, connectedPDiskId, NKikimrBlobStorage::EDriveStatus::ACTIVE, {}, true);

        AddPileAndWait(env, "pile_3");
        UNIT_ASSERT_VALUES_EQUAL(CheckPileParts(env, 2, FirstThirdPileNodeId, LastThirdPileNodeId), 1);
        RejoinPile(env, 2);
        checker.Write(groupId);
        checker.CheckAll();
        CheckPileData(env, checker, 2, FirstThirdPileNodeId);
    }
}
