#include "bridge_pile_ops.h"

#include <ydb/core/base/hive.h>
#include <ydb/core/blobstorage/base/bridge_events.h>
#include <ydb/core/blobstorage/base/blobstorage_console_events.h>
#include <ydb/core/blobstorage/ut_blobstorage/lib/env.h>

#if !defined(KIKIMR_DISABLE_S3_OPS) && !defined(_win_)
#include <ydb/core/wrappers/fake_storage.h>
#endif

#include <util/generic/scope.h>

using namespace NBridgeTest;

namespace {

    using TResult = NKikimrBlobStorage::TEvNodeConfigInvokeOnRootResult;

    // make explicit host config for existing hosts and add another one, which has drives with one slot only
    TResult AddSingleSlotHostConfig(TEnvironmentSetup& env) {
        return ReplaceMainConfig(env, [](YAML::Node& config) {
            config["host_configs"][0]["host_config_id"] = 1;
            for (YAML::Node host : config["hosts"]) {
                host["host_config_id"] = 1;
            }
            YAML::Node drive;
            drive["path"] = "SectorMap:X:1000";
            drive["type"] = "NVME";
            drive["expected_slot_count"] = 1;
            YAML::Node hostConfig;
            hostConfig["host_config_id"] = 2;
            hostConfig["drive"].push_back(drive);
            config["host_configs"].push_back(hostConfig);
        });
    }

#if !defined(KIKIMR_DISABLE_S3_OPS) && !defined(_win_)
    void AddBackupPileToConfig(TEnvironmentSetup& env, const TString& bucket, const TString& secretKey) {
        auto add = [&](YAML::Node& config) {
            auto pile = YAML::Load(R"(
name: pile_3
type: BACKUP
backup:
  channel_profiles:
    - {storage_pool_name: backup-local, channel_kind: System, count: 2}
    - {storage_pool_name: backup-local, channel_kind: Data, count: 2}
  s3:
    settings: {endpoint: fake.fake, access_key: test-key, object_key_pattern: bridge}
    async_mode: {max_pending_bytes: 1073741824}
)");
            pile["backup"]["s3"]["settings"]["bucket"] = bucket;
            pile["backup"]["s3"]["settings"]["secret_key"] = secretKey;
            config["bridge_config"]["piles"].push_back(pile);
            AddPileHosts(config, "pile_3", FirstThirdPileNodeId, LastThirdPileNodeId);
        };
        WaitUntil(env, [&] {
            const auto response = ReplaceMainConfig(env, add);
            if (response.GetStatus() == TResult::OK) {
                return true;
            }
            UNIT_ASSERT_EQUAL_C(response.GetStatus(), TResult::ERROR, response.ShortDebugString());
            UNIT_ASSERT_STRING_CONTAINS(response.GetErrorReason(), "Some static nodes did not report their BridgeConfig");
            return false;
        }, "all static nodes report their bridge configuration", TDuration::Seconds(1), 30);
    }

#endif
} // anonymous

Y_UNIT_TEST_SUITE(BridgeAddPile) {
#if !defined(KIKIMR_DISABLE_S3_OPS) && !defined(_win_)
    Y_UNIT_TEST(BackupPileJoinsExportsAndBecomesPrimary) {
        const TString bucket = "bridge-backup-pile";
        const TString secretKey = "bridge-backup-key";
        auto* s3 = Singleton<NWrappers::NExternalStorage::TFakeExternalStorage>();
        s3->Clear();
        s3->SetSecretKey(secretKey);

        TFeatureFlags flags;
        flags.SetEnableBlobDepotDataCooling(true);
        const ui8 keyData[32] = "bridge backup test key";
        const TEncryptionKey key{
            .Key = {keyData, sizeof(keyData)},
            .Version = 1,
            .Id = "backup test key",
        };
        TEnvironmentSetup env({
            .NodeCount = 24,
            .Erasure = TBlobStorageGroupType::Erasure4Plus2Block,
            .Encryption = true,
            .ConfigPreprocessor = [key](ui32, TNodeWardenConfig& config) {
                config.TenantKey = key;
                // Let Wardens run the generated per-pile StateStorage rings across the loss of node 1.
                config.DomainsConfig = std::make_unique<NKikimrConfig::TDomainsConfig>();
            },
            .LocationGenerator = GetBridgedLocation,
            .SetupHive = true,
            .FeatureFlags = flags,
            .SelfManagementConfig = true,
            .NumPiles = 2,
            .AutomaticBootstrap = true,
            .InitialConfiguredNodeCount = 16,
        });
        const ui32 groupId = CreatePoolAndGetGroup(env);
        TDataChecker data(env);
        data.Write(groupId);

        AddBackupPileToConfig(env, bucket, secretKey);
        env.Sim(TDuration::Seconds(10));

        CreateAnotherPool(env, 2, [](auto& pool) {
            pool.SetName("backup-local");
            pool.SetKind("backup-local");
            pool.SetBridgeMode(false);
            pool.SetBackupPileName("pile_3");
            pool.SetEncryptionMode(TBlobStorageGroupInfo::EEM_NONE);
            pool.ClearPDiskFilter();
            auto* filter = pool.AddPDiskFilter();
            filter->AddProperty()->SetType(NKikimrBlobStorage::NVME);
            filter->AddProperty()->MutableLocation()->SetBridgePileName("pile_3");
        });
        const auto registered = QueryStorageConfig(env).GetClusterStateDetails().GetPileRegistry().GetPiles(2);
        auto* localPile = env.Runtime->GetAppDataBridgeConfig().AddPiles();
        localPile->SetName(registered.GetName());
        localPile->SetType(registered.GetType());
        localPile->MutableBackup()->CopyFrom(registered.GetBackup());
        env.ConfiguredNodeCount = LastThirdPileNodeId;
        for (ui32 nodeId = FirstThirdPileNodeId; nodeId <= LastThirdPileNodeId; ++nodeId) {
            env.StartNode(nodeId);
        }
        env.Sim(TDuration::Seconds(30));
        AddPileAndWait(env, "pile_3");

        const auto baseConfig = env.FetchBaseConfig();
        const auto& groups = baseConfig.GetGroup();
        const auto backup = std::find_if(groups.begin(), groups.end(), [](const auto& group) {
            return group.HasVirtualGroupInfo();
        });
        UNIT_ASSERT(backup != groups.end());
        UNIT_ASSERT(backup->GetGroupId() != groupId);
        UNIT_ASSERT_VALUES_EQUAL(backup->VSlotIdSize(), 0);
        RejoinPile(env, 2);
        WaitUntil(env, [&] { return !s3->GetBucket(bucket).GetObjectKeys().empty(); }, "backup pile exports synchronized data");

        for (const ui64 hotLimit : {512_MB, 256_MB}) {
            const auto update = ReplaceMainConfig(env, [&](YAML::Node& config) {
                config["bridge_config"]["piles"][2]["backup"]["s3"]["async_mode"]["max_pending_bytes"] = hotLimit;
            });
            UNIT_ASSERT_EQUAL_C(update.GetStatus(), TResult::OK, update.GetErrorReason());
        }
        const auto revision = QueryStorageConfig(env).GetClusterStateDetails().GetPileRegistry().GetPiles(2)
            .GetBackupConfigRevision();
        UNIT_ASSERT(revision > registered.GetBackupConfigRevision());
        const auto queryEdge = env.Runtime->AllocateEdgeActor(1, __FILE__, __LINE__);
        WaitUntil(env, [&] {
            auto request = std::make_unique<TEvBlobStorage::TEvControllerQueryBridgeStorage>();
            request->Record.SetView(NKikimrBridge::TStorageQuery::GROUPS);
            request->Record.SetPileName("pile_3");
            env.Runtime->SendToPipe(env.TabletId, queryEdge, request.release(), 0,
                TTestActorSystem::GetPipeConfigWithRetries());
            const auto response = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvControllerQueryBridgeStorageResult>(
                queryEdge, false, env.Runtime->GetClock() + TDuration::Seconds(30));
            UNIT_ASSERT(response);
            const auto& rows = response->Get()->Record.GetGroups().groups();
            return std::ranges::any_of(rows, [&](const auto& row) {
                return row.group_id() == groupId && row.has_virtual_group_id() &&
                    row.applied_config_revision() == revision && row.desired_config_revision() == revision;
            });
        }, "newest live backup profile is applied");

        const ui32 backupNode = FirstThirdPileNodeId;
        const auto& [writtenGroup, blobId, expected] = data.GetWritten().front();
        UNIT_ASSERT_VALUES_EQUAL(writtenGroup, groupId);
        for (const auto& objectKey : s3->GetBucket(bucket).GetObjectKeys()) {
            const auto object = s3->GetBucket(bucket).GetObject(objectKey);
            UNIT_ASSERT(object);
            UNIT_ASSERT(!object->Contains(expected));
        }
        const auto edge = env.Runtime->AllocateEdgeActor(backupNode, __FILE__, __LINE__);
        TString race;
        WaitUntil(env, [&] {
            const auto groupInfo = GetGroupInfo(env, groupId);
            const auto& member = groupInfo.GetBridgeGroupState().GetPile(2);
            UNIT_ASSERT_VALUES_EQUAL(member.GetGroupId(), backup->GetGroupId());
            const auto deadline = env.Runtime->GetClock() + TDuration::Seconds(30);
            env.Runtime->WrapInActorContext(edge, [&] {
                auto get = std::make_unique<TEvBlobStorage::TEvGet>(blobId, 1, 3, deadline, NKikimrBlobStorage::FastRead);
                get->ForceGroupGeneration = member.GetGroupGeneration();
                SendToBSProxy(edge, member.GetGroupId(), get.release());
            });
            const auto read = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvGetResult>(edge, false, deadline);
            UNIT_ASSERT(read);
            if (read->Get()->Status == NKikimrProto::RACE) {
                race = TStringBuilder() << "parent member generation " << member.GetGroupGeneration()
                    << ", child generation " << GetGroupInfo(env, member.GetGroupId()).GetGroupGeneration()
                    << ", node warden generation " << GetNodeGroupGeneration(env, member.GetGroupId(), backupNode)
                    << ", reported generation " << read->Get()->RacingGeneration << ": " << read->Get()->ErrorReason;
                return false; // the direct request raced the node's or the tablet's generation update
            }
            CheckGetResult(*read->Get(), expected.substr(1, 3));
            return true;
        }, "cold read at the current backup group generation", TDuration::MilliSeconds(100), 100, [&] { return race; });

        auto next = QueryStorageConfig(env).GetClusterState();
        TBridgePileId::FromPileIndex(2).CopyToProto(&next, &NKikimrBridge::TClusterState::SetPrimaryPile);
        TBridgePileId::FromPileIndex(2).CopyToProto(&next, &NKikimrBridge::TClusterState::SetPromotedPile);
        next.SetGeneration(next.GetGeneration() + 1);
        const auto promotion = InvokeDistconf(env, [&](auto& record) {
            record.MutableSwitchBridgeClusterState()->MutableNewClusterState()->CopyFrom(next);
        });
        UNIT_ASSERT_EQUAL_C(promotion.GetStatus(), TResult::OK, promotion.GetErrorReason());
        data.Write(groupId, backupNode);
        data.CheckAll(backupNode);

        auto waitForBackupConfig = [&](const NKikimrBridge::TClusterState& expected) {
            const auto deadline = env.Runtime->GetClock() + TDuration::Seconds(30);
            for (ui32 nodeId = backupNode; nodeId <= LastThirdPileNodeId; ++nodeId) {
                const auto local = env.Runtime->AllocateEdgeActor(nodeId, __FILE__, __LINE__);
                const auto warden = MakeBlobStorageNodeWardenID(nodeId);
                env.Runtime->Send(new IEventHandle(warden, local, new TEvNodeWardenQueryStorageConfig(true)), nodeId);
                for (;;) {
                    const auto reply = env.WaitForEdgeActorEvent<TEvNodeWardenStorageConfig>(local, false, deadline);
                    UNIT_ASSERT_C(reply && reply->Get()->Config, "No failover configuration on node " << nodeId);
                    const auto& current = reply->Get()->Config->GetClusterState();
                    if (current.GetGeneration() >= expected.GetGeneration() &&
                            current.GetPrimaryPile() == expected.GetPrimaryPile() &&
                            current.GetPerPileState(0) == expected.GetPerPileState(0) &&
                            current.GetPerPileState(1) == expected.GetPerPileState(1)) {
                        const auto& config = *reply->Get()->Config;
                        for (const auto* component : {&config.GetStateStorageConfig(), &config.GetStateStorageBoardConfig(),
                                &config.GetSchemeBoardConfig()}) {
                            bool hasBackupRing = false;
                            for (const auto& ring : component->GetRingGroups()) {
                                const ui32 pile = TBridgePileId::FromProto(&ring,
                                    &NKikimrConfig::TStateStorageConfig::TRing::GetBridgePileId).GetPileIndex();
                                if (pile == 2) {
                                    hasBackupRing = true;
                                    UNIT_ASSERT_EQUAL_C(ring.GetPileState(), NKikimrConfig::TStateStorageConfig::PRIMARY,
                                        ring.ShortDebugString());
                                } else if (expected.GetPerPileState(pile) == NKikimrBridge::TClusterState::DISCONNECTED) {
                                    UNIT_ASSERT_EQUAL_C(ring.GetPileState(), NKikimrConfig::TStateStorageConfig::DISCONNECTED,
                                        ring.ShortDebugString());
                                }
                            }
                            UNIT_ASSERT_C(hasBackupRing, component->ShortDebugString());
                        }
                        break;
                    }
                }
                env.Runtime->Send(new IEventHandle(TEvents::TSystem::Unsubscribe, 0, warden, local, nullptr, 0), nodeId);
            }
        };
        waitForBackupConfig(next);
        const ui32 regularMember = GetGroupInfo(env, groupId).GetBridgeGroupState().GetPile(1).GetGroupId();
        auto isolated = QueryStorageConfig(env, backupNode).GetClusterState();
        isolated.SetPerPileState(0, NKikimrBridge::TClusterState::DISCONNECTED);
        isolated.SetPerPileState(1, NKikimrBridge::TClusterState::DISCONNECTED);
        isolated.SetGeneration(isolated.GetGeneration() + 1);
        // Bound the immediate retries of the fixture's pinned system tablet bootstrappers during the transition.
        std::vector<std::pair<ui32, std::unique_ptr<IEventHandle>>> deaths;
        env.Runtime->FilterFunction = [&](ui32 nodeId, std::unique_ptr<IEventHandle>& event) {
            if (event->GetTypeRewrite() == TEvTablet::TEvTabletDead::EventType) {
                const auto tablet = event->Get<TEvTablet::TEvTabletDead>()->TabletID;
                if (tablet == env.TabletId || tablet == MakeDefaultHiveID()) {
                    deaths.emplace_back(nodeId, std::move(event));
                    return false;
                }
            }
            return true;
        };
        Y_DEFER { env.Runtime->FilterFunction = {}; };
        for (ui32 nodeId = 1; nodeId < backupNode; ++nodeId) {
            env.StopNode(nodeId);
        }
        WaitUntil(env, [&] {
            const auto response = InvokeDistconf(env, [&](auto& record) {
                record.MutableSwitchBridgeClusterState()->MutableNewClusterState()->CopyFrom(isolated);
            }, backupNode, env.Runtime->GetClock() + TDuration::Seconds(30));
            if (response.GetStatus() == TResult::NO_QUORUM || response.GetStatus() == TResult::RACE) {
                return false;
            }
            UNIT_ASSERT_EQUAL_C(response.GetStatus(), TResult::OK, response.ShortDebugString());
            return true;
        }, "disconnect both failed regular piles", TDuration::Seconds(1), 120);

        waitForBackupConfig(isolated);
        env.Runtime->FilterFunction = {};
        for (auto& [nodeId, event] : deaths) {
            env.Runtime->Send(event.release(), nodeId);
        }
        // The test fixture pins both system tablet bootstrappers to the lost first node.
        env.Runtime->CreateTestBootstrapper(TTestActorSystem::CreateTestTabletInfo(env.TabletId,
            TTabletTypes::BSController, env.Settings.Erasure.GetErasure(), env.GroupId, 3),
            &CreateFlatBsController, backupNode);
        env.Runtime->CreateTestBootstrapper(TTestActorSystem::CreateTestTabletInfo(MakeDefaultHiveID(),
            TTabletTypes::Hive, env.Settings.Erasure.GetErasure(), env.GroupId, 3), &CreateDefaultHive, backupNode);

        std::map<ui64, TString> lookups;
        std::map<ui64, TString> bootErrors;
        Y_DEFER { env.Runtime->FilterFunction = {}; };
        env.Runtime->FilterFunction = [&](ui32, std::unique_ptr<IEventHandle>& event) {
            if (event->GetTypeRewrite() == TEvStateStorage::EvInfo) {
                const auto* lookup = event->Get<TEvStateStorage::TEvInfo>();
                if (lookup->TabletID == env.TabletId || lookup->TabletID == MakeDefaultHiveID()) {
                    lookups[lookup->TabletID] = lookup->ToString();
                }
            } else if (event->GetTypeRewrite() == TEvTablet::EvTabletDead) {
                const auto* death = event->Get<TEvTablet::TEvTabletDead>();
                bootErrors[death->TabletID] = TStringBuilder() << "death reason " << ui32(death->Reason);
            }
            return true;
        };
        auto queryController = [&] {
            const auto caller = env.Runtime->AllocateEdgeActor(backupNode, __FILE__, __LINE__);
            auto request = std::make_unique<TEvBlobStorage::TEvControllerGetGroup>();
            request->Record.AddGroupIDs(groupId);
            env.Runtime->SendToPipe(env.TabletId, caller, request.release(), Max<ui64>(),
                TTestActorSystem::GetPipeConfigWithRetries());
            return env.WaitForEdgeActorEvent<TEvBlobStorage::TEvControllerNodeServiceSetUpdate>(caller, false,
                env.Runtime->GetClock() + TDuration::Minutes(2));
        };
        const auto bscCaller = env.Runtime->AllocateEdgeActor(backupNode, __FILE__, __LINE__);
        auto configRequest = std::make_unique<TEvBlobStorage::TEvControllerConfigRequest>();
        auto* pool = configRequest->Record.MutableRequest()->AddCommand()->MutableReadStoragePool();
        pool->SetBoxId(1);
        pool->AddStoragePoolId(1);
        env.Runtime->SendToPipe(env.TabletId, bscCaller, configRequest.release(), 0,
            TTestActorSystem::GetPipeConfigWithRetries());
        const auto bscReply = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvControllerConfigResponse>(bscCaller, false,
            env.Runtime->GetClock() + TDuration::Minutes(2));
        UNIT_ASSERT_C(bscReply && bscReply->Get()->Record.GetResponse().GetSuccess(),
            "BSC did not serve its storage pool on the isolated backup pile " << lookups[env.TabletId] << ' ' << bootErrors[env.TabletId]);
        const auto hiveCaller = env.Runtime->AllocateEdgeActor(backupNode, __FILE__, __LINE__);
        env.Runtime->SendToPipe(MakeDefaultHiveID(), hiveCaller, new TEvHive::TEvRequestHiveNodeStats, 0,
            TTestActorSystem::GetPipeConfigWithRetries());
        UNIT_ASSERT_C(env.WaitForEdgeActorEvent<TEvHive::TEvResponseHiveNodeStats>(hiveCaller, false,
            env.Runtime->GetClock() + TDuration::Minutes(2)), "Hive did not boot on the isolated backup pile "
            << lookups[MakeDefaultHiveID()] << ' ' << bootErrors[MakeDefaultHiveID()]);
        env.Runtime->FilterFunction = {};
        data.SendPut(groupId, edge);
        const auto put = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvPutResult>(edge, false,
            env.Runtime->GetClock() + TDuration::Minutes(2));
        UNIT_ASSERT_C(put, "Timed out writing to the backup as the only available pile");
        UNIT_ASSERT_VALUES_EQUAL_C(put->Get()->Status, NKikimrProto::OK, put->Get()->ToString());
        const auto& [_, recoveryId, recoveryData] = data.GetWritten().back();
        auto readRecoveredBlob = [&](ui32 nodeId, ui32 targetGroup, std::optional<ui32> generation) {
            const auto reader = env.Runtime->AllocateEdgeActor(nodeId, __FILE__, __LINE__);
            const auto deadline = env.Runtime->GetClock() + TDuration::Seconds(30);
            env.Runtime->WrapInActorContext(reader, [&] {
                auto* request = new TEvBlobStorage::TEvGet(recoveryId, 0, 0, deadline, NKikimrBlobStorage::FastRead);
                request->ForceGroupGeneration = generation;
                SendToBSProxy(reader, targetGroup, request);
            });
            const auto read = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvGetResult>(reader, false, deadline);
            UNIT_ASSERT_C(read, "Timed out reading the backup-only write from group " << targetGroup);
            CheckGetResult(*read->Get(), recoveryData);
        };
        readRecoveredBlob(backupNode, groupId, {});

        constexpr ui32 regularNode = 9;
        std::map<ui32, TString> lastSync;
        Y_DEFER { env.Runtime->FilterFunction = {}; };
        env.Runtime->FilterFunction = [&](ui32 nodeId, std::unique_ptr<IEventHandle>& event) {
            if (event->GetTypeRewrite() == NStorage::TEvNodeWardenNotifySyncerFinished::EventType) {
                const auto* finished = event->Get<NStorage::TEvNodeWardenNotifySyncerFinished>();
                lastSync[finished->TargetGroupId.GetRawId()] = TStringBuilder() << "node " << nodeId
                    << " from " << finished->SourceGroupId << " generation " << finished->BridgeProxyGroupGeneration
                    << " result " << finished->ErrorReason.value_or("OK");
            }
            return true;
        };
        for (ui32 nodeId = regularNode; nodeId < backupNode; ++nodeId) {
            env.StartNode(nodeId);
        }
        auto rejoining = QueryStorageConfig(env, backupNode).GetClusterState();
        rejoining.SetPerPileState(1, NKikimrBridge::TClusterState::NOT_SYNCHRONIZED_1);
        rejoining.SetGeneration(rejoining.GetGeneration() + 1);
        const auto rejoin = InvokeDistconf(env, [&](auto& record) {
            record.MutableSwitchBridgeClusterState()->MutableNewClusterState()->CopyFrom(rejoining);
        }, backupNode, env.Runtime->GetClock() + TDuration::Seconds(30));
        UNIT_ASSERT_EQUAL_C(rejoin.GetStatus(), TResult::OK, rejoin.ShortDebugString());
        WaitUntil(env, [&] {
            return QueryStorageConfig(env, backupNode).GetClusterState().GetPerPileState(1)
                == NKikimrBridge::TClusterState::SYNCHRONIZED;
        }, "regular pile synchronizes from backup", [&] {
            const auto storage = QueryStorageConfig(env, backupNode);
            TStringBuilder description;
            description << "cluster " << storage.GetClusterState().ShortDebugString();
            for (const auto& state : storage.GetClusterStateDetails().GetPileSyncState()) {
                description << " sync state " << state.ShortDebugString();
            }
            for (const auto& [target, status] : lastSync) {
                description << " sync target " << target << ": " << status;
            }
            for (const auto& group : storage.GetBlobStorageConfig().GetServiceSet().GetGroups()) {
                if (group.HasBridgeGroupState()) {
                    description << " static group " << group.GetGroupID() << ' ' << group.GetBridgeGroupState().ShortDebugString();
                }
            }
            if (const auto reply = queryController()) {
                for (const auto& group : reply->Get()->Record.GetServiceSet().GetGroups()) {
                    description << " group state " << group.GetBridgeGroupState().ShortDebugString();
                }
            } else {
                description << " no BSC reply";
            }
            return TString(description);
        });
        readRecoveredBlob(regularNode, regularMember, GetNodeGroupGeneration(env, regularMember, regularNode));
    }

    Y_UNIT_TEST(BackupPilePreflightChecksDedicatedStorageBeforeAddingAnyGroups) {
        TFeatureFlags flags;
        flags.SetEnableBlobDepotDataCooling(false);
        TEnvironmentSetup env({
            .NodeCount = 24,
            .Erasure = TBlobStorageGroupType::Erasure4Plus2Block,
            .LocationGenerator = GetBridgedLocation,
            .SetupHive = true,
            .FeatureFlags = flags,
            .SelfManagementConfig = true,
            .NumPiles = 2,
            .AutomaticBootstrap = true,
            .InitialConfiguredNodeCount = 16,
        });
        AddBackupPileToConfig(env, "backup-preflight", "test-key");
        env.Sim(TDuration::Seconds(10));

        ExpectError(AddPile(env, "pile_3"), "EnableBlobDepotDataCooling");
        const auto edge = env.Runtime->AllocateEdgeActor(env.Settings.ControllerNodeId, __FILE__, __LINE__);
        env.Runtime->WrapInActorContext(edge, [] { AppData()->FeatureFlags.SetEnableBlobDepotDataCooling(true); });
        ExpectError(AddPile(env, "pile_3"), "requires dedicated infrastructure pool backup-local");

        NKikimrBlobStorage::TConfigRequest request;
        auto* pool = request.AddCommand()->MutableDefineStoragePool();
        pool->SetBoxId(1);
        pool->SetStoragePoolId(2);
        pool->SetName("backup-local");
        pool->SetKind("backup-local");
        pool->SetErasureSpecies("block-4-2");
        pool->SetVDiskKind("Default");
        pool->SetNumGroups(1);
        pool->SetBackupPileName("pile_3");
        auto* filter = pool->AddPDiskFilter();
        filter->AddProperty()->SetType(NKikimrBlobStorage::NVME);
        auto* location = filter->AddProperty()->MutableLocation();
        location->SetBridgePileName("pile_2");
        const auto conflict = env.Invoke(request);
        UNIT_ASSERT(!conflict.GetSuccess());
        UNIT_ASSERT_STRING_CONTAINS(conflict.GetErrorDescription(), "different pile");

        pool->ClearPDiskFilter();
        pool->AddPDiskFilter()->AddProperty()->SetType(NKikimrBlobStorage::EPDiskType::NVME);
        const auto defined = env.Invoke(request);
        UNIT_ASSERT_C(defined.GetSuccess(), defined.GetErrorDescription());
        UNIT_ASSERT(GetPoolGroups(env.FetchBaseConfig(), 2).empty());
        UNIT_ASSERT_VALUES_EQUAL(ReadStoragePool(env, 2).GetBackupPileName(), "pile_3");

        AddPileAndWait(env, "pile_3");
        const auto base = env.FetchBaseConfig();
        const auto groups = GetPoolGroups(base, 2);
        UNIT_ASSERT_VALUES_EQUAL(groups.size(), 1);
        UNIT_ASSERT(!groups.front()->HasVirtualGroupInfo());
        UNIT_ASSERT(groups.front()->VSlotIdSize());
        for (const auto& slot : groups.front()->GetVSlotId()) {
            UNIT_ASSERT_LE(FirstThirdPileNodeId, slot.GetNodeId());
            UNIT_ASSERT_LE(slot.GetNodeId(), LastThirdPileNodeId);
        }

        RemovePileAndWait(env, "pile_3");
        UNIT_ASSERT(GetPoolGroups(env.FetchBaseConfig(), 2).empty());
        UNIT_ASSERT_VALUES_EQUAL(ReadStoragePool(env, 2).GetNumGroups(), 1);
        AddPileAndWait(env, "pile_3");
        const auto reprovisioned = env.FetchBaseConfig();
        const auto newGroups = GetPoolGroups(reprovisioned, 2);
        UNIT_ASSERT_VALUES_EQUAL(newGroups.size(), 1);
        UNIT_ASSERT_UNEQUAL(newGroups.front()->GetGroupId(), groups.front()->GetGroupId());
    }

#endif

    Y_UNIT_TEST(GroupCreatedWhileBscCommitIsPending) {
        TEnvironmentSetup env(MakeExpandableSettings());
        CreatePoolAndGetGroup(env);
        auto pool = ReadStoragePool(env, 1);
        pool.SetStoragePoolId(2);
        pool.SetName("pool_2");
        pool.SetKind("pool_2");
        pool.ClearItemConfigGeneration();

        auto res = AddThirdPileToConfig(env);
        UNIT_ASSERT_EQUAL_C(res.GetStatus(), TResult::OK, res.GetErrorReason());
        env.Sim(TDuration::Seconds(10));
        StartPileNodes(env, "pile_3", FirstThirdPileNodeId, LastThirdPileNodeId);

        const TActorId sender = env.Runtime->AllocateEdgeActor(env.Settings.ControllerNodeId, __FILE__, __LINE__);
        std::optional<NKikimrBlobStorage::TConfigResponse> response;
        bool holdCommits = false;
        ui32 numNotifications = 0;
        std::vector<std::pair<ui32, std::unique_ptr<IEventHandle>>> heldCommits;
        env.Runtime->FilterFunction = [&](ui32 nodeId, std::unique_ptr<IEventHandle>& ev) {
            if (ev->GetRecipientRewrite() == sender) {
                if (ev->GetTypeRewrite() == TEvBlobStorage::EvControllerConfigResponse) {
                    response = ev->Get<TEvBlobStorage::TEvControllerConfigResponse>()->Record.GetResponse();
                }
                return false;
            }
            if (ev->GetTypeRewrite() == TEvNodeWardenStorageConfig::EventType) {
                const auto& details = ev->Get<TEvNodeWardenStorageConfig>()->Config->GetClusterStateDetails();
                if (details.HasPendingPileOperation() && details.GetPendingPileOperation().GetKind() ==
                        NKikimrBridge::TClusterStateDetails::TPendingPileOperation::ADD) {
                    if (IActor *actor = env.Runtime->GetActor(ev->GetRecipientRewrite()); actor &&
                            TypeName(*actor).Contains("TBlobStorageController")) {
                        holdCommits = true;
                    }
                }
            }
            if (holdCommits && ev->GetTypeRewrite() == TEvBlobStorage::EvPut &&
                    ev->Get<TEvBlobStorage::TEvPut>()->Id.TabletID() == env.TabletId) {
                heldCommits.emplace_back(nodeId, std::move(ev));
                return false;
            }
            if (GetBridgePileOperationNotification(*ev)) {
                ++numNotifications;
            }
            return true;
        };

        res = AddPile(env, "pile_3");
        UNIT_ASSERT_EQUAL_C(res.GetStatus(), TResult::OK, res.GetErrorReason());
        WaitUntil(env, [&] { return !heldCommits.empty(); }, "BSC commit held", TDuration::MilliSeconds(100));
        env.Sim(TDuration::Seconds(1));
        UNIT_ASSERT_VALUES_EQUAL(numNotifications, 0);

        auto request = std::make_unique<TEvBlobStorage::TEvControllerConfigRequest>();
        request->Record.MutableRequest()->AddCommand()->MutableDefineStoragePool()->CopyFrom(pool);
        env.Runtime->SendToPipe(env.TabletId, sender, request.release(), 0, TTestActorSystem::GetPipeConfigWithRetries());
        env.Sim(TDuration::Seconds(1));
        UNIT_ASSERT(!response);
        UNIT_ASSERT_VALUES_EQUAL(numNotifications, 0);

        holdCommits = false;
        for (auto& [nodeId, ev] : heldCommits) {
            env.Runtime->Send(ev.release(), nodeId);
        }
        WaitUntil(env, [&] { return response.has_value(); }, "pool creation committed", TDuration::Seconds(1));
        UNIT_ASSERT_C(response->GetSuccess(), response->GetErrorDescription());
        env.Runtime->FilterFunction = {};
        WaitPileOperationFinished(env);
        UNIT_ASSERT_VALUES_EQUAL(CheckPileParts(env, 2, FirstThirdPileNodeId, LastThirdPileNodeId), 2);

        RejoinPile(env, 2);
        UNIT_ASSERT_VALUES_EQUAL(CheckPileParts(env, 2, FirstThirdPileNodeId, LastThirdPileNodeId), 2);
    }

    Y_UNIT_TEST(AddedPileSyncersAreLimited) {
        TEnvironmentSetup env(MakeExpandableSettings());
        CreatePoolAndGetGroup(env, 4);
        SetBridgeSyncMaxSyncers(env, 2);
        auto res = AddThirdPileToConfig(env);
        UNIT_ASSERT_EQUAL_C(res.GetStatus(), TResult::OK, res.GetErrorReason());
        env.Sim(TDuration::Seconds(10));
        StartPileNodes(env, "pile_3", FirstThirdPileNodeId, LastThirdPileNodeId);
        AddPileAndWait(env, "pile_3");

        TSyncerTracker tracker;
        tracker.Hold = true;
        env.Runtime->FilterFunction = [&](ui32 nodeId, std::unique_ptr<IEventHandle>& ev) {
            return tracker.OnEvent(nodeId, ev);
        };
        SwitchPileState(env, 2, NKikimrBridge::TClusterState::NOT_SYNCHRONIZED_1);
        WaitUntil(env, [&] { return tracker.GetNumRunning() >= 2; }, "syncers running");
        env.Sim(TDuration::Seconds(5)); // the static group gets synced by now, so it takes no slots
        UNIT_ASSERT_VALUES_EQUAL(tracker.GetNumRunning(), 2);
        tracker.RunUntil(env, 2, "pile synchronized", [&] {
            return GetPileState(env, 2) == NKikimrBridge::TClusterState::SYNCHRONIZED;
        });
        env.Runtime->FilterFunction = {};
    }

    Y_UNIT_TEST(NativeTwoToThree) {
        TEnvironmentSetup env(MakeExpandableSettings());
        const ui32 groupId = CreatePoolAndGetGroup(env);
        TDataChecker checker(env);
        checker.Write(groupId);

        auto res = AddThirdPileToConfig(env);
        UNIT_ASSERT_EQUAL_C(res.GetStatus(), TResult::OK, res.GetErrorReason());
        env.Sim(TDuration::Seconds(10));
        StartPileNodes(env, "pile_3", FirstThirdPileNodeId, LastThirdPileNodeId);

        res = AddPile(env, "pile_4");
        UNIT_ASSERT_EQUAL_C(res.GetStatus(), TResult::ERROR, res.GetErrorReason());
        UNIT_ASSERT_STRING_CONTAINS(res.GetErrorReason(), "is not listed in BridgeConfig");
        res = AddPile(env, "pile_2");
        UNIT_ASSERT_EQUAL_C(res.GetStatus(), TResult::ERROR, res.GetErrorReason());
        UNIT_ASSERT_STRING_CONTAINS(res.GetErrorReason(),
            "has to be INACTIVE to be added to the cluster, its state is SYNCHRONIZED");

        res = AddPile(env, "pile_3");
        UNIT_ASSERT_EQUAL_C(res.GetStatus(), TResult::OK, res.GetErrorReason());
        const auto config = WaitPileOperationFinished(env);
        UNIT_ASSERT_VALUES_EQUAL(GetPileStates(config), "pile_1:SYNCHRONIZED pile_2:SYNCHRONIZED pile_3:DISCONNECTED");
        UNIT_ASSERT_VALUES_EQUAL(CheckPileParts(env, 2, FirstThirdPileNodeId, LastThirdPileNodeId), 1);
        checker.Write(groupId);
        checker.CheckAll();

        RejoinPile(env, 2);
        checker.Write(groupId);
        checker.CheckAll();
        CheckPileData(env, checker, 2, FirstThirdPileNodeId);

        // new groups get their parts in every pile
        CreateAnotherPool(env, 2);
        env.Sim(TDuration::Seconds(30));
        UNIT_ASSERT_VALUES_EQUAL(CheckPileParts(env, 2, FirstThirdPileNodeId, LastThirdPileNodeId), 2);
        const auto baseConfig = env.FetchBaseConfig(); // groups are referenced by pointers
        for (const auto *group : GetPoolGroups(baseConfig, 2)) {
            if (group->GetIsProxyGroup()) {
                checker.Write(group->GetGroupId());
            }
        }
        checker.CheckAll();
        CheckPileData(env, checker, 2, FirstThirdPileNodeId);
        CheckPileData(env, checker, 0, 1);
    }

    Y_UNIT_TEST(ConvertedOneToTwo) {
        const auto named = std::make_shared<bool>(false);
        TEnvironmentSetup env(MakeConvertedClusterSettings(named));
        TDataChecker checker(env);
        const ui32 groupId = ConvertCluster(env, checker);

        // other piles can't be listed in the config while nodes of the converted cluster have no pile names
        ExpectError(ReplaceMainConfig(env, [&](YAML::Node& config) {
            for (const char *name : {"pile_1", "pile_2"}) {
                YAML::Node pile;
                pile["name"] = name;
                config["bridge_config"]["piles"].push_back(pile);
            }
        }), "mandatory pile name is missing for node 1");

        // nodes of the converted cluster get the name of their pile (implicit one), restarted with it one by one
        NameConvertedClusterNodes(env, *named, [&] {
            checker.Write(groupId);
            checker.CheckAll();
        });
        UNIT_ASSERT_VALUES_EQUAL(GetPileStates(QueryStorageConfig(env)), "pile_1:SYNCHRONIZED");

        auto res = AddPileToConfig(env, "pile_2", 10, 17);
        UNIT_ASSERT_EQUAL_C(res.GetStatus(), TResult::OK, res.GetErrorReason());
        env.Sim(TDuration::Seconds(10));
        StartPileNodes(env, "pile_2", 10, 17);

        res = AddPile(env, "pile_2");
        UNIT_ASSERT_EQUAL_C(res.GetStatus(), TResult::OK, res.GetErrorReason());
        const auto config = WaitPileOperationFinished(env);
        UNIT_ASSERT_VALUES_EQUAL(GetPileStates(config), "pile_1:SYNCHRONIZED pile_2:DISCONNECTED");
        // groups without pile id stay in the implicit pile
        UNIT_ASSERT_VALUES_EQUAL(config.GetClusterStateDetails().GetPileRegistry().GetImplicitPileId(), 1);
        UNIT_ASSERT_VALUES_EQUAL(CheckPileParts(env, 1, 10, 17), 1);
        checker.Write(groupId);
        checker.CheckAll();

        // the group relies on its original part until the part in the new pile is synced, and the original pile can't
        // go out meanwhile
        auto getAuthorityPileId = [&] {
            const auto info = GetGroupInfo(env, groupId);
            return TBridgePileId::FromProto(&info.GetBridgeGroupState(),
                &NKikimrBridge::TGroupState::GetAuthorityPileId);
        };
        UNIT_ASSERT(getAuthorityPileId() == TBridgePileId::FromPileIndex(0));
        ExpectError(TrySwitchPileState(env, 0, NKikimrBridge::TClusterState::DISCONNECTED),
            "Incorrect primary pile state");

        RejoinPile(env, 1);
        WaitUntil(env, [&] { return !getAuthorityPileId(); }, "group not relying on its original part");
        checker.Write(groupId);
        checker.CheckAll();
        CheckPileData(env, checker, 1, 10);
        CheckPileData(env, checker, 0, 1);

        // nodes of both piles keep working after restarts
        for (ui32 nodeId : {2u, 11u}) {
            env.RestartNode(nodeId);
            env.Sim(TDuration::Seconds(30));
            checker.Write(groupId);
            checker.CheckAll();
        }
    }

    // groups of the pools not converted along with the cluster get no parts in the added pile, while static groups do
    Y_UNIT_TEST(ConvertedOneToTwoSkippingStoragePools) {
        const auto named = std::make_shared<bool>(false);
        TEnvironmentSetup env(MakeConvertedClusterSettings(named));
        TDataChecker checker(env);
        const ui32 groupId = ConvertCluster(env, checker, 1, true);
        const ui32 staticGroupId = 0;
        checker.Write(staticGroupId);

        NameConvertedClusterNodes(env, *named, [&] {
            checker.Write(groupId);
            checker.CheckAll();
        });
        auto res = AddPileToConfig(env, "pile_2", 10, 17);
        UNIT_ASSERT_EQUAL_C(res.GetStatus(), TResult::OK, res.GetErrorReason());
        env.Sim(TDuration::Seconds(10));
        StartPileNodes(env, "pile_2", 10, 17);
        AddPileAndWait(env, "pile_2");
        UNIT_ASSERT_VALUES_EQUAL(CheckPileParts(env, 1, 10, 17), 0); // no dynamic bridged groups
        RejoinPile(env, 1);

        const auto info = GetGroupInfo(env, groupId);
        UNIT_ASSERT_C(!info.HasBridgeGroupState(), info.ShortDebugString());
        for (ui32 nodeId : {1u, 10u}) {
            checker.Write(groupId, nodeId);
            checker.Write(staticGroupId, nodeId);
            checker.CheckAll(nodeId);
        }
    }

    // nothing gets committed when there are not enough resources in the pile, neither for static groups, nor for the
    // groups of BSC; nodes of the pile don't have to be running to add it
    Y_UNIT_TEST(NotEnoughResources) {
        TEnvironmentSetup env(MakeExpandableSettings());
        const ui32 groupId = CreatePoolAndGetGroup(env, 2);
        TDataChecker checker(env);
        checker.Write(groupId);

        auto res = AddSingleSlotHostConfig(env);
        UNIT_ASSERT_EQUAL_C(res.GetStatus(), TResult::OK, res.GetErrorReason());
        res = AddPileToConfig(env, "pile_3", FirstThirdPileNodeId, FirstThirdPileNodeId + 3, 2); // not enough hosts
        UNIT_ASSERT_EQUAL_C(res.GetStatus(), TResult::OK, res.GetErrorReason());
        env.Sim(TDuration::Seconds(10));

        auto checkNothingCommitted = [&](const NKikimrBlobStorage::TStorageConfig& before) {
            const auto config = QueryStorageConfig(env);
            UNIT_ASSERT_VALUES_EQUAL(config.GetGeneration(), before.GetGeneration());
            UNIT_ASSERT_VALUES_EQUAL(GetPileStates(config), "pile_1:SYNCHRONIZED pile_2:SYNCHRONIZED pile_3:INACTIVE");
            UNIT_ASSERT(!config.GetClusterStateDetails().HasPendingPileOperation());
        };

        auto before = QueryStorageConfig(env);
        res = AddPile(env, "pile_3");
        UNIT_ASSERT_EQUAL_C(res.GetStatus(), TResult::ERROR, res.GetErrorReason());
        UNIT_ASSERT_STRING_CONTAINS(res.GetErrorReason(), "failed to allocate part of static group");
        checkNothingCommitted(before);

        // static group fits now, but the groups of BSC need two slots on each drive
        res = ReplaceMainConfig(env, [](YAML::Node& config) {
            AddPileHosts(config, "pile_3", FirstThirdPileNodeId + 4, LastThirdPileNodeId, 2);
        });
        UNIT_ASSERT_EQUAL_C(res.GetStatus(), TResult::OK, res.GetErrorReason());
        env.Sim(TDuration::Seconds(10));
        before = QueryStorageConfig(env);
        res = AddPile(env, "pile_3");
        UNIT_ASSERT_EQUAL_C(res.GetStatus(), TResult::ERROR, res.GetErrorReason());
        UNIT_ASSERT_STRING_CONTAINS(res.GetErrorReason(), "BSC can't add parts of its groups to the pile");
        checkNothingCommitted(before);
        UNIT_ASSERT_VALUES_EQUAL(GetPoolGroups(env.FetchBaseConfig(), 1).size(), 3 * 2); // proxy and two parts for each

        auto setSlotCount = [&](ui32 slotCount) {
            const auto res = ReplaceMainConfig(env, [&](YAML::Node& config) {
                config["host_configs"][1]["drive"][0]["expected_slot_count"] = slotCount;
            });
            UNIT_ASSERT_EQUAL_C(res.GetStatus(), TResult::OK, res.GetErrorReason());
            env.Sim(TDuration::Seconds(10));
        };

        // two slots would fit the groups of BSC, but static group takes one of them
        setSlotCount(2);
        before = QueryStorageConfig(env);
        res = AddPile(env, "pile_3");
        UNIT_ASSERT_EQUAL_C(res.GetStatus(), TResult::ERROR, res.GetErrorReason());
        UNIT_ASSERT_STRING_CONTAINS(res.GetErrorReason(), "BSC can't add parts of its groups to the pile");
        checkNothingCommitted(before);

        // there are enough slots now
        setSlotCount(4);
        res = AddPile(env, "pile_3");
        UNIT_ASSERT_EQUAL_C(res.GetStatus(), TResult::OK, res.GetErrorReason());
        const auto config = WaitPileOperationFinished(env);
        UNIT_ASSERT_VALUES_EQUAL(GetPileStates(config), "pile_1:SYNCHRONIZED pile_2:SYNCHRONIZED pile_3:DISCONNECTED");
        UNIT_ASSERT_VALUES_EQUAL(CheckPileParts(env, 2, FirstThirdPileNodeId, LastThirdPileNodeId), 2);

        // BSC knows static VDisks placed on its PDisks in the pile without being restarted
        const auto baseConfig = env.FetchBaseConfig();
        for (const auto& pdisk : baseConfig.GetPDisk()) {
            if (FirstThirdPileNodeId <= pdisk.GetNodeId()) {
                UNIT_ASSERT_VALUES_EQUAL_C(pdisk.GetNumStaticSlots(), 1, pdisk.DebugString());
            }
        }

        // nodes of the pile are started only now, the pile gets synced
        StartPileNodes(env, "pile_3", FirstThirdPileNodeId, LastThirdPileNodeId);
        RejoinPile(env, 2);
        checker.Write(groupId);
        checker.CheckAll();
        CheckPileData(env, checker, 2, FirstThirdPileNodeId);
    }

    // static groups are placed on PDisks known to BSC, so the pile can't be added while BSC doesn't provide them
    Y_UNIT_TEST(BaseConfigUnavailable) {
        TEnvironmentSetup env(MakeExpandableSettings());
        CreatePoolAndGetGroup(env);
        auto res = AddThirdPileToConfig(env);
        UNIT_ASSERT_EQUAL_C(res.GetStatus(), TResult::OK, res.GetErrorReason());
        env.Sim(TDuration::Seconds(10));

        // pipe of NodeWarden to BSC breaks while the base config is being queried
        bool breakPipe = true;
        env.Runtime->FilterFunction = [&](ui32 nodeId, std::unique_ptr<IEventHandle>& ev) {
            if (breakPipe && ev->GetTypeRewrite() == TEvTabletPipe::EvSend &&
                    ev->Type == TEvBlobStorage::EvControllerConfigRequest) {
                const auto& request = ev->Get<TEvBlobStorage::TEvControllerConfigRequest>()->Record.GetRequest();
                if (request.CommandSize() == 1 && request.GetCommand(0).HasQueryBaseConfig()) {
                    breakPipe = false;
                    env.Runtime->Send(new IEventHandle(TEvents::TSystem::PoisonPill, 0, ev->GetRecipientRewrite(), {},
                        nullptr, 0), nodeId);
                    return false;
                }
            }
            return true;
        };
        const auto before = QueryStorageConfig(env);
        res = AddPile(env, "pile_3");
        UNIT_ASSERT(!breakPipe);
        UNIT_ASSERT_EQUAL_C(res.GetStatus(), TResult::ERROR, res.GetErrorReason());
        UNIT_ASSERT_STRING_CONTAINS(res.GetErrorReason(), "Can't get base config: pipe to BSC disconnected");
        const auto config = QueryStorageConfig(env);
        UNIT_ASSERT_VALUES_EQUAL(config.GetGeneration(), before.GetGeneration());
        UNIT_ASSERT_VALUES_EQUAL(GetPileStates(config), "pile_1:SYNCHRONIZED pile_2:SYNCHRONIZED pile_3:INACTIVE");

        // pipe gets reestablished, the pile is added on PDisks of BSC
        res = AddPile(env, "pile_3");
        UNIT_ASSERT_EQUAL_C(res.GetStatus(), TResult::OK, res.GetErrorReason());
        WaitPileOperationFinished(env);
        UNIT_ASSERT_VALUES_EQUAL(CheckPileParts(env, 2, FirstThirdPileNodeId, LastThirdPileNodeId), 1);
    }

    // BSC fails to perform its part after the pile has been added (as resources has gone after validation); the error is
    // recorded and the operation is retried, so that it finishes when there are enough resources
    Y_UNIT_TEST(BscFailureRetried) {
        TEnvironmentSetup env(MakeExpandableSettings());
        const ui32 groupId = CreatePoolAndGetGroup(env);
        TDataChecker checker(env);
        checker.Write(groupId);

        auto res = AddSingleSlotHostConfig(env);
        UNIT_ASSERT_EQUAL_C(res.GetStatus(), TResult::OK, res.GetErrorReason());
        res = AddPileToConfig(env, "pile_3", FirstThirdPileNodeId, LastThirdPileNodeId, 2);
        UNIT_ASSERT_EQUAL_C(res.GetStatus(), TResult::OK, res.GetErrorReason());
        env.Sim(TDuration::Seconds(10));

        // validation by BSC fails, but distconf is told it succeeded
        bool spoofValidation = true;
        ui32 numSpoofed = 0;
        ui32 numNotifications = 0;
        TBscNotificationRetry retry(env, [](const auto& config) {
            return config.GetClusterStateDetails().GetPendingPileOperation().HasErrorReason();
        });
        env.Runtime->FilterFunction = [&](ui32 nodeId, std::unique_ptr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == TEvBlobStorage::EvControllerDistconfResponse && spoofValidation) {
                auto& record = ev->Get<TEvBlobStorage::TEvControllerDistconfResponse>()->Record;
                if (record.GetStatus() == NKikimrBlobStorage::TEvControllerDistconfResponse::Error) {
                    record.SetStatus(NKikimrBlobStorage::TEvControllerDistconfResponse::OK);
                    record.ClearErrorReason();
                    ++numSpoofed;
                }
            } else if (const auto *notify = GetBridgePileOperationNotification(*ev)) {
                ++numNotifications;
                if (notify->GetNotifyBridgePileOperation().HasErrorReason()) {
                    retry.ObserveNotification(*ev);
                }
            }
            return retry.Filter(nodeId, ev);
        };
        Y_DEFER { env.Runtime->FilterFunction = {}; };
        res = AddPile(env, "pile_3");
        UNIT_ASSERT_EQUAL_C(res.GetStatus(), TResult::OK, res.GetErrorReason());
        UNIT_ASSERT_VALUES_EQUAL(numSpoofed, 1);
        spoofValidation = false;

        retry.CheckRetryWaitsForCommit();

        WaitUntil(env, [&] {
            return QueryStorageConfig(env).GetClusterStateDetails().GetPendingPileOperation().HasErrorReason();
        }, "error of BSC recorded");
        auto config = QueryStorageConfig(env);
        UNIT_ASSERT_VALUES_EQUAL(GetPileStates(config), "pile_1:SYNCHRONIZED pile_2:SYNCHRONIZED pile_3:DISCONNECTED");
        UNIT_ASSERT_STRING_CONTAINS(config.GetClusterStateDetails().GetPendingPileOperation().GetErrorReason(),
            "Group fit error");

        // BSC keeps retrying, the same error doesn't change the config
        const ui32 notificationsBefore = numNotifications;
        env.Sim(TDuration::Minutes(2));
        UNIT_ASSERT_GT(numNotifications, notificationsBefore);
        UNIT_ASSERT_VALUES_EQUAL(QueryStorageConfig(env).GetGeneration(), config.GetGeneration());

        res = TrySwitchPileState(env, 2, NKikimrBridge::TClusterState::NOT_SYNCHRONIZED_1);
        UNIT_ASSERT_EQUAL_C(res.GetStatus(), TResult::ERROR, res.GetErrorReason());
        UNIT_ASSERT_STRING_CONTAINS(res.GetErrorReason(), "is being added to the cluster");
        checker.Write(groupId);
        checker.CheckAll();

        // resources appear, BSC finishes the operation
        res = ReplaceMainConfig(env, [](YAML::Node& config) {
            config["host_configs"][1]["drive"][0]["expected_slot_count"] = 4;
        });
        UNIT_ASSERT_EQUAL_C(res.GetStatus(), TResult::OK, res.GetErrorReason());
        config = WaitPileOperationFinished(env);
        UNIT_ASSERT_VALUES_EQUAL(GetPileStates(config), "pile_1:SYNCHRONIZED pile_2:SYNCHRONIZED pile_3:DISCONNECTED");
        UNIT_ASSERT_VALUES_EQUAL(CheckPileParts(env, 2, FirstThirdPileNodeId, LastThirdPileNodeId), 1);

        StartPileNodes(env, "pile_3", FirstThirdPileNodeId, LastThirdPileNodeId);
        RejoinPile(env, 2);
        checker.Write(groupId);
        checker.CheckAll();
        CheckPileData(env, checker, 2, FirstThirdPileNodeId);
    }

    // groups created while the addition of the pile is stuck get no part in the pile (the addition gives it to them), so
    // they neither fail for lack of resources there nor take the ones the addition counts on; the stuck addition is
    // cancelled by removing the pile, which can be added again then
    Y_UNIT_TEST(StuckAdditionCancelled) {
        TEnvironmentSetup env(MakeExpandableSettings());
        const ui32 groupId = CreatePoolAndGetGroup(env);
        TDataChecker checker(env);
        checker.Write(groupId);

        auto res = AddSingleSlotHostConfig(env);
        UNIT_ASSERT_EQUAL_C(res.GetStatus(), TResult::OK, res.GetErrorReason());
        res = AddPileToConfig(env, "pile_3", FirstThirdPileNodeId, LastThirdPileNodeId, 2);
        UNIT_ASSERT_EQUAL_C(res.GetStatus(), TResult::OK, res.GetErrorReason());
        env.Sim(TDuration::Seconds(10));

        // validation by BSC fails, but distconf is told it succeeded, so BSC can't place the parts afterwards
        bool spoofValidation = true;
        env.Runtime->FilterFunction = [&](ui32 /*nodeId*/, std::unique_ptr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == TEvBlobStorage::EvControllerDistconfResponse && spoofValidation) {
                auto& record = ev->Get<TEvBlobStorage::TEvControllerDistconfResponse>()->Record;
                if (record.GetStatus() == NKikimrBlobStorage::TEvControllerDistconfResponse::Error) {
                    record.SetStatus(NKikimrBlobStorage::TEvControllerDistconfResponse::OK);
                    record.ClearErrorReason();
                }
            }
            return true;
        };
        res = AddPile(env, "pile_3");
        UNIT_ASSERT_EQUAL_C(res.GetStatus(), TResult::OK, res.GetErrorReason());
        spoofValidation = false;
        WaitUntil(env, [&] {
            return QueryStorageConfig(env).GetClusterStateDetails().GetPendingPileOperation().HasErrorReason();
        }, "error of BSC recorded");

        // the new pool gets its group with no part in the pile
        CreateAnotherPool(env, 2);
        env.Sim(TDuration::Seconds(30));
        std::optional<ui32> newGroupId;
        {
            const auto baseConfig = env.FetchBaseConfig(); // groups are referenced by pointers
            for (const auto *group : GetPoolGroups(baseConfig, 2)) {
                if (group->GetIsProxyGroup()) {
                    newGroupId = group->GetGroupId();
                }
            }
        }
        UNIT_ASSERT(newGroupId);
        const auto info = GetGroupInfo(env, *newGroupId);
        UNIT_ASSERT_C(!NBridge::GetGroupPile(info.GetBridgeGroupState(), TBridgePileId::FromPileIndex(2)),
            info.DebugString());
        checker.Write(*newGroupId);

        // the stuck addition is cancelled by the removal
        RemovePileAndWait(env, "pile_3");
        CheckPileGone(env, 2, FirstThirdPileNodeId, LastThirdPileNodeId);
        checker.CheckAll();

        // resources appear, the pile is added with parts of both groups
        res = ReplaceMainConfig(env, [](YAML::Node& config) {
            config["host_configs"][1]["drive"][0]["expected_slot_count"] = 4;
        });
        UNIT_ASSERT_EQUAL_C(res.GetStatus(), TResult::OK, res.GetErrorReason());
        env.Sim(TDuration::Seconds(10));
        AddPileAndWait(env, "pile_3");
        UNIT_ASSERT_VALUES_EQUAL(CheckPileParts(env, 2, FirstThirdPileNodeId, LastThirdPileNodeId), 2);
        StartPileNodes(env, "pile_3", FirstThirdPileNodeId, LastThirdPileNodeId);
        RejoinPile(env, 2);
        checker.Write(groupId);
        checker.CheckAll();
        CheckPileData(env, checker, 2, FirstThirdPileNodeId);
    }

    // the pile can't be rejoined until BSC adds its parts to every group; BSC finishes the operation after restart
    Y_UNIT_TEST(RejoinWaitsForBsc) {
        TEnvironmentSetup env(MakeExpandableSettings());
        const ui32 groupId = CreatePoolAndGetGroup(env);
        TDataChecker checker(env);
        checker.Write(groupId);

        auto res = AddThirdPileToConfig(env);
        UNIT_ASSERT_EQUAL_C(res.GetStatus(), TResult::OK, res.GetErrorReason());
        env.Sim(TDuration::Seconds(10));
        StartPileNodes(env, "pile_3", FirstThirdPileNodeId, LastThirdPileNodeId);

        // BSC has created parts of its groups, but distconf doesn't know it yet
        bool hold = true;
        ui32 numNotifications = 0;
        env.Runtime->FilterFunction = [&](ui32 /*nodeId*/, std::unique_ptr<IEventHandle>& ev) {
            if (GetBridgePileOperationNotification(*ev)) {
                ++numNotifications;
                return !hold;
            }
            return true;
        };

        res = AddPile(env, "pile_3");
        UNIT_ASSERT_EQUAL_C(res.GetStatus(), TResult::OK, res.GetErrorReason());
        env.Sim(TDuration::Seconds(30));
        UNIT_ASSERT(numNotifications);
        auto config = QueryStorageConfig(env);
        UNIT_ASSERT_VALUES_EQUAL(GetPileStates(config), "pile_1:SYNCHRONIZED pile_2:SYNCHRONIZED pile_3:DISCONNECTED");
        UNIT_ASSERT(config.GetClusterStateDetails().HasPendingPileOperation());

        res = TrySwitchPileState(env, 2, NKikimrBridge::TClusterState::NOT_SYNCHRONIZED_1);
        UNIT_ASSERT_EQUAL_C(res.GetStatus(), TResult::ERROR, res.GetErrorReason());
        UNIT_ASSERT_STRING_CONTAINS(res.GetErrorReason(), "is being added to the cluster");
        res = AddPile(env, "pile_3");
        UNIT_ASSERT_EQUAL_C(res.GetStatus(), TResult::ERROR, res.GetErrorReason());
        checker.Write(groupId);
        checker.CheckAll();

        // BSC (residing on node 1) is restarted, it performs the operation again finding everything done already
        hold = false;
        env.RestartNode(1);
        config = WaitPileOperationFinished(env);
        UNIT_ASSERT_VALUES_EQUAL(GetPileStates(config), "pile_1:SYNCHRONIZED pile_2:SYNCHRONIZED pile_3:DISCONNECTED");
        UNIT_ASSERT_VALUES_EQUAL(CheckPileParts(env, 2, FirstThirdPileNodeId, LastThirdPileNodeId), 1);
        UNIT_ASSERT_VALUES_EQUAL(GetPoolGroups(env.FetchBaseConfig(), 1).size(), 4); // proxy and three parts

        RejoinPile(env, 2);
        checker.Write(groupId);
        checker.CheckAll();
        CheckPileData(env, checker, 2, FirstThirdPileNodeId);
    }

    // restarts of the nodes of the new pile and of the cluster while the pile is being added and synced
    Y_UNIT_TEST(RestartsWhileAdding) {
        TEnvironmentSetup env(MakeExpandableSettings());
        const ui32 groupId = CreatePoolAndGetGroup(env);
        TDataChecker checker(env);
        checker.Write(groupId);

        auto res = AddThirdPileToConfig(env);
        UNIT_ASSERT_EQUAL_C(res.GetStatus(), TResult::OK, res.GetErrorReason());
        env.Sim(TDuration::Seconds(10));
        StartPileNodes(env, "pile_3", FirstThirdPileNodeId, LastThirdPileNodeId);

        res = AddPile(env, "pile_3");
        UNIT_ASSERT_EQUAL_C(res.GetStatus(), TResult::OK, res.GetErrorReason());
        for (ui32 nodeId : {FirstThirdPileNodeId, 2u, 9u}) {
            env.RestartNode(nodeId);
            env.Sim(TDuration::Seconds(10));
        }
        WaitPileOperationFinished(env);
        checker.Write(groupId);
        checker.CheckAll();

        SwitchPileState(env, 2, NKikimrBridge::TClusterState::NOT_SYNCHRONIZED_1);
        for (ui32 nodeId : {FirstThirdPileNodeId + 1, 1u}) {
            env.RestartNode(nodeId);
            env.Sim(TDuration::Seconds(10));
        }
        WaitForPileState(env, 2, NKikimrBridge::TClusterState::SYNCHRONIZED);
        checker.Write(groupId);
        checker.CheckAll();
        CheckPileData(env, checker, 2, FirstThirdPileNodeId);
    }
}
