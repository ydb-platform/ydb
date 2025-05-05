#include <ydb/core/blobstorage/ut_blobstorage/lib/env.h>
#include <ydb/core/blobstorage/ut_blobstorage/lib/common.h>
#include <ydb/core/mind/bscontroller/layout_helpers.h>

Y_UNIT_TEST_SUITE(GroupLayoutSanitizer) {
    using NBsController::TPDiskId;
    using NKikimrBlobStorage::EDriveStatus;

    bool CatchSanitizeRequests(ui32 /*nodeId*/, std::unique_ptr<IEventHandle>& ev) {
        if (ev->GetTypeRewrite() == TEvBlobStorage::TEvControllerConfigRequest::EventType) {
            const auto& request = ev->Get<TEvBlobStorage::TEvControllerConfigRequest>()->Record.GetRequest();
            for (const auto& command : request.GetCommand()) {
                UNIT_ASSERT(command.GetCommandCase() != NKikimrBlobStorage::TConfigRequest::TCommand::kSanitizeGroup);
            }
        }
        return true;
    }

    using TLocationGenerator = std::function<NActorsInterconnect::TNodeLocation(ui32, ui32, ui32)>;

    void MakeLocations(std::vector<TNodeLocation>& locations, ui32 numDatacenters, ui32 numRacksPerDC, ui32 numNodesPerRack,
            TLocationGenerator locationGenerator) {
        for (ui32 dc = 0; dc < numDatacenters; ++dc) {
            for (ui32 rack = 0; rack < numRacksPerDC; ++rack) {
                for (ui32 unit = 0; unit < numNodesPerRack; ++unit) {
                    locations.emplace_back(locationGenerator(dc, rack, unit));
                }
            }
        }
    }

    void CreateEnv(std::unique_ptr<TEnvironmentSetup>& env, std::vector<TNodeLocation>& locations,
            TBlobStorageGroupType groupType) {
        const ui32 numNodes = locations.size();

        env.reset(new TEnvironmentSetup({
            .NodeCount = numNodes,
            .Erasure = groupType,
            .LocationGenerator = [&](ui32 nodeId) { return locations[nodeId - 1]; },
        }));

        const ui32 disksPerNode = 1;
        const ui32 slotsPerDisk = 3;

        env->Runtime->FilterFunction = CatchSanitizeRequests;
        env->CreateBoxAndPool(disksPerNode, numNodes * disksPerNode * slotsPerDisk / 9);
        env->Runtime->FilterFunction = {};
    }

    NActorsInterconnect::TNodeLocation LocationGenerator(ui32 dc, ui32 rack, ui32 unit) {
        NActorsInterconnect::TNodeLocation proto;
        proto.SetDataCenter(ToString(dc));
        proto.SetRack(ToString(rack));
        proto.SetUnit(ToString(unit));
        return proto;
    }

    void Test(TBlobStorageGroupType groupType, ui32 dcs, ui32 racks, ui32 units) {
        std::vector<TNodeLocation> locations;

        MakeLocations(locations, dcs, racks, units, LocationGenerator);
        std::unique_ptr<TEnvironmentSetup> env;

        CreateEnv(env, locations, groupType);


        // Assure that sanitizer doesn't send request to initially allocated groups
        env->Runtime->FilterFunction = CatchSanitizeRequests;
        env->UpdateSettings(true, false, true);
        env->Sim(TDuration::Minutes(3));
        env->UpdateSettings(false, false, false);

        TGroupGeometryInfo geom = CreateGroupGeometry(groupType);

        TString error;
        auto cfg = env->FetchBaseConfig();
        UNIT_ASSERT_C(CheckBaseConfigLayout(geom, cfg, true, error), error);

        // Shuffle node locayion, assure that layout error occured
        do {
            env->Cleanup();
            std::random_shuffle(locations.begin(), locations.end());
            env->Initialize();
            env->Sim(TDuration::Seconds(100));
            cfg = env->FetchBaseConfig();
        } while (CheckBaseConfigLayout(geom, cfg, true, error));
        Cerr << error << Endl;

        // Sanitize groups
        env->Runtime->FilterFunction = {};
        env->UpdateSettings(true, false, true);
        env->Sim(TDuration::Minutes(15));
        cfg = env->FetchBaseConfig();
        UNIT_ASSERT_C(CheckBaseConfigLayout(geom, cfg, true, error), error);

        // Assure that sanitizer doesn't send request to fully sane groups
        env->Runtime->FilterFunction = CatchSanitizeRequests;
        env->Sim(TDuration::Minutes(3));
        cfg = env->FetchBaseConfig();
        UNIT_ASSERT_C(CheckBaseConfigLayout(geom, cfg, true, error), error);
    }

    Y_UNIT_TEST(Test3dc) {
        Test(TBlobStorageGroupType::ErasureMirror3dc, 3, 5, 1);
    }

    Y_UNIT_TEST(TestBlock4Plus2) {
        Test(TBlobStorageGroupType::Erasure4Plus2Block, 1, 10, 2);
    }

    Y_UNIT_TEST(TestMirror3of4) {
        Test(TBlobStorageGroupType::ErasureMirror3of4, 1, 10, 2);
    }

    TString PrintGroups(TBlobStorageGroupType groupType, const NKikimrBlobStorage::TBaseConfig& cfg,
            std::vector<TNodeLocation> locations) {
        TGroupGeometryInfo geom = CreateGroupGeometry(groupType);
        NLayoutChecker::TDomainMapper domainMapper;

        std::unordered_map<ui32, TNodeLocation> nodes;
        std::unordered_map<TPDiskId, NLayoutChecker::TPDiskLayoutPosition> pdisks;

        for (ui32 i = 0; i < cfg.NodeSize(); ++i) {
            auto node = cfg.GetNode(i);
            nodes[node.GetNodeId()] = TNodeLocation(node.GetLocation());
        }

        for (ui32 i = 0; i < cfg.PDiskSize(); ++i) {
            auto pdisk = cfg.GetPDisk(i);
            TNodeLocation loc = nodes[pdisk.GetNodeId()];
            TPDiskId pdiskId(pdisk.GetNodeId(), pdisk.GetPDiskId());
            pdisks[pdiskId] = NLayoutChecker::TPDiskLayoutPosition(domainMapper, loc, pdiskId, geom);
        }

        std::unordered_map<ui32, TGroupMapper::TGroupDefinition> groups;
        for (ui32 i = 0; i < cfg.VSlotSize(); ++i) {
            auto vslot = cfg.GetVSlot(i);
            TPDiskId pdiskId(vslot.GetVSlotId().GetNodeId(), vslot.GetVSlotId().GetPDiskId());
            Y_ABORT_UNLESS(pdiskId != TPDiskId());
            Y_ABORT_UNLESS(pdisks.find(pdiskId) != pdisks.end());
            ui32 groupId = vslot.GetGroupId();
            geom.ResizeGroup(groups[groupId]);
            groups[groupId][vslot.GetFailRealmIdx()][vslot.GetFailDomainIdx()][vslot.GetVDiskIdx()] = pdiskId;
        }

        TStringStream s;
        for (auto& [groupId, group] : groups) {
            s << "GroupId# " << groupId << Endl;
            for (const auto& realm : group) {
                for (const auto& domain : realm) {
                    for (const auto& pdiskId : domain) {
                        const auto& pdisk = pdisks[pdiskId];
                        ui32 nodeId = pdiskId.NodeId;
                        auto loc = locations[nodeId - 1];
                        s << "{ NodeLocation# " << loc.GetRackId() <<
                            " PDisk Location: Realm# " << pdisk.Realm << " Domain# " << pdisk.Domain << " } ";
                    }
                }
                s << Endl;
            }
        }
        return s.Str();
    }

    void TestMultipleRealmsOccupation(bool allowMultipleRealmsOccupation) {
        TBlobStorageGroupType groupType = TBlobStorageGroupType::ErasureMirror3dc;
        std::vector<TNodeLocation> locations;
        TLocationGenerator locationGenerator = [](ui32 dc, ui32 rack, ui32 unit) {
            NActorsInterconnect::TNodeLocation proto;
            if (dc == 3) {
                proto.SetDataCenter("2");
                proto.SetRack(ToString(rack + 5));
            } else {
                proto.SetDataCenter(ToString(dc));
                proto.SetRack(ToString(rack));
            }
            proto.SetUnit(ToString(unit));
            return proto;
        };
        MakeLocations(locations, 4, 5, 1, locationGenerator);
        std::unique_ptr<TEnvironmentSetup> env;
        CreateEnv(env, locations, groupType);

        TGroupGeometryInfo geom = CreateGroupGeometry(groupType);

        env->Runtime->FilterFunction = CatchSanitizeRequests;

        TString error;
        auto cfg = env->FetchBaseConfig();
        // Cerr << "Before splitting DC: " << Endl << PrintGroups(groupType, cfg, locations);
        UNIT_ASSERT_C(CheckBaseConfigLayout(geom, cfg, allowMultipleRealmsOccupation, error), error);
        env->Cleanup();

        locationGenerator = [](ui32 dc, ui32 rack, ui32 unit) {
            NActorsInterconnect::TNodeLocation proto;
            if (dc >= 2) {
                if (rack % 2 == 0) {
                    proto.SetDataCenter("2");
                    proto.SetRack(ToString(rack + 5));
                } else {
                    proto.SetDataCenter("3");
                    proto.SetRack(ToString(rack));
                }
            } else {
                proto.SetDataCenter(ToString(dc));
                proto.SetRack(ToString(rack));
            }
            proto.SetUnit(ToString(unit));
            return proto;
        };
        locations.clear();
        MakeLocations(locations, 4, 5, 1, locationGenerator);

        env->Initialize();

        if (!allowMultipleRealmsOccupation) {
            env->Runtime->FilterFunction = {};
        }

        // Update bs config
        {
            NKikimrBlobStorage::TConfigRequest request;
            auto *cmd = request.AddCommand();
            auto *us = cmd->MutableUpdateSettings();
            us->AddEnableSelfHeal(true);
            us->AddEnableGroupLayoutSanitizer(true);
            // us->AddAllowMultipleRealmsOccupation(allowMultipleRealmsOccupation);
            auto response = env->Invoke(request);
            UNIT_ASSERT_C(response.GetSuccess(), response.GetErrorDescription());
        }

        env->Sim(TDuration::Seconds(100));
        cfg = env->FetchBaseConfig();
        // Cerr << "After splitting DC: " << Endl << PrintGroups(groupType, cfg, locations) << Endl;
        UNIT_ASSERT_C(CheckBaseConfigLayout(geom, cfg, allowMultipleRealmsOccupation, error), error);
    }

    Y_UNIT_TEST(MultipleRealmsOccupation) {
        TestMultipleRealmsOccupation(true);
    }

    Y_UNIT_TEST(ForbidMultipleRealmsOccupation) {
        TestMultipleRealmsOccupation(false);
    }

    void StressTest(TBlobStorageGroupType groupType, ui32 dcs, ui32 racks, ui32 units) {
        const ui32 steps = 100;
        std::vector<TNodeLocation> locations;

        MakeLocations(locations, dcs, racks, units, LocationGenerator);
        std::unique_ptr<TEnvironmentSetup> env;

        CreateEnv(env, locations, groupType);
        env->Sim(TDuration::Minutes(3));
        env->UpdateSettings(false, false, false);

        std::vector<TPDiskId> pdisks;

        {
            auto cfg = env->FetchBaseConfig();
            for (const auto& pdisk : cfg.GetPDisk()) {
                pdisks.emplace_back(pdisk.GetNodeId(), pdisk.GetPDiskId());
            }
        }

        auto shuffleLocations = [&]() {
            TString error;
            env->Cleanup();
            std::random_shuffle(locations.begin(), locations.end());
            env->Initialize();
            env->Sim(TDuration::Seconds(100));
        };

        auto updateDriveStatus = [&](ui32 drives) {
            NKikimrBlobStorage::TConfigRequest request;
            request.SetIgnoreGroupFailModelChecks(true);
            request.SetIgnoreGroupSanityChecks(true);
            request.SetIgnoreDegradedGroupsChecks(true);
            request.SetIgnoreDisintegratedGroupsChecks(true);
            for (ui32 i = 0; i < drives; ++i) {
                auto* cmd = request.AddCommand();
                auto* drive = cmd->MutableUpdateDriveStatus();
                TPDiskId pdiskId = pdisks[RandomNumber<ui32>(pdisks.size())];
                drive->MutableHostKey()->SetNodeId(pdiskId.NodeId);
                drive->SetPDiskId(pdiskId.PDiskId);
                switch (RandomNumber<ui32>(7)) {
                    case 0:
                        drive->SetStatus(EDriveStatus::INACTIVE);
                        break;
                    case 1:
                        drive->SetStatus(EDriveStatus::BROKEN);
                        break;
                    case 2:
                        drive->SetStatus(EDriveStatus::FAULTY);
                        break;
                    default:
                        drive->SetStatus(EDriveStatus::ACTIVE);
                }
            }

            env->Invoke(request);
        };

        enum class EActions {
            SHUFFLE_LOCATIONS = 0,
            UPDATE_STATUS,
            ENABLE_SANITIZER,
            DISABLE_SANITIZER,
            ENABLE_SELF_HEAL,
            DISABLE_SELF_HEAL,
        };
        TWeightedRandom<EActions> act;
    
        act.AddValue(EActions::SHUFFLE_LOCATIONS, 1);
        act.AddValue(EActions::UPDATE_STATUS, 5);
        act.AddValue(EActions::ENABLE_SANITIZER, 1);
        act.AddValue(EActions::DISABLE_SANITIZER, 1);
        act.AddValue(EActions::ENABLE_SELF_HEAL, 1);
        act.AddValue(EActions::DISABLE_SELF_HEAL, 1);

        bool selfHeal = false;
        bool groupLayoutSanitizer = false;

        for (ui32 i = 0; i < steps; ++i) {
            switch (act.GetRandom()) {
                case EActions::SHUFFLE_LOCATIONS:
                    shuffleLocations();
                    break;
                case EActions::UPDATE_STATUS:
                    updateDriveStatus(RandomNumber<ui32>(5) + 1);
                    break;
                case EActions::ENABLE_SANITIZER:
                    groupLayoutSanitizer = true;
                    env->UpdateSettings(selfHeal, false, groupLayoutSanitizer);
                    break;
                case EActions::DISABLE_SANITIZER:
                    groupLayoutSanitizer = false;
                    env->UpdateSettings(selfHeal, false, groupLayoutSanitizer);
                    break;
                case EActions::ENABLE_SELF_HEAL:
                    selfHeal = true;
                    env->UpdateSettings(selfHeal, false, groupLayoutSanitizer);
                    break;
                case EActions::DISABLE_SELF_HEAL:
                    selfHeal = false;
                    env->UpdateSettings(selfHeal, false, groupLayoutSanitizer);
                    break;
            }
        }
    }

    Y_UNIT_TEST(StressMirror3dc) {
        StressTest(TBlobStorageGroupType::ErasureMirror3dc, 3, 5, 1);
    }

    Y_UNIT_TEST(StressBlock4Plus2) {
        StressTest(TBlobStorageGroupType::Erasure4Plus2Block, 1, 10, 2);
    }

    Y_UNIT_TEST(StressMirror3of4) {
        StressTest(TBlobStorageGroupType::ErasureMirror3of4, 1, 10, 2);
    }
}
