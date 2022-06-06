#include <ydb/core/blobstorage/ut_blobstorage/lib/env.h>

Y_UNIT_TEST_SUITE(GroupLayoutSanitizer) {
    Y_UNIT_TEST(Test3dc) {
        const ui32 numRacks = 15;
        std::vector<ui32> nodesPerRack(numRacks);
        std::vector<ui32> nodeToRack;
        for (ui32 numFilledRacks = 0; numFilledRacks < numRacks; ) {
//            const ui32 rackId = RandomNumber(numRacks);
            const ui32 rackId = numFilledRacks;
            nodeToRack.emplace_back(rackId);
            numFilledRacks += !nodesPerRack[rackId]++;
        }
        const ui32 numDatacenters = 3;
        std::vector<ui32> rackToDatacenter;
        for (ui32 i = 0; i < numRacks; ++i) {
            rackToDatacenter.push_back(i % numDatacenters);
        }

        std::vector<TNodeLocation> locations;
        for (ui32 i = 0; i < nodeToRack.size(); ++i) {
            NActorsInterconnect::TNodeLocation proto;
            proto.SetDataCenter(ToString(rackToDatacenter[nodeToRack[i]]));
            proto.SetRack(ToString(nodeToRack[i]));
            proto.SetUnit(ToString(i));
            locations.emplace_back(proto);
        }

        TEnvironmentSetup env{{
            .NodeCount = (ui32)nodeToRack.size(),
            .Erasure = TBlobStorageGroupType::ErasureMirror3dc,
            .LocationGenerator = [&](ui32 nodeId) { return locations[nodeId - 1]; },
        }};

        auto getGroupsWithIncorrectLayout = [&] {
            auto config = env.FetchBaseConfig();

            std::map<ui32, std::tuple<TString, TString>> nodeIdToLocation;
            for (const auto& node : config.GetNode()) {
                const auto& location = node.GetLocation();
                nodeIdToLocation.emplace(node.GetNodeId(), std::make_tuple(location.GetDataCenter(), location.GetRack()));
            }

            std::map<ui32, std::vector<std::vector<std::tuple<TString, TString>>>> groups;
            for (const auto& vslot : config.GetVSlot()) {
                auto& group = groups[vslot.GetGroupId()];
                if (group.empty()) {
                    group.resize(3, {3, {"", ""}});
                }
                group[vslot.GetFailRealmIdx()][vslot.GetFailDomainIdx()] = nodeIdToLocation[vslot.GetVSlotId().GetNodeId()];
            }

            std::set<ui32> badGroups;

            for (auto& [groupId, group] : groups) {
                std::set<TString> usedRealms;

                for (const auto& row : group) {
                    TString realm;
                    std::set<TString> usedRacks;

                    for (const auto& [dc, rack] : row) {
                        Y_VERIFY(dc && rack);

                        if (!usedRacks.insert(rack).second) {
                            badGroups.insert(groupId);
                        }

                        if (!realm) {
                            if (!usedRealms.insert(dc).second) {
                                badGroups.insert(groupId);
                            }
                            realm = dc;
                        } else if (realm != dc) {
                            badGroups.insert(groupId);
                        }
                    }
                }
            }

            return badGroups;
        };

        const ui32 disksPerNode = 1;
        const ui32 slotsPerDisk = 3;
        env.CreateBoxAndPool(disksPerNode, nodeToRack.size() * disksPerNode * slotsPerDisk / 9);
        env.Sim(TDuration::Seconds(30));
        auto before = getGroupsWithIncorrectLayout();
        Cerr << "bad groups before shuffling# " << FormatList(before) << Endl;
        UNIT_ASSERT(before.empty());
        env.Cleanup();
        std::random_shuffle(locations.begin(), locations.end());
        env.Initialize();
        env.Sim(TDuration::Seconds(100));
        auto after = getGroupsWithIncorrectLayout();
        Cerr << "bad groups just after shuffling# " << FormatList(after) << Endl;
        env.UpdateSettings(true, false, true);
        env.Sim(TDuration::Minutes(15));
        auto corrected = getGroupsWithIncorrectLayout();
        Cerr << "bad groups after shuffling and fixing# " << FormatList(corrected) << Endl;
        UNIT_ASSERT(corrected.empty());
    }
}
