#include "sentinel_ut_helpers.h"
#include "cms_ut_common.h"
#include "sentinel.h"
#include "sentinel_impl.h"
#include "cms_impl.h"

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/hash_set.h>
#include <util/generic/vector.h>
#include <util/generic/xrange.h>
#include <util/random/random.h>
#include <util/string/builder.h>

namespace NKikimr::NCmsTest {

Y_UNIT_TEST_SUITE(TSentinelBaseTests) {
    using namespace NCms;
    using namespace NCms::NSentinel;
    using TPDiskID = NCms::TPDiskID;

    Y_UNIT_TEST(PDiskInitialStatus) {
        const EPDiskStatus AllStatuses[] = {
            EPDiskStatus::UNKNOWN,
            EPDiskStatus::ACTIVE,
            EPDiskStatus::INACTIVE,
            EPDiskStatus::BROKEN,
            EPDiskStatus::FAULTY,
            EPDiskStatus::TO_BE_REMOVED,
        };

        for (const EPDiskStatus status : AllStatuses) {
            TPDiskStatus st(status, DefaultStateLimit, DefaultStateLimits);

            UNIT_ASSERT(!st.IsChanged());
            UNIT_ASSERT_VALUES_EQUAL(st.GetStatus(), status);
        }
    }

    Y_UNIT_TEST(PDiskErrorState) {
        for (const EPDiskState state : ErrorStates) {
            const EPDiskStatus initialStatus = EPDiskStatus::ACTIVE;
            TPDiskStatus st(initialStatus, DefaultStateLimit, DefaultStateLimits);

            for (ui32 i = 1; i < DefaultStateLimits[state]; ++i) {
                st.AddState(state);

                UNIT_ASSERT(!st.IsChanged());
                UNIT_ASSERT_VALUES_EQUAL(st.GetStatus(), initialStatus);
            }
            st.AddState(state);
            UNIT_ASSERT(st.IsChanged());

            st.ApplyChanges();
            UNIT_ASSERT_VALUES_EQUAL(st.GetStatus(), EPDiskStatus::FAULTY);
        }
    }

    Y_UNIT_TEST(PDiskInactiveAfterStateChange) {
        for (const EPDiskState state : ErrorStates) {
            const EPDiskStatus initialStatus = EPDiskStatus::ACTIVE;
            TPDiskStatus st(initialStatus, DefaultStateLimit, DefaultStateLimits);

            for (ui32 i = 1; i < DefaultStateLimits[state]; ++i) {
                st.AddState(state);

                UNIT_ASSERT(!st.IsChanged());
                UNIT_ASSERT_VALUES_EQUAL(st.GetStatus(), initialStatus);
            }
            st.AddState(state);
            UNIT_ASSERT(st.IsChanged());
            st.ApplyChanges();
            UNIT_ASSERT_VALUES_EQUAL(st.GetStatus(), EPDiskStatus::FAULTY);

            auto it = DefaultStateLimits.find(NKikimrBlobStorage::TPDiskState::Normal);
            const ui32 stateLimit = (it != DefaultStateLimits.end()) ? it->second : DefaultStateLimit;
            for (ui32 i = 1; i < stateLimit; ++i) {
                st.AddState(NKikimrBlobStorage::TPDiskState::Normal);

                if (i == 1) {
                    UNIT_ASSERT(st.IsChanged());
                    st.ApplyChanges();
                } else {
                    UNIT_ASSERT(!st.IsChanged());
                }
                UNIT_ASSERT_VALUES_EQUAL(st.GetStatus(), EPDiskStatus::INACTIVE);
            }
            st.AddState(NKikimrBlobStorage::TPDiskState::Normal);
            UNIT_ASSERT(st.IsChanged());
            st.ApplyChanges();
            UNIT_ASSERT_VALUES_EQUAL(st.GetStatus(), EPDiskStatus::ACTIVE);
        }
    }

    Y_UNIT_TEST(PDiskFaultyState) {
        for (const EPDiskState state : FaultyStates) {
            const EPDiskStatus initialStatus = EPDiskStatus::ACTIVE;
            TPDiskStatus st(initialStatus, DefaultStateLimit, DefaultStateLimits);

            for (ui32 i = 1; i < DefaultStateLimit; ++i) {
                st.AddState(state);

                UNIT_ASSERT(!st.IsChanged());
                UNIT_ASSERT_VALUES_EQUAL(st.GetStatus(), initialStatus);
            }

            st.AddState(state);

            UNIT_ASSERT(st.IsChanged());

            st.ApplyChanges();
            UNIT_ASSERT_VALUES_EQUAL(st.GetStatus(), EPDiskStatus::FAULTY);
        }
    }

    std::pair<TCmsStatePtr, TSentinelState::TPtr> MockCmsState(ui16 numDataCenter, ui16 racksPerDataCenter, ui16 nodesPerRack, ui16 pdisksPerNode, bool anyDC, bool anyRack) {
        TSentinelState::TPtr sentinelState = new TSentinelState;
        TCmsStatePtr state = new TCmsState;
        state->ClusterInfo = new TClusterInfo;

        for (ui64 dc : xrange(numDataCenter)) {
            for (ui64 rack : xrange(racksPerDataCenter)) {
                for (ui64 node : xrange(nodesPerRack)) {
                    const ui64 id = (dc << 32) | (rack << 16) | node;
                    const TString name = TStringBuilder() << "dc_" << dc << "-rack_" << rack << "-node_" << node;

                    NActorsInterconnect::TNodeLocation location;
                    if (!anyDC) {
                        location.SetDataCenter(ToString(dc + 1));
                    }
                    if (!anyRack) {
                        location.SetRack(ToString(rack + 1));
                    }
                    location.SetUnit(ToString(id));

                    state->ClusterInfo->AddNode(TEvInterconnect::TNodeInfo(id, name, name, name, 10000, TNodeLocation(location)), nullptr);
                    sentinelState->Nodes[id] = NSentinel::TNodeInfo{name, NActors::TNodeLocation(location), {}};

                    for (ui64 npdisk : xrange(pdisksPerNode)) {
                        NKikimrBlobStorage::TBaseConfig::TPDisk pdisk;
                        pdisk.SetNodeId(id);
                        pdisk.SetPDiskId(npdisk);
                        pdisk.SetPath(TString("pdisk") + ToString(npdisk) + ".data");
                        state->ClusterInfo->AddPDisk(pdisk);
                    }
                }
            }
        }

        return {state, sentinelState};
    }

    THashSet<TPDiskID, TPDiskIDHash> MapKeys(TClusterMap::TPDiskIgnoredMap& map) {
        THashSet<TPDiskID, TPDiskIDHash> result;

        for (auto& [k, _] : map) {
            result.insert(k);
        }

        return result;
    };

    void GuardianDataCenterRatio(ui16 numDataCenter, const TVector<ui16>& nodesPerDataCenterVariants, bool anyDC = false) {
        UNIT_ASSERT(!anyDC || numDataCenter == 1);

        for (ui16 nodesPerDataCenter : nodesPerDataCenterVariants) {
            auto [state, sentinelState] = MockCmsState(numDataCenter, nodesPerDataCenter, 1, 1, anyDC, false);
            TGuardian all(sentinelState);
            TGuardian changed(sentinelState, 50);
            THashSet<TPDiskID, TPDiskIDHash> changedSet;

            const auto& nodes = state->ClusterInfo->AllNodes();

            TVector<ui32> changedCount(numDataCenter);
            for (const auto& node : nodes) {
                const ui64 nodeId = node.second->NodeId;
                const TPDiskID id(nodeId, 0);

                all.AddPDisk(id);
                if (changedCount[nodeId >> 32]++ < (nodesPerDataCenter / 2)) {
                    changed.AddPDisk(id);
                    changedSet.insert(id);
                }
            }

            TString issues;
            TClusterMap::TPDiskIgnoredMap disallowed;

            UNIT_ASSERT_VALUES_EQUAL(changed.GetAllowedPDisks(all, issues, disallowed), changedSet);
            UNIT_ASSERT(disallowed.empty());
            UNIT_ASSERT(issues.empty());

            changedCount.assign(numDataCenter, 0);
            for (const auto& node : nodes) {
                const ui64 nodeId = node.second->NodeId;
                const TPDiskID id(nodeId, 0);

                if (changedCount[nodeId >> 32]++ < ((nodesPerDataCenter / 2) + 1)) {
                    changed.AddPDisk(id);
                    changedSet.insert(id);
                }
            }

            disallowed.clear();
            if (!anyDC) {
                UNIT_ASSERT(changed.GetAllowedPDisks(all, issues, disallowed).empty());
                UNIT_ASSERT_VALUES_EQUAL(MapKeys(disallowed), changedSet);
                UNIT_ASSERT_STRING_CONTAINS(issues, "due to DataCenterRatio");
            } else {
                UNIT_ASSERT_VALUES_EQUAL(changed.GetAllowedPDisks(all, issues, disallowed), changedSet);
                UNIT_ASSERT(disallowed.empty());
                UNIT_ASSERT(issues.empty());
            }
        }
    }

    Y_UNIT_TEST(GuardianDataCenterRatio) {
        GuardianDataCenterRatio(1, {3, 4, 5});
        GuardianDataCenterRatio(3, {3, 4, 5});
        GuardianDataCenterRatio(1, {3, 4, 5}, true);
    }

    void GuardianRackRatio(ui16 numRacks, const TVector<ui16>& nodesPerRackVariants, ui16 numPDisks, bool anyRack) {
        for (ui16 nodesPerRack : nodesPerRackVariants) {
            auto [state, sentinelState] = MockCmsState(1, numRacks, nodesPerRack, numPDisks, false, anyRack);

            TGuardian all(sentinelState);
            TGuardian changed(sentinelState, 100, 100, 50);
            THashSet<TPDiskID, TPDiskIDHash> changedSet;

            const auto& nodes = state->ClusterInfo->AllNodes();

            TVector<ui32> changedCount(numRacks);
            for (const auto& node : nodes) {
                const ui64 nodeId = node.second->NodeId;
                for (ui16 pdiskId : xrange(numPDisks)) {
                    const TPDiskID id(nodeId, pdiskId);

                    all.AddPDisk(id);
                    if (changedCount[nodeId >> 16]++ < nodesPerRack * numPDisks / 2) {
                        changed.AddPDisk(id);
                        changedSet.insert(id);
                    }
                }
            }

            TString issues;
            TClusterMap::TPDiskIgnoredMap disallowed;

            UNIT_ASSERT_VALUES_EQUAL(changed.GetAllowedPDisks(all, issues, disallowed), changedSet);
            UNIT_ASSERT(disallowed.empty());
            UNIT_ASSERT(issues.empty());

            changedCount.assign(numRacks, 0);
            for (const auto& node : nodes) {
                const ui64 nodeId = node.second->NodeId;
                for (ui16 pdiskId : xrange(numPDisks)) {
                    const TPDiskID id(nodeId, pdiskId);

                    if (changedCount[nodeId >> 16]++ < nodesPerRack * numPDisks / 2 + 1) {
                        changed.AddPDisk(id);
                        changedSet.insert(id);
                    }
                }
            }

            disallowed.clear();
            const auto& allowed = changed.GetAllowedPDisks(all, issues, disallowed);

            if (anyRack || nodesPerRack == 1) {
                UNIT_ASSERT_VALUES_EQUAL(allowed, changedSet);
                UNIT_ASSERT(disallowed.empty());
                UNIT_ASSERT(issues.empty());
            } else {
                UNIT_ASSERT_VALUES_EQUAL(allowed, decltype(allowed){});
                UNIT_ASSERT_VALUES_EQUAL(MapKeys(disallowed), changedSet);
                UNIT_ASSERT_STRING_CONTAINS(issues, "due to RackRatio");
            }
        }
    }

    Y_UNIT_TEST(GuardianRackRatio) {
        for (int anyRack = 0; anyRack < 2; ++anyRack) {
            for (int numRacks = 1; numRacks < 5; ++numRacks) {
                for (int numPDisks = 1; numPDisks < 4; ++numPDisks) {
                    GuardianRackRatio(numRacks, {1, 2, 3, 4, 5}, numPDisks, anyRack);
                }
            }
        }
    }

} // TSentinelBaseTests

Y_UNIT_TEST_SUITE(TSentinelTests) {
    Y_UNIT_TEST(Smoke) {
        TTestEnv env(8, 4);
    }

    Y_UNIT_TEST(PDiskUnknownState) {
        TTestEnv env(8, 4);

        const auto reservedStates = TVector<EPDiskState>{
            NKikimrBlobStorage::TPDiskState::Reserved14,
            NKikimrBlobStorage::TPDiskState::Reserved15,
            NKikimrBlobStorage::TPDiskState::Reserved16,
        };

        for (const auto state : reservedStates) {
            const TPDiskID id = env.RandomPDiskID();
            env.SetPDiskState({id}, state);
        }
    }

    Y_UNIT_TEST(PDiskErrorState) {
        TTestEnv env(8, 4);

        for (const EPDiskState state : ErrorStates) {
            const TPDiskID id = env.RandomPDiskID();

            env.SetPDiskState({id}, state, EPDiskStatus::FAULTY);
            env.SetPDiskState({id}, NKikimrBlobStorage::TPDiskState::Normal, EPDiskStatus::ACTIVE);
        }
    }

    Y_UNIT_TEST(PDiskFaultyState) {
        TTestEnv env(8, 4);

        for (const EPDiskState state : FaultyStates) {
            const TPDiskID id = env.RandomPDiskID();

            for (ui32 i = 1; i < DefaultStateLimit; ++i) {
                env.SetPDiskState({id}, state);
            }

            env.SetPDiskState({id}, state, EPDiskStatus::FAULTY);
            env.SetPDiskState({id}, NKikimrBlobStorage::TPDiskState::Normal, EPDiskStatus::ACTIVE);
        }
    }

    Y_UNIT_TEST(PDiskRackGuardHalfRack) {
        TTestEnv env(16, 4); // 16 nodes are distributed into 8 racks, 2 per rack

        for (const EPDiskState state : ErrorStates) {
            auto pdisks = env.PDisksForRandomNode();

            // disks should become INACTIVE immediately after disk is broken
            env.SetPDiskState(pdisks, state, EPDiskStatus::INACTIVE);
            for (ui32 i = 1; i < DefaultErrorStateLimit - 1; ++i) {
                env.SetPDiskState(pdisks, state);
            }
            // for half of rack pdisks is expected to become FAULTY
            env.SetPDiskState(pdisks, state, EPDiskStatus::FAULTY);

            env.SetPDiskState(pdisks, NKikimrBlobStorage::TPDiskState::Normal, EPDiskStatus::INACTIVE);
            for (ui32 i = 1; i < DefaultStateLimit - 1; ++i) {
                env.SetPDiskState(pdisks, NKikimrBlobStorage::TPDiskState::Normal);
            }
            env.SetPDiskState(pdisks, NKikimrBlobStorage::TPDiskState::Normal, EPDiskStatus::ACTIVE);
        }
    }

    Y_UNIT_TEST(PDiskRackGuardFullRack) {
        TTestEnv env(16, 4); // 16 nodes are distributed into 8 racks, 2 per rack

        for (const EPDiskState state : ErrorStates) {
            auto pdisks = env.PDisksForRandomRack();

            // disks should become INACTIVE immediately after disk is broken
            env.SetPDiskState(pdisks, state, EPDiskStatus::INACTIVE);
            for (ui32 i = 1; i < DefaultErrorStateLimit; ++i) {
                env.SetPDiskState(pdisks, state);
            }

            // for full rack pdisks is not expected to become FAULTY, so they become ACTIVE immediatetly
            // after pdisk becomes Normal
            env.SetPDiskState(pdisks, NKikimrBlobStorage::TPDiskState::Normal, EPDiskStatus::ACTIVE);
        }
    }

    Y_UNIT_TEST(BSControllerUnresponsive) {
        TTestEnv env(8, 4);
        env.EnableNoisyBSCPipe();

        const TPDiskID id1 = env.RandomPDiskID();
        const TPDiskID id2 = env.RandomPDiskID();
        const TPDiskID id3 = env.RandomPDiskID();

        for (size_t i = 0; i < sizeof(ErrorStates) / sizeof(ErrorStates[0]); ++i) {
            env.AddBSCFailures(id1, {false, true});
            env.AddBSCFailures(id2, {false, false, false, false, false, false});
        }

        for (const EPDiskState state : ErrorStates) {
            env.SetPDiskState({id1, id2, id3}, state, EPDiskStatus::FAULTY);
            env.SetPDiskState({id1, id2, id3}, NKikimrBlobStorage::TPDiskState::Normal, EPDiskStatus::ACTIVE);
        }
    }
} // TSentinelTests

}
