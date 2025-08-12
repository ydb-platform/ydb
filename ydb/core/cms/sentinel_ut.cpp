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

    void AddState(TPDiskStatus& st, const EPDiskState state) {
        st.AddState(state, false);
    }

    void AddStateNodeLocked(TPDiskStatus& st, const EPDiskState state) {
        st.AddState(state, true);
    }

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
            TPDiskStatus st(status, DefaultStateLimit, GoodStateLimit, DefaultStateLimits);

            UNIT_ASSERT(!st.IsChanged());
            UNIT_ASSERT_VALUES_EQUAL(st.GetStatus(), status);
        }
    }

    Y_UNIT_TEST(PDiskErrorState) {
        for (const EPDiskState state : ErrorStates) {
            const EPDiskStatus initialStatus = EPDiskStatus::ACTIVE;
            TPDiskStatus st(initialStatus, DefaultStateLimit, GoodStateLimit, DefaultStateLimits);

            for (ui32 i = 1; i < DefaultStateLimits[state]; ++i) {
                AddState(st, state);

                UNIT_ASSERT(!st.IsChanged());
                UNIT_ASSERT_VALUES_EQUAL(st.GetStatus(), initialStatus);
            }
            AddState(st, state);
            UNIT_ASSERT(st.IsChanged());

            st.ApplyChanges();
            UNIT_ASSERT_VALUES_EQUAL(st.GetStatus(), EPDiskStatus::FAULTY);
        }
    }

    Y_UNIT_TEST(PDiskInactiveAfterStateChange) {
        for (const EPDiskState state : ErrorStates) {
            const EPDiskStatus initialStatus = EPDiskStatus::ACTIVE;
            TPDiskStatus st(initialStatus, DefaultStateLimit, GoodStateLimit, DefaultStateLimits);

            for (ui32 i = 1; i < DefaultStateLimits[state]; ++i) {
                AddState(st, state);

                UNIT_ASSERT(!st.IsChanged());
                UNIT_ASSERT_VALUES_EQUAL(st.GetStatus(), initialStatus);
            }
            AddState(st, state);
            UNIT_ASSERT(st.IsChanged());
            st.ApplyChanges();
            UNIT_ASSERT_VALUES_EQUAL(st.GetStatus(), EPDiskStatus::FAULTY);

            auto it = DefaultStateLimits.find(NKikimrBlobStorage::TPDiskState::Normal);
            const ui32 stateLimit = (it != DefaultStateLimits.end()) ? it->second : DefaultStateLimit;
            for (ui32 i = 1; i < stateLimit; ++i) {
                AddState(st, NKikimrBlobStorage::TPDiskState::Normal);

                if (i == 1) {
                    UNIT_ASSERT(st.IsChanged());
                    st.ApplyChanges();
                } else {
                    UNIT_ASSERT(!st.IsChanged());
                }
                UNIT_ASSERT_VALUES_EQUAL(st.GetStatus(), EPDiskStatus::INACTIVE);
            }
            AddState(st, NKikimrBlobStorage::TPDiskState::Normal);
            UNIT_ASSERT(st.IsChanged());
            st.ApplyChanges();
            UNIT_ASSERT_VALUES_EQUAL(st.GetStatus(), EPDiskStatus::ACTIVE);
        }
    }

    Y_UNIT_TEST(PDiskFaultyState) {
        for (const EPDiskState state : FaultyStates) {
            const EPDiskStatus initialStatus = EPDiskStatus::ACTIVE;
            TPDiskStatus st(initialStatus, DefaultStateLimit, GoodStateLimit, DefaultStateLimits);

            for (ui32 i = 1; i < DefaultStateLimit; ++i) {
                AddState(st, state);

                UNIT_ASSERT(!st.IsChanged());
                UNIT_ASSERT_VALUES_EQUAL(st.GetStatus(), initialStatus);
            }

            AddState(st, state);

            UNIT_ASSERT(st.IsChanged());

            st.ApplyChanges();
            UNIT_ASSERT_VALUES_EQUAL(st.GetStatus(), EPDiskStatus::FAULTY);
        }
    }

    Y_UNIT_TEST(PDiskStateChangeNormalFlow) {
        // If disk has only been in good state, then change to Normal state within GoodStateLimit
        const EPDiskStatus initialStatus = EPDiskStatus::ACTIVE;
        const ui32 defaultStateLimit = 60;
        TPDiskStatus st(initialStatus, defaultStateLimit, GoodStateLimit, DefaultStateLimits);

        AddState(st, NKikimrBlobStorage::TPDiskState::Initial);
        UNIT_ASSERT(!st.IsChanged());

        AddState(st, NKikimrBlobStorage::TPDiskState::InitialFormatRead);
        UNIT_ASSERT(st.IsChanged());
        st.ApplyChanges();
        UNIT_ASSERT_VALUES_EQUAL(st.GetStatus(), EPDiskStatus::INACTIVE);

        AddState(st, NKikimrBlobStorage::TPDiskState::InitialSysLogRead);
        UNIT_ASSERT(!st.IsChanged());

        AddState(st, NKikimrBlobStorage::TPDiskState::InitialCommonLogRead);
        UNIT_ASSERT(!st.IsChanged());

        for (ui32 i = 0; i < GoodStateLimit - 1; ++i) {
            AddState(st, NKikimrBlobStorage::TPDiskState::Normal);
            UNIT_ASSERT(!st.IsChanged());
        }

        AddState(st, NKikimrBlobStorage::TPDiskState::Normal);

        UNIT_ASSERT(st.IsChanged());
        st.ApplyChanges();
        UNIT_ASSERT_VALUES_EQUAL(st.GetStatus(), EPDiskStatus::ACTIVE);
    }

    Y_UNIT_TEST(PDiskStateChangeNodePermanentlyBad) {
        // If node is restarting all the time, then disk should never become ACTIVE
        const EPDiskStatus initialStatus = EPDiskStatus::INACTIVE;
        const ui32 defaultStateLimit = 60;
        TPDiskStatus st(initialStatus, defaultStateLimit, GoodStateLimit, DefaultStateLimits);

        AddState(st, NKikimrBlobStorage::TPDiskState::Unknown);
        UNIT_ASSERT(!st.IsChanged());
        UNIT_ASSERT_VALUES_EQUAL(st.GetStatus(), EPDiskStatus::INACTIVE);

        auto goodStates = {
            NKikimrBlobStorage::TPDiskState::InitialFormatRead,
            NKikimrBlobStorage::TPDiskState::InitialSysLogRead,
            NKikimrBlobStorage::TPDiskState::InitialCommonLogRead,
            NKikimrBlobStorage::TPDiskState::Normal,
        };

        for (ui32 i = 0; i < 10; ++i) {
            for (auto state : goodStates) {
                for (ui32 j = 0; j < 2; ++j) {
                    AddState(st, state);
                    UNIT_ASSERT(!st.IsChanged());
                }
            }

            AddState(st, NKikimrBlobStorage::TPDiskState::NodeDisconnected);
            UNIT_ASSERT(!st.IsChanged());
        }
    }

    Y_UNIT_TEST(PDiskStateChangeNodeNotExpectedRestart) {
        // Node restarts and it is not planned, so disk should become ACTIVE only after defaultStateLimit
        const EPDiskStatus initialStatus = EPDiskStatus::INACTIVE;
        const ui32 defaultStateLimit = 60;
        TPDiskStatus st(initialStatus, defaultStateLimit, GoodStateLimit, DefaultStateLimits);

        auto nodeStartFn = [&st]() {
            AddState(st, NKikimrBlobStorage::TPDiskState::Unknown);
            UNIT_ASSERT(!st.IsChanged());
            UNIT_ASSERT_VALUES_EQUAL(st.GetStatus(), EPDiskStatus::INACTIVE);

            AddState(st, NKikimrBlobStorage::TPDiskState::InitialFormatRead);
            UNIT_ASSERT(!st.IsChanged());

            AddState(st, NKikimrBlobStorage::TPDiskState::InitialSysLogRead);
            UNIT_ASSERT(!st.IsChanged());

            AddState(st, NKikimrBlobStorage::TPDiskState::InitialSysLogRead);
            UNIT_ASSERT(!st.IsChanged());

            for (ui32 i = 0; i < GoodStateLimit; ++i) {
                AddState(st, NKikimrBlobStorage::TPDiskState::Normal);
            }
            UNIT_ASSERT(!st.IsChanged());

            for (ui32 i = 0; i < defaultStateLimit - GoodStateLimit; ++i) {
                AddState(st, NKikimrBlobStorage::TPDiskState::Normal);
            }

            UNIT_ASSERT(st.IsChanged());
            st.ApplyChanges();
            UNIT_ASSERT_VALUES_EQUAL(st.GetStatus(), EPDiskStatus::ACTIVE);
        };

        nodeStartFn();

        AddState(st, NKikimrBlobStorage::TPDiskState::NodeDisconnected);

        UNIT_ASSERT(st.IsChanged());
        st.ApplyChanges();
        UNIT_ASSERT_VALUES_EQUAL(st.GetStatus(), EPDiskStatus::INACTIVE);

        nodeStartFn();
    }

    Y_UNIT_TEST(PDiskStateChangeNodeExpectedRestart) {
        // Node restarts, but it is planned (node is locked by CMS), so disk should become ACTIVE after GoodStateLimit
        const EPDiskStatus initialStatus = EPDiskStatus::INACTIVE;
        const ui32 defaultStateLimit = 60;
        TPDiskStatus st(initialStatus, defaultStateLimit, GoodStateLimit, DefaultStateLimits);

        auto nodeStartFn = [&st]() {
            AddStateNodeLocked(st, NKikimrBlobStorage::TPDiskState::Unknown);
            UNIT_ASSERT(!st.IsChanged());
            UNIT_ASSERT_VALUES_EQUAL(st.GetStatus(), EPDiskStatus::INACTIVE);

            AddStateNodeLocked(st, NKikimrBlobStorage::TPDiskState::InitialFormatRead);
            UNIT_ASSERT(!st.IsChanged());

            AddStateNodeLocked(st, NKikimrBlobStorage::TPDiskState::InitialSysLogRead);
            UNIT_ASSERT(!st.IsChanged());

            AddStateNodeLocked(st, NKikimrBlobStorage::TPDiskState::InitialSysLogRead);
            UNIT_ASSERT(!st.IsChanged());

            for (ui32 i = 0; i < GoodStateLimit; ++i) {
                AddStateNodeLocked(st, NKikimrBlobStorage::TPDiskState::Normal);
            }

            UNIT_ASSERT(st.IsChanged());
            st.ApplyChanges();
            UNIT_ASSERT_VALUES_EQUAL(st.GetStatus(), EPDiskStatus::ACTIVE);
        };

        nodeStartFn();

        AddStateNodeLocked(st, NKikimrBlobStorage::TPDiskState::NodeDisconnected);

        UNIT_ASSERT(st.IsChanged());
        st.ApplyChanges();
        UNIT_ASSERT_VALUES_EQUAL(st.GetStatus(), EPDiskStatus::INACTIVE);

        nodeStartFn();
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
                    sentinelState->Nodes[id] = NSentinel::TNodeInfo{
                        .Host = name,
                        .Location = NActors::TNodeLocation(location),
                        .PileId = Nothing(),
                        .Markers = {},
                    };

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

    void GuardianBadPDisksByNode(ui32 shelvesPerNode, ui32 disksPerShelf, ui32 badDisks) {
        ui32 disksPerNode = shelvesPerNode * disksPerShelf;
        ui32 maxFaultyDisksPerNode = disksPerShelf - 1;

        auto [state, sentinelState] = MockCmsState(1, 8, 1, disksPerNode, true, false);
        TClusterMap all(sentinelState);

        TGuardian changed(sentinelState, 100, 100, 100, 100, maxFaultyDisksPerNode);

        const auto& nodes = state->ClusterInfo->AllNodes();

        for (const auto& node : nodes) {
            const ui64 nodeId = node.second->NodeId;

            for (ui32 i = 0; i < disksPerNode; i++) {
                const TPDiskID id(nodeId, i);

                if (i < badDisks) {
                    all.AddPDisk(id, false);
                    changed.AddPDisk(id, false);
                } else {
                    all.AddPDisk(id);
                }
            }
        }

        TString issues;
        TClusterMap::TPDiskIgnoredMap disallowed;

        auto allowed = changed.GetAllowedPDisks(all, issues, disallowed);

        THashMap<ui64, ui32> allowedDisksByNode;
        THashMap<ui64, ui32> disallowedDisksByNode;

        for (const auto& id : allowed) {
            allowedDisksByNode[id.NodeId]++;
        }

        for (const auto& kv : disallowed) {
            UNIT_ASSERT(kv.second == NKikimrCms::TPDiskInfo::TOO_MANY_FAULTY_PER_NODE);
            disallowedDisksByNode[kv.first.NodeId]++;
        }

        for (const auto& node : nodes) {
            const ui64 nodeId = node.second->NodeId;
            if (badDisks <= maxFaultyDisksPerNode) {
                UNIT_ASSERT_VALUES_EQUAL(allowedDisksByNode[nodeId], badDisks);
                UNIT_ASSERT_VALUES_EQUAL(disallowedDisksByNode[nodeId], 0);
            } else {
                UNIT_ASSERT_VALUES_EQUAL(allowedDisksByNode[nodeId], 0);
                UNIT_ASSERT_VALUES_EQUAL(disallowedDisksByNode[nodeId], badDisks);
            }
        }
    }

    Y_UNIT_TEST(GuardianFaultyPDisks) {
        for (ui32 i = 0; i < 56; i++) {
            GuardianBadPDisksByNode(2, 28, i);
        }
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
            NKikimrBlobStorage::TPDiskState::Reserved15,
            NKikimrBlobStorage::TPDiskState::Reserved16,
            NKikimrBlobStorage::TPDiskState::Reserved17,
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

    Y_UNIT_TEST(PDiskPileGuardHalfPile) {
        TTestEnv env(8, 4);
        env.MockBridgeModePiles(2);

        auto pdisks = env.PDisksForRandomPile();

        // erase exactly half of pile
        std::erase_if(pdisks, [&](TPDiskID id){ return id.NodeId - env.GetFirstNodeId() <= 4; });

        // disks should become INACTIVE immediately after disk is broken
        env.SetPDiskState(pdisks, ErrorStates[0], EPDiskStatus::INACTIVE);
        for (ui32 i = 1; i < DefaultErrorStateLimit - 1; ++i) {
            env.SetPDiskState(pdisks, ErrorStates[0]);
        }
        // for half of pile pdisks are expected to become FAULTY
        env.SetPDiskState(pdisks, ErrorStates[0], EPDiskStatus::FAULTY);

        env.SetPDiskState(pdisks, NKikimrBlobStorage::TPDiskState::Normal, EPDiskStatus::INACTIVE);
        for (ui32 i = 1; i < DefaultStateLimit - 1; ++i) {
            env.SetPDiskState(pdisks, NKikimrBlobStorage::TPDiskState::Normal);
        }
        env.SetPDiskState(pdisks, NKikimrBlobStorage::TPDiskState::Normal, EPDiskStatus::ACTIVE);
    }

    Y_UNIT_TEST(PDiskPileGuardFullPile) {
        TTestEnv env(8, 4);
        env.MockBridgeModePiles(2);

        auto pdisks = env.PDisksForRandomPile();

        // disks should become INACTIVE immediately after disk is broken
        env.SetPDiskState(pdisks, ErrorStates[0], EPDiskStatus::INACTIVE);
        for (ui32 i = 1; i < DefaultErrorStateLimit; ++i) {
            env.SetPDiskState(pdisks, ErrorStates[0]);
        }

        // for full pile pdisks are not expected to become FAULTY, so they become ACTIVE immediately
        // after pdisk becomes Normal
        env.SetPDiskState(pdisks, NKikimrBlobStorage::TPDiskState::Normal, EPDiskStatus::ACTIVE);
    }

    Y_UNIT_TEST(PDiskPileGuardConfig) {
        NKikimrCms::TCmsConfig config;
        config.MutableSentinelConfig()->SetPileRatio(30);
        TTestEnv env(8, 4, config);
        env.MockBridgeModePiles(2);

        auto pdisks = env.PDisksForRandomPile();

        // erase exactly half of pile
        std::erase_if(pdisks, [&](TPDiskID id){ return id.NodeId - env.GetFirstNodeId() <= 4; });

        // disks should become INACTIVE immediately after disk is broken
        env.SetPDiskState(pdisks, ErrorStates[0], EPDiskStatus::INACTIVE);
        for (ui32 i = 1; i < DefaultErrorStateLimit; ++i) {
            env.SetPDiskState(pdisks, ErrorStates[0]);
        }

        // for half of pile pdisks are not expected to become FAULTY because of config,
        // so they become ACTIVE immediatetly after pdisk becomes Normal
        env.SetPDiskState(pdisks, NKikimrBlobStorage::TPDiskState::Normal, EPDiskStatus::ACTIVE);
    }

    Y_UNIT_TEST(PDiskPileGuardWithoutBridgeMode) {
        NKikimrCms::TCmsConfig config;
        config.MutableSentinelConfig()->SetPileRatio(1); // very low ratio
        TTestEnv env(8, 4, config);

        auto pdisks = env.PDisksForRandomNode();

        // disks should become INACTIVE immediately after disk is broken
        env.SetPDiskState(pdisks, ErrorStates[0], EPDiskStatus::INACTIVE);
        for (ui32 i = 1; i < DefaultErrorStateLimit - 1; ++i) {
            env.SetPDiskState(pdisks, ErrorStates[0]);
        }
        // Without bridge mode pdisks are expected to become FAULTY
        env.SetPDiskState(pdisks, ErrorStates[0], EPDiskStatus::FAULTY);

        env.SetPDiskState(pdisks, NKikimrBlobStorage::TPDiskState::Normal, EPDiskStatus::INACTIVE);
        for (ui32 i = 1; i < DefaultStateLimit - 1; ++i) {
            env.SetPDiskState(pdisks, NKikimrBlobStorage::TPDiskState::Normal);
        }
        env.SetPDiskState(pdisks, NKikimrBlobStorage::TPDiskState::Normal, EPDiskStatus::ACTIVE);
    }

    Y_UNIT_TEST(PDiskFaultyGuard) {
        ui32 nodes = 2;
        ui32 disksPerShelf = 5;
        ui32 disksPerNode = 2 * disksPerShelf;

        for (auto wholeShelfFailure : {true, false}) {
            NKikimrCms::TCmsConfig config;

            config.MutableSentinelConfig()->SetFaultyPDisksThresholdPerNode(disksPerShelf - 1);
            TTestEnv env(nodes, disksPerNode, config);
            env.SetLogPriority(NKikimrServices::CMS, NLog::PRI_ERROR);

            for (ui32 nodeIdx = 0; nodeIdx < nodes; ++nodeIdx) {
                ui32 badDisks = wholeShelfFailure ? disksPerShelf : disksPerShelf / 2;

                for (ui32 pdiskIdx = 0; pdiskIdx < badDisks - 1; ++pdiskIdx) {
                    const TPDiskID id = env.PDiskId(nodeIdx, pdiskIdx);

                    env.SetPDiskState({id}, FaultyStates[0]);
                }

                // Next disk (last badDisk)
                const TPDiskID id = env.PDiskId(nodeIdx, badDisks);

                bool targetSeenFaulty = false;

                auto observerHolder = env.AddObserver<TEvBlobStorage::TEvControllerConfigRequest>([&](TEvBlobStorage::TEvControllerConfigRequest::TPtr& event) {
                    const auto& request = event->Get()->Record;
                    for (const auto& command : request.GetRequest().GetCommand()) {
                        if (command.HasUpdateDriveStatus()) {
                            const auto& update = command.GetUpdateDriveStatus();
                            ui32 nodeId = update.GetHostKey().GetNodeId();
                            ui32 pdiskId = update.GetPDiskId();

                            if (id.NodeId == nodeId && id.DiskId == pdiskId) {
                                if (update.GetStatus() == NKikimrBlobStorage::EDriveStatus::FAULTY) {
                                    targetSeenFaulty = true;
                                }
                            }
                        }
                    }
                });

                for (ui32 i = 1; i < DefaultErrorStateLimit + 1; ++i) { // More than DefaultErrorStateLimit just to be sure
                    env.SetPDiskState({id}, FaultyStates[0]);
                }

                env.SimulateSleep(TDuration::Minutes(5));

                observerHolder.Remove();

                if (wholeShelfFailure) {
                    UNIT_ASSERT_C(!targetSeenFaulty, "Faulty state should not have been sent to BS controller because whole shelf failed");
                } else {
                    UNIT_ASSERT_C(targetSeenFaulty, "Faulty state should have been sent to BS controller");
                }
            }
        }
    }

    Y_UNIT_TEST(PDiskFaultyGuardWithForced) {
        ui32 nodes = 2;
        ui32 disksPerShelf = 5;
        ui32 disksPerNode = 2 * disksPerShelf;

        NKikimrCms::TCmsConfig config;

        config.MutableSentinelConfig()->SetFaultyPDisksThresholdPerNode(disksPerShelf - 1);
        TTestEnv env(nodes, disksPerNode, config);
        env.SetLogPriority(NKikimrServices::CMS, NLog::PRI_ERROR);

        std::map<ui32, std::set<ui32>> faultyDisks;

        auto observerHolder = env.AddObserver<TEvBlobStorage::TEvControllerConfigRequest>([&](TEvBlobStorage::TEvControllerConfigRequest::TPtr& event) {
            const auto& request = event->Get()->Record;
            for (const auto& command : request.GetRequest().GetCommand()) {
                if (command.HasUpdateDriveStatus()) {
                    const auto& update = command.GetUpdateDriveStatus();
                    ui32 nodeId = update.GetHostKey().GetNodeId();
                    ui32 pdiskId = update.GetPDiskId();

                    faultyDisks[nodeId].insert(pdiskId);
                }
            }
        });

        for (ui32 nodeIdx = 0; nodeIdx < nodes; ++nodeIdx) {
            env.SetNodeFaulty(env.GetNodeId(nodeIdx), true);

            env.SimulateSleep(TDuration::Minutes(5));
        }

        observerHolder.Remove();

        for (ui32 nodeIdx = 0; nodeIdx < nodes; ++nodeIdx) {
            ui32 nodeId = env.GetNodeId(nodeIdx);

            UNIT_ASSERT_VALUES_EQUAL(faultyDisks[nodeId].size(), disksPerNode);
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

    Y_UNIT_TEST(NodeStatusComputer) {
        TNodeStatusComputer computer{
            .BadStateLimit = 5,
            .GoodStateLimit = 5,
            .PrettyGoodStateLimit = 3,
        };
        UNIT_ASSERT(computer.GetCurrentNodeState() == TNodeStatusComputer::ENodeState::GOOD);
        for (ui32 _ : xrange(2)) {
            computer.AddState(TNodeStatusComputer::ENodeState::GOOD);
            UNIT_ASSERT(!computer.Compute());
            UNIT_ASSERT(computer.GetCurrentNodeState() == TNodeStatusComputer::ENodeState::MAY_BE_GOOD);
        }
        for (ui32 _ : xrange(2)) {
            computer.AddState(TNodeStatusComputer::ENodeState::GOOD);
            UNIT_ASSERT(!computer.Compute());
            UNIT_ASSERT(computer.ActualState == TNodeStatusComputer::ENodeState::PRETTY_GOOD);
            UNIT_ASSERT(computer.GetCurrentNodeState() == TNodeStatusComputer::ENodeState::GOOD);
        }
        computer.AddState(TNodeStatusComputer::ENodeState::GOOD);

        UNIT_ASSERT(computer.Compute());
        for (ui32 _ : xrange(4)) {
            UNIT_ASSERT(computer.GetCurrentNodeState() == TNodeStatusComputer::ENodeState::GOOD);
            computer.AddState(TNodeStatusComputer::ENodeState::GOOD);
            UNIT_ASSERT(!computer.Compute());
        }
        for (ui32 _ : xrange(4)) {
            computer.AddState(TNodeStatusComputer::ENodeState::BAD);
            UNIT_ASSERT(!computer.Compute());
            UNIT_ASSERT(computer.GetCurrentNodeState() == TNodeStatusComputer::ENodeState::MAY_BE_BAD);
        }
        computer.AddState(TNodeStatusComputer::ENodeState::BAD);
        UNIT_ASSERT(computer.Compute());
        UNIT_ASSERT(computer.GetCurrentNodeState() == TNodeStatusComputer::ENodeState::BAD);
        for (ui32 _ : xrange(6)) {
            computer.AddState(TNodeStatusComputer::ENodeState::BAD);
            UNIT_ASSERT(!computer.Compute());
            UNIT_ASSERT(computer.GetCurrentNodeState() == TNodeStatusComputer::ENodeState::BAD);
        }
        for (ui32 _ : xrange(6)) {
            computer.AddState(TNodeStatusComputer::ENodeState::GOOD);
            UNIT_ASSERT(!computer.Compute());
            UNIT_ASSERT(computer.GetCurrentNodeState() == TNodeStatusComputer::ENodeState::MAY_BE_GOOD);
            computer.AddState(TNodeStatusComputer::ENodeState::BAD);
            UNIT_ASSERT(!computer.Compute());
            UNIT_ASSERT(computer.GetCurrentNodeState() == TNodeStatusComputer::ENodeState::MAY_BE_BAD);
        }
        for (ui32 _ : xrange(2)) {
            computer.AddState(TNodeStatusComputer::ENodeState::GOOD);
            UNIT_ASSERT(!computer.Compute());
            UNIT_ASSERT(computer.GetCurrentNodeState() == TNodeStatusComputer::ENodeState::MAY_BE_GOOD);
        }
        for (ui32 _ : xrange(2)) {
            computer.AddState(TNodeStatusComputer::ENodeState::GOOD);
            UNIT_ASSERT(!computer.Compute());
            UNIT_ASSERT(computer.ActualState == TNodeStatusComputer::ENodeState::PRETTY_GOOD);
            UNIT_ASSERT(computer.GetCurrentNodeState() == TNodeStatusComputer::ENodeState::GOOD);
        }
        computer.AddState(TNodeStatusComputer::ENodeState::GOOD);
        UNIT_ASSERT(computer.Compute());
        UNIT_ASSERT(computer.GetCurrentNodeState() == TNodeStatusComputer::ENodeState::GOOD);
    }

} // TSentinelTests

}
