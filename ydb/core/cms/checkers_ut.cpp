#include "cluster_info.h"
#include "erasure_checkers.h"
#include "node_checkers.h"
#include "ut_helpers.h"
#include "util/string/cast.h"

#include <ydb/core/protos/cms.pb.h>
#include <ydb/public/api/protos/draft/ydb_maintenance.pb.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/ptr.h>
#include <util/generic/vector.h>

#include <bitset>
#include <string>

namespace NKikimr::NCmsTest {

using namespace NCms;
using namespace NKikimrCms;
using namespace Ydb::Maintenance;

TVector<TVDiskID> GenerateDefaultBlock42Group() {
    TVector<TVDiskID> group;
    for (ui32 i = 0; i < 8; ++i) {
        group.push_back(TVDiskID(0, 1, 0, i % 8, 0));
    }
    return group;
}

TVector<TVDiskID> GenerateDefaultMirror3dcGroup() {
    TVector<TVDiskID> group;
    for (ui32 i = 0; i < 9; ++i) {
        group.push_back(TVDiskID(0, 1, i / 3, i % 3, 0));
    }
    return group;
}

Y_UNIT_TEST_SUITE(TCmsCheckersTest) {
    Y_UNIT_TEST(DefaultErasureCheckerAvailabilityMode)
    {
        TDefaultErasureChecker checker(0);

        auto vdisks = GenerateDefaultBlock42Group();
        for (auto vdisk : vdisks) {
            checker.UpdateVDisk(vdisk, UP);
        }
        UNIT_ASSERT_EQUAL(checker.TryToLockVDisk(vdisks[0], MODE_MAX_AVAILABILITY, 0, 0), ActionState::ACTION_REASON_OK);

        checker.UpdateVDisk(vdisks[0], DOWN);

        UNIT_ASSERT_EQUAL(checker.TryToLockVDisk(vdisks[0], MODE_MAX_AVAILABILITY, 0, 0), ActionState::ACTION_REASON_OK);
        UNIT_ASSERT_EQUAL(checker.TryToLockVDisk(vdisks[1], MODE_MAX_AVAILABILITY, 0, 0), ActionState::ACTION_REASON_TOO_MANY_UNAVAILABLE_VDISKS);

        UNIT_ASSERT_EQUAL(checker.TryToLockVDisk(vdisks[0], MODE_KEEP_AVAILABLE, 0, 0), ActionState::ACTION_REASON_OK);
        UNIT_ASSERT_EQUAL(checker.TryToLockVDisk(vdisks[1], MODE_KEEP_AVAILABLE, 0, 0), ActionState::ACTION_REASON_OK);

        checker.LockVDisk(vdisks[0]);

        UNIT_ASSERT_EQUAL(checker.TryToLockVDisk(vdisks[0], MODE_MAX_AVAILABILITY, 0, 0), ActionState::ACTION_REASON_ALREADY_LOCKED);
        UNIT_ASSERT_EQUAL(checker.TryToLockVDisk(vdisks[1], MODE_MAX_AVAILABILITY, 0, 0), ActionState::ACTION_REASON_TOO_MANY_UNAVAILABLE_VDISKS);

        UNIT_ASSERT_EQUAL(checker.TryToLockVDisk(vdisks[0], MODE_KEEP_AVAILABLE, 0, 0), ActionState::ACTION_REASON_ALREADY_LOCKED);
        UNIT_ASSERT_EQUAL(checker.TryToLockVDisk(vdisks[1], MODE_KEEP_AVAILABLE, 0, 0), ActionState::ACTION_REASON_OK);
    }

    Y_UNIT_TEST(Mirror3dcCheckerAvailabilityMode)
    {
        TMirror3dcChecker checker(0);

        auto vdisks = GenerateDefaultMirror3dcGroup();
        for (auto vdisk : vdisks) {
            checker.UpdateVDisk(vdisk, UP);
        }

        // One disabled disk for max availability
        for (ui32 i = 0; i < 9; ++i) {
            UNIT_ASSERT_EQUAL(checker.TryToLockVDisk(vdisks[i], MODE_MAX_AVAILABILITY, 0, 0), ActionState::ACTION_REASON_OK);
            checker.LockVDisk(vdisks[i]);
            UNIT_ASSERT_EQUAL(checker.TryToLockVDisk(vdisks[i], MODE_MAX_AVAILABILITY, 0, 0), ActionState::ACTION_REASON_ALREADY_LOCKED);

            for (ui32 j = 0; j < 9; ++j) {
                if (i == j)
                    continue;

                UNIT_ASSERT_EQUAL(checker.TryToLockVDisk(vdisks[j], MODE_MAX_AVAILABILITY, 0, 0), ActionState::ACTION_REASON_TOO_MANY_UNAVAILABLE_VDISKS);
            }

            checker.UnlockVDisk(vdisks[i]);
        }

        for (ui32 dc = 0; dc < 3; ++dc) {
            // Minus 1 dc
            checker.LockVDisk(vdisks[dc * 3]);
            checker.LockVDisk(vdisks[dc * 3 + 1]);
            checker.LockVDisk(vdisks[dc * 3 + 2]);

            for (ui32 i = 0; i < 9; ++i) {
                if ((i <= dc * 3 + 2) && (i >= dc * 3)) {
                    UNIT_ASSERT_EQUAL(checker.TryToLockVDisk(vdisks[i], MODE_MAX_AVAILABILITY, 0, 0), ActionState::ACTION_REASON_ALREADY_LOCKED);
                    UNIT_ASSERT_EQUAL(checker.TryToLockVDisk(vdisks[i], MODE_KEEP_AVAILABLE, 0, 0), ActionState::ACTION_REASON_ALREADY_LOCKED);
                    continue;
                }
                UNIT_ASSERT_EQUAL(checker.TryToLockVDisk(vdisks[i], MODE_MAX_AVAILABILITY, 0, 0), ActionState::ACTION_REASON_TOO_MANY_UNAVAILABLE_VDISKS);
                UNIT_ASSERT_EQUAL(checker.TryToLockVDisk(vdisks[i], MODE_KEEP_AVAILABLE, 0, 0), ActionState::ACTION_REASON_OK);
            }

            // Minus 2 in dc
            for (ui32 i = 0; i < 3; ++i) {
                checker.UnlockVDisk(vdisks[dc * 3 + i]);

                UNIT_ASSERT_EQUAL(checker.TryToLockVDisk(vdisks[dc * 3 + i], MODE_KEEP_AVAILABLE, 0, 0), ActionState::ACTION_REASON_OK);
                UNIT_ASSERT_EQUAL(checker.TryToLockVDisk(vdisks[dc * 3 + i], MODE_MAX_AVAILABILITY, 0, 0), ActionState::ACTION_REASON_TOO_MANY_UNAVAILABLE_VDISKS);

                for (ui32 j = 0; j < 9; ++j) {
                    if ((j <= dc * 3 + 2) && (j >= dc * 3))
                        continue;

                    UNIT_ASSERT_EQUAL(checker.TryToLockVDisk(vdisks[i], MODE_MAX_AVAILABILITY, 0, 0), ActionState::ACTION_REASON_TOO_MANY_UNAVAILABLE_VDISKS);
                    UNIT_ASSERT_EQUAL(checker.TryToLockVDisk(vdisks[i], MODE_KEEP_AVAILABLE, 0, 0), ActionState::ACTION_REASON_OK);
                }

                checker.LockVDisk(vdisks[dc * 3 + i]);
            }

            checker.UnlockVDisk(vdisks[dc * 3]);
            checker.UnlockVDisk(vdisks[dc * 3 + 1]);
            checker.UnlockVDisk(vdisks[dc * 3 + 2]);
        }

        // Minus 1 in each dc
        for (ui32 i = 0; i < 3; ++i) {
            checker.LockVDisk(vdisks[i]);
            checker.LockVDisk(vdisks[i + 3]);
            checker.LockVDisk(vdisks[i + 6]);

            for (ui32 j = 0; j < 9; ++j) {
                if (j % 3 == i)
                    continue;
                UNIT_ASSERT_EQUAL(checker.TryToLockVDisk(vdisks[j], MODE_MAX_AVAILABILITY, 0, 0), ActionState::ACTION_REASON_TOO_MANY_UNAVAILABLE_VDISKS);
                UNIT_ASSERT_EQUAL(checker.TryToLockVDisk(vdisks[j], MODE_KEEP_AVAILABLE, 0, 0), ActionState::ACTION_REASON_TOO_MANY_UNAVAILABLE_VDISKS);
            }

            checker.UnlockVDisk(vdisks[i]);
            checker.UnlockVDisk(vdisks[i + 3]);
            checker.UnlockVDisk(vdisks[i + 6]);
        }

        checker.UpdateVDisk(vdisks[0], DOWN);

        UNIT_ASSERT_EQUAL(checker.TryToLockVDisk(vdisks[0], MODE_MAX_AVAILABILITY, 0, 0), ActionState::ACTION_REASON_OK);
        UNIT_ASSERT_EQUAL(checker.TryToLockVDisk(vdisks[1], MODE_MAX_AVAILABILITY, 0, 0), ActionState::ACTION_REASON_TOO_MANY_UNAVAILABLE_VDISKS);
    }

    Y_UNIT_TEST(DefaultErasureCheckerPriorities)
    {
        TDefaultErasureChecker checker(0);

        auto vdisks = GenerateDefaultBlock42Group();
        for (auto vdisk : vdisks) {
            checker.UpdateVDisk(vdisk, UP);
        }

        // Check one scheduled task with order
        for (ui32 i = 0; i < 8; ++i) {
            checker.EmplaceTask(vdisks[i], 0, 1, "task-1");

            UNIT_ASSERT_EQUAL(checker.TryToLockVDisk(vdisks[i], MODE_MAX_AVAILABILITY, 0, 2), ActionState::ACTION_REASON_LOW_PRIORITY);
            UNIT_ASSERT_EQUAL(checker.TryToLockVDisk(vdisks[i], MODE_KEEP_AVAILABLE, 0, 2), ActionState::ACTION_REASON_LOW_PRIORITY);

            for (ui32 j = 0; j < 8; ++j) {
                if (j == i)
                    continue;
                UNIT_ASSERT_EQUAL(checker.TryToLockVDisk(vdisks[j], MODE_MAX_AVAILABILITY, 0, 2), ActionState::ACTION_REASON_TOO_MANY_UNAVAILABLE_VDISKS);
                UNIT_ASSERT_EQUAL(checker.TryToLockVDisk(vdisks[j], MODE_KEEP_AVAILABLE, 0, 2), ActionState::ACTION_REASON_OK);
            }

            checker.RemoveTask("task-1");
        }

        // Check two scheduled task with priority and order
        checker.EmplaceTask(vdisks[1], 1, 1, "task-1");
        checker.EmplaceTask(vdisks[2], 2, 1, "task-2");

        // Priority is higher than task-1 but lower than task-2
        UNIT_ASSERT_EQUAL(checker.TryToLockVDisk(vdisks[3], MODE_MAX_AVAILABILITY, 2, 2), ActionState::ACTION_REASON_TOO_MANY_UNAVAILABLE_VDISKS);
        UNIT_ASSERT_EQUAL(checker.TryToLockVDisk(vdisks[3], MODE_KEEP_AVAILABLE, 2, 2), ActionState::ACTION_REASON_OK);

        checker.RemoveTask("task-1");
        checker.RemoveTask("task-2");
    }

    Y_UNIT_TEST(Mirror3dcCheckerPriorities)
    {
        TMirror3dcChecker checker(0);

        auto vdisks = GenerateDefaultMirror3dcGroup();
        for (auto vdisk : vdisks) {
            checker.UpdateVDisk(vdisk, UP);
        }

        // task-1 > task-2 > task-3 > task-4
        checker.EmplaceTask(vdisks[0], 2, 2, "task-1");
        checker.EmplaceTask(vdisks[1], 2, 5, "task-2");
        checker.EmplaceTask(vdisks[2], 1, 2, "task-3");
        checker.EmplaceTask(vdisks[3], 1, 4, "task-4");

        // Highest priority
        UNIT_ASSERT_EQUAL(checker.TryToLockVDisk(vdisks[4], MODE_MAX_AVAILABILITY, 3, 1), ActionState::ACTION_REASON_OK);
        // Blocked by all tasks
        UNIT_ASSERT_EQUAL(checker.TryToLockVDisk(vdisks[4], MODE_KEEP_AVAILABLE, 1, 6), ActionState::ACTION_REASON_TOO_MANY_UNAVAILABLE_VDISKS);
        // Blocked by task-1, task-2, task-3
        UNIT_ASSERT_EQUAL(checker.TryToLockVDisk(vdisks[3], MODE_KEEP_AVAILABLE, 1, 3), ActionState::ACTION_REASON_OK);
    }

    Y_UNIT_TEST(ClusterNodesCounter)
    {
        const ui32 nodeCount = 30;
        TClusterLimitsCounter checker(0, 0);

        for (ui32 i = 1; i <= nodeCount; ++i) {
            checker.UpdateNode(i, UP);
        }

        // Without limit allow all nodes
        for (ui32 i = 1; i < nodeCount; ++i) {
            checker.LockNode(i);
        }

        UNIT_ASSERT_EQUAL(checker.TryToLockNode(nodeCount, MODE_MAX_AVAILABILITY, 0, 0), ActionState::ACTION_REASON_OK);
        UNIT_ASSERT_EQUAL(checker.TryToLockNode(nodeCount, MODE_KEEP_AVAILABLE, 0, 0), ActionState::ACTION_REASON_OK);

        for (ui32 i = 1; i < nodeCount; ++i) {
            checker.UnlockNode(i);
        }

        // Limit 15 nodes
        checker.ApplyLimits(15, 0);
        for (ui32 i = 1; i < 15; ++i) {
            checker.LockNode(i);
        }

        UNIT_ASSERT_EQUAL(checker.TryToLockNode(17, MODE_MAX_AVAILABILITY, 0, 0), ActionState::ACTION_REASON_OK);
        UNIT_ASSERT_EQUAL(checker.TryToLockNode(17, MODE_KEEP_AVAILABLE, 0, 0), ActionState::ACTION_REASON_OK);

        checker.ApplyLimits(0, 50);

        UNIT_ASSERT_EQUAL(checker.TryToLockNode(17, MODE_MAX_AVAILABILITY, 0, 0), ActionState::ACTION_REASON_OK);
        UNIT_ASSERT_EQUAL(checker.TryToLockNode(17, MODE_KEEP_AVAILABLE, 0, 0), ActionState::ACTION_REASON_OK);

        checker.LockNode(15);
        checker.ApplyLimits(15, 0);

        UNIT_ASSERT_EQUAL(checker.TryToLockNode(17, MODE_MAX_AVAILABILITY, 0, 0), ActionState::ACTION_REASON_DISABLED_NODES_LIMIT_REACHED);
        UNIT_ASSERT_EQUAL(checker.TryToLockNode(17, MODE_KEEP_AVAILABLE, 0, 0), ActionState::ACTION_REASON_DISABLED_NODES_LIMIT_REACHED);

        checker.ApplyLimits(0, 50);

        for (ui32 i = 1; i <= 15; ++i) {
            checker.UnlockNode(i);
        }

        checker.ApplyLimits(0, 0);
        for (ui32 i = 1; i < nodeCount; ++i) {
            checker.EmplaceTask(i, 1, 3, "task-1");
        }

        UNIT_ASSERT_EQUAL(checker.TryToLockNode(nodeCount, MODE_MAX_AVAILABILITY, 1, 1), ActionState::ACTION_REASON_OK);
        UNIT_ASSERT_EQUAL(checker.TryToLockNode(nodeCount, MODE_KEEP_AVAILABLE, 1, 1), ActionState::ACTION_REASON_OK);

        UNIT_ASSERT_EQUAL(checker.TryToLockNode(nodeCount, MODE_MAX_AVAILABILITY, 1, 4), ActionState::ACTION_REASON_OK);
        UNIT_ASSERT_EQUAL(checker.TryToLockNode(nodeCount, MODE_KEEP_AVAILABLE, 1, 4), ActionState::ACTION_REASON_OK);

        checker.RemoveTask("task-1");

        checker.ApplyLimits(15, 0);
        for (ui32 i = 1; i < 15; ++i) {
            checker.EmplaceTask(i, 1, 3, "task-2");
        }
        checker.EmplaceTask(15, 1, 4, "task-3");

        UNIT_ASSERT_EQUAL(checker.TryToLockNode(17, MODE_MAX_AVAILABILITY, 1, 1), ActionState::ACTION_REASON_OK);
        UNIT_ASSERT_EQUAL(checker.TryToLockNode(17, MODE_KEEP_AVAILABLE, 1, 1), ActionState::ACTION_REASON_OK);

        UNIT_ASSERT_EQUAL(checker.TryToLockNode(17, MODE_MAX_AVAILABILITY, 1, 5), ActionState::ACTION_REASON_DISABLED_NODES_LIMIT_REACHED);
        UNIT_ASSERT_EQUAL(checker.TryToLockNode(17, MODE_KEEP_AVAILABLE, 1, 5), ActionState::ACTION_REASON_DISABLED_NODES_LIMIT_REACHED);

        checker.RemoveTask("task-2");
        checker.RemoveTask("task-3");
    }

}
}
