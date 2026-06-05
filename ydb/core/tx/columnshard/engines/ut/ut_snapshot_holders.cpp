#include "test_path_id_translator.h"

#include <ydb/core/tx/columnshard/engines/snapshot_holders.h>
#include <ydb/core/tx/columnshard/test_helper/portion_test_helper.h>
#include <ydb/core/tx/long_tx_service/public/snapshot_registry.h>

#include <library/cpp/testing/unittest/registar.h>

#ifndef _win_
#include <sys/wait.h>

#include <cerrno>
#include <unistd.h>
#endif

namespace NKikimr::NOlap {

Y_UNIT_TEST_SUITE(TSnapshotHoldersTests) {
    TSnapshot Step(const ui64 planStep) {
        return TSnapshot(planStep, 1);
    }

    TPortionInfo::TConstPtr MakePortion(const ui64 appearedPlanStep, const std::optional<ui64> removedPlanStep,
        const NColumnShard::TInternalPathId pathId, const ui64 portionId) {
        std::optional<TSnapshot> removeSnapshot;
        if (removedPlanStep) {
            removeSnapshot = Step(*removedPlanStep);
        }
        return NTest::MakeTestCompactedPortion(
            pathId, portionId, portionId * 10, portionId * 10 + 9, 10, Step(appearedPlanStep), removeSnapshot);
    }

    TTrueAtomicSharedPtr<IImmutableSnapshotRegistry> CreateSnapshotRegistry(
        const std::vector<std::pair<NKikimr::TTableId, TRowVersion>>& snapshots = {}) {
        auto registryBuilder = CreateImmutableSnapshotRegistryBuilder();
        for (const auto& [tableId, snapshot] : snapshots) {
            registryBuilder->AddSnapshot({ tableId }, snapshot);
        }
        return TTrueAtomicSharedPtr<IImmutableSnapshotRegistry>(std::move(*registryBuilder).Build().release());
    }

    Y_UNIT_TEST(PortionsCouldBeUsedAfterMinReadSnapshot) {
        // time        0--1--------------------------------10---11
        //                                                 ^ minSnapshotForNewReads
        // p1             [.....................................)
        // p2                                                   [11....................20)
        // p3             [................................)
        const TSnapshotHoldersPerTable holders(Step(10), {});
        const auto pathId = NColumnShard::TInternalPathId::FromRawValue(1);
        const auto p1 = MakePortion(1, 11, pathId, 1);
        const auto p2 = MakePortion(11, 20, pathId, 2);
        const auto p3 = MakePortion(1, 10, pathId, 3);

        UNIT_ASSERT(holders.CouldUsePortion(p1));
        UNIT_ASSERT(holders.CouldUsePortion(p2));
        UNIT_ASSERT(!holders.CouldUsePortion(p3));
    }

    Y_UNIT_TEST(TxCouldUsePortions) {
        // time      0--1---------5-------------10-------------20
        //                        ^ tx                         ^ minSnapshotForNewReads
        // portion1     [.......................)
        // portion2              [5.............)
        const TSnapshotHoldersPerTable holders(Step(20), { Step(5) });
        const auto pathId = NColumnShard::TInternalPathId::FromRawValue(1);
        const auto portion1 = MakePortion(1, 10, pathId, 1);
        const auto portion2 = MakePortion(5, 10, pathId, 2);

        UNIT_ASSERT(holders.CouldUsePortion(portion1));
        UNIT_ASSERT(holders.CouldUsePortion(portion2));
    }

    Y_UNIT_TEST(TxsCouldNotUsePortions) {
        // time        0--1-----4-5-6-----10----12-13----18----20
        //                        ^ tx1         ^ tx2          ^ minSnapshotForNewReads
        // portion1       [1......5)
        // portion2                 [6....10)
        // portion3                                [13..18)
        const TSnapshotHoldersPerTable holders(Step(20), { Step(5), Step(12) });
        const auto pathId = NColumnShard::TInternalPathId::FromRawValue(1);
        const auto portion1 = MakePortion(1, 5, pathId, 1);
        const auto portion2 = MakePortion(6, 10, pathId, 2);
        const auto portion3 = MakePortion(13, 18, pathId, 3);

        UNIT_ASSERT(!holders.CouldUsePortion(portion1));
        UNIT_ASSERT(!holders.CouldUsePortion(portion2));
        UNIT_ASSERT(!holders.CouldUsePortion(portion3));
    }

    Y_UNIT_TEST(LegacySnapshotHoldersCouldUsePortion) {
        const TLegacySnapshotHolders holders(Step(20), { Step(5) });
        const auto pathId = NColumnShard::TInternalPathId::FromRawValue(1);

        {
            // Removed later than min snapshot for new reads -> always usable by new scans.
            auto portion = MakePortion(1, 25, pathId, 1);
            UNIT_ASSERT(holders.CouldUsePortion(portion));
        }

        {
            // Removed before min snapshot, but visible to active tx snapshot (step=5).
            auto portion = MakePortion(1, 10, pathId, 2);
            UNIT_ASSERT(holders.CouldUsePortion(portion));
        }

        {
            // Removed before min snapshot and not visible to active tx snapshot (step=5).
            auto portion = MakePortion(1, 4, pathId, 3);
            UNIT_ASSERT(!holders.CouldUsePortion(portion));
        }
    }

    Y_UNIT_TEST(RegistrySnapshotHoldersCouldUsePortionByActiveSnapshot) {
        const ui64 schemeShardId = 777;
        const auto internalPathId = NColumnShard::TInternalPathId::FromRawValue(2);
        const auto ssPathId = NColumnShard::TSchemeShardLocalPathId::FromRawValue(20);
        auto portion = MakePortion(1, 10, internalPathId, 1);

        NTest::TTestPathIdTranslator translator;
        translator.Add(internalPathId, { ssPathId });

        auto registry = CreateSnapshotRegistry({ { NKikimr::TTableId(schemeShardId, ssPathId.GetRawValue(), 0), TRowVersion(5, 1) } });

        const TRegistrySnapshotHolders holders(Step(20), registry, schemeShardId, translator);
        UNIT_ASSERT(holders.CouldUsePortion(portion));
    }

    Y_UNIT_TEST(RegistrySnapshotHoldersUsesAllSchemeShardLocalPathIdsForPortions) {
        const ui64 schemeShardId = 888;
        const auto internalPathId = NColumnShard::TInternalPathId::FromRawValue(3);
        const auto ssPathId1 = NColumnShard::TSchemeShardLocalPathId::FromRawValue(30);
        const auto ssPathId2 = NColumnShard::TSchemeShardLocalPathId::FromRawValue(31);
        const auto unrelatedSsPathId = NColumnShard::TSchemeShardLocalPathId::FromRawValue(999);
        auto portion = MakePortion(1, 15, internalPathId, 1);

        NTest::TTestPathIdTranslator translator;
        translator.Add(internalPathId, { ssPathId1, ssPathId2 });

        {
            // Scan for an unrelated table cannot see the portion
            auto registry =
                CreateSnapshotRegistry({ { NKikimr::TTableId(schemeShardId, unrelatedSsPathId.GetRawValue(), 0), TRowVersion(9, 1) } });
            const TRegistrySnapshotHolders holders(Step(20), registry, schemeShardId, translator);
            UNIT_ASSERT(!holders.CouldUsePortion(portion));
        }
        {
            // Scan for the first alias but outside portion lifetime [1..15) cannot see the portion
            auto registry = CreateSnapshotRegistry({ { NKikimr::TTableId(schemeShardId, ssPathId1.GetRawValue(), 0), TRowVersion(15, 1) } });
            const TRegistrySnapshotHolders holders(Step(20), registry, schemeShardId, translator);
            UNIT_ASSERT(!holders.CouldUsePortion(portion));
        }
        {
            // Scan for the first alias can see the portion
            auto registry = CreateSnapshotRegistry({ { NKikimr::TTableId(schemeShardId, ssPathId1.GetRawValue(), 0), TRowVersion(9, 1) } });
            const TRegistrySnapshotHolders holders(Step(20), registry, schemeShardId, translator);
            UNIT_ASSERT(holders.CouldUsePortion(portion));
        }
        {
            // Snapshot for the second alias also can see the portion
            auto registry = CreateSnapshotRegistry({ { NKikimr::TTableId(schemeShardId, ssPathId2.GetRawValue(), 0), TRowVersion(9, 1) } });
            const TRegistrySnapshotHolders holders(Step(20), registry, schemeShardId, translator);
            UNIT_ASSERT(holders.CouldUsePortion(portion));
        }
    }

#ifndef _win_
    template <class TCallback>
    int RunInChild(const TCallback& callback) {
        const pid_t pid = fork();
        UNIT_ASSERT_C(pid >= 0, "fork() failed");
        if (pid == 0) {
            callback();
            _exit(0);
        }

        int status = 0;
        while (true) {
            const pid_t waitResult = waitpid(pid, &status, 0);
            if (waitResult == pid) {
                break;
            }
            UNIT_ASSERT_C(waitResult != -1 || errno == EINTR, "waitpid() failed");
        }
        return status;
    }
#endif

    Y_UNIT_TEST(ConstructorVerifiesInvariants) {
#ifndef _win_
        const int unsortedTxs = RunInChild([] {
            [[maybe_unused]] TSnapshotHoldersPerTable holders(Step(20), { Step(12), Step(5) });
        });
        UNIT_ASSERT_C(!WIFEXITED(unsortedTxs) || WEXITSTATUS(unsortedTxs) != 0, "unsorted tx list must fail constructor checks");

        const int tooYoungTx = RunInChild([] {
            [[maybe_unused]] TSnapshotHoldersPerTable holders(Step(20), { Step(20) });
        });
        UNIT_ASSERT_C(!WIFEXITED(tooYoungTx) || WEXITSTATUS(tooYoungTx) != 0, "tx snapshot >= minReadSnapshot must fail constructor checks");
#else
        UNIT_ASSERT_C(true, "POSIX-only test is skipped on Windows");
#endif
    }
}

}   // namespace NKikimr::NOlap
