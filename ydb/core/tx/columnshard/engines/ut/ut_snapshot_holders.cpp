#include "test_path_id_translator.h"

#include <ydb/core/tx/columnshard/engines/snapshot_holders.h>
#include <ydb/core/tx/columnshard/tables_manager.h>
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

    struct TMyPortion {
        TSnapshot Appeared;
        TSnapshot Removed;

        TMyPortion(const ui64 appearedPlanStep, const ui64 removedPlanStep)
            : Appeared(appearedPlanStep, 1)
            , Removed(removedPlanStep, 1)
        {
        }
    };

    bool CouldUse(const TSnapshotHoldersPerTable& holders, const TMyPortion& portion) {
        return holders.CouldUse(
            [&portion](const TSnapshot& heldSnapshot) {
                return portion.Removed <= heldSnapshot;
            },
            [&portion](const TSnapshot& heldSnapshot) {
                return portion.Appeared <= heldSnapshot;
            });
    }

    NColumnShard::TTableInfo MakeTable(
        const ui64 internalPathIdRaw, const ui64 ssPathIdRaw, const ui64 firstVersionSnapshotStep, const ui64 dropSnapshotStep) {
        const auto internalPathId = NColumnShard::TInternalPathId::FromRawValue(internalPathIdRaw);
        const auto ssPathId = NColumnShard::TSchemeShardLocalPathId::FromRawValue(ssPathIdRaw);
        NColumnShard::TTableInfo table({ NColumnShard::TUnifiedPathId::BuildValid(internalPathId, ssPathId) });
        table.AddVersion(Step(firstVersionSnapshotStep));
        table.SetDropVersion(ssPathId, Step(dropSnapshotStep));
        return table;
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
        const TMyPortion p1(1, 11);
        const TMyPortion p2(11, 20);
        const TMyPortion p3(1, 10);

        UNIT_ASSERT(CouldUse(holders, p1));
        UNIT_ASSERT(CouldUse(holders, p2));
        UNIT_ASSERT(!CouldUse(holders, p3));
    }

    Y_UNIT_TEST(TxCouldUseAPortions) {
        // time      0--1---------5-------------10-------------20
        //                        ^ tx                         ^ minSnapshotForNewReads
        // portion1     [.......................)
        // portion2              [5.............)
        const TSnapshotHoldersPerTable holders(Step(20), { Step(5) });
        const TMyPortion portion1(1, 10);
        const TMyPortion portion2(5, 10);

        UNIT_ASSERT(CouldUse(holders, portion1));
        UNIT_ASSERT(CouldUse(holders, portion2));
    }

    Y_UNIT_TEST(TxsCouldNotUsePortions) {
        // time        0--1-----4-5-6-----10----12-13----18----20
        //                        ^ tx1         ^ tx2          ^ minSnapshotForNewReads
        // portion1       [1......5)
        // portion2                 [6....10)
        // portion3                                [13..18)
        const TSnapshotHoldersPerTable holders(Step(20), { Step(5), Step(12) });
        const TMyPortion portion1(1, 5);
        const TMyPortion portion2(6, 10);
        const TMyPortion portion3(13, 18);

        UNIT_ASSERT(!CouldUse(holders, portion1));
        UNIT_ASSERT(!CouldUse(holders, portion2));
        UNIT_ASSERT(!CouldUse(holders, portion3));
    }

    Y_UNIT_TEST(LegacySnapshotHoldersCouldUseTable) {
        const TLegacySnapshotHolders holders(Step(20), { Step(5) });

        {
            // Removed later than min snapshot for new reads -> always usable by new scans.
            auto table = MakeTable(1, 10, 1, 25);
            UNIT_ASSERT(holders.CouldUseTable(table));
        }

        {
            // Removed before min snapshot, but visible to active tx snapshot (step=5).
            auto table = MakeTable(2, 20, 1, 10);
            UNIT_ASSERT(holders.CouldUseTable(table));
        }

        {
            // Removed before min snapshot and not visible to active tx snapshot (step=5).
            auto table = MakeTable(3, 30, 1, 4);
            UNIT_ASSERT(!holders.CouldUseTable(table));
        }
    }

    Y_UNIT_TEST(RegistrySnapshotHoldersCouldUseTableByActiveSnapshot) {
        const ui64 schemeShardId = 777;
        const auto internalPathId = NColumnShard::TInternalPathId::FromRawValue(2);
        const auto ssPathId = NColumnShard::TSchemeShardLocalPathId::FromRawValue(20);
        auto table = MakeTable(2, 20, 1, 10);

        NTest::TTestPathIdTranslator translator;
        translator.Add(internalPathId, { ssPathId });

        auto registry = CreateSnapshotRegistry({ { NKikimr::TTableId(schemeShardId, ssPathId.GetRawValue(), 0), TRowVersion(5, 1) } });

        const TRegistrySnapshotHolders holders(Step(20), registry, schemeShardId, translator);
        UNIT_ASSERT(holders.CouldUseTable(table));
    }

    Y_UNIT_TEST(RegistrySnapshotHoldersUsesAllSchemeShardLocalPathIds) {
        const ui64 schemeShardId = 888;
        const auto internalPathId = NColumnShard::TInternalPathId::FromRawValue(3);
        const auto ssPathId1 = NColumnShard::TSchemeShardLocalPathId::FromRawValue(30);
        const auto ssPathId2 = NColumnShard::TSchemeShardLocalPathId::FromRawValue(31);
        const auto unrelatedSsPathId = NColumnShard::TSchemeShardLocalPathId::FromRawValue(999);
        NColumnShard::TTableInfo table({ NColumnShard::TUnifiedPathId::BuildValid(internalPathId, ssPathId1),
            NColumnShard::TUnifiedPathId::BuildValid(internalPathId, ssPathId2) });
        table.AddVersion(Step(1));
        table.SetCopyVersion(ssPathId2, Step(8));
        table.SetDropVersion(ssPathId1, Step(10));
        table.SetDropVersion(ssPathId2, Step(15));

        NTest::TTestPathIdTranslator translator;
        translator.Add(internalPathId, { ssPathId1, ssPathId2 });

        {
            // Snapshot for an unrelated table must not keep this table alive.
            auto registry =
                CreateSnapshotRegistry({ { NKikimr::TTableId(schemeShardId, unrelatedSsPathId.GetRawValue(), 0), TRowVersion(9, 1) } });
            const TRegistrySnapshotHolders holders(Step(20), registry, schemeShardId, translator);
            UNIT_ASSERT(!holders.CouldUseTable(table));
        }
        {
            // Snapshot for this table but outside table lifetime [1..15) must not keep it alive.
            auto registry = CreateSnapshotRegistry({ { NKikimr::TTableId(schemeShardId, ssPathId1.GetRawValue(), 0), TRowVersion(15, 1) } });
            const TRegistrySnapshotHolders holders(Step(20), registry, schemeShardId, translator);
            UNIT_ASSERT(!holders.CouldUseTable(table));
        }
        {
            // Snapshot for the first alias keeps table alive.
            auto registry = CreateSnapshotRegistry({ { NKikimr::TTableId(schemeShardId, ssPathId1.GetRawValue(), 0), TRowVersion(9, 1) } });
            const TRegistrySnapshotHolders holders(Step(20), registry, schemeShardId, translator);
            UNIT_ASSERT(holders.CouldUseTable(table));
        }
        {
            // Snapshot for the second alias also keeps table alive.
            auto registry = CreateSnapshotRegistry({ { NKikimr::TTableId(schemeShardId, ssPathId2.GetRawValue(), 0), TRowVersion(9, 1) } });
            const TRegistrySnapshotHolders holders(Step(20), registry, schemeShardId, translator);
            UNIT_ASSERT(holders.CouldUseTable(table));
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
