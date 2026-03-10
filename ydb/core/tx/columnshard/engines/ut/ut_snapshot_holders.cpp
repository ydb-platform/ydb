#include <ydb/core/tx/columnshard/engines/snapshot_holders.h>

#include <library/cpp/testing/unittest/registar.h>

#ifndef _win_
#include <cerrno>
#include <sys/wait.h>
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
        {}
    };

    bool CouldUse(const TSnapshotHolders& holders, const TMyPortion& portion) {
        return holders.CouldUse(
            [&portion](const TSnapshot& heldSnapshot) { return portion.Removed <= heldSnapshot; },
            [&portion](const TSnapshot& heldSnapshot) { return portion.Appeared <= heldSnapshot; }
        );
    }

    Y_UNIT_TEST(PortionsCouldBeUsedAfterMinReadSnapshot) {
        // time        0--1--------------------------------10---11
        //                                                 ^ minSnapshotForNewReads
        // p1             [.....................................)
        // p2                                                   [11....................20)
        // p3             [................................)
        const TSnapshotHolders holders(Step(10), {});
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
        const TSnapshotHolders holders(Step(20), { Step(5) });
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
        const TSnapshotHolders holders(Step(20), { Step(5), Step(12) });
        const TMyPortion portion1(1, 5);
        const TMyPortion portion2(6, 10);
        const TMyPortion portion3(13, 18);

        UNIT_ASSERT(!CouldUse(holders, portion1));
        UNIT_ASSERT(!CouldUse(holders, portion2));
        UNIT_ASSERT(!CouldUse(holders, portion3));
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
            [[maybe_unused]] TSnapshotHolders holders(Step(20), { Step(12), Step(5) });
        });
        UNIT_ASSERT_C(!WIFEXITED(unsortedTxs) || WEXITSTATUS(unsortedTxs) != 0,
            "unsorted tx list must fail constructor checks");

        const int tooYoungTx = RunInChild([] {
            [[maybe_unused]] TSnapshotHolders holders(Step(20), { Step(20) });
        });
        UNIT_ASSERT_C(!WIFEXITED(tooYoungTx) || WEXITSTATUS(tooYoungTx) != 0,
            "tx snapshot >= minReadSnapshot must fail constructor checks");
#else
        UNIT_ASSERT_C(true, "POSIX-only test is skipped on Windows");
#endif
    }

}

}   // namespace NKikimr::NOlap
