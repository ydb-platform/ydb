#include "global_write_txs.h"

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NReplication::NController {

namespace {

TRowVersion Version(ui64 step) {
    return TRowVersion(step, 0);
}

TGlobalWriteTxs MakeWriteTxs(ui64 maxOpenWriteTxs, const TVector<ui64>& freeWriteTxIds) {
    TGlobalWriteTxs writeTxs(TGlobalWriteTxsConfig{.MaxOpenWriteTxs = maxOpenWriteTxs});
    writeTxs.AddFreeWriteTxIds(freeWriteTxIds);
    return writeTxs;
}

}

Y_UNIT_TEST_SUITE(GlobalWriteTxs) {
    Y_UNIT_TEST(ShouldReturnAssignedWriteTxWhenVersionWasAssigned) {
        // given
        auto writeTxs = MakeWriteTxs(5, {1, 2});
        writeTxs.AssignWriteTxs({Version(20)}, {}, false);

        // when
        const auto assignment = writeTxs.AssignWriteTxs({Version(20)}, {}, false);

        // then
        UNIT_ASSERT_VALUES_EQUAL(assignment.PendingVersionToWriteTxId.at(Version(20)), 1);
        UNIT_ASSERT_VALUES_EQUAL(writeTxs.CountOpenWriteTxs(), 1);
    }

    Y_UNIT_TEST(ShouldMoveLastWriteTxToPendingVersionWhenLimitIsReached) {
        // given
        auto writeTxs = MakeWriteTxs(1, {1, 2});
        writeTxs.AssignWriteTxs({Version(20)}, {}, false);

        // when
        const auto assignment = writeTxs.AssignWriteTxs({Version(30)}, {}, false);

        // then
        UNIT_ASSERT_VALUES_EQUAL(assignment.PendingVersionToWriteTxId.at(Version(30)), 1);
        UNIT_ASSERT(assignment.OpenWriteTxLimitReached);
        const auto openWriteTxs = writeTxs.GetOpenWriteTxs();
        UNIT_ASSERT_VALUES_EQUAL(openWriteTxs.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(openWriteTxs.front().Version, Version(30));
    }

    Y_UNIT_TEST(ShouldAssignByNextVersionWhenLimitIsReachedBelowLastVersion) {
        // given
        auto writeTxs = MakeWriteTxs(1, {1});
        writeTxs.AssignWriteTxs({Version(30)}, {}, false);
        writeTxs.TakeChanges();

        // when
        const auto assignment = writeTxs.AssignWriteTxs({Version(20)}, {}, false);

        // then
        UNIT_ASSERT_VALUES_EQUAL(assignment.PendingVersionToWriteTxId.at(Version(20)), 1);
        const auto changes = writeTxs.TakeChanges();
        UNIT_ASSERT(changes.Inserted.empty());
        UNIT_ASSERT(changes.Deleted.empty());
    }

    Y_UNIT_TEST(ShouldOpenSeparateWriteTxWhenExtensionCrossesBoundary) {
        // given
        auto writeTxs = MakeWriteTxs(1, {1, 2});
        writeTxs.AssignWriteTxs({Version(20)}, {}, false);

        // when
        const auto assignment = writeTxs.AssignWriteTxs({Version(40)}, {Version(30)}, false);

        // then
        UNIT_ASSERT_VALUES_EQUAL(assignment.PendingVersionToWriteTxId.at(Version(40)), 2);
        UNIT_ASSERT_VALUES_EQUAL(writeTxs.CountOpenWriteTxs(), 2);
    }

    Y_UNIT_TEST(ShouldLeaveVersionUnassignedWhenBoundaryNeedsWriteTxAndPoolIsEmpty) {
        // given
        auto writeTxs = MakeWriteTxs(1, {1});
        writeTxs.AssignWriteTxs({Version(20)}, {}, false);

        // when
        const auto assignment = writeTxs.AssignWriteTxs({Version(40)}, {Version(30)}, false);

        // then
        UNIT_ASSERT(assignment.PendingVersionToWriteTxId.empty());
        UNIT_ASSERT(assignment.OpenWriteTxLimitReached);
        UNIT_ASSERT(!assignment.FreeWriteTxIdsExhausted);
        UNIT_ASSERT_VALUES_EQUAL(writeTxs.CountOpenWriteTxs(), 1);
    }

    Y_UNIT_TEST(ShouldReportExhaustedFreeWriteTxIdsWhenPoolIsEmpty) {
        // given
        auto writeTxs = MakeWriteTxs(5, {});

        // when
        const auto assignment = writeTxs.AssignWriteTxs({Version(20)}, {}, false);

        // then
        UNIT_ASSERT(assignment.PendingVersionToWriteTxId.empty());
        UNIT_ASSERT(assignment.FreeWriteTxIdsExhausted);
    }

    Y_UNIT_TEST(ShouldAssignPendingVersionsUntilFreeWriteTxIdsAreExhausted) {
        // given
        auto writeTxs = MakeWriteTxs(5, {1});

        // when
        const auto assignment = writeTxs.AssignWriteTxs({Version(10), Version(20)}, {}, false);

        // then
        UNIT_ASSERT_VALUES_EQUAL(assignment.PendingVersionToWriteTxId.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(assignment.PendingVersionToWriteTxId.at(Version(10)), 1);
        UNIT_ASSERT(assignment.FreeWriteTxIdsExhausted);
    }

    Y_UNIT_TEST(ShouldAssignNextWriteTxWhenReuseNextWriteTxIsRequested) {
        // given
        auto writeTxs = MakeWriteTxs(5, {1, 2});
        writeTxs.AssignWriteTxs({Version(30)}, {}, false);

        // when
        const auto assignment = writeTxs.AssignWriteTxs({Version(20)}, {}, true);

        // then
        UNIT_ASSERT_VALUES_EQUAL(assignment.PendingVersionToWriteTxId.at(Version(20)), 1);
        UNIT_ASSERT_VALUES_EQUAL(writeTxs.CountOpenWriteTxs(), 1);
    }

    Y_UNIT_TEST(ShouldWriteOnlyMovedVersionWhenWriteTxIsOpenedAndMovedInOneAssignment) {
        // given
        auto writeTxs = MakeWriteTxs(1, {1, 2});

        // when
        const auto assignment = writeTxs.AssignWriteTxs({Version(20), Version(30)}, {}, false);

        // then
        UNIT_ASSERT_VALUES_EQUAL(assignment.PendingVersionToWriteTxId.at(Version(20)), 1);
        UNIT_ASSERT_VALUES_EQUAL(assignment.PendingVersionToWriteTxId.at(Version(30)), 1);
        const auto changes = writeTxs.TakeChanges();
        UNIT_ASSERT_VALUES_EQUAL(changes.Inserted.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(changes.Inserted.at(Version(30)), 1);
        UNIT_ASSERT_VALUES_EQUAL(changes.Deleted.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(changes.Deleted.at(0), Version(20));
    }

    Y_UNIT_TEST(ShouldDeleteEveryMovedVersionWhenLastWriteTxIsMovedTwiceBeforeSave) {
        // given
        auto writeTxs = MakeWriteTxs(1, {1});
        writeTxs.AssignWriteTxs({Version(20)}, {}, false);
        writeTxs.TakeChanges();
        writeTxs.AssignWriteTxs({Version(30)}, {}, false);

        // when
        writeTxs.AssignWriteTxs({Version(40)}, {}, false);

        // then
        const auto changes = writeTxs.TakeChanges();
        UNIT_ASSERT_VALUES_EQUAL(changes.Inserted.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(changes.Inserted.at(Version(40)), 1);
        UNIT_ASSERT_VALUES_EQUAL(changes.Deleted.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(changes.Deleted.at(0), Version(20));
        UNIT_ASSERT_VALUES_EQUAL(changes.Deleted.at(1), Version(30));
    }

    Y_UNIT_TEST(ShouldDeleteVersionWhenWriteTxIsCommitted) {
        // given
        auto writeTxs = MakeWriteTxs(5, {1});
        writeTxs.AssignWriteTxs({Version(20)}, {}, false);
        writeTxs.TakeChanges();

        // when
        writeTxs.RemoveCommittedWriteTx(1);

        // then
        const auto changes = writeTxs.TakeChanges();
        UNIT_ASSERT(changes.Inserted.empty());
        UNIT_ASSERT_VALUES_EQUAL(changes.Deleted.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(changes.Deleted.at(0), Version(20));
        UNIT_ASSERT(!writeTxs.HasOpenWriteTxs());
    }

    Y_UNIT_TEST(ShouldReportUncommittedWriteTxsWhenAnyOfThemIsOpen) {
        // given
        auto writeTxs = MakeWriteTxs(5, {1, 2});
        writeTxs.AssignWriteTxs({Version(20), Version(30)}, {}, false);
        writeTxs.RemoveCommittedWriteTx(1);

        // when
        const bool uncommitted = writeTxs.HasUncommittedWriteTxs({1, 2});

        // then
        UNIT_ASSERT(uncommitted);
        UNIT_ASSERT(!writeTxs.HasUncommittedWriteTxs({1}));
    }

    Y_UNIT_TEST(ShouldKeepFreeWriteTxIdsWhenWriteTxsAreRestored) {
        // given
        auto writeTxs = MakeWriteTxs(5, {1, 2});
        writeTxs.AssignWriteTxs({Version(20)}, {}, false);

        // when
        writeTxs.RestoreWriteTxs({{Version(50), 7}});

        // then
        UNIT_ASSERT_VALUES_EQUAL(writeTxs.CountFreeWriteTxIds(), 1);
        const auto changes = writeTxs.TakeChanges();
        UNIT_ASSERT(changes.Inserted.empty());
        UNIT_ASSERT(changes.Deleted.empty());
        const auto openWriteTxs = writeTxs.GetOpenWriteTxs();
        UNIT_ASSERT_VALUES_EQUAL(openWriteTxs.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(openWriteTxs.front().WriteTxId, 7);
        UNIT_ASSERT_VALUES_EQUAL(openWriteTxs.front().Version, Version(50));
    }

    Y_UNIT_TEST(ShouldReturnFoundVersionForIndexBuildWhenLaterWriteTxExists) {
        // given
        auto writeTxs = MakeWriteTxs(5, {1});
        writeTxs.AssignWriteTxs({Version(30)}, {}, false);

        // when
        const auto writeTx = writeTxs.AssignIndexBuildWriteTx(Version(20), Version(10), 0);

        // then
        UNIT_ASSERT(writeTx);
        UNIT_ASSERT_VALUES_EQUAL(writeTx->WriteTxId, 1);
        UNIT_ASSERT_VALUES_EQUAL(writeTx->Version, Version(30));
    }

    Y_UNIT_TEST(ShouldMoveLastWriteTxForIndexBuildWhenItEndsAfterJoin) {
        // given
        auto writeTxs = MakeWriteTxs(1, {1, 2});
        writeTxs.AssignWriteTxs({Version(20)}, {}, false);

        // when
        const auto writeTx = writeTxs.AssignIndexBuildWriteTx(Version(30), Version(10), 0);

        // then
        UNIT_ASSERT(writeTx);
        UNIT_ASSERT_VALUES_EQUAL(writeTx->WriteTxId, 1);
        UNIT_ASSERT_VALUES_EQUAL(writeTx->Version, Version(30));
        UNIT_ASSERT_VALUES_EQUAL(writeTxs.CountOpenWriteTxs(), 1);
    }

    Y_UNIT_TEST(ShouldOpenWriteTxForIndexBuildWhenLastWriteTxIsCommitting) {
        // given
        auto writeTxs = MakeWriteTxs(1, {1, 2});
        writeTxs.AssignWriteTxs({Version(20)}, {}, false);

        // when
        const auto writeTx = writeTxs.AssignIndexBuildWriteTx(Version(30), Version(10), 1);

        // then
        UNIT_ASSERT(writeTx);
        UNIT_ASSERT_VALUES_EQUAL(writeTx->WriteTxId, 2);
        UNIT_ASSERT_VALUES_EQUAL(writeTxs.CountOpenWriteTxs(), 2);
    }

    Y_UNIT_TEST(ShouldNotAssignIndexBuildWriteTxWhenLastWriteTxEndsBeforeJoinAndPoolIsEmpty) {
        // given
        auto writeTxs = MakeWriteTxs(1, {1});
        writeTxs.AssignWriteTxs({Version(20)}, {}, false);

        // when
        const auto writeTx = writeTxs.AssignIndexBuildWriteTx(Version(30), Version(25), 0);

        // then
        UNIT_ASSERT(!writeTx);
        UNIT_ASSERT_VALUES_EQUAL(writeTxs.CountOpenWriteTxs(), 1);
    }

    Y_UNIT_TEST(ShouldNotAssignIndexBuildWriteTxWhenOpenWriteTxsAreAboveLimit) {
        // given
        auto writeTxs = MakeWriteTxs(1, {1, 2, 3});
        writeTxs.AssignWriteTxs({Version(20)}, {}, false);
        writeTxs.AssignIndexBuildWriteTx(Version(30), Version(10), 1);

        // when
        const auto writeTx = writeTxs.AssignIndexBuildWriteTx(Version(40), Version(35), 1);

        // then
        UNIT_ASSERT(!writeTx);
        UNIT_ASSERT_VALUES_EQUAL(writeTxs.CountOpenWriteTxs(), 2);
        UNIT_ASSERT_VALUES_EQUAL(writeTxs.CountFreeWriteTxIds(), 1);
    }
}

}
