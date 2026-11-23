#include <ydb/core/tx/locks/locks.h>
#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NDataShard {
namespace {

using TUpdate = TLocksUpdate::TWriteSeqNumUpdate;
using EStatus = TUpdate::EStatus;
const TPathId Table(1, 2);

TLockInfo::TPtr MakeLock() {
    ILocksDb::TLockRow row{};
    row.LockId = 100;
    row.Generation = 1;
    row.Counter = 1;
    return new TLockInfo(nullptr, row);
}

void AddAncestor(TLockInfo& lock, ui64 createdOn, TVector<TWriteSeqNumBranch> branches, ui64 counter = 1) {
    TAncestorLock ancestor;
    ancestor.TabletId = 10;
    ancestor.CreatedOn = createdOn;
    ancestor.Counter = counter;
    auto& state = ancestor.WriteSeqNumStates[7];
    state.WriterIndex = 7;
    state.Branches = std::move(branches);
    for (const auto& branch : state.Branches) {
        state.WriteSeqNum = Max(state.WriteSeqNum, branch.WriteSeqNum);
    }
    lock.AddAncestorLock(std::move(ancestor));
}

TUpdate::TResult Stage(TUpdate& update, const TLockInfo* lock, TVector<ui64> ancestors, ui64 wsn) {
    return update.StageOperation(lock, 40, ancestors, wsn, Table);
}

} // namespace

Y_UNIT_TEST_SUITE(DataShardWriteSeqNums) {
    Y_UNIT_TEST(CurrentDuplicatesAndFreshSuffixAreStaged) {
        auto lock = MakeLock();
        lock->SetWriteSeqNum(7, 2, nullptr);
        lock->SetWriteSeqNumResult(7, "saved");
        TUpdate update(7);
        auto duplicate = Stage(update, lock.Get(), {}, 1);
        UNIT_ASSERT(duplicate.Status == EStatus::Duplicate);
        UNIT_ASSERT_VALUES_EQUAL(duplicate.SerializedResult, "saved");
        UNIT_ASSERT(Stage(update, lock.Get(), {}, 3).Status == EStatus::Fresh);
        UNIT_ASSERT_VALUES_EQUAL(update.Branches.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(update.Branches[0].Branch.WriteSeqNum, 3);
        UNIT_ASSERT_VALUES_EQUAL(lock->GetWriteSeqNum(7), 2);
    }

    Y_UNIT_TEST(SiblingMaximumDoesNotDeduplicateAnotherBranch) {
        auto lock = MakeLock();
        AddAncestor(*lock, 10, {{{10}, 10, "base"}, {{10, 30}, 11, "child"}});
        TUpdate update(7);
        auto duplicate = Stage(update, lock.Get(), {10, 30}, 11);
        UNIT_ASSERT(duplicate.Status == EStatus::Duplicate);
        UNIT_ASSERT_VALUES_EQUAL(duplicate.SerializedResult, "child");
        UNIT_ASSERT(Stage(update, lock.Get(), {10, 20}, 11).Status == EStatus::Fresh);
        UNIT_ASSERT_VALUES_EQUAL(update.EchoLocks.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(update.Branches.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(update.Branches[0].Branch.ShardsPath[1], 20);
        lock->SetAncestorBranch(update.Branches[0].Key, 7, update.Branches[0].Branch, nullptr);
        const auto* ancestor = lock->FindAncestorLock({10, 10});
        UNIT_ASSERT_VALUES_EQUAL(ancestor->WriteSeqNumStates.at(7).Branches.size(), 3);
        UNIT_ASSERT_VALUES_EQUAL(ancestor->WriteSeqNumStates.at(7).WriteSeqNum, 11);
    }

    Y_UNIT_TEST(StrictPrefixesRemainAvailableForSiblingRetries) {
        auto lock = MakeLock();
        AddAncestor(*lock, 10, {{{10}, 10, "prefix"}});
        TUpdate update(7);
        UNIT_ASSERT(Stage(update, lock.Get(), {10, 20}, 9).Status == EStatus::Duplicate);
        UNIT_ASSERT(Stage(update, lock.Get(), {10, 20}, 11).Status == EStatus::Fresh);
        UNIT_ASSERT(Stage(update, lock.Get(), {10, 30}, 10).Status == EStatus::Duplicate);
        UNIT_ASSERT(Stage(update, lock.Get(), {10, 30}, 11).Status == EStatus::Fresh);
        UNIT_ASSERT_VALUES_EQUAL(update.Branches.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(lock->FindAncestorLock({10, 10})->WriteSeqNumStates.at(7).Branches.size(), 1);
    }

    Y_UNIT_TEST(IndependentCreatedOnLocksAreSelectedInEitherOrder) {
        for (bool reverse : {false, true}) {
            auto lock = MakeLock();
            AddAncestor(*lock, 30, {{{10, 30}, 1, "existing"}});
            TUpdate update(7);
            if (reverse) {
                UNIT_ASSERT(Stage(update, lock.Get(), {10, 30}, 1).Status == EStatus::Duplicate);
            }
            UNIT_ASSERT(Stage(update, lock.Get(), {10, 20}, 1).Status == EStatus::Fresh);
            if (!reverse) {
                UNIT_ASSERT(Stage(update, lock.Get(), {10, 30}, 1).Status == EStatus::Duplicate);
            }
            UNIT_ASSERT_VALUES_EQUAL(update.NewLocks.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(update.EchoLocks.size(), 2);
            TAncestorLock created;
            created.TabletId = 10;
            created.CreatedOn = 40;
            created.Counter = 2;
            lock->AddAncestorLock(std::move(created));
            lock->SetAncestorBranch(update.Branches[0].Key, 7, update.Branches[0].Branch, nullptr);
            TUpdate retry(7);
            UNIT_ASSERT(Stage(retry, lock.Get(), {10, 20}, 1).Status == EStatus::Duplicate);
            UNIT_ASSERT(Stage(retry, lock.Get(), {10, 30}, 1).Status == EStatus::Duplicate);
            UNIT_ASSERT(retry.Branches.empty());
            UNIT_ASSERT(retry.NewLocks.empty());
        }
    }

    Y_UNIT_TEST(FirstExistingCandidateCannotBeSkipped) {
        auto lock = MakeLock();
        AddAncestor(*lock, 10, {}, TSysLocks::TLock::ErrorBroken);
        AddAncestor(*lock, 30, {{{10, 30}, 1, "later"}});
        TUpdate update(7);
        UNIT_ASSERT(Stage(update, lock.Get(), {10, 30}, 1).Status == EStatus::LocksBroken);
        UNIT_ASSERT(update.NewLocks.empty());
        UNIT_ASSERT(update.Branches.empty());
    }

    Y_UNIT_TEST(SelectedLockMayHaveOnlyAnUnrelatedBranch) {
        auto lock = MakeLock();
        AddAncestor(*lock, 10, {{{10, 20}, 5, "sibling"}});
        TUpdate update(7);
        UNIT_ASSERT(Stage(update, lock.Get(), {10, 30}, 1).Status == EStatus::Fresh);
        UNIT_ASSERT(update.NewLocks.empty());
        UNIT_ASSERT_VALUES_EQUAL(update.Branches[0].Key.CreatedOn, 10);
        lock->SetAncestorBranch(update.Branches[0].Key, 7, update.Branches[0].Branch, nullptr);
        UNIT_ASSERT_VALUES_EQUAL(lock->FindAncestorLock({10, 10})->WriteSeqNumStates.at(7).WriteSeqNum, 5);
    }

    Y_UNIT_TEST(MissingLocksRequireFirstPositionAndGapsDoNotPublishState) {
        TUpdate missing(7);
        UNIT_ASSERT(Stage(missing, nullptr, {10}, 2).Status == EStatus::LocksBroken);
        TUpdate current(7);
        UNIT_ASSERT(Stage(current, nullptr, {}, 2).Status == EStatus::LocksBroken);
        auto lock = MakeLock();
        TUpdate update(7);
        UNIT_ASSERT(Stage(update, lock.Get(), {10}, 1).Status == EStatus::Fresh);
        UNIT_ASSERT(Stage(update, lock.Get(), {10}, 3).Status == EStatus::BadRequest);
        UNIT_ASSERT(lock->GetAncestorLocks().empty());
        UNIT_ASSERT(lock->GetWriteSeqNumStates().empty());
    }

    Y_UNIT_TEST(CurrentBarrierAllowsOnlyAncestorDuplicates) {
        auto lock = MakeLock();
        AddAncestor(*lock, 10, {{{10}, 1, "saved"}});
        lock->SetWriteSeqNum(7, 1, nullptr);
        TUpdate duplicate(7);
        UNIT_ASSERT(Stage(duplicate, lock.Get(), {10}, 1).Status == EStatus::Duplicate);
        TUpdate fresh(7);
        UNIT_ASSERT(Stage(fresh, lock.Get(), {10}, 2).Status == EStatus::BadRequest);
        TUpdate order(7);
        UNIT_ASSERT(Stage(order, lock.Get(), {}, 1).Status == EStatus::Duplicate);
        UNIT_ASSERT(Stage(order, lock.Get(), {10}, 1).Status == EStatus::BadRequest);
    }

    Y_UNIT_TEST(InvalidPathsAndOtherWritersAreRejected) {
        for (TVector<ui64> path : {TVector<ui64>{10, 10}, {10, 40}, {0}}) {
            TUpdate update(7);
            UNIT_ASSERT(Stage(update, nullptr, std::move(path), 1).Status == EStatus::BadRequest);
        }
        auto lock = MakeLock();
        lock->SetWriteSeqNum(8, 1, nullptr);
        TUpdate writer(7);
        UNIT_ASSERT(Stage(writer, lock.Get(), {}, 1).Status == EStatus::BadRequest);
    }
}

} // namespace NKikimr::NDataShard
