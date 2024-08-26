#include "datashard_dep_tracker.h"
#include "datashard_impl.h"

#include "const.h"

#include <util/string/escape.h>

namespace NKikimr {
namespace NDataShard {

static constexpr bool DelayPlannedRanges = true;
static constexpr bool DelayImmediateRanges = true;

namespace {
    template<class T>
    void SwapConsume(T& value) {
        T tmp;
        tmp.swap(value);
    }

    TRangeTreeBase::TRange MakeSearchRange(const TTableRange& range) {
        if (range.Point) {
            return TRangeTreeBase::TRange(range.From, true, range.From, true);
        } else {
            return TRangeTreeBase::TRange(
                range.From,
                range.InclusiveFrom || !range.From,
                range.To,
                range.InclusiveTo || !range.To);
        }
    }

    TRangeTreeBase::TOwnedRange MakeOwnedRange(const TOwnedTableRange& range) {
        if (range.Point) {
            return TRangeTreeBase::TOwnedRange(range.GetOwnedFrom(), true, range.GetOwnedFrom(), true);
        } else {
            return TRangeTreeBase::TOwnedRange(
                range.GetOwnedFrom(),
                range.InclusiveFrom || !range.From,
                range.GetOwnedTo(),
                range.InclusiveTo || !range.To);
        }
    }

    bool IsLess(const TOperation& a, const TRowVersion& b) {
        return a.GetStep() < b.Step || (a.GetStep() == b.Step && a.GetTxId() < b.TxId);
    }

    bool IsEqual(const TOperation& a, const TRowVersion& b) {
        return a.GetStep() == b.Step && a.GetTxId() == b.TxId;
    }
}

void TDependencyTracker::UpdateSchema(const TPathId& tableId, const TUserTable& tableInfo) noexcept {
    auto& state = Tables[tableId.LocalPathId];
    state.PlannedReads.SetKeyTypes(tableInfo.KeyColumnTypes);
    state.PlannedWrites.SetKeyTypes(tableInfo.KeyColumnTypes);
    state.ImmediateReads.SetKeyTypes(tableInfo.KeyColumnTypes);
    state.ImmediateWrites.SetKeyTypes(tableInfo.KeyColumnTypes);
}

void TDependencyTracker::RemoveSchema(const TPathId& tableId) noexcept {
    Tables.erase(tableId.LocalPathId);
}

void TDependencyTracker::ClearTmpRead() noexcept {
    TmpRead.clear();
}

void TDependencyTracker::ClearTmpWrite() noexcept {
    TmpWrite.clear();
}

void TDependencyTracker::AddPlannedReads(const TOperation::TPtr& op, const TKeys& reads) noexcept {
    for (const auto& read : reads) {
        auto it = Tables.find(read.TableId);
        Y_ABORT_UNLESS(it != Tables.end());
        auto ownedRange = MakeOwnedRange(read.Key);
        it->second.PlannedReads.AddRange(std::move(ownedRange), op);
    }
}

void TDependencyTracker::AddPlannedWrites(const TOperation::TPtr& op, const TKeys& writes) noexcept {
    for (const auto& write : writes) {
        auto it = Tables.find(write.TableId);
        Y_ABORT_UNLESS(it != Tables.end());
        auto ownedRange = MakeOwnedRange(write.Key);
        it->second.PlannedWrites.AddRange(std::move(ownedRange), op);
    }
}

void TDependencyTracker::AddImmediateReads(const TOperation::TPtr& op, const TKeys& reads) noexcept {
    for (const auto& read : reads) {
        auto it = Tables.find(read.TableId);
        Y_ABORT_UNLESS(it != Tables.end());
        auto ownedRange = MakeOwnedRange(read.Key);
        it->second.ImmediateReads.AddRange(std::move(ownedRange), op);
    }
}

void TDependencyTracker::AddImmediateWrites(const TOperation::TPtr& op, const TKeys& writes) noexcept {
    for (const auto& write : writes) {
        auto it = Tables.find(write.TableId);
        Y_ABORT_UNLESS(it != Tables.end());
        auto ownedRange = MakeOwnedRange(write.Key);
        it->second.ImmediateWrites.AddRange(std::move(ownedRange), op);
    }
}

void TDependencyTracker::FlushPlannedReads() noexcept {
    while (!DelayedPlannedReads.Empty()) {
        TOperation::TPtr op = DelayedPlannedReads.PopFront();
        auto reads = op->RemoveDelayedKnownReads();
        AddPlannedReads(op, reads);
    }
}

void TDependencyTracker::FlushPlannedWrites() noexcept {
    while (!DelayedPlannedWrites.Empty()) {
        TOperation::TPtr op = DelayedPlannedWrites.PopFront();
        auto writes = op->RemoveDelayedKnownWrites();
        AddPlannedWrites(op, writes);
    }
}

void TDependencyTracker::FlushImmediateReads() noexcept {
    while (!DelayedImmediateReads.Empty()) {
        TOperation::TPtr op = DelayedImmediateReads.PopFront();
        auto reads = op->RemoveDelayedKnownReads();
        AddImmediateReads(op, reads);
    }
}

void TDependencyTracker::FlushImmediateWrites() noexcept {
    while (!DelayedImmediateWrites.Empty()) {
        TOperation::TPtr op = DelayedImmediateWrites.PopFront();
        auto writes = op->RemoveDelayedKnownWrites();
        AddImmediateWrites(op, writes);
    }
}

const TDependencyTracker::TDependencyTrackingLogic& TDependencyTracker::GetTrackingLogic() const noexcept {
    if (Self->IsFollower())
        return FollowerLogic;

    if (Self->IsMvccEnabled())
        return MvccLogic;

    return DefaultLogic;
}

void TDependencyTracker::TDefaultDependencyTrackingLogic::AddOperation(const TOperation::TPtr& op) const noexcept {
    if (op->IsUsingSnapshot()) {
        return;
    }

    // WARNING: kqp scan transactions don't have known keys, that means
    // we must assume they are global reader and must be careful not to
    // look into extracted keys for these transactions.
    bool haveKeys = !op->IsKqpScanTransaction();

    bool isGlobalReader = op->IsGlobalReader();
    bool isGlobalWriter = op->IsGlobalWriter();

    bool tooManyKeys = false;
    if (haveKeys && op->KeysCount() > MAX_REORDER_TX_KEYS) {
        tooManyKeys = true;
    }

    // Non-immediate snapshot operations cannot be reordered
    if (!op->IsImmediate() && op->IsSnapshotTx()) {
        if (Parent.LastSnapshotOp) {
            op->AddDependency(Parent.LastSnapshotOp);
        }
        Parent.LastSnapshotOp = op;
    }

    // First pass, gather all reads/writes expanded with locks, add lock based dependencies
    bool haveReads = false;
    bool haveWrites = false;
    if (haveKeys) {
        size_t keysCount = 0;
        const auto& keysInfo = op->GetKeysInfo();
        const auto& locksCache = op->LocksCache();
        for (const auto& vk : keysInfo.Keys) {
            const auto& k = *vk.Key;
            if (Parent.Self->IsUserTable(k.TableId)) {
                const ui64 tableId = k.TableId.PathId.LocalPathId;
                Y_VERIFY_DEBUG_S(!k.Range.IsAmbiguous(Parent.Self->GetUserTables().at(tableId)->KeyColumnTypes.size()),
                    (vk.IsWrite ? "Write" : "Read")
                    << " From# \"" << EscapeC(TSerializedCellVec::Serialize(k.Range.From)) << "\""
                    << " To# \"" << EscapeC(TSerializedCellVec::Serialize(k.Range.To)) << "\""
                    << " InclusiveFrom# " << (k.Range.InclusiveFrom ? "true" : "false")
                    << " InclusiveTo# " << (k.Range.InclusiveTo ? "true" : "false")
                    << " Point# " << (k.Range.Point ? "true" : "false")
                    << ": " << k.Range.IsAmbiguousReason(Parent.Self->GetUserTables().at(tableId)->KeyColumnTypes.size()));
                if (!tooManyKeys && ++keysCount > MAX_REORDER_TX_KEYS) {
                    tooManyKeys = true;
                }
                if (vk.IsWrite) {
                    haveWrites = true;
                    if (!tooManyKeys && !isGlobalWriter) {
                        Parent.TmpWrite.emplace_back(tableId, k.Range);
                    }
                } else {
                    haveReads = true;
                    if (!tooManyKeys && !isGlobalReader) {
                        Parent.TmpRead.emplace_back(tableId, k.Range);
                    }
                }
            } else if (TSysTables::IsLocksTable(k.TableId)) {
                Y_ABORT_UNLESS(k.Range.Point, "Unexpected non-point read from the locks table");
                const ui64 lockTxId = Parent.Self->SysLocksTable().ExtractLockTxId(k.Range.From);

                // Add hard dependency on all operations that worked with the same lock
                auto& lastLockOp = Parent.LastLockOps[lockTxId];
                if (lastLockOp != op) {
                    op->AddAffectedLock(lockTxId);
                    if (lastLockOp) {
                        op->AddDependency(lastLockOp);
                    }
                    lastLockOp = op;
                }

                // Reading a lock means checking it for validity, i.e. "reading" those predicted keys
                if (!vk.IsWrite) {
                    if (auto it = locksCache.Locks.find(lockTxId); it != locksCache.Locks.end()) {
                        // This transaction uses locks cache, so lock check
                        // outcome was persisted and restored. Unfortunately
                        // now we cannot know which keys it translated to, and
                        // have to assume "whole shard" worst case.
                        isGlobalReader = true;
                    } else if (auto it = Parent.Locks.find(lockTxId); it != Parent.Locks.end()) {
                        haveReads = true;
                        if (!tooManyKeys && (keysCount += it->second.Keys.size()) > MAX_REORDER_TX_KEYS) {
                            tooManyKeys = true;
                        }
                        if (!tooManyKeys && !isGlobalReader) {
                            if (it->second.WholeShard) {
                                isGlobalReader = true;
                            } else {
                                Parent.TmpRead.insert(Parent.TmpRead.end(), it->second.Keys.begin(), it->second.Keys.end());
                            }
                        }
                    } else if (auto lock = Parent.Self->SysLocksTable().GetRawLock(lockTxId)) {
                        Y_ASSERT(!lock->IsBroken());
                        haveReads = true;
                        if (!tooManyKeys && (keysCount += (lock->NumPoints() + lock->NumRanges())) > MAX_REORDER_TX_KEYS) {
                            tooManyKeys = true;
                        }
                        if (!tooManyKeys && !isGlobalReader) {
                            if (lock->IsShardLock()) {
                                isGlobalReader = true;
                            } else {
                                for (const auto& point : lock->GetPoints()) {
                                    Parent.TmpRead.emplace_back(point.Table->GetTableId().LocalPathId, point.ToOwnedTableRange());
                                }
                                for (const auto& range : lock->GetRanges()) {
                                    Parent.TmpRead.emplace_back(range.Table->GetTableId().LocalPathId, range.ToOwnedTableRange());
                                }
                            }
                        }
                    }
                }
            }
        }

        if (tooManyKeys) {
            if (haveReads) {
                isGlobalReader = true;
            }
            if (haveWrites) {
                isGlobalWriter = true;
            }
        }

        if (haveReads && isGlobalReader) {
            Parent.ClearTmpRead();
            haveReads = false;
        }

        if (haveWrites && isGlobalWriter) {
            Parent.ClearTmpWrite();
            haveWrites = false;
        }
    }

    auto processImmediatePlanned = [&](const TRangeTreeBase::TRange&, const TOperation::TPtr& conflict) {
        conflict->AddImmediateConflict(op);
    };

    auto processPlannedPlanned = [&](const TRangeTreeBase::TRange&, const TOperation::TPtr& conflict) {
        op->AddDependency(conflict);
    };

    auto processPlannedImmediate = [&](const TRangeTreeBase::TRange&, const TOperation::TPtr& conflict) {
        op->AddImmediateConflict(conflict);
    };

    // Second pass, add dependencies
    if (isGlobalWriter) {
        // We are potentially writing to all keys in all tables, thus we conflict with everything
        if (op->IsImmediate()) {
            for (auto& item : Parent.AllPlannedReaders) {
                item.AddImmediateConflict(op);
            }
            for (auto& item : Parent.AllPlannedWriters) {
                item.AddImmediateConflict(op);
            }
        } else {
            for (auto& item : Parent.AllPlannedReaders) {
                op->AddDependency(&item);
            }
            for (auto& item : Parent.AllPlannedWriters) {
                op->AddDependency(&item);
            }
            for (auto& item : Parent.AllImmediateReaders) {
                op->AddImmediateConflict(&item);
            }
            for (auto& item : Parent.AllImmediateWriters) {
                op->AddImmediateConflict(&item);
            }
        }
    } else {
        if (haveWrites) {
            // Each write may conflict with previous reads or writes
            Parent.FlushPlannedReads();
            Parent.FlushPlannedWrites();
            if (!op->IsImmediate()) {
                Parent.FlushImmediateReads();
                Parent.FlushImmediateWrites();
            }
            for (const auto& write : Parent.TmpWrite) {
                if (auto it = Parent.Tables.find(write.TableId); it != Parent.Tables.end()) {
                    auto searchRange = MakeSearchRange(write.Key);
                    if (op->IsImmediate()) {
                        it->second.PlannedReads.EachIntersection(searchRange, processImmediatePlanned);
                        it->second.PlannedWrites.EachIntersection(searchRange, processImmediatePlanned);
                    } else {
                        it->second.PlannedReads.EachIntersection(searchRange, processPlannedPlanned);
                        it->second.PlannedWrites.EachIntersection(searchRange, processPlannedPlanned);
                        it->second.ImmediateReads.EachIntersection(searchRange, processPlannedImmediate);
                        it->second.ImmediateWrites.EachIntersection(searchRange, processPlannedImmediate);
                    }
                }
            }

            // If we have any reads we conflict with global reads or writes
            if (op->IsImmediate()) {
                for (auto& item : Parent.GlobalPlannedReaders) {
                    item.AddImmediateConflict(op);
                }
                for (auto& item : Parent.GlobalPlannedWriters) {
                    item.AddImmediateConflict(op);
                }
            } else {
                for (auto& item : Parent.GlobalPlannedReaders) {
                    op->AddDependency(&item);
                }
                for (auto& item : Parent.GlobalPlannedWriters) {
                    op->AddDependency(&item);
                }
                for (auto& item : Parent.GlobalImmediateReaders) {
                    op->AddImmediateConflict(&item);
                }
                for (auto& item : Parent.GlobalImmediateWriters) {
                    op->AddImmediateConflict(&item);
                }
            }
        }

        if (isGlobalReader) {
            // We are potentially reading all keys in all tables, thus we conflict with all writes
            if (op->IsImmediate()) {
                for (auto& item : Parent.AllPlannedWriters) {
                    item.AddImmediateConflict(op);
                }
            } else {
                for (auto& item : Parent.AllPlannedWriters) {
                    op->AddDependency(&item);
                }
                for (auto& item : Parent.AllImmediateWriters) {
                    op->AddImmediateConflict(&item);
                }
            }
        } else if (haveReads) {
            // Each read may conflict with previous writes
            Parent.FlushPlannedWrites();
            if (!op->IsImmediate()) {
                Parent.FlushImmediateWrites();
            }
            for (const auto& read : Parent.TmpRead) {
                if (auto it = Parent.Tables.find(read.TableId); it != Parent.Tables.end()) {
                    auto searchRange = MakeSearchRange(read.Key);
                    if (op->IsImmediate()) {
                        it->second.PlannedWrites.EachIntersection(searchRange, processImmediatePlanned);
                    } else {
                        it->second.PlannedWrites.EachIntersection(searchRange, processPlannedPlanned);
                        it->second.ImmediateWrites.EachIntersection(searchRange, processPlannedImmediate);
                    }
                }
            }

            // If we have any reads we conflict with global writes
            // But if we also have writes, then we already added them above
            if (!haveWrites) {
                if (op->IsImmediate()) {
                    for (auto& item : Parent.GlobalPlannedWriters) {
                        item.AddImmediateConflict(op);
                    }
                } else {
                    for (auto& item : Parent.GlobalPlannedWriters) {
                        op->AddDependency(&item);
                    }
                    for (auto& item : Parent.GlobalImmediateWriters) {
                        op->AddImmediateConflict(&item);
                    }
                }
            }
        }
    }

    // Third pass, add new operation to relevant tables
    if (isGlobalWriter) {
        // Global writer transactions conflict with everything, so we only add them once
        // If it op is also is global reader it is shadowed by being a global writer
        if (op->IsImmediate()) {
            Parent.GlobalImmediateWriters.PushBack(op.Get());
        } else {
            Parent.GlobalPlannedWriters.PushBack(op.Get());
        }
    } else {
        if (haveWrites) {
            if (op->IsImmediate()) {
                if (DelayImmediateRanges) {
                    op->SetDelayedKnownWrites(Parent.TmpWrite);
                    Parent.DelayedImmediateWrites.PushBack(op.Get());
                } else {
                    Parent.AddImmediateWrites(op, Parent.TmpWrite);
                }
            } else {
                if (DelayPlannedRanges) {
                    op->SetDelayedKnownWrites(Parent.TmpWrite);
                    Parent.DelayedPlannedWrites.PushBack(op.Get());
                } else {
                    Parent.AddPlannedWrites(op, Parent.TmpWrite);
                }
            }
        }

        if (isGlobalReader) {
            if (op->IsImmediate()) {
                Parent.GlobalImmediateReaders.PushBack(op.Get());
            } else {
                Parent.GlobalPlannedReaders.PushBack(op.Get());
            }
        } else if (haveReads) {
            if (op->IsImmediate()) {
                if (DelayImmediateRanges) {
                    op->SetDelayedKnownReads(Parent.TmpRead);
                    Parent.DelayedImmediateReads.PushBack(op.Get());
                } else {
                    Parent.AddImmediateReads(op, Parent.TmpRead);
                }
            } else {
                if (DelayPlannedRanges) {
                    op->SetDelayedKnownReads(Parent.TmpRead);
                    Parent.DelayedPlannedReads.PushBack(op.Get());
                } else {
                    Parent.AddPlannedReads(op, Parent.TmpRead);
                }
            }
        }
    }

    if (isGlobalWriter || haveWrites) {
        if (op->IsImmediate()) {
            Parent.AllImmediateWriters.PushBack(op.Get());
        } else {
            Parent.AllPlannedWriters.PushBack(op.Get());
        }
    } else if (isGlobalReader || haveReads) {
        if (op->IsImmediate()) {
            Parent.AllImmediateReaders.PushBack(op.Get());
        } else {
            Parent.AllPlannedReaders.PushBack(op.Get());
        }
    }

    if (const ui64 lockTxId = op->LockTxId()) {
        // Add hard dependency on all operations that worked with the same lock
        auto& lastLockOp = Parent.LastLockOps[lockTxId];
        if (lastLockOp != op) {
            op->AddAffectedLock(lockTxId);
            if (lastLockOp) {
                op->AddDependency(lastLockOp);
            }
            lastLockOp = op;
        }

        // Update lock state with worst case prediction
        if (isGlobalReader) {
            auto& lockState = Parent.Locks[lockTxId];
            if (!lockState.Initialized) {
                lockState.WholeShard = true;
                lockState.Initialized = true;
            } else if (!lockState.WholeShard && !lockState.Broken) {
                SwapConsume(lockState.Keys);
                lockState.WholeShard = true;
            }
        } else if (haveReads) {
            auto lock = Parent.Self->SysLocksTable().GetRawLock(lockTxId);
            Y_ASSERT(!lock || !lock->IsBroken());
            auto& lockState = Parent.Locks[lockTxId];
            if (!lockState.Initialized) {
                if (lock) {
                    if (lock->IsBroken()) {
                        lockState.Broken = true;
                    } else if (lock->IsShardLock()) {
                        lockState.WholeShard = true;
                    } else {
                        for (const auto& point : lock->GetPoints()) {
                            lockState.Keys.emplace_back(point.Table->GetTableId().LocalPathId, point.ToOwnedTableRange());
                        }
                        for (const auto& range : lock->GetRanges()) {
                            lockState.Keys.emplace_back(range.Table->GetTableId().LocalPathId, range.ToOwnedTableRange());
                        }
                    }
                }
                lockState.Initialized = true;
            }
            if (!lockState.WholeShard && !lockState.Broken) {
                lockState.Keys.insert(lockState.Keys.end(), Parent.TmpRead.begin(), Parent.TmpRead.end());
            }
        }
    }

    if (haveReads) {
        Parent.ClearTmpRead();
    }

    if (haveWrites) {
        Parent.ClearTmpWrite();
    }
}

void TDependencyTracker::TDefaultDependencyTrackingLogic::RemoveOperation(const TOperation::TPtr& op) const noexcept {
    if (Parent.LastSnapshotOp == op) {
        Parent.LastSnapshotOp = nullptr;
    }

    for (ui64 lockTxId : op->GetAffectedLocks()) {
        auto it = Parent.LastLockOps.find(lockTxId);
        if (it != Parent.LastLockOps.end() && it->second == op) {
            Parent.Locks.erase(lockTxId);
            Parent.LastLockOps.erase(it);
        }
    }

    op->UnlinkFromList<TOperationAllListTag>();
    op->UnlinkFromList<TOperationGlobalListTag>();

    if (op->IsInList<TOperationDelayedReadListTag>()) {
        op->UnlinkFromList<TOperationDelayedReadListTag>();
        op->RemoveDelayedKnownReads();
    } else {
        for (auto& kv : Parent.Tables) {
            if (op->IsImmediate()) {
                kv.second.ImmediateReads.RemoveRanges(op);
            } else {
                kv.second.PlannedReads.RemoveRanges(op);
            }
        }
    }

    if (op->IsInList<TOperationDelayedWriteListTag>()) {
        op->UnlinkFromList<TOperationDelayedWriteListTag>();
        op->RemoveDelayedKnownWrites();
    } else {
        for (auto& kv : Parent.Tables) {
            if (op->IsImmediate()) {
                kv.second.ImmediateWrites.RemoveRanges(op);
            } else {
                kv.second.PlannedWrites.RemoveRanges(op);
            }
        }
    }
}

void TDependencyTracker::TMvccDependencyTrackingLogic::AddOperation(const TOperation::TPtr& op) const noexcept {
    if (op->IsUsingSnapshot()) {
        return;
    }

    // WARNING: kqp scan transactions don't have known keys, that means
    // we must assume they are global reader and must be careful not to
    // look into extracted keys for these transactions.
    bool haveKeys = !op->IsKqpScanTransaction();

    bool isGlobalReader = op->IsGlobalReader();
    bool isGlobalWriter = op->IsGlobalWriter();

    bool tooManyKeys = false;
    if (haveKeys && op->KeysCount() > MAX_REORDER_TX_KEYS) {
        tooManyKeys = true;
    }

    // Non-immediate snapshot operations cannot be reordered
    if (!op->IsImmediate() && op->IsSnapshotTx()) {
        if (Parent.LastSnapshotOp) {
            op->AddDependency(Parent.LastSnapshotOp);
        }
        Parent.LastSnapshotOp = op;
    }

    // We use an optimistic readVersion assuming this transaction is read-only.
    // If it happens that this transaction is not read-only we would include
    // some locked keys in conflicts that are actually broken, but it's not
    // a problem.
    TRowVersion readVersion = Parent.Self->GetMvccTxVersion(EMvccTxMode::ReadOnly, op.Get());

    // First pass, gather all reads/writes expanded with locks, add lock based dependencies
    bool haveReads = false;
    bool haveWrites = false;
    bool commitWriteLock = false;
    if (haveKeys) {
        size_t keysCount = 0;
        const auto& keysInfo = op->GetKeysInfo();
        const auto& locksCache = op->LocksCache();
        for (const auto& vk : keysInfo.Keys) {
            const auto& k = *vk.Key;
            if (Parent.Self->IsUserTable(k.TableId)) {
                const ui64 tableId = k.TableId.PathId.LocalPathId;
                Y_VERIFY_DEBUG_S(!k.Range.IsAmbiguous(Parent.Self->GetUserTables().at(tableId)->KeyColumnTypes.size()),
                    (vk.IsWrite ? "Write" : "Read")
                    << " From# \"" << EscapeC(TSerializedCellVec::Serialize(k.Range.From)) << "\""
                    << " To# \"" << EscapeC(TSerializedCellVec::Serialize(k.Range.To)) << "\""
                    << " InclusiveFrom# " << (k.Range.InclusiveFrom ? "true" : "false")
                    << " InclusiveTo# " << (k.Range.InclusiveTo ? "true" : "false")
                    << " Point# " << (k.Range.Point ? "true" : "false")
                    << ": " << k.Range.IsAmbiguousReason(Parent.Self->GetUserTables().at(tableId)->KeyColumnTypes.size()));
                if (!tooManyKeys && ++keysCount > MAX_REORDER_TX_KEYS) {
                    tooManyKeys = true;
                }
                if (vk.IsWrite) {
                    haveWrites = true;
                    if (!tooManyKeys && !isGlobalWriter) {
                        Parent.TmpWrite.emplace_back(tableId, k.Range);
                    }
                } else {
                    haveReads = true;
                    if (!tooManyKeys && !isGlobalReader) {
                        Parent.TmpRead.emplace_back(tableId, k.Range);
                    }
                }
            } else if (TSysTables::IsLocksTable(k.TableId)) {
                Y_ABORT_UNLESS(k.Range.Point, "Unexpected non-point read from the locks table");
                const ui64 lockTxId = Parent.Self->SysLocksTable().ExtractLockTxId(k.Range.From);

                // Add hard dependency on all operations that worked with the same lock
                auto& lastLockOp = Parent.LastLockOps[lockTxId];
                if (lastLockOp != op) {
                    op->AddAffectedLock(lockTxId);
                    if (lastLockOp) {
                        op->AddDependency(lastLockOp);
                    }
                    lastLockOp = op;
                }

                // Reading a lock means checking it for validity, i.e. "reading" those predicted keys
                if (!vk.IsWrite) {
                    auto lock = Parent.Self->SysLocksTable().GetRawLock(lockTxId, readVersion);

                    if (lock) {
                        lock->SetLastOpId(op->GetTxId());
                        if (locksCache.Locks.contains(lockTxId) && lock->IsPersistent() && !lock->IsFrozen()) {
                            // This lock was cached before, and since we know
                            // it's persistent, we know it was also frozen
                            // during that lock caching. Restore the frozen
                            // flag for this lock.
                            // Note: this code path is only for older shards
                            // which didn't persist the frozen flag.
                            lock->SetFrozen();
                        }
                    }

                    if (lock && lock->IsWriteLock()) {
                        commitWriteLock = true;
                    }

                    if (lock && !op->IsImmediate()) {
                        // We must be careful not to reorder operations that we
                        // know break each other on commit. These dependencies
                        // are only needed between planned operations, and
                        // always point one way (towards older operations), so
                        // there are no deadlocks. Immediate operations will
                        // detect breaking attempts at runtime and may decide
                        // to add new dependencies dynamically, depending on
                        // the real order between operations.
                        lock->ForAllConflicts([&](auto* conflictLock) {
                            if (auto conflictOp = Parent.FindLastLockOp(conflictLock->GetLockId())) {
                                if (!conflictOp->IsImmediate()) {
                                    op->AddDependency(conflictOp);
                                }
                            }
                        });
                    }

                    if (auto it = locksCache.Locks.find(lockTxId); it != locksCache.Locks.end()) {
                        // This transaction uses locks cache, so lock check
                        // outcome was persisted and restored. Unfortunately
                        // now we cannot know which keys it translated to, and
                        // have to assume "whole shard" worst case.
                        isGlobalReader = true;
                    } else if (auto it = Parent.Locks.find(lockTxId); it != Parent.Locks.end()) {
                        haveReads = true;
                        if (!tooManyKeys && (keysCount += it->second.Keys.size()) > MAX_REORDER_TX_KEYS) {
                            tooManyKeys = true;
                        }
                        if (!tooManyKeys && !isGlobalReader) {
                            if (it->second.WholeShard) {
                                isGlobalReader = true;
                            } else {
                                Parent.TmpRead.insert(Parent.TmpRead.end(), it->second.Keys.begin(), it->second.Keys.end());
                            }
                        }
                    } else if (lock) {
                        Y_ASSERT(!lock->IsBroken(readVersion));
                        haveReads = true;
                        if (!tooManyKeys && (keysCount += (lock->NumPoints() + lock->NumRanges())) > MAX_REORDER_TX_KEYS) {
                            tooManyKeys = true;
                        }
                        if (!tooManyKeys && !isGlobalReader) {
                            if (lock->IsShardLock()) {
                                isGlobalReader = true;
                            } else {
                                for (const auto& point : lock->GetPoints()) {
                                    Parent.TmpRead.emplace_back(point.Table->GetTableId().LocalPathId, point.ToOwnedTableRange());
                                }
                                for (const auto& range : lock->GetRanges()) {
                                    Parent.TmpRead.emplace_back(range.Table->GetTableId().LocalPathId, range.ToOwnedTableRange());
                                }
                            }
                        }
                    }
                }
            }
        }

        // Distributed commits of some unknown keys are complicated, and mean
        // there are almost certainly readsets involved and it's difficult to
        // make it atomic, so we currently make them global writers to handle
        // out-of-order execution issues.
        // We also can't allow immediate commits to happen between readset
        // generation and applying effects, so we have to make them global
        // writers as well.
        // TODO: figure out how to handle lock conflicts directly
        if (commitWriteLock) {
            isGlobalWriter = true;
        }

        if (tooManyKeys) {
            if (haveReads) {
                isGlobalReader = true;
            }
            if (haveWrites) {
                isGlobalWriter = true;
            }
        }

        if (haveReads && isGlobalReader) {
            Parent.ClearTmpRead();
            haveReads = false;
        }

        if (haveWrites && isGlobalWriter) {
            Parent.ClearTmpWrite();
            haveWrites = false;
        }
    }

    TRowVersion snapshot = TRowVersion::Max();
    bool snapshotRepeatable = false;
    if (op->IsMvccSnapshotRead()) {
        snapshot = op->GetMvccSnapshot();
        snapshotRepeatable = op->IsMvccSnapshotRepeatable();
    } else if (op->IsImmediate() && (op->IsReadTable() || op->IsDataTx() && !haveWrites && !isGlobalWriter && !commitWriteLock)) {
        snapshot = readVersion;
        op->SetMvccSnapshot(snapshot, /* repeatable */ false);
    }

    if (snapshotRepeatable) {
        // Repeatable snapshot writes are uncommitted, not externally visible, and don't conflict with anything
        isGlobalWriter = false;
        if (haveWrites) {
            Parent.ClearTmpWrite();
            haveWrites = false;
        }
    }

    auto onImmediateConflict = [&](TOperation& conflict) {
        Y_ABORT_UNLESS(!conflict.IsImmediate());
        if (snapshot.IsMax()) {
            conflict.AddImmediateConflict(op);
        } else if (IsLess(conflict, snapshot)) {
            op->AddDependency(&conflict);
        } else if (IsEqual(conflict, snapshot)) {
            op->AddRepeatableReadConflict(&conflict);
        }
    };

    auto processImmediatePlanned = [&](const TRangeTreeBase::TRange&, const TOperation::TPtr& conflict) {
        onImmediateConflict(*conflict);
    };

    auto processPlannedPlanned = [&](const TRangeTreeBase::TRange&, const TOperation::TPtr& conflict) {
        op->AddDependency(conflict);
    };

    auto processPlannedImmediate = [&](const TRangeTreeBase::TRange&, const TOperation::TPtr& conflict) {
        op->AddImmediateConflict(conflict);
    };

    // Second pass, add dependencies
    if (isGlobalWriter) {
        Y_ASSERT(snapshot.IsMax());

        // We are potentially writing to all keys in all tables, thus we conflict with everything
        if (op->IsImmediate()) {
            for (auto& item : Parent.AllPlannedWriters) {
                item.AddImmediateConflict(op);
            }
        } else {
            for (auto& item : Parent.AllPlannedWriters) {
                op->AddDependency(&item);
            }
            for (auto& item : Parent.AllImmediateWriters) {
                op->AddImmediateConflict(&item);
            }
            // In mvcc case we skip immediate readers because they read at a fixed time point,
            // so that they already calculated all their dependencies and cannot have new ones,
            // however we are interesting in immediate readers who are immediate writers as well,
            // but we already processed such transactions in the previous loop.
        }
    } else {
        if (haveWrites) {
            Y_ASSERT(snapshot.IsMax());

            // Each write may conflict with previous writes
            Parent.FlushPlannedWrites();
            if (!op->IsImmediate()) {
                Parent.FlushImmediateReads();
                Parent.FlushImmediateWrites();
            }
            for (const auto& write : Parent.TmpWrite) {
                if (auto it = Parent.Tables.find(write.TableId); it != Parent.Tables.end()) {
                    auto searchRange = MakeSearchRange(write.Key);
                    if (op->IsImmediate()) {
                        it->second.PlannedWrites.EachIntersection(searchRange, processImmediatePlanned);
                    } else {
                        it->second.PlannedWrites.EachIntersection(searchRange, processPlannedPlanned);
                        // You should consider immediate reads as reads in scope of a writing immediate transaction
                        it->second.ImmediateReads.EachIntersection(searchRange, processPlannedImmediate);
                        it->second.ImmediateWrites.EachIntersection(searchRange, processPlannedImmediate);
                    }
                }
            }

            // If we have any reads or writes we conflict with global writes
            if (op->IsImmediate()) {
                for (auto& item : Parent.GlobalPlannedWriters) {
                    item.AddImmediateConflict(op);
                }
            } else {
                for (auto& item : Parent.GlobalPlannedWriters) {
                    op->AddDependency(&item);
                }
                // Here we have immediate writers who are global readers as well
                for (auto& item : Parent.GlobalImmediateReaders) {
                    op->AddImmediateConflict(&item);
                }
                for (auto& item : Parent.GlobalImmediateWriters) {
                    op->AddImmediateConflict(&item);
                }
            }
        }

        if (isGlobalReader) {
            // We are potentially reading all keys in all tables, thus we conflict with all writes
            if (op->IsImmediate()) {
                for (auto& item : Parent.AllPlannedWriters) {
                    onImmediateConflict(item);
                }
            } else {
                for (auto& item : Parent.AllPlannedWriters) {
                    op->AddDependency(&item);
                }
                // Since each happened read moves incomplete edge, neither immediate write cannot affect
                // such read, that's why a planned reader cannot be a dependency for any immediate write
            }
        } else if (haveReads) {
            // Each read may conflict with previous writes
            Parent.FlushPlannedWrites();
            for (const auto& read : Parent.TmpRead) {
                if (auto it = Parent.Tables.find(read.TableId); it != Parent.Tables.end()) {
                    auto searchRange = MakeSearchRange(read.Key);
                    if (op->IsImmediate()) {
                        it->second.PlannedWrites.EachIntersection(searchRange, processImmediatePlanned);
                    } else {
                        it->second.PlannedWrites.EachIntersection(searchRange, processPlannedPlanned);
                    }
                }
            }

            // If we have any reads we conflict with previous global writes
            // But if we also have writes, then we already added them above.
            if (!haveWrites) {
                if (op->IsImmediate()) {
                    for (auto& item : Parent.GlobalPlannedWriters) {
                        onImmediateConflict(item);
                    }
                } else {
                    for (auto& item : Parent.GlobalPlannedWriters) {
                        op->AddDependency(&item);
                    }
                    // Since each happened read moves incomplete edge, neither immediate write cannot affect
                    // such read, that's why a planned reader cannot be a dependency for any immediate write
                }
            }
        }
    }

    // Third pass, add new operation to relevant tables
    if (isGlobalWriter) {
        // Global writer transactions conflict with everything, so we only add them once
        // If the op is also a reader it's shadowed by being a global writer
        if (op->IsImmediate()) {
            Parent.GlobalImmediateWriters.PushBack(op.Get());
        } else {
            Parent.GlobalPlannedWriters.PushBack(op.Get());
        }
    } else {
        if (haveWrites) {
            if (op->IsImmediate()) {
                if (DelayImmediateRanges) {
                    op->SetDelayedKnownWrites(Parent.TmpWrite);
                    Parent.DelayedImmediateWrites.PushBack(op.Get());
                } else {
                    Parent.AddImmediateWrites(op, Parent.TmpWrite);
                }

                // In mvcc case we don't track reads, but sice a writing immediate transaction may be
                // rescheduled, a read in scope of the writing immediate transaction is an exception
                if (isGlobalReader) {
                    Parent.GlobalImmediateReaders.PushBack(op.Get());
                } else if (haveReads) {
                    if (DelayImmediateRanges) {
                        op->SetDelayedKnownReads(Parent.TmpRead);
                        Parent.DelayedImmediateReads.PushBack(op.Get());
                    } else {
                        Parent.AddImmediateReads(op, Parent.TmpRead);
                    }
                }
            } else {
                if (DelayPlannedRanges) {
                    op->SetDelayedKnownWrites(Parent.TmpWrite);
                    Parent.DelayedPlannedWrites.PushBack(op.Get());
                } else {
                    Parent.AddPlannedWrites(op, Parent.TmpWrite);
                }
            }
        }
        // In mvcc case we don't track planned reads, immediate reads
        // are tracked in a special case, this is already done above
    }

    if (isGlobalWriter || haveWrites) {
        if (op->IsImmediate()) {
            Parent.AllImmediateWriters.PushBack(op.Get());
        } else {
            Parent.AllPlannedWriters.PushBack(op.Get());
        }
    }
    // In mvcc case we don't track planned reads, immediate reads
    // are tracked in a special case, this is already done above

    if (const ui64 lockTxId = op->LockTxId()) {
        // Add hard dependency on all operations that worked with the same lock
        auto& lastLockOp = Parent.LastLockOps[lockTxId];
        if (lastLockOp != op) {
            op->AddAffectedLock(lockTxId);
            if (lastLockOp) {
                op->AddDependency(lastLockOp);
            }
            lastLockOp = op;
        }

        // Update lock state with worst case prediction
        if (isGlobalReader) {
            auto& lockState = Parent.Locks[lockTxId];
            if (!lockState.Initialized) {
                lockState.WholeShard = true;
                lockState.Initialized = true;
            } else if (!lockState.WholeShard && !lockState.Broken) {
                SwapConsume(lockState.Keys);
                lockState.WholeShard = true;
            }
        } else if (haveReads) {
            auto lock = Parent.Self->SysLocksTable().GetRawLock(lockTxId, readVersion);
            Y_ASSERT(!lock || !lock->IsBroken(readVersion));
            auto& lockState = Parent.Locks[lockTxId];
            if (!lockState.Initialized) {
                if (lock) {
                    if (lock->IsBroken(readVersion)) {
                        lockState.Broken = true;
                    } else if (lock->IsShardLock()) {
                        lockState.WholeShard = true;
                    } else {
                        for (const auto& point : lock->GetPoints()) {
                            lockState.Keys.emplace_back(point.Table->GetTableId().LocalPathId, point.ToOwnedTableRange());
                        }
                        for (const auto& range : lock->GetRanges()) {
                            lockState.Keys.emplace_back(range.Table->GetTableId().LocalPathId, range.ToOwnedTableRange());
                        }
                    }
                }
                lockState.Initialized = true;
            }
            if (!lockState.WholeShard && !lockState.Broken) {
                lockState.Keys.insert(lockState.Keys.end(), Parent.TmpRead.begin(), Parent.TmpRead.end());
            }
        }
    }

    if (haveReads) {
        Parent.ClearTmpRead();
    }

    if (haveWrites) {
        Parent.ClearTmpWrite();
    }
}

void TDependencyTracker::TMvccDependencyTrackingLogic::RemoveOperation(const TOperation::TPtr& op) const noexcept {
    if (Parent.LastSnapshotOp == op) {
        Parent.LastSnapshotOp = nullptr;
    }

    for (ui64 lockTxId : op->GetAffectedLocks()) {
        auto it = Parent.LastLockOps.find(lockTxId);
        if (it != Parent.LastLockOps.end() && it->second == op) {
            Parent.Locks.erase(lockTxId);
            Parent.LastLockOps.erase(it);
        }
    }

    op->UnlinkFromList<TOperationAllListTag>();
    op->UnlinkFromList<TOperationGlobalListTag>();

    if (op->IsInList<TOperationDelayedReadListTag>()) {
        op->UnlinkFromList<TOperationDelayedReadListTag>();
        op->RemoveDelayedKnownReads();
    } else {
        for (auto& kv : Parent.Tables) {
            if (op->IsImmediate()) {
                kv.second.ImmediateReads.RemoveRanges(op);
            } else {
                kv.second.PlannedReads.RemoveRanges(op);
            }
        }
    }

    if (op->IsInList<TOperationDelayedWriteListTag>()) {
        op->UnlinkFromList<TOperationDelayedWriteListTag>();
        op->RemoveDelayedKnownWrites();
    } else {
        for (auto& kv : Parent.Tables) {
            if (op->IsImmediate()) {
                kv.second.ImmediateWrites.RemoveRanges(op);
            } else {
                kv.second.PlannedWrites.RemoveRanges(op);
            }
        }
    }
}

void TDependencyTracker::TFollowerDependencyTrackingLogic::AddOperation(const TOperation::TPtr&) const noexcept {
    // all follower operations are readonly and don't conflict
}

void TDependencyTracker::TFollowerDependencyTrackingLogic::RemoveOperation(const TOperation::TPtr&) const noexcept {
    // all follower operations are readonly and don't conflict
}

} // namespace NDataShard
} // namespace NKikimr
