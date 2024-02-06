#pragma once

#include "datashard.h"
#include "datashard_user_table.h"
#include "datashard_active_transaction.h"
#include <ydb/core/tx/locks/range_treap.h>

#include <library/cpp/containers/absl_flat_hash/flat_hash_map.h>

namespace NKikimr {
namespace NDataShard {

/**
 * TDependencyTracker - tracks dependencies between operations
 */
class TDependencyTracker {
private:
    using TKeys = TVector<TOperationKey>;

    struct TOperationPtrTraits {
        static bool Less(const TOperation::TPtr& a, const TOperation::TPtr& b) noexcept {
            return a.Get() < b.Get();
        }

        static bool Equal(const TOperation::TPtr& a, const TOperation::TPtr& b) noexcept {
            return a.Get() == b.Get();
        }
    };

    /**
     * Predicts the state of a lock in the future
     */
    struct TLockPrediction {
        // For each table tracks the full list of ranges
        TKeys Keys;
        // When true the lock is known to be broken
        bool Broken = false;
        // When true the range is "everything in all tables"
        bool WholeShard = false;
        // When true lock has been imported from lock store
        bool Initialized = false;
    };

    /**
     * Tracks current per-table read/write sets
     */
    struct TTableState {
        // Mapping from read/write ranges to corresponding transactions
        TRangeTreap<TOperation::TPtr, TOperationPtrTraits> PlannedReads;
        TRangeTreap<TOperation::TPtr, TOperationPtrTraits> PlannedWrites;
        TRangeTreap<TOperation::TPtr, TOperationPtrTraits> ImmediateReads;
        TRangeTreap<TOperation::TPtr, TOperationPtrTraits> ImmediateWrites;
    };

    struct TDependencyTrackingLogic {
        TDependencyTracker& Parent;

        explicit TDependencyTrackingLogic(TDependencyTracker& parent)
            : Parent(parent) {}

        // Adds operation to the tracker
        virtual void AddOperation(const TOperation::TPtr& op) const noexcept = 0;

        // Removes operation from the tracker, no future operations may conflict with it
        virtual void RemoveOperation(const TOperation::TPtr& op) const noexcept = 0;
    };

    struct TDefaultDependencyTrackingLogic : public TDependencyTrackingLogic {
        explicit TDefaultDependencyTrackingLogic(TDependencyTracker& parent)
            : TDependencyTrackingLogic(parent) {}

        void AddOperation(const TOperation::TPtr& op) const noexcept override;
        void RemoveOperation(const TOperation::TPtr& op) const noexcept override;
    };

    struct TMvccDependencyTrackingLogic : public TDependencyTrackingLogic {
        explicit TMvccDependencyTrackingLogic(TDependencyTracker& parent)
            : TDependencyTrackingLogic(parent) {}

        void AddOperation(const TOperation::TPtr& op) const noexcept override;
        void RemoveOperation(const TOperation::TPtr& op) const noexcept override;
    };

    struct TFollowerDependencyTrackingLogic : public TDependencyTrackingLogic {
        explicit TFollowerDependencyTrackingLogic(TDependencyTracker& parent)
            : TDependencyTrackingLogic(parent) {}

        void AddOperation(const TOperation::TPtr& op) const noexcept override;
        void RemoveOperation(const TOperation::TPtr& op) const noexcept override;
    };

public:
    TDependencyTracker(TDataShard* self)
        : Self(self)
    { }

public:
    // Called to update this table schema
    void UpdateSchema(const TPathId& tableId, const TUserTable& tableInfo) noexcept;

    // Calld to update this table schema upon move
    void RemoveSchema(const TPathId& tableId) noexcept;

    // Adds operation to the tracker
    void AddOperation(const TOperation::TPtr& op) noexcept {
        GetTrackingLogic().AddOperation(op);
    }

    // Removes operation from the tracker, no future operations may conflict with it
    void RemoveOperation(const TOperation::TPtr& op) noexcept {
        GetTrackingLogic().RemoveOperation(op);
    }

    TOperation::TPtr FindLastLockOp(ui64 lockTxId) const {
        auto it = LastLockOps.find(lockTxId);
        if (it != LastLockOps.end()) {
            return it->second;
        }
        return nullptr;
    }

private:
    void ClearTmpRead() noexcept;
    void ClearTmpWrite() noexcept;

    void AddPlannedReads(const TOperation::TPtr& op, const TKeys& reads) noexcept;
    void AddPlannedWrites(const TOperation::TPtr& op, const TKeys& writes) noexcept;
    void AddImmediateReads(const TOperation::TPtr& op, const TKeys& reads) noexcept;
    void AddImmediateWrites(const TOperation::TPtr& op, const TKeys& writes) noexcept;

    void FlushPlannedReads() noexcept;
    void FlushPlannedWrites() noexcept;
    void FlushImmediateReads() noexcept;
    void FlushImmediateWrites() noexcept;

    const TDependencyTrackingLogic& GetTrackingLogic() const noexcept;

private:
    TDataShard* const Self;
    // Temporary vectors for building dependencies
    TKeys TmpRead;
    TKeys TmpWrite;
    // Last planned snapshot operation
    TOperation::TPtr LastSnapshotOp;
    // Maps lock id to prediction of what this lock would look like when all transactions are complete
    absl::flat_hash_map<ui64, TLockPrediction> Locks;
    // Maps lock id to the last operation that used them, all lock operations are serialized
    absl::flat_hash_map<ui64, TOperation::TPtr> LastLockOps;
    // Maps table id to current ranges that have been read/written by all transactions
    absl::flat_hash_map<ui64, TTableState> Tables;
    // All operations that are reading or writing something
    TIntrusiveList<TOperation, TOperationAllListTag> AllPlannedReaders;
    TIntrusiveList<TOperation, TOperationAllListTag> AllPlannedWriters;
    TIntrusiveList<TOperation, TOperationAllListTag> AllImmediateReaders;
    TIntrusiveList<TOperation, TOperationAllListTag> AllImmediateWriters;
    // A set of operations that read/write table globally
    TIntrusiveList<TOperation, TOperationGlobalListTag> GlobalPlannedReaders;
    TIntrusiveList<TOperation, TOperationGlobalListTag> GlobalPlannedWriters;
    TIntrusiveList<TOperation, TOperationGlobalListTag> GlobalImmediateReaders;
    TIntrusiveList<TOperation, TOperationGlobalListTag> GlobalImmediateWriters;
    // Immediate operations that have delayed adding their keys to ranges
    TIntrusiveList<TOperation, TOperationDelayedReadListTag> DelayedPlannedReads;
    TIntrusiveList<TOperation, TOperationDelayedReadListTag> DelayedImmediateReads;
    TIntrusiveList<TOperation, TOperationDelayedWriteListTag> DelayedPlannedWrites;
    TIntrusiveList<TOperation, TOperationDelayedWriteListTag> DelayedImmediateWrites;

    const TDefaultDependencyTrackingLogic DefaultLogic{ *this };
    const TMvccDependencyTrackingLogic MvccLogic{ *this };
    const TFollowerDependencyTrackingLogic FollowerLogic{ *this };
};

} // namespace NDataShard
} // namespace NKikimr
