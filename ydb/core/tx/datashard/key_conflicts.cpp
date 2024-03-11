#include "key_conflicts.h"
#include <ydb/core/tx/locks/sys_tables.h>

namespace NKikimr {
namespace NDataShard {

using TValidationInfo = NMiniKQL::IEngineFlat::TValidationInfo;
using TValidatedKey = NMiniKQL::IEngineFlat::TValidatedKey;

namespace {

bool HasKeyConflict(const TKeyDesc& aKey,
                    const TKeyDesc& bKey)
{
    return CheckRangesOverlap(aKey.Range, bKey.Range, aKey.KeyColumnTypes, bKey.KeyColumnTypes);
}

bool HasKeyConflict(const TValidatedKey& a,
                    const TValidatedKey& b)
{
    Y_ABORT_UNLESS(a.Key && b.Key);
    Y_ABORT_UNLESS(a.IsWrite || b.IsWrite);

    const TKeyDesc& aKey = *a.Key;
    const TKeyDesc& bKey = *b.Key;

    bool aLocks = TSysTables::IsLocksTable(aKey.TableId);
    bool bLocks = TSysTables::IsLocksTable(bKey.TableId);

    // We must check for lock conflicts even when different table versions are used
    if (aLocks && bLocks) {
        using TLocksTable = TSysTables::TLocksTable;

        ui64 aLockId, bLockId;
        Y_ABORT_UNLESS(aKey.Range.Point && bKey.Range.Point, "Unexpected non-point locks table key accesses");
        bool ok = TLocksTable::ExtractKey(aKey.Range.From, TLocksTable::EColumns::LockId, aLockId) &&
                TLocksTable::ExtractKey(bKey.Range.From, TLocksTable::EColumns::LockId, bLockId);
        Y_ABORT_UNLESS(ok, "Cannot extract LockId from locks table key accesses");

        // Only conflict on the same LockId
        return aLockId == bLockId;
    }

    // There is no conflict between lock and non-lock tables
    if (aLocks || bLocks) {
        return false;
    }

    // Future support for colocated tables: different tables never conflict
    if (Y_LIKELY(aKey.TableId.HasSamePath(bKey.TableId))) {
        return HasKeyConflict(aKey, bKey);
    }

    return false;
}

}

/// @note O(N^2) in worst case
bool HasKeyConflict(const TValidationInfo& infoA,
                    const TValidationInfo& infoB)
{
    if (!infoA.HasWrites() && !infoB.HasWrites())
        return false;

    for (const TValidatedKey& a : infoA.Keys) {
        if (!a.IsWrite && !infoB.HasWrites())
            continue;
        for (const TValidatedKey& b : infoB.Keys) {
            if (!a.IsWrite && !b.IsWrite)
                continue;
            if (HasKeyConflict(a, b))
                return true;
        }
    }
    return false;
}

} // namespace NDataShard
} // namespace NKikimr
