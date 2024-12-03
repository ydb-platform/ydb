#pragma once
#include <ydb/library/accessor/accessor.h>

#include <util/generic/hash_set.h>
#include <util/generic/string.h>

#include <memory>
#include <optional>
#include <set>
#include <vector>

namespace NKikimr::NOlap {
class TPortionInfo;
class TGranuleMeta;
}   // namespace NKikimr::NOlap

namespace NKikimr::NOlap::NDataLocks {

enum class ELockCategory : ui32 {
    Compaction = 0,
    Cleanup,
    Sharing,
    Actualization,
    Tables,
    Any,
    MAX
};

static const inline std::array<std::set<ELockCategory>, (ui32)ELockCategory::MAX> LockCategoriesInteraction = {
    //Compaction
    std::set<ELockCategory>({ ELockCategory::Compaction, ELockCategory::Actualization, ELockCategory::Tables, ELockCategory::Any}),
    //Cleanup
    std::set<ELockCategory>({ ELockCategory::Cleanup, ELockCategory::Sharing, ELockCategory::Tables, ELockCategory::Any }),
    //Sharing
    std::set<ELockCategory>({ ELockCategory::Sharing, ELockCategory::Cleanup, ELockCategory::Tables, ELockCategory::Any }),
    //Actualization
    std::set<ELockCategory>({ ELockCategory::Actualization, ELockCategory::Compaction, ELockCategory::Tables, ELockCategory::Any }),
    //Tables
    std::set<ELockCategory>({ ELockCategory::Cleanup, ELockCategory::Sharing, ELockCategory::Actualization, ELockCategory::Compaction,
        ELockCategory::Tables, ELockCategory::Any }),
    //Any
    std::set<ELockCategory>({ ELockCategory::Cleanup, ELockCategory::Sharing, ELockCategory::Actualization, ELockCategory::Compaction,
        ELockCategory::Tables, ELockCategory::Any }),
};

class ILock {
private:
    YDB_READONLY_DEF(TString, LockName);
    YDB_READONLY_FLAG(ReadOnly, false);
    const ELockCategory Category;

protected:
    virtual std::optional<TString> DoIsLocked(
        const TPortionInfo& portion, const ELockCategory category, const THashSet<TString>& excludedLocks = {}) const = 0;
    virtual std::optional<TString> DoIsLocked(
        const TGranuleMeta& granule, const ELockCategory category, const THashSet<TString>& excludedLocks = {}) const = 0;
    virtual bool DoIsEmpty() const = 0;

public:
    ILock(const TString& lockName, const ELockCategory category, const bool isReadOnly = false)
        : LockName(lockName)
        , ReadOnlyFlag(isReadOnly)
        , Category(category) {
    }

    virtual ~ILock() = default;

    std::optional<TString> IsLocked(const TPortionInfo& portion, const ELockCategory portionForLock, const THashSet<TString>& excludedLocks = {},
        const bool readOnly = false) const {
        if (IsReadOnly() && readOnly) {
            return {};
        }
        if (!LockCategoriesInteraction[(ui32)Category].contains(portionForLock)) {
            return {};
        }
        return DoIsLocked(portion, portionForLock, excludedLocks);
    }
    std::optional<TString> IsLocked(const TGranuleMeta& g, const ELockCategory portionForLock, const THashSet<TString>& excludedLocks = {},
        const bool readOnly = false) const {
        if (IsReadOnly() && readOnly) {
            return {};
        }
        if (!LockCategoriesInteraction[(ui32)Category].contains(portionForLock)) {
            return {};
        }
        return DoIsLocked(g, portionForLock, excludedLocks);
    }
    bool IsEmpty() const {
        return DoIsEmpty();
    }
};

}   // namespace NKikimr::NOlap::NDataLocks
