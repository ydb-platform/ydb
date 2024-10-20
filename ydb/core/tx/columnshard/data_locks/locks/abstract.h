#pragma once
#include <ydb/library/accessor/accessor.h>

#include <util/generic/string.h>
#include <util/generic/hash_set.h>

#include <optional>
#include <memory>
#include <vector>

namespace NKikimr::NOlap {
class TPortionInfo;
class TGranuleMeta;
}

namespace NKikimr::NOlap::NDataLocks {

enum class EAction {
    Create,
    Read,
    Modify,
    Delete,
};

enum class EOriginator {
    Tx, //distributed transaction
    Bg, //background process
};

struct TLockScope {
    EAction Action;
    EOriginator Originator;
};

class ILock {
private:
    YDB_READONLY_DEF(TString, LockName);
public:
    using TPtr = std::unique_ptr<ILock>;
    virtual bool IsEqualTo(ILock& lock) { Y_UNUSED(lock); return false; };
    virtual bool IsCompatibleWith(ILock& lock) { Y_UNUSED(lock);  return true; }
    virtual std::optional<TString> IsLocked(const TPortionInfo& portion, const TLockScope& scope) const = 0;
    virtual std::optional<TString> IsLocked(const TGranuleMeta& granule, const TLockScope& scope) const = 0;
    virtual std::optional<TString> IsLockedTableSchema(const ui64 pathId, const TLockScope& scope) const = 0;
    virtual bool IsEmpty() const = 0;
public:
    ILock(const TString& lockName)
        : LockName(lockName)
    {}
    virtual ~ILock() = default;
};

} //NKikimr::NOlap::NDataLocks