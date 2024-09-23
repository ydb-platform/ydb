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

class ILock {
private:
    YDB_READONLY_DEF(TString, LockName);
    YDB_READONLY_FLAG(ReadOnly, false);
protected:
    virtual std::optional<TString> DoIsLocked(const TPortionInfo& portion, const THashSet<TString>& excludedLocks = {}) const = 0;
    virtual std::optional<TString> DoIsLocked(const TGranuleMeta& granule, const THashSet<TString>& excludedLocks = {}) const = 0;
    virtual bool DoIsEmpty() const = 0;
public:
    ILock(const TString& lockName, const bool isReadOnly = false)
        : LockName(lockName)
        , ReadOnlyFlag(isReadOnly)
    {

    }

    virtual ~ILock() = default;

    std::optional<TString> IsLocked(const TPortionInfo& portion, const THashSet<TString>& excludedLocks = {}, const bool readOnly = false) const {
        if (IsReadOnly() && readOnly) {
            return {};
        }
        return DoIsLocked(portion, excludedLocks);
    }
    std::optional<TString> IsLocked(const TGranuleMeta& g, const THashSet<TString>& excludedLocks = {}, const bool readOnly = false) const {
        if (IsReadOnly() && readOnly) {
            return {};
        }
        return DoIsLocked(g, excludedLocks);
    }
    bool IsEmpty() const {
        return DoIsEmpty();
    }
};

}