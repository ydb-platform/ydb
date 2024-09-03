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

template<typename T>
concept IsLocable = 
    std::same_as<T, TPortionInfo> || 
    std::same_as<T, TGranuleMeta> ||
    std::same_as<T, ui64>; //table pathId


class ILock {
private:
    YDB_READONLY_DEF(TString, LockName);
    YDB_READONLY_FLAG(ReadOnly, false);
protected:
    virtual std::optional<TString> DoIsLocked(const TPortionInfo& portion, const THashSet<TString>& excludedLocks = {}) const = 0;
    virtual std::optional<TString> DoIsLocked(const TGranuleMeta& granule, const THashSet<TString>& excludedLocks = {}) const = 0;
    virtual std::optional<TString> DoIsLocked(const ui64 pathId, const THashSet<TString>& excludedLocks = {}) const = 0;
    virtual bool DoIsEmpty() const = 0;
public:
    ILock(const TString& lockName, const bool isReadOnly = false)
        : LockName(lockName)
        , ReadOnlyFlag(isReadOnly)
    {

    }

    virtual ~ILock() = default;

    template <IsLocable T>
    std::optional<TString> IsLocked(const T& obj, const THashSet<TString>& excludedLocks = {}, const bool readOnly = false) const {
    if (IsReadOnly() && readOnly) {
            return {};
        }
        return DoIsLocked(obj, excludedLocks);
    }
    bool IsEmpty() const {
        return DoIsEmpty();
    }
};

}