#pragma once
#include <ydb/library/accessor/accessor.h>
#include <vector>
#include <memory>

namespace NKikimr::NOlap {
class TPortionInfo;
class TGranuleMeta;
}

namespace NKikimr::NOlap::NDataLocks {

class ILock {
private:
    YDB_READONLY_FLAG(ReadOnly, false);
protected:
    virtual bool DoIsLocked(const TPortionInfo& portion) const = 0;
    virtual bool DoIsLocked(const TGranuleMeta& granule) const = 0;
    virtual bool DoIsEmpty() const = 0;
public:
    ILock(const bool isReadOnly = false)
        : ReadOnlyFlag(isReadOnly)
    {

    }

    virtual ~ILock() = default;

    bool IsLocked(const TPortionInfo& portion, const bool readOnly = false) const {
        if (IsReadOnly() && readOnly) {
            return false;
        }
        return DoIsLocked(portion);
    }
    bool IsLocked(const TGranuleMeta& g, const bool readOnly = false) const {
        if (IsReadOnly() && readOnly) {
            return false;
        }
        return DoIsLocked(g);
    }
    bool IsEmpty() const {
        return DoIsEmpty();
    }
};

}