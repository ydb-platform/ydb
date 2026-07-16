#pragma once

#include <ydb/core/scheme/scheme_pathid.h>

namespace NKikimr::NSchemeShard {

class TSchemeShard;

// Owning handle for a DbRefCount reference on a path element (issue #33764).
// Construction acquires, destruction releases: the inc/dec pairing that was a
// convention across CreateTx/RemoveTx/init becomes structural, so a restored
// object holds the same refs as a fresh one and can't leak or double-release.
// Copying re-acquires, moving transfers; Disarm() drops the handle without
// releasing (rollback where Paths snapshots own the counter, and shutdown).
class TPathRef {
public:
    TPathRef() = default;

    TPathRef(TSchemeShard* ss, const TPathId& pathId, const char* reason)
        : SS(ss)
        , PathId(pathId)
        , Reason(reason)
    {
        Acquire();
    }

    TPathRef(const TPathRef& other)
        : SS(other.SS)
        , PathId(other.PathId)
        , Reason(other.Reason)
    {
        Acquire();
    }

    TPathRef(TPathRef&& other) noexcept
        : SS(other.SS)
        , PathId(other.PathId)
        , Reason(other.Reason)
    {
        other.SS = nullptr;
    }

    TPathRef& operator=(const TPathRef& other) {
        if (this != &other) {
            Release();
            SS = other.SS;
            PathId = other.PathId;
            Reason = other.Reason;
            Acquire();
        }
        return *this;
    }

    TPathRef& operator=(TPathRef&& other) noexcept {
        if (this != &other) {
            Release();
            SS = other.SS;
            PathId = other.PathId;
            Reason = other.Reason;
            other.SS = nullptr;
        }
        return *this;
    }

    ~TPathRef() {
        Release();
    }

    void Reset() {
        Release();
    }

    void Reset(TSchemeShard* ss, const TPathId& pathId, const char* reason) {
        Release();
        SS = ss;
        PathId = pathId;
        Reason = reason;
        Acquire();
    }

    void Disarm() {
        SS = nullptr;
    }

    explicit operator bool() const {
        return SS != nullptr;
    }

    // Meaningful only while armed.
    TPathId GetPathId() const {
        return PathId;
    }

private:
    void Acquire();
    void Release();

    TSchemeShard* SS = nullptr;
    TPathId PathId;
    // String literal (stored by pointer); a label for DbRefCount logging.
    const char* Reason = "";
};

// Acquire/release a path's DbRefCount without an owning handle, for containers
// (TSelfRefMap) whose membership is the reference. `reason` must be a string literal.
void AcquirePathDbRef(TSchemeShard* ss, const TPathId& pathId, const char* reason);
void ReleasePathDbRef(TSchemeShard* ss, const TPathId& pathId, const char* reason);

} // NKikimr::NSchemeShard
