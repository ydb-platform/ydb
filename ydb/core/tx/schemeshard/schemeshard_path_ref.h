#pragma once

#include <ydb/core/scheme/scheme_pathid.h>

namespace NKikimr::NSchemeShard {

class TSchemeShard;

// Owning handle for a DbRefCount reference on a path element.
//
// Taking a reference is construction, releasing it is destruction: the
// increment/decrement pairing that used to be a convention spread across
// CreateTx, RemoveTx and TTxInit restore becomes structural. Holding the
// handle IS holding the reference, so a restored object acquires exactly the
// same references as a freshly created one, and a destroyed object cannot
// leak or double-release them (issue #33764).
//
// Copying re-acquires (a copy holds its own reference), moving transfers
// ownership. Disarm() drops the handle without releasing: it is for rollback
// paths where TMemoryChanges path snapshots own the counter restoration, and
// for shutdown.
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

    // Drops the handle without releasing the reference.
    void Disarm() {
        SS = nullptr;
    }

    explicit operator bool() const {
        return SS != nullptr;
    }

    // Meaningful only while the handle is armed (operator bool()); a default or
    // disarmed handle carries no reference and its PathId is not significant.
    TPathId GetPathId() const {
        return PathId;
    }

private:
    void Acquire();
    void Release();

    TSchemeShard* SS = nullptr;
    TPathId PathId;
    // Stored by pointer, not copied: pass a string literal (or other pointer with
    // static storage). It is only a label for DbRefCount change logging.
    const char* Reason = "";
};

// Acquire/release a path's DbRefCount without an owning handle. For containers
// (TSelfRefMap) that already track membership in their primary map: the entry's
// existence is the reference, so a second per-entry handle would only mirror it.
// Same counter and null-SS guard as TPathRef. `reason` must be a string literal.
void AcquirePathDbRef(TSchemeShard* ss, const TPathId& pathId, const char* reason);
void ReleasePathDbRef(TSchemeShard* ss, const TPathId& pathId, const char* reason);

} // NKikimr::NSchemeShard
