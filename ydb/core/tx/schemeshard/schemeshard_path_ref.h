#pragma once

#include <ydb/core/scheme/scheme_pathid.h>

namespace NKikimr::NSchemeShard {

class TSchemeShard;

// A DbRefCount log label. Only binds char arrays (string literals); a raw
// const char* (e.g. TString::c_str()) has no viable ctor and fails to compile,
// so a stored label can never dangle.
class TRefLabel {
public:
    template <size_t N>
    constexpr TRefLabel(const char (&s)[N]) noexcept
        : Str(s)
    {}

    const char* c_str() const noexcept { return Str; }

private:
    const char* Str;
};

// Owning handle for a path's DbRefCount reference: ctor acquires, dtor releases,
// copy re-acquires, move transfers. DetachWithoutRelease() drops it without
// releasing (a Paths snapshot owns the counter, or at shutdown).
class TPathRef {
public:
    TPathRef() = default;

    TPathRef(TSchemeShard* ss, const TPathId& pathId, TRefLabel reason)
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

    void Reset(TSchemeShard* ss, const TPathId& pathId, TRefLabel reason) {
        Release();
        SS = ss;
        PathId = pathId;
        Reason = reason;
        Acquire();
    }

    void DetachWithoutRelease() {
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
    TRefLabel Reason = "";
};

// Acquire/release a path's DbRefCount without an owning handle, for containers
// (TSelfRefMap) whose membership is the reference. `reason` must be a string literal.
void AcquirePathDbRef(TSchemeShard* ss, const TPathId& pathId, TRefLabel reason);
void ReleasePathDbRef(TSchemeShard* ss, const TPathId& pathId, TRefLabel reason);

} // NKikimr::NSchemeShard
