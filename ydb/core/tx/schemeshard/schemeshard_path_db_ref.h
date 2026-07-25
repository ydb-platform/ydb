#pragma once

#include <ydb/core/scheme/scheme_pathid.h>

#include <type_traits>

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
// move transfers, copy is deleted. DetachWithoutRelease() drops it without
// releasing (a Paths snapshot owns the counter, or at shutdown).
class TPathDbRef {
public:
    TPathDbRef() = default;

    TPathDbRef(TSchemeShard* ss, const TPathId& pathId, TRefLabel reason)
        : SS(ss)
        , PathId(pathId)
        , Reason(reason)
    {
        Acquire();
    }

    TPathDbRef(const TPathDbRef&) = delete;

    TPathDbRef(TPathDbRef&& other) noexcept
        : SS(other.SS)
        , PathId(other.PathId)
        , Reason(other.Reason)
    {
        other.SS = nullptr;
    }

    TPathDbRef& operator=(const TPathDbRef&) = delete;

    TPathDbRef& operator=(TPathDbRef&& other) noexcept {
        if (this != &other) {
            Release();
            SS = other.SS;
            PathId = other.PathId;
            Reason = other.Reason;
            other.SS = nullptr;
        }
        return *this;
    }

    ~TPathDbRef() {
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

// The move checks bite only alongside the copy ones: T&& binds to const T&.
static_assert(!std::is_copy_constructible_v<TPathDbRef>);
static_assert(!std::is_copy_assignable_v<TPathDbRef>);
static_assert(std::is_move_constructible_v<TPathDbRef>);
static_assert(std::is_move_assignable_v<TPathDbRef>);

// Acquire/release a path's DbRefCount without an owning handle, for containers
// (TDbRefMap) whose membership is the reference. `reason` must be a string literal.
void AcquirePathDbRef(TSchemeShard* ss, const TPathId& pathId, TRefLabel reason);
void ReleasePathDbRef(TSchemeShard* ss, const TPathId& pathId, TRefLabel reason);

// True inside an armed propose. Free function so headers holding only a forward
// declaration of TSchemeShard can assert on it.
bool IsProposeArmed(const TSchemeShard* ss);

} // NKikimr::NSchemeShard
