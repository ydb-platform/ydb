#pragma once
#include <util/system/types.h>
#include <util/generic/hash.h>
#include <util/stream/output.h>

namespace NKikimr::NColumnShard {
class TInternalPathId {
private:
    ui64 PathId;
    explicit TInternalPathId(ui64 pathId)
        : PathId(pathId) {
    }

public:
    TInternalPathId()
        : PathId(0) {
    }
    TInternalPathId(const TInternalPathId&) = default;
    TInternalPathId(TInternalPathId&&) = default;
    TInternalPathId& operator=(const TInternalPathId&) = default;
    TInternalPathId& operator=(TInternalPathId&&) = default;

    static TInternalPathId FromRawValue(const ui64 pathId) {
        return TInternalPathId(pathId);
    }

    explicit operator bool() const {
        return PathId != 0;
    }

    ui64 GetRawValue() const {
        return PathId;
    }

    auto operator<=>(const TInternalPathId&) const = default;
};

static_assert(sizeof(TInternalPathId)==sizeof(ui64));

class TLocalPathId {
    ui64 PathId;
private:
    explicit TLocalPathId(const ui64 pathId)
        : PathId(pathId) {
    }
public:
    TLocalPathId()
        : PathId(0) {
    }
    explicit operator bool() const {
        return PathId != 0;
    }

    static TLocalPathId FromRawValue(ui64 pathId) {
        return TLocalPathId(pathId);
    }

    ui64 GetRawValue() const {
        return PathId;
    }

    auto operator<=>(const TLocalPathId&) const = default;
};

class TUnifiedPathId {
private:    
    TInternalPathId InternalPathId;
    TLocalPathId LocalPathId;
public:
    TUnifiedPathId() = default;
    TUnifiedPathId(TInternalPathId internalPathId, TLocalPathId localPathId)
        : InternalPathId(internalPathId)
        , LocalPathId(localPathId) {
        Y_ABORT_UNLESS(!!GetLocalPathId() == !!GetInternalPathId());
    }
    TUnifiedPathId(const TUnifiedPathId&) = default;
    TUnifiedPathId(TUnifiedPathId&&) = default;
    TUnifiedPathId& operator=(const TUnifiedPathId&) = default;
    TUnifiedPathId& operator=(TUnifiedPathId&&) = default;

    TInternalPathId GetInternalPathId() const {
        return InternalPathId;
    }

    TLocalPathId GetLocalPathId() const {
        return LocalPathId;
    }

    explicit operator bool() const {
        return !!GetInternalPathId();
    }

    auto operator<=>(const TUnifiedPathId&) const = default;
};


} //namespace NKikimr::NColumnShard

namespace NKikimr::NOlap {

using TInternalPathId = NColumnShard::TInternalPathId;
using TLocalPathId = NColumnShard::TLocalPathId;
using TUnifiedPathId = NColumnShard::TUnifiedPathId;
    
} //namespace NKikimr::NOlap

template <>
struct THash<NKikimr::NColumnShard::TInternalPathId> {
    size_t operator()(const NKikimr::NColumnShard::TInternalPathId& p) const {
        return p.GetRawValue();
    }
};

template <>
struct THash<NKikimr::NColumnShard::TLocalPathId> {
    size_t operator()(const NKikimr::NColumnShard::TLocalPathId& p) const {
        return p.GetRawValue();
    }
};
