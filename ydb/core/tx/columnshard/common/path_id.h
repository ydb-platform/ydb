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

} //namespace NKikimr::NColumnShard

namespace NKikimr::NOlap {

using TInternalPathId = NColumnShard::TInternalPathId;

} //namespace NKikimr::NOlap

template <>
struct THash<NKikimr::NColumnShard::TInternalPathId> {
    size_t operator()(const NKikimr::NColumnShard::TInternalPathId& p) const {
        return THash<ui64>()(p.GetRawValue());
    }
};
