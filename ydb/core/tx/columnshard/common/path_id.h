#pragma once
#include <ydb/library/actors/core/log.h>
#include <util/system/types.h>
#include <util/generic/hash.h>
#include <util/stream/output.h>
#include <optional>


namespace NKikimr::NColumnShard {


class TInternalPathId {
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
    TInternalPathId& operator=(TInternalPathId&) = default;

    static TInternalPathId FromInternalPathIdValue(ui64 pathId) {
        return TInternalPathId(pathId);
    }

    explicit operator bool() const {
        return PathId != 0;
    }

    ui64 GetInternalPathIdValue() const {
        return PathId;
    }

    auto operator<=>(const TInternalPathId&) const = default;
};
static_assert(sizeof(TInternalPathId)==sizeof(ui64));


} //namespace NKikimr::NColumnShard

namespace std {

template<>
struct hash<NKikimr::NColumnShard::TInternalPathId> {
    size_t operator()(const NKikimr::NColumnShard::TInternalPathId& p) const {
        return p.GetInternalPathIdValue();
    }
};

} //namespace std

template <>
struct THash<NKikimr::NColumnShard::TInternalPathId> {
    size_t operator()(const NKikimr::NColumnShard::TInternalPathId& p) const {
        return p.GetInternalPathIdValue();
    }
};

void Out(IOutputStream& s, NKikimr::NColumnShard::TInternalPathId v);


template<>
void Out<NKikimr::NColumnShard::TInternalPathId>(IOutputStream& s, TTypeTraits<NKikimr::NColumnShard::TInternalPathId>::TFuncParam v);
