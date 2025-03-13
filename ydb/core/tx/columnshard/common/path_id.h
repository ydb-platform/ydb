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

    static TLocalPathId FromLocalPathIdValue(ui64 pathId) {
        return TLocalPathId(pathId);
    }

    ui64 GetLocalPathIdValue() const {
        return PathId;
    }

    auto operator<=>(const TLocalPathId&) const = default;
};

struct TUnifiedPathId {
    TLocalPathId LocalPathId;
    TInternalPathId InternalPathId;
    void Validate() const {
        AFL_VERIFY(!!LocalPathId == !!InternalPathId);
    }
    explicit operator bool() const {
        Validate();
        return !!InternalPathId;
    }
};


} //namespace NKikimr::NColumnShard

namespace std {

template<>
struct hash<NKikimr::NColumnShard::TInternalPathId> {
    size_t operator()(const NKikimr::NColumnShard::TInternalPathId& p) const {
        return p.GetInternalPathIdValue();
    }
};

template<>
struct hash<NKikimr::NColumnShard::TLocalPathId> {
    size_t operator()(const NKikimr::NColumnShard::TLocalPathId& p) const {
        return p.GetLocalPathIdValue();
    }
};

} //namespace std

template <>
struct THash<NKikimr::NColumnShard::TInternalPathId> {
    size_t operator()(const NKikimr::NColumnShard::TInternalPathId& p) const {
        return p.GetInternalPathIdValue();
    }
};

template <>
struct THash<NKikimr::NColumnShard::TLocalPathId> {
    size_t operator()(const NKikimr::NColumnShard::TLocalPathId& p) const {
        return p.GetLocalPathIdValue();
    }
};




void Out(IOutputStream& s, NKikimr::NColumnShard::TInternalPathId v);


void Out(IOutputStream& s, NKikimr::NColumnShard::TLocalPathId v);

void Out(IOutputStream& s, const NKikimr::NColumnShard::TUnifiedPathId& v);


template<>
void Out<NKikimr::NColumnShard::TInternalPathId>(IOutputStream& s, TTypeTraits<NKikimr::NColumnShard::TInternalPathId>::TFuncParam v);


template<>
void Out<NKikimr::NColumnShard::TLocalPathId>(IOutputStream& s, TTypeTraits<NKikimr::NColumnShard::TLocalPathId>::TFuncParam v);

template<>
void Out<NKikimr::NColumnShard::TUnifiedPathId>(IOutputStream& s, TTypeTraits<NKikimr::NColumnShard::TUnifiedPathId>::TFuncParam v);