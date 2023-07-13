#pragma once

#include <ydb/core/scheme/scheme_pathid.h>

#include <util/generic/fwd.h>

#include <tuple>

namespace NKikimr {
namespace NDataShard {

namespace NPrivate {

template <typename T, size_t I = std::tuple_size<T>::value - 1>
size_t HashTuple(const T& tp) {
    if constexpr (I != 0) {
        return std::get<I>(tp) + 31 * HashTuple<T, I - 1>(tp);
    } else {
        return std::get<I>(tp);
    }
}

template <typename TDerived>
struct TCommonOps {
    friend inline bool operator<(const TDerived& a, const TDerived& b) {
        return a.ToTuple() < b.ToTuple();
    }

    friend inline bool operator==(const TDerived& a, const TDerived& b) {
        return a.ToTuple() == b.ToTuple();
    }

    explicit operator size_t() const noexcept {
        return HashTuple(static_cast<const TDerived*>(this)->ToTuple());
    }
};

} // NPrivate

struct TSnapshotTableKey : public NPrivate::TCommonOps<TSnapshotTableKey> {
    ui64 OwnerId = 0;
    ui64 PathId = 0;

    TSnapshotTableKey() = default;

    TSnapshotTableKey(ui64 ownerId, ui64 pathId)
        : OwnerId(ownerId)
        , PathId(pathId)
    { }

    auto ToTuple() const {
        return std::make_tuple(OwnerId, PathId);
    }

    using TCommonOps<TSnapshotTableKey>::operator size_t;
};

struct TDataSnapshotKey
    : public TSnapshotTableKey
    , public NPrivate::TCommonOps<TDataSnapshotKey>
{
    ui64 Step = 0;
    ui64 TxId = 0;

    TDataSnapshotKey() = default;

    TDataSnapshotKey(ui64 ownerId, ui64 pathId, ui64 step, ui64 txId)
        : TSnapshotTableKey(ownerId, pathId)
        , Step(step)
        , TxId(txId)
    { }

    TDataSnapshotKey(const TPathId& pathId, ui64 step, ui64 txId)
        : TDataSnapshotKey(pathId.OwnerId, pathId.LocalPathId, step, txId)
    { }

    auto ToTuple() const {
        return std::tuple_cat(TSnapshotTableKey::ToTuple(), std::make_tuple(Step, TxId));
    }

    using TCommonOps<TDataSnapshotKey>::operator size_t;
};

using TSnapshotKey = TDataSnapshotKey;

struct TSchemaSnapshotKey
    : public TSnapshotTableKey
    , public NPrivate::TCommonOps<TSchemaSnapshotKey>
{
    ui64 Version = 0;

    TSchemaSnapshotKey() = default;

    TSchemaSnapshotKey(ui64 ownerId, ui64 pathId, ui64 version)
        : TSnapshotTableKey(ownerId, pathId)
        , Version(version)
    { }

    TSchemaSnapshotKey(const TPathId& pathId, ui64 version)
        : TSchemaSnapshotKey(pathId.OwnerId, pathId.LocalPathId, version)
    { }

    auto ToTuple() const {
        return std::tuple_cat(TSnapshotTableKey::ToTuple(), std::make_tuple(Version));
    }

    using TCommonOps<TSchemaSnapshotKey>::operator size_t;
};

} // NDataShard
} // NKikimr
