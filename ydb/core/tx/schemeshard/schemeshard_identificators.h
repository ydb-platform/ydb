#pragma once

#include <ydb/core/scheme/scheme_pathid.h>
#include <ydb/core/util/ui64id.h>
#include <ydb/library/conclusion/result.h>

#include <util/generic/utility.h>
#include <util/stream/output.h>

namespace NKikimr {
namespace NSchemeShard {

constexpr TPathId InvalidPathId = TPathId();

STRONG_UI64_TYPE_DEF_DV(TTabletId, Max<ui64>(), Max<ui64>());
constexpr TTabletId InvalidTabletId = TTabletId();

STRONG_UI64_TYPE_DEF_DV(TLocalShardIdx, Max<ui64>(), Max<ui64>());
constexpr TLocalShardIdx InvalidLocalShardIdx = TLocalShardIdx();

class TShardIdx: public std::pair<TOwnerId, TLocalShardIdx> {
    using TBase = std::pair<TOwnerId, TLocalShardIdx>;
public:
    using TBase::TBase;

    TOwnerId GetOwnerId() const {
        return first;
    }

    TLocalShardIdx GetLocalId() const {
        return second;
    }

    ui64 Hash() const noexcept {
        return Hash128to32(first, ui64(second));
    }

    explicit operator bool() const {
        return GetOwnerId() != InvalidOwnerId && GetLocalId() != InvalidLocalShardIdx;
    }

    NKikimrSchemeOp::TShardIdx SerializeToProto() const;

    static TConclusion<TShardIdx> BuildFromProto(const NKikimrSchemeOp::TShardIdx& proto);
};
constexpr TShardIdx InvalidShardIdx = TShardIdx(InvalidOwnerId, InvalidLocalShardIdx);

STRONG_UI64_TYPE_DEF_DV(TStepId, 0, 0);
constexpr TStepId InvalidStepId = TStepId();

STRONG_UI64_TYPE_DEF_DV(TTxId, 0, 0);
constexpr TTxId InvalidTxId = TTxId();

using TSubTxId = ui32;
constexpr TSubTxId InvalidSubTxId = Max<ui32>();
constexpr TSubTxId FirstSubTxId = TSubTxId(0);

class TOperationId: public std::pair<TTxId, TSubTxId> {
    using TBase = std::pair<TTxId, TSubTxId>;
public:
    using TBase::TBase;

    TTxId GetTxId() const {
        return first;
    }

    TSubTxId GetSubTxId() const {
        return second;
    }

    ui64 Hash() const noexcept {
        return Hash128to32(ui64(first), ui64(second));
    }

    explicit operator bool() const {
        return GetTxId() != InvalidTxId && GetSubTxId() != InvalidSubTxId;
    }

    TString SerializeToString() const {
        return "SSO:" + ::ToString(GetTxId().GetValue()) + ":" + ::ToString(GetSubTxId());
    }

    bool DeserializeFromString(const TString& data) {
        TStringBuf sb(data.data(), data.size());
        if (!sb.StartsWith("SSO:")) {
            return false;
        }
        sb.Skip(4);
        TStringBuf l;
        TStringBuf r;
        if (!sb.TrySplit(':', l, r)) {
            return false;
        }
        ui64 txId;
        TSubTxId subTxId;
        if (!TryFromString(l, txId)) {
            return false;
        }
        if (!TryFromString(r, subTxId)) {
            return false;
        }
        first = TTxId(txId);
        second = subTxId;
        return true;
    }
};
constexpr TOperationId InvalidOperationId = TOperationId(InvalidTxId, InvalidSubTxId);

NKikimrSchemeOp::TShardIdx AsProto(const TShardIdx& shardIdx);
TShardIdx FromProto(const NKikimrSchemeOp::TShardIdx& shardIdx);

STRONG_UI64_TYPE_DEF_DV(TIndexBuildId, Max<ui64>(), Max<ui64>());
constexpr TIndexBuildId InvalidIndexBuildId = TIndexBuildId();

enum class EIndexColumnKind : ui8 {
    KeyColumn = 0,
    DataColumn = 1
};

class TPipeMessageId: public std::pair<ui64, ui64> {
    using TBase = std::pair<ui64, ui64>;
public:
    using TBase::TBase;
};

}
}

template<>
struct THash<NKikimr::NSchemeShard::TOperationId> {
    inline ui64 operator()(const NKikimr::NSchemeShard::TOperationId &x) const noexcept {
        return x.Hash();
    }
};

template<>
inline void Out<NKikimr::NSchemeShard::TOperationId>(IOutputStream &o, const NKikimr::NSchemeShard::TOperationId &x) {
    o << x.GetTxId() << ":" << x.GetSubTxId();
}

template<>
inline void Out<NKikimr::NSchemeShard::TPipeMessageId>(IOutputStream &o, const NKikimr::NSchemeShard::TPipeMessageId &x) {
    o << x.first << ":" << x.second;
}

template<>
struct THash<NKikimr::NSchemeShard::TShardIdx> {
    inline ui64 operator()(const NKikimr::NSchemeShard::TShardIdx &x) const noexcept {
        return x.Hash();
    }
};

template<>
inline void Out<NKikimr::NSchemeShard::TShardIdx>(IOutputStream &o, const NKikimr::NSchemeShard::TShardIdx &x) {
    o << x.GetOwnerId() << ":" << x.GetLocalId();
}
