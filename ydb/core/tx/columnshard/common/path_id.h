#pragma once
#include <util/system/types.h>
#include <util/generic/hash.h>
#include <util/stream/output.h>
#include <ydb/core/tx/columnshard/common/protos/path_id.pb.h>

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

    static TInternalPathId FromProto(const NKikimrColumnShardPathIdProto::TInternalPathId& pathId) {
        return TInternalPathId(pathId.GetPathId());
    }

    template<typename TProto>
    static TInternalPathId FromProto(const TProto& proto) {
        if (proto.HasPathId()) {
            return FromProto(proto.GetPathId());
        } else {
            return TInternalPathId(proto.GetPathId_Deprecated());
        }
    }

    explicit operator bool() const {
        return PathId != 0;
    }

    void ToProto(NKikimrColumnShardPathIdProto::TInternalPathId& proto) const {
        proto.SetPathId(PathId);
    }

    template<typename Proto>
    void ToProto(Proto& proto) const {
        ToProto(*proto.MutablePathId());
    }


    ui64 GetRawValue() const {
        return PathId;
    }

    auto operator<=>(const TInternalPathId&) const = default;
};

static_assert(sizeof(TInternalPathId)==sizeof(ui64));

class TSchemeShardLocalPathId {
    ui64 PathId;
private:
    explicit TSchemeShardLocalPathId(const ui64 pathId)
        : PathId(pathId) {
    }
public:
    TSchemeShardLocalPathId()
        : PathId(0) {
    }
    explicit operator bool() const {
        return PathId != 0;
    }

    static TSchemeShardLocalPathId FromRawValue(const ui64 pathId) {
        return TSchemeShardLocalPathId(pathId);
    }

    static TSchemeShardLocalPathId FromProto(const NKikimrColumnShardPathIdProto::TSchemeShardLocalPathId& protoPathId) {
        return TSchemeShardLocalPathId(protoPathId.GetPathId());
    }

    template<typename TProto>
    static TSchemeShardLocalPathId FromProto(const TProto& proto) {
        if (proto.HasPathId()) {
            return FromProto(proto.GetPathId());
        } else {
            return TSchemeShardLocalPathId(proto.GetPathId_Deprecated());
        }
    }

    template<typename TProto>
    static TSchemeShardLocalPathId FromProtoOrTableId(const TProto& proto) {
        if (proto.HasPathId()) {
            return FromProto(proto.GetPathId());
        } else {
            return TSchemeShardLocalPathId(proto.GetTableId_Deprecated());
        }
    }

    ui64 GetRawValue() const {
        return PathId;
    }

    void ToProto(NKikimrColumnShardPathIdProto::TSchemeShardLocalPathId& proto) const {
        proto.SetPathId(PathId);
    }

    template<typename Proto>
    void ToProto(Proto& proto) const {
        ToProto(*proto.MutablePathId());
    }

    auto operator<=>(const TSchemeShardLocalPathId&) const = default;
};

static_assert(sizeof(TSchemeShardLocalPathId)==sizeof(ui64));

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

template <>
struct THash<NKikimr::NColumnShard::TSchemeShardLocalPathId> {
    size_t operator()(const NKikimr::NColumnShard::TSchemeShardLocalPathId& p) const {
        return THash<ui64>()(p.GetRawValue());
    }
};
