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

    explicit operator bool() const {
        return PathId != 0;
    }

    static TInternalPathId FromRawValue(const ui64 pathId) {
        return TInternalPathId(pathId);
    }
    ui64 GetRawValue() const {
        return PathId;
    }

    //Template function whithout generic implementation
    //Must be explicitly instantiated for messages that hold internal path id
    template<typename Proto>
    static TInternalPathId FromProto(const Proto& proto);

    //Template function whithout generic implementation
    //Must be explicitly instantiated for messages that hold internal path id
    template<typename Proto>
    void ToProto(Proto& proto) const;

    auto operator<=>(const TInternalPathId&) const = default;
};

static_assert(sizeof(TInternalPathId) == sizeof(ui64));

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

    ui64 GetRawValue() const {
        return PathId;
    }

    //Templated function whithout generic implementation
    //Must be explicitly instantiated for messages that hold SchemeShardLocalPathId
    template<typename Proto>
    static TSchemeShardLocalPathId FromProto(const Proto& proto);
    
    //Templated function whithout generic implementation
    //Must be explicitly instantiated for messages that hold SchemeShardLocalPathId
    template<typename Proto>
    void ToProto(Proto& proto) const;

    auto operator<=>(const TSchemeShardLocalPathId&) const = default;
};

static_assert(sizeof(TSchemeShardLocalPathId) == sizeof(ui64));

struct TUnifiedPathId {
    TInternalPathId InternalPathId;
    TSchemeShardLocalPathId SchemeShardLocalPathId;
    explicit operator bool() const {
        return InternalPathId && SchemeShardLocalPathId;
    }
};

static_assert(sizeof(TUnifiedPathId) == 2 * sizeof(ui64));

} //namespace NKikimr::NColumnShard

namespace NKikimr::NOlap {

using TInternalPathId = NColumnShard::TInternalPathId;

struct IPathIdTranslator {
    virtual ~IPathIdTranslator() {}
    virtual std::optional<NColumnShard::TSchemeShardLocalPathId> ResolveSchemeShardLocalPathId(const TInternalPathId internalPathId) const = 0;
    virtual std::optional<TInternalPathId> ResolveInternalPathId(const NColumnShard::TSchemeShardLocalPathId schemeShardLocalPathId) const = 0;
};

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
