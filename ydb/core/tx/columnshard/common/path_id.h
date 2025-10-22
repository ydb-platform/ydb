#pragma once
#include <util/generic/hash.h>
#include <util/stream/output.h>
#include <util/system/types.h>

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

    bool IsValid() const {
        return PathId != 0;
    }

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
    template <typename Proto>
    static TInternalPathId FromProto(const Proto& proto);

    //Template function whithout generic implementation
    //Must be explicitly instantiated for messages that hold internal path id
    template <typename Proto>
    void ToProto(Proto& proto) const;

    TString DebugString() const;

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

    bool IsValid() const {
        return PathId != 0;
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
    template <typename Proto>
    static TSchemeShardLocalPathId FromProto(const Proto& proto);

    //Templated function whithout generic implementation
    //Must be explicitly instantiated for messages that hold SchemeShardLocalPathId
    template <typename Proto>
    void ToProto(Proto& proto) const;

    auto operator<=>(const TSchemeShardLocalPathId&) const = default;

    TString DebugString() const;
};

static_assert(sizeof(TSchemeShardLocalPathId) == sizeof(ui64));

class TUnifiedOptionalPathId {
private:
    std::optional<TInternalPathId> InternalPathId;
    std::optional<TSchemeShardLocalPathId> SchemeShardLocalPathId;

public:
    TUnifiedOptionalPathId(const std::optional<TInternalPathId> internalPathId, const std::optional<TSchemeShardLocalPathId> externalPathId)
        : InternalPathId(internalPathId)
        , SchemeShardLocalPathId(externalPathId) {
    }

    bool HasInternalPathId() const {
        return !!InternalPathId;
    }
    bool HasSchemeShardLocalPathId() const {
        return !!SchemeShardLocalPathId;
    }

    std::optional<TInternalPathId> GetInternalPathIdOptional() const {
        return InternalPathId;
    }
    std::optional<TSchemeShardLocalPathId> GetSchemeShardLocalPathIdOptional() const {
        return SchemeShardLocalPathId;
    }

    TInternalPathId GetInternalPathIdVerified() const;
    TSchemeShardLocalPathId GetSchemeShardLocalPathIdVerified() const;

    static TUnifiedOptionalPathId BuildFull(const TInternalPathId internalPathId, const TSchemeShardLocalPathId externalPathId) {
        return TUnifiedOptionalPathId(internalPathId, externalPathId);
    }

    static TUnifiedOptionalPathId BuildInternal(
        const TInternalPathId internalPathId, const std::optional<TSchemeShardLocalPathId> externalPathId = std::nullopt) {
        return TUnifiedOptionalPathId(internalPathId, externalPathId);
    }

    static TUnifiedOptionalPathId BuildExternal(
        const TSchemeShardLocalPathId externalPathId, const std::optional<TInternalPathId> internalPathId = std::nullopt) {
        return TUnifiedOptionalPathId(internalPathId, externalPathId);
    }
};

class TUnifiedPathId {
private:
    TUnifiedPathId(const TInternalPathId internalPathId, const TSchemeShardLocalPathId externalPathId)
        : InternalPathId(internalPathId)
        , SchemeShardLocalPathId(externalPathId) {
    }

public:
    TUnifiedPathId() = default;
    TUnifiedPathId(const TUnifiedOptionalPathId& optionalId)
        : TUnifiedPathId(optionalId.GetInternalPathIdVerified(), optionalId.GetSchemeShardLocalPathIdVerified())
    {
    
    }

    TInternalPathId InternalPathId;
    TSchemeShardLocalPathId SchemeShardLocalPathId;

    const TInternalPathId& GetInternalPathId() const {
        return InternalPathId;
    }
    const TSchemeShardLocalPathId& GetSchemeShardLocalPathId() const {
        return SchemeShardLocalPathId;
    }

    bool IsValid() const {
        return InternalPathId.IsValid() && SchemeShardLocalPathId.IsValid();
    }

    auto operator<=>(const TUnifiedPathId&) const = default;


    static TUnifiedPathId BuildValid(const TInternalPathId internalPathId, const TSchemeShardLocalPathId externalPathId);
};

static_assert(sizeof(TUnifiedPathId) == 2 * sizeof(ui64));

}   //namespace NKikimr::NColumnShard

namespace NKikimr::NOlap {

using TInternalPathId = NColumnShard::TInternalPathId;

class IPathIdTranslator {
public:
    virtual ~IPathIdTranslator() = default;
    virtual std::optional<NColumnShard::TSchemeShardLocalPathId> ResolveSchemeShardLocalPathIdOptional(
        const TInternalPathId internalPathId) const = 0;
    virtual std::optional<TInternalPathId> ResolveInternalPathIdOptional(
        const NColumnShard::TSchemeShardLocalPathId schemeShardLocalPathId, const bool withTabletPathId) const = 0;
    std::optional<NColumnShard::TSchemeShardLocalPathId> ResolveSchemeShardLocalPathId(const TInternalPathId internalPathId) const {
        return ResolveSchemeShardLocalPathIdOptional(internalPathId);
    }
    std::optional<TInternalPathId> ResolveInternalPathId(
        const NColumnShard::TSchemeShardLocalPathId schemeShardLocalPathId, const bool withTabletPathId) const {
        return ResolveInternalPathIdOptional(schemeShardLocalPathId, withTabletPathId);
    }
    NColumnShard::TSchemeShardLocalPathId ResolveSchemeShardLocalPathIdVerified(const TInternalPathId internalPathId) const;
    TInternalPathId ResolveInternalPathIdVerified(
        const NColumnShard::TSchemeShardLocalPathId schemeShardLocalPathId, const bool withTabletPathId) const;
    NColumnShard::TUnifiedPathId GetUnifiedByInternalVerified(const TInternalPathId internalPathId) const;
};

}   //namespace NKikimr::NOlap

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
