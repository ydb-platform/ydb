#pragma once

#include <ydb/core/protos/subdomains.pb.h>

#include <util/generic/maybe.h>
#include <util/system/types.h>
#include <utility>

namespace NKikimr {
struct TSubDomainKey : public std::pair<ui64, ui64> {
    using TBase = std::pair<ui64, ui64>;

    using TBase::TBase;
    TSubDomainKey() = default;
    TSubDomainKey(const TSubDomainKey&) = default;
    TSubDomainKey(TSubDomainKey&&) = default;
    TSubDomainKey(ui64 shardId, ui64 pathId);
    explicit TSubDomainKey(const NKikimrSubDomains::TDomainKey &domainKey);

    TSubDomainKey& operator =(const TSubDomainKey&) = default;
    TSubDomainKey& operator =(TSubDomainKey&&) = default;

    ui64 GetSchemeShard() const;
    ui64 GetPathId() const;

    operator NKikimrSubDomains::TDomainKey() const;
    operator bool() const;

    ui64 Hash() const noexcept;
};

static const TSubDomainKey InvalidSubDomainKey = TSubDomainKey();

using TMaybeServerlessComputeResourcesMode = TMaybeFail<NKikimrSubDomains::EServerlessComputeResourcesMode>;
}

template <>
struct THash<NKikimr::TSubDomainKey> {
    inline ui64 operator()(const NKikimr::TSubDomainKey &x) const {
        return x.Hash();
    }
};

template<>
inline void Out<NKikimr::TSubDomainKey>(IOutputStream &o, const NKikimr::TSubDomainKey &x) {
    o << x.GetSchemeShard() << ":" << x.GetPathId();
}
