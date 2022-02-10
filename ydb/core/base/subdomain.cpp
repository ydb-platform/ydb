#include "subdomain.h"

#include <ydb/core/base/defs.h>

namespace NKikimr {
TSubDomainKey::TSubDomainKey(ui64 shardId, ui64 pathId)
    : TBase(shardId, pathId)
{}

TSubDomainKey::TSubDomainKey(const NKikimrSubDomains::TDomainKey &domainKey)
    : TBase(domainKey.GetSchemeShard(), domainKey.GetPathId())
{}

ui64 TSubDomainKey::GetSchemeShard() const {
    return first;
}

ui64 TSubDomainKey::GetPathId() const {
    return second;
}

ui64 TSubDomainKey::Hash() const noexcept {
    return Hash128to32(GetSchemeShard(), GetPathId());
}

TSubDomainKey::operator NKikimrSubDomains::TDomainKey() const {
    NKikimrSubDomains::TDomainKey result;
    result.SetSchemeShard(GetSchemeShard());
    result.SetPathId(GetPathId());
    return result;
}

TSubDomainKey::operator bool() const {
    return GetSchemeShard() || GetPathId();
}
}
