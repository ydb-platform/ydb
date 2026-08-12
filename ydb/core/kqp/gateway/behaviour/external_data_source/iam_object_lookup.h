#pragma once

#include <ydb/core/tx/scheme_cache/scheme_cache.h>

namespace NKikimr::NKqp::NExternalDataSource {

enum class EIamObjectLookupResult {
    Found,
    NotFound,
    Error,
};

inline EIamObjectLookupResult ClassifyIamObjectLookup(
    NSchemeCache::TSchemeCacheNavigate::EStatus status,
    bool hasExternalDataSourceInfo)
{
    using EStatus = NSchemeCache::TSchemeCacheNavigate::EStatus;
    if (status == EStatus::RootUnknown || status == EStatus::PathErrorUnknown) {
        return EIamObjectLookupResult::NotFound;
    }
    if (status == EStatus::Ok && hasExternalDataSourceInfo) {
        return EIamObjectLookupResult::Found;
    }
    return EIamObjectLookupResult::Error;
}

} // namespace NKikimr::NKqp::NExternalDataSource
