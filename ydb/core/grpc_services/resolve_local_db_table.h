#pragma once

#include <ydb/core/tx/scheme_cache/scheme_cache.h>
#include <ydb/core/tablet_flat/flat_dbase_scheme.h>
#include <ydb/core/base/appdata.h>

#include <util/generic/string.h>
#include <util/generic/vector.h>

namespace NKikimr {
namespace NGRpcService {

    TMaybe<ui64> TryParseLocalDbPath(const TVector<TString>& path);

    void FillLocalDbTableSchema(
            NSchemeCache::TSchemeCacheNavigate& result,
            const NTabletFlatScheme::TSchemeChanges& fullScheme,
            const TString& tableName);

    bool IsSuperUser(const NACLib::TUserToken& userToken, const TAppData& appData);

} // namespace NKikimr
} // namespace NGRpcService
