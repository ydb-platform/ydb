#pragma once

#include <ydb/core/yq/libs/control_plane_storage/events/events.h>
#include <ydb/core/yq/libs/events/events.h>

namespace NYq {

using namespace NActors;
using namespace NYql;

    TString BuildStructuredToken(
        const YandexQuery::IamAuth& auth,
        const TString& authToken,
        const THashMap<TString, TString>& accountIdSignatures);

    void TryAddDatabaseToResolve(
        const YandexQuery::IamAuth& auth,
        const TString& databaseId,
        DatabaseType type,
        const TString& authToken,
        const THashMap<TString, TString>& accountIdSignatures,
        THashMap<std::pair<TString, DatabaseType>, TEvents::TDatabaseAuth>& databaseIds);

} // NYq 
