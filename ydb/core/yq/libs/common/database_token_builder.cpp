#include "database_token_builder.h"
#include <ydb/library/yql/providers/common/structured_token/yql_token_builder.h>

namespace NYq {

using namespace NYql;

    TString BuildStructuredToken(const YandexQuery::IamAuth& auth, const TString& authToken, const THashMap<TString, TString>& accountIdSignatures) {
        TStructuredTokenBuilder result;
        switch (auth.identity_case()) {
        case YandexQuery::IamAuth::kCurrentIam:
            result.SetIAMToken(authToken);
            break;
        case YandexQuery::IamAuth::kServiceAccount: {
            const auto& signature = accountIdSignatures.at(auth.service_account().id());
            result.SetServiceAccountIdAuth(auth.service_account().id(), signature);
            break;
        }
        default:
            result.SetNoAuth();
            break;
        }

        return result.ToJson();
    }

    void TryAddDatabaseToResolve(
        const YandexQuery::IamAuth& auth,
        const TString& databaseId,
        DatabaseType type,
        const TString& authToken,
        const THashMap<TString, TString>& accountIdSignatures,
        THashMap<std::pair<TString, DatabaseType>, TEvents::TDatabaseAuth>& databaseIds) {
        if (!databaseId) {
            return;
        }

        TEvents::TDatabaseAuth info;
        info.StructuredToken = BuildStructuredToken(auth, authToken, accountIdSignatures);
        info.AddBearerToToken = true; // XXX
        databaseIds[std::make_pair(databaseId, type)] = info;
    }

} // NYq

