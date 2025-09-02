#pragma once

#include "common.h"

#include <ydb/library/accessor/accessor.h>


namespace NKikimr::NMetadata::NModifications {

class TEvFetchDatabaseResponse : public TEventLocal<TEvFetchDatabaseResponse, EvFetchDatabaseResponse> {
private:
    YDB_READONLY_DEF(bool, Serverless);
    YDB_READONLY_DEF(std::optional<TString>, ErrorString);

public:
    TEvFetchDatabaseResponse(bool serverless, const std::optional<TString>& errorString)
        : Serverless(serverless)
        , ErrorString(errorString)
    {}
};

IActor* CreateDatabaseFetcherActor(const TString& database);

}  // NKikimr::NMetadata::NModifications
