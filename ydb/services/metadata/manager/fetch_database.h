#pragma once

#include "common.h"

#include <ydb/library/accessor/accessor.h>


namespace NKikimr::NMetadata::NModifications {

class TEvFetchDatabaseResponse : public TEventLocal<TEvFetchDatabaseResponse, EvFetchDatabaseResponse> {
private:
    YDB_READONLY_DEF(bool, Serverless);
    YDB_READONLY_DEF(TString, ErrorString);

public:
    explicit TEvFetchDatabaseResponse(bool serverless, const TString& errorString)
        : Serverless(serverless)
        , ErrorString(errorString)
    {}
};

IActor* CreateDatabaseFetcherActor(const TString& database);

}  // NKikimr::NMetadata::NModifications
