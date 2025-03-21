#pragma once

#include "external_source.h"
#include <ydb/library/yql/providers/common/token_accessor/client/factory.h>
#include <ydb/library/yql/providers/common/db_id_async_resolver/database_type.h>

namespace NKikimr::NExternalSource {

struct IExternalSourceFactory : public TThrRefBase {
    using TPtr = TIntrusivePtr<IExternalSourceFactory>;

    virtual IExternalSource::TPtr GetOrCreate(const TString& type) const = 0;
};

IExternalSourceFactory::TPtr CreateExternalSourceFactory(const std::vector<TString>& hostnamePatterns,
                                                         NActors::TActorSystem* actorSystem = nullptr,
                                                         size_t pathsLimit = 50000,
                                                         std::shared_ptr<NYql::ISecuredServiceAccountCredentialsFactory> credentialsFactory = nullptr,
                                                         bool enableInfer = false,
                                                         bool allowLocalFiles = false,
                                                         const std::set<TString>& availableExternalDataSources = NYql::GetAllExternalDataSourceTypes());

}
