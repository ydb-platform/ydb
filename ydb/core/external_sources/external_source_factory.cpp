#include "external_source_factory.h"
#include "object_storage.h"
#include "external_data_source.h"

#include <util/generic/map.h>
#include <util/string/cast.h>

#include <yql/essentials/providers/common/provider/yql_provider_names.h>
#include <ydb/library/yql/providers/common/db_id_async_resolver/db_async_resolver.h>


namespace NKikimr::NExternalSource {

namespace {

struct TExternalSourceFactory : public IExternalSourceFactory {
    TExternalSourceFactory(
        const TMap<TString, IExternalSource::TPtr>& sources,
        const std::set<TString>& availableExternalDataSources)
        : Sources(sources)
        , AvailableExternalDataSources(availableExternalDataSources)
    {}

    IExternalSource::TPtr GetOrCreate(const TString& type) const override {
        auto it = Sources.find(type);
        if (it == Sources.end()) {
            throw TExternalSourceException() << "External source with type " << type << " was not found";
        }
        if (!AvailableExternalDataSources.contains(type)) {
            throw TExternalSourceException() << "External source with type " << type << " is disabled. Please contact your system administrator to enable it";
        }
        return it->second;
    }

private:
    const TMap<TString, IExternalSource::TPtr> Sources;
    const std::set<TString> AvailableExternalDataSources;
};

}

IExternalSourceFactory::TPtr CreateExternalSourceFactory(const std::vector<TString>& hostnamePatterns,
                                                         NActors::TActorSystem* actorSystem,
                                                         size_t pathsLimit,
                                                         std::shared_ptr<NYql::ISecuredServiceAccountCredentialsFactory> credentialsFactory,
                                                         bool enableInfer,
                                                         bool allowLocalFiles,
                                                         const std::set<TString>& availableExternalDataSources) {
    std::vector<TRegExMatch> hostnamePatternsRegEx(hostnamePatterns.begin(), hostnamePatterns.end());
    return MakeIntrusive<TExternalSourceFactory>(TMap<TString, IExternalSource::TPtr>{
        {
            ToString(NYql::EDatabaseType::ObjectStorage),
            CreateObjectStorageExternalSource(hostnamePatternsRegEx, actorSystem, pathsLimit, std::move(credentialsFactory), enableInfer, allowLocalFiles)
        },
        {
            ToString(NYql::EDatabaseType::ClickHouse),
            CreateExternalDataSource(TString{NYql::GenericProviderName}, {"MDB_BASIC", "BASIC"}, {"database_name", "protocol", "mdb_cluster_id", "use_tls"}, hostnamePatternsRegEx)
        },
        {
            ToString(NYql::EDatabaseType::PostgreSQL),
            CreateExternalDataSource(TString{NYql::GenericProviderName}, {"MDB_BASIC", "BASIC"}, {"database_name", "protocol", "mdb_cluster_id", "use_tls", "schema"}, hostnamePatternsRegEx)
        },
        {
            ToString(NYql::EDatabaseType::MySQL),
            CreateExternalDataSource(TString{NYql::GenericProviderName}, {"MDB_BASIC", "BASIC"}, {"database_name", "mdb_cluster_id", "use_tls"}, hostnamePatternsRegEx)
        },
        {
            ToString(NYql::EDatabaseType::Ydb),
            CreateExternalDataSource(TString{NYql::GenericProviderName}, {"BASIC", "SERVICE_ACCOUNT"}, {"database_name", "use_tls", "database_id"}, hostnamePatternsRegEx)
        },
        {
            ToString(NYql::EDatabaseType::YT),
            CreateExternalDataSource(TString{NYql::YtProviderName}, {"NONE", "TOKEN"}, {}, hostnamePatternsRegEx)
        },
        {
            ToString(NYql::EDatabaseType::Greenplum),
            CreateExternalDataSource(TString{NYql::GenericProviderName}, {"MDB_BASIC", "BASIC"}, {"database_name", "mdb_cluster_id", "use_tls", "schema"}, hostnamePatternsRegEx)
        },
        {
            ToString(NYql::EDatabaseType::MsSQLServer),
            CreateExternalDataSource(TString{NYql::GenericProviderName}, {"BASIC"}, {"database_name", "use_tls"}, hostnamePatternsRegEx)
        },
        {
            ToString(NYql::EDatabaseType::Oracle),
            CreateExternalDataSource(TString{NYql::GenericProviderName}, {"BASIC"}, {"database_name", "use_tls", "service_name"}, hostnamePatternsRegEx)
        },
        {
            ToString(NYql::EDatabaseType::Logging),
            CreateExternalDataSource(TString{NYql::GenericProviderName}, {"SERVICE_ACCOUNT"}, {"folder_id"}, hostnamePatternsRegEx)
        },
        {
            ToString(NYql::EDatabaseType::Solomon),
            CreateExternalDataSource(TString{NYql::SolomonProviderName}, {"NONE", "TOKEN"}, {"use_ssl", "grpc_port"}, hostnamePatternsRegEx)
        }
    },
    availableExternalDataSources); 
}

}
