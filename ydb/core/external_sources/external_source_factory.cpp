#include "external_source_factory.h"
#include "object_storage.h"
#include "external_data_source.h"
#include "iceberg_fields.h"
#include "external_source_builder.h"

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


IExternalSource::TPtr BuildIcebergSource(const std::vector<TRegExMatch>& hostnamePatternsRegEx) {
    using namespace NKikimr::NExternalSource::NIceberg;

    return TExternalSourceBuilder(TString{NYql::GenericProviderName})
        // Basic, Token and SA Auth are available only if warehouse type is set to s3
        .Auth(
            {"BASIC", "TOKEN", "SERVICE_ACCOUNT"},
            GetHasSettingCondition(WAREHOUSE_TYPE, VALUE_S3)
        )
        // DataBase is a required field
        .Property(WAREHOUSE_DB, GetRequiredValidator())
        // Tls is an optional field
        .Property(WAREHOUSE_TLS)
        // Warehouse type is a required field and can be equal only to "s3"
        .Property(
            WAREHOUSE_TYPE,
            GetIsInListValidator({VALUE_S3}, true)
        )
        // If a warehouse type is equal to "s3", fields "s3_endpoint", "s3_region" and "s3_uri" are required
        .Properties(
            {
                WAREHOUSE_S3_ENDPOINT,
                WAREHOUSE_S3_REGION,
                WAREHOUSE_S3_URI
            },
            GetRequiredValidator(),
            GetHasSettingCondition(WAREHOUSE_TYPE, VALUE_S3)
        )
        // Catalog type is a required field and can be equal only to "hive" or "hadoop"
        .Property(
            CATALOG_TYPE,
            GetIsInListValidator({VALUE_HIVE, VALUE_HADOOP}, true)
        )
        // If catalog type is equal to "hive" the field "hive_uri" is required
        .Property(
            CATALOG_HIVE_URI,
            GetRequiredValidator(),
            GetHasSettingCondition(CATALOG_TYPE,VALUE_HIVE)
        )
        .HostnamePatterns(hostnamePatternsRegEx)
        .Build();
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
        },
        {
            ToString(NYql::EDatabaseType::Iceberg),
            BuildIcebergSource(hostnamePatternsRegEx)
        },
        {
            ToString(NYql::EDatabaseType::Redis),
            CreateExternalDataSource(TString{NYql::GenericProviderName}, {"BASIC"}, {"database_name", "use_tls"}, hostnamePatternsRegEx)
        }
    },
    availableExternalDataSources); 
}

}
