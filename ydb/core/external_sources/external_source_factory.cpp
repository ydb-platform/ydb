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
        bool allExternalDataSourcesAreAvailable,
        const std::set<TString>& availableExternalDataSources)
        : Sources(sources)
        , AllExternalDataSourcesAreAvailable(allExternalDataSourcesAreAvailable)
        , AvailableExternalDataSources(availableExternalDataSources)
    {
        for (const auto& [type, source] : sources) {
            if (AvailableExternalDataSources.contains(type)) {
                AvailableProviders.insert(source->GetName());
            }
        }
    }

    IExternalSource::TPtr GetOrCreate(const TString& type) const override {
        auto it = Sources.find(type);
        if (it == Sources.end()) {
            throw TExternalSourceException() << "External source with type " << type << " was not found";
        }
        if (!AllExternalDataSourcesAreAvailable && !AvailableExternalDataSources.contains(type)) {
            throw TExternalSourceException() << "External source with type " << type << " is disabled. Please contact your system administrator to enable it";
        }
        return it->second;
    }

    bool IsAvailableProvider(const TString& provider) const override {
        if (AllExternalDataSourcesAreAvailable) {
            return true;
        }
        return AvailableProviders.contains(provider);
    }

private:
    const TMap<TString, IExternalSource::TPtr> Sources;
    bool AllExternalDataSourcesAreAvailable;
    const std::set<TString> AvailableExternalDataSources;
    std::set<TString> AvailableProviders;
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
        // Catalog type is a required field and can be equal only to "hive_metastore" or "hadoop"
        .Property(
            CATALOG_TYPE,
            GetIsInListValidator({VALUE_HIVE_METASTORE, VALUE_HADOOP}, true)
        )
        // If catalog type is equal to "hive_metastore" the field "catalog_hive_metastore_uri" is required
        .Property(
            CATALOG_HIVE_METASTORE_URI,
            GetRequiredValidator(),
            GetHasSettingCondition(CATALOG_TYPE, VALUE_HIVE_METASTORE)
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
                                                         bool allExternalDataSourcesAreAvailable,
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
            CreateExternalDataSource(TString{NYql::GenericProviderName}, {"NONE", "BASIC", "SERVICE_ACCOUNT", "TOKEN"}, {"database_name", "use_tls", "database_id"}, hostnamePatternsRegEx)
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
            CreateExternalDataSource(TString{NYql::SolomonProviderName}, {"NONE", "TOKEN", "SERVICE_ACCOUNT"}, {"use_tls", "grpc_location", "project", "cluster"}, hostnamePatternsRegEx)
        },
        {
            ToString(NYql::EDatabaseType::Iceberg),
            BuildIcebergSource(hostnamePatternsRegEx)
        },
        {
            ToString(NYql::EDatabaseType::Redis),
            CreateExternalDataSource(TString{NYql::GenericProviderName}, {"BASIC"}, {"database_name", "use_tls"}, hostnamePatternsRegEx)
        },
        {
            ToString(NYql::EDatabaseType::Prometheus),
            CreateExternalDataSource(TString{NYql::GenericProviderName}, {"BASIC"}, {"protocol", "use_tls"}, hostnamePatternsRegEx)
        },
        {
            ToString(NYql::EDatabaseType::MongoDB),
            CreateExternalDataSource(TString{NYql::GenericProviderName}, {"BASIC"}, {"database_name", "use_tls", "reading_mode", "unexpected_type_display_mode", "unsupported_type_display_mode"}, hostnamePatternsRegEx)
        },
        {
            ToString(NYql::EDatabaseType::OpenSearch),
            CreateExternalDataSource(TString{NYql::GenericProviderName}, {"BASIC"}, {"database_name", "use_tls"}, hostnamePatternsRegEx)
        },
        {
            ToString(YdbTopicsType),
            CreateExternalDataSource(TString{NYql::PqProviderName}, {"NONE", "BASIC", "TOKEN"}, {"database_name", "use_tls"}, hostnamePatternsRegEx)
        }
    },
    allExternalDataSourcesAreAvailable,
    availableExternalDataSources); 
}

}
