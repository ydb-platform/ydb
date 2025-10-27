#pragma once

#include "yql_generic_settings.h"

#include <yql/essentials/core/yql_data_provider.h>
#include <ydb/library/yql/providers/common/token_accessor/client/factory.h>
#include <ydb/library/yql/providers/generic/connector/api/service/protos/connector.pb.h>
#include <ydb/library/yql/providers/generic/connector/libcpp/client.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/credentials/credentials.h>

namespace NKikimr::NMiniKQL {
    class IFunctionRegistry;
} // namespace NKikimr::NMiniKQL

namespace NYql {
    ///
    /// A key for a select query on a cluster table. Hash value is
    /// calculated in a constructor and stored in a Hash field.
    ///
    struct TSelectKey {
        const TString Cluster;
        const TString Table;
        const std::vector<TString> Columns;
        const TString Where;
        const size_t Hash;

        TSelectKey(const TSelectKey& select) = default;

        TSelectKey(const TString& Cluster, const NConnector::NApi::TSelect& select);

        bool operator==(const TSelectKey& other) const = default;

        TSelectKey& operator=(const TSelectKey& other) = default;

        size_t CalculateHash() const;
    };

    ///
    /// Hasher for TSelectKey
    ///
    struct TSelectKeyHash {
        size_t operator()(const TSelectKey& key) const noexcept {
            return key.Hash;
        }
    };

    struct TGenericState: public TThrRefBase {
        using TPtr = TIntrusivePtr<TGenericState>;

        struct TTableAddress {
            TString ClusterName;
            TString TableName;

            TString ToString() const {
                return TStringBuilder() << "`" << ClusterName << "`.`" << TableName << "`";
            }

            bool operator==(const TTableAddress& other) const {
                return ClusterName == other.ClusterName && TableName == other.TableName;
            }

            explicit operator size_t() const;

            ///
            /// Make a key for a select request on a cluster table
            ///
            TSelectKey MakeKeyFor(const NConnector::NApi::TSelect& select) const;
        };

        struct TTableMeta {
            const TStructExprType* ItemType = nullptr;
            // TODO: check why is it important
            TVector<TString> ColumnOrder;
            // External datasource description
            NYql::TGenericDataSourceInstance DataSourceInstance;
            // External table schema
            NYql::NConnector::NApi::TSchema Schema;
            // Deprecated
            // Contains some binary description of table splits (partitions) produced by Connector
            std::vector<NYql::NConnector::NApi::TSplit> Splits;
            // Contains splits for a particular select
            std::unordered_map<TSelectKey, std::vector<NYql::NConnector::NApi::TSplit>, TSelectKeyHash> SelectSplits;

            bool HasSplitsForSelect(const TSelectKey& key) const;

            void AttachSplitsForSelect(const TSelectKey& key,
                                       std::vector<NYql::NConnector::NApi::TSplit>& splits);

            const std::vector<NYql::NConnector::NApi::TSplit>& GetSplitsForSelect(const TSelectKey& key) const;
        };

        using TGetTableResult = std::pair<const TTableMeta*, TIssues>;

        TGenericState() = delete;

        TGenericState(
            TTypeAnnotationContext* types,
            const NKikimr::NMiniKQL::IFunctionRegistry* functionRegistry,
            const std::shared_ptr<IDatabaseAsyncResolver>& databaseResolver,
            const ISecuredServiceAccountCredentialsFactory::TPtr& credentialsFactory,
            const NConnector::IClient::TPtr& genericClient,
            const TGenericGatewayConfig& gatewayConfig)
            : Types(types)
            , Configuration(MakeIntrusive<TGenericConfiguration>())
            , FunctionRegistry(functionRegistry)
            , DatabaseResolver(databaseResolver)
            , CredentialsFactory(credentialsFactory)
            , GenericClient(genericClient)
        {
            Configuration->Init(gatewayConfig, databaseResolver, DatabaseAuth, types->Credentials);
        }

        bool HasTable(const TTableAddress& tableAddress);
        void AddTable(const TTableAddress& tableAddress, TTableMeta&& tableMeta);
        std::optional<TIssue> AttachSplitsToTable(const TTableAddress& tableAddress,
                                                  const TSelectKey& key,
                                                  std::vector<NYql::NConnector::NApi::TSplit>& splits);
        TGetTableResult GetTable(const TTableAddress& tableAddress) const;

        TTypeAnnotationContext* Types;
        TGenericConfiguration::TPtr Configuration = MakeIntrusive<TGenericConfiguration>();
        const NKikimr::NMiniKQL::IFunctionRegistry* FunctionRegistry;

        // key - (database id, database type), value - credentials to access managed APIs
        IDatabaseAsyncResolver::TDatabaseAuthMap DatabaseAuth;
        std::shared_ptr<IDatabaseAsyncResolver> DatabaseResolver;

        // key - cluster name, value - TCredentialsProviderPtr
        // It's important to cache credentials providers, because they make IO
        // (synchronous call via Token Accessor client) during the construction.
        std::unordered_map<TString, NYdb::TCredentialsProviderPtr> CredentialProviders;
        ISecuredServiceAccountCredentialsFactory::TPtr CredentialsFactory;

        NConnector::IClient::TPtr GenericClient;

    private:
        THashMap<TTableAddress, TTableMeta> Tables_;
    };

} // namespace NYql
