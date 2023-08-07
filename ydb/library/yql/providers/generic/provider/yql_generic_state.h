#pragma once

#include "yql_generic_settings.h"

#include <ydb/library/yql/core/yql_data_provider.h>
#include <ydb/library/yql/providers/generic/connector/libcpp/client.h>

namespace NKikimr::NMiniKQL {
    class IFunctionRegistry;
}

namespace NYql {
    struct TGenericState: public TThrRefBase {
        using TPtr = TIntrusivePtr<TGenericState>;

        using TTableAddress = std::pair<TString, TString>; // std::pair<clusterName, tableName>

        struct TTableMeta {
            const TStructExprType* ItemType = nullptr;
            TVector<TString> ColumnOrder;
            NYql::NConnector::NApi::TSchema Schema;
            NYql::NConnector::NApi::TDataSourceInstance DataSourceInstance;
        };

        using TGetTableResult = std::pair<std::optional<const TTableMeta*>, std::optional<TIssue>>;

        void AddTable(const TStringBuf& clusterName, const TStringBuf& tableName, TTableMeta&& tableMeta) {
            Tables_.emplace(TTableAddress(clusterName, tableName), tableMeta);
        }

        TGetTableResult GetTable(const TStringBuf& clusterName, const TStringBuf& tableName) const {
            auto result = Tables_.FindPtr(TTableAddress(clusterName, tableName));
            if (result) {
                return std::make_pair(result, std::nullopt);
            }

            return std::make_pair(
                std::nullopt,
                TIssue(TStringBuilder() << "no metadata for table " << clusterName << "." << tableName));
        };

        TGetTableResult GetTable(const TStringBuf& clusterName, const TStringBuf& tableName, const TPosition& position) const {
            auto pair = GetTable(clusterName, tableName);
            if (pair.second.has_value()) {
                pair.second->Position = position;
            }

            return pair;
        }

        // FIXME: not used anymore, delete it some day
        std::unordered_map<std::string_view, std::string_view> Timezones;

        TTypeAnnotationContext* Types = nullptr;
        TGenericConfiguration::TPtr Configuration = MakeIntrusive<TGenericConfiguration>();
        const NKikimr::NMiniKQL::IFunctionRegistry* FunctionRegistry = nullptr;

        // key - (database id, database type), value - credentials to access MDB API
        NYql::IDatabaseAsyncResolver::TDatabaseAuthMap DatabaseAuth;
        std::shared_ptr<NYql::IDatabaseAsyncResolver> DatabaseResolver;

    private:
        THashMap<TTableAddress, TTableMeta> Tables_;
    };
}
