#pragma once

#include "yql_generic_settings.h"

#include <sstream>
#include <ydb/library/yql/core/yql_data_provider.h>
#include <ydb/library/yql/providers/common/db_id_async_resolver/db_async_resolver.h>
#include <ydb/library/yql/providers/common/http_gateway/yql_http_gateway.h>
#include <ydb/library/yql/providers/generic/connector/libcpp/client.h>

namespace NKikimr::NMiniKQL {
    class IFunctionRegistry;
}

namespace NYql {

    struct TGenericState: public TThrRefBase {
        using TPtr = TIntrusivePtr<TGenericState>;

        struct TTableMeta {
            const TStructExprType* ItemType = nullptr;
            TVector<TString> ColumnOrder;
            NYql::Connector::API::Schema Schema;
            NYql::Connector::API::DataSourceInstance DataSourceInstance;

            TString ToString() const {
                TStringBuilder sb;
                sb << "Schema: " << Schema.ShortDebugString();
                sb << "; ColumnOrder: ";
                for (size_t i = 0; i < ColumnOrder.size(); i++) {
                    sb << i << " " << ColumnOrder[i];
                }
                if (ItemType) {
                    sb << "; ItemType: " << ItemType->ToString();
                } else {
                    sb << "; ItemType: nullptr";
                }

                return sb;
            }
        };

        TTableMeta& GetTable(const TString& cluster, const TString& table) {
            auto search = Tables.find(std::make_pair(cluster, table));
            if (search != Tables.end()) {
                return search->second;
            }

            ythrow yexception() << "unknown (" << cluster << ", " << table << ") pair";
        };

        THashMap<std::pair<TString, TString>, TTableMeta> Tables;
        std::unordered_map<std::string_view, std::string_view> Timezones;

        TTypeAnnotationContext* Types = nullptr;
        TGenericConfiguration::TPtr Configuration = MakeIntrusive<TGenericConfiguration>();
        const NKikimr::NMiniKQL::IFunctionRegistry* FunctionRegistry = nullptr;
        THashMap<std::pair<TString, NYql::DatabaseType>, NYql::TDatabaseAuth> DatabaseIds;
        std::shared_ptr<NYql::IDatabaseAsyncResolver> DbResolver;

        TString DumpToString() const {
            TStringBuilder sb;
            if (Tables) {
                for (const auto& kv : Tables) {
                    sb << "Table '" << kv.first << "':";
                    sb << kv.second.ToString() << "\n";
                }
            }
            return sb;
        }
    };

    TDataProviderInitializer
    GetGenericDataProviderInitializer(Connector::IClient::TPtr genericClient,
                                      std::shared_ptr<NYql::IDatabaseAsyncResolver> dbResolver = nullptr);

    TIntrusivePtr<IDataProvider> CreateGenericDataSource(TGenericState::TPtr state,
                                                         Connector::IClient::TPtr genericClient);
    TIntrusivePtr<IDataProvider> CreateGenericDataSink(TGenericState::TPtr state);

} // namespace NYql
