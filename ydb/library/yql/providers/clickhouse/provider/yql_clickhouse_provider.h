#pragma once

#include "yql_clickhouse_settings.h"

#include <ydb/library/yql/core/yql_data_provider.h>
#include <ydb/library/yql/providers/common/http_gateway/yql_http_gateway.h>
#include <ydb/library/yql/providers/common/db_id_async_resolver/db_async_resolver.h>

namespace NKikimr::NMiniKQL {
   class IFunctionRegistry;
}

namespace NYql {

struct TClickHouseState : public TThrRefBase
{
    using TPtr = TIntrusivePtr<TClickHouseState>;

    struct TTableMeta {
        const TStructExprType* ItemType = nullptr;
        TVector<TString> ColumnOrder;
    };

    THashMap<std::pair<TString, TString>, TTableMeta> Tables;
    std::unordered_map<std::string_view, std::string_view> Timezones;

    TTypeAnnotationContext* Types = nullptr;
    TClickHouseConfiguration::TPtr Configuration = MakeIntrusive<TClickHouseConfiguration>();
    const NKikimr::NMiniKQL::IFunctionRegistry* FunctionRegistry = nullptr;
    THashMap<std::pair<TString, NYql::EDatabaseType>, NYql::TDatabaseAuth> DatabaseIds;
    std::shared_ptr<NYql::IDatabaseAsyncResolver> DbResolver;
};

TDataProviderInitializer GetClickHouseDataProviderInitializer(
    IHTTPGateway::TPtr gateway,
    std::shared_ptr<NYql::IDatabaseAsyncResolver> dbResolver = nullptr
);

TIntrusivePtr<IDataProvider> CreateClickHouseDataSource(TClickHouseState::TPtr state, IHTTPGateway::TPtr gateway);
TIntrusivePtr<IDataProvider> CreateClickHouseDataSink(TClickHouseState::TPtr state);

} // namespace NYql
