#pragma once

#include <ydb/library/yql/core/yql_data_provider.h>
#include "yql_ydb_settings.h"

#include <ydb/library/yql/providers/common/token_accessor/client/factory.h>
#include <ydb/public/sdk/cpp/client/ydb_driver/driver.h>
#include <ydb/public/lib/experimental/ydb_clickhouse_internal.h>
#include <ydb/library/yql/providers/common/db_id_async_resolver/db_async_resolver.h>

namespace NKikimr::NMiniKQL {
   class IFunctionRegistry;
}

namespace NYql {

struct TYdbState : public TThrRefBase
{
    using TPtr = TIntrusivePtr<TYdbState>;

    struct TTableMeta {
        const TStructExprType* ItemType = nullptr;
        std::vector<NUdf::TDataTypeId>  KeyTypes;
        TVector<TString> ColumnOrder;
        NYdb::NClickhouseInternal::TSnapshotHandle Snapshot;
        std::vector<std::array<TString, 2U>> Partitions;
        bool ReadAsync = false;
    };

    std::unordered_map<std::pair<TString, TString>, TTableMeta, THash<std::pair<TString, TString>>> Tables;

    TTypeAnnotationContext* Types = nullptr;
    TYdbConfiguration::TPtr Configuration = MakeIntrusive<TYdbConfiguration>();
    const NKikimr::NMiniKQL::IFunctionRegistry* FunctionRegistry = nullptr;
    ISecuredServiceAccountCredentialsFactory::TPtr CredentialsFactory;
    THashMap<std::pair<TString, NYql::EDatabaseType>, NYql::TDatabaseAuth> DatabaseIds;
    std::shared_ptr<NYql::IDatabaseAsyncResolver> DbResolver;
};

TDataProviderInitializer GetYdbDataProviderInitializer(
    NYdb::TDriver driver,
    ISecuredServiceAccountCredentialsFactory::TPtr credentialsFactory = nullptr,
    const std::shared_ptr<NYql::IDatabaseAsyncResolver> dbResolver = nullptr
);

TIntrusivePtr<IDataProvider> CreateYdbDataSource(
    TYdbState::TPtr state,
    NYdb::TDriver driver
);

TIntrusivePtr<IDataProvider> CreateYdbDataSink(TYdbState::TPtr state);

} // namespace NYql
