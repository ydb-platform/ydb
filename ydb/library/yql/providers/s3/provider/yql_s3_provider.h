#pragma once

#include <ydb/library/yql/core/yql_data_provider.h>
#include <ydb/library/yql/providers/common/token_accessor/client/factory.h>
#include <ydb/library/yql/providers/common/http_gateway/yql_http_gateway.h>

#include "yql_s3_settings.h"

namespace NKikimr::NMiniKQL {
   class IFunctionRegistry;
}

namespace NYql {

struct TS3State : public TThrRefBase
{
    using TPtr = TIntrusivePtr<TS3State>;

    struct TTableMeta {
        const TStructExprType* ItemType = nullptr;
        TVector<TString> ColumnOrder;
    };

    std::unordered_map<std::pair<TString, TString>, TTableMeta, THash<std::pair<TString, TString>>> Tables;

    TTypeAnnotationContext* Types = nullptr;
    TS3Configuration::TPtr Configuration = MakeIntrusive<TS3Configuration>();
    const NKikimr::NMiniKQL::IFunctionRegistry* FunctionRegistry = nullptr;
    ISecuredServiceAccountCredentialsFactory::TPtr CredentialsFactory;
    IHTTPGateway::TPtr Gateway;
    TMaybe<ui64> MaxTasksPerStage;
};

TDataProviderInitializer GetS3DataProviderInitializer(IHTTPGateway::TPtr gateway, ISecuredServiceAccountCredentialsFactory::TPtr credentialsFactory = nullptr, bool allowLocalFiles = false);

TIntrusivePtr<IDataProvider> CreateS3DataSource(TS3State::TPtr state, IHTTPGateway::TPtr gateway);
TIntrusivePtr<IDataProvider> CreateS3DataSink(TS3State::TPtr state, IHTTPGateway::TPtr gateway);

} // namespace NYql
