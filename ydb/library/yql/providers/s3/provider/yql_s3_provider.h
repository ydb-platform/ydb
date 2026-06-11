#pragma once

#include "yql_s3_settings.h"

#include <ydb/library/actors/core/actorsystem_fwd.h>
#include <ydb/library/yql/providers/common/http_gateway/yql_http_gateway.h>

#include <yql/essentials/core/yql_data_provider.h>

#include <util/generic/hash.h>
#include <util/generic/ptr.h>
#include <util/generic/vector.h>

#include <list>
#include <memory>
#include <unordered_map>

namespace NKikimr::NMiniKQL {

class IFunctionRegistry;

} // namespace NKikimr::NMiniKQL

namespace NYql {

struct TS3Configuration;
class ISecuredServiceAccountCredentialsFactory;

struct TS3State : public TThrRefBase {
    using TPtr = TIntrusivePtr<TS3State>;

    TS3State();

    struct TTableMeta {
        const TStructExprType* ItemType = nullptr;
        TVector<TString> ColumnOrder;
    };

    std::unordered_map<std::pair<TString, TString>, TTableMeta, THash<std::pair<TString, TString>>> Tables;

    TTypeAnnotationContext* Types = nullptr;
    TIntrusivePtr<TS3Configuration> Configuration;
    const NKikimr::NMiniKQL::IFunctionRegistry* FunctionRegistry = nullptr;
    std::shared_ptr<ISecuredServiceAccountCredentialsFactory> CredentialsFactory;
    IHTTPGateway::TPtr Gateway;
    IHTTPGateway::TRetryPolicy::TPtr GatewayRetryPolicy;
    ui32 ExecutorPoolId = 0;
    std::list<TVector<TString>> PrimaryKeys;
    NActors::TActorSystem* ActorSystem = nullptr;
    bool EnableS3ConstraintsTransformer = false;
};

TDataProviderInitializer GetS3DataProviderInitializer(
    IHTTPGateway::TPtr gateway, std::shared_ptr<ISecuredServiceAccountCredentialsFactory> credentialsFactory = nullptr,
    NActors::TActorSystem* actorSystem = nullptr, TS3Configuration::TSetupper configurationInit = nullptr);

TIntrusivePtr<IDataProvider> CreateS3DataSource(TS3State::TPtr state);
TIntrusivePtr<IDataProvider> CreateS3DataSink(TS3State::TPtr state);

} // namespace NYql
