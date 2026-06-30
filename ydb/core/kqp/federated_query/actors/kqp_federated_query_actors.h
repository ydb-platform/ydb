#pragma once

#include <ydb/core/kqp/common/events/script_executions.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/driver/driver.h>

#include <library/cpp/threading/future/future.h>

#include <util/generic/ptr.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>

#include <memory>

namespace NActors {

class IActor;
struct TActorId;
class TActorSystem;

} // namespace NActors

namespace NACLib {

class TUserToken;

} // namespace NACLib

namespace NKikimrSchemeOp {

class TAuth;

} // namespace NKikimrSchemeOp

namespace NKikimr::NKqp {

void RegisterDescribeSecretsActor(
    const NActors::TActorId& replyActorId,
    const TIntrusiveConstPtr<NACLib::TUserToken> userToken,
    const TString& database,
    const std::vector<TString>& secretIds,
    NActors::TActorSystem* actorSystem
);

NThreading::TFuture<TEvDescribeSecretsResponse::TDescription> DescribeExternalDataSourceSecrets(
    const NKikimrSchemeOp::TAuth& authDescription,
    const TIntrusiveConstPtr<NACLib::TUserToken> userToken,
    const TString& database,
    NActors::TActorSystem* actorSystem
);

NThreading::TFuture<TEvDescribeResourceIdResponse::TDescription> DescribeExternalDataSourceResourceId(
    const TString& endpoint,
    const TString& database,
    bool ssl,
    const TString& caCert,
    const TString& token,
    NActors::TActorSystem* actorSystem
);

NActors::IActor* CreateDescribeResourceIdServiceActor(const std::shared_ptr<NYdb::TDriver>& driver);

std::unique_ptr<NActors::IActor> NewCachingIamServiceCredentialsProviderService();

}  // namespace NKikimr::NKqp
