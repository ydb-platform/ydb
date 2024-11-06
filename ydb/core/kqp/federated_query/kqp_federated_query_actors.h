#pragma once

#include <ydb/services/metadata/secret/secret.h>
#include <ydb/core/kqp/common/events/script_executions.h>
#include <ydb/core/protos/flat_scheme_op.pb.h>

#include <ydb/library/actors/core/actor.h>
#include <ydb/library/aclib/aclib.h>

#include <library/cpp/threading/future/future.h>


namespace NKikimr::NKqp {

IActor* CreateDescribeSecretsActor(const std::optional<NACLib::TUserToken>& userToken, const std::vector<NMetadata::NSecret::TSecretIdOrValue>& secretIdsOrNames, NThreading::TPromise<TEvDescribeSecretsResponse::TDescription> promise);
IActor* CreateDescribeSecretsActor(const std::optional<NACLib::TUserToken>& userToken, const std::vector<TString>& secretNames, NThreading::TPromise<TEvDescribeSecretsResponse::TDescription> promise);

void RegisterDescribeSecretsActor(const TActorId& replyActorId, const std::optional<NACLib::TUserToken>& userToken, const std::vector<NMetadata::NSecret::TSecretIdOrValue>& secretIdsOrNames, TActorSystem* actorSystem);
void RegisterDescribeSecretsActor(const TActorId& replyActorId, const std::optional<NACLib::TUserToken>& userToken, const std::vector<TString>& secretNames, NActors::TActorSystem* actorSystem);

NThreading::TFuture<TEvDescribeSecretsResponse::TDescription> DescribeExternalDataSourceSecrets(const NKikimrSchemeOp::TAuth& authDescription, const std::optional<NACLib::TUserToken>& userToken, TActorSystem* actorSystem);

}  // namespace NKikimr::NKqp
