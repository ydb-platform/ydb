#pragma once

#include <ydb/core/kqp/common/events/script_executions.h>
#include <ydb/core/protos/flat_scheme_op.pb.h>

#include <ydb/library/actors/core/actor.h>


namespace NKikimr::NKqp {

IActor* CreateDescribeSecretsActor(const TString& ownerUserId, const std::vector<TString>& secretIds, NThreading::TPromise<TEvDescribeSecretsResponse::TDescription> promise);

void RegisterDescribeSecretsActor(const TActorId& replyActorId, const TString& ownerUserId, const std::vector<TString>& secretIds, TActorSystem* actorSystem);

NThreading::TFuture<TEvDescribeSecretsResponse::TDescription> DescribeExternalDataSourceSecrets(const NKikimrSchemeOp::TAuth& authDescription, const TString& ownerUserId, TActorSystem* actorSystem);

}  // namespace NKikimr::NKqp
