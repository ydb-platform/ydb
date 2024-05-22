#pragma once

#include <ydb/core/kqp/common/events/script_executions.h>
#include <ydb/core/protos/flat_scheme_op.pb.h>

#include <library/cpp/threading/future/future.h>

namespace NActors {
class TActorSystem;
}

namespace NKikimr::NKqp {

IActor* CreateDescribeSecretsActor(const TString& ownerUserId, const std::vector<TString>& secretIds, NThreading::TPromise<TEvDescribeSecretsResponse::TDescription> promise, TDuration maximalSecretsSnapshotWaitTime = TDuration::Zero());

void RegisterDescribeSecretsActor(const TActorId& replyActorId, const TString& ownerUserId, const std::vector<TString>& secretIds, TActorSystem* actorSystem, TDuration maximalSecretsSnapshotWaitTime = TDuration::Zero());

NThreading::TFuture<TEvDescribeSecretsResponse::TDescription> DescribeExternalDataSourceSecrets(const NKikimrSchemeOp::TAuth& authDescription, const TString& ownerUserId, TActorSystem* actorSystem, TDuration maximalSecretsSnapshotWaitTime = TDuration::Zero());

}  // namespace NKikimr::NKqp
