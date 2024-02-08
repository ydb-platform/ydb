#pragma once

#include <ydb/core/kqp/common/events/script_executions.h>

#include <ydb/library/actors/core/actor.h>


namespace NKikimr::NKqp {

NActors::IActor* CreateDescribeSecretsActor(const TString& ownerUserId, const std::vector<TString>& secretIds, NThreading::TPromise<TEvDescribeSecretsResponse::TDescription> promise, TDuration maximalSecretsSnapshotWaitTime);
void RegisterDescribeSecretsActor(const NActors::TActorId& replyActorId, const TString& ownerUserId, const std::vector<TString>& secretIds, const TActorContext& actorContext, TDuration maximalSecretsSnapshotWaitTime);

}  // namespace NKikimr::NKqp
