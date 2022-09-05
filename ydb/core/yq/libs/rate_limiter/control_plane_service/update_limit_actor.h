#pragma once
#include <ydb/core/yq/libs/ydb/schema.h>
#include <ydb/core/yq/libs/ydb/ydb.h>

#include <library/cpp/actors/core/actor.h>

#include <util/generic/string.h>

namespace NYq {

NActors::IActor* MakeUpdateCloudRateLimitActor(
    NActors::TActorId parent,
    TYdbConnectionPtr connection,
    const TString& coordinationNodePath,
    const TString& cloudId,
    ui64 limit,
    TYdbSdkRetryPolicy::TPtr retryPolicy,
    ui64 cookie);

} // namespace NYq
