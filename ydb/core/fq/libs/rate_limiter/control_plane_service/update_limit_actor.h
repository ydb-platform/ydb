#pragma once
#include <ydb/core/fq/libs/ydb/schema.h>
#include <ydb/core/fq/libs/ydb/ydb.h>

#include <ydb/library/actors/core/actor.h>

#include <util/generic/string.h>

namespace NFq {

NActors::IActor* MakeUpdateCloudRateLimitActor(
    NActors::TActorId parent,
    TYdbConnectionPtr connection,
    const TString& coordinationNodePath,
    const TString& cloudId,
    ui64 limit,
    TYdbSdkRetryPolicy::TPtr retryPolicy,
    ui64 cookie);

} // namespace NFq
