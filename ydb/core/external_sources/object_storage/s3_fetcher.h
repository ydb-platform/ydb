#pragma once

#include "events.h"

#include <util/generic/guid.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/yql/providers/common/http_gateway/yql_http_default_retry_policy.h>
#include <ydb/library/yql/providers/s3/credentials/credentials.h>

namespace NKikimr::NExternalSource::NObjectStorage {

NActors::IActor* CreateS3FetcherActor(
    TString url,
    NYql::IHTTPGateway::TPtr gateway,
    NYql::IHTTPGateway::TRetryPolicy::TPtr retryPolicy,
    const NYql::TS3Credentials& credentials);
} // namespace NKikimr::NExternalSource::NObjectStorage
