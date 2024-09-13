#pragma once

#include <ydb/library/yql/dq/actors/compute/dq_compute_actor_async_io.h>
#include <ydb/library/yql/providers/common/http_gateway/yql_http_gateway.h>
#include "ydb/library/yql/providers/s3/object_listers/yql_s3_list.h"
#include <ydb/library/yql/providers/s3/proto/retry_config.pb.h>
#include <ydb/library/yql/providers/s3/proto/source.pb.h>
#include "ydb/library/yql/providers/s3/range_helpers/path_list_reader.h"
#include <ydb/library/yql/providers/common/token_accessor/client/factory.h>
#include <ydb/library/actors/core/actor.h>

namespace NYql::NDq {

NActors::IActor* CreateS3FileQueueActor(
        TTxId  txId,
        NS3Details::TPathList paths,
        size_t prefetchSize,
        ui64 fileSizeLimit,
        ui64 readLimit,
        bool useRuntimeListing,
        ui64 consumersCount,
        ui64 batchSizeLimit,
        ui64 batchObjectCountLimit,
        IHTTPGateway::TPtr gateway,
        IHTTPGateway::TRetryPolicy::TPtr retryPolicy,
        TString url,
        TS3Credentials::TAuthInfo authInfo,
        TString pattern,
        NYql::NS3Lister::ES3PatternVariant patternVariant,
        NS3Lister::ES3PatternType patternType);

} // namespace NYql::NDq
