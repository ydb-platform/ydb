#pragma once 

#include <ydb/library/actors/core/actor.h>

#include <ydb/library/yql/providers/s3/actors_factory/yql_s3_actors_factory.h>
#include <ydb/library/yql/providers/s3/credentials/credentials.h>
#include <ydb/library/yql/providers/s3/object_listers/yql_s3_list.h>
#include <ydb/library/yql/providers/s3/range_helpers/path_list_reader.h>

#include <ydb/library/yql/dq/actors/compute/dq_compute_actor_async_io.h>
#include <ydb/library/yql/providers/common/http_gateway/yql_http_gateway.h>

namespace NYql::NDq {

std::pair<NYql::NDq::IDqComputeActorAsyncInput*, NActors::IActor*> CreateRawReadActor(
    ui64 inputIndex,
    TCollectStatsLevel statsLevel,
    const TTxId& txId,
    IHTTPGateway::TPtr gateway,
    const NKikimr::NMiniKQL::THolderFactory& holderFactory,
    const TString& url,
    const TS3Credentials& credentials,
    const TString& pattern,
    NYql::NS3Lister::ES3PatternVariant patternVariant,
    NYql::NS3Details::TPathList&& paths,
    bool addPathIndex,
    const NActors::TActorId& computeActorId,
    ui64 sizeLimit,
    const IHTTPGateway::TRetryPolicy::TPtr& retryPolicy,
    const TS3ReadActorFactoryConfig& readActorFactoryCfg,
    ::NMonitoring::TDynamicCounterPtr counters,
    ::NMonitoring::TDynamicCounterPtr taskCounters,
    ui64 fileSizeLimit,
    std::optional<ui64> rowsLimitHint,
    bool useRuntimeListing,
    NActors::TActorId fileQueueActor,
    ui64 fileQueueBatchSizeLimit,
    ui64 fileQueueBatchObjectCountLimit,
    ui64 fileQueueConsumersCountDelta,
    bool allowLocalFiles
);

} // namespace NYql::NDq
