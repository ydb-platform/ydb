#pragma once

#include <ydb/library/yql/dq/actors/compute/dq_compute_actor_async_io.h>
#include <ydb/library/yql/providers/common/http_gateway/yql_http_gateway.h>
#include <ydb/library/yql/providers/s3/actors_factory/yql_s3_actors_factory.h>
#include <ydb/library/yql/providers/s3/object_listers/yql_s3_list.h>
#include <ydb/library/yql/providers/s3/proto/retry_config.pb.h>
#include <ydb/library/yql/providers/s3/proto/source.pb.h>
#include <ydb/library/yql/providers/s3/range_helpers/path_list_reader.h>
#include <ydb/library/yql/providers/common/token_accessor/client/factory.h>

#include <ydb/library/actors/core/actor.h>

namespace NYql::NDq {

NActors::IActor* CreateS3FileQueueActor(
        TTxId txId,
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
        const TS3Credentials& credentials,
        TString pattern,
        NYql::NS3Lister::ES3PatternVariant patternVariant,
        NS3Lister::ES3PatternType patternType);

std::pair<NYql::NDq::IDqComputeActorAsyncInput*, NActors::IActor*> CreateS3ReadActor(
    const NKikimr::NMiniKQL::TTypeEnvironment& typeEnv,
    const NKikimr::NMiniKQL::THolderFactory& holderFactory,
    IHTTPGateway::TPtr gateway,
    NS3::TSource&& params,
    ui64 inputIndex,
    TCollectStatsLevel statsLevel,
    const TTxId& txId,
    const THashMap<TString, TString>& secureParams,
    const THashMap<TString, TString>& taskParams,
    const TVector<TString>& readRanges,
    const NActors::TActorId& computeActorId,
    ISecuredServiceAccountCredentialsFactory::TPtr credentialsFactory,
    const IHTTPGateway::TRetryPolicy::TPtr& retryPolicy,
    const TS3ReadActorFactoryConfig& cfg,
    ::NMonitoring::TDynamicCounterPtr counters,
    ::NMonitoring::TDynamicCounterPtr taskCounters,
    IMemoryQuotaManager::TPtr memoryQuotaManager);

} // namespace NYql::NDq
