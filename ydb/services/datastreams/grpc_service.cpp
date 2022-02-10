#include "grpc_service.h" 
 
#include "datastreams_proxy.h" 
 
#include <ydb/core/base/appdata.h>
 
#include <ydb/core/grpc_services/grpc_helper.h>
#include <ydb/core/grpc_services/rpc_calls.h>
 
#include <ydb/core/tx/scheme_board/cache.h>
 
namespace NKikimr::NGRpcService { 
 
TGRpcDataStreamsService::TGRpcDataStreamsService(NActors::TActorSystem *system, 
                                 TIntrusivePtr<NMonitoring::TDynamicCounters> counters, 
                                 NActors::TActorId id) 
    : ActorSystem_(system) 
    , Counters_(counters) 
    , GRpcRequestProxyId_(id) 
{ 
} 
 
void TGRpcDataStreamsService::InitService(grpc::ServerCompletionQueue *cq, NGrpc::TLoggerPtr logger)
{ 
    CQ_ = cq; 
 
    InitNewSchemeCache(); 
    IActor *proxyService = NDataStreams::V1::CreateDataStreamsService(Counters_, NewSchemeCache); 
    auto actorId = ActorSystem_->Register(proxyService, TMailboxType::HTSwap, ActorSystem_->AppData<TAppData>()->UserPoolId); 
    ActorSystem_->RegisterLocalService(NDataStreams::V1::GetDataStreamsServiceActorID(), actorId); 
 
    SetupIncomingRequests(logger); 
} 
 
void TGRpcDataStreamsService::SetGlobalLimiterHandle(NGrpc::TGlobalLimiter *limiter) {
    Limiter_ = limiter; 
} 
 
bool TGRpcDataStreamsService::IncRequest() { 
    return Limiter_->Inc(); 
} 
 
void TGRpcDataStreamsService::DecRequest() { 
    Limiter_->Dec(); 
    Y_ASSERT(Limiter_->GetCurrentInFlight() >= 0); 
} 
 
void TGRpcDataStreamsService::InitNewSchemeCache() { 
    auto appData = ActorSystem_->AppData<TAppData>(); 
    auto cacheCounters = GetServiceCounters(Counters_, "pqproxy|schemecache");
    auto cacheConfig = MakeIntrusive<NSchemeCache::TSchemeCacheConfig>(appData, cacheCounters);
    NewSchemeCache = ActorSystem_->Register(CreateSchemeBoardSchemeCache(cacheConfig.Get()),
        TMailboxType::HTSwap, ActorSystem_->AppData<TAppData>()->UserPoolId);
} 
 
void TGRpcDataStreamsService::SetupIncomingRequests(NGrpc::TLoggerPtr logger)
{ 
    auto getCounterBlock = CreateCounterCb(Counters_, ActorSystem_); 
#ifdef ADD_REQUEST 
#error ADD_REQUEST macro already defined 
#endif 
#define ADD_REQUEST(NAME, IN, OUT, ACTION) \ 
    MakeIntrusive<TGRpcRequest<Ydb::DataStreams::V1::IN, Ydb::DataStreams::V1::OUT, TGRpcDataStreamsService>>(this, &Service_, CQ_, \ 
        [this](NGrpc::IRequestContextBase *ctx) { \
            ReportGrpcReqToMon(*ActorSystem_, ctx->GetPeer()); \ 
            ACTION; \ 
        }, &Ydb::DataStreams::V1::DataStreamsService::AsyncService::Request ## NAME, \ 
        #NAME, logger, getCounterBlock("data_streams", #NAME))->Run();
 
    ADD_REQUEST(DescribeStream, DescribeStreamRequest, DescribeStreamResponse, { 
        ActorSystem_->Send(GRpcRequestProxyId_, new TEvDataStreamsDescribeStreamRequest(ctx)); 
    }) 
    ADD_REQUEST(CreateStream, CreateStreamRequest, CreateStreamResponse, { 
        ActorSystem_->Send(GRpcRequestProxyId_, new TEvDataStreamsCreateStreamRequest(ctx)); 
    }) 
    ADD_REQUEST(ListStreams, ListStreamsRequest, ListStreamsResponse, { 
        ActorSystem_->Send(GRpcRequestProxyId_, new TEvDataStreamsListStreamsRequest(ctx)); 
    }) 
    ADD_REQUEST(DeleteStream, DeleteStreamRequest, DeleteStreamResponse, { 
        ActorSystem_->Send(GRpcRequestProxyId_, new TEvDataStreamsDeleteStreamRequest(ctx)); 
    }) 
    ADD_REQUEST(ListShards, ListShardsRequest, ListShardsResponse, { 
        ActorSystem_->Send(GRpcRequestProxyId_, new TEvDataStreamsListShardsRequest(ctx)); 
    }) 
    ADD_REQUEST(PutRecord, PutRecordRequest, PutRecordResponse, { 
        ActorSystem_->Send(GRpcRequestProxyId_, new TEvDataStreamsPutRecordRequest(ctx)); 
    }) 
    ADD_REQUEST(PutRecords, PutRecordsRequest, PutRecordsResponse, { 
        ActorSystem_->Send(GRpcRequestProxyId_, new TEvDataStreamsPutRecordsRequest(ctx)); 
    }) 
    ADD_REQUEST(GetRecords, GetRecordsRequest, GetRecordsResponse, { 
        ActorSystem_->Send(GRpcRequestProxyId_, new TEvDataStreamsGetRecordsRequest(ctx)); 
    }) 
    ADD_REQUEST(GetShardIterator, GetShardIteratorRequest, GetShardIteratorResponse, { 
        ActorSystem_->Send(GRpcRequestProxyId_, new TEvDataStreamsGetShardIteratorRequest(ctx)); 
    }) 
    ADD_REQUEST(SubscribeToShard, SubscribeToShardRequest, SubscribeToShardResponse, { 
        ActorSystem_->Send(GRpcRequestProxyId_, new TEvDataStreamsSubscribeToShardRequest(ctx)); 
    }) 
    ADD_REQUEST(DescribeLimits, DescribeLimitsRequest, DescribeLimitsResponse, { 
        ActorSystem_->Send(GRpcRequestProxyId_, new TEvDataStreamsDescribeLimitsRequest(ctx)); 
    }) 
    ADD_REQUEST(DescribeStreamSummary, DescribeStreamSummaryRequest, DescribeStreamSummaryResponse, { 
        ActorSystem_->Send(GRpcRequestProxyId_, new TEvDataStreamsDescribeStreamSummaryRequest(ctx)); 
    }) 
    ADD_REQUEST(DecreaseStreamRetentionPeriod, DecreaseStreamRetentionPeriodRequest, DecreaseStreamRetentionPeriodResponse, { 
        ActorSystem_->Send(GRpcRequestProxyId_, new TEvDataStreamsDecreaseStreamRetentionPeriodRequest(ctx)); 
    }) 
    ADD_REQUEST(IncreaseStreamRetentionPeriod, IncreaseStreamRetentionPeriodRequest, IncreaseStreamRetentionPeriodResponse, { 
        ActorSystem_->Send(GRpcRequestProxyId_, new TEvDataStreamsIncreaseStreamRetentionPeriodRequest(ctx)); 
    }) 
    ADD_REQUEST(UpdateShardCount, UpdateShardCountRequest, UpdateShardCountResponse, { 
        ActorSystem_->Send(GRpcRequestProxyId_, new TEvDataStreamsUpdateShardCountRequest(ctx)); 
    }) 
    ADD_REQUEST(RegisterStreamConsumer, RegisterStreamConsumerRequest, RegisterStreamConsumerResponse, { 
        ActorSystem_->Send(GRpcRequestProxyId_, new TEvDataStreamsRegisterStreamConsumerRequest(ctx)); 
    }) 
    ADD_REQUEST(DeregisterStreamConsumer, DeregisterStreamConsumerRequest, DeregisterStreamConsumerResponse, { 
        ActorSystem_->Send(GRpcRequestProxyId_, new TEvDataStreamsDeregisterStreamConsumerRequest(ctx)); 
    }) 
    ADD_REQUEST(DescribeStreamConsumer, DescribeStreamConsumerRequest, DescribeStreamConsumerResponse, { 
        ActorSystem_->Send(GRpcRequestProxyId_, new TEvDataStreamsDescribeStreamConsumerRequest(ctx)); 
    }) 
    ADD_REQUEST(ListStreamConsumers, ListStreamConsumersRequest, ListStreamConsumersResponse, { 
        ActorSystem_->Send(GRpcRequestProxyId_, new TEvDataStreamsListStreamConsumersRequest(ctx)); 
    }) 
    ADD_REQUEST(AddTagsToStream, AddTagsToStreamRequest, AddTagsToStreamResponse, { 
        ActorSystem_->Send(GRpcRequestProxyId_, new TEvDataStreamsAddTagsToStreamRequest(ctx)); 
    }) 
    ADD_REQUEST(DisableEnhancedMonitoring, DisableEnhancedMonitoringRequest, DisableEnhancedMonitoringResponse, { 
        ActorSystem_->Send(GRpcRequestProxyId_, new TEvDataStreamsDisableEnhancedMonitoringRequest(ctx)); 
    }) 
    ADD_REQUEST(EnableEnhancedMonitoring, EnableEnhancedMonitoringRequest, EnableEnhancedMonitoringResponse, { 
        ActorSystem_->Send(GRpcRequestProxyId_, new TEvDataStreamsEnableEnhancedMonitoringRequest(ctx)); 
    }) 
    ADD_REQUEST(ListTagsForStream, ListTagsForStreamRequest, ListTagsForStreamResponse, { 
        ActorSystem_->Send(GRpcRequestProxyId_, new TEvDataStreamsListTagsForStreamRequest(ctx)); 
    }) 
    ADD_REQUEST(MergeShards, MergeShardsRequest, MergeShardsResponse, { 
        ActorSystem_->Send(GRpcRequestProxyId_, new TEvDataStreamsMergeShardsRequest(ctx)); 
    }) 
    ADD_REQUEST(RemoveTagsFromStream, RemoveTagsFromStreamRequest, RemoveTagsFromStreamResponse, { 
        ActorSystem_->Send(GRpcRequestProxyId_, new TEvDataStreamsRemoveTagsFromStreamRequest(ctx)); 
    }) 
    ADD_REQUEST(SplitShard, SplitShardRequest, SplitShardResponse, { 
        ActorSystem_->Send(GRpcRequestProxyId_, new TEvDataStreamsSplitShardRequest(ctx)); 
    }) 
    ADD_REQUEST(StartStreamEncryption, StartStreamEncryptionRequest, StartStreamEncryptionResponse, { 
        ActorSystem_->Send(GRpcRequestProxyId_, new TEvDataStreamsStartStreamEncryptionRequest(ctx)); 
    }) 
    ADD_REQUEST(StopStreamEncryption, StopStreamEncryptionRequest, StopStreamEncryptionResponse, { 
        ActorSystem_->Send(GRpcRequestProxyId_, new TEvDataStreamsStopStreamEncryptionRequest(ctx)); 
    }) 
    ADD_REQUEST(UpdateStream, UpdateStreamRequest, UpdateStreamResponse, { 
        ActorSystem_->Send(GRpcRequestProxyId_, new TEvDataStreamsUpdateStreamRequest(ctx)); 
    }) 
    ADD_REQUEST(SetWriteQuota, SetWriteQuotaRequest, SetWriteQuotaResponse, { 
        ActorSystem_->Send(GRpcRequestProxyId_, new TEvDataStreamsSetWriteQuotaRequest(ctx)); 
    }) 
 
#undef ADD_REQUEST 
} 
 
} 
