#include "grpc_service.h"

#include <ydb/core/grpc_services/service_datastreams.h>
#include <ydb/core/base/counters.h>
#include <ydb/core/base/appdata.h>
#include <ydb/core/base/ticket_parser.h>
#include <ydb/core/grpc_services/grpc_helper.h>
#include <ydb/core/grpc_services/base/base.h>
#include <ydb/core/tx/scheme_board/cache.h>

namespace {

using namespace NKikimr;

void YdsProcessAttr(const TSchemeBoardEvents::TDescribeSchemeResult& schemeData, NGRpcService::ICheckerIface* checker) {
    static const std::vector<TString> allowedAttributes = {"folder_id", "service_account_id", "database_id"};
    //full list of permissions for compatility. remove old permissions later.
    static const TVector<TString> permissions = {
        "yds.streams.write",
        "ydb.databases.list",
        "ydb.databases.create",
        "ydb.databases.connect"
    };
    TVector<std::pair<TString, TString>> attributes;
    attributes.reserve(schemeData.GetPathDescription().UserAttributesSize());
    for (const auto& attr : schemeData.GetPathDescription().GetUserAttributes()) {
        if (std::find(allowedAttributes.begin(), allowedAttributes.end(), attr.GetKey()) != allowedAttributes.end()) {
            attributes.emplace_back(attr.GetKey(), attr.GetValue());
        }
    }
    if (!attributes.empty()) {
        checker->SetEntries({{permissions, attributes}});
    }
}

}

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
    using std::placeholders::_1;
    using std::placeholders::_2;

#ifdef ADD_REQUEST
#error ADD_REQUEST macro already defined
#endif
#define ADD_REQUEST(NAME, CB, ATTR) \
    MakeIntrusive<TGRpcRequest<Ydb::DataStreams::V1::NAME##Request, Ydb::DataStreams::V1::NAME##Response, TGRpcDataStreamsService>> \
        (this, &Service_, CQ_,                                                                                  \
            [this](NGrpc::IRequestContextBase *ctx) {                                                           \
                NGRpcService::ReportGrpcReqToMon(*ActorSystem_, ctx->GetPeer());                                \
                ActorSystem_->Send(GRpcRequestProxyId_,                                                         \
                    new TGrpcRequestOperationCall<Ydb::DataStreams::V1::NAME##Request, Ydb::DataStreams::V1::NAME##Response>      \
                        (ctx, CB, TRequestAuxSettings{TRateLimiterMode::Off, ATTR}));                       \
            }, &Ydb::DataStreams::V1::DataStreamsService::AsyncService::Request ## NAME,                                  \
            #NAME, logger, getCounterBlock("data_streams", #NAME))->Run();

    ADD_REQUEST(DescribeStream, DoDataStreamsDescribeStreamRequest, nullptr)
    ADD_REQUEST(CreateStream, std::bind(DoDataStreamsCreateStreamRequest, _1, _2, NewSchemeCache), nullptr)
    ADD_REQUEST(ListStreams, std::bind(DoDataStreamsListStreamsRequest, _1, _2, NewSchemeCache), nullptr)
    ADD_REQUEST(DeleteStream, DoDataStreamsDeleteStreamRequest, nullptr)
    ADD_REQUEST(ListShards, std::bind(DoDataStreamsListShardsRequest, _1, _2, NewSchemeCache), nullptr)
    ADD_REQUEST(PutRecord, std::bind(DoDataStreamsPutRecordRequest, _1, _2, NewSchemeCache), YdsProcessAttr)
    ADD_REQUEST(PutRecords, std::bind(DoDataStreamsPutRecordsRequest, _1, _2, NewSchemeCache), YdsProcessAttr)
    ADD_REQUEST(GetRecords, std::bind(DoDataStreamsGetRecordsRequest, _1, _2, NewSchemeCache), nullptr)
    ADD_REQUEST(GetShardIterator, std::bind(DoDataStreamsGetShardIteratorRequest, _1, _2, NewSchemeCache), nullptr)
    ADD_REQUEST(SubscribeToShard, DoDataStreamsSubscribeToShardRequest, nullptr)
    ADD_REQUEST(DescribeLimits, DoDataStreamsDescribeLimitsRequest, nullptr)
    ADD_REQUEST(DescribeStreamSummary, DoDataStreamsDescribeStreamSummaryRequest, nullptr)
    ADD_REQUEST(DecreaseStreamRetentionPeriod, DoDataStreamsDecreaseStreamRetentionPeriodRequest, nullptr)
    ADD_REQUEST(IncreaseStreamRetentionPeriod, DoDataStreamsIncreaseStreamRetentionPeriodRequest, nullptr)
    ADD_REQUEST(UpdateShardCount, DoDataStreamsUpdateShardCountRequest, nullptr)
    ADD_REQUEST(RegisterStreamConsumer, DoDataStreamsRegisterStreamConsumerRequest, nullptr)
    ADD_REQUEST(DeregisterStreamConsumer, DoDataStreamsDeregisterStreamConsumerRequest, nullptr)
    ADD_REQUEST(DescribeStreamConsumer, DoDataStreamsDescribeStreamConsumerRequest, nullptr)
    ADD_REQUEST(ListStreamConsumers, DoDataStreamsListStreamConsumersRequest, nullptr)
    ADD_REQUEST(AddTagsToStream, DoDataStreamsAddTagsToStreamRequest, nullptr)
    ADD_REQUEST(DisableEnhancedMonitoring, DoDataStreamsDisableEnhancedMonitoringRequest, nullptr)
    ADD_REQUEST(EnableEnhancedMonitoring, DoDataStreamsEnableEnhancedMonitoringRequest, nullptr)
    ADD_REQUEST(ListTagsForStream, DoDataStreamsListTagsForStreamRequest, nullptr)
    ADD_REQUEST(MergeShards, DoDataStreamsMergeShardsRequest, nullptr)
    ADD_REQUEST(RemoveTagsFromStream, DoDataStreamsRemoveTagsFromStreamRequest, nullptr)
    ADD_REQUEST(SplitShard, DoDataStreamsSplitShardRequest, nullptr)
    ADD_REQUEST(StartStreamEncryption, DoDataStreamsStartStreamEncryptionRequest, nullptr)
    ADD_REQUEST(StopStreamEncryption, DoDataStreamsStopStreamEncryptionRequest, nullptr)
    ADD_REQUEST(UpdateStream, DoDataStreamsUpdateStreamRequest, nullptr)
    ADD_REQUEST(SetWriteQuota, DoDataStreamsSetWriteQuotaRequest, nullptr)

#undef ADD_REQUEST
}

}
