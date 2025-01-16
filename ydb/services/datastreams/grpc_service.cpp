#include "grpc_service.h"

#include <ydb/core/grpc_services/service_datastreams.h>
#include <ydb/core/base/counters.h>
#include <ydb/core/base/appdata.h>
#include <ydb/core/base/ticket_parser.h>
#include <ydb/core/grpc_services/grpc_helper.h>
#include <ydb/core/grpc_services/base/base.h>
#include <ydb/core/tx/scheme_board/cache.h>
#include <ydb/core/tx/scheme_board/events.h>

namespace {

using namespace NKikimr;

void YdsProcessAttr(const TSchemeBoardEvents::TDescribeSchemeResult& schemeData, NGRpcService::ICheckerIface* checker) {
    static const std::vector<TString> allowedAttributes = {"folder_id", "service_account_id", "database_id"};
    //full list of permissions for compatibility. remove old permissions later.
    static const TVector<TString> permissions = {
        "ydb.streams.write",
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

void TGRpcDataStreamsService::SetupIncomingRequests(NYdbGrpc::TLoggerPtr logger)
{
    auto getCounterBlock = CreateCounterCb(Counters_, ActorSystem_);
    using std::placeholders::_1;
    using std::placeholders::_2;

#ifdef ADD_REQUEST
#error ADD_REQUEST macro already defined
#endif
#define ADD_REQUEST(NAME, CB, ATTR, LIMIT_TYPE) \
    MakeIntrusive<TGRpcRequest<Ydb::DataStreams::V1::NAME##Request, Ydb::DataStreams::V1::NAME##Response, TGRpcDataStreamsService>> \
        (this, &Service_, CQ_,                                                                                                      \
            [this](NYdbGrpc::IRequestContextBase *ctx) {                                                                               \
                NGRpcService::ReportGrpcReqToMon(*ActorSystem_, ctx->GetPeer());                                                    \
                ActorSystem_->Send(GRpcRequestProxyId_,                                                                             \
                    new TGrpcRequestOperationCall<Ydb::DataStreams::V1::NAME##Request, Ydb::DataStreams::V1::NAME##Response>        \
                        (ctx, CB, TRequestAuxSettings{RLSWITCH(TRateLimiterMode::LIMIT_TYPE), ATTR}));                              \
            }, &Ydb::DataStreams::V1::DataStreamsService::AsyncService::Request ## NAME,                                            \
            #NAME, logger, getCounterBlock("data_streams", #NAME))->Run();

    ADD_REQUEST(DescribeStream, DoDataStreamsDescribeStreamRequest, nullptr, Off)
    ADD_REQUEST(CreateStream, DoDataStreamsCreateStreamRequest, nullptr, Off)
    ADD_REQUEST(ListStreams, DoDataStreamsListStreamsRequest, nullptr, Off)
    ADD_REQUEST(DeleteStream, DoDataStreamsDeleteStreamRequest, nullptr, Off)
    ADD_REQUEST(ListShards, DoDataStreamsListShardsRequest, nullptr, Off)
    ADD_REQUEST(PutRecord, DoDataStreamsPutRecordRequest, YdsProcessAttr, RuManual)
    ADD_REQUEST(PutRecords, DoDataStreamsPutRecordsRequest, YdsProcessAttr, RuManual)
    ADD_REQUEST(GetRecords, DoDataStreamsGetRecordsRequest, nullptr, RuManual)
    ADD_REQUEST(GetShardIterator, DoDataStreamsGetShardIteratorRequest, nullptr, Off)
    ADD_REQUEST(SubscribeToShard, DoDataStreamsSubscribeToShardRequest, nullptr, Off)
    ADD_REQUEST(DescribeLimits, DoDataStreamsDescribeLimitsRequest, nullptr, Off)
    ADD_REQUEST(DescribeStreamSummary, DoDataStreamsDescribeStreamSummaryRequest, nullptr, Off)
    ADD_REQUEST(DecreaseStreamRetentionPeriod, DoDataStreamsDecreaseStreamRetentionPeriodRequest, nullptr, Off)
    ADD_REQUEST(IncreaseStreamRetentionPeriod, DoDataStreamsIncreaseStreamRetentionPeriodRequest, nullptr, Off)
    ADD_REQUEST(UpdateShardCount, DoDataStreamsUpdateShardCountRequest, nullptr, Off)
    ADD_REQUEST(UpdateStreamMode, DoDataStreamsUpdateStreamModeRequest, nullptr, Off)
    ADD_REQUEST(RegisterStreamConsumer, DoDataStreamsRegisterStreamConsumerRequest, nullptr, Off)
    ADD_REQUEST(DeregisterStreamConsumer, DoDataStreamsDeregisterStreamConsumerRequest, nullptr, Off)
    ADD_REQUEST(DescribeStreamConsumer, DoDataStreamsDescribeStreamConsumerRequest, nullptr, Off)
    ADD_REQUEST(ListStreamConsumers, DoDataStreamsListStreamConsumersRequest, nullptr, Off)
    ADD_REQUEST(AddTagsToStream, DoDataStreamsAddTagsToStreamRequest, nullptr, Off)
    ADD_REQUEST(DisableEnhancedMonitoring, DoDataStreamsDisableEnhancedMonitoringRequest, nullptr, Off)
    ADD_REQUEST(EnableEnhancedMonitoring, DoDataStreamsEnableEnhancedMonitoringRequest, nullptr, Off)
    ADD_REQUEST(ListTagsForStream, DoDataStreamsListTagsForStreamRequest, nullptr, Off)
    ADD_REQUEST(MergeShards, DoDataStreamsMergeShardsRequest, nullptr, Off)
    ADD_REQUEST(RemoveTagsFromStream, DoDataStreamsRemoveTagsFromStreamRequest, nullptr, Off)
    ADD_REQUEST(SplitShard, DoDataStreamsSplitShardRequest, nullptr, Off)
    ADD_REQUEST(StartStreamEncryption, DoDataStreamsStartStreamEncryptionRequest, nullptr, Off)
    ADD_REQUEST(StopStreamEncryption, DoDataStreamsStopStreamEncryptionRequest, nullptr, Off)
    ADD_REQUEST(UpdateStream, DoDataStreamsUpdateStreamRequest, nullptr, Off)
    ADD_REQUEST(SetWriteQuota, DoDataStreamsSetWriteQuotaRequest, nullptr, Off)

#undef ADD_REQUEST
}

}
