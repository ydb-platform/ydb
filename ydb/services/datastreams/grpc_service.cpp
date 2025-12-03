#include "grpc_service.h"

#include <ydb/core/grpc_services/service_datastreams.h>
#include <ydb/core/base/counters.h>
#include <ydb/core/base/appdata.h>
#include <ydb/core/base/ticket_parser.h>
#include <ydb/core/grpc_services/grpc_helper.h>
#include <ydb/core/grpc_services/base/base.h>
#include <ydb/core/tx/scheme_board/cache.h>
#include <ydb/core/tx/scheme_board/events.h>
#include <ydb/library/cloud_permissions/cloud_permissions.h>
#include <ydb/library/grpc/server/grpc_method_setup.h>

namespace {

using namespace NKikimr;

void YdsProcessAttr(const TSchemeBoardEvents::TDescribeSchemeResult& schemeData, NGRpcService::ICheckerIface* checker) {
    static const std::vector<TString> allowedAttributes = {"folder_id", "service_account_id", "database_id"};
    TVector<std::pair<TString, TString>> attributes;
    attributes.reserve(schemeData.GetPathDescription().UserAttributesSize());
    for (const auto& attr : schemeData.GetPathDescription().GetUserAttributes()) {
        if (std::find(allowedAttributes.begin(), allowedAttributes.end(), attr.GetKey()) != allowedAttributes.end()) {
            attributes.emplace_back(attr.GetKey(), attr.GetValue());
        }
    }
    if (!attributes.empty()) {
        //full list of permissions for compatibility. remove old permissions later.
        checker->SetEntries({{NCloudPermissions::TCloudPermissions<NCloudPermissions::EType::DEFAULT>::Get(), attributes}});
    }
}

}

namespace NKikimr::NGRpcService {

void TGRpcDataStreamsService::SetupIncomingRequests(NYdbGrpc::TLoggerPtr logger) {
    using std::placeholders::_1;
    using std::placeholders::_2;
    using namespace Ydb::DataStreams::V1;
    auto getCounterBlock = CreateCounterCb(Counters_, ActorSystem_);

#ifdef SETUP_DS_METHOD
#error SETUP_DS_METHOD macro already defined
#endif

#define SETUP_DS_METHOD(methodName, methodCallback, rlMode, requestType, auditMode, customAttributeProcessorCallback) \
    SETUP_RUNTIME_EVENT_METHOD(methodName,                       \
        YDB_API_DEFAULT_REQUEST_TYPE(methodName),                \
        YDB_API_DEFAULT_RESPONSE_TYPE(methodName),               \
        methodCallback,                                          \
        rlMode,                                                  \
        requestType,                                             \
        YDB_API_DEFAULT_COUNTER_BLOCK(data_streams, methodName), \
        auditMode,                                               \
        COMMON,                                                  \
        ::NKikimr::NGRpcService::TGrpcRequestOperationCall,      \
        GRpcRequestProxyId_,                                     \
        CQ_,                                                     \
        nullptr,                                                 \
        customAttributeProcessorCallback)

    SETUP_DS_METHOD(DescribeStream, DoDataStreamsDescribeStreamRequest, RLMODE(Off), UNSPECIFIED, TAuditMode::NonModifying(), nullptr);
    SETUP_DS_METHOD(CreateStream, DoDataStreamsCreateStreamRequest, RLMODE(Off), UNSPECIFIED, TAuditMode::Modifying(TAuditMode::TLogClassConfig::Ddl), nullptr);
    SETUP_DS_METHOD(ListStreams, DoDataStreamsListStreamsRequest, RLMODE(Off), UNSPECIFIED, TAuditMode::NonModifying(), nullptr);
    SETUP_DS_METHOD(DeleteStream, DoDataStreamsDeleteStreamRequest, RLMODE(Off), UNSPECIFIED, TAuditMode::Modifying(TAuditMode::TLogClassConfig::Ddl), nullptr);
    SETUP_DS_METHOD(ListShards, DoDataStreamsListShardsRequest, RLMODE(Off), UNSPECIFIED, TAuditMode::NonModifying(), nullptr);
    SETUP_DS_METHOD(PutRecord, DoDataStreamsPutRecordRequest, RLSWITCH(RuTopic), UNSPECIFIED, TAuditMode::Modifying(TAuditMode::TLogClassConfig::Dml), YdsProcessAttr);
    SETUP_DS_METHOD(PutRecords, DoDataStreamsPutRecordsRequest, RLSWITCH(RuTopic), UNSPECIFIED, TAuditMode::Modifying(TAuditMode::TLogClassConfig::Dml), YdsProcessAttr);
    SETUP_DS_METHOD(GetRecords, DoDataStreamsGetRecordsRequest, RLSWITCH(RuTopic), UNSPECIFIED, TAuditMode::NonModifying(), nullptr);
    SETUP_DS_METHOD(GetShardIterator, DoDataStreamsGetShardIteratorRequest, RLMODE(Off), UNSPECIFIED, TAuditMode::NonModifying(), nullptr);
    SETUP_DS_METHOD(SubscribeToShard, DoDataStreamsSubscribeToShardRequest, RLMODE(Off), UNSPECIFIED, TAuditMode::NonModifying(), nullptr);
    SETUP_DS_METHOD(DescribeLimits, DoDataStreamsDescribeLimitsRequest, RLMODE(Off), UNSPECIFIED, TAuditMode::NonModifying(), nullptr);
    SETUP_DS_METHOD(DescribeStreamSummary, DoDataStreamsDescribeStreamSummaryRequest, RLMODE(Off), UNSPECIFIED, TAuditMode::NonModifying(), nullptr);
    SETUP_DS_METHOD(DecreaseStreamRetentionPeriod, DoDataStreamsDecreaseStreamRetentionPeriodRequest, RLMODE(Off), UNSPECIFIED, TAuditMode::Modifying(TAuditMode::TLogClassConfig::Ddl), nullptr);
    SETUP_DS_METHOD(IncreaseStreamRetentionPeriod, DoDataStreamsIncreaseStreamRetentionPeriodRequest, RLMODE(Off), UNSPECIFIED, TAuditMode::Modifying(TAuditMode::TLogClassConfig::Ddl), nullptr);
    SETUP_DS_METHOD(UpdateShardCount, DoDataStreamsUpdateShardCountRequest, RLMODE(Off), UNSPECIFIED, TAuditMode::Modifying(TAuditMode::TLogClassConfig::Ddl), nullptr);
    SETUP_DS_METHOD(UpdateStreamMode, DoDataStreamsUpdateStreamModeRequest, RLMODE(Off), UNSPECIFIED, TAuditMode::Modifying(TAuditMode::TLogClassConfig::Ddl), nullptr);
    SETUP_DS_METHOD(RegisterStreamConsumer, DoDataStreamsRegisterStreamConsumerRequest, RLMODE(Off), UNSPECIFIED, TAuditMode::Modifying(TAuditMode::TLogClassConfig::Ddl), nullptr);
    SETUP_DS_METHOD(DeregisterStreamConsumer, DoDataStreamsDeregisterStreamConsumerRequest, RLMODE(Off), UNSPECIFIED, TAuditMode::Modifying(TAuditMode::TLogClassConfig::Ddl), nullptr);
    SETUP_DS_METHOD(DescribeStreamConsumer, DoDataStreamsDescribeStreamConsumerRequest, RLMODE(Off), UNSPECIFIED, TAuditMode::NonModifying(), nullptr);
    SETUP_DS_METHOD(ListStreamConsumers, DoDataStreamsListStreamConsumersRequest, RLMODE(Off), UNSPECIFIED, TAuditMode::NonModifying(), nullptr);
    SETUP_DS_METHOD(AddTagsToStream, DoDataStreamsAddTagsToStreamRequest, RLMODE(Off), UNSPECIFIED, TAuditMode::Modifying(TAuditMode::TLogClassConfig::Ddl), nullptr);
    SETUP_DS_METHOD(DisableEnhancedMonitoring, DoDataStreamsDisableEnhancedMonitoringRequest, RLMODE(Off), UNSPECIFIED, TAuditMode::Modifying(TAuditMode::TLogClassConfig::Ddl), nullptr);
    SETUP_DS_METHOD(EnableEnhancedMonitoring, DoDataStreamsEnableEnhancedMonitoringRequest, RLMODE(Off), UNSPECIFIED, TAuditMode::Modifying(TAuditMode::TLogClassConfig::Ddl), nullptr);
    SETUP_DS_METHOD(ListTagsForStream, DoDataStreamsListTagsForStreamRequest, RLMODE(Off), UNSPECIFIED, TAuditMode::NonModifying(), nullptr);
    SETUP_DS_METHOD(MergeShards, DoDataStreamsMergeShardsRequest, RLMODE(Off), UNSPECIFIED, TAuditMode::Modifying(TAuditMode::TLogClassConfig::Ddl), nullptr);
    SETUP_DS_METHOD(RemoveTagsFromStream, DoDataStreamsRemoveTagsFromStreamRequest, RLMODE(Off), UNSPECIFIED, TAuditMode::Modifying(TAuditMode::TLogClassConfig::Ddl), nullptr);
    SETUP_DS_METHOD(SplitShard, DoDataStreamsSplitShardRequest, RLMODE(Off), UNSPECIFIED, TAuditMode::Modifying(TAuditMode::TLogClassConfig::Ddl), nullptr);
    SETUP_DS_METHOD(StartStreamEncryption, DoDataStreamsStartStreamEncryptionRequest, RLMODE(Off), UNSPECIFIED, TAuditMode::Modifying(TAuditMode::TLogClassConfig::Ddl), nullptr);
    SETUP_DS_METHOD(StopStreamEncryption, DoDataStreamsStopStreamEncryptionRequest, RLMODE(Off), UNSPECIFIED, TAuditMode::Modifying(TAuditMode::TLogClassConfig::Ddl), nullptr);
    SETUP_DS_METHOD(UpdateStream, DoDataStreamsUpdateStreamRequest, RLMODE(Off), UNSPECIFIED, TAuditMode::Modifying(TAuditMode::TLogClassConfig::Ddl), nullptr);
    SETUP_DS_METHOD(SetWriteQuota, DoDataStreamsSetWriteQuotaRequest, RLMODE(Off), UNSPECIFIED, TAuditMode::Modifying(TAuditMode::TLogClassConfig::Ddl), nullptr);

#undef SETUP_DS_METHOD
}

}
