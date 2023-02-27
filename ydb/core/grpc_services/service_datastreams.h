#pragma once
#include <memory>

namespace NActors {
struct TActorId;
}

namespace NKikimr {
namespace NGRpcService {

class IRequestOpCtx;
class IFacilityProvider;

void DoDataStreamsCreateStreamRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f);
void DoDataStreamsDeleteStreamRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f);
void DoDataStreamsDescribeStreamRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f);
void DoDataStreamsPutRecordRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f);
void DoDataStreamsRegisterStreamConsumerRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f);
void DoDataStreamsDeregisterStreamConsumerRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f);
void DoDataStreamsDescribeStreamConsumerRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f);
void DoDataStreamsListStreamsRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f);
void DoDataStreamsListShardsRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f);
void DoDataStreamsPutRecordsRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f);
void DoDataStreamsGetRecordsRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f);
void DoDataStreamsGetShardIteratorRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f);
void DoDataStreamsSubscribeToShardRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f);
void DoDataStreamsDescribeLimitsRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f);
void DoDataStreamsDescribeStreamSummaryRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f);
void DoDataStreamsDecreaseStreamRetentionPeriodRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f);
void DoDataStreamsIncreaseStreamRetentionPeriodRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f);
void DoDataStreamsUpdateShardCountRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f);
void DoDataStreamsUpdateStreamModeRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f);
void DoDataStreamsListStreamConsumersRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f);
void DoDataStreamsAddTagsToStreamRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f);
void DoDataStreamsDisableEnhancedMonitoringRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f);
void DoDataStreamsEnableEnhancedMonitoringRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f);
void DoDataStreamsListTagsForStreamRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f);
void DoDataStreamsUpdateStreamRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f);
void DoDataStreamsSetWriteQuotaRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f);
void DoDataStreamsMergeShardsRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f);
void DoDataStreamsRemoveTagsFromStreamRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f);
void DoDataStreamsSplitShardRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f);
void DoDataStreamsStartStreamEncryptionRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f);
void DoDataStreamsStopStreamEncryptionRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f);

}
}
