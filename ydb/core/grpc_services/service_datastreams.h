#pragma once
#include <memory>

namespace NActors {
struct TActorId;
}

namespace NKikimr {
namespace NGRpcService {

class IRequestOpCtx;
class IFacilityProvider;

void DoDataStreamsCreateStreamRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&, const NActors::TActorId& schemeCache);
void DoDataStreamsDeleteStreamRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&);
void DoDataStreamsDescribeStreamRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&);
void DoDataStreamsPutRecordRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&, const NActors::TActorId& schemeCache);
void DoDataStreamsRegisterStreamConsumerRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&);
void DoDataStreamsDeregisterStreamConsumerRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&);
void DoDataStreamsDescribeStreamConsumerRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&);
void DoDataStreamsListStreamsRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&, const NActors::TActorId& schemeCache);
void DoDataStreamsListShardsRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&, const NActors::TActorId& schemeCache);
void DoDataStreamsPutRecordsRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&, const NActors::TActorId& schemeCache);
void DoDataStreamsGetRecordsRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&, const NActors::TActorId& schemeCache);
void DoDataStreamsGetShardIteratorRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&, const NActors::TActorId& schemeCache);
void DoDataStreamsSubscribeToShardRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&);
void DoDataStreamsDescribeLimitsRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&);
void DoDataStreamsDescribeStreamSummaryRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&);
void DoDataStreamsDecreaseStreamRetentionPeriodRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&);
void DoDataStreamsIncreaseStreamRetentionPeriodRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&);
void DoDataStreamsUpdateShardCountRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&);
void DoDataStreamsListStreamConsumersRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&);
void DoDataStreamsAddTagsToStreamRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&);
void DoDataStreamsDisableEnhancedMonitoringRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&);
void DoDataStreamsEnableEnhancedMonitoringRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&);
void DoDataStreamsListTagsForStreamRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&);
void DoDataStreamsUpdateStreamRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&);
void DoDataStreamsSetWriteQuotaRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&);
void DoDataStreamsMergeShardsRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&);
void DoDataStreamsRemoveTagsFromStreamRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&);
void DoDataStreamsSplitShardRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&);
void DoDataStreamsStartStreamEncryptionRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&);
void DoDataStreamsStopStreamEncryptionRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&);

}
}
