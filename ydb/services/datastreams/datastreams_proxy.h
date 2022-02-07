#pragma once

#include "events.h"

#include <ydb/core/client/server/grpc_base.h>
#include <ydb/core/grpc_services/rpc_calls.h>

#include <library/cpp/grpc/server/grpc_server.h>

#include <library/cpp/actors/core/actor_bootstrapped.h>
#include <library/cpp/actors/core/actorsystem.h>

namespace NKikimr::NDataStreams::V1 {

    inline TActorId GetDataStreamsServiceActorID() {
        return TActorId(0, "PqDsProxy");
    }

    IActor* CreateDataStreamsService(TIntrusivePtr<NMonitoring::TDynamicCounters> counters, TActorId newSchemeCache);

    class TDataStreamsService : public NActors::TActorBootstrapped<TDataStreamsService> {
    public:
        TDataStreamsService(TIntrusivePtr<NMonitoring::TDynamicCounters> counters, TActorId newSchemeCache);

        void Bootstrap(const TActorContext& ctx);

    private:
        STFUNC(StateFunc) {
            switch (ev->GetTypeRewrite()) {
                HFunc(NKikimr::NGRpcService::TEvDataStreamsCreateStreamRequest, Handle);
                HFunc(NKikimr::NGRpcService::TEvDataStreamsPutRecordRequest, Handle);
                HFunc(NKikimr::NGRpcService::TEvDataStreamsDeleteStreamRequest, Handle);
                HFunc(NKikimr::NGRpcService::TEvDataStreamsDescribeStreamRequest, Handle);
                HFunc(NKikimr::NGRpcService::TEvDataStreamsRegisterStreamConsumerRequest, Handle);
                HFunc(NKikimr::NGRpcService::TEvDataStreamsDeregisterStreamConsumerRequest, Handle);
                HFunc(NKikimr::NGRpcService::TEvDataStreamsDescribeStreamConsumerRequest, Handle);
                HFunc(NKikimr::NGRpcService::TEvDataStreamsListStreamsRequest, Handle);
                HFunc(NKikimr::NGRpcService::TEvDataStreamsListShardsRequest, Handle);
                HFunc(NKikimr::NGRpcService::TEvDataStreamsPutRecordsRequest, Handle);
                HFunc(NKikimr::NGRpcService::TEvDataStreamsGetRecordsRequest, Handle);
                HFunc(NKikimr::NGRpcService::TEvDataStreamsGetShardIteratorRequest, Handle);
                HFunc(NKikimr::NGRpcService::TEvDataStreamsSubscribeToShardRequest, Handle);
                HFunc(NKikimr::NGRpcService::TEvDataStreamsDescribeLimitsRequest, Handle);
                HFunc(NKikimr::NGRpcService::TEvDataStreamsDescribeStreamSummaryRequest, Handle);
                HFunc(NKikimr::NGRpcService::TEvDataStreamsDecreaseStreamRetentionPeriodRequest, Handle);
                HFunc(NKikimr::NGRpcService::TEvDataStreamsIncreaseStreamRetentionPeriodRequest, Handle);
                HFunc(NKikimr::NGRpcService::TEvDataStreamsUpdateShardCountRequest, Handle);
                HFunc(NKikimr::NGRpcService::TEvDataStreamsListStreamConsumersRequest, Handle);
                HFunc(NKikimr::NGRpcService::TEvDataStreamsAddTagsToStreamRequest, Handle);
                HFunc(NKikimr::NGRpcService::TEvDataStreamsDisableEnhancedMonitoringRequest, Handle);
                HFunc(NKikimr::NGRpcService::TEvDataStreamsEnableEnhancedMonitoringRequest, Handle);
                HFunc(NKikimr::NGRpcService::TEvDataStreamsListTagsForStreamRequest, Handle);
                HFunc(NKikimr::NGRpcService::TEvDataStreamsMergeShardsRequest, Handle);
                HFunc(NKikimr::NGRpcService::TEvDataStreamsRemoveTagsFromStreamRequest, Handle);
                HFunc(NKikimr::NGRpcService::TEvDataStreamsSplitShardRequest, Handle);
                HFunc(NKikimr::NGRpcService::TEvDataStreamsStartStreamEncryptionRequest, Handle);
                HFunc(NKikimr::NGRpcService::TEvDataStreamsStopStreamEncryptionRequest, Handle);
                HFunc(NKikimr::NGRpcService::TEvDataStreamsUpdateStreamRequest, Handle);
            }
        }

        void Handle(NKikimr::NGRpcService::TEvDataStreamsCreateStreamRequest::TPtr& ev, const TActorContext& ctx);
        void Handle(NKikimr::NGRpcService::TEvDataStreamsDeleteStreamRequest::TPtr& ev, const TActorContext& ctx);
        void Handle(NKikimr::NGRpcService::TEvDataStreamsDescribeStreamRequest::TPtr& ev, const TActorContext& ctx);
        void Handle(NKikimr::NGRpcService::TEvDataStreamsPutRecordRequest::TPtr& ev, const TActorContext& ctx);
        void Handle(NKikimr::NGRpcService::TEvDataStreamsRegisterStreamConsumerRequest::TPtr& ev, const TActorContext& ctx);
        void Handle(NKikimr::NGRpcService::TEvDataStreamsDeregisterStreamConsumerRequest::TPtr& ev, const TActorContext& ctx);
        void Handle(NKikimr::NGRpcService::TEvDataStreamsDescribeStreamConsumerRequest::TPtr& ev, const TActorContext& ctx);
        void Handle(NKikimr::NGRpcService::TEvDataStreamsListStreamsRequest::TPtr& ev, const TActorContext& ctx);
        void Handle(NKikimr::NGRpcService::TEvDataStreamsListShardsRequest::TPtr& ev, const TActorContext& ctx);
        void Handle(NKikimr::NGRpcService::TEvDataStreamsPutRecordsRequest::TPtr& ev, const TActorContext& ctx);
        void Handle(NKikimr::NGRpcService::TEvDataStreamsGetRecordsRequest::TPtr& ev, const TActorContext& ctx);
        void Handle(NKikimr::NGRpcService::TEvDataStreamsGetShardIteratorRequest::TPtr& ev, const TActorContext& ctx);
        void Handle(NKikimr::NGRpcService::TEvDataStreamsSubscribeToShardRequest::TPtr& ev, const TActorContext& ctx);
        void Handle(NKikimr::NGRpcService::TEvDataStreamsDescribeLimitsRequest::TPtr& ev, const TActorContext& ctx);
        void Handle(NKikimr::NGRpcService::TEvDataStreamsDescribeStreamSummaryRequest::TPtr& ev, const TActorContext& ctx);
        void Handle(NKikimr::NGRpcService::TEvDataStreamsDecreaseStreamRetentionPeriodRequest::TPtr& ev, const TActorContext& ctx);
        void Handle(NKikimr::NGRpcService::TEvDataStreamsIncreaseStreamRetentionPeriodRequest::TPtr& ev, const TActorContext& ctx);
        void Handle(NKikimr::NGRpcService::TEvDataStreamsUpdateShardCountRequest::TPtr& ev, const TActorContext& ctx);
        void Handle(NKikimr::NGRpcService::TEvDataStreamsUpdateStreamRequest::TPtr& ev, const TActorContext& ctx);
        void Handle(NKikimr::NGRpcService::TEvDataStreamsListStreamConsumersRequest::TPtr& ev, const TActorContext& ctx);
        void Handle(NKikimr::NGRpcService::TEvDataStreamsAddTagsToStreamRequest::TPtr& ev, const TActorContext& ctx);
        void Handle(NKikimr::NGRpcService::TEvDataStreamsDisableEnhancedMonitoringRequest::TPtr& ev, const TActorContext& ctx);
        void Handle(NKikimr::NGRpcService::TEvDataStreamsEnableEnhancedMonitoringRequest::TPtr& ev, const TActorContext& ctx);
        void Handle(NKikimr::NGRpcService::TEvDataStreamsListTagsForStreamRequest::TPtr& ev, const TActorContext& ctx);
        void Handle(NKikimr::NGRpcService::TEvDataStreamsMergeShardsRequest::TPtr& ev, const TActorContext& ctx);
        void Handle(NKikimr::NGRpcService::TEvDataStreamsRemoveTagsFromStreamRequest::TPtr& ev, const TActorContext& ctx);
        void Handle(NKikimr::NGRpcService::TEvDataStreamsSplitShardRequest::TPtr& ev, const TActorContext& ctx);
        void Handle(NKikimr::NGRpcService::TEvDataStreamsStartStreamEncryptionRequest::TPtr& ev, const TActorContext& ctx);
        void Handle(NKikimr::NGRpcService::TEvDataStreamsStopStreamEncryptionRequest::TPtr& ev, const TActorContext& ctx);

    private:
        TIntrusivePtr<NMonitoring::TDynamicCounters> Counters;

        TActorId NewSchemeCache;
    };

}
