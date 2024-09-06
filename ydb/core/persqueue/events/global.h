#pragma once
#include <ydb/core/keyvalue/defs.h>
#include <ydb/core/tablet/tablet_counters.h>

#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/actorid.h>
#include <ydb/core/base/blobstorage.h>
#include <ydb/core/protos/msgbus.pb.h>
#include <ydb/core/protos/msgbus_pq.pb.h>
#include <ydb/core/tx/datashard/datashard.h>
#include <ydb/public/api/protos/draft/persqueue_common.pb.h>

#include <ydb/core/protos/pqconfig.pb.h>

namespace NKikimr {

struct TEvPersQueue {
    enum EEv {
        EvRequest = EventSpaceBegin(TKikimrEvents::ES_PQ),
        EvUpdateConfig, //change config for all partitions and count of partitions
        EvUpdateConfigResponse,
        EvOffsets, //get offsets from all partitions in order 0..n-1 - it's for scheemeshard to change (TabletId,PartId) to Partition
        EvOffsetsResponse,
        EvDropTablet,
        EvDropTabletResult,
        EvStatus,
        EvStatusResponse,
        EvHasDataInfo, //how much data is available to fetch from partition
        EvHasDataInfoResponse,
        EvPartitionClientInfo,
        EvPartitionClientInfoResponse,
        EvUpdateBalancerConfig,
        EvRegisterReadSession,
        EvLockPartition,
        EvReleasePartition,
        EvPartitionReleased,
        EvDescribe,
        EvDescribeResponse,
        EvGetReadSessionsInfo,
        EvReadSessionsInfoResponse,
        EvWakeupClient, // deprecated
        EvUpdateACL,
        EvCheckACL,
        EvCheckACLResponse,
        EvError,
        EvGetPartitionIdForWrite,
        EvGetPartitionIdForWriteResponse,
        EvReportPartitionError,
        EvProposeTransaction,
        EvProposeTransactionResult,
        EvCancelTransactionProposal,
        EvPeriodicTopicStats,
        EvGetPartitionsLocation,
        EvGetPartitionsLocationResponse,
        EvReadingPartitionFinished,
        EvReadingPartitionStarted,
        EvResponse = EvRequest + 256,
        EvInternalEvents = EvResponse + 256,
        EvEnd
    };

    static_assert(
        EvEnd < EventSpaceEnd(TKikimrEvents::ES_PQ),
        "expect EvEnd < EventSpaceEnd(TKikimrEvents::ES_PQ)");

    struct TEvRequest : public TEventPB<TEvRequest,
            NKikimrClient::TPersQueueRequest, EvRequest> {
        TEvRequest() {}
    };

    struct TEvResponse: public TEventPB<TEvResponse,
            NKikimrClient::TResponse, EvResponse> {
        TEvResponse() {}
    };

    struct TEvUpdateConfig: public TEventPB<TEvUpdateConfig,
            NKikimrPQ::TUpdateConfig, EvUpdateConfig> {
            TEvUpdateConfig() {}
    };

    struct TEvUpdateBalancerConfig: public TEventPB<TEvUpdateBalancerConfig,
            NKikimrPQ::TUpdateBalancerConfig, EvUpdateBalancerConfig> {
            TEvUpdateBalancerConfig() {}
    };

    struct TEvRegisterReadSession: public TEventPB<TEvRegisterReadSession,
            NKikimrPQ::TRegisterReadSession, EvRegisterReadSession> {
            TEvRegisterReadSession() {}
    };

    struct TEvGetReadSessionsInfo: public TEventPB<TEvGetReadSessionsInfo,
            NKikimrPQ::TGetReadSessionsInfo, EvGetReadSessionsInfo> {
            explicit TEvGetReadSessionsInfo(const TString& consumer = "") {
                if (!consumer.empty()) {
                    Record.SetClientId(consumer);
                }
            }
            explicit TEvGetReadSessionsInfo(const TVector<ui32>& partitions) {
                if (!partitions.empty()) {
                    for (auto p: partitions) {
                        Record.AddPartitions(p);
                    }
                }
            }
    };

    struct TEvReadSessionsInfoResponse: public TEventPB<TEvReadSessionsInfoResponse,
            NKikimrPQ::TReadSessionsInfoResponse, EvReadSessionsInfoResponse> {
            TEvReadSessionsInfoResponse() {}
    };

    struct TEvGetPartitionsLocation: public TEventPB<TEvGetPartitionsLocation,
            NKikimrPQ::TGetPartitionsLocation, EvGetPartitionsLocation> {
            TEvGetPartitionsLocation(const TVector<ui64>& partitionIds = {}) {
                for (const auto& p : partitionIds) {
                    Record.AddPartitions(p);
                }
            }
    };

    struct TEvGetPartitionsLocationResponse: public TEventPB<TEvGetPartitionsLocationResponse,
            NKikimrPQ::TPartitionsLocationResponse, EvGetPartitionsLocationResponse> {
    };

    struct TEvLockPartition : public TEventPB<TEvLockPartition,
            NKikimrPQ::TLockPartition, EvLockPartition> {
            TEvLockPartition() {}
    };

    struct TEvReleasePartition : public TEventPB<TEvReleasePartition,
            NKikimrPQ::TReleasePartition, EvReleasePartition> {
            TEvReleasePartition() {}
    };

    struct TEvPartitionReleased : public TEventPB<TEvPartitionReleased,
            NKikimrPQ::TPartitionReleased, EvPartitionReleased> {
            TEvPartitionReleased() {}
    };

    struct TEvUpdateConfigResponse: public TEventPB<TEvUpdateConfigResponse,
            NKikimrPQ::TUpdateConfigResponse, EvUpdateConfigResponse> {
        TEvUpdateConfigResponse() {}

        ui64 GetOrigin() const {
            return Record.GetOrigin();
        }
    };

    struct TEvOffsets : public TEventPB<TEvOffsets,
            NKikimrPQ::TOffsets, EvOffsets> {
        TEvOffsets() {}
    };

    struct TEvOffsetsResponse : public TEventPB<TEvOffsetsResponse,
            NKikimrPQ::TOffsetsResponse, EvOffsetsResponse> {
        TEvOffsetsResponse() {}
    };

    struct TEvStatus : public TEventPB<TEvStatus,
            NKikimrPQ::TStatus, EvStatus> {
        explicit TEvStatus(const TString& consumer = "", bool getStatForAllConsumers = false) {
            if (!consumer.empty())
                Record.SetClientId(consumer);
            if (getStatForAllConsumers)
                Record.SetGetStatForAllConsumers(true);
        }
    };

    struct TEvStatusResponse : public TEventPB<TEvStatusResponse,
            NKikimrPQ::TStatusResponse, EvStatusResponse> {
        TEvStatusResponse() {}
    };

    struct TEvHasDataInfo : public TEventPB<TEvHasDataInfo,
            NKikimrPQ::THasDataInfo, EvHasDataInfo> {
        TEvHasDataInfo() {}
    };

    struct TEvHasDataInfoResponse : public TEventPB<TEvHasDataInfoResponse,
            NKikimrPQ::THasDataInfoResponse, EvHasDataInfoResponse> {
        TEvHasDataInfoResponse() {}
    };


    struct TEvDropTablet : public TEventPB<TEvDropTablet, NKikimrPQ::TDropTablet, EvDropTablet> {
        TEvDropTablet()
        {}
    };

    struct TEvDropTabletReply : public TEventPB<TEvDropTabletReply, NKikimrPQ::TDropTabletResult,  EvDropTabletResult> {
        TEvDropTabletReply()
        {}
    };

    struct TEvPartitionClientInfo : TEventPB<TEvPartitionClientInfo, NKikimrPQ::TPartitionClientInfo, EvPartitionClientInfo> {
        TEvPartitionClientInfo() = default;
    };

    struct TEvPartitionClientInfoResponse : TEventPB<TEvPartitionClientInfoResponse, NKikimrPQ::TClientInfoResponse, EvPartitionClientInfoResponse> {
        TEvPartitionClientInfoResponse() = default;
    };

    struct TEvDescribe : public TEventPB<TEvDescribe, NKikimrPQ::TDescribe, EvDescribe> {
        TEvDescribe()
        {}
    };

    struct TEvDescribeResponse : public TEventPB<TEvDescribeResponse, NKikimrPQ::TDescribeResponse, EvDescribeResponse> {
        TEvDescribeResponse()
        {}
    };

    struct TEvUpdateACL : public TEventLocal<TEvUpdateACL, EvUpdateACL> {
        TEvUpdateACL()
        {}
    };

    struct TEvCheckACL : public TEventPB<TEvCheckACL, NKikimrPQ::TCheckACL, EvCheckACL> {
        TEvCheckACL()
        {}
    };

    struct TEvCheckACLResponse : public TEventPB<TEvCheckACLResponse, NKikimrPQ::TCheckACLResponse, EvCheckACLResponse> {
        TEvCheckACLResponse()
        {};
    };

    struct TEvError : public TEventPB<TEvError,
            NPersQueueCommon::TError, EvError> {
            TEvError() {}
    };

    struct TEvGetPartitionIdForWrite : public TEventPB<TEvGetPartitionIdForWrite, NKikimrPQ::TGetPartitionIdForWrite, EvGetPartitionIdForWrite> {
        TEvGetPartitionIdForWrite()
        {}
    };

    struct TEvGetPartitionIdForWriteResponse : public TEventPB<TEvGetPartitionIdForWriteResponse, NKikimrPQ::TGetPartitionIdForWriteResponse, EvGetPartitionIdForWriteResponse> {
        TEvGetPartitionIdForWriteResponse()
        {};
    };

    struct TEvReportPartitionError : public TEventPB<TEvReportPartitionError, NKikimrPQ::TStatusResponse::TErrorMessage, EvReportPartitionError> {
        TEvReportPartitionError()
        {}
    };

    struct TEvProposeTransaction : public TEventPB<TEvProposeTransaction, NKikimrPQ::TEvProposeTransaction, EvProposeTransaction> {
    };

    struct TEvProposeTransactionResult : public TEventPB<TEvProposeTransactionResult, NKikimrPQ::TEvProposeTransactionResult, EvProposeTransactionResult> {
    };

    struct TEvCancelTransactionProposal : public TEventPB<TEvCancelTransactionProposal, NKikimrPQ::TEvCancelTransactionProposal, EvCancelTransactionProposal> {
        TEvCancelTransactionProposal() = default;

        explicit TEvCancelTransactionProposal(ui64 txId) {
            Record.SetTxId(txId);
        }
    };

    struct TEvPeriodicTopicStats : public TEventPB<TEvPeriodicTopicStats, NKikimrPQ::TEvPeriodicTopicStats, EvPeriodicTopicStats> {
    };

    using TEvProposeTransactionAttach = TEvDataShard::TEvProposeTransactionAttach;
    using TEvProposeTransactionAttachResult = TEvDataShard::TEvProposeTransactionAttachResult;

    struct TEvReadingPartitionFinishedRequest : public TEventPB<TEvReadingPartitionFinishedRequest, NKikimrPQ::TEvReadingPartitionFinishedRequest, EvReadingPartitionFinished> {
        TEvReadingPartitionFinishedRequest() = default;

        TEvReadingPartitionFinishedRequest(const TString& consumer, ui32 partitionId, bool scaleAwareSDK, bool startedReadingFromEndOffset) {
            Record.SetConsumer(consumer);
            Record.SetPartitionId(partitionId);
            Record.SetScaleAwareSDK(scaleAwareSDK);
            Record.SetStartedReadingFromEndOffset(startedReadingFromEndOffset);
        }
    };

    struct TEvReadingPartitionStartedRequest : public TEventPB<TEvReadingPartitionStartedRequest, NKikimrPQ::TEvReadingPartitionStartedRequest, EvReadingPartitionStarted> {
        TEvReadingPartitionStartedRequest() = default;

        TEvReadingPartitionStartedRequest(const TString& consumer, ui32 partitionId) {
            Record.SetConsumer(consumer);
            Record.SetPartitionId(partitionId);
        }
    };

};
} //NKikimr
