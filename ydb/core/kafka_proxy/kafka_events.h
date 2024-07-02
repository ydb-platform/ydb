#pragma once

#include <ydb/library/actors/core/event_local.h>
#include <ydb/core/base/events.h>
#include <ydb/services/persqueue_v1/actors/events.h>

#include "kafka_messages.h"
#include <ydb/library/aclib/aclib.h>
#include "actors/actors.h"

using namespace NActors;

namespace NKafka {

struct TEvKafka {
    enum EEv {
        EvRequest = EventSpaceBegin(NKikimr::TKikimrEvents::TKikimrEvents::ES_KAFKA),
        EvProduceRequest,
        EvAuthResult,
        EvHandshakeResult,
        EvWakeup,
        EvUpdateCounter,
        EvUpdateHistCounter,
        EvTopicOffsetsResponse,
        EvJoinGroupRequest,
        EvSyncGroupRequest,
        EvHeartbeatRequest,
        EvLeaveGroupRequest,
        EvKillReadSession,
        EvCommitedOffsetsResponse,
        EvCreateTopicsResponse,
        EvReadSessionInfo,
        EvResponse = EvRequest + 256,
        EvInternalEvents = EvResponse + 256,
        EvEnd
    };

    static_assert(
        EvEnd < EventSpaceEnd(NKikimr::TKikimrEvents::TKikimrEvents::ES_KAFKA),
        "expect EvEnd < EventSpaceEnd(TKikimrEvents::ES_KAFKA)");


    struct TEvProduceRequest : public TEventLocal<TEvProduceRequest, EvProduceRequest> {
        TEvProduceRequest(const ui64 correlationId, const TMessagePtr<TProduceRequestData>& request)
        : CorrelationId(correlationId)
        , Request(request)
        {}

        ui64 CorrelationId;
        const TMessagePtr<TProduceRequestData> Request;
    };

    struct TEvJoinGroupRequest : public TEventLocal<TEvJoinGroupRequest, EvJoinGroupRequest> {
        TEvJoinGroupRequest(const ui64 correlationId, const TMessagePtr<TJoinGroupRequestData>& request)
        : CorrelationId(correlationId)
        , Request(request)
        {}

        ui64 CorrelationId;
        const TMessagePtr<TJoinGroupRequestData> Request;
    };

    struct TEvLeaveGroupRequest : public TEventLocal<TEvLeaveGroupRequest, EvLeaveGroupRequest> {
        TEvLeaveGroupRequest(const ui64 correlationId, const TMessagePtr<TLeaveGroupRequestData>& request)
        : CorrelationId(correlationId)
        , Request(request)
        {}

        ui64 CorrelationId;
        const TMessagePtr<TLeaveGroupRequestData> Request;
    };

    struct TEvSyncGroupRequest : public TEventLocal<TEvSyncGroupRequest, EvSyncGroupRequest> {
        TEvSyncGroupRequest(const ui64 correlationId, const TMessagePtr<TSyncGroupRequestData>& request)
        : CorrelationId(correlationId)
        , Request(request)
        {}

        ui64 CorrelationId;
        const TMessagePtr<TSyncGroupRequestData> Request;
    };

    struct TEvHeartbeatRequest : public TEventLocal<TEvHeartbeatRequest, EvHeartbeatRequest> {
        TEvHeartbeatRequest(const ui64 correlationId, const TMessagePtr<THeartbeatRequestData>& request)
        : CorrelationId(correlationId)
        , Request(request)
        {}

        ui64 CorrelationId;
        const TMessagePtr<THeartbeatRequestData> Request;
    };

    struct TEvResponse : public TEventLocal<TEvResponse, EvResponse> {
        TEvResponse(const ui64 correlationId, const TApiMessage::TPtr response, EKafkaErrors errorCode)
            : CorrelationId(correlationId)
            , Response(std::move(response))
            , ErrorCode(errorCode) {
        }

        const ui64 CorrelationId;
        const TApiMessage::TPtr Response;
        const EKafkaErrors ErrorCode;
    };

    struct TEvAuthResult : public TEventLocal<TEvAuthResult, EvAuthResult> {

        TEvAuthResult(EAuthSteps authStep, std::shared_ptr<TEvKafka::TEvResponse> clientResponse, TString error = "")
            : AuthStep(authStep)
            , Error(error)
            , ClientResponse(clientResponse) {
        }

        TEvAuthResult(EAuthSteps authStep, std::shared_ptr<TEvKafka::TEvResponse> clientResponse, TIntrusiveConstPtr<NACLib::TUserToken> token, TString databasePath, TString databaseId,
                      TString folderId, TString cloudId, TString serviceAccountId, TString coordinator, TString resourcePath, bool isServerless, TString error = "")
            : AuthStep(authStep)
            , UserToken(token)
            , DatabasePath(databasePath)
            , CloudId(cloudId)
            , FolderId(folderId)
            , ServiceAccountId(serviceAccountId)
            , DatabaseId(databaseId)
            , Coordinator(coordinator)
            , ResourcePath(resourcePath)
            , IsServerless(isServerless)
            , Error(error)
            , ClientResponse(std::move(clientResponse)) {
        }

        EAuthSteps AuthStep;
        TIntrusiveConstPtr<NACLib::TUserToken> UserToken;
        TString DatabasePath;
        TString CloudId;
        TString FolderId;
        TString ServiceAccountId;
        TString DatabaseId;
        TString Coordinator;
        TString ResourcePath;
        bool IsServerless;

        TString Error;
        TString SaslMechanism;
        std::shared_ptr<TEvKafka::TEvResponse> ClientResponse;
    };

    struct TEvHandshakeResult : public TEventLocal<TEvHandshakeResult, EvHandshakeResult> {
        TEvHandshakeResult(EAuthSteps authStep, std::shared_ptr<TEvKafka::TEvResponse> clientResponse, TString saslMechanism, TString error = "")
        : AuthStep(authStep),
          Error(error),
          SaslMechanism(saslMechanism),
          ClientResponse(std::move(clientResponse))
        {}

        EAuthSteps AuthStep;
        TString Error;
        TString SaslMechanism;
        std::shared_ptr<TEvKafka::TEvResponse> ClientResponse;
    };

    struct TEvUpdateCounter : public TEventLocal<TEvUpdateCounter, EvUpdateCounter> {
        i64 Delta;
        TVector<std::pair<TString, TString>> Labels;

        TEvUpdateCounter(const i64 delta, const TVector<std::pair<TString, TString>> labels)
        : Delta(delta)
        , Labels(labels)
        {}
    };

    struct TEvReadSessionInfo : public TEventLocal<TEvReadSessionInfo, EvReadSessionInfo> {
        TEvReadSessionInfo(const TString& groupId)
        : GroupId(groupId)
        {}

        TString GroupId;
    };

    struct TEvKillReadSession : public TEventLocal<TEvKillReadSession, EvKillReadSession> {};

    struct TEvUpdateHistCounter : public TEventLocal<TEvUpdateHistCounter, EvUpdateHistCounter> {
        i64 Value;
        ui64 Count;
        TVector<std::pair<TString, TString>> Labels;

        TEvUpdateHistCounter(const i64 value, const ui64 count, const TVector<std::pair<TString, TString>> labels)
        : Value(value)
        , Count(count)
        , Labels(labels)
        {}
    };

    struct TEvWakeup : public TEventLocal<TEvWakeup, EvWakeup> {
    };

struct TPartitionOffsetsInfo {
    ui64 PartitionId;
    ui64 Generation;
    ui64 StartOffset;
    ui64 EndOffset;
};

struct TGetOffsetsRequest : public NKikimr::NGRpcProxy::V1::TLocalRequestBase {
    TGetOffsetsRequest() = default;
    TGetOffsetsRequest(const TString& topic, const TString& database, const TString& token, const TVector<ui32>& partitionIds)
        : TLocalRequestBase(topic, database, token)
        , PartitionIds(partitionIds)
    {}

    TVector<ui32> PartitionIds;
};

struct TEvTopicOffsetsResponse : public NActors::TEventLocal<TEvTopicOffsetsResponse, EvTopicOffsetsResponse> 
                           , public NKikimr::NGRpcProxy::V1::TEvPQProxy::TLocalResponseBase
{
    TEvTopicOffsetsResponse()
    {}

    TVector<TPartitionOffsetsInfo> Partitions;
};

struct TEvCommitedOffsetsResponse : public NActors::TEventLocal<TEvCommitedOffsetsResponse, EvTopicOffsetsResponse> 
                           , public NKikimr::NGRpcProxy::V1::TEvPQProxy::TLocalResponseBase
{
    TEvCommitedOffsetsResponse()
    {}

    TString TopicName;
    EKafkaErrors Status;
    std::shared_ptr<std::unordered_map<ui32, std::unordered_map<TString, ui32>>> PartitionIdToOffsets;
};

struct TEvTopicModificationResponse : public NActors::TEventLocal<TEvTopicModificationResponse, EvCreateTopicsResponse> 
                           , public NKikimr::NGRpcProxy::V1::TEvPQProxy::TLocalResponseBase
{
    enum EStatus {
        OK,
        ERROR,
        BAD_REQUEST,
        INVALID_CONFIG,
        TOPIC_DOES_NOT_EXIST,
    };

    TEvTopicModificationResponse()
    {}

    TString TopicPath;
    EKafkaErrors Status;
    TString Message;
};
};

} // namespace NKafka
