#pragma once

#include <ydb/core/base/path.h>
#include <ydb/core/persqueue/pq_rl_helpers.h>
#include <ydb/core/protos/config.pb.h>
#include <ydb/library/aclib/aclib.h>
#include <ydb/public/api/protos/persqueue_error_codes_v1.pb.h>
#include <ydb/public/api/protos/draft/persqueue_error_codes.pb.h> // strange

#include <ydb/core/kafka_proxy/kafka_messages.h>

namespace NKafka {

static constexpr int ProxyNodeId = 1;
static constexpr char UnderlayPrefix[] = "u-";

static_assert(sizeof(UnderlayPrefix) == 3);

enum EAuthSteps {
    WAIT_HANDSHAKE,
    WAIT_AUTH,
    SUCCESS,
    FAILED
};

struct TContext {
    using TPtr = std::shared_ptr<TContext>;

    TContext(const NKikimrConfig::TKafkaProxyConfig& config)
        : Config(config) {
    }

    const NKikimrConfig::TKafkaProxyConfig& Config;

    TActorId ConnectionId;
    TString KafkaClient;


    EAuthSteps AuthenticationStep = EAuthSteps::WAIT_HANDSHAKE;
    TString SaslMechanism;

    TString GroupId;
    TString DatabasePath;
    TString FolderId;
    TString CloudId;
    TString DatabaseId;
    TIntrusiveConstPtr<NACLib::TUserToken> UserToken;
    TString ClientDC;
    bool IsServerless = false;

    NKikimr::NPQ::TRlContext RlContext;

    bool Authenticated() { return AuthenticationStep == SUCCESS; }
};

template<std::derived_from<TApiMessage> T>
class TMessagePtr {
public:
    TMessagePtr(const std::shared_ptr<TBuffer>& buffer, const std::shared_ptr<TApiMessage>& message)
        : Buffer(buffer)
        , Message(message)
        , Ptr(dynamic_cast<T*>(message.get())) {
    }

    template<std::derived_from<TApiMessage> O>
    TMessagePtr<O> Cast() {
        return TMessagePtr<O>(Buffer, Message);
    }

    T* operator->() const {
        return Ptr;
    }

    operator bool() const {
        return nullptr != Ptr;
    }

private:
    const std::shared_ptr<TBuffer> Buffer;
    const std::shared_ptr<TApiMessage> Message;
    T* Ptr;
};

inline bool RequireAuthentication(EApiKey apiKey) {
    return !(EApiKey::API_VERSIONS == apiKey || EApiKey::SASL_HANDSHAKE == apiKey || EApiKey::SASL_AUTHENTICATE == apiKey);
}

inline EKafkaErrors ConvertErrorCode(Ydb::StatusIds::StatusCode status) {
    switch (status) {
        case Ydb::StatusIds::SUCCESS:
            return EKafkaErrors::NONE_ERROR;
        case Ydb::StatusIds::BAD_REQUEST:
            return EKafkaErrors::INVALID_REQUEST;
        case Ydb::StatusIds::SCHEME_ERROR:
            return EKafkaErrors::UNKNOWN_TOPIC_OR_PARTITION;
        case Ydb::StatusIds::UNAUTHORIZED:
            return EKafkaErrors::TOPIC_AUTHORIZATION_FAILED;
        default:
            return EKafkaErrors::UNKNOWN_SERVER_ERROR;
    }
}

inline EKafkaErrors ConvertErrorCode(NPersQueue::NErrorCode::EErrorCode code) {
    switch (code) {
        case NPersQueue::NErrorCode::EErrorCode::OK:
            return EKafkaErrors::NONE_ERROR;
        case NPersQueue::NErrorCode::EErrorCode::BAD_REQUEST:
            return EKafkaErrors::INVALID_REQUEST;
        case NPersQueue::NErrorCode::EErrorCode::READ_ERROR_TOO_SMALL_OFFSET:
            return EKafkaErrors::OFFSET_OUT_OF_RANGE;
        case NPersQueue::NErrorCode::EErrorCode::READ_ERROR_TOO_BIG_OFFSET:
            return EKafkaErrors::OFFSET_OUT_OF_RANGE;
        case NPersQueue::NErrorCode::EErrorCode::UNKNOWN_TOPIC:
            return EKafkaErrors::UNKNOWN_TOPIC_OR_PARTITION;
        case NPersQueue::NErrorCode::EErrorCode::ACCESS_DENIED:
            return EKafkaErrors::TOPIC_AUTHORIZATION_FAILED;
        case NPersQueue::NErrorCode::EErrorCode::WRONG_PARTITION_NUMBER:
            return EKafkaErrors::UNKNOWN_TOPIC_OR_PARTITION;
        case NPersQueue::NErrorCode::EErrorCode::READ_TIMEOUT:
            return EKafkaErrors::REQUEST_TIMED_OUT;
        default:
            return EKafkaErrors::UNKNOWN_SERVER_ERROR;
    }
}

inline EKafkaErrors ConvertErrorCode(Ydb::PersQueue::ErrorCode::ErrorCode code) {
    switch (code) {
        case Ydb::PersQueue::ErrorCode::ErrorCode::OK:
            return EKafkaErrors::NONE_ERROR;
        case Ydb::PersQueue::ErrorCode::ErrorCode::BAD_REQUEST:
            return EKafkaErrors::INVALID_REQUEST;
        case Ydb::PersQueue::ErrorCode::ErrorCode::ERROR:
            return EKafkaErrors::UNKNOWN_SERVER_ERROR;
        case Ydb::PersQueue::ErrorCode::ErrorCode::UNKNOWN_TOPIC:
            return EKafkaErrors::UNKNOWN_TOPIC_OR_PARTITION;
        case Ydb::PersQueue::ErrorCode::ErrorCode::ACCESS_DENIED:
            return EKafkaErrors::TOPIC_AUTHORIZATION_FAILED;
        case Ydb::PersQueue::ErrorCode::ErrorCode::SET_OFFSET_ERROR_COMMIT_TO_FUTURE:
        case Ydb::PersQueue::ErrorCode::ErrorCode::SET_OFFSET_ERROR_COMMIT_TO_PAST:
            return EKafkaErrors::OFFSET_OUT_OF_RANGE;
        default:
            return EKafkaErrors::UNKNOWN_SERVER_ERROR;
    }
}

inline TString NormalizePath(const TString& database, const TString& topic) {
    if (topic.Size() > database.Size() && topic.at(database.Size()) == '/' && topic.StartsWith(database)) {
        return topic;
    }
    return NKikimr::CanonizePath(database + "/" + topic);
}

inline TString GetTopicNameWithoutDb(const TString& database, TString topic) {
    auto topicWithDb = NormalizePath(database, topic);
    topic = topicWithDb.substr(database.Size()+1);
    return topic;
}

NActors::IActor* CreateKafkaApiVersionsActor(const TContext::TPtr context, const ui64 correlationId, const TMessagePtr<TApiVersionsRequestData>& message);
NActors::IActor* CreateKafkaInitProducerIdActor(const TContext::TPtr context, const ui64 correlationId, const TMessagePtr<TInitProducerIdRequestData>& message);
NActors::IActor* CreateKafkaMetadataActor(const TContext::TPtr context, const ui64 correlationId, const TMessagePtr<TMetadataRequestData>& message);
NActors::IActor* CreateKafkaProduceActor(const TContext::TPtr context);
NActors::IActor* CreateKafkaReadSessionActor(const TContext::TPtr context, ui64 cookie);
NActors::IActor* CreateKafkaSaslHandshakeActor(const TContext::TPtr context, const ui64 correlationId, const TMessagePtr<TSaslHandshakeRequestData>& message);
NActors::IActor* CreateKafkaSaslAuthActor(const TContext::TPtr context, const ui64 correlationId, const NKikimr::NRawSocket::TSocketDescriptor::TSocketAddressType address, const TMessagePtr<TSaslAuthenticateRequestData>& message);
NActors::IActor* CreateKafkaListOffsetsActor(const TContext::TPtr context, const ui64 correlationId, const TMessagePtr<TListOffsetsRequestData>& message);
NActors::IActor* CreateKafkaFetchActor(const TContext::TPtr context, const ui64 correlationId, const TMessagePtr<TFetchRequestData>& message);
NActors::IActor* CreateKafkaFindCoordinatorActor(const TContext::TPtr context, const ui64 correlationId, const TMessagePtr<TFindCoordinatorRequestData>& message);
NActors::IActor* CreateKafkaOffsetCommitActor(const TContext::TPtr context, const ui64 correlationId, const TMessagePtr<TOffsetCommitRequestData>& message);
NActors::IActor* CreateKafkaOffsetFetchActor(const TContext::TPtr context, const ui64 correlationId, const TMessagePtr<TOffsetFetchRequestData>& message);
NActors::IActor* CreateKafkaCreateTopicsActor(const TContext::TPtr context, const ui64 correlationId, const TMessagePtr<TCreateTopicsRequestData>& message);
NActors::IActor* CreateKafkaCreatePartitionsActor(const TContext::TPtr context, const ui64 correlationId, const TMessagePtr<TCreatePartitionsRequestData>& message);
NActors::IActor* CreateKafkaAlterConfigsActor(const TContext::TPtr context, const ui64 correlationId, const TMessagePtr<TAlterConfigsRequestData>& message);

} // namespace NKafka
