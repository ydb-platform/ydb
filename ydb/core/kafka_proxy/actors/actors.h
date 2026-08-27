#pragma once

#include <ydb/core/raw_socket/sock_impl.h>
#include <ydb/core/base/path.h>
#include <ydb/core/base/ticket_parser.h>
#include <ydb/core/kafka_proxy/kafka_messages.h>
#include <ydb/core/persqueue/public/pq_rl_helpers.h>
#include <ydb/core/protos/config.pb.h>
#include <ydb/library/aclib/aclib.h>
#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/events.h>
#include <ydb/library/actors/core/log.h>
#include <ydb/library/services/services.pb.h>
#include <ydb/public/api/protos/persqueue_error_codes_v1.pb.h>
#include <ydb/public/api/protos/draft/persqueue_error_codes.pb.h> // strange

#include <util/datetime/base.h>
#include <util/generic/hash_set.h>
#include <util/system/backtrace.h>
#include <util/system/type_name.h>
#include <optional>

namespace NKafka {

template <typename TDerived>
class TKafkaExceptionHandler: public NActors::IActorExceptionHandler {
public:
    bool OnUnhandledException(const std::exception& exc) override {
        auto* self = static_cast<TDerived*>(this);
        const auto& ctx = self->ActorContext();
        YDB_LOG_CRIT_CTX_COMP(ctx, NKikimrServices::KAFKA_PROXY, "Unhandled exception in kafka actor",
            {"actor", TypeName<TDerived>()},
            {"typeName", TypeName(exc)},
            {"exception", exc.what()},
            {"backTrace", TBackTrace::FromCurrentException().PrintToString()});
        self->OnKafkaUnhandledException(exc, ctx);
        return true;
    }

    // Default: tear down the TCP connection when available so the client reconnects.
    // Override GetKafkaConnectionId() in actors that own a TContext.
    NActors::TActorId GetKafkaConnectionId() const {
        return {};
    }

    void OnKafkaUnhandledException(const std::exception&, const NActors::TActorContext& ctx) {
        auto* self = static_cast<TDerived*>(this);
        if (const auto connectionId = self->GetKafkaConnectionId()) {
            ctx.Send(connectionId, new NActors::TEvents::TEvPoison);
            return;
        }
        ctx.Send(self->SelfId(), new NActors::TEvents::TEvPoison);
    }
};

static constexpr int ProxyNodeId = 1;
static constexpr char UnderlayPrefix[] = "u-";

static_assert(sizeof(UnderlayPrefix) == 3);

enum EAuthSteps {
    WAIT_HANDSHAKE,
    WAIT_AUTH,
    SUCCESS,
    FAILED
};

enum class ETokenCheckStatus {
    Ok,
    Invalid,
    Unavailable
};

enum class EBalancingMode {
    Server,
    Native,
};

struct TReadSession {
    EBalancingMode BalancingMode = EBalancingMode::Native;
    std::optional<EBalancingMode> PendingBalancingMode;
    NActors::TActorId ProxyActorId;
};

struct TCredentials {
    TIntrusiveConstPtr<NACLib::TUserToken> UserToken;
    TString Ticket;
    TVector<NKikimr::TEvTicketParser::TEvAuthorizeTicket::TEntry> TicketParserEntries;
    TString AuthDatabasePath;
    TString PeerName;
    ETokenCheckStatus Status = ETokenCheckStatus::Ok;

    std::optional<EKafkaErrors> UnusableError() const {
        switch (Status) {
            case ETokenCheckStatus::Invalid:
                return EKafkaErrors::TOPIC_AUTHORIZATION_FAILED;
            case ETokenCheckStatus::Unavailable:
                return EKafkaErrors::BROKER_NOT_AVAILABLE;
            case ETokenCheckStatus::Ok:
                return std::nullopt;
        }
        return std::nullopt;
    }
};

struct TContext {
    using TPtr = std::shared_ptr<TContext>;

    TContext(const NKikimrConfig::TKafkaProxyConfig& config)
        : Config(config) {
    }

    TContext(const TContext& other)
        : Config(other.Config)
        , ConnectionId(other.ConnectionId)
        , KafkaClient(other.KafkaClient)
        , AuthenticationStep(other.AuthenticationStep)
        , SaslMechanism(other.SaslMechanism)
        , GroupId(other.GroupId)
        , DatabasePath(other.DatabasePath)
        , FolderId(other.FolderId)
        , CloudId(other.CloudId)
        , DatabaseId(other.DatabaseId)
        , ResourceDatabasePath(other.ResourceDatabasePath)
        , InitialServerlessTransactionsFlagValue(other.InitialServerlessTransactionsFlagValue)
        , Token(other.Token)
        , ClientDC(other.ClientDC)
        , IsServerless(other.IsServerless)
        , RequireAuthentication(other.RequireAuthentication)
        , RlContext(other.RlContext)
    {
    }

    const NKikimrConfig::TKafkaProxyConfig& Config;

    NActors::TActorId ConnectionId;
    TString KafkaClient;


    EAuthSteps AuthenticationStep = EAuthSteps::WAIT_HANDSHAKE;
    TString SaslMechanism;

    TString GroupId;
    TString DatabasePath;
    TString FolderId;
    TString CloudId;
    TString DatabaseId;
    TString ResourceDatabasePath;
    std::optional<bool> InitialServerlessTransactionsFlagValue;
    TCredentials Token;
    TString ClientDC;
    bool IsServerless = false;
    bool RequireAuthentication = false;
    TReadSession ReadSession;

    NKikimr::NPQ::TRlContext RlContext;

    THashSet<TString> TopicAclOk;

    bool Authenticated() {
        return !RequireAuthentication || AuthenticationStep == SUCCESS;
    }

    bool ShouldCheckTopicAcl() const {
        return RequireAuthentication || bool(Token.UserToken);
    }

    bool HasTopicAccess(const NACLib::TSecurityObject* securityObject, NACLib::EAccessRights rights) const {
        if (!ShouldCheckTopicAcl()) {
            return true;
        }
        if (!Token.UserToken || !securityObject) {
            return false;
        }
        return securityObject->CheckAccess(rights, *Token.UserToken);
    }

    TDuration TokenRecheckInterval() const {
        return TDuration::MilliSeconds(Config.GetTokenRecheckIntervalMs());
    }

    bool TokenRecheckEnabled() const {
        return Config.GetTokenRecheckIntervalMs() > 0 && !Token.Ticket.empty();
    }

    void RememberTopicAclOk(const TString& topic) {
        TopicAclOk.insert(topic);
    }

    bool HadTopicAclOk(const TString& topic) const {
        return TopicAclOk.find(topic) != TopicAclOk.end();
    }

    bool KafkaTableFeatureFlagChanged(bool serverlessTransactionsEnabledNow) const {
        return InitialServerlessTransactionsFlagValue.has_value() &&
               *InitialServerlessTransactionsFlagValue != serverlessTransactionsEnabledNow;
    }
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

    T& operator*() const {
        return *Ptr;
    }

    operator bool() const {
        return nullptr != Ptr;
    }

private:
    const std::shared_ptr<TBuffer> Buffer;
    const std::shared_ptr<TApiMessage> Message;
    T* Ptr;
};

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
        case Ydb::StatusIds::TIMEOUT:
            return EKafkaErrors::REQUEST_TIMED_OUT;
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
        case NPersQueue::NErrorCode::EErrorCode::READ_NOT_DONE:
            return EKafkaErrors::NONE_ERROR;
        case NPersQueue::NErrorCode::EErrorCode::TABLET_PIPE_DISCONNECTED:
            return EKafkaErrors::NOT_LEADER_OR_FOLLOWER;
        default:
            return EKafkaErrors::UNKNOWN_SERVER_ERROR;
    }
}

inline EKafkaErrors ConvertErrorCode(Ydb::PersQueue::ErrorCode::ErrorCode code) {
    switch (code) {
        case Ydb::PersQueue::ErrorCode::ErrorCode::OK:
            return EKafkaErrors::NONE_ERROR;
        case Ydb::PersQueue::ErrorCode::ErrorCode::UNKNOWN_READ_RULE:
            return EKafkaErrors::GROUP_ID_NOT_FOUND;
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

inline TString NormalizePath(const TString& database, const TString& path) {
    return NKikimr::NormalizePath(database, path);
}

inline TString GetTopicNameWithoutDb(const TString& database, TString topic) {
    auto topicWithDb = NormalizePath(database, topic);
    topic = topicWithDb.substr(database.size()+1);
    return topic;
}

inline TString GetUsernameOrAnonymous(std::shared_ptr<TContext> context) {
    return context->Token.UserToken ? context->Token.UserToken->GetUserSID() : "anonymous";
}

inline TString GetUserSerializedToken(std::shared_ptr<TContext> context) {
    if (!context->Token.UserToken) {
        return "";
    }
    if (!context->Token.UserToken->GetSerializedToken().empty()) {
        return context->Token.UserToken->GetSerializedToken();
    }
    return context->Token.UserToken->SerializeAsString();
}

NActors::IActor* CreateKafkaApiVersionsActor(const TContext::TPtr context, const ui64 correlationId, const TMessagePtr<TApiVersionsRequestData>& message,
                                            TKafkaVersion requestApiVersion);
NActors::IActor* CreateKafkaInitProducerIdActor(const TContext::TPtr context, const ui64 correlationId, const TMessagePtr<TInitProducerIdRequestData>& message);
NActors::IActor* CreateKafkaMetadataActor(const TContext::TPtr context, const ui64 correlationId,
                                          const TMessagePtr<TMetadataRequestData>& message,
                                          const NActors::TActorId& discoveryCacheActor);
NActors::IActor* CreateKafkaProduceActor(const TContext::TPtr context);
NActors::IActor* CreateKafkaReadSessionProxyActor(const TContext::TPtr context, ui64 cookie);
NActors::IActor* CreateKafkaReadSessionActor(const TContext::TPtr context, ui64 cookie);
NActors::IActor* CreateKafkaBalancerActor(const TContext::TPtr context, ui64 cookie);
NActors::IActor* CreateKafkaSaslHandshakeActor(const TContext::TPtr context, const ui64 correlationId, const TMessagePtr<TSaslHandshakeRequestData>& message);
NActors::IActor* CreateKafkaSaslAuthActor(const TContext::TPtr context, const NKikimr::NRawSocket::TSocketDescriptor::TSocketAddressType address);
NActors::IActor* CreateKafkaListOffsetsActor(const TContext::TPtr context, const ui64 correlationId, const TMessagePtr<TListOffsetsRequestData>& message);
NActors::IActor* CreateKafkaListGroupsActor(const TContext::TPtr context, const ui64 correlationId, const TMessagePtr<TListGroupsRequestData>& message);
NActors::IActor* CreateKafkaDescribeGroupsActor(const TContext::TPtr context, const ui64 correlationId, const TMessagePtr<TDescribeGroupsRequestData>& message);
NActors::IActor* CreateKafkaFetchActor(const TContext::TPtr context, const ui64 correlationId, const TMessagePtr<TFetchRequestData>& message);
NActors::IActor* CreateKafkaFindCoordinatorActor(const TContext::TPtr context, const ui64 correlationId, const TMessagePtr<TFindCoordinatorRequestData>& message);
NActors::IActor* CreateKafkaOffsetCommitActor(const TContext::TPtr context, const ui64 correlationId, const TMessagePtr<TOffsetCommitRequestData>& message);
NActors::IActor* CreateKafkaOffsetFetchActor(const TContext::TPtr context, const ui64 correlationId, const TMessagePtr<TOffsetFetchRequestData>& message);
NActors::IActor* CreateKafkaCreateTopicsActor(const TContext::TPtr context, const ui64 correlationId, const TMessagePtr<TCreateTopicsRequestData>& message);
NActors::IActor* CreateKafkaCreatePartitionsActor(const TContext::TPtr context, const ui64 correlationId, const TMessagePtr<TCreatePartitionsRequestData>& message);
NActors::IActor* CreateKafkaDescribeConfigsActor(const TContext::TPtr context, const ui64 correlationId, const TMessagePtr<TDescribeConfigsRequestData>& message);
NActors::IActor* CreateKafkaAlterConfigsActor(const TContext::TPtr context, const ui64 correlationId, const TMessagePtr<TAlterConfigsRequestData>& message);

} // namespace NKafka
