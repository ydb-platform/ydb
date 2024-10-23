#include <ydb/core/grpc_services/local_rpc/local_rpc.h>
#include <ydb/public/api/grpc/ydb_auth_v1.grpc.pb.h>
#include <ydb/core/base/path.h>
#include <ydb/core/base/ticket_parser.h>
#include <ydb/core/kafka_proxy/kafka_events.h>
#include <ydb/core/tx/scheme_board/subscriber.h>
#include <ydb/services/persqueue_v1/actors/persqueue_utils.h>
#include <ydb/library/actors/core/actor.h>

#include "kafka_sasl_auth_actor.h"


namespace NKafka {

static constexpr char EmptyAuthBytes[] = "";

NActors::IActor* CreateKafkaSaslAuthActor(const TContext::TPtr context, const ui64 correlationId, const NKikimr::NRawSocket::TSocketDescriptor::TSocketAddressType address, const TMessagePtr<TSaslAuthenticateRequestData>& message) {
    return new TKafkaSaslAuthActor(context, correlationId, address, message);
}

void TKafkaSaslAuthActor::Bootstrap(const NActors::TActorContext& ctx) {
    if (Context->AuthenticationStep != EAuthSteps::WAIT_AUTH) {
        SendResponseAndDie(EKafkaErrors::ILLEGAL_SASL_STATE,
                             "Request is not valid given the current SASL state.",
                             TStringBuilder() << "Current step: " << static_cast<int>(Context->AuthenticationStep),
                             ctx);
        return;
    }
    if (Context->SaslMechanism != "PLAIN") {
        SendResponseAndDie(EKafkaErrors::UNSUPPORTED_SASL_MECHANISM,
                             "Does not support the requested SASL mechanism.",
                             TStringBuilder() << "Requested mechanism '" << Context->SaslMechanism << "'",
                             ctx);
        return;
    }
    Become(&TKafkaSaslAuthActor::StateWork);
    StartPlainAuth(ctx);
}

void TKafkaSaslAuthActor::StartPlainAuth(const NActors::TActorContext& ctx) {
    if (!TryParseAuthDataTo(ClientAuthData, ctx)) {
        return;
    }

    DatabasePath = CanonizePath(ClientAuthData.Database);

    SendDescribeRequest(ctx);
}

void TKafkaSaslAuthActor::Handle(NKikimr::TEvTicketParser::TEvAuthorizeTicketResult::TPtr& ev, const NActors::TActorContext& ctx) {
    if (ev->Get()->Error) {
        SendResponseAndDie(EKafkaErrors::SASL_AUTHENTICATION_FAILED, "", ev->Get()->Error.Message, ctx);
        return;
    }
    UserToken = ev->Get()->Token;

    if (ClientAuthData.UserName.empty()) {
        bool gotPermission = false;
        for (auto & sid : UserToken->GetGroupSIDs()) {
            if (sid == NKikimr::NGRpcProxy::V1::KafkaPlainAuthSid) {
                gotPermission = true;
                break;
            }
        }
        if (!gotPermission) {
            SendResponseAndDie(EKafkaErrors::SASL_AUTHENTICATION_FAILED, "", TStringBuilder() << "no permission '" << NKikimr::NGRpcProxy::V1::KafkaPlainAuthPermission << "'", ctx);
            return;
        }
    }

    SendResponseAndDie(EKafkaErrors::NONE_ERROR, "", "", ctx);
}

void TKafkaSaslAuthActor::Handle(TEvPrivate::TEvTokenReady::TPtr& ev, const NActors::TActorContext& /*ctx*/) {
    Send(NKikimr::MakeTicketParserID(), new NKikimr::TEvTicketParser::TEvAuthorizeTicket({
        .Database = ev->Get()->Database,
        .Ticket = "Login " + ev->Get()->LoginResult.token(),
        .PeerName = TStringBuilder() << Address,
    }));
}

void TKafkaSaslAuthActor::SendResponseAndDie(EKafkaErrors errorCode, const TString& errorMessage, const TString& details, const NActors::TActorContext& ctx) {
    auto isFailed = errorCode != EKafkaErrors::NONE_ERROR;

    auto responseToClient = std::make_shared<TSaslAuthenticateResponseData>();
    responseToClient->ErrorCode = errorCode;
    responseToClient->AuthBytes = TKafkaRawBytes(EmptyAuthBytes, sizeof(EmptyAuthBytes));

    if (isFailed) {
        KAFKA_LOG_ERROR("Authentication failure. " << errorMessage << " " << details);
        responseToClient->ErrorMessage = TStringBuilder() << "Authentication failure. " << errorMessage;

        auto evResponse = std::make_shared<TEvKafka::TEvResponse>(CorrelationId, responseToClient, errorCode);
        auto authResult = new TEvKafka::TEvAuthResult(EAuthSteps::FAILED, evResponse, errorMessage);
        Send(Context->ConnectionId, authResult);
    } else {
        KAFKA_LOG_D("Authentificated success. Database='" << DatabasePath << "', "
                                      << "FolderId='" << FolderId << "', "
                                      << "ServiceAccountId='" << ServiceAccountId << "', "
                                      << "DatabaseId='" << DatabaseId << "', "
                                      << "Coordinator='" << Coordinator << "', "
                                      << "ResourcePath='" << ResourcePath << "'");
        responseToClient->ErrorMessage = "";

        auto evResponse = std::make_shared<TEvKafka::TEvResponse>(CorrelationId, responseToClient, errorCode);
        auto authResult = new TEvKafka::TEvAuthResult(EAuthSteps::SUCCESS, evResponse, UserToken, DatabasePath, DatabaseId, FolderId, CloudId, ServiceAccountId, Coordinator,
                                                                ResourcePath, IsServerless, errorMessage);
        Send(Context->ConnectionId, authResult);
    }

    Die(ctx);
}

void TKafkaSaslAuthActor::Handle(TEvPrivate::TEvAuthFailed::TPtr& ev, const NActors::TActorContext& ctx) {
    SendResponseAndDie(EKafkaErrors::SASL_AUTHENTICATION_FAILED, "", ev->Get()->ErrorMessage, ctx);
}

bool TKafkaSaslAuthActor::TryParseAuthDataTo(TKafkaSaslAuthActor::TAuthData& authData, const NActors::TActorContext& ctx) {
    if (!AuthenticateRequestData->AuthBytes.has_value()) {
        SendResponseAndDie(EKafkaErrors::SASL_AUTHENTICATION_FAILED, "", "AuthBytes is empty.",  ctx);
        return false;
    }

    TKafkaRawBytes rawAuthBytes = AuthenticateRequestData->AuthBytes.value();
    TString auth(rawAuthBytes.data(), rawAuthBytes.size());
    TVector<TString> tokens = StringSplitter(auth).Split('\0');
    if (tokens.size() != 3) {
        SendResponseAndDie(EKafkaErrors::SASL_AUTHENTICATION_FAILED, TStringBuilder() << "Invalid SASL/PLAIN response: expected 3 tokens, got " << tokens.size(), "", ctx);
        return false;
    }

    // tokens[0] is authorizationIdFromClient. Ignored
    auto userAndDatabase = tokens[1];
    auto password = tokens[2];
    size_t atPos = userAndDatabase.rfind('@');
    if (atPos == TString::npos) {
        authData.UserName = "";
        authData.Database = userAndDatabase;
    } else {
        authData.UserName = userAndDatabase.substr(0, atPos);
        authData.Database = userAndDatabase.substr(atPos + 1);
    }

    authData.Password = password;
    return true;
}

void TKafkaSaslAuthActor::SendLoginRequest(TKafkaSaslAuthActor::TAuthData authData, const NActors::TActorContext& ctx) {
    Ydb::Auth::LoginRequest request;
    request.set_user(authData.UserName);
    request.set_password(authData.Password);
    auto* actorSystem = ctx.ActorSystem();

    using TRpcEv = NKikimr::NGRpcService::TGRpcRequestWrapperNoAuth<NKikimr::NGRpcService::TRpcServices::EvLogin, Ydb::Auth::LoginRequest, Ydb::Auth::LoginResponse>;
    auto rpcFuture = NKikimr::NRpcService::DoLocalRpc<TRpcEv>(std::move(request), authData.Database, {}, actorSystem);
    rpcFuture.Subscribe([authData, actorSystem, selfId = SelfId()](const NThreading::TFuture<Ydb::Auth::LoginResponse>& future) {
        auto& response = future.GetValueSync();

        if (response.operation().status() == Ydb::StatusIds::SUCCESS) {
            auto tokenReadyEvent = std::make_unique<TEvPrivate::TEvTokenReady>();
            response.operation().result().UnpackTo(&(tokenReadyEvent->LoginResult));
            tokenReadyEvent->Database = authData.Database;
            actorSystem->Schedule(TDuration::Seconds(1), new IEventHandle(selfId, selfId, tokenReadyEvent.release())); // FIXME(savnik): replace Schedule to Send
        } else {
            auto authFailedEvent = std::make_unique<TEvPrivate::TEvAuthFailed>();
            if (response.operation().issues_size() > 0) {
                authFailedEvent->ErrorMessage = response.operation().issues(0).message();
            } else {
                authFailedEvent->ErrorMessage = Ydb::StatusIds_StatusCode_Name(response.operation().status());
            }
            actorSystem->Send(selfId, authFailedEvent.release());
        }
    });
}

void TKafkaSaslAuthActor::SendApiKeyRequest() {
    auto entries = NKikimr::NGRpcProxy::V1::GetTicketParserEntries(DatabaseId, FolderId, true);

    Send(NKikimr::MakeTicketParserID(), new NKikimr::TEvTicketParser::TEvAuthorizeTicket({
        .Database = DatabasePath,
        .Ticket = "ApiKey " + ClientAuthData.Password,
        .PeerName = TStringBuilder() << Address,
        .Entries = entries
    }));
}

void TKafkaSaslAuthActor::SendDescribeRequest(const TActorContext& ctx) {
    auto schemeCacheRequest = std::make_unique<NKikimr::NSchemeCache::TSchemeCacheNavigate>();
    NKikimr::NSchemeCache::TSchemeCacheNavigate::TEntry entry;
    entry.Path = NKikimr::SplitPath(DatabasePath);
    entry.Operation = NKikimr::NSchemeCache::TSchemeCacheNavigate::OpPath;
    entry.SyncVersion = false;
    schemeCacheRequest->ResultSet.emplace_back(entry);
    ctx.Send(NKikimr::MakeSchemeCacheID(), MakeHolder<NKikimr::TEvTxProxySchemeCache::TEvNavigateKeySet>(schemeCacheRequest.release()));
}

void TKafkaSaslAuthActor::Handle(NKikimr::TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev, const TActorContext& ctx) {
    const NKikimr::NSchemeCache::TSchemeCacheNavigate* navigate = ev->Get()->Request.Get();
    if (navigate->ErrorCount) {
        SendResponseAndDie(EKafkaErrors::SASL_AUTHENTICATION_FAILED, "", TStringBuilder() << "Database with path '" << DatabasePath << "' doesn't exists", ctx);
        return;
    }
    Y_ABORT_UNLESS(navigate->ResultSet.size() == 1);
    IsServerless = navigate->ResultSet.front().DomainInfo->IsServerless();

    for (const auto& attr : navigate->ResultSet.front().Attributes) {
        if (attr.first == "folder_id") FolderId = attr.second;
        else if (attr.first == "cloud_id") CloudId = attr.second;
        else if (attr.first == "database_id") DatabaseId = attr.second;
        else if (attr.first == "service_account_id") ServiceAccountId = attr.second;
        else if (attr.first == "serverless_rt_coordination_node_path") Coordinator = attr.second;
        else if (attr.first == "serverless_rt_base_resource_ru") ResourcePath = attr.second;
        else if (attr.first == "kafka_api") KafkaApiFlag = attr.second;
    }

    if (ClientAuthData.UserName.empty()) {
        // ApiKey IAM authentification
        SendApiKeyRequest();
    } else {
        // Login/Password authentification
        SendLoginRequest(ClientAuthData, ctx);
    }
}

} // NKafka
