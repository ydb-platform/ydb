#include <ydb/core/grpc_services/local_rpc/local_rpc.h>
#include <ydb/public/api/grpc/ydb_auth_v1.grpc.pb.h>
#include <library/cpp/actors/core/actor.h>
#include <ydb/core/base/ticket_parser.h>
#include "kafka_sasl_auth_actor.h"

namespace NKafka {

NActors::IActor* CreateKafkaSaslAuthActor(const TActorId parent, const ui64 correlationId, const TSaslHandshakeRequestData* message) {
    return new TKafkaSaslAuthActor(parent, correlationId, message);
}

NActors::IActor* CreateKafkaSaslAuthActor(const TActorId parent, const ui64 correlationId, const NKikimr::NRawSocket::TSocketDescriptor::TSocketAddressType address, const TSaslAuthenticateRequestData* message) {
    return new TKafkaSaslAuthActor(parent, correlationId, address, message);
}    

void TKafkaSaslAuthActor::Bootstrap(const NActors::TActorContext& ctx) {
    if (ReqType) {
        Send(Parent, new TEvKafka::TEvResponse(CorrelationId, Handshake()));
        Die(ctx);
    } else {
        Become(&TKafkaSaslAuthActor::StateAuth);
        PlainAuth(ctx);
    }
}

void TKafkaSaslAuthActor::PlainAuth(const NActors::TActorContext& ctx) {
    TKafkaBytes auth_bytes_opt = AuthenticateRequestData->AuthBytes;
    TString username;
    TString password;
    TString database;

    if (auth_bytes_opt.has_value()) {
        TKafkaRawBytes auth_bytes = auth_bytes_opt.value();
        TString auth_data(auth_bytes.data(), auth_bytes.size());

        size_t first_null_pos = auth_data.find('\0');
        if(first_null_pos != TString::npos && first_null_pos < auth_data.length() - 1) {
            size_t second_null_pos = auth_data.find('\0', first_null_pos + 1);
            if(second_null_pos != TString::npos) {
                auto loginAndDatabase = auth_data.substr(first_null_pos + 1, second_null_pos - first_null_pos - 1);
                std::tie(username, database) = ParseLoginAndDatabase(loginAndDatabase);
                password = auth_data.substr(second_null_pos + 1);
            }
        }
    }

    Ydb::Auth::LoginRequest request;
    request.set_user(username);
    request.set_password(password);
    TActorSystem* actorSystem = ctx.ActorSystem();
    using TRpcEv = NKikimr::NGRpcService::TGRpcRequestWrapperNoAuth<NKikimr::NGRpcService::TRpcServices::EvLogin, Ydb::Auth::LoginRequest, Ydb::Auth::LoginResponse>;
    auto rpcFuture = NKikimr::NRpcService::DoLocalRpc<TRpcEv>(std::move(request), database, {}, actorSystem);
    rpcFuture.Subscribe([database, actorSystem, selfId = SelfId()](const NThreading::TFuture<Ydb::Auth::LoginResponse>& future) {
        auto& response = future.GetValueSync();
        if (response.operation().status() == Ydb::StatusIds::SUCCESS) {
            auto tokenReady = std::make_unique<TEvPrivate::TEvTokenReady>();
            response.operation().result().UnpackTo(&(tokenReady->LoginResult));
            tokenReady->Database = database;
            actorSystem->Send(selfId, tokenReady.release());
        } else {
            auto authFailed = std::make_unique<TEvPrivate::TEvAuthFailed>();
            if (response.operation().issues_size() > 0) {
                authFailed->ErrorMessage = response.operation().issues(0).message();
            } else {
                authFailed->ErrorMessage = Ydb::StatusIds_StatusCode_Name(response.operation().status());
            }
            actorSystem->Send(selfId, authFailed.release());
        }
    });
}

void TKafkaSaslAuthActor::Handle(NKikimr::TEvTicketParser::TEvAuthorizeTicketResult::TPtr& ev, const NActors::TActorContext& ctx) {
    TSaslAuthenticateResponseData::TPtr response = std::make_shared<TSaslAuthenticateResponseData>();
    response->AuthBytes = TKafkaRawBytes("", "");
    if (ev->Get()->Error) {
        response->ErrorCode = 1;
        response->ErrorMessage = ev->Get()->Error.Message;
        //savnik: close conn
    } else {
        response->ErrorCode = 0;
        response->ErrorMessage = "";
        Send(Parent, new TEvKafka::TEvAuthSuccess(ev->Get()->Token));
    }
    Send(Parent, new TEvKafka::TEvResponse(CorrelationId, response));
    Die(ctx);
}

void TKafkaSaslAuthActor::Handle(TEvPrivate::TEvTokenReady::TPtr& ev, const NActors::TActorContext&) {
    Send(NKikimr::MakeTicketParserID(), new NKikimr::TEvTicketParser::TEvAuthorizeTicket({
        .Database = ev->Get()->Database,
        .Ticket = ev->Get()->LoginResult.token(),
        .PeerName = TStringBuilder() << Address,
    }));
}

void TKafkaSaslAuthActor::Handle(TEvPrivate::TEvAuthFailed::TPtr& ev, const NActors::TActorContext& ctx) {
    TSaslAuthenticateResponseData::TPtr saslResponse = std::make_shared<TSaslAuthenticateResponseData>();
    saslResponse->AuthBytes = TKafkaRawBytes("", "");
    saslResponse->ErrorCode = 1;
    saslResponse->ErrorMessage = ev->Get()->ErrorMessage;
    Send(Parent, new TEvKafka::TEvResponse(CorrelationId, saslResponse));
    //savnik: close conn
    Die(ctx);
}

std::pair<TString, TString> TKafkaSaslAuthActor::ParseLoginAndDatabase(const TString& str) {
    size_t pos = str.rfind('@');
    if (pos == std::string::npos) {
        return {str, ""}; //savnik
    }
    return {str.substr(0, pos), str.substr(pos + 1)};
}

TSaslHandshakeResponseData::TPtr TKafkaSaslAuthActor::Handshake() {
    TSaslHandshakeResponseData::TPtr response = std::make_shared<TSaslHandshakeResponseData>();
    if (HandshakeRequestData->Mechanism == "PLAIN") {
        response->ErrorCode = 0;
        response->Mechanisms.push_back("PLAIN");
    } else {
        response->ErrorCode = 1;
    }
    
    return response;
}

}