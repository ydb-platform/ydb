#pragma once

#include "ydb/library/aclib/aclib.h"
#include <ydb/core/base/ticket_parser.h>
#include <ydb/core/kafka_proxy/kafka_events.h>
#include <ydb/core/security/sasl/events.h>
#include <ydb/core/tx/scheme_board/events.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>
#include <ydb/public/api/grpc/ydb_auth_v1.grpc.pb.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>
#include <ydb/public/api/grpc/ydb_auth_v1.grpc.pb.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>

#include "actors.h"

namespace NKafka {

using namespace NKikimr;

class TKafkaSaslAuthActor: public NActors::TActorBootstrapped<TKafkaSaslAuthActor> {

struct TAuthData {
    TString UserName;
    TString Password;
};

public:
    TKafkaSaslAuthActor(const TContext::TPtr context, NRawSocket::TSocketDescriptor::TSocketAddressType address)
        : Context(context)
        , Address(address) {
    }

    void Bootstrap();

private:
    STATEFN(StateWork) {
        KAFKA_LOG_T("Received event: " << (*ev.Get()).GetTypeName());
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvKafka::TEvAuthRequest, HandleAuthRequest);
            CFunc(TEvents::TEvPoison::EventType, Die);
        }
    }

    STATEFN(StateResolveDatabase) {
        KAFKA_LOG_T("Received event: " << (*ev.Get()).GetTypeName());
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvTxProxySchemeCache::TEvNavigateKeySetResult, HandleNavigate);
            CFunc(TEvents::TEvPoison::EventType, Die);
        }
    }

    STATEFN(StateResolveSharedDatabase) {
        KAFKA_LOG_T("Received event: " << (*ev.Get()).GetTypeName());
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvTxProxySchemeCache::TEvNavigateKeySetResult, HandleNavigate);
            CFunc(TEvents::TEvPoison::EventType, Die);
        }
    }

    STATEFN(StateSaslPlainLogin) {
        KAFKA_LOG_T("Received event: " << (*ev.Get()).GetTypeName());
        switch (ev->GetTypeRewrite()) {
            HFunc(NSasl::TEvSasl::TEvSaslPlainLoginResponse, HandleLoginResult);
            HFunc(NSasl::TEvSasl::TEvSaslPlainLdapLoginResponse, HandleLoginResult);
            CFunc(TEvents::TSystem::Wakeup, HandleTimeout);
            CFunc(TEvents::TEvPoison::EventType, Die);
        }
    }

    STATEFN(StateSaslScramLogin) {
        KAFKA_LOG_T("Received event: " << (*ev.Get()).GetTypeName());
        switch (ev->GetTypeRewrite()) {
            hFunc(NSasl::TEvSasl::TEvSaslScramFirstServerResponse, HandleFirstLoginResponse);
            HFunc(TEvKafka::TEvAuthRequest, HandleAuthRequest);
            HFunc(NSasl::TEvSasl::TEvSaslScramFinalServerResponse, HandleLoginResult);
            CFunc(TEvents::TSystem::Wakeup, HandleTimeout);
            CFunc(TEvents::TEvPoison::EventType, CleanupAndDie);
        }
    }

    STATEFN(StateTicketResolve) {
        KAFKA_LOG_T("Received event: " << (*ev.Get()).GetTypeName());
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvTicketParser::TEvAuthorizeTicketResult, Handle);
            CFunc(TEvents::TEvPoison::EventType, Die);
        }
    }

    void Handle(TEvTicketParser::TEvAuthorizeTicketResult::TPtr& ev, const NActors::TActorContext& ctx);
    void HandleFirstLoginResponse(NSasl::TEvSasl::TEvSaslScramFirstServerResponse::TPtr& ev);
    void HandleAuthRequest(TEvKafka::TEvAuthRequest::TPtr& ev, const NActors::TActorContext& ctx);
    void HandleLoginResult(const NYql::TIssue& issue, const std::string& reason, const std::string& token,
        const std::string& sanitizedToken, bool isAdmin, const NActors::TActorContext& ctx);
    void HandleLoginResult(NSasl::TEvSasl::TEvSaslPlainLoginResponse::TPtr& ev, const NActors::TActorContext& ctx);
    void HandleLoginResult(NSasl::TEvSasl::TEvSaslPlainLdapLoginResponse::TPtr& ev, const NActors::TActorContext& ctx);
    void HandleLoginResult(NSasl::TEvSasl::TEvSaslScramFinalServerResponse::TPtr& ev, const NActors::TActorContext& ctx);
    void HandleTimeout(const NActors::TActorContext& ctx);
    void HandleNavigate(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev, const TActorContext& ctx);

    void StartPlainAuth(const NActors::TActorContext& ctx);
    void StartScramAuth();
    void SendPlainLoginRequest(const NActors::TActorContext& ctx);
    void SendScramLoginRequest(const NActors::TActorContext& ctx);
    void SendTicketParserRequest();
    void SendDescribeRequest();
    bool TryParseAuthDataTo(TKafkaSaslAuthActor::TAuthData& authData, const NActors::TActorContext& ctx);
    void CleanupAndDie(const NActors::TActorContext& ctx);
    void SendResponse();
    void SendResponseAndDie(EKafkaErrors errorCode, const TString& errorMessage, const TString& details, const NActors::TActorContext& ctx);
    void GetPathByPathId(const TPathId& pathId);

private:
    const TContext::TPtr Context;
    ui64 CorrelationId;

    TString AuthRequest = "";
    TString AuthResponse = "";
    const NRawSocket::TNetworkConfig::TSocketAddressType Address;

    static const TDuration Timeout;

    TAuthData ClientAuthData;
    TString DatabasePath;

    TString Ticket;
    TVector<TEvTicketParser::TEvAuthorizeTicket::TEntry> TicketParserEntries;
    TIntrusiveConstPtr<NACLib::TUserToken> UserToken;
    TString FolderId;
    TString ServiceAccountId;
    TString DatabaseId;
    TString Coordinator;
    TString ResourcePath;
    TString CloudId;
    TString KafkaApiFlag;
    TString ResourseDatabasePath;
    bool IsServerless = false;

    TActorId ScramAuthActor;
};

} // NKafka
