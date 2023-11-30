#pragma once

#include "ydb/library/aclib/aclib.h"
#include <ydb/core/base/ticket_parser.h>
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

struct TEvPrivate {
    enum EEv {
        EvTokenReady = EventSpaceBegin(NActors::TEvents::ES_PRIVATE),
        EvAuthFailed,
        EvEnd
    };

    static_assert(EvEnd < EventSpaceEnd(NActors::TEvents::ES_PRIVATE), "expect EvEnd < EventSpaceEnd(NActors::TEvents::ES_PRIVATE)");

    struct TEvTokenReady : NActors::TEventLocal<TEvTokenReady, EvTokenReady> {
        Ydb::Auth::LoginResult LoginResult;
        TString Database;
    };

    struct TEvAuthFailed : NActors::TEventLocal<TEvAuthFailed, EvAuthFailed> {
        TString ErrorMessage;
    };
};

struct TAuthData {
    TString UserName;
    TString Password;
    TString Database;
};

public:
    TKafkaSaslAuthActor(const TContext::TPtr context, const ui64 correlationId, NKikimr::NRawSocket::TSocketDescriptor::TSocketAddressType address, const TMessagePtr<TSaslAuthenticateRequestData>& message)
        : Context(context)
        , CorrelationId(correlationId)
        , AuthenticateRequestData(message)
        , Address(address) {
    }

    void Bootstrap(const NActors::TActorContext& ctx);

private:
    STATEFN(StateWork) {
        KAFKA_LOG_T("Received event: " << (*ev.Get()).GetTypeName());
        switch (ev->GetTypeRewrite()) {
            HFunc(NKikimr::TEvTicketParser::TEvAuthorizeTicketResult, Handle);
            HFunc(TEvPrivate::TEvTokenReady, Handle);
            HFunc(TEvPrivate::TEvAuthFailed, Handle);
            HFunc(TEvTxProxySchemeCache::TEvNavigateKeySetResult, Handle);
        }
    }

    void Handle(NKikimr::TEvTicketParser::TEvAuthorizeTicketResult::TPtr& ev, const NActors::TActorContext& ctx);
    void Handle(TEvPrivate::TEvTokenReady::TPtr& ev, const NActors::TActorContext& ctx);
    void Handle(TEvPrivate::TEvAuthFailed::TPtr& ev, const NActors::TActorContext& ctx);
    void Handle(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev, const TActorContext& ctx);

    void StartPlainAuth(const NActors::TActorContext& ctx);
    void SendLoginRequest(TKafkaSaslAuthActor::TAuthData authData, const NActors::TActorContext& ctx);
    void SendApiKeyRequest();
    void SendDescribeRequest(const NActors::TActorContext& ctx);
    bool TryParseAuthDataTo(TKafkaSaslAuthActor::TAuthData& authData, const NActors::TActorContext& ctx);
    void SendResponseAndDie(EKafkaErrors errorCode, const TString& errorMessage, const TString& details, const NActors::TActorContext& ctx);

private:
    const TContext::TPtr Context;
    const ui64 CorrelationId;

    const TMessagePtr<TSaslAuthenticateRequestData> AuthenticateRequestData;
    const NKikimr::NRawSocket::TNetworkConfig::TSocketAddressType Address;

    TAuthData ClientAuthData;

    TString DatabasePath;
    TIntrusiveConstPtr<NACLib::TUserToken> UserToken;
    TString FolderId;
    TString ServiceAccountId;
    TString DatabaseId;
    TString Coordinator;
    TString ResourcePath;
    TString CloudId;
    TString KafkaApiFlag;
    bool IsServerless = false;
};

} // NKafka
