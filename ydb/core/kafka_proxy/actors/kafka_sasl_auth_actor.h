#pragma once

#include "ydb/library/aclib/aclib.h"
#include <ydb/core/base/ticket_parser.h>
#include <ydb/public/api/grpc/ydb_auth_v1.grpc.pb.h>
#include <library/cpp/actors/core/actor_bootstrapped.h>

#include "../kafka_events.h"
#include "actors.h"

namespace NKafka {

class TKafkaSaslAuthActor: public NActors::TActorBootstrapped<TKafkaSaslAuthActor> {

struct TEvPrivate {
    enum EEv {
        EvTokenReady = EventSpaceBegin(NActors::TEvents::ES_PRIVATE),
        EvAuthFailed,
        EvEnd
    };

    static_assert(EvEnd < EventSpaceEnd(NActors::TEvents::ES_PRIVATE), "expect EvEnd < EventSpaceEnd(NActors::TEvents::ES_PRIVATE)");

    struct TEvTokenReady : TEventLocal<TEvTokenReady, EvTokenReady> {
        Ydb::Auth::LoginResult LoginResult;
        TString Database;
    };

    struct TEvAuthFailed : TEventLocal<TEvAuthFailed, EvAuthFailed> {
        TString ErrorMessage;
    };
};

struct TAuthData {
    TString UserName;
    TString Password;
    TString Database;
};

public:
    TKafkaSaslAuthActor(const TActorId parent, const ui64 correlationId, NKikimr::NRawSocket::TSocketDescriptor::TSocketAddressType address, EAuthSteps authStep, TString saslMechanism, const TSaslAuthenticateRequestData* message)
        : Parent(parent)
        , CorrelationId(correlationId)
        , AuthenticateRequestData(message)
        , Address(address)
        , AuthStep(authStep)
        , SaslMechanism(saslMechanism) {
    }

    void Bootstrap(const NActors::TActorContext& ctx);

private:
    STATEFN(StateWork) {
        switch (ev->GetTypeRewrite()) {
            HFunc(NKikimr::TEvTicketParser::TEvAuthorizeTicketResult, Handle);
            HFunc(TEvPrivate::TEvTokenReady, Handle);
            HFunc(TEvPrivate::TEvAuthFailed, Handle);
        }
    }

    void Handle(NKikimr::TEvTicketParser::TEvAuthorizeTicketResult::TPtr& ev, const NActors::TActorContext& ctx);
    void Handle(TEvPrivate::TEvTokenReady::TPtr& ev, const NActors::TActorContext& ctx);
    void Handle(TEvPrivate::TEvAuthFailed::TPtr& ev, const NActors::TActorContext& ctx);

    void StartPlainAuth(const NActors::TActorContext& ctx);
    void SendLoginRequest(TKafkaSaslAuthActor::TAuthData authData, const NActors::TActorContext& ctx);
    void SendAuthFailedAndDie(TString errorMessage, EKafkaErrors errorCode, const NActors::TActorContext& ctx);
    bool TryParseAuthDataTo(TKafkaSaslAuthActor::TAuthData& authData, const NActors::TActorContext& ctx);

private:
    const TActorId Parent;
    const ui64 CorrelationId;
    const TSaslAuthenticateRequestData* AuthenticateRequestData;
    const NKikimr::NRawSocket::TNetworkConfig::TSocketAddressType Address;
    const EAuthSteps AuthStep;
    const TString SaslMechanism;
    TString Database;
};

} // NKafka
