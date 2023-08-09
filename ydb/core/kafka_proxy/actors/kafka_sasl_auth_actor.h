#include "ydb/library/aclib/aclib.h"
#include <ydb/core/base/ticket_parser.h>
#include <library/cpp/actors/core/actor_bootstrapped.h>
#include <ydb/public/api/grpc/ydb_auth_v1.grpc.pb.h>

#include "../kafka_events.h"

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

public:
    TKafkaSaslAuthActor(const TActorId parent, const ui64 correlationId, const TSaslHandshakeRequestData* message)
        : Parent(parent)
        , CorrelationId(correlationId)
        , HandshakeRequestData(message)
        , ReqType(true) {
    }

    TKafkaSaslAuthActor(const TActorId parent, const ui64 correlationId, NKikimr::NRawSocket::TSocketDescriptor::TSocketAddressType address, const TSaslAuthenticateRequestData* message)
        : Parent(parent)
        , CorrelationId(correlationId)
        , AuthenticateRequestData(message)
        , Address(address)
        , ReqType(false) {
    }

    STATEFN(StateAuth) {
        switch (ev->GetTypeRewrite()) {
            HFunc(NKikimr::TEvTicketParser::TEvAuthorizeTicketResult, Handle);
            HFunc(TEvPrivate::TEvTokenReady, Handle);
            HFunc(TEvPrivate::TEvAuthFailed, Handle);
            //savnik: poison pill
        }
    }

    void Bootstrap(const NActors::TActorContext& ctx);
    
    void Handle(NKikimr::TEvTicketParser::TEvAuthorizeTicketResult::TPtr& ev, const NActors::TActorContext& ctx);
    void Handle(TEvPrivate::TEvTokenReady::TPtr& ev, const NActors::TActorContext& ctx);
    void Handle(TEvPrivate::TEvAuthFailed::TPtr& ev, const NActors::TActorContext& ctx);

    TSaslHandshakeResponseData::TPtr Handshake();
    void PlainAuth(const NActors::TActorContext& ctx);
    
    std::pair<TString, TString> ParseLoginAndDatabase(const TString& str);

private:
    const TActorId Parent;
    const ui64 CorrelationId;
    const TSaslHandshakeRequestData* HandshakeRequestData;
    const TSaslAuthenticateRequestData* AuthenticateRequestData;
    const NKikimr::NRawSocket::TNetworkConfig::TSocketAddressType Address;
    const TIntrusiveConstPtr<NACLib::TUserToken> Token;
    const bool ReqType;
};

} // NKafka
