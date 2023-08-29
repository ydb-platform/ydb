#pragma once

#include <ydb/core/persqueue/pq_rl_helpers.h>
#include <ydb/core/protos/config.pb.h>
#include <ydb/library/aclib/aclib.h>

#include "../kafka_messages.h"

namespace NKafka {

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


    EAuthSteps AuthenticationStep = EAuthSteps::WAIT_HANDSHAKE;
    TString SaslMechanism;

    TString Database;
    TIntrusiveConstPtr<NACLib::TUserToken> UserToken;
    TString ClientDC;

    NKikimr::NPQ::TRlContext RlContext;

    bool Authenticated() { return AuthenticationStep == SUCCESS; }
};

inline bool RequireAuthentication(EApiKey apiKey) {
    return !(EApiKey::API_VERSIONS == apiKey || EApiKey::SASL_HANDSHAKE == apiKey || EApiKey::SASL_AUTHENTICATE == apiKey);
}

NActors::IActor* CreateKafkaApiVersionsActor(const TContext::TPtr context, const ui64 correlationId, const TApiVersionsRequestData* message);
NActors::IActor* CreateKafkaInitProducerIdActor(const TContext::TPtr context, const ui64 correlationId, const TInitProducerIdRequestData* message);
NActors::IActor* CreateKafkaMetadataActor(const TContext::TPtr context, const ui64 correlationId, const TMetadataRequestData* message);
NActors::IActor* CreateKafkaProduceActor(const TContext::TPtr context);
NActors::IActor* CreateKafkaSaslHandshakeActor(const TContext::TPtr context, const ui64 correlationId, const TSaslHandshakeRequestData* message);
NActors::IActor* CreateKafkaSaslAuthActor(const TContext::TPtr context, const ui64 correlationId, const NKikimr::NRawSocket::TSocketDescriptor::TSocketAddressType address, const TSaslAuthenticateRequestData* message);

    
} // namespace NKafka
