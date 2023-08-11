#pragma once

#include "ydb/library/aclib/aclib.h"
#include <ydb/core/base/ticket_parser.h>
#include <ydb/public/api/grpc/ydb_auth_v1.grpc.pb.h>
#include <library/cpp/actors/core/actor_bootstrapped.h>

#include "../kafka_events.h"

namespace NKafka {

class TKafkaSaslHandshakeActor: public NActors::TActorBootstrapped<TKafkaSaslHandshakeActor> {

const TVector<TString> SUPPORTED_SASL_MECHANISMS = {
    "PLAIN"
};

public:
    TKafkaSaslHandshakeActor(const TActorId parent, const ui64 correlationId, const EAuthSteps authStep, const TSaslHandshakeRequestData* message)
        : Parent(parent)
        , CorrelationId(correlationId)
        , HandshakeRequestData(message)
        , AuthStep(authStep) {
    }

void Bootstrap(const NActors::TActorContext& ctx);

private:
    void Handshake();
    void SendResponse(TString errorMessage, EKafkaErrors kafkaError, EAuthSteps authStep, TString saslMechanism = "");

private:
    const TActorId Parent;
    const ui64 CorrelationId;
    const TSaslHandshakeRequestData* HandshakeRequestData;
    EAuthSteps AuthStep;
};

} // NKafka
