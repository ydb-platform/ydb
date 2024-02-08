#pragma once

#include "ydb/library/aclib/aclib.h"
#include <ydb/core/base/ticket_parser.h>
#include <ydb/public/api/grpc/ydb_auth_v1.grpc.pb.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>

#include "actors.h"

namespace NKafka {

class TKafkaSaslHandshakeActor: public NActors::TActorBootstrapped<TKafkaSaslHandshakeActor> {

const TVector<TString> SUPPORTED_SASL_MECHANISMS = {
    "PLAIN"
};

public:
    TKafkaSaslHandshakeActor(const TContext::TPtr context, const ui64 correlationId, const TMessagePtr<TSaslHandshakeRequestData>& message)
        : Context(context)
        , CorrelationId(correlationId)
        , HandshakeRequestData(message) {
    }

void Bootstrap(const NActors::TActorContext& ctx);

private:
    void Handshake();
    void SendResponse(const TString& errorMessage, EKafkaErrors kafkaError, EAuthSteps authStep, const TString& saslMechanism = "");

private:
    const TContext::TPtr Context;
    const ui64 CorrelationId;
    const TMessagePtr<TSaslHandshakeRequestData> HandshakeRequestData;
};

} // NKafka
