#include <ydb/core/grpc_services/local_rpc/local_rpc.h>
#include <ydb/public/api/grpc/ydb_auth_v1.grpc.pb.h>
#include <ydb/core/base/ticket_parser.h>
#include <ydb/library/actors/core/actor.h>
#include <ydb/core/kafka_proxy/kafka_events.h>

#include "kafka_sasl_handshake_actor.h"

namespace NKafka {

NActors::IActor* CreateKafkaSaslHandshakeActor(const TContext::TPtr context, const ui64 correlationId, const TMessagePtr<TSaslHandshakeRequestData>& message) {
    return new TKafkaSaslHandshakeActor(context, correlationId, message);
}

void TKafkaSaslHandshakeActor::Bootstrap(const NActors::TActorContext& ctx) {
    Handshake();
    Die(ctx);
}

void TKafkaSaslHandshakeActor::Handshake() {
    if (Context->AuthenticationStep != EAuthSteps::WAIT_HANDSHAKE) {
        SendResponse("Authentication failure. Request is not valid given the current SASL state.", EKafkaErrors::ILLEGAL_SASL_STATE, EAuthSteps::FAILED);
        return;
    }
    if (std::find(SUPPORTED_SASL_MECHANISMS.begin(), SUPPORTED_SASL_MECHANISMS.end(), HandshakeRequestData->Mechanism) == SUPPORTED_SASL_MECHANISMS.end()) {
        SendResponse("Does not support the requested SASL mechanism.", EKafkaErrors::UNSUPPORTED_SASL_MECHANISM, EAuthSteps::FAILED);
        return;
    }
    SendResponse("", EKafkaErrors::NONE_ERROR, EAuthSteps::WAIT_AUTH, TStringBuilder() << HandshakeRequestData->Mechanism);
}

void TKafkaSaslHandshakeActor::SendResponse(const TString& errorMessage, EKafkaErrors kafkaError, EAuthSteps authStep, const TString& saslMechanism) {
    auto responseToClient = std::make_shared<TSaslHandshakeResponseData>();
    responseToClient->ErrorCode = kafkaError;
    responseToClient->Mechanisms.insert(responseToClient->Mechanisms.end(), SUPPORTED_SASL_MECHANISMS.begin(), SUPPORTED_SASL_MECHANISMS.end());

    auto evResponse = std::make_shared<TEvKafka::TEvResponse>(CorrelationId, responseToClient, kafkaError);
    auto handshakeResult = new TEvKafka::TEvHandshakeResult(authStep, evResponse, saslMechanism, errorMessage);
    Send(Context->ConnectionId, handshakeResult);
}

} // NKafka
