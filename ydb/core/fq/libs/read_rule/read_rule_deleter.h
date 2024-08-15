#pragma once
#include <ydb/core/fq/libs/protos/fq_private.pb.h>

#include <ydb-cpp-sdk/client/driver/driver.h>

#include <ydb/library/actors/core/actor.h>

namespace NFq {

NActors::IActor* MakeReadRuleDeleterActor(
    NActors::TActorId owner,
    TString queryId,
    NYdb::TDriver ydbDriver,
    const ::google::protobuf::RepeatedPtrField<Fq::Private::TopicConsumer>& topicConsumers,
    TVector<std::shared_ptr<NYdb::ICredentialsProviderFactory>> credentials, // For each topic
    size_t maxRetries = 15
);

} // namespace NFq
