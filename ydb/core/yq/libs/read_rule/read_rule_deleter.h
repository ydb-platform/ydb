#pragma once
#include <ydb/core/yq/libs/protos/fq_private.pb.h>

#include <ydb/public/sdk/cpp/client/ydb_driver/driver.h>

#include <library/cpp/actors/core/actor.h>

namespace NYq {

NActors::IActor* MakeReadRuleDeleterActor(
    NActors::TActorId owner,
    TString queryId,
    NYdb::TDriver ydbDriver,
    TVector<Fq::Private::TopicConsumer> topics,
    TVector<std::shared_ptr<NYdb::ICredentialsProviderFactory>> credentials, // For each topic
    size_t maxRetries = 15
);

} // namespace NYq
