#pragma once
#include <ydb/library/yql/providers/pq/proto/dq_io.pb.h>

#include <ydb/public/sdk/cpp/client/ydb_driver/driver.h>

#include <library/cpp/actors/core/actor.h>

#include <util/generic/maybe.h>

#include <google/protobuf/any.pb.h>

namespace NYq {

NActors::IActor* MakeReadRuleCreatorActor(
    NActors::TActorId owner,
    TString queryId,
    NYdb::TDriver ydbDriver,
    TVector<NYql::NPq::NProto::TDqPqTopicSource> topics,
    TVector<std::shared_ptr<NYdb::ICredentialsProviderFactory>> credentials // For each topic
);

} // namespace NYq
