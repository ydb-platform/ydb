#pragma once

#include <ydb/core/fq/libs/protos/dq_effects.pb.h>
#include <ydb/library/yql/dq/common/dq_common.h>
#include <ydb/library/yql/providers/common/http_gateway/yql_http_gateway.h>
#include <ydb/library/yql/providers/common/token_accessor/client/factory.h>
#include <ydb/library/actors/core/actor.h>

namespace NYql::NDq {

THolder<NActors::IActor> MakeS3ApplicatorActor(
    NActors::TActorId parentId,
    IHTTPGateway::TPtr gateway,
    const TString& queryId,
    const TString& jobId,
    std::optional<ui32> restartNumber,
    bool commit,
    const THashMap<TString, TString>& secureParams,
    ISecuredServiceAccountCredentialsFactory::TPtr credentialsFactory,
    const NYql::NDqProto::TExternalEffect& externalEffect);

} // namespace NYql::NDq