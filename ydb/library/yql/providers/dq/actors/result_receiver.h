#pragma once

#include <ydb/library/yql/providers/dq/common/yql_dq_settings.h>
#include <ydb/library/yql/providers/dq/api/protos/service.pb.h>

#include <ydb/library/actors/core/actor.h>

namespace NYql {

THolder<NActors::IActor> MakeResultReceiver(
    const TVector<TString>& columns,
    const NActors::TActorId& executerId,
    const TString& traceId,
    const TDqConfiguration::TPtr& settings,
//    const Yql::DqsProto::TFullResultTable& resultTable,
    const THashMap<TString, TString>& secureParams,
    const TString& resultBuilder,
    const NActors::TActorId& graphExecutionEventsId,
    bool discard
);

} // namespace NYql
