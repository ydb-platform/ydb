#pragma once

#include <ydb/library/yql/providers/dq/interface/yql_dq_full_result_writer.h>
#include <ydb/library/actors/core/actor.h>

namespace NYql::NDqs {

std::unique_ptr<NActors::IActor> MakeFullResultWriterActor(
    const TString& traceId,
    const TString& resultType,
    std::unique_ptr<IDqFullResultWriter>&& writer,
    const NActors::TActorId& aggregatorId);

} // namespace NYql::NDqs
