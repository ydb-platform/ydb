#pragma once

#include <ydb/core/protos/kqp.pb.h>

#include <library/cpp/actors/core/actor.h>

namespace NKikimr {

NActors::IActor *CreateYqlSingleQueryActor(
    NActors::TActorId parent,
    TString workingDir,
    TString query,
    NKikimrKqp::EQueryType queryType,
    bool readOnly,
    TString result,
    TDuration timeout = TDuration::Seconds(10)
);

} // namespace NKikimr
