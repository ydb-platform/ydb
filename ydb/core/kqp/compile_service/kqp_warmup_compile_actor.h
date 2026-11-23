#pragma once

#include <ydb/core/kqp/common/warmup/warmup.h>

namespace NKikimr::NKqp {

NActors::IActor* CreateKqpWarmupActor(
    const TKqpWarmupConfig& config,
    const TString& database = {},
    const TString& cluster = {},
    TVector<NActors::TActorId> notifyActorIds = {});

} // namespace NKikimr::NKqp
