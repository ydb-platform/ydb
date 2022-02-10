#pragma once

#include <ydb/core/kqp/counters/kqp_counters.h>
#include <ydb/core/protos/config.pb.h>

#include <library/cpp/actors/core/actor.h>


namespace NKikimr::NKqp {

NActors::IActor* CreateKqpLocalFileSpillingActor(ui64 txId, const TString& details, const NActors::TActorId& client,
    bool removeBlobsAfterRead);

NActors::IActor* CreateKqpLocalFileSpillingService(
    const NKikimrConfig::TTableServiceConfig::TSpillingServiceConfig::TLocalFileConfig& config,
    TIntrusivePtr<TKqpCounters> counters);

} // namespace NKikimr::NKqp
