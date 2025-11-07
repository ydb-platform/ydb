#pragma once

#include <ydb/library/actors/core/actorsystem_fwd.h>
#include <ydb/core/persqueue/public/config.h>

#include <util/system/types.h>

#include <optional>


class TDuration;

namespace NKikimr::NPQ::NMLP {

// MLP не работает если включена компактифкация по ключу!!! (иначе не понятно как прореживать скомпакченные значения)
NActors::IActor* CreateConsumerActor(
    ui64 tabletId,
    const NActors::TActorId& tabletActorId,
    ui32 partitionId,
    const NActors::TActorId& partitionActorId,
    const NKikimrPQ::TPQTabletConfig_TConsumer& config,
    const std::optional<TDuration> retentionPeriod
);

} // namespace NKikimr::NPQ::NMLP
