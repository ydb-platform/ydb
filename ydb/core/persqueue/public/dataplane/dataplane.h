#pragma once

#include <ydb/core/persqueue/writer/writer.h>
#include <ydb/library/actors/core/actorsystem_fwd.h>

namespace NKikimr::NPQ {

class TPartitionWriterCacheActor;

NActors::IActor* CreatePartitionWriterCacheActor(
    const NActors::TActorId& owner,
    ui32 partition,
    ui64 tabletId,
    const TPartitionWriterOpts& opts);

} // namespace NKikimr::NPQ
