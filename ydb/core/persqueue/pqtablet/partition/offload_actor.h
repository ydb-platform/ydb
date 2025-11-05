#pragma once

#include <ydb/core/persqueue/common/partition_id.h>
#include <ydb/library/actors/core/actorsystem_fwd.h>

#include <util/system/types.h>

namespace NKikimrPQ {

class TOffloadConfig;

}

namespace NKikimr::NPQ {

NActors::IActor* CreateOffloadActor(NActors::TActorId parentTablet, ui64 tabletId,
    TPartitionId partition, const TString& database, const NKikimrPQ::TOffloadConfig& config);

} // namespace NKikimr::NPQ
