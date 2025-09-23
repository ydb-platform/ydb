#pragma once

#include <ydb/core/persqueue/public/config.h>
#include <ydb/library/actors/core/actorsystem_fwd.h>

namespace NKikimr::NPQ {

NActors::IActor* CreateMirrorDescriber(
    const ui64 tabletId,
    const NActors::TActorId& readBalancerActorId,
    const TString& topicName,
    const NKikimrPQ::TMirrorPartitionConfig& config
);

}
