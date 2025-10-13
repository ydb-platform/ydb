#pragma once

#include <ydb/core/scheme/scheme_pathid.h>
#include <ydb/core/protos/statistics.pb.h>
#include <ydb/core/statistics/events.h>
#include <ydb/library/actors/core/actor.h>

namespace NKikimr::NStat {

NActors::IActor* CreateStatisticsTableCreator(std::unique_ptr<NActors::IEventBase> event, const TString& database);

NActors::IActor* CreateSaveStatisticsQuery(const NActors::TActorId& replyActorId, const TString& database,
    const TPathId& pathId, std::vector<TStatisticsItem>&& items);

NActors::IActor* CreateLoadStatisticsQuery(const NActors::TActorId& replyActorId, const TString& database,
    const TPathId& pathId, ui64 statType, ui32 columnTag);

NActors::IActor* CreateDeleteStatisticsQuery(const NActors::TActorId& replyActorId, const TString& database,
    const TPathId& pathId);

};
