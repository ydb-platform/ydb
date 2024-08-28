#pragma once

#include <ydb/core/scheme/scheme_pathid.h>
#include <ydb/library/actors/core/actor.h>

namespace NKikimr::NStat {

NActors::IActor* CreateStatisticsTableCreator(std::unique_ptr<NActors::IEventBase> event);

NActors::IActor* CreateSaveStatisticsQuery(const NActors::TActorId& replyActorId,
    const TPathId& pathId, ui64 statType, std::vector<ui32>&& columnTags, std::vector<TString>&& data);

NActors::IActor* CreateLoadStatisticsQuery(const NActors::TActorId& replyActorId,
    const TPathId& pathId, ui64 statType, ui32 columnTag, ui64 cookie);

NActors::IActor* CreateDeleteStatisticsQuery(const NActors::TActorId& replyActorId, const TPathId& pathId);

};
