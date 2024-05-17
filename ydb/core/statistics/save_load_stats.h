#pragma once

#include <ydb/core/scheme/scheme_pathid.h>
#include <ydb/library/actors/core/actor.h>

namespace NKikimr::NStat {

NActors::IActor* CreateStatisticsTableCreator(std::unique_ptr<NActors::IEventBase> event);

NActors::IActor* CreateSaveStatisticsQuery(const TPathId& pathId, ui64 statType,
    std::vector<TString>&& columnNames, std::vector<TString>&& data);

NActors::IActor* CreateLoadStatisticsQuery(const TPathId& pathId, ui64 statType,
    const TString& columnName, ui64 cookie);

NActors::IActor* CreateDeleteStatisticsQuery(const TPathId& pathId);

};
