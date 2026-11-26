#pragma once

#include <ydb/core/statistics/service_id.h>

#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/event_local.h>

namespace NKikimr::NStat {

struct TStatServiceSettings {
    TStatServiceSettings() = default;
};

THolder<NActors::IActor> CreateStatService(const TStatServiceSettings& settings = TStatServiceSettings());

} // NKikimr::NStat
