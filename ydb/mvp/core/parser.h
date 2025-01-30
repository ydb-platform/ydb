#pragma once
#include <ydb/library/actors/core/actorsystem.h>
#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/events.h>
#include <ydb/library/actors/core/event_local.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <library/cpp/json/json_reader.h>
#include <library/cpp/json/json_writer.h>
#include "merger.h"

namespace NMVP {

NActors::TActorId CreateJsonParser(const NActors::TActorContext& ctx);

} // namespace NMVP
