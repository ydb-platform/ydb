#pragma once

#include <library/cpp/actors/core/actor.h>

namespace NKikimr::NBsController {

NActors::IActor *CreateStorageStatsCalculator();

} // NKikimr::NBsController
