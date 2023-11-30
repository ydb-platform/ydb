#pragma once

#include "impl.h"

#include <ydb/library/actors/core/actor.h>

#include <util/system/types.h>

#include <memory>
#include <vector>

namespace NKikimr::NBsController {

struct TControllerSystemViewsState;

std::unique_ptr<NActors::IActor> CreateStorageStatsCoroCalculator(
    const TControllerSystemViewsState& state,
    const TBlobStorageController::THostRecordMap& hostRecordMap,
    ui32 groupReserveMin,
    ui32 groupReservePart);

} // NKikimr::NBsController
