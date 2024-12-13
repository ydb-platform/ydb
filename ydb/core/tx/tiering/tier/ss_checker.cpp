#include "ss_checker.h"

#include <ydb/core/tx/scheme_cache/scheme_cache.h>
#include <ydb/library/services/services.pb.h>

namespace NKikimr::NColumnShard::NTiers {

void TSSFetchingActor::Handle(NSchemeShard::TEvSchemeShard::TEvProcessingResponse::TPtr& ev) {
    auto g = PassAwayGuard();
    Controller->FetchingResult(ev->Get()->Record);
}

TSSFetchingActor::TSSFetchingActor(NSchemeShard::ISSDataProcessor::TPtr processor,
    ISSFetchingController::TPtr controller, const TDuration livetime)
    : TBase(livetime)
    , Processor(processor)
    , Controller(controller)
{

}

constexpr NKikimrServices::TActivity::EType TSSFetchingActor::ActorActivityType() {
    return NKikimrServices::TActivity::SS_FETCHING_ACTOR;
}

}
