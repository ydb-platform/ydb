#include "session.h"
#include "actor.h"

namespace NKikimr::NSchemeShard::NOlap::NBackground {

TConclusion<std::unique_ptr<NActors::IActor>> TTxChainSession::DoCreateActor(const NKikimr::NOlap::NBackground::TStartContext& context) const {
    return std::make_unique<TTxChainActor>(context.GetSessionSelfPtr(), context.GetAdapter());
}

}