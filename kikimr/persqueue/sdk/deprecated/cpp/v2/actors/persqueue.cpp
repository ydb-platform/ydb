#include "actor_wrappers.h"
#include "persqueue.h"

#include <kikimr/persqueue/sdk/deprecated/cpp/v2/impl/internals.h>

namespace NPersQueue {

TPQLibActorsWrapper::TPQLibActorsWrapper(NActors::TActorSystem* actorSystem, const TPQLibSettings& settings)
    : PQLib(new TPQLib(settings))
    , ActorSystem(actorSystem)
{
}

TPQLibActorsWrapper::TPQLibActorsWrapper(NActors::TActorSystem* actorSystem, std::shared_ptr<TPQLib> pqLib)
    : PQLib(std::move(pqLib))
    , ActorSystem(actorSystem)
{
}

TPQLibActorsWrapper::~TPQLibActorsWrapper() = default;

} // namespace NPersQueue
