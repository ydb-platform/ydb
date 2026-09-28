#include "partition_session_control.h"

#include "events.h"

#include <ydb/library/actors/core/actorsystem.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

namespace {

// Delivers commands to the owner actor; session state is changed only there.
class TPartitionSessionControl final: public IPartitionSessionControl
{
public:
    TPartitionSessionControl(
        NActors::TActorSystem* actorSystem,
        const NActors::TActorId& actorId);

    NThreading::TFuture<TResultOrError<TString>> Mount(
        const TString& clientId) override;

    NThreading::TFuture<NProto::TError> Unmount(
        const TString& clientId,
        const TString& sessionId) override;

private:
    NActors::TActorSystem* const ActorSystem;
    const NActors::TActorId ActorId;
};

TPartitionSessionControl::TPartitionSessionControl(
    NActors::TActorSystem* actorSystem,
    const NActors::TActorId& actorId)
    : ActorSystem(actorSystem)
    , ActorId(actorId)
{
    Y_ABORT_UNLESS(ActorSystem && ActorId);
}

NThreading::TFuture<TResultOrError<TString>> TPartitionSessionControl::Mount(
    const TString& clientId)
{
    auto event = std::make_unique<TEvPartitionSession::TEvMount>(clientId);
    auto future = event->Result.GetFuture();
    ActorSystem->Send(ActorId, event.release());
    return future;
}

NThreading::TFuture<NProto::TError> TPartitionSessionControl::Unmount(
    const TString& clientId,
    const TString& sessionId)
{
    auto event =
        std::make_unique<TEvPartitionSession::TEvUnmount>(clientId, sessionId);
    auto future = event->Result.GetFuture();
    ActorSystem->Send(ActorId, event.release());
    return future;
}

}   // namespace

IPartitionSessionControlPtr CreatePartitionSessionControl(
    NActors::TActorSystem* actorSystem,
    const NActors::TActorId& actorId)
{
    return std::make_shared<TPartitionSessionControl>(actorSystem, actorId);
}

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
