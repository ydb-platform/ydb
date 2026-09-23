#pragma once

#include "public.h"

#include <ydb/core/nbs/cloud/storage/core/libs/common/error.h>

#include <library/cpp/threading/future/future.h>

namespace NActors {
class TActorSystem;
struct TActorId;
}   // namespace NActors

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

// Submits session commands to one partition incarnation without exposing its
// transport. Calls return without waiting for command completion.
struct IPartitionSessionControl
{
    virtual ~IPartitionSessionControl() = default;

    // Creates or reuses a session and returns its ID or an error.
    virtual NThreading::TFuture<TResultOrError<TString>> Mount(
        const TString& clientId) = 0;

    // Revokes the matching session on this partition incarnation.
    virtual NThreading::TFuture<NProto::TError> Unmount(
        const TString& clientId,
        const TString& sessionId) = 0;
};

// Binds commands to a partition actor owning one frontend registration.
// Does not own the actor system: all command submissions must finish before
// actor-system shutdown. The actor may disappear while commands are in flight.
IPartitionSessionControlPtr CreatePartitionSessionControl(
    NActors::TActorSystem* actorSystem,
    const NActors::TActorId& actorId);

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
