#pragma once

#include "public.h"

#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/event.h>
#include <ydb/library/actors/core/events.h>

#include <util/generic/set.h>

#include <optional>

namespace NYdb::NBS {

////////////////////////////////////////////////////////////////////////////////

// The owner of TPoisonPillHelper must implement this interface to make it
// possible to kill the parent actor.
class IPoisonPillHelperOwner
{
public:
    virtual void Die(const NActors::TActorContext& ctx) = 0;
};

// Helps to handle the TEvPoisonPill for actor who owns other actors. The helper
// sends TEvPoisonPill to all owned actors and waits for a response
// TEvPoisonTaken from everyone. After that, it responds with the TEvPoisonTaken
// message.
class TPoisonPillHelper
{
private:
    struct TPoisoner
    {
        NActors::TActorId Sender;
        ui64 Cookie = 0;
    };

    IPoisonPillHelperOwner* Owner;
    TSet<NActors::TActorId> OwnedActors;
    std::optional<TPoisoner> Poisoner;

public:
    explicit TPoisonPillHelper(IPoisonPillHelperOwner* owner);
    virtual ~TPoisonPillHelper();

    void TakeOwnership(const NActors::TActorContext& ctx,
                       NActors::TActorId actor);
    void ReleaseOwnership(const NActors::TActorContext& ctx,
                          NActors::TActorId actor);

    void HandlePoisonPill(const NActors::TEvents::TEvPoisonPill::TPtr& ev,
                          const NActors::TActorContext& ctx);

    void HandlePoisonTaken(const NActors::TEvents::TEvPoisonTaken::TPtr& ev,
                           const NActors::TActorContext& ctx);

private:
    void KillActors(const NActors::TActorContext& ctx);
    void ReplyAndDie(const NActors::TActorContext& ctx);
};

}   // namespace NYdb::NBS
