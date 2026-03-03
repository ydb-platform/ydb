#include "poison_pill_helper.h"

#include <ydb/core/nbs/cloud/storage/core/libs/actors/helpers.h>

using namespace NActors;

namespace NYdb::NBS {

////////////////////////////////////////////////////////////////////////////////

TPoisonPillHelper::TPoisonPillHelper(IPoisonPillHelperOwner* owner)
    : Owner(owner)
{}

TPoisonPillHelper::~TPoisonPillHelper() = default;

void TPoisonPillHelper::TakeOwnership(const TActorContext& ctx,
                                      NActors::TActorId actor)
{
    if (actor == TActorId()) {
        return;
    }
    OwnedActors.insert(actor);
    if (Poisoner) {
        NYdb::NBS::Send<TEvents::TEvPoisonPill>(ctx, actor);
    }
}

void TPoisonPillHelper::ReleaseOwnership(const NActors::TActorContext& ctx,
                                         NActors::TActorId actor)
{
    OwnedActors.erase(actor);
    ReplyAndDie(ctx);
}

void TPoisonPillHelper::HandlePoisonPill(
    const NActors::TEvents::TEvPoisonPill::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    Y_DEBUG_ABORT_UNLESS(!Poisoner);

    Poisoner = TPoisoner{ev->Sender, ev->Cookie};
    KillActors(ctx);
    ReplyAndDie(ctx);
}

void TPoisonPillHelper::HandlePoisonTaken(
    const NActors::TEvents::TEvPoisonTaken::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    OwnedActors.erase(ev->Sender);
    ReplyAndDie(ctx);
}

void TPoisonPillHelper::KillActors(const TActorContext& ctx)
{
    for (auto actor: OwnedActors) {
        NYdb::NBS::Send<TEvents::TEvPoisonPill>(ctx, actor);
    }
}

void TPoisonPillHelper::ReplyAndDie(const TActorContext& ctx)
{
    if (!Poisoner || !OwnedActors.empty()) {
        return;
    }

    ctx.Send(
        Poisoner->Sender,
        std::make_unique<TEvents::TEvPoisonTaken>(),
        0,   // flags
        Poisoner->Cookie);
    Owner->Die(ctx);
}

}   // namespace NYdb::NBS
