#pragma once

#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/events.h>
#include <util/generic/hash_set.h>

namespace NActors {

    ////////////////////////////////////////////////////////////////////////////
    // TActiveActors
    // This class helps manage created actors and kill them all on PoisonPill.
    ////////////////////////////////////////////////////////////////////////////
    class TActiveActors : public THashSet<TActorId> {
    public:
        void Insert(const TActorId &aid) {
            bool inserted = insert(aid).second;
            Y_ABORT_UNLESS(inserted);
        }

        void Insert(const TActiveActors &moreActors) {
            for (const auto &aid : moreActors) {
                Insert(aid);
            }
        }

        void Erase(const TActorId &aid) {
            auto num = erase(aid);
            Y_ABORT_UNLESS(num == 1);
        }

        size_t KillAndClear(const TActorContext &ctx) {
            size_t s = size(); // number of actors managed
            for (const auto &x: *this) {
                ctx.Send(x, new TEvents::TEvPoisonPill());
            }
            clear();
            return s; // how many actors we killed
        }
    };

} // NKikimr

