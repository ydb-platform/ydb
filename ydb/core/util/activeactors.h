#pragma once

#include <library/cpp/actors/core/actor.h>
#include <library/cpp/actors/core/events.h>
#include <library/cpp/actors/core/log.h>
#include <util/generic/hash_set.h>
#include <ydb/library/services/services.pb.h>

namespace NActors {

    ////////////////////////////////////////////////////////////////////////////
    // TActiveActors
    // This class helps manage created actors and kill them all on PoisonPill.
    ////////////////////////////////////////////////////////////////////////////
    class TActiveActors {
        struct TActivityCounter {
            ui32 Value = 0;
            bool Warning = false;
        };

        using TActivityCounters = std::unordered_map<TString, TActivityCounter>;
        TActivityCounters ActivityCounters;
        std::unordered_map<TActorId, TActivityCounters::value_type*> Actors;

        const ui32 CounterLimit = 1'000'000;

    public:
        void Insert(const TActorId &aid, const char *file, int line, const TActorContext& ctx,
                NKikimrServices::EServiceKikimr service) {
            TString key = TStringBuilder() << file << ':' << line;
            const auto it = ActivityCounters.try_emplace(key).first;
            AddValue(it->first, it->second, 1, ctx, service);
            const bool inserted = Actors.emplace(aid, &*it).second;
            Y_VERIFY(inserted); // value must be unique
        }

        void Insert(TActiveActors&& moreActors, const TActorContext& ctx, NKikimrServices::EServiceKikimr service) {
            ActivityCounters.merge(moreActors.ActivityCounters);
            for (auto& [key, value] : moreActors.Actors) {
                if (moreActors.ActivityCounters.contains(value->first)) { // remap to local ActivityCounters
                    const auto it = ActivityCounters.find(value->first);
                    Y_VERIFY(it != ActivityCounters.end());
                    value = &*it;
                }
            }
            for (const auto& [key, value] : moreActors.ActivityCounters) { // merge existing activity counters
                AddValue(key, ActivityCounters[key], value.Value, ctx, service);
            }
            Actors.merge(moreActors.Actors);
            moreActors.ActivityCounters.clear();
            Y_VERIFY_DEBUG(moreActors.Actors.empty());
        }

        void Erase(const TActorId &aid) {
            auto nh = Actors.extract(aid);
            Y_VERIFY(nh);
            TActivityCounters::value_type *valptr = nh.mapped();
            Y_VERIFY(valptr->second.Value);
            --valptr->second.Value;
        }

        size_t KillAndClear(const TActorContext &ctx) {
            size_t s = 0;
            for (const auto& [key, value] : Actors) {
                ctx.Send(key, new TEvents::TEvPoison);
                ++s;
            }
            ActivityCounters.clear();
            Actors.clear();
            return s; // how many actors we killed
        }

    private:
        void AddValue(const TString& key, TActivityCounter& counter, ui32 value, const TActorContext& ctx,
                NKikimrServices::EServiceKikimr service) {
            counter.Value += value;
            if (counter.Value >= CounterLimit && !std::exchange(counter.Warning, true)) {
                LOG_CRIT_S(ctx, service, "Activity " << key << " reached active actors limit");
                Y_VERIFY_DEBUG(false, "Activity %s reached active actors limit", key.c_str());
            }
        }
    };

} // NKikimr

