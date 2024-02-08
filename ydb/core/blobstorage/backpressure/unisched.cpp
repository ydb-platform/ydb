#include "unisched.h"
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/core/base/events.h>
#include <util/generic/set.h>

namespace NKikimr {

    using namespace NActors;

    class TUniversalSchedulerActor : public TActor<TUniversalSchedulerActor> {
        enum {
            EvRegister = EventSpaceBegin(TKikimrEvents::ES_PRIVATE),
        };

    public:
        struct TEvRegister : TEventLocal<TEvRegister, EvRegister> {
            const TActorId ActorId;
            TIntrusivePtr<NBackpressure::TFlowRecord> FlowRecord;

            TEvRegister(const TActorId& actorId, TIntrusivePtr<NBackpressure::TFlowRecord> flowRecord)
                : ActorId(actorId)
                , FlowRecord(std::move(flowRecord))
            {}
        };

    private:
        class TSubschedulerActor : public TActorBootstrapped<TSubschedulerActor> {
            const TDuration Interval;
            THashMap<TActorId, TIntrusivePtr<NBackpressure::TFlowRecord>> Subscribers;

        public:
            TSubschedulerActor(TDuration interval)
                : Interval(interval)
            {}

            void Bootstrap(const TActorContext& ctx) {
                Become(&TThis::StateFunc);
                HandleWakeup(ctx);
            }

            void Handle(TEvRegister::TPtr& ev, const TActorContext& /*ctx*/) {
                TEvRegister *msg = ev->Get();
                if (msg->FlowRecord) {
                    const bool inserted = Subscribers.emplace(msg->ActorId, std::move(msg->FlowRecord)).second;
                    Y_ABORT_UNLESS(inserted);
                } else {
                    const ui32 numErased = Subscribers.erase(msg->ActorId);
                    Y_ABORT_UNLESS(numErased == 1);
                }
            }

            void HandleWakeup(const TActorContext& ctx) {
                for (const auto& [key, value] : Subscribers) {
                    if (value->GetPredictedDelayNs()) {
                        ctx.Send(key, new TEvents::TEvWakeup);
                    }
                }
                ctx.Schedule(GenerateIntervalWithJitter(), new TEvents::TEvWakeup);
            }

            STRICT_STFUNC(StateFunc,
                CFunc(TEvents::TSystem::Poison, Die);
                HFunc(TEvRegister, Handle);
                CFunc(TEvents::TSystem::Wakeup, HandleWakeup);
            )

        private:
            TDuration GenerateIntervalWithJitter() const {
                auto range = Interval.GetValue() / 64 + 1;
                return TDuration::FromValue(Interval.GetValue() + RandomNumber(range) - range / 2);
            }
        };

    private:
        struct TSubschedulerInfo {
            TActorId ActorId;
            size_t NumSubscribers = 0;

            TSubschedulerInfo(const TActorId& actorId)
                : ActorId(actorId)
            {}

            friend bool operator <(const TSubschedulerInfo& x, const TSubschedulerInfo& y) {
                return x.NumSubscribers > y.NumSubscribers || // stored in reversed order
                    (x.NumSubscribers == y.NumSubscribers && x.ActorId < y.ActorId);
            };
        };

    private:
        const size_t MaxActorsPerSubscheduler = 1024;
        const TDuration Interval;
        THashMap<TActorId, TActorId> SubscriberToSubscheduler; // subscriber to subscheduler actor id map
        using TOrderedSubschedulerSet = TSet<TSubschedulerInfo>;
        TOrderedSubschedulerSet PartialSubschedulers;
        TOrderedSubschedulerSet FullSubschedulers;
        THashMap<TActorId, TOrderedSubschedulerSet::iterator> SubschedulerToOrderedSetPosition;

    public:
        TUniversalSchedulerActor(const TDuration interval = TDuration::Seconds(1))
            : TActor(&TThis::StateFunc)
            , Interval(interval)
        {}

        void Handle(TEvRegister::TPtr& ev, const TActorContext& ctx) {
            TEvRegister *msg = ev->Get();
            (this->*(msg->FlowRecord ? &TThis::RegisterSubscriber : &TThis::UnregisterSubscriber))(msg->ActorId,
                std::unique_ptr<TEvRegister>(ev->Release().Release()), ctx);
        }

        void RegisterSubscriber(const TActorId& actorId, std::unique_ptr<TEvRegister> msg, const TActorContext& ctx) {
            TActorId subschedulerActorId;

            // first, check if we have partially filled subscheduler actor -- if so, find the most filled one and stuff
            // it with one more subscriber
            if (!PartialSubschedulers) {
                PartialSubschedulers.emplace_hint(PartialSubschedulers.end(), ctx.Register(new TSubschedulerActor(Interval)));
            }

            // we must have one partial subscheduler actor here; if not, it was created a few lines ago
            TOrderedSubschedulerSet::iterator it;
            auto node = PartialSubschedulers.extract(PartialSubschedulers.begin());
            subschedulerActorId = node.value().ActorId;

            // update node usage counter
            if (++node.value().NumSubscribers == MaxActorsPerSubscheduler) {
                // this actor is now full one, so we have to move it to the full subschedulers set
                auto res = FullSubschedulers.insert(std::move(node));
                Y_ABORT_UNLESS(res.inserted);
                it = res.position;
            } else {
                // put this actor back
                it = PartialSubschedulers.insert(PartialSubschedulers.begin(), std::move(node));
            }

            // update subscheduler actor reverse iterator mapping
            SubschedulerToOrderedSetPosition[subschedulerActorId] = it;

            // forward message to subscheduler actor
            const bool inserted = SubscriberToSubscheduler.emplace(actorId, subschedulerActorId).second;
            Y_ABORT_UNLESS(inserted);
            ctx.Send(subschedulerActorId, msg.release());
        }

        void UnregisterSubscriber(const TActorId& actorId, std::unique_ptr<TEvRegister> msg, const TActorContext& ctx) {
            // find the subscheduler actor for this subscriber
            auto it = SubscriberToSubscheduler.find(actorId);
            Y_ABORT_UNLESS(it != SubscriberToSubscheduler.end());

            // erase it from the map, remembering its actor id
            const TActorId subschedulerActorId = it->second;
            SubscriberToSubscheduler.erase(it);

            // send unregister message to the actor
            ctx.Send(subschedulerActorId, msg.release());

            // find this subscheduler and its matching ordered set position
            auto subsIt = SubschedulerToOrderedSetPosition.find(subschedulerActorId);
            Y_ABORT_UNLESS(subsIt != SubschedulerToOrderedSetPosition.end());

            // determine the set to which this subscheduler belongs and extract its node from the set
            auto& set = subsIt->second->NumSubscribers != MaxActorsPerSubscheduler ? PartialSubschedulers : FullSubschedulers;
            auto node = set.extract(subsIt->second);
            Y_ABORT_UNLESS(node.value().ActorId == subschedulerActorId);

            if (!--node.value().NumSubscribers) {
                // this child actor has no more subscribers left -- kill the actor, forget it here
                ctx.Send(subschedulerActorId, new TEvents::TEvPoison);
                SubschedulerToOrderedSetPosition.erase(subsIt);
            } else {
                // this child actor still has subscribers, but it is in PartialSubschedulers for sure now
                auto res = PartialSubschedulers.insert(std::move(node));
                Y_ABORT_UNLESS(res.inserted);
                subsIt->second = res.position;
            }
        }

        void Die(const TActorContext& ctx) override {
            for (const auto& [subschedulerActorId, it] : SubschedulerToOrderedSetPosition) {
                ctx.Send(subschedulerActorId, new TEvents::TEvPoison);
            }
            TActor::Die(ctx);
        }

        STRICT_STFUNC(StateFunc,
            CFunc(TEvents::TSystem::Poison, Die);
            HFunc(TEvRegister, Handle);
        )
    };

    IActor *CreateUniversalSchedulerActor() {
        return new TUniversalSchedulerActor;
    }

    bool RegisterActorInUniversalScheduler(const TActorId& actorId, TIntrusivePtr<NBackpressure::TFlowRecord> flowRecord,
            TActorSystem *actorSystem) {
        return actorSystem->Send(MakeUniversalSchedulerActorId(), new TUniversalSchedulerActor::TEvRegister(actorId,
            std::move(flowRecord)));
    }

} // NKikimr
