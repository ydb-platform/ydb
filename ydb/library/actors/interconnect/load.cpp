#include "load.h"
#include "interconnect_common.h"
#include "events_local.h"
#include <ydb/library/actors/protos/services_common.pb.h>
#include <ydb/library/actors/core/log.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/events.h>
#include <ydb/library/actors/core/hfunc.h>
#include <util/generic/queue.h>

namespace NInterconnect {
    using namespace NActors;

    enum {
        EvGenerateMessages = EventSpaceBegin(TEvents::ES_PRIVATE),
        EvPublishResults,
        EvQueryTrafficCounter,
        EvTrafficCounter,
    };

    struct TEvQueryTrafficCounter : TEventLocal<TEvQueryTrafficCounter, EvQueryTrafficCounter> {};

    struct TEvTrafficCounter : TEventLocal<TEvTrafficCounter, EvTrafficCounter> {
        std::shared_ptr<std::atomic_uint64_t> Traffic;

        TEvTrafficCounter(std::shared_ptr<std::atomic_uint64_t> traffic)
            : Traffic(std::move(traffic))
        {}
    };

    class TLoadResponderActor : public TActor<TLoadResponderActor> {
        STRICT_STFUNC(StateFunc,
            HFunc(TEvLoadMessage, Handle);
            CFunc(TEvents::TSystem::PoisonPill, Die);
        )

        void Handle(TEvLoadMessage::TPtr& ev, const TActorContext& ctx) {
            ui64 bytes = ev->Get()->CalculateSerializedSizeCached();
            auto& record = ev->Get()->Record;
            auto *hops = record.MutableHops();
            while (!hops->empty() && !hops->begin()->HasNextHop()) {
                record.ClearPayload();
                ev->Get()->StripPayload();
                hops->erase(hops->begin());
            }
            if (!hops->empty()) {
                // extract actor id of the next hop
                const TActorId nextHopActorId = ActorIdFromProto(hops->begin()->GetNextHop());
                hops->erase(hops->begin());

                // forward message to next hop; preserve flags and cookie
                auto msg = MakeHolder<TEvLoadMessage>();
                record.Swap(&msg->Record);
                bytes += msg->CalculateSerializedSizeCached();
                ctx.Send(nextHopActorId, msg.Release(), ev->Flags, ev->Cookie);
            }
            *Traffic += bytes;
        }

    public:
        TLoadResponderActor(std::shared_ptr<std::atomic_uint64_t> traffic)
            : TActor(&TLoadResponderActor::StateFunc)
            , Traffic(std::move(traffic))
        {}

        static constexpr IActor::EActivityType ActorActivityType() {
            return IActor::EActivityType::INTERCONNECT_LOAD_RESPONDER;
        }

    private:
        std::shared_ptr<std::atomic_uint64_t> Traffic;
    };

    class TLoadResponderMasterActor : public TActorBootstrapped<TLoadResponderMasterActor> {
        TVector<TActorId> Slaves;
        ui32 SlaveIndex = 0;

        STRICT_STFUNC(StateFunc,
            HFunc(TEvLoadMessage, Handle);
            HFunc(TEvQueryTrafficCounter, Handle);
            CFunc(TEvents::TSystem::PoisonPill, Die);
        )

        void Handle(TEvLoadMessage::TPtr& ev, const TActorContext& ctx) {
            ctx.ExecutorThread.ActorSystem->Send(ev->Forward(Slaves[SlaveIndex]));
            if (++SlaveIndex == Slaves.size()) {
                SlaveIndex = 0;
            }
        }

        void Handle(TEvQueryTrafficCounter::TPtr ev, const TActorContext& ctx) {
            ctx.Send(ev->Sender, new TEvTrafficCounter(Traffic));
        }

        void Die(const TActorContext& ctx) override {
            for (const TActorId& actorId : Slaves) {
                ctx.Send(actorId, new TEvents::TEvPoisonPill);
            }
            TActorBootstrapped::Die(ctx);
        }

    public:
        static constexpr IActor::EActivityType ActorActivityType() {
            return IActor::EActivityType::INTERCONNECT_LOAD_RESPONDER;
        }

        TLoadResponderMasterActor()
        {}

        void Bootstrap(const TActorContext& ctx) {
            Become(&TLoadResponderMasterActor::StateFunc);
            while (Slaves.size() < 10) {
                Slaves.push_back(ctx.Register(new TLoadResponderActor(Traffic)));
            }
        }

    private:
        std::shared_ptr<std::atomic_uint64_t> Traffic = std::make_shared<std::atomic_uint64_t>();
    };

    IActor* CreateLoadResponderActor() {
        return new TLoadResponderMasterActor();
    }

    TActorId MakeLoadResponderActorId(ui32 nodeId) {
        char x[12] = {'I', 'C', 'L', 'o', 'a', 'd', 'R', 'e', 's', 'p', 'A', 'c'};
        return TActorId(nodeId, TStringBuf(x, 12));
    }

    class TLoadActor: public TActorBootstrapped<TLoadActor> {
        struct TEvGenerateMessages : TEventLocal<TEvGenerateMessages, EvGenerateMessages> {};
        struct TEvPublishResults : TEventLocal<TEvPublishResults, EvPublishResults> {};

        struct TMessageInfo {
            TInstant SendTimestamp;

            TMessageInfo(const TInstant& sendTimestamp)
                : SendTimestamp(sendTimestamp)
            {
            }
        };

        const TLoadParams Params;
        TInstant NextMessageTimestamp;
        THashMap<TString, TMessageInfo> InFly;
        ui64 NextId = 1;
        TVector<TActorId> Hops;
        TActorId FirstHop;
        ui64 NumDropped = 0;
        std::shared_ptr<std::atomic_uint64_t> Traffic;

    public:
        static constexpr IActor::EActivityType ActorActivityType() {
            return IActor::EActivityType::INTERCONNECT_LOAD_ACTOR;
        }

        TLoadActor(const TLoadParams& params)
            : Params(params)
        {}

        void Bootstrap(const TActorContext& ctx) {
            Become(&TLoadActor::QueryTrafficCounter);
            ctx.Send(MakeLoadResponderActorId(SelfId().NodeId()), new TEvQueryTrafficCounter);
        }

        void Handle(TEvTrafficCounter::TPtr ev, const TActorContext& ctx) {
            Traffic = std::move(ev->Get()->Traffic);

            for (const ui32 nodeId : Params.NodeHops) {
                const TActorId& actorId = nodeId ? MakeLoadResponderActorId(nodeId) : TActorId();
                if (!FirstHop) {
                    FirstHop = actorId;
                } else {
                    Hops.push_back(actorId);
                }
            }

            Hops.push_back(ctx.SelfID);

            Become(&TLoadActor::StateFunc);
            NextMessageTimestamp = ctx.Now();
            ResetThroughput(NextMessageTimestamp, *Traffic);
            GenerateMessages(ctx);
            ctx.Schedule(Params.Duration, new TEvents::TEvPoisonPill);
            SchedulePublishResults(ctx);
        }

        void GenerateMessages(const TActorContext& ctx) {
            while (InFly.size() < Params.InFlyMax && ctx.Now() >= NextMessageTimestamp) {
                // generate payload
                const ui32 size = Params.SizeMin + RandomNumber(Params.SizeMax - Params.SizeMin + 1);

                // generate message id
                const ui64 cookie = NextId++;
                TString id = Sprintf("%" PRIu64, cookie);

                // create message and send it to the first hop
                THolder<TEvLoadMessage> ev;
                if (Params.UseProtobufWithPayload && size) {
                    auto buffer = TRopeAlignedBuffer::Allocate(size);
                    memset(buffer->GetBuffer(), '*', size);
                    ev.Reset(new TEvLoadMessage(Hops, id, TRope(buffer)));
                } else {
                    TString payload;
                    if (size) {
                        payload = TString::Uninitialized(size);
                        memset(payload.Detach(), '*', size);
                    }
                    ev.Reset(new TEvLoadMessage(Hops, id, payload ? &payload : nullptr));
                }
                UpdateThroughput(ev->CalculateSerializedSizeCached());
                ctx.Send(FirstHop, ev.Release(), IEventHandle::MakeFlags(Params.Channel, 0), cookie);

                // register in the map
                InFly.emplace(id, TMessageInfo(ctx.Now()));

                // put item into timeout queue
                PutTimeoutQueueItem(ctx, id);

                const TDuration duration = TDuration::MicroSeconds(Params.IntervalMin.GetValue() +
                    RandomNumber(Params.IntervalMax.GetValue() - Params.IntervalMin.GetValue() + 1));
                if (Params.SoftLoad) {
                    NextMessageTimestamp += duration;
                } else {
                    NextMessageTimestamp = ctx.Now() + duration;
                }
            }

            // schedule next generate messages call
            if (NextMessageTimestamp > ctx.Now() && InFly.size() < Params.InFlyMax) {
                ctx.Schedule(NextMessageTimestamp - ctx.Now(), new TEvGenerateMessages);
            }
        }

        void Handle(TEvLoadMessage::TPtr& ev, const TActorContext& ctx) {
            const auto& record = ev->Get()->Record;
            auto it = InFly.find(record.GetId());
            if (it != InFly.end()) {
                // record message rtt
                const TDuration rtt = ctx.Now() - it->second.SendTimestamp;
                UpdateHistogram(ctx.Now(), rtt);

                // update throughput
                UpdateThroughput(ev->Get()->CalculateSerializedSizeCached());

                // remove message from the in fly map
                InFly.erase(it);
            } else {
                ++NumDropped;
            }
            GenerateMessages(ctx);
        }

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // RTT HISTOGRAM
        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////

        const TDuration AggregationPeriod = TDuration::Seconds(20);
        TDeque<std::pair<TInstant, TDuration>> Histogram;

        void UpdateHistogram(TInstant when, TDuration rtt) {
            Histogram.emplace_back(when, rtt);

            const TInstant barrier = when - AggregationPeriod;
            while (Histogram && Histogram.front().first < barrier) {
                Histogram.pop_front();
            }
        }

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // THROUGHPUT
        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////

        TInstant ThroughputFirstSample = TInstant::Zero();
        ui64 ThroughputSamples = 0;
        ui64 ThroughputBytes = 0;
        ui64 TrafficAtBegin = 0;

        void UpdateThroughput(ui64 bytes) {
            ThroughputBytes += bytes;
            ++ThroughputSamples;
        }

        void ResetThroughput(TInstant when, ui64 traffic) {
            ThroughputFirstSample = when;
            ThroughputSamples = 0;
            ThroughputBytes = 0;
            TrafficAtBegin = traffic;
        }

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // TIMEOUT QUEUE OPERATIONS
        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////

        TQueue<std::pair<TInstant, TString>> TimeoutQueue;

        void PutTimeoutQueueItem(const TActorContext& ctx, TString id) {
            TimeoutQueue.emplace(ctx.Now() + TDuration::Minutes(1), std::move(id));
            if (TimeoutQueue.size() == 1) {
                ScheduleWakeup(ctx);
            }
        }

        void ScheduleWakeup(const TActorContext& ctx) {
            ctx.Schedule(TimeoutQueue.front().first - ctx.Now(), new TEvents::TEvWakeup);
        }

        void HandleWakeup(const TActorContext& ctx) {
            // ui32 numDropped = 0;

            while (TimeoutQueue && TimeoutQueue.front().first <= ctx.Now()) {
                /*numDropped += */InFly.erase(TimeoutQueue.front().second);
                TimeoutQueue.pop();
            }
            if (TimeoutQueue) {
                // we still have some elements in timeout queue, so schedule next wake up to tidy up
                ScheduleWakeup(ctx);
            }

            GenerateMessages(ctx);
        }

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // RESULT PUBLISHING
        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////

        const TDuration ResultPublishPeriod = TDuration::Seconds(15);

        void SchedulePublishResults(const TActorContext& ctx) {
            ctx.Schedule(ResultPublishPeriod, new TEvPublishResults);
        }

        void PublishResults(const TActorContext& ctx, bool schedule = true) {
            const TInstant now = ctx.Now();

            TStringStream msg;

            msg << "Load# '" << Params.Name << "'";

            msg << " Throughput# ";
            const TDuration duration = now - ThroughputFirstSample;
            const ui64 traffic = *Traffic;
            msg << "{window# " << duration
                << " bytes# " << ThroughputBytes
                << " samples# " << ThroughputSamples
                << " b/s# " << ui64(ThroughputBytes * 1000000 / duration.MicroSeconds())
                << " common# " << ui64((traffic - TrafficAtBegin) * 1000000 / duration.MicroSeconds())
                << "}";
            ResetThroughput(now, traffic);

            msg << " RTT# ";
            if (Histogram) {
                const TDuration duration = Histogram.back().first - Histogram.front().first;
                msg << "{window# " << duration << " samples# " << Histogram.size();
                TVector<TDuration> v;
                v.reserve(Histogram.size());
                for (const auto& item : Histogram) {
                    v.push_back(item.second);
                }
                std::sort(v.begin(), v.end());
                for (double q : {0.5, 0.9, 0.99, 0.999, 0.9999, 1.0}) {
                    const size_t pos = q * (v.size() - 1);
                    msg << Sprintf(" %.4f# %s", q, v[pos].ToString().data());
                }
                msg << "}";
            } else {
                msg << "<empty>";
            }

            msg << " NumDropped# " << NumDropped;

            if (!schedule) {
                msg << " final";
            }

            LOG_NOTICE(ctx, NActorsServices::INTERCONNECT_SPEED_TEST, "%s", msg.Str().data());

            if (schedule) {
                SchedulePublishResults(ctx);
            }
        }

        STRICT_STFUNC(QueryTrafficCounter,
            HFunc(TEvTrafficCounter, Handle);
        )

        STRICT_STFUNC(StateFunc,
            CFunc(TEvents::TSystem::PoisonPill, Die);
            CFunc(TEvents::TSystem::Wakeup, HandleWakeup);
            CFunc(EvPublishResults, PublishResults);
            CFunc(EvGenerateMessages, GenerateMessages);
            HFunc(TEvLoadMessage, Handle);
        )

        void Die(const TActorContext& ctx) override {
            PublishResults(ctx, false);
            TActorBootstrapped::Die(ctx);
        }
    };

    IActor* CreateLoadActor(const TLoadParams& params) {
        return new TLoadActor(params);
    }

}
