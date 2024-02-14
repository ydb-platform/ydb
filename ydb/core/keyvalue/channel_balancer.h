#pragma once

#include "defs.h"
#include "keyvalue_events.h"
#include <bitset>

namespace NKikimr::NKeyValue {

    class TChannelBalancer : public TActorBootstrapped<TChannelBalancer> {
    public:
        class TWeightManager {
            TVector<ui64> Weights;

        public:
            TWeightManager(TVector<ui64> weights)
                : Weights(std::move(weights))
            {
                Y_ABORT_UNLESS(0 < Weights.size() && Weights.size() <= 256);
            }

            int Pick(const std::bitset<256>& enabled) const {
                std::array<ui64, 256> accum;
                ui64 counter = 0;
                for (size_t i = 0; i < Weights.size(); ++i) {
                    if (enabled[i]) {
                        counter += Weights[i];
                    }
                    accum[i] = counter;
                }
                if (!counter) {
                    return -1;
                }

                const ui64 r = RandomNumber(counter);
                const auto begin = accum.begin();
                const auto end = begin + Weights.size();
                const auto iter = std::upper_bound(begin, end, r);
                Y_ABORT_UNLESS(iter != end);
                return iter - begin;
            }
        };

        struct TEvReportWriteLatency : TEventLocal<TEvReportWriteLatency, TEvKeyValue::EvReportWriteLatency> {
            const ui8 Channel;
            const TDuration Latency;

            TEvReportWriteLatency(ui8 channel, TDuration latency)
                : Channel(channel)
                , Latency(latency)
            {}
        };

        struct TEvUpdateWeights : TEventLocal<TEvUpdateWeights, TEvKeyValue::EvUpdateWeights> {
            THolder<TWeightManager> WeightManager;

            TEvUpdateWeights(THolder<TWeightManager>&& wm)
                : WeightManager(std::move(wm))
            {}
        };

    private:
        static constexpr TDuration UpdateWeightsTimeout = TDuration::Seconds(10);

        class TChannelInfo {
            struct TLatencyRecord {
                TInstant Timestamp;
                TDuration Latency;
            };

            static constexpr TDuration AggregationWindow = TDuration::Minutes(10);
            static constexpr ui32 MaxSamples = 65536;
            static constexpr TDuration MeanExpectedLatency = TDuration::MilliSeconds(100);
            static constexpr ui32 MinSamplesToDecide = 10;

            TDeque<TLatencyRecord> LatencyQueue;
            TMaybe<ui64> PrevWeight;

        public:
            void ReportWriteLatency(TInstant timestamp, TDuration latency) {
                Cleanup(timestamp);
                if (LatencyQueue.size() == MaxSamples) {
                    TDeque<TLatencyRecord> filtered;
                    for (auto it = LatencyQueue.begin(); it != LatencyQueue.end(); it += 2) {
                        filtered.push_back(*it);
                    }
                    LatencyQueue = std::move(filtered);
                }
                LatencyQueue.push_back(TLatencyRecord{timestamp, latency});
            }

            ui64 UpdateWeight(TInstant timestamp) {
                // IIR filter with depth=1
                ui64 w = GetWeight(timestamp);
                if (PrevWeight) {
                    w = (w + *PrevWeight * 9) / 10;
                }
                PrevWeight = w;
                return w;
            }

        private:
            ui64 GetWeight(TInstant timestamp) {
                Cleanup(timestamp);

                ui64 weight = 1000;

                if (LatencyQueue.size() >= MinSamplesToDecide) {
                    TVector<TDuration> latencies;
                    latencies.reserve(LatencyQueue.size());
                    for (const TLatencyRecord& record : LatencyQueue) {
                        latencies.push_back(record.Latency);
                    }
                    std::sort(latencies.begin(), latencies.end());
                    const size_t index = (LatencyQueue.size() - 1) * 99 / 100;
                    const TDuration perc = latencies[index];
                    weight = MeanExpectedLatency.GetValue() * weight / Max(perc, TDuration::MilliSeconds(1)).GetValue();
                    //Y_DEBUG_ABORT_UNLESS(weight);
                    if (!weight) {
                        weight = 1;
                    }
                }

                return weight;
            }

            void Cleanup(TInstant now) {
                // leave only aggregation window samples
                while (LatencyQueue && now - LatencyQueue.front().Timestamp > AggregationWindow) {
                    LatencyQueue.pop_front();
                }
            }
        };

        TVector<TChannelInfo> ChannelInfo;
        const TActorId ActorId;

    public:
        static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
            return NKikimrServices::TActivity::KEYVALUE_ACTOR;
        }

        TChannelBalancer(ui8 numChannels, TActorId actorId)
            : ChannelInfo(numChannels)
            , ActorId(actorId)
        {}

        void Bootstrap(const TActorContext& ctx) {
            Become(&TThis::StateFunc, ctx, UpdateWeightsTimeout, new TEvents::TEvWakeup);
        }

        STRICT_STFUNC(StateFunc,
            HFunc(TEvReportWriteLatency, Handle)
            CFunc(TEvents::TSystem::Wakeup, HandleWakeup)
            CFunc(TEvents::TSystem::PoisonPill, Die)
        )

        void Handle(TEvReportWriteLatency::TPtr ev, const TActorContext& ctx) {
            const ui8 channel = ev->Get()->Channel;
            if (channel < ChannelInfo.size()) {
                TChannelInfo& info = ChannelInfo[channel];
                info.ReportWriteLatency(ctx.Now(), ev->Get()->Latency);
            }
        }

        void HandleWakeup(const TActorContext& ctx) {
            ctx.Schedule(UpdateWeightsTimeout, new TEvents::TEvWakeup);
            const TInstant now = ctx.Now();
            TVector<ui64> weights;
            weights.reserve(ChannelInfo.size());
            for (TChannelInfo& info : ChannelInfo) {
                weights.push_back(info.UpdateWeight(now));
            }
            ctx.Send(ActorId, new TEvUpdateWeights(MakeHolder<TWeightManager>(std::move(weights))));
        }
    };

} // NKikimr::NKeyValue
