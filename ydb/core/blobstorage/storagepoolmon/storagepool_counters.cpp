#include "storagepool_counters.h"

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>

#include <util/generic/hash_set.h>

namespace NKikimr {

    namespace {

        class TDsProxyInFlightLatencyAggregator
            : public TActorBootstrapped<TDsProxyInFlightLatencyAggregator> {
            static constexpr TDuration PublishInterval = TDuration::Seconds(1);
            static constexpr TDuration SnapshotTtl = TDuration::Seconds(3);

            struct TSourceSnapshot {
                TMonotonic UpdatedAt;
                TVector<TDsProxyInFlightLatencyBucket> Buckets;
            };

            THashMap<TActorId, TSourceSnapshot, TActorId::THash> SourceSnapshots;
            THashSet<TDsProxyInFlightLatencyBucketKey> PublishedBuckets;

        public:
            void Bootstrap() {
                Become(&TThis::StateFunc);
                Schedule(PublishInterval, new TEvents::TEvWakeup);
            }

        private:
            STRICT_STFUNC(StateFunc,
                          hFunc(TEvDsProxyInFlightLatencySnapshot, Handle);
                          cFunc(TEvents::TEvWakeup::EventType, HandleWakeup);
                          cFunc(TEvents::TSystem::Poison, PassAway);)

            void Handle(TEvDsProxyInFlightLatencySnapshot::TPtr& ev) {
                if (ev->Get()->Buckets.empty()) {
                    SourceSnapshots.erase(ev->Sender);
                } else {
                    SourceSnapshots[ev->Sender] = TSourceSnapshot{
                        .UpdatedAt = TActivationContext::Monotonic(),
                        .Buckets = std::move(ev->Get()->Buckets),
                    };
                }
            }

            void HandleWakeup() {
                Publish(TActivationContext::Monotonic());
                Schedule(PublishInterval, new TEvents::TEvWakeup);
            }

            void DropStaleSources(TMonotonic now) {
                for (auto it = SourceSnapshots.begin(); it != SourceSnapshots.end();) {
                    if (now - it->second.UpdatedAt > SnapshotTtl) {
                        auto next = it;
                        ++next;
                        SourceSnapshots.erase(it);
                        it = next;
                    } else {
                        ++it;
                    }
                }
            }

            void Publish(TMonotonic now) {
                DropStaleSources(now);

                THashMap<TDsProxyInFlightLatencyBucketKey, TDsProxyInFlightLatencyStats> aggregate;
                for (const auto& [source, snapshot] : SourceSnapshots) {
                    Y_UNUSED(source);
                    for (const TDsProxyInFlightLatencyBucket& bucket : snapshot.Buckets) {
                        if (!bucket.Key.PoolCounters || !bucket.Stats.InFlightCount) {
                            continue;
                        }
                        aggregate[bucket.Key].Add(bucket.Stats);
                    }
                }

                THashSet<TDsProxyInFlightLatencyBucketKey> currentBuckets;
                for (const auto& [key, stats] : aggregate) {
                    key.PoolCounters->SetInFlightLatencyStats(
                        static_cast<TStoragePoolCounters::EHandleClass>(key.HandleClass),
                        key.SizeClassIdx,
                        stats);
                    currentBuckets.insert(key);
                }

                const TDsProxyInFlightLatencyStats emptyStats;
                for (const TDsProxyInFlightLatencyBucketKey& key : PublishedBuckets) {
                    if (currentBuckets.find(key) == currentBuckets.end() && key.PoolCounters) {
                        key.PoolCounters->SetInFlightLatencyStats(
                            static_cast<TStoragePoolCounters::EHandleClass>(key.HandleClass),
                            key.SizeClassIdx,
                            emptyStats);
                    }
                }

                PublishedBuckets.swap(currentBuckets);
            }
        };

    } // namespace

    IActor* CreateDsProxyInFlightLatencyAggregator() {
        return new TDsProxyInFlightLatencyAggregator;
    }

} // namespace NKikimr
