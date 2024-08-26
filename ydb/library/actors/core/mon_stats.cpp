#include "mon_stats.h"


namespace NActors {

    TLogHistogram::TLogHistogram() {
        memset(Buckets, 0, sizeof(Buckets));
    }

    void TLogHistogram::Aggregate(const TLogHistogram& other) {
        const ui64 inc = RelaxedLoad(&other.TotalSamples);
        RelaxedStore(&TotalSamples, RelaxedLoad(&TotalSamples) + inc);
        for (size_t i = 0; i < Y_ARRAY_SIZE(Buckets); ++i) {
            Buckets[i] += RelaxedLoad(&other.Buckets[i]);
        }
    }

    ui32 TLogHistogram::Count() const {
        return Y_ARRAY_SIZE(Buckets);
    }

    NMonitoring::TBucketBound TLogHistogram::UpperBound(ui32 index) const {
        Y_ASSERT(index < Y_ARRAY_SIZE(Buckets));
        if (index == 0) {
            return 1;
        }
        return NMonitoring::TBucketBound(1ull << (index - 1)) * 2.0;
    }

    NMonitoring::TBucketValue TLogHistogram::Value(ui32 index) const {
        Y_ASSERT(index < Y_ARRAY_SIZE(Buckets));
        return Buckets[index];
    }

    TExecutorThreadStats::TExecutorThreadStats() // must be not empty as 0 used as default
        : ElapsedTicksByActivity(TLocalProcessKeyStateIndexLimiter::GetMaxKeysCount())
        , ReceivedEventsByActivity(TLocalProcessKeyStateIndexLimiter::GetMaxKeysCount())
        , ActorsAliveByActivity(TLocalProcessKeyStateIndexLimiter::GetMaxKeysCount())
        , ScheduledEventsByActivity(TLocalProcessKeyStateIndexLimiter::GetMaxKeysCount())
        , StuckActorsByActivity(TLocalProcessKeyStateIndexLimiter::GetMaxKeysCount())
        , UsageByActivity(TLocalProcessKeyStateIndexLimiter::GetMaxKeysCount())
    {}

    namespace {
        template <typename T>
        void AggregateOne(TVector<T>& self, const TVector<T>& other) {
            const size_t selfSize = self.size();
            const size_t otherSize = other.size();
            if (selfSize < otherSize)
                self.resize(otherSize);
            for (size_t at = 0; at < otherSize; ++at)
                self[at] += RelaxedLoad(&other[at]);
        }
    }

    void TExecutorThreadStats::Aggregate(const TExecutorThreadStats& other) {
        SentEvents += RelaxedLoad(&other.SentEvents);
        ReceivedEvents += RelaxedLoad(&other.ReceivedEvents);
        PreemptedEvents += RelaxedLoad(&other.PreemptedEvents);
        NonDeliveredEvents += RelaxedLoad(&other.NonDeliveredEvents);
        EmptyMailboxActivation += RelaxedLoad(&other.EmptyMailboxActivation);
        CpuUs += RelaxedLoad(&other.CpuUs);
        SafeElapsedTicks += RelaxedLoad(&other.SafeElapsedTicks);
        RelaxedStore(
            &WorstActivationTimeUs,
            std::max(RelaxedLoad(&WorstActivationTimeUs), RelaxedLoad(&other.WorstActivationTimeUs)));
        ElapsedTicks += RelaxedLoad(&other.ElapsedTicks);
        ParkedTicks += RelaxedLoad(&other.ParkedTicks);
        BlockedTicks += RelaxedLoad(&other.BlockedTicks);
        MailboxPushedOutByTailSending += RelaxedLoad(&other.MailboxPushedOutByTailSending);
        MailboxPushedOutBySoftPreemption += RelaxedLoad(&other.MailboxPushedOutBySoftPreemption);
        MailboxPushedOutByTime += RelaxedLoad(&other.MailboxPushedOutByTime);
        MailboxPushedOutByEventCount += RelaxedLoad(&other.MailboxPushedOutByEventCount);
        NotEnoughCpuExecutions += RelaxedLoad(&other.NotEnoughCpuExecutions);

        ActivationTimeHistogram.Aggregate(other.ActivationTimeHistogram);
        EventDeliveryTimeHistogram.Aggregate(other.EventDeliveryTimeHistogram);
        EventProcessingCountHistogram.Aggregate(other.EventProcessingCountHistogram);
        EventProcessingTimeHistogram.Aggregate(other.EventProcessingTimeHistogram);

        AggregateOne(ElapsedTicksByActivity, other.ElapsedTicksByActivity);
        AggregateOne(ReceivedEventsByActivity, other.ReceivedEventsByActivity);
        AggregateOne(ActorsAliveByActivity, other.ActorsAliveByActivity);
        AggregateOne(ScheduledEventsByActivity, other.ScheduledEventsByActivity);
        AggregateOne(StuckActorsByActivity, other.StuckActorsByActivity);

        // AggregatedCurrentActivationTime is readed and modified only from one thread
        auto timeUs = RelaxedLoad(&other.CurrentActivationTime.TimeUs);
        if (timeUs) {
            AggregatedCurrentActivationTime.push_back(TActivationTime{
                .TimeUs = timeUs,
                .LastActivity = RelaxedLoad(&other.CurrentActivationTime.LastActivity)});
        }
        if (other.AggregatedCurrentActivationTime.size()) {
            AggregatedCurrentActivationTime.insert(AggregatedCurrentActivationTime.end(), other.AggregatedCurrentActivationTime.begin(), other.AggregatedCurrentActivationTime.end());
        }

        if (UsageByActivity.size() < other.UsageByActivity.size()) {
            UsageByActivity.resize(other.UsageByActivity.size());
        }
        for (size_t i = 0; i < UsageByActivity.size(); ++i) {
            for (size_t j = 0; j < 10; ++j) {
                UsageByActivity[i][j] += RelaxedLoad(&other.UsageByActivity[i][j]);
            }
        }

        RelaxedStore(
            &PoolActorRegistrations,
            std::max(RelaxedLoad(&PoolActorRegistrations), RelaxedLoad(&other.PoolActorRegistrations)));
        RelaxedStore(
            &PoolDestroyedActors,
            std::max(RelaxedLoad(&PoolDestroyedActors), RelaxedLoad(&other.PoolDestroyedActors)));
        RelaxedStore(
            &PoolAllocatedMailboxes,
            std::max(RelaxedLoad(&PoolAllocatedMailboxes), RelaxedLoad(&other.PoolAllocatedMailboxes)));
    }

    size_t TExecutorThreadStats::MaxActivityType() const {
        return ActorsAliveByActivity.size();
    }

}