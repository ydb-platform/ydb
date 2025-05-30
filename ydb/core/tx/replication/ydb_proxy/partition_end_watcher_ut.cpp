#include "partition_end_watcher.h"

#include <library/cpp/testing/unittest/registar.h>

#include <vector>

namespace NKikimr::NReplication {

Y_UNIT_TEST_SUITE(PartitionEndWatcher) {

    struct MockActorOps: public IActorOps {
        using CType = std::vector<std::pair<TActorId, IEventBase*>>;

        void Describe(IOutputStream&) const noexcept {
            Y_ABORT("Unexpected");
        };

        bool Send(const TActorId& recipient, IEventBase* event, IEventHandle::TEventFlags = 0, ui64 = 0, NWilson::TTraceId = {}) const noexcept {
            ((CType*)&Events)->push_back({recipient, event});
            return true;
        };

        void Schedule(TInstant, IEventBase*, ISchedulerCookie* = nullptr) const noexcept {
            Y_ABORT("Unexpected");
        };

        void Schedule(TMonotonic, IEventBase*, ISchedulerCookie* = nullptr) const noexcept {
            Y_ABORT("Unexpected");
        }

        void Schedule(TDuration, IEventBase*, ISchedulerCookie* = nullptr) const noexcept {
            Y_ABORT("Unexpected");
        }

        TActorId Register(IActor*, TMailboxType::EType = TMailboxType::HTSwap, ui32 = Max<ui32>()) const noexcept {
            Y_ABORT("Unexpected");
        }

        TActorId RegisterWithSameMailbox(IActor*) const noexcept {
            Y_ABORT("Unexpected");
        }

        TActorId RegisterAlias() noexcept {
            Y_ABORT("Unexpected");
        }

        void UnregisterAlias(const TActorId&) noexcept {
            Y_ABORT("Unexpected");
        }

        ~MockActorOps() {
            for (auto [_, ptr] : Events) {
                std::unique_ptr<IEventBase> p;
                p.reset(ptr);
            }
        }

        CType Events;
    };

    struct MockPartitionSession: public TPartitionSession {
        MockPartitionSession()
            : TPartitionSession()
        {
            PartitionSessionId = 11;
            PartitionId = 13;
        }

        void RequestStatus() {}
    };

    TActorId MakeActorId() {
        return TActorId(1, 2, RandomNumber<ui64>(), 0);
    }

    TReadSessionEvent::TEndPartitionSessionEvent MakeTEndPartitionSessionEvent() {
        TPartitionSession::TPtr partitionSession = MakeIntrusive<MockPartitionSession>();
        return TReadSessionEvent::TEndPartitionSessionEvent(partitionSession, {1}, {2});
    }

    TReadSessionEvent::TDataReceivedEvent MakeTDataReceivedEvent() {
        TPartitionSession::TPtr partitionSession = MakeIntrusive<MockPartitionSession>();
        TReadSessionEvent::TDataReceivedEvent::TMessage msg("data", nullptr,
            TReadSessionEvent::TDataReceivedEvent::TMessageInformation(31, "producer-id", 29, TInstant::Now(),
                TInstant::Now(), nullptr, nullptr, 99, "message-group-id" ),
            partitionSession);
        return TReadSessionEvent::TDataReceivedEvent({ msg }, {}, partitionSession);
    }

    Y_UNIT_TEST(EmptyPartition) {
        TActorId client = MakeActorId();
        MockActorOps actorOps;
        TPartitionEndWatcher watcher(&actorOps);

        watcher.SetEvent(std::move(MakeTEndPartitionSessionEvent()), client);

        UNIT_ASSERT_VALUES_EQUAL(actorOps.Events.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(actorOps.Events[0].first, client);
        auto* e = dynamic_cast<TEvYdbProxy::TEvEndTopicPartition*>(actorOps.Events[0].second);
        UNIT_ASSERT(e);
        UNIT_ASSERT_VALUES_EQUAL(e->Result.AdjacentPartitionsIds, TVector<ui64>{1});
        UNIT_ASSERT_VALUES_EQUAL(e->Result.ChildPartitionsIds, TVector<ui64>{2});
    }

    Y_UNIT_TEST(AfterCommit) {
        TActorId client = MakeActorId();
        MockActorOps actorOps;
        TPartitionEndWatcher watcher(&actorOps);

        watcher.UpdatePendingCommittedOffset(MakeTDataReceivedEvent());
        UNIT_ASSERT_VALUES_EQUAL(actorOps.Events.size(), 0);

        watcher.SetCommittedOffset(19, client);
        UNIT_ASSERT_VALUES_EQUAL(actorOps.Events.size(), 0);

        watcher.SetEvent(std::move(MakeTEndPartitionSessionEvent()), client);
        UNIT_ASSERT_VALUES_EQUAL(actorOps.Events.size(), 0);

        watcher.SetCommittedOffset(31, client);

        UNIT_ASSERT_VALUES_EQUAL(actorOps.Events.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(actorOps.Events[0].first, client);
        auto* e = dynamic_cast<TEvYdbProxy::TEvEndTopicPartition*>(actorOps.Events[0].second);
        UNIT_ASSERT(e);
        UNIT_ASSERT_VALUES_EQUAL(e->Result.AdjacentPartitionsIds, TVector<ui64>{1});
        UNIT_ASSERT_VALUES_EQUAL(e->Result.ChildPartitionsIds, TVector<ui64>{2});
    }
}

}
