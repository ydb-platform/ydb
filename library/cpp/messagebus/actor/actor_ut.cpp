#include <library/cpp/testing/unittest/registar.h>

#include "actor.h"
#include "queue_in_actor.h"

#include <library/cpp/messagebus/misc/test_sync.h>

#include <util/generic/object_counter.h>
#include <util/system/event.h>

using namespace NActor;

template <typename TThis>
struct TTestActorBase: public TAtomicRefCount<TThis>, public TActor<TThis> {
    TTestSync Started;
    TTestSync Acted;

    TTestActorBase(TExecutor* executor)
        : TActor<TThis>(executor)
    {
    }

    void Act(TDefaultTag) {
        Started.Inc();
        static_cast<TThis*>(this)->Act2();
        Acted.Inc();
    }
};

struct TNopActor: public TTestActorBase<TNopActor> {
    TObjectCounter<TNopActor> AllocCounter;

    TNopActor(TExecutor* executor)
        : TTestActorBase<TNopActor>(executor)
    {
    }

    void Act2() {
    }
};

struct TWaitForSignalActor: public TTestActorBase<TWaitForSignalActor> {
    TWaitForSignalActor(TExecutor* executor)
        : TTestActorBase<TWaitForSignalActor>(executor)
    {
    }

    TSystemEvent WaitFor;

    void Act2() {
        WaitFor.Wait();
    }
};

struct TDecrementAndSendActor: public TTestActorBase<TDecrementAndSendActor>, public TQueueInActor<TDecrementAndSendActor, int> {
    TSystemEvent Done;

    TDecrementAndSendActor* Next;

    TDecrementAndSendActor(TExecutor* executor)
        : TTestActorBase<TDecrementAndSendActor>(executor)
        , Next(nullptr)
    {
    }

    void ProcessItem(TDefaultTag, TDefaultTag, int n) {
        if (n == 0) {
            Done.Signal();
        } else {
            Next->EnqueueAndSchedule(n - 1);
        }
    }

    void Act(TDefaultTag) {
        DequeueAll();
    }
};

struct TObjectCountChecker {
    TObjectCountChecker() {
        CheckCounts();
    }

    ~TObjectCountChecker() {
        CheckCounts();
    }

    void CheckCounts() {
        UNIT_ASSERT_VALUES_EQUAL(TAtomicBase(0), TObjectCounter<TNopActor>::ObjectCount());
        UNIT_ASSERT_VALUES_EQUAL(TAtomicBase(0), TObjectCounter<TWaitForSignalActor>::ObjectCount());
        UNIT_ASSERT_VALUES_EQUAL(TAtomicBase(0), TObjectCounter<TDecrementAndSendActor>::ObjectCount());
    }
};

Y_UNIT_TEST_SUITE(TActor) {
    Y_UNIT_TEST(Simple) {
        TObjectCountChecker objectCountChecker;

        TExecutor executor(4);

        TIntrusivePtr<TNopActor> actor(new TNopActor(&executor));

        actor->Schedule();

        actor->Acted.WaitFor(1u);
    }

    Y_UNIT_TEST(ScheduleAfterStart) {
        TObjectCountChecker objectCountChecker;

        TExecutor executor(4);

        TIntrusivePtr<TWaitForSignalActor> actor(new TWaitForSignalActor(&executor));

        actor->Schedule();

        actor->Started.WaitFor(1);

        actor->Schedule();

        actor->WaitFor.Signal();

        // make sure Act is called second time
        actor->Acted.WaitFor(2u);
    }

    void ComplexImpl(int queueSize, int actorCount) {
        TObjectCountChecker objectCountChecker;

        TExecutor executor(queueSize);

        TVector<TIntrusivePtr<TDecrementAndSendActor>> actors;
        for (int i = 0; i < actorCount; ++i) {
            actors.push_back(new TDecrementAndSendActor(&executor));
        }

        for (int i = 0; i < actorCount; ++i) {
            actors.at(i)->Next = &*actors.at((i + 1) % actorCount);
        }

        for (int i = 0; i < actorCount; ++i) {
            actors.at(i)->EnqueueAndSchedule(10000);
        }

        for (int i = 0; i < actorCount; ++i) {
            actors.at(i)->Done.WaitI();
        }
    }

    Y_UNIT_TEST(ComplexContention) {
        ComplexImpl(4, 6);
    }

    Y_UNIT_TEST(ComplexNoContention) {
        ComplexImpl(6, 4);
    }
}
