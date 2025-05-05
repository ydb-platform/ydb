#include "mailbox_lockfree.h"
#include "events.h"
#include "actor.h"

#include <library/cpp/testing/unittest/registar.h>

#include <util/system/sanitizers.h>

#include <latch>
#include <thread>

namespace NActors {

Y_UNIT_TEST_SUITE(LockFreeMailbox) {

    Y_UNIT_TEST(Basics) {
        TMailbox m;

        UNIT_ASSERT(m.IsFree());
        UNIT_ASSERT(m.IsEmpty());

        // Check that we cannot push events to free mailboxes
        TAutoPtr<IEventHandle> ev1 = new IEventHandle(TActorId(), TActorId(), new TEvents::TEvPing);
        IEventHandle* ev1raw = ev1.Get();
        UNIT_ASSERT(m.Push(ev1) == EMailboxPush::Free);
        UNIT_ASSERT(ev1);

        // Check that we can push events after mailbox is locked from free
        m.LockFromFree();
        UNIT_ASSERT(m.Push(ev1) == EMailboxPush::Pushed);
        UNIT_ASSERT(!ev1);

        // Check that we pop the event we just pushed
        TAutoPtr<IEventHandle> ev2 = m.Pop();
        UNIT_ASSERT(ev2.Get() == ev1raw);

        // Check that the mailbox is now empty
        UNIT_ASSERT(!m.Pop());

        // Unlocking should succeed
        UNIT_ASSERT(m.TryUnlock());

        // Push m
        ev1 = std::move(ev2);
        ev2 = new IEventHandle(TActorId(), TActorId(), new TEvents::TEvPing);
        IEventHandle* ev2raw = ev2.Get();
        TAutoPtr<IEventHandle> ev3 = new IEventHandle(TActorId(), TActorId(), new TEvents::TEvPing);
        IEventHandle* ev3raw = ev3.Get();
        UNIT_ASSERT(m.Push(ev1) == EMailboxPush::Locked);
        UNIT_ASSERT(!ev1);
        UNIT_ASSERT(m.Push(ev2) == EMailboxPush::Pushed);
        UNIT_ASSERT(!ev2);
        UNIT_ASSERT(m.Push(ev3) == EMailboxPush::Pushed);
        UNIT_ASSERT(!ev3);

        // Switch back to free
        m.LockToFree();

        TAutoPtr<IEventHandle> ev4 = new IEventHandle(TActorId(), TActorId(), new TEvents::TEvPing);
        UNIT_ASSERT(m.Push(ev4) == EMailboxPush::Free);

        UNIT_ASSERT(m.Pop().Get() == ev1raw);
        UNIT_ASSERT(m.Pop().Get() == ev2raw);
        UNIT_ASSERT(m.Pop().Get() == ev3raw);
        UNIT_ASSERT(!m.Pop());

        // We shouldn't be able to unlock a free mailbox
        UNIT_ASSERT(!m.TryUnlock());

        // Mailbox is now back in an initial state
        UNIT_ASSERT(m.IsFree());
        UNIT_ASSERT(m.IsEmpty());
    }

    class TSimpleActor : public TActor<TSimpleActor> {
    public:
        TSimpleActor()
            : TActor(&TThis::StateFunc)
        {}

    private:
        void StateFunc(TAutoPtr<IEventHandle>&) {
            // nothing
        }
    };

    Y_UNIT_TEST(RegisterActors) {
        for (int count = 1; count < 16; ++count) {
            TMailbox m;
            std::vector<std::unique_ptr<IActor>> actors;
            for (int i = 1; i <= count; ++i) {
                actors.emplace_back(new TSimpleActor);
                m.AttachActor(i, actors.back().get());
                UNIT_ASSERT(!m.IsEmpty());
                UNIT_ASSERT(m.FindActor(i) == actors.back().get());
            }
            for (int i = 1; i <= count; ++i) {
                UNIT_ASSERT(m.FindActor(i) == actors[i-1].get());
                UNIT_ASSERT(m.DetachActor(i) == actors[i-1].get());
                UNIT_ASSERT(m.FindActor(i) == nullptr);
            }
            UNIT_ASSERT(m.IsEmpty());
        }
    }

    Y_UNIT_TEST(RegisterAliases) {
        TMailbox m;
        std::vector<std::unique_ptr<IActor>> actors;
        for (int i = 1; i <= 4; ++i) {
            actors.emplace_back(new TSimpleActor);
            IActor* actor = actors.back().get();
            m.AttachActor(i * 100, actor);
            UNIT_ASSERT(m.FindActor(i * 100) == actor);
            for (int j = 1; j <= 16; ++j) {
                m.AttachAlias(i * 100 + j, actor);
                UNIT_ASSERT(m.FindActor(i * 100 + j) == nullptr);
                UNIT_ASSERT(m.FindAlias(i * 100 + j) == actor);
            }
        }
        for (int i = 1; i <= 4; ++i) {
            IActor* actor = actors[i - 1].get();
            UNIT_ASSERT(m.FindActor(i * 100) == actor);
            // Verify every alias can be removed individually
            for (int j = 1; j <= 8; ++j) {
                UNIT_ASSERT(m.FindAlias(i * 100 + j) == actor);
                IActor* detached = m.DetachAlias(i * 100 + j);
                UNIT_ASSERT(detached == actor);
                UNIT_ASSERT(m.FindAlias(i * 100 + j) == nullptr);
            }
            UNIT_ASSERT(m.DetachActor(i * 100) == actor);
            // Verify all aliases are detached with the actor
            for (int j = 9; j <= 16; ++j) {
                UNIT_ASSERT(m.FindAlias(i * 100 + j) == nullptr);
            }
        }
        UNIT_ASSERT(m.IsEmpty());
    }

    Y_UNIT_TEST(MultiThreadedPushPop) {
        constexpr size_t nThreads = 3;
        constexpr size_t nEvents = NSan::PlainOrUnderSanitizer(1000000, 100000);

        TMailbox mailbox;
        mailbox.LockFromFree();
        UNIT_ASSERT(mailbox.TryUnlock());

        std::atomic<size_t> eventIndex{ 0 };
        std::atomic<size_t> switches{ 0 };

        std::vector<std::thread> threads;
        threads.reserve(nThreads);
        std::latch start(nThreads);

        std::vector<size_t> observed;

        for (size_t i = 0; i < nThreads; ++i) {
            threads.emplace_back([&]{
                start.arrive_and_wait();

                bool is_producer = true;

                for (;;) {
                    if (is_producer) {
                        // Work as a producer
                        size_t index = eventIndex.fetch_add(1, std::memory_order_relaxed);
                        if (index >= nEvents) {
                            // All events have been produced
                            break;
                        }
                        TAutoPtr<IEventHandle> ev = new IEventHandle(TActorId(), TActorId(), new TEvents::TEvPing, 0, index);
                        switch (mailbox.Push(ev)) {
                            case EMailboxPush::Locked:
                                // This thread is now a consumer
                                switches.fetch_add(1, std::memory_order_relaxed);
                                is_producer = false;
                                break;
                            case EMailboxPush::Pushed:
                                // This thread is still a producer
                                break;
                            case EMailboxPush::Free:
                                // Cannot happen during this test
                                Y_ABORT();
                        }
                    } else {
                        // Work as a consumer
                        auto ev = mailbox.Pop();
                        if (!ev) {
                            if (mailbox.TryUnlock()) {
                                // This thread is now a producer
                                is_producer = true;
                                continue;
                            }
                            // We must have one more event
                            ev = mailbox.Pop();
                            Y_ABORT_UNLESS(ev);
                        }
                        // Only one thread is supposed to be a consumer
                        observed.push_back(ev->Cookie);
                    }
                }
            });
        }
        for (auto& thread : threads) {
            thread.join();
        }

        UNIT_ASSERT_VALUES_EQUAL(observed.size(), nEvents);

        // We must observe every event exactly once
        std::sort(observed.begin(), observed.end());
        for (size_t i = 0; i < observed.size(); ++i) {
            UNIT_ASSERT_VALUES_EQUAL(observed[i], i);
        }

        Cerr << "... there have been " << switches.load() << " switches to consumer mode" << Endl;
    }

} // Y_UNIT_TEST_SUITE(LockFreeMailbox)

} // namespace NActors
