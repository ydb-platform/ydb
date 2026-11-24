#include "event_bundle.h"
#include "scheduler_queue.h"

#include <library/cpp/testing/unittest/registar.h>
#include <util/system/event.h>

#include <thread>

#if defined(_unix_) && !defined(NDEBUG)
#include <cerrno>
#include <csignal>
#include <sys/wait.h>
#include <unistd.h>
#endif

using namespace NActors;

namespace {
    struct alignas(64) TEvTracked final : TEventLocalBundled<TEvTracked, 1> {
        std::atomic<unsigned>& Destroyed;

        explicit TEvTracked(std::atomic<unsigned>& destroyed)
            : Destroyed(destroyed)
        {}

        ~TEvTracked() override {
            ++Destroyed;
        }
    };

    struct TEvThrowing final : TEventLocalBundled<TEvThrowing, 2> {
        unsigned& Destroyed;

        TEvThrowing(unsigned& destroyed, bool throwOnConstruction)
            : Destroyed(destroyed)
        {
            if (throwOnConstruction) {
                ythrow yexception() << "payload construction failed";
            }
        }

        ~TEvThrowing() override {
            ++Destroyed;
        }

        ui32 Type() const override {
            ythrow yexception() << "handle construction failed";
        }
    };
}

Y_UNIT_TEST_SUITE(EventBundle) {
    Y_UNIT_TEST(AlignmentAndTypedHandleOwnership) {
        std::atomic<unsigned> destroyed = 0;
        auto event = MakeBundled<TEvTracked>({.Recipient = {}}, destroyed);
        auto typed = IEventHandle::Downcast<TEvTracked>(TAutoPtr<IEventHandle>(event.release()));
        UNIT_ASSERT_VALUES_EQUAL(reinterpret_cast<uintptr_t>(typed->Get()) % alignof(TEvTracked), 0);
        typed.Reset();
        UNIT_ASSERT_VALUES_EQUAL(destroyed.load(), 1);

        THolder<IEventBase> standalone(new TEvTracked(destroyed));
        standalone.Reset();
        UNIT_ASSERT_VALUES_EQUAL(destroyed.load(), 2);
    }

    Y_UNIT_TEST(ReleasedPayloadOutlivesHandleOrDiesFirst) {
        std::atomic<unsigned> destroyed = 0;
        auto event = MakeBundled<TEvTracked>({.Recipient = {}}, destroyed);
        auto* payload = event->Get<TEvTracked>();
        auto released = event->Release<TEvTracked>();
        UNIT_ASSERT_VALUES_EQUAL(released.Get(), payload);
        event.reset();
        UNIT_ASSERT_VALUES_EQUAL(destroyed.load(), 0);
        released.Reset();

        event = MakeBundled<TEvTracked>({.Recipient = {}}, destroyed);
        auto base = event->ReleaseBase();
        base.Reset();
        UNIT_ASSERT_VALUES_EQUAL(destroyed.load(), 2);
        event.reset();
        UNIT_ASSERT_VALUES_EQUAL(destroyed.load(), 2);
    }

    Y_UNIT_TEST(ForwardMovesPayloadAndCopiesEnvelope) {
        std::atomic<unsigned> destroyed = 0;
        const TActorId original(1, "original");
        const TActorId recipient(1, "recipient");
        const TActorId sender(1, "sender");
        auto event = MakeBundled<TEvTracked>({
            .Recipient = original,
            .Sender = sender,
            .Flags = IEventHandle::FlagTrackDelivery,
            .Cookie = 17,
        }, destroyed);
        auto* payload = event->Get<TEvTracked>();
        auto forwarded = event->Forward(recipient);
        UNIT_ASSERT_VALUES_EQUAL(forwarded->Recipient, recipient);
        UNIT_ASSERT_VALUES_EQUAL(forwarded->Sender, sender);
        UNIT_ASSERT_VALUES_EQUAL(forwarded->Flags & ~IEventHandle::FlagDebugTrackReceive, IEventHandle::FlagTrackDelivery);
        UNIT_ASSERT_VALUES_EQUAL(forwarded->Cookie, 17);
        UNIT_ASSERT_VALUES_EQUAL(forwarded->Get<TEvTracked>(), payload);
        event.reset();
        UNIT_ASSERT_VALUES_EQUAL(destroyed.load(), 0);
        forwarded.Reset();
        UNIT_ASSERT_VALUES_EQUAL(destroyed.load(), 1);
    }

    Y_UNIT_TEST(ConstructionFailures) {
        unsigned destroyed = 0;
        UNIT_ASSERT_EXCEPTION(MakeBundled<TEvThrowing>({.Recipient = {}}, destroyed, true), yexception);
        UNIT_ASSERT_VALUES_EQUAL(destroyed, 0);
        UNIT_ASSERT_EXCEPTION(MakeBundledWithCookie<TEvThrowing>({.Recipient = {}}, destroyed, false), yexception);
        UNIT_ASSERT_VALUES_EQUAL(destroyed, 1);
        UNIT_ASSERT_EXCEPTION(THolder<IEventBase>(new TEvThrowing(destroyed, true)), yexception);
        UNIT_ASSERT_VALUES_EQUAL(destroyed, 1);
    }

    Y_UNIT_TEST(SchedulerDeliversArmedCookie) {
        std::atomic<unsigned> destroyed = 0;
        auto event = MakeBundledWithCookie<TEvTracked>({.Recipient = {}}, destroyed);
        NSchedulerQueue::TQueueType queue;
        queue.Writer.Push(1, event.Event.release(), event.SchedulerCookie.Release());
        auto* entry = queue.Reader.Pop();
        UNIT_ASSERT(entry);
        UNIT_ASSERT(entry->Cookie->Detach());
        std::unique_ptr<IEventHandle> delivered(entry->Ev);
        UNIT_ASSERT(!event.OwnerCookie.Detach());
        delivered.reset();
        UNIT_ASSERT_VALUES_EQUAL(destroyed.load(), 1);
    }

    Y_UNIT_TEST(CancellationAndSchedulerDrain) {
        for (bool cancel : {false, true}) {
            std::atomic<unsigned> destroyed = 0;
            auto event = MakeBundledWithCookie<TEvTracked>({.Recipient = {}}, destroyed);
            {
                NSchedulerQueue::TQueueType queue;
                queue.Writer.Push(1, event.Event.release(), event.SchedulerCookie.Release());
                UNIT_ASSERT(event.OwnerCookie.Get()->IsArmed());
                if (cancel) {
                    UNIT_ASSERT(event.OwnerCookie.Detach());
                    auto* entry = queue.Reader.Pop();
                    UNIT_ASSERT(entry);
                    UNIT_ASSERT(!entry->Cookie->Detach());
                    delete entry->Ev;
                }
            }
            UNIT_ASSERT_VALUES_EQUAL(destroyed.load(), 1);
            if (!cancel) {
                UNIT_ASSERT(!event.OwnerCookie.Get()->IsArmed());
                UNIT_ASSERT(!event.OwnerCookie.Detach());
            }
        }
    }

    Y_UNIT_TEST(IndependentSlotsCanBeDestroyedConcurrently) {
        for (unsigned i = 0; i < 40; ++i) {
            std::atomic<unsigned> destroyed = 0;
            auto event = MakeBundledWithCookie<TEvTracked>({.Recipient = {}}, destroyed);
            auto payload = event.Event->Release<TEvTracked>();
            TManualEvent start;
            bool ownerWon = false;
            bool schedulerWon = false;
            std::thread owner([&] {
                start.WaitI();
                ownerWon = event.OwnerCookie.Detach();
            });
            std::thread scheduler([&] {
                start.WaitI();
                schedulerWon = event.SchedulerCookie.Detach();
                event.Event.reset();
            });
            start.Signal();
            payload.Reset();
            owner.join();
            scheduler.join();
            UNIT_ASSERT(ownerWon != schedulerWon);
            UNIT_ASSERT_VALUES_EQUAL(destroyed.load(), 1);
        }
    }

#if defined(_unix_) && !defined(NDEBUG)
    Y_UNIT_TEST(RejectsRepeatReleaseOfEverySlot) {
        using TAllocation = NEventBundle::TAllocation;
        for (auto slot : {TAllocation::Handle, TAllocation::Event, TAllocation::Cookie}) {
            const pid_t child = fork();
            UNIT_ASSERT(child >= 0);
            if (child == 0) {
                auto* allocation = TAllocation::Allocate(sizeof(TAllocation), alignof(TAllocation));
                allocation->AddBeforePublish(TAllocation::Event);
                allocation->AddBeforePublish(TAllocation::Cookie);
                allocation->Release(slot);
                allocation->Release(slot);
                _exit(0);
            }

            int status = 0;
            pid_t waited;
            do {
                waited = waitpid(child, &status, 0);
            } while (waited < 0 && errno == EINTR);
            UNIT_ASSERT_VALUES_EQUAL(waited, child);
            UNIT_ASSERT(WIFSIGNALED(status));
            UNIT_ASSERT_VALUES_EQUAL(WTERMSIG(status), SIGABRT);
        }
    }
#endif
}
