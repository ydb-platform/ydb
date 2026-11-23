#include "scheduler_cookie.h"

#include <library/cpp/testing/unittest/registar.h>

#include <algorithm>
#include <array>
#include <thread>
#include <util/system/event.h>

using namespace NActors;

Y_UNIT_TEST_SUITE(SchedulerCookie) {
    Y_UNIT_TEST(TwoOwnersReleaseInEitherOrder) {
        for (unsigned first = 0; first < 2; ++first) {
            auto* cookie = ISchedulerCookie::Make2Way();
            TSchedulerCookieHolder owners[] = {cookie, cookie};
            UNIT_ASSERT(cookie->IsArmed());
            UNIT_ASSERT(owners[first].Detach());
            UNIT_ASSERT(!owners[first].Get());
            UNIT_ASSERT(!cookie->IsArmed());
            UNIT_ASSERT(!owners[1 - first].Detach());
            // Both references have been released; cookie is no longer usable.
            UNIT_ASSERT(!owners[1 - first].Get());
        }
    }

    Y_UNIT_TEST(ThreeOwnersReleaseInEveryOrder) {
        std::array<unsigned, 3> order = {0, 1, 2};
        do {
            auto* cookie = ISchedulerCookie::Make3Way();
            TSchedulerCookieHolder owners[] = {cookie, cookie, cookie};
            // Owners 0 and 1 use Detach, owner 2 represents the event.
            for (unsigned step = 0; step < order.size(); ++step) {
                UNIT_ASSERT_VALUES_EQUAL(cookie->IsArmed(), step == 0);
                const auto owner = order[step];
                const bool result = owner == 2
                    ? owners[owner].DetachEvent() : owners[owner].Detach();
                UNIT_ASSERT_VALUES_EQUAL(result, owner == 2 ? step == 1 : step == 0);
                UNIT_ASSERT(!owners[owner].Get());
                UNIT_ASSERT(!owners[owner].Detach());
                UNIT_ASSERT(!owners[owner].DetachEvent());
                // The next iteration only queries cookie if an owner remains.
            }
        } while (std::next_permutation(order.begin(), order.end()));
    }

    Y_UNIT_TEST(DroppedThreeWayEventReleasesItsReference) {
        auto* cookie = ISchedulerCookie::Make3Way();
        TSchedulerCookieHolder scheduler(cookie);
        TSchedulerCookieHolder owner(cookie);
        {
            TSchedulerCookieHolder event(cookie);
            UNIT_ASSERT(cookie->IsArmed());
        }
        // Dropping an unprocessed event releases its reference via Detach.
        UNIT_ASSERT(!cookie->IsArmed());
        UNIT_ASSERT(!scheduler.Detach());
        UNIT_ASSERT(!owner.Detach());
    }

    Y_UNIT_TEST(ConcurrentTwoOwnerDetachHasExactlyOneWinner) {
        for (unsigned iteration = 0; iteration < 100; ++iteration) {
            auto* cookie = ISchedulerCookie::Make2Way();
            TManualEvent start;
            bool results[2] = {};
            std::thread first([&] { start.WaitI(); results[0] = cookie->Detach(); });
            std::thread second([&] { start.WaitI(); results[1] = cookie->Detach(); });
            start.Signal();
            first.join();
            second.join();
            // Each thread owns one reference. Joining publishes both results;
            // the final detach deletes cookie, so do not inspect it afterwards.
            UNIT_ASSERT_VALUES_EQUAL(unsigned(results[0]) + unsigned(results[1]), 1);
        }
    }

    Y_UNIT_TEST(ReleaseTransfersOwnershipWithoutDisarming) {
        auto* cookie = ISchedulerCookie::Make2Way();
        TSchedulerCookieHolder scheduler(cookie);
        {
            TSchedulerCookieHolder source(cookie);
            UNIT_ASSERT_VALUES_EQUAL(source.Release(), cookie);
            UNIT_ASSERT(!source.Get());
            UNIT_ASSERT(!source.Release());
        }
        TSchedulerCookieHolder destination(cookie);
        UNIT_ASSERT(cookie->IsArmed());
        UNIT_ASSERT(destination.Detach());
        UNIT_ASSERT(!scheduler.Get()->IsArmed());
        UNIT_ASSERT(!scheduler.Detach());
    }

    Y_UNIT_TEST(ResetReleasesOldOwnerAndAdoptsNewOwner) {
        TSchedulerCookieHolder oldScheduler(ISchedulerCookie::Make2Way());
        TSchedulerCookieHolder newScheduler(ISchedulerCookie::Make2Way());
        TSchedulerCookieHolder owner(oldScheduler.Get());
        owner.Reset(newScheduler.Get());
        UNIT_ASSERT(!oldScheduler.Get()->IsArmed());
        UNIT_ASSERT(!oldScheduler.Detach());
        UNIT_ASSERT_VALUES_EQUAL(owner.Get(), newScheduler.Get());
        UNIT_ASSERT(owner.Get()->IsArmed());
        owner.Reset(nullptr);
        UNIT_ASSERT(!owner.Get());
        UNIT_ASSERT(!newScheduler.Get()->IsArmed());
        UNIT_ASSERT(!newScheduler.Detach());
        owner.Reset(nullptr);
    }

    Y_UNIT_TEST(OwnerDestructorDisarmsCookieBeforeSchedulerRelease) {
        TSchedulerCookieHolder scheduler(ISchedulerCookie::Make2Way());
        {
            TSchedulerCookieHolder owner(scheduler.Get());
            UNIT_ASSERT(owner.Get()->IsArmed());
        }
        UNIT_ASSERT(!scheduler.Get()->IsArmed());
        UNIT_ASSERT(!scheduler.Detach());
    }

    Y_UNIT_TEST(OwnerDestructorReleasesLastReferenceAfterSchedulerRelease) {
        TSchedulerCookieHolder scheduler(ISchedulerCookie::Make2Way());
        {
            TSchedulerCookieHolder owner(scheduler.Get());
            UNIT_ASSERT(scheduler.Detach());
            UNIT_ASSERT(!owner.Get()->IsArmed());
            // The owner destructor releases the last reference here.
        }
    }

    Y_UNIT_TEST(EmptyHolderOperations) {
        TSchedulerCookieHolder holder;
        UNIT_ASSERT(!holder.Get());
        UNIT_ASSERT(!holder.Release());
        UNIT_ASSERT(!holder.Detach());
        UNIT_ASSERT(!holder.DetachEvent());
        holder.Reset(nullptr);
    }
}
