#include "txallocator_ut_helpers.h"
#include "txallocator_impl.h"
#include <ydb/core/testlib/basics/runtime.h>

#include <util/generic/xrange.h>

using namespace NKikimr;
using namespace NTxAllocatorUT_Private;

Y_UNIT_TEST_SUITE(TTxLocatorTest) {
    Y_UNIT_TEST(Boot) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
    }

    Y_UNIT_TEST(TestZeroRange) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        AllocateAndCheck(runtime, 0, NKikimrTx::TEvTxAllocateResult::SUCCESS);
    }

    Y_UNIT_TEST(TestImposibleSize) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        const ui64 capacity = NTxAllocator::TTxAllocator::MaxCapacity;
        const ui64 requestSize = 123456;

        AllocateAndCheck(runtime, capacity + 1, NKikimrTx::TEvTxAllocateResult::IMPOSIBLE);
        AllocateAndCheck(runtime, requestSize, NKikimrTx::TEvTxAllocateResult::SUCCESS);
        AllocateAndCheck(runtime, capacity - requestSize + 1, NKikimrTx::TEvTxAllocateResult::IMPOSIBLE);
        AllocateAndCheck(runtime, 2*requestSize, NKikimrTx::TEvTxAllocateResult::SUCCESS);
        AllocateAndCheck(runtime, capacity - 3*requestSize + 1, NKikimrTx::TEvTxAllocateResult::IMPOSIBLE);
    }

    Y_UNIT_TEST(TestAllocateAll) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        const ui64 capacity = NTxAllocator::TTxAllocator::MaxCapacity;
        AllocateAndCheck(runtime, capacity, NKikimrTx::TEvTxAllocateResult::SUCCESS);
        AllocateAndCheck(runtime, 1, NKikimrTx::TEvTxAllocateResult::IMPOSIBLE);
    }

    Y_UNIT_TEST(TestAllocateAllByPieces) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        const ui64 capacity = NTxAllocator::TTxAllocator::MaxCapacity;
        const ui64 pieces = 1u << 5;
        const ui64 requestSize = capacity/pieces;

        for (auto round: xrange(pieces)) {
            Y_UNUSED(round);
            AllocateAndCheck(runtime, requestSize, NKikimrTx::TEvTxAllocateResult::SUCCESS);
        }
        AllocateAndCheck(runtime, pieces - 1, NKikimrTx::TEvTxAllocateResult::SUCCESS);
        AllocateAndCheck(runtime, 1, NKikimrTx::TEvTxAllocateResult::IMPOSIBLE);
    }

    void DoSignificantRequests(const bool restartsEnable) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);

        const ui64 rounds = 10;
        const ui64 useRangePerRound = 1000000;
        const ui64 pieces = 10;
        const ui64 requestSize = useRangePerRound/pieces;

        TIntersectionChecker inersections;

        for (auto round: xrange(rounds)) {
            Y_UNUSED(round);
            for (auto request: xrange(pieces)) {
                Y_UNUSED(request);
                AsyncAllocate(runtime, requestSize);
            }

            for (auto request: xrange(pieces)) {
                Y_UNUSED(request);
                auto result = GrabAnswer(runtime);
                auto &event = result.first;
                CheckExpectedStatus(NKikimrTx::TEvTxAllocateResult::SUCCESS, event.GetStatus());
                ui64 cookie = result.second;
                CheckExpectedCookie(event, cookie);
                inersections.Add(event.GetRangeBegin(), event.GetRangeEnd());
            }

            if (restartsEnable) {
                env.Reboot(runtime);
            }
        }

        const bool continuesRange = !restartsEnable;
        inersections.AssertIntersection(continuesRange);
    }

    Y_UNIT_TEST(TestSignificantRequestWhenRunReserveTx) {
        const bool restartsEnable = false;
        DoSignificantRequests(restartsEnable);
    }

    Y_UNIT_TEST(TestWithReboot) {
        const bool restartsEnable = true;
        DoSignificantRequests(restartsEnable);
    }

}
