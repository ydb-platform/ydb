#include "immediate_control_board_impl.h"
#include "immediate_control_board_wrapper.h"
#include <library/cpp/testing/unittest/registar.h>
#include <util/random/mersenne64.h>
#include <util/random/entropy.h>
#include <util/string/printf.h>
#include <util/system/thread.h>
#include <array>

namespace NKikimr {

#define TEST_REPEATS 1000000
#define TEST_THREADS_CNT 4
#define IS_VERBOSE 1

#if IS_VERBOSE
#   define VERBOSE_COUT(a)  \
        Cout << a;          \
        Cout << Endl
#endif

Y_UNIT_TEST_SUITE(ControlImplementationTests) {
    Y_UNIT_TEST(TestTControl) {
        NPrivate::TMersenne64 randGen(Seed());
        std::array<i64, 3> bounds;
        for (ui64 i = 0; i < 3; ++i) {
            bounds[i] = (i64)randGen.GenRand();
        }
        std::sort(bounds.begin(), bounds.end());
        i64 lowerBound = bounds[0];
        i64 defaultValue = bounds[1];
        i64 upperBound = bounds[2];

        TIntrusivePtr<TControl> control(new TControl(defaultValue, lowerBound, upperBound));
        for (ui64 i = 0; i < TEST_REPEATS; ++i) {
            i64 num = (i64)randGen.GenRand();
            control->Set(num);
            UNIT_ASSERT_EQUAL(control->Get(), num);
            UNIT_ASSERT_EQUAL(control->GetDefault(), num);
        }
        control = new TControl(defaultValue, lowerBound, upperBound);
        for (ui64 i = 0; i < TEST_REPEATS; ++i) {
            i64 num = (i64)randGen.GenRand();
            control->SetFromHtmlRequest(num);
            if (num < lowerBound) {
                UNIT_ASSERT_EQUAL(control->Get(), lowerBound);
            } else if (upperBound < num) {
                UNIT_ASSERT_EQUAL(control->Get(), upperBound);
            } else {
                UNIT_ASSERT_EQUAL(control->Get(), num);
            }
            UNIT_ASSERT_EQUAL(control->GetDefault(), defaultValue);
            control->RestoreDefault();
            UNIT_ASSERT(control->IsDefault());
            UNIT_ASSERT_EQUAL(control->Get(), defaultValue);
            UNIT_ASSERT_EQUAL(control->GetDefault(), defaultValue);
        }
    }

    Y_UNIT_TEST(TestControlWrapperAsI64) {
        NPrivate::TMersenne64 randGen(Seed());
        TControlWrapper wrapper1;
        for (ui64 i = 0; i < TEST_REPEATS; ++i) {
            i64 num = (i64)randGen.GenRand();
            wrapper1 = num;
            TControlWrapper wrapper2(num);
            UNIT_ASSERT_EQUAL(wrapper1, num);
            UNIT_ASSERT_EQUAL(wrapper2, num);
        }
    }

    Y_UNIT_TEST(TestControlWrapperBounds) {
        NPrivate::TMersenne64 randGen(Seed());
        std::array<i64, 3> bounds;
        for (ui64 i = 0; i < 3; ++i) {
            bounds[i] = (i64)randGen.GenRand();
        }
        std::sort(bounds.begin(), bounds.end());
        i64 lowerBound = bounds[0];
        i64 defaultValue = bounds[1];
        i64 upperBound = bounds[2];

        TControlWrapper wrapper(defaultValue, lowerBound, upperBound);
        for (ui64 i = 0; i < TEST_REPEATS; ++i) {
            i64 num = (i64)randGen.GenRand();
            wrapper = num;
            UNIT_ASSERT_EQUAL(wrapper, num);
        }
    }

    Y_UNIT_TEST(TestRegisterLocalControl) {
        TIntrusivePtr<TControlBoard> Icb(new TControlBoard);
        TControlWrapper control1(1, 1, 1);
        TControlWrapper control2(2, 2, 2);
        UNIT_ASSERT(Icb->RegisterLocalControl(control1, "localControl"));
        UNIT_ASSERT(!Icb->RegisterLocalControl(control2, "localControl"));
        UNIT_ASSERT_EQUAL(1, 1);
    }

    Y_UNIT_TEST(TestRegisterSharedControl) {
        TIntrusivePtr<TControlBoard> Icb(new TControlBoard);
        TControlWrapper control1(1, 1, 1);
        TControlWrapper control1_origin(control1);
        TControlWrapper control2(2, 2, 2);
        TControlWrapper control2_origin(control2);
        Icb->RegisterSharedControl(control1, "sharedControl");
        UNIT_ASSERT(control1.IsTheSame(control1_origin));
        Icb->RegisterSharedControl(control2, "sharedControl");
        UNIT_ASSERT(control2.IsTheSame(control1_origin));
    }

    Y_UNIT_TEST(TestParallelRegisterSharedControl) {
        void* (*parallelJob)(void*) = [](void *controlBoard) -> void *{
            for (ui64 i = 0; i < 10000; ++i) {
                TControlBoard *Icb = reinterpret_cast<TControlBoard *>(controlBoard);
                TControlWrapper control1(1, 1, 1);
                Icb->RegisterSharedControl(control1, "sharedControl");
                // Useless because running this test with --sanitize=thread cannot reveal
                // race condition in Icb->RegisterLocalControl(...) without mutex
                TControlWrapper control2(2, 2, 2);
                TControlWrapper control2_origin(control2);
                Icb->RegisterLocalControl(control2, "localControl");
                UNIT_ASSERT_EQUAL(control2, control2_origin);
            }
            return nullptr;
        };
        TIntrusivePtr<TControlBoard> Icb(new TControlBoard);
        TVector<THolder<TThread>> threads;
        threads.reserve(TEST_THREADS_CNT);
        for (ui64 i = 0; i < TEST_THREADS_CNT; ++i) {
            threads.emplace_back(new TThread(parallelJob, (void *)Icb.Get()));
        }
        for (ui64 i = 0; i < TEST_THREADS_CNT; ++i) {
            threads[i]->Start();
        }
        for (ui64 i = 0; i < TEST_THREADS_CNT; ++i) {
            threads[i]->Join();
        }
    }
}

} // namespace NKikimr

