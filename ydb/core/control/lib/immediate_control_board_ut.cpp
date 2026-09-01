#include "immediate_control_board_impl.h"
#include "immediate_control_board_wrapper.h"
#include "dynamic_control_board_impl.h"
#include "immediate_control_board_html_renderer.h"

#include <library/cpp/testing/unittest/registar.h>
#include <util/random/mersenne64.h>
#include <util/random/entropy.h>
#include <util/string/printf.h>
#include <util/system/thread.h>
#include <array>
#include <tuple>
#include <utility>

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

    Y_UNIT_TEST(TestExplicitOverrideLifecycle) {
        constexpr i64 defaultValue = 10;
        TIntrusivePtr<TControl> control(new TControl(defaultValue, 0, 20));

        UNIT_ASSERT(control->IsDefault());
        UNIT_ASSERT_VALUES_EQUAL(control->SetFromHtmlRequest(defaultValue), defaultValue);
        UNIT_ASSERT(control->IsDefault());
        UNIT_ASSERT(!control->HasOverride());
        UNIT_ASSERT(!control->GetOverride());

        const TControlMutation overridden = control->SetFromHtmlRequestWithState(15);
        UNIT_ASSERT(!overridden.Before.Overridden);
        UNIT_ASSERT(overridden.After.Overridden);
        UNIT_ASSERT_VALUES_EQUAL(overridden.After.Value, 15);

        control->UpdateDefault(15);
        UNIT_ASSERT_VALUES_EQUAL(control->Get(), 15);
        UNIT_ASSERT_VALUES_EQUAL(control->GetDefault(), 15);
        UNIT_ASSERT_VALUES_EQUAL(*control->GetOverride(), 15);

        const TControlMutation clearedByDefault =
            control->SetFromHtmlRequestWithState(15);
        UNIT_ASSERT(clearedByDefault.Before.Overridden);
        UNIT_ASSERT(!clearedByDefault.After.Overridden);
        UNIT_ASSERT_VALUES_EQUAL(
            clearedByDefault.Before.Value,
            clearedByDefault.After.Value);

        const TControlMutation clamped =
            control->SetFromHtmlRequestWithState(25);
        UNIT_ASSERT_VALUES_EQUAL(clamped.Before.Value, 15);
        UNIT_ASSERT(!clamped.Before.Overridden);
        UNIT_ASSERT_VALUES_EQUAL(clamped.After.Value, 20);
        UNIT_ASSERT(clamped.After.Overridden);
        UNIT_ASSERT_VALUES_EQUAL(*control->GetOverride(), 20);

        const TControlMutation repeated =
            control->SetFromHtmlRequestWithState(20);
        UNIT_ASSERT_VALUES_EQUAL(repeated.Before.Value, repeated.After.Value);
        UNIT_ASSERT_VALUES_EQUAL(repeated.Before.Default, repeated.After.Default);
        UNIT_ASSERT_VALUES_EQUAL(repeated.Before.Overridden, repeated.After.Overridden);

        control->UpdateDefault(20);
        UNIT_ASSERT_VALUES_EQUAL(*control->GetOverride(), 20);
        const TControlMutation clampedToDefault =
            control->SetFromHtmlRequestWithState(25);
        UNIT_ASSERT(clampedToDefault.Before.Overridden);
        UNIT_ASSERT(!clampedToDefault.After.Overridden);
        UNIT_ASSERT_VALUES_EQUAL(clampedToDefault.After.Value, 20);
        UNIT_ASSERT(control->IsDefault());
        UNIT_ASSERT(!control->GetOverride());
        UNIT_ASSERT_VALUES_EQUAL(control->Get(), 20);

        control->UpdateDefault(5);
        UNIT_ASSERT_VALUES_EQUAL(control->Get(), 5);
        UNIT_ASSERT_VALUES_EQUAL(control->GetDefault(), 5);
    }

    Y_UNIT_TEST(TestCoherentStateSnapshots) {
        TIntrusivePtr<TControl> control(new TControl(0, -100000, 100000));
        TAtomic writersDone = 0;
        std::tuple<TControl*, TAtomic*, TAtomicBase> firstContext(
            control.Get(), &writersDone, 1);
        std::tuple<TControl*, TAtomic*, TAtomicBase> secondContext(
            control.Get(), &writersDone, 2);

        auto writerFunction = [](void* opaque) -> void* {
            auto& [control, writersDone, overrideValue] =
                *static_cast<std::tuple<TControl*, TAtomic*, TAtomicBase>*>(opaque);
            for (ui32 i = 0; i < 100000; ++i) {
                control->SetFromHtmlRequestWithState(overrideValue);
                control->ClearOverride();
            }
            AtomicIncrement(*writersDone);
            return nullptr;
        };
        TThread firstWriter(writerFunction, &firstContext);
        TThread secondWriter(writerFunction, &secondContext);

        firstWriter.Start();
        secondWriter.Start();
        do {
            const TControlState state = control->GetState();
            UNIT_ASSERT_VALUES_EQUAL(state.Default, 0);
            if (state.Overridden) {
                UNIT_ASSERT(state.Value == 1 || state.Value == 2);
            } else {
                UNIT_ASSERT_VALUES_EQUAL(state.Value, state.Default);
            }
        } while (AtomicGet(writersDone) != 2);
        firstWriter.Join();
        secondWriter.Join();
    }

    Y_UNIT_TEST(TestHtmlShowsOverrideEqualToUpdatedDefault) {
        TIntrusivePtr<TControl> control(new TControl(10, 0, 20));
        control->SetFromHtmlRequest(20);
        control->UpdateDefault(20);

        UNIT_ASSERT(control->HasOverride());
        UNIT_ASSERT_VALUES_EQUAL(control->Get(), control->GetDefault());

        TControlBoardTableHtmlRenderer renderer;
        renderer.AddNewTable("Controls", EControlBoardType::Dynamic);
        renderer.AddTableItem("TestControl", control);
        const TString html = renderer.GetHtml();

        UNIT_ASSERT(html.find("<span>override</span>") != TString::npos);
        UNIT_ASSERT(html.find("Reset override") != TString::npos);
        UNIT_ASSERT(html.find("name='__icb_action' value='resetOverride'") != TString::npos);
        UNIT_ASSERT(html.find("name='__icb_board' value='dynamic'") != TString::npos);
        UNIT_ASSERT(html.find("name='__icb_control' value='TestControl'") != TString::npos);
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
        TIntrusivePtr<TDynamicControlBoard> controlBoard(new TDynamicControlBoard);
        TControlWrapper control1(1, 1, 1);
        TControlWrapper control2(2, 2, 2);
        UNIT_ASSERT(controlBoard->RegisterLocalControl(control1, "localControl"));
        UNIT_ASSERT(!controlBoard->RegisterLocalControl(control2, "localControl"));
        UNIT_ASSERT_EQUAL(1, 1);
    }

    Y_UNIT_TEST(TestRegisterSharedControl) {
        TIntrusivePtr<TDynamicControlBoard> controlBoard(new TDynamicControlBoard);
        TControlWrapper control1(1, 1, 1);
        TControlWrapper control1_origin(control1);
        TControlWrapper control2(2, 2, 2);
        TControlWrapper control2_origin(control2);
        controlBoard->RegisterSharedControl(control1, "sharedControl");
        UNIT_ASSERT(control1.IsTheSame(control1_origin));
        controlBoard->RegisterSharedControl(control2, "sharedControl");
        UNIT_ASSERT(control2.IsTheSame(control1_origin));
    }

    Y_UNIT_TEST(TestParallelRegisterSharedControl) {
        void* (*parallelJob)(void*) = [](void *controlBoard) -> void *{
            for (ui64 i = 0; i < 10000; ++i) {
                TDynamicControlBoard *dcb = reinterpret_cast<TDynamicControlBoard *>(controlBoard);
                TControlWrapper control1(1, 1, 1);
                dcb->RegisterSharedControl(control1, "sharedControl");
                // Useless because running this test with --sanitize=thread cannot reveal
                // race condition in dcb->RegisterLocalControl(...) without mutex
                TControlWrapper control2(2, 2, 2);
                TControlWrapper control2_origin(control2);
                dcb->RegisterLocalControl(control2, "localControl");
                UNIT_ASSERT_EQUAL(control2, control2_origin);
            }
            return nullptr;
        };
        TIntrusivePtr<TDynamicControlBoard> Icb(new TDynamicControlBoard);
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
