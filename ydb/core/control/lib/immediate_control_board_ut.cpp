#include "immediate_control_board_impl.h"
#include "immediate_control_board_wrapper.h"
#include "dynamic_control_board_impl.h"
#include "immediate_control_board_html_renderer.h"

#include <ydb/core/protos/config.pb.h>

#include <library/cpp/testing/unittest/registar.h>
#include <util/random/mersenne64.h>
#include <util/random/entropy.h>
#include <util/string/printf.h>
#include <util/system/thread.h>
#include <array>
#include <atomic>
#include <barrier>
#include <thread>

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

    // Verify override-presence transitions across equal values, default
    // updates, bounds clamping, repeated assignments, and restoration.
    Y_UNIT_TEST(TestExplicitOverrideLifecycle) {
        constexpr i64 defaultValue = 10;
        TIntrusivePtr<TControl> control(new TControl(defaultValue, 0, 20));

        // Assign the current default through HTML and verify that the
        // assignment creates an override without changing the numeric value.
        UNIT_ASSERT(control->IsDefault());
        UNIT_ASSERT_VALUES_EQUAL(control->SetFromHtmlRequest(defaultValue), defaultValue);
        UNIT_ASSERT(!control->IsDefault());
        UNIT_ASSERT_VALUES_EQUAL(*control->GetOverride(), defaultValue);
        control->RestoreDefault();

        // Set a non-default value and inspect both sides of the mutation.
        const TControlMutation overridden = control->SetFromHtmlRequestWithState(15);
        UNIT_ASSERT(!overridden.Before.Overridden);
        UNIT_ASSERT(overridden.After.Overridden);
        UNIT_ASSERT_VALUES_EQUAL(overridden.After.Value, 15);

        // Move the default to the active value and verify that this does not
        // implicitly clear the override.
        control->UpdateDefault(15);
        UNIT_ASSERT_VALUES_EQUAL(control->Get(), 15);
        UNIT_ASSERT_VALUES_EQUAL(control->GetDefault(), 15);
        UNIT_ASSERT_VALUES_EQUAL(*control->GetOverride(), 15);

        // Repeat the equal value and keep the explicit override active.
        const TControlMutation equalToDefault =
            control->SetFromHtmlRequestWithState(15);
        UNIT_ASSERT(equalToDefault.Before.Overridden);
        UNIT_ASSERT(equalToDefault.After.Overridden);
        UNIT_ASSERT_VALUES_EQUAL(equalToDefault.After.Value, 15);

        // Clear only override presence when the value already equals default.
        const TControlMutation clearedByDefault = control->RestoreDefault();
        UNIT_ASSERT(clearedByDefault.Before.Overridden);
        UNIT_ASSERT(!clearedByDefault.After.Overridden);
        UNIT_ASSERT_VALUES_EQUAL(
            clearedByDefault.Before.Value,
            clearedByDefault.After.Value);

        // Clamp an out-of-range HTML value and retain override presence.
        const TControlMutation clamped =
            control->SetFromHtmlRequestWithState(25);
        UNIT_ASSERT_VALUES_EQUAL(clamped.Before.Value, 15);
        UNIT_ASSERT(!clamped.Before.Overridden);
        UNIT_ASSERT_VALUES_EQUAL(clamped.After.Value, 20);
        UNIT_ASSERT(clamped.After.Overridden);
        UNIT_ASSERT_VALUES_EQUAL(*control->GetOverride(), 20);

        // Repeat the effective value and verify that no state component changes.
        const TControlMutation repeated =
            control->SetFromHtmlRequestWithState(20);
        UNIT_ASSERT_VALUES_EQUAL(repeated.Before.Value, repeated.After.Value);
        UNIT_ASSERT_VALUES_EQUAL(repeated.Before.Default, repeated.After.Default);
        UNIT_ASSERT_VALUES_EQUAL(repeated.Before.Overridden, repeated.After.Overridden);

        // Keep a clamped override active when it equals the updated default.
        control->UpdateDefault(20);
        UNIT_ASSERT_VALUES_EQUAL(*control->GetOverride(), 20);
        const TControlMutation clampedToDefault =
            control->SetFromHtmlRequestWithState(25);
        UNIT_ASSERT(clampedToDefault.Before.Overridden);
        UNIT_ASSERT(clampedToDefault.After.Overridden);
        UNIT_ASSERT_VALUES_EQUAL(clampedToDefault.After.Value, 20);
        UNIT_ASSERT(!control->IsDefault());
        UNIT_ASSERT_VALUES_EQUAL(*control->GetOverride(), 20);

        // Change the default under an active override, then restore the latest
        // default rather than the value used when the override was created.
        control->UpdateDefault(5);
        UNIT_ASSERT_VALUES_EQUAL(control->Get(), 20);
        control->RestoreDefault();
        UNIT_ASSERT_VALUES_EQUAL(control->Get(), 5);
        UNIT_ASSERT_VALUES_EQUAL(control->GetDefault(), 5);
    }

    // Verify that programmatic overrides preserve registry defaults, apply
    // bounds, and expose flag-only transitions.
    Y_UNIT_TEST(TestSetOverridePreservesDefaultAndPresence) {
        TIntrusivePtr<TControl> control(new TControl(10, 0, 20));

        // Create an equal-to-default override and inspect its complete state.
        const auto equal = control->SetOverride(10);
        UNIT_ASSERT(!equal.Before.Overridden);
        UNIT_ASSERT(equal.After.Overridden);
        UNIT_ASSERT_VALUES_EQUAL(equal.After.Value, 10);
        UNIT_ASSERT_VALUES_EQUAL(equal.After.Default, 10);
        UNIT_ASSERT(control->GetOverride());
        UNIT_ASSERT_VALUES_EQUAL(*control->GetOverride(), 10);

        // Update the default while preserving the programmatic override value.
        control->UpdateDefault(15);
        UNIT_ASSERT_VALUES_EQUAL(control->Get(), 10);
        UNIT_ASSERT_VALUES_EQUAL(control->GetDefault(), 15);

        // Clamp an override above the upper bound without changing the default.
        const auto upper = control->SetOverride(30);
        UNIT_ASSERT_VALUES_EQUAL(upper.After.Value, 20);
        UNIT_ASSERT_VALUES_EQUAL(upper.After.Default, 15);
        UNIT_ASSERT(upper.After.Overridden);

        // Make the clamped value equal to default and clear only its presence.
        control->UpdateDefault(20);
        const auto clampedToDefault = control->SetOverride(30);
        UNIT_ASSERT_VALUES_EQUAL(clampedToDefault.After.Value, 20);
        UNIT_ASSERT(clampedToDefault.After.Overridden);

        const auto restored = control->RestoreDefault();
        UNIT_ASSERT(restored.Before.Overridden);
        UNIT_ASSERT(!restored.After.Overridden);
        UNIT_ASSERT_VALUES_EQUAL(restored.Before.Value, restored.After.Value);
        UNIT_ASSERT_VALUES_EQUAL(restored.After.Default, 20);
        UNIT_ASSERT(!control->GetOverride());

        // Clamp an override below the lower bound.
        const auto lower = control->SetOverride(-1);
        UNIT_ASSERT_VALUES_EQUAL(lower.After.Value, 0);
        UNIT_ASSERT_VALUES_EQUAL(lower.After.Default, 20);
        UNIT_ASSERT(lower.After.Overridden);

        // Preserve an out-of-bounds default while applying bounds only to the
        // override, then restore that exact default.
        control->UpdateDefault(30);
        const auto outsideBounds = control->SetOverride(30);
        UNIT_ASSERT_VALUES_EQUAL(outsideBounds.After.Value, 20);
        UNIT_ASSERT_VALUES_EQUAL(outsideBounds.After.Default, 30);
        UNIT_ASSERT(outsideBounds.After.Overridden);
        control->RestoreDefault();
        UNIT_ASSERT_VALUES_EQUAL(control->Get(), 30);
        UNIT_ASSERT(!control->GetOverride());
    }

    // Verify that copied wrappers mutate and observe the same control state.
    Y_UNIT_TEST(TestWrapperSetOverrideUpdatesSharedControl) {
        TControlWrapper control(10, 0, 20);
        TControlWrapper shared = control;

        // Set the override through one wrapper and observe it through the other.
        const auto mutation = shared.SetOverride(10);
        UNIT_ASSERT(!mutation.Before.Overridden);
        UNIT_ASSERT(mutation.After.Overridden);

        // Update the default through the first wrapper and verify that both
        // wrappers retain the shared override and default.
        control.UpdateDefault(15);
        UNIT_ASSERT_VALUES_EQUAL(static_cast<i64>(control), 10);
        UNIT_ASSERT(control.GetOverride());
        UNIT_ASSERT_VALUES_EQUAL(*control.GetOverride(), 10);
        UNIT_ASSERT_VALUES_EQUAL(shared.GetDefault(), 15);
    }

    // Verify that generated config field presence sets an override and field
    // absence clears it.
    Y_UNIT_TEST(TestGeneratedConfigPresenceSetsAndClearsOverride) {
        TControlBoard controlBoard;
        controlBoard.CreateConfigControls(false);
        auto control = controlBoard.DataShardControls.MaxTxInFly.AtomicLoad();
        UNIT_ASSERT(control);

        const TAtomicBase defaultValue = control->GetDefault();
        NKikimrConfig::TImmediateControlsConfig config;

        // Apply a present non-default field and record it as an override.
        config.MutableDataShardControls()->SetMaxTxInFly(defaultValue + 1);
        controlBoard.UpdateControls(config);

        const auto override = control->GetOverride();
        UNIT_ASSERT(override);
        UNIT_ASSERT_VALUES_EQUAL(*override, defaultValue + 1);

        // Apply a present equal-to-default field and keep explicit presence.
        config.MutableDataShardControls()->SetMaxTxInFly(defaultValue);
        controlBoard.UpdateControls(config);
        UNIT_ASSERT(!control->IsDefault());
        UNIT_ASSERT_VALUES_EQUAL(*control->GetOverride(), defaultValue);

        // Move the default away and back to prove that the equal-value override
        // remains independent from default updates.
        control->UpdateDefault(defaultValue + 2);
        UNIT_ASSERT_VALUES_EQUAL(control->Get(), defaultValue);
        control->UpdateDefault(defaultValue);

        // Apply a config without the field and restore the current default.
        controlBoard.UpdateControls({});

        UNIT_ASSERT(!control->GetOverride());
        UNIT_ASSERT_VALUES_EQUAL(control->Get(), defaultValue);
    }

    // Verify coherent snapshots while concurrent writers set overrides, clear
    // them, and update defaults.
    Y_UNIT_TEST(TestConcurrentSetClearAndDefaultUpdatesKeepSnapshotsCoherent) {
        constexpr ui32 writerCount = 3;
        constexpr ui32 iterationCount = 1000;

        TIntrusivePtr<TControl> control(new TControl(0, -100000, 100000));
        std::atomic<ui32> writersDone = 0;
        std::barrier startBarrier(writerCount + 1);

        // Alternate a fixed override with restoration from two writers.
        auto writeOverrides = [&](TAtomicBase overrideValue) {
            startBarrier.arrive_and_wait();
            for (ui32 i = 0; i < iterationCount; ++i) {
                control->SetOverride(overrideValue);
                control->RestoreDefault();
            }
            ++writersDone;
        };

        // Move the default between two values while override writers run.
        auto writeDefaults = [&] {
            startBarrier.arrive_and_wait();
            for (ui32 i = 0; i < iterationCount; ++i) {
                control->UpdateDefault(3);
                control->UpdateDefault(4);
            }
            ++writersDone;
        };

        // Construct all writers before releasing the shared start barrier.
        std::thread firstWriter(writeOverrides, 1);
        std::thread secondWriter(writeOverrides, 2);
        std::thread defaultWriter(writeDefaults);

        // Release all participants together and accept only complete states
        // produced by one writer operation.
        startBarrier.arrive_and_wait();
        do {
            const TControlState state = control->GetState();
            UNIT_ASSERT(
                state.Default == 0 || state.Default == 3 || state.Default == 4);
            if (state.Overridden) {
                UNIT_ASSERT(state.Value == 1 || state.Value == 2);
            } else {
                UNIT_ASSERT_VALUES_EQUAL(state.Value, state.Default);
            }
            const auto override = control->GetOverride();
            UNIT_ASSERT(!override || *override == 1 || *override == 2);
        } while (writersDone.load() != writerCount);

        // Wait for every writer after the reader observes completion.
        firstWriter.join();
        secondWriter.join();
        defaultWriter.join();
    }

    // Verify renderer output and counts for an override numerically equal to
    // its default.
    Y_UNIT_TEST(TestHtmlShowsOverrideEqualToUpdatedDefault) {
        TIntrusivePtr<TControl> control(new TControl(10, 0, 20));

        // Create an override, then move the default to the same value.
        control->SetFromHtmlRequest(20);
        control->UpdateDefault(20);

        UNIT_ASSERT(!control->IsDefault());
        UNIT_ASSERT_VALUES_EQUAL(control->Get(), control->GetDefault());

        // Render the active override and verify its marker, restore action, and
        // aggregate count.
        TControlBoardTableHtmlRenderer renderer;
        renderer.AddNewTable("Controls");
        renderer.AddTableItem("TestControl", control);
        const TString html = renderer.GetHtml();

        UNIT_ASSERT(html.find("<span>(overridden)</span>") != TString::npos);
        UNIT_ASSERT(html.find("<b>Restore Default</b>") != TString::npos);
        UNIT_ASSERT(html.find("name='restoreDefault' value='TestControl'") != TString::npos);
        UNIT_ASSERT(html.find("<td>1</td></tr>") != TString::npos);
        UNIT_ASSERT_VALUES_EQUAL(renderer.GetOverriddenCount(), 1);

        // Restore the control and verify that the rendered override indicators
        // and aggregate count disappear.
        control->RestoreDefault();
        TControlBoardTableHtmlRenderer restoredRenderer;
        restoredRenderer.AddNewTable("Controls");
        restoredRenderer.AddTableItem("TestControl", control);
        const TString restoredHtml = restoredRenderer.GetHtml();
        UNIT_ASSERT(restoredHtml.find("<span>(overridden)</span>") == TString::npos);
        UNIT_ASSERT(restoredHtml.find("<b>Restore Default</b>") == TString::npos);
        UNIT_ASSERT(restoredHtml.find("<td>0</td></tr>") != TString::npos);
        UNIT_ASSERT_VALUES_EQUAL(restoredRenderer.GetOverriddenCount(), 0);
    }

    // Verify that HTML accepts the exact current default even when it lies
    // outside the operator bounds.
    Y_UNIT_TEST(TestHtmlOverrideAcceptsDefaultOutsideBounds) {
        TControl control(30, 0, 20);

        // Assign the out-of-bounds default and preserve its exact value as an
        // explicit override.
        const auto mutation = control.SetFromHtmlRequestWithState(30);
        UNIT_ASSERT(!mutation.Before.Overridden);
        UNIT_ASSERT(mutation.After.Overridden);
        UNIT_ASSERT_VALUES_EQUAL(mutation.After.Value, 30);
        UNIT_ASSERT_VALUES_EQUAL(mutation.After.Default, 30);

        // Clear presence and keep the same out-of-bounds default value.
        control.RestoreDefault();
        UNIT_ASSERT(control.IsDefault());
        UNIT_ASSERT_VALUES_EQUAL(control.Get(), 30);
    }

    // Verify DCB equal-default assignment, named restoration, and unknown-name
    // safety.
    Y_UNIT_TEST(TestDynamicDefaultOverrideAndNamedRestore) {
        TDynamicControlBoard board;
        TControlWrapper control(10, 0, 20);
        board.RegisterSharedControl(control, "control");

        // Use the compatibility overload to create an equal-default override.
        TAtomic previous = -1;
        UNIT_ASSERT(!board.SetValue("control", 10, previous));
        UNIT_ASSERT_VALUES_EQUAL(previous, 10);
        UNIT_ASSERT_VALUES_EQUAL(*control.GetOverride(), 10);

        // Restore by name and inspect the flag-only mutation.
        TControlMutation mutation;
        UNIT_ASSERT(board.RestoreDefault("control", mutation));
        UNIT_ASSERT(mutation.Before.Overridden);
        UNIT_ASSERT(!mutation.After.Overridden);
        UNIT_ASSERT_VALUES_EQUAL(mutation.Before.Value, mutation.After.Value);

        // Reject an unknown name without changing the last mutation or control.
        UNIT_ASSERT(!board.RestoreDefault("unknown", mutation));
        UNIT_ASSERT(mutation.Before.Overridden);
        UNIT_ASSERT(!mutation.After.Overridden);
        UNIT_ASSERT(!control.GetOverride());
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
