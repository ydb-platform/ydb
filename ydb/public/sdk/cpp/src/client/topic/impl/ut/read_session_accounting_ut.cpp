#define INCLUDE_READ_SESSION_IMPL_H
#include <ydb/public/sdk/cpp/src/client/topic/impl/read_session_impl.h>
#undef INCLUDE_READ_SESSION_IMPL_H

#include <library/cpp/testing/unittest/registar.h>

namespace NYdb::inline Dev::NTopic {

    Y_UNIT_TEST_SUITE(TReadSessionDecompressionAccounting) {
        Y_UNIT_TEST(ReadyTransitionDuringCleanupReleasesTaskTwice) {
            // Both messages belong to one decompression task and share its flags.
            std::atomic<bool> ready = false;
            std::atomic<bool> abandoned = false;
            TDataDecompressionEvent<false> first(0, 0, {}, ready, abandoned);
            TDataDecompressionEvent<false> second(0, 1, {}, ready, abandoned);

            // Cleanup sees the first message before the task becomes ready.
            UNIT_ASSERT(!first.IsReady());
            UNIT_ASSERT(first.SetAbandoned());

            // The task finishes before cleanup examines the next message.
            ready = true;
            constexpr i64 firstSize = 10;
            constexpr i64 secondSize = 20;
            const i64 cleanupReleased = second.IsReady() || !second.SetAbandoned() ? secondSize : 0;

            // The task's abandoned path releases all its messages.
            bool expected = false;
            const i64 taskReleased = !abandoned.compare_exchange_strong(expected, true)
                                         ? firstSize + secondSize
                                         : 0;

            UNIT_ASSERT_VALUES_EQUAL(cleanupReleased + taskReleased, firstSize + secondSize);
        }
    } // Y_UNIT_TEST_SUITE(TReadSessionDecompressionAccounting)

} // namespace NYdb::inline Dev::NTopic
