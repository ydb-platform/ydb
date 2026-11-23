#include <ydb/library/actors/interconnect/interconnect_uring_engine_client.h>
#include <ydb/library/actors/interconnect/interconnect_common.h>

#include <ydb/library/actors/testlib/test_runtime.h>

#include <library/cpp/testing/unittest/registar.h>

#include <array>
#include <thread>

using namespace NActors;

namespace {

    struct TEvTracked final : TEventLocal<TEvTracked, TEvents::ES_PRIVATE> {
        size_t& Destroyed;
        const ui32 Size;

        TEvTracked(size_t& destroyed, ui32 size)
            : Destroyed(destroyed)
            , Size(size)
        {}

        ~TEvTracked() override {
            ++Destroyed;
        }

        ui32 CalculateSerializedSize() const override {
            return Size;
        }
    };

    struct TClientFixture {
        TTestActorRuntimeBase Runtime;
        TInterconnectProxyCommon Common;
        IUringEngineClientPtr Client;
        size_t Destroyed = 0;
        static constexpr TAtomicBase Baseline = 17;

        TClientFixture() {
            Runtime.Initialize();
            Common.DestructorId = Runtime.AllocateEdgeActor();
            Common.DestructorQueueSize = std::make_shared<std::atomic<TAtomicBase>>(Baseline);
            Client = CreateUringEngineClient(Common, Runtime.GetActorSystem(0));
        }

        ~TClientFixture() {
            // Objects borrow Destroyed, so destroy them before fixture members are torn down.
            Client->Stop();
            TakeBatches().clear();
            Client.reset();
        }

        void AddEvent(ui32 size = 1, TIntrusivePtr<TEventSerializedData> buffer = {}) {
            Client->DestroyEvent(std::make_unique<TEvTracked>(Destroyed, size), std::move(buffer));
        }

        TEventsList TakeBatches() {
            return Runtime.CaptureMailboxEvents(Common.DestructorId.Hint(), Common.DestructorId.NodeId());
        }

        void CheckReleased(TEventsList& batches, size_t objects, size_t bytes) {
            UNIT_ASSERT_VALUES_EQUAL(batches.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(batches.front()->Recipient, Common.DestructorId);
            UNIT_ASSERT_VALUES_EQUAL(Destroyed, 0);
            UNIT_ASSERT_VALUES_EQUAL(Common.DestructorQueueSize->load(), Baseline + bytes);
            batches.clear();
            UNIT_ASSERT_VALUES_EQUAL(Destroyed, objects);
            UNIT_ASSERT_VALUES_EQUAL(Common.DestructorQueueSize->load(), Baseline);
        }
    };

}

Y_UNIT_TEST_SUITE(InterconnectUringEngineClient) {
    Y_UNIT_TEST(TimeIncludesUnflushedEvents) {
        TClientFixture fixture;
        UNIT_ASSERT_VALUES_EQUAL(fixture.Client->GetAndResetDestroyEventsCycles(), 0);
        fixture.AddEvent();
        UNIT_ASSERT(fixture.TakeBatches().empty());
        UNIT_ASSERT(fixture.Client->GetAndResetDestroyEventsCycles() > 0);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Client->GetAndResetDestroyEventsCycles(), 0);
        fixture.Client->Stop();
        UNIT_ASSERT_VALUES_EQUAL(fixture.Client->GetAndResetDestroyEventsCycles(), 0);
        auto batches = fixture.TakeBatches();
        fixture.CheckReleased(batches, 1, 1);
        // Destruction in the receiving actor is outside the client's measured work.
        UNIT_ASSERT_VALUES_EQUAL(fixture.Client->GetAndResetDestroyEventsCycles(), 0);
    }

    Y_UNIT_TEST(ConcurrentTimeReadersDoNotDuplicate) {
        TClientFixture fixture;
        fixture.AddEvent();
        std::array<ui64, 4> cycles{};
        std::array<std::thread, 4> readers;
        for (size_t i = 0; i < readers.size(); ++i) {
            readers[i] = std::thread([&, i] {
                cycles[i] = fixture.Client->GetAndResetDestroyEventsCycles();
            });
        }
        for (auto& reader : readers) {
            reader.join();
        }
        size_t nonzero = 0;
        for (const ui64 value : cycles) {
            nonzero += value != 0;
        }
        UNIT_ASSERT_VALUES_EQUAL(nonzero, 1);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Client->GetAndResetDestroyEventsCycles(), 0);
    }

    Y_UNIT_TEST(StopFlushesPartialBatchOnce) {
        TClientFixture fixture;
        auto buffer = MakeIntrusive<TEventSerializedData>(TString("buffer"), TEventSerializationInfo{});
        constexpr ui32 eventSize = 11;
        fixture.AddEvent(eventSize, buffer);
        UNIT_ASSERT(fixture.TakeBatches().empty());
        UNIT_ASSERT_VALUES_EQUAL(fixture.Destroyed, 0);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Common.DestructorQueueSize->load(), fixture.Baseline);
        fixture.Client->Stop();
        auto batches = fixture.TakeBatches();
        UNIT_ASSERT_VALUES_EQUAL(buffer.RefCount(), 2);
        fixture.CheckReleased(batches, 1, eventSize + buffer->GetSize());
        UNIT_ASSERT_VALUES_EQUAL(buffer.RefCount(), 1);
        fixture.Client->Stop();
        UNIT_ASSERT(fixture.TakeBatches().empty());
    }

    Y_UNIT_TEST(ObjectThresholdCountsEventsAndBuffers) {
        TClientFixture fixture;
        auto buffer = MakeIntrusive<TEventSerializedData>(TString("x"), TEventSerializationInfo{});
        for (size_t i = 0; i < 127; ++i) {
            fixture.AddEvent(1, buffer);
        }
        fixture.AddEvent();
        UNIT_ASSERT(fixture.TakeBatches().empty());
        fixture.Client->DestroyEvent({}, buffer);
        auto batches = fixture.TakeBatches();
        fixture.CheckReleased(batches, 128, 128 + 128 * buffer->GetSize());
        UNIT_ASSERT_VALUES_EQUAL(buffer.RefCount(), 1);
        fixture.Client->Stop();
        UNIT_ASSERT(fixture.TakeBatches().empty());
    }

    Y_UNIT_TEST(ByteThresholdCountsEventsAndBuffers) {
        TClientFixture fixture;
        auto buffer = MakeIntrusive<TEventSerializedData>(TString(32 * 1024, 'x'), TEventSerializationInfo{});
        const ui32 eventSize = 64 * 1024 - buffer->GetSize() - 1;
        fixture.AddEvent(eventSize, buffer);
        UNIT_ASSERT(fixture.TakeBatches().empty());
        fixture.AddEvent();
        auto batches = fixture.TakeBatches();
        fixture.CheckReleased(batches, 2, eventSize + buffer->GetSize() + 1);
        UNIT_ASSERT_VALUES_EQUAL(buffer.RefCount(), 1);
    }

    Y_UNIT_TEST(QueueLimitDestroysSynchronously) {
        TClientFixture fixture;
        fixture.Common.MaxDestructorQueueSize = fixture.Baseline + 10;
        fixture.Client = CreateUringEngineClient(fixture.Common, fixture.Runtime.GetActorSystem(0));
        fixture.AddEvent(11);
        fixture.Client->Stop();
        UNIT_ASSERT(fixture.TakeBatches().empty());
        UNIT_ASSERT_VALUES_EQUAL(fixture.Destroyed, 1);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Common.DestructorQueueSize->load(), fixture.Baseline);
    }
}
