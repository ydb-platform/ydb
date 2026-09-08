#include <ydb/library/actors/core/event_pb.h>
#include <ydb/library/actors/core/events.h>
#include <ydb/library/actors/interconnect/v2_event_serializer.h>
#include <ydb/library/actors/interconnect/v2_serialize_window.h>
#include <ydb/library/actors/protos/unittests.pb.h>

#include <library/cpp/testing/unittest/registar.h>

using namespace NActors;

namespace {
    constexpr size_t MinWindow = 4096;
    constexpr size_t MaxWindow = 64 * 1024;

    struct TEvPayload: TEventPB<TEvPayload, TMessageWithPayload, EventSpaceBegin(TEvents::ES_PRIVATE)> {};
}

Y_UNIT_TEST_SUITE(InterconnectV2SerializeWindow) {

    Y_UNIT_TEST(XdcFullBatchesGrowRegardlessOfCompletionOrder) {
        for (bool xdcFirst : {false, true}) {
            TSerializeWindow window(MinWindow);
            for (size_t batch = 0; batch < 32; ++batch) {
                const size_t size = window.GetSize();
                const size_t main = 128;
                const size_t xdc = size - main;
                window.BeginBatch(main + xdc);

                const size_t first = xdcFirst ? xdc : main;
                window.CompleteWrite(first, first, false, MinWindow, MaxWindow);
                UNIT_ASSERT_VALUES_EQUAL(window.GetSize(), size);

                const size_t last = xdcFirst ? main : xdc;
                window.CompleteWrite(last, last, true, MinWindow, MaxWindow);
                UNIT_ASSERT_VALUES_EQUAL(window.GetSize(), Min(size + MinWindow, MaxWindow));
            }
            UNIT_ASSERT_VALUES_EQUAL(window.GetSize(), MaxWindow);
        }
    }

    Y_UNIT_TEST(ShortWriteIsRememberedUntilBothSocketsComplete) {
        for (bool xdcFirst : {false, true}) {
            TSerializeWindow window(4 * MinWindow);
            const size_t main = 1024;
            const size_t xdc = window.GetSize() - main;
            window.BeginBatch(main + xdc);

            const size_t first = xdcFirst ? xdc : main;
            window.CompleteWrite(first / 2, first, false, MinWindow, MaxWindow);
            UNIT_ASSERT_VALUES_EQUAL(window.GetSize(), 4 * MinWindow);

            // Retry the short write while the other socket's original write is still pending.
            window.CompleteWrite(first / 2, first / 2, false, MinWindow, MaxWindow);
            UNIT_ASSERT_VALUES_EQUAL(window.GetSize(), 4 * MinWindow);

            const size_t last = xdcFirst ? main : xdc;
            window.CompleteWrite(last, last, true, MinWindow, MaxWindow);
            UNIT_ASSERT_VALUES_EQUAL(window.GetSize(), 3 * MinWindow);
        }
    }

    Y_UNIT_TEST(UnderfilledBatchShrinksOnce) {
        TSerializeWindow window(4 * MinWindow);
        for (size_t batch = 0; batch < 5; ++batch) {
            const size_t size = window.GetSize();
            window.BeginBatch(2048);
            window.CompleteWrite(128, 128, false, MinWindow, MaxWindow);
            UNIT_ASSERT_VALUES_EQUAL(window.GetSize(), size);
            window.CompleteWrite(1920, 1920, true, MinWindow, MaxWindow);
            UNIT_ASSERT_VALUES_EQUAL(window.GetSize(), Max(size - MinWindow, MinWindow));
        }
        UNIT_ASSERT_VALUES_EQUAL(window.GetSize(), MinWindow);
    }

    Y_UNIT_TEST(SingleSocketBatchesAndShortWrites) {
        TSerializeWindow window(MinWindow);
        window.BeginBatch(MinWindow);
        window.CompleteWrite(MinWindow, MinWindow, true, MinWindow, MaxWindow);
        UNIT_ASSERT_VALUES_EQUAL(window.GetSize(), 2 * MinWindow);

        window.BeginBatch(2 * MinWindow);
        window.CompleteWrite(MinWindow, 2 * MinWindow, true, MinWindow, MaxWindow);
        UNIT_ASSERT_VALUES_EQUAL(window.GetSize(), MinWindow);

        // A successful new batch must not inherit the previous batch's short-write result.
        window.BeginBatch(MinWindow);
        window.CompleteWrite(MinWindow, MinWindow, true, MinWindow, MaxWindow);
        UNIT_ASSERT_VALUES_EQUAL(window.GetSize(), 2 * MinWindow);
    }

    Y_UNIT_TEST(MainOnlyBatchAfterXdc) {
        TSerializeWindow window(MinWindow);
        window.BeginBatch(MinWindow);
        window.CompleteWrite(128, 128, false, MinWindow, MaxWindow);
        window.CompleteWrite(MinWindow - 128, MinWindow - 128, true, MinWindow, MaxWindow);

        window.BeginBatch(2 * MinWindow);
        window.CompleteWrite(2 * MinWindow, 2 * MinWindow, true, MinWindow, MaxWindow);
        UNIT_ASSERT_VALUES_EQUAL(window.GetSize(), 3 * MinWindow);
    }

    Y_UNIT_TEST(FixedWindow) {
        TSerializeWindow window(MaxWindow);
        window.BeginBatch(MaxWindow);
        window.CompleteWrite(128, 128, false, MaxWindow, MaxWindow);
        window.CompleteWrite(MaxWindow - 128, MaxWindow - 128, true, MaxWindow, MaxWindow);
        UNIT_ASSERT_VALUES_EQUAL(window.GetSize(), MaxWindow);

        window.BeginBatch(MinWindow);
        window.CompleteWrite(1024, MinWindow, true, MaxWindow, MaxWindow);
        UNIT_ASSERT_VALUES_EQUAL(window.GetSize(), MaxWindow);
    }

    Y_UNIT_TEST(FourKiBPayloadsUseGrowingBatches) {
        for (bool preserialize : {false, true}) {
            for (bool xdcFirst : {false, true}) {
                TEventSerializer serializer(false, true);
                TSerializeWindow window(MinWindow);
                constexpr size_t NumEvents = 512;
                for (size_t i = 0; i < NumEvents; ++i) {
                    auto ev = std::make_unique<TEvPayload>();
                    ev->Record.SetMeta("window");
                    ev->AddPayload(TRope(TString(4096, 'x')));
                    auto handle = std::make_unique<IEventHandle>(TActorId(2, 0, 1, 0), TActorId(1, 0, 1, 0),
                        ev.release(), 0, i);
                    if (preserialize) {
                        handle->Preserialize(true);
                    }
                    serializer.Push(std::move(handle));
                }

                TRcBuf mainBuffer;
                TRcBuf xdcBuffer;
                size_t peakWindow = 0;
                size_t eventsCommitted = 0;
                for (size_t batch = 0; serializer.IsTrafficPending(); ++batch) {
                    UNIT_ASSERT_LT(batch, 2 * NumEvents);
                    const size_t size = window.GetSize();
                    peakWindow = Max(peakWindow, size);
                    const ui64 mainBefore = serializer.GetCumulativeProducedMain();
                    const ui64 xdcBefore = serializer.GetCumulativeProducedXdc();
                    std::vector<TContiguousSpan> mainSpans;
                    std::vector<TContiguousSpan> xdcSpans;
                    size_t produced = 0;
                    while (produced < size && mainSpans.size() < 64 && xdcSpans.size() < 64) {
                        if (mainBuffer.size() < MinWindow) {
                            mainBuffer = TRcBuf::Uninitialized(MinWindow);
                        }
                        if (xdcBuffer.size() < MinWindow) {
                            xdcBuffer = TRcBuf::Uninitialized(MinWindow);
                        }
                        const size_t n = serializer.ProduceOutputStream(mainBuffer, &mainSpans,
                            &xdcBuffer, &xdcSpans, size - produced);
                        if (!n) {
                            break;
                        }
                        produced += n;
                    }
                    UNIT_ASSERT_GT(produced, 0);

                    const size_t main = serializer.GetCumulativeProducedMain() - mainBefore;
                    const size_t xdc = serializer.GetCumulativeProducedXdc() - xdcBefore;
                    window.BeginBatch(main + xdc);
                    std::vector<std::unique_ptr<IEventBase>> events;
                    std::vector<TIntrusivePtr<TEventSerializedData>> buffers;
                    auto complete = [&](bool isXdc, bool last) {
                        const size_t n = isXdc ? xdc : main;
                        if (n) {
                            window.CompleteWrite(n, n, last, MinWindow, MaxWindow);
                            serializer.CommitProducedBytes(isXdc ? 0 : n, isXdc ? n : 0, nullptr, &events, &buffers);
                        }
                    };
                    complete(xdcFirst, !(xdcFirst ? main : xdc));
                    complete(!xdcFirst, true);
                    eventsCommitted += events.size() + buffers.size();
                }
                UNIT_ASSERT_VALUES_EQUAL(eventsCommitted, NumEvents);
                UNIT_ASSERT_VALUES_EQUAL(peakWindow, MaxWindow);
            }
        }
    }
}
