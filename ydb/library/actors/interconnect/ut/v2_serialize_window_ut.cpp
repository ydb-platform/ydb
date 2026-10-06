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

    Y_UNIT_TEST(StartsAtMaxAndReportsRemaining) {
        TSerializeWindow window(MinWindow, MaxWindow);
        UNIT_ASSERT_VALUES_EQUAL(window.GetMainSize(), MaxWindow);
        UNIT_ASSERT_VALUES_EQUAL(window.GetXdcSize(), 0);
        UNIT_ASSERT_VALUES_EQUAL(window.GetSize(), MaxWindow);
        UNIT_ASSERT_VALUES_EQUAL(window.RemainingMain(0), MaxWindow);
        UNIT_ASSERT_VALUES_EQUAL(window.RemainingMain(MaxWindow / 2), MaxWindow / 2);
        UNIT_ASSERT_VALUES_EQUAL(window.RemainingMain(MaxWindow), 0);
        UNIT_ASSERT_VALUES_EQUAL(window.RemainingXdc(0), 0);
    }

    Y_UNIT_TEST(XdcCapsMainSeparately) {
        constexpr size_t xdcMax = 256 * 1024;
        TSerializeWindow window(MinWindow, xdcMax, /*hasXdc=*/ true);
        UNIT_ASSERT_VALUES_EQUAL(window.GetMainSize(), TSerializeWindow::MaxMainWindowWithXdc);
        UNIT_ASSERT_VALUES_EQUAL(window.GetXdcSize(), xdcMax);
        UNIT_ASSERT_VALUES_EQUAL(window.GetSize(), window.GetMainSize() + window.GetXdcSize());
        UNIT_ASSERT_VALUES_EQUAL(window.RemainingXdc(1024), xdcMax - 1024);
    }

    Y_UNIT_TEST(ShortWriteShrinksOnlyThatSocket) {
        TSerializeWindow window(MinWindow, MaxWindow, /*hasXdc=*/ true);
        const size_t mainBefore = window.GetMainSize();
        const size_t xdcBefore = window.GetXdcSize();

        window.CompleteWrite(xdcBefore / 2, xdcBefore, /*xdc=*/ true);
        UNIT_ASSERT_VALUES_EQUAL(window.GetXdcSize(), xdcBefore - MinWindow);
        UNIT_ASSERT_VALUES_EQUAL(window.GetMainSize(), mainBefore);

        window.CompleteWrite(mainBefore / 2, mainBefore, /*xdc=*/ false);
        UNIT_ASSERT_VALUES_EQUAL(window.GetMainSize(), mainBefore - MinWindow);
        UNIT_ASSERT_VALUES_EQUAL(window.GetXdcSize(), xdcBefore - MinWindow);
    }

    Y_UNIT_TEST(FullTargetWriteGrowsBack) {
        TSerializeWindow window(MinWindow, MaxWindow);
        window.CompleteWrite(MinWindow, MaxWindow, /*xdc=*/ false);
        UNIT_ASSERT_VALUES_EQUAL(window.GetMainSize(), MaxWindow - MinWindow);

        window.CompleteWrite(window.GetMainSize(), window.GetMainSize(), /*xdc=*/ false);
        UNIT_ASSERT_VALUES_EQUAL(window.GetMainSize(), MaxWindow);
    }

    Y_UNIT_TEST(UnderfilledFullWriteDoesNotShrink) {
        TSerializeWindow window(MinWindow, MaxWindow, /*hasXdc=*/ true);
        const size_t mainBefore = window.GetMainSize();
        const size_t xdcBefore = window.GetXdcSize();

        window.CompleteWrite(128, 128, /*xdc=*/ false);
        window.CompleteWrite(2048, 2048, /*xdc=*/ true);
        UNIT_ASSERT_VALUES_EQUAL(window.GetMainSize(), mainBefore);
        UNIT_ASSERT_VALUES_EQUAL(window.GetXdcSize(), xdcBefore);
    }

    Y_UNIT_TEST(FixedWindowStaysPut) {
        TSerializeWindow window(MaxWindow, MaxWindow);
        window.CompleteWrite(128, 128, /*xdc=*/ false);
        UNIT_ASSERT_VALUES_EQUAL(window.GetMainSize(), MaxWindow);
        window.CompleteWrite(1024, MaxWindow, /*xdc=*/ false);
        UNIT_ASSERT_VALUES_EQUAL(window.GetMainSize(), MaxWindow);
        window.CompleteWrite(MaxWindow, MaxWindow, /*xdc=*/ false);
        UNIT_ASSERT_VALUES_EQUAL(window.GetMainSize(), MaxWindow);
    }

    Y_UNIT_TEST(IndependentStreamBudgetsDrainXdcEvents) {
        for (bool preserialize : {false, true}) {
            TEventSerializer serializer(false, true);
            TSerializeWindow window(MinWindow, MaxWindow, /*hasXdc=*/ true);
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
            size_t eventsCommitted = 0;
            size_t mainUnsent = 0;
            size_t xdcUnsent = 0;
            for (size_t batch = 0; serializer.IsTrafficPending() || mainUnsent || xdcUnsent; ++batch) {
                UNIT_ASSERT_LT(batch, 4 * NumEvents);
                const size_t mainBudget = window.RemainingMain(mainUnsent);
                const size_t xdcBudget = window.RemainingXdc(xdcUnsent);
                if (mainBudget || xdcBudget) {
                    if (mainBuffer.size() < MinWindow) {
                        mainBuffer = TRcBuf::Uninitialized(MinWindow);
                    }
                    if (xdcBuffer.size() < MinWindow) {
                        xdcBuffer = TRcBuf::Uninitialized(MinWindow);
                    }
                    std::vector<TContiguousSpan> mainSpans;
                    std::vector<TContiguousSpan> xdcSpans;
                    const ui64 mainBefore = serializer.GetCumulativeProducedMain();
                    const ui64 xdcBefore = serializer.GetCumulativeProducedXdc();
                    serializer.ProduceOutputStream(mainBuffer, &mainSpans, &xdcBuffer, &xdcSpans,
                        mainBudget, xdcBudget);
                    mainUnsent += serializer.GetCumulativeProducedMain() - mainBefore;
                    xdcUnsent += serializer.GetCumulativeProducedXdc() - xdcBefore;
                }

                UNIT_ASSERT(mainUnsent || xdcUnsent || !serializer.IsTrafficPending());

                std::vector<std::unique_ptr<IEventBase>> events;
                std::vector<TIntrusivePtr<TEventSerializedData>> buffers;
                // Complete one stream at a time so a short XDC write cannot move the main cap.
                if (xdcUnsent) {
                    const size_t n = Min(xdcUnsent, window.GetXdcSize());
                    window.CompleteWrite(n, n, /*xdc=*/ true);
                    serializer.CommitProducedBytes(0, n, nullptr, &events, &buffers);
                    xdcUnsent -= n;
                } else if (mainUnsent) {
                    const size_t n = Min(mainUnsent, window.GetMainSize());
                    window.CompleteWrite(n, n, /*xdc=*/ false);
                    serializer.CommitProducedBytes(n, 0, nullptr, &events, &buffers);
                    mainUnsent -= n;
                }
                eventsCommitted += events.size() + buffers.size();
            }
            UNIT_ASSERT_VALUES_EQUAL(eventsCommitted, NumEvents);
            UNIT_ASSERT_VALUES_EQUAL(window.GetXdcSize(), MaxWindow);
            UNIT_ASSERT_VALUES_EQUAL(window.GetMainSize(), Min(MaxWindow, TSerializeWindow::MaxMainWindowWithXdc));
        }
    }

    Y_UNIT_TEST(XdcShortWriteDoesNotShrinkMainWhileDraining) {
        TEventSerializer serializer(false, true);
        TSerializeWindow window(MinWindow, MaxWindow, /*hasXdc=*/ true);
        auto ev = std::make_unique<TEvPayload>();
        ev->Record.SetMeta("short");
        ev->AddPayload(TRope(TString(32 * 1024, 'y')));
        auto handle = std::make_unique<IEventHandle>(TActorId(2, 0, 1, 0), TActorId(1, 0, 1, 0),
            ev.release(), 0, 1);
        serializer.Push(std::move(handle));

        TRcBuf mainBuffer = TRcBuf::Uninitialized(MinWindow);
        TRcBuf xdcBuffer = TRcBuf::Uninitialized(MinWindow);
        std::vector<TContiguousSpan> mainSpans;
        std::vector<TContiguousSpan> xdcSpans;
        const size_t produced = serializer.ProduceOutputStream(mainBuffer, &mainSpans, &xdcBuffer, &xdcSpans,
            window.RemainingMain(0), window.RemainingXdc(0));
        UNIT_ASSERT_GT(produced, 0);
        const size_t xdc = serializer.GetCumulativeProducedXdc();
        UNIT_ASSERT_GT(xdc, 0);

        const size_t mainBefore = window.GetMainSize();
        window.CompleteWrite(xdc / 2, xdc, /*xdc=*/ true);
        UNIT_ASSERT_VALUES_EQUAL(window.GetMainSize(), mainBefore);
        UNIT_ASSERT_VALUES_EQUAL(window.GetXdcSize(), MaxWindow - MinWindow);
    }
}
