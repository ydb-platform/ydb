#include "event_local.h"
#include "event_pb.h"
#include "events.h"

#include <library/cpp/testing/unittest/registar.h>
#include <ydb/library/actors/protos/unittests.pb.h>

using namespace NActors;

Y_UNIT_TEST_SUITE(TEventHandleAccess) {
    enum {
        EvProto = EventSpaceBegin(TEvents::ES_PRIVATE),
        EvLocal,
        EvNullLoad,
        EvThrowingLoad,
    };

    struct TProtoEvent : TEventPB<TProtoEvent, TSimple, EvProto> {};
    struct TLocalEvent : TEventLocal<TLocalEvent, EvLocal> {};

    struct TNullLoadEvent : TEventLocal<TNullLoadEvent, EvNullLoad> {
        static TNullLoadEvent* Load(const TEventSerializedData*) {
            return nullptr;
        }
    };

    struct TThrowingLoadEvent : TEventLocal<TThrowingLoadEvent, EvThrowingLoad> {
        static TThrowingLoadEvent* Load(const TEventSerializedData*) {
            ythrow yexception() << "test loader failure";
        }
    };

    Y_UNIT_TEST(LocalGetAndReleasePreservePointer) {
        auto* event = new TLocalEvent;
        IEventHandle handle(TActorId(), TActorId(), event);

        UNIT_ASSERT_VALUES_EQUAL(handle.Get<TLocalEvent>(), event);
        auto released = handle.Release<TLocalEvent>();
        UNIT_ASSERT_VALUES_EQUAL(released.Get(), event);
        UNIT_ASSERT(!handle.HasEvent());
        UNIT_ASSERT(!handle.HasBuffer());
    }

    Y_UNIT_TEST(MismatchedLocalTypeThrows) {
        auto* event = new TLocalEvent;
        IEventHandle handle(TActorId(), TActorId(), event);

        UNIT_ASSERT_EXCEPTION_CONTAINS(handle.Get<TProtoEvent>(), yexception, "doesn't match the expected type");
        UNIT_ASSERT_VALUES_EQUAL(handle.Get<TLocalEvent>(), event);
    }

    Y_UNIT_TEST(MismatchedBufferedTypeThrows) {
        IEventHandle handle(TProtoEvent::EventType, 0, TActorId(), TActorId(), new TEventSerializedData, 0);

        UNIT_ASSERT_EXCEPTION_CONTAINS(handle.Get<TLocalEvent>(), yexception, "doesn't match the expected type");
        UNIT_ASSERT(handle.HasBuffer());
        UNIT_ASSERT(!handle.HasEvent());
    }

    Y_UNIT_TEST(NonLoadableEventThrows) {
        IEventHandle handle(TLocalEvent::EventType, 0, TActorId(), TActorId(), new TEventSerializedData, 0);

        UNIT_ASSERT_EXCEPTION_CONTAINS(handle.Get<TLocalEvent>(), yexception, "cannot be loaded by class");
        UNIT_ASSERT(handle.HasBuffer());
        UNIT_ASSERT(!handle.HasEvent());
    }

    Y_UNIT_TEST(NullLoaderResultThrows) {
        IEventHandle handle(TNullLoadEvent::EventType, 0, TActorId(), TActorId(), new TEventSerializedData, 0);

        UNIT_ASSERT_EXCEPTION_CONTAINS(handle.Get<TNullLoadEvent>(), yexception, "Failed to Load() event type");
        UNIT_ASSERT(!handle.HasBuffer());
        UNIT_ASSERT(!handle.HasEvent());
    }

    Y_UNIT_TEST(ThrowingLoaderPreservesBuffer) {
        IEventHandle handle(TThrowingLoadEvent::EventType, 0, TActorId(), TActorId(), new TEventSerializedData, 0);

        UNIT_ASSERT_EXCEPTION_CONTAINS(handle.Get<TThrowingLoadEvent>(), yexception, "test loader failure");
        UNIT_ASSERT(handle.HasBuffer());
        UNIT_ASSERT(!handle.HasEvent());
    }

    Y_UNIT_TEST(SuccessfulLoadClearsBufferAndReleaseTransfersOwnership) {
        auto* event = new TProtoEvent;
        event->Record.SetStr1("serialized-event-value");
        IEventHandle handle(TActorId(), TActorId(), event);
        handle.Preserialize(false);
        UNIT_ASSERT(handle.HasBuffer());
        UNIT_ASSERT(!handle.HasEvent());

        auto* loaded = handle.Get<TProtoEvent>();
        UNIT_ASSERT_VALUES_EQUAL(loaded->Record.GetStr1(), "serialized-event-value");
        UNIT_ASSERT(!handle.HasBuffer());
        UNIT_ASSERT(handle.HasEvent());
        UNIT_ASSERT_VALUES_EQUAL(handle.Get<TProtoEvent>(), loaded);

        auto released = handle.Release<TProtoEvent>();
        UNIT_ASSERT_VALUES_EQUAL(released.Get(), loaded);
        UNIT_ASSERT(!handle.HasEvent());
        UNIT_ASSERT(!handle.HasBuffer());
    }
}
