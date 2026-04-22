#include "event_flat.h"
#include "event_pb.h"
#include "events.h"

#include <library/cpp/testing/unittest/registar.h>
#include <util/generic/vector.h>
#include <util/system/unaligned_mem.h>

using namespace NActors;

enum {
    EvFlatMessage = EventSpaceBegin(TEvents::ES_PRIVATE),
    EvFlatFixedFields,
    EvFlatRepeatedFields,
};

struct TPoint {
    ui32 X;
    ui32 Y;
};

static_assert(std::is_trivially_copyable_v<TPoint>);

struct TRepeatedItem {
    ui16 Left;
    ui16 Right;
    ui32 Score;
};

static_assert(std::is_trivially_copyable_v<TRepeatedItem>);

struct TEvFlatMessage : TEventFlat<TEvFlatMessage> {
    using TBase = TEventFlat<TEvFlatMessage>;

    static constexpr ui32 EventType = EvFlatMessage;

    using TTabletIdTag = TBase::FixedField<i64, 0>;
    using TOldFieldTag = TBase::FixedField<i64, 1>;
    using TArrayFieldTag = TBase::ArrayField<ui32, 2>;

    using TSchemeV1 = TBase::Scheme<TTabletIdTag, TOldFieldTag, TArrayFieldTag>;
    using TSchemeV2 = TBase::Scheme<TTabletIdTag, TArrayFieldTag>;
    using TScheme = TBase::Versions<TSchemeV1, TSchemeV2>;

    friend class TEventFlat<TEvFlatMessage>;

    static TEvFlatMessage* Make(ui32 arraySize) {
        return TBase::MakeEvent(typename TBase::template TRepeatedFieldSize<TArrayFieldTag>{arraySize});
    }

    auto TabletId() {
        return TBase::template Field<TTabletIdTag>();
    }

    auto TabletId() const {
        return TBase::template Field<TTabletIdTag>();
    }

    bool HasOldField() const {
        return TBase::template HasField<TOldFieldTag>();
    }

    auto OldField() {
        return TBase::template Field<TOldFieldTag>();
    }

    auto OldField() const {
        return TBase::template Field<TOldFieldTag>();
    }

    auto Numbers() {
        return TBase::template Array<TArrayFieldTag>();
    }

    auto Numbers() const {
        return TBase::template Array<TArrayFieldTag>();
    }

    size_t NumbersSize() const {
        return TBase::template GetSize<TArrayFieldTag>();
    }
};

struct TEvFlatFixedFields : TEventFlat<TEvFlatFixedFields> {
    using TBase = TEventFlat<TEvFlatFixedFields>;

    static constexpr ui32 EventType = EvFlatFixedFields;

    using TSmallTag = TBase::FixedField<ui16, 0>;
    using TTabletIdTag = TBase::FixedField<i64, 1>;
    using TPointTag = TBase::FixedField<TPoint, 2>;
    using TCookieTag = TBase::FixedField<ui32, 3>;

    using TSchemeV1 = TBase::Scheme<TSmallTag, TTabletIdTag, TPointTag, TCookieTag>;
    using TScheme = TBase::Versions<TSchemeV1>;

    friend class TEventFlat<TEvFlatFixedFields>;

    static TEvFlatFixedFields* Make() {
        return TBase::MakeEvent();
    }

    auto Small() { return TBase::template Field<TSmallTag>(); }
    auto Small() const { return TBase::template Field<TSmallTag>(); }

    auto TabletId() { return TBase::template Field<TTabletIdTag>(); }
    auto TabletId() const { return TBase::template Field<TTabletIdTag>(); }

    auto Point() { return TBase::template Field<TPointTag>(); }
    auto Point() const { return TBase::template Field<TPointTag>(); }

    auto Cookie() { return TBase::template Field<TCookieTag>(); }
    auto Cookie() const { return TBase::template Field<TCookieTag>(); }
};

struct TEvFlatRepeatedFields : TEventFlat<TEvFlatRepeatedFields> {
    using TBase = TEventFlat<TEvFlatRepeatedFields>;

    static constexpr ui32 EventType = EvFlatRepeatedFields;

    using TMarkerTag = TBase::FixedField<ui32, 0>;
    using TPointTag = TBase::FixedField<TPoint, 1>;
    using TNumbersTag = TBase::ArrayField<ui32, 2>;
    using TItemsTag = TBase::ArrayField<TRepeatedItem, 3>;
    using TEmptyItemsTag = TBase::ArrayField<TRepeatedItem, 4>;

    using TSchemeV1 = TBase::Scheme<TMarkerTag, TPointTag, TNumbersTag, TItemsTag, TEmptyItemsTag>;
    using TScheme = TBase::Versions<TSchemeV1>;

    friend class TEventFlat<TEvFlatRepeatedFields>;

    static TEvFlatRepeatedFields* Make(size_t numbers, size_t items, size_t emptyItems) {
        return TBase::MakeEvent(
            typename TBase::template TRepeatedFieldSize<TNumbersTag>{numbers},
            typename TBase::template TRepeatedFieldSize<TItemsTag>{items},
            typename TBase::template TRepeatedFieldSize<TEmptyItemsTag>{emptyItems}
        );
    }

    auto Marker() { return TBase::template Field<TMarkerTag>(); }
    auto Marker() const { return TBase::template Field<TMarkerTag>(); }

    auto Point() { return TBase::template Field<TPointTag>(); }
    auto Point() const { return TBase::template Field<TPointTag>(); }

    auto Numbers() { return TBase::template Array<TNumbersTag>(); }
    auto Numbers() const { return TBase::template Array<TNumbersTag>(); }
    size_t NumbersSize() const { return TBase::template GetSize<TNumbersTag>(); }

    auto Items() { return TBase::template Array<TItemsTag>(); }
    auto Items() const { return TBase::template Array<TItemsTag>(); }
    size_t ItemsSize() const { return TBase::template GetSize<TItemsTag>(); }

    auto EmptyItems() { return TBase::template Array<TEmptyItemsTag>(); }
    auto EmptyItems() const { return TBase::template Array<TEmptyItemsTag>(); }
    size_t EmptyItemsSize() const { return TBase::template GetSize<TEmptyItemsTag>(); }
};

namespace {

    TString MakeV1Buffer(i64 tabletId, i64 oldField, const TVector<ui32>& values) {
        const size_t totalSize = sizeof(ui16) + sizeof(i64) + sizeof(i64) + sizeof(ui32) + values.size() * sizeof(ui32);
        TString data = TString::Uninitialized(totalSize);
        char* ptr = data.Detach();

        WriteUnaligned<ui16>(ptr + 0, 1);
        WriteUnaligned<i64>(ptr + sizeof(ui16), tabletId);
        WriteUnaligned<i64>(ptr + sizeof(ui16) + sizeof(i64), oldField);
        WriteUnaligned<ui32>(ptr + sizeof(ui16) + 2 * sizeof(i64), values.size());

        size_t payloadOffset = sizeof(ui16) + 2 * sizeof(i64) + sizeof(ui32);
        for (size_t i = 0; i < values.size(); ++i) {
            WriteUnaligned<ui32>(ptr + payloadOffset + i * sizeof(ui32), values[i]);
        }

        return data;
    }

} // namespace

Y_UNIT_TEST_SUITE(TEventFlatTest) {
    Y_UNIT_TEST(FixedFieldsWithTrivialStructRoundTrip) {
        THolder<TEvFlatFixedFields> ev(TEvFlatFixedFields::Make());
        ev->Small() = 17;
        ev->TabletId() = 123456789;
        ev->Point() = TPoint{11, 22};
        ev->Cookie() = 77;

        UNIT_ASSERT_VALUES_EQUAL(ev->GetVersion(), 1);
        UNIT_ASSERT_VALUES_EQUAL(static_cast<ui16>(ev->Small()), 17);
        UNIT_ASSERT_VALUES_EQUAL(static_cast<i64>(ev->TabletId()), 123456789);

        const TPoint point = ev->Point();
        UNIT_ASSERT_VALUES_EQUAL(point.X, 11);
        UNIT_ASSERT_VALUES_EQUAL(point.Y, 22);
        UNIT_ASSERT_VALUES_EQUAL(static_cast<ui32>(ev->Cookie()), 77);

        auto serializer = MakeHolder<TAllocChunkSerializer>();
        UNIT_ASSERT(ev->SerializeToArcadiaStream(serializer.Get()));
        auto buffers = serializer->Release(TEventSerializationInfo{});

        THolder<IEventHandle> handle(new IEventHandle(
            EvFlatFixedFields, 0, TActorId(), TActorId(), buffers, 0));

        TEvFlatFixedFields* loaded = handle->Get<TEvFlatFixedFields>();
        UNIT_ASSERT_VALUES_EQUAL(loaded->GetVersion(), 1);
        UNIT_ASSERT_VALUES_EQUAL(static_cast<ui16>(loaded->Small()), 17);
        UNIT_ASSERT_VALUES_EQUAL(static_cast<i64>(loaded->TabletId()), 123456789);
        const TPoint loadedPoint = loaded->Point();
        UNIT_ASSERT_VALUES_EQUAL(loadedPoint.X, 11);
        UNIT_ASSERT_VALUES_EQUAL(loadedPoint.Y, 22);
        UNIT_ASSERT_VALUES_EQUAL(static_cast<ui32>(loaded->Cookie()), 77);
    }

    Y_UNIT_TEST(RepeatedFieldsWithTrivialStructAndEmptyArray) {
        THolder<TEvFlatRepeatedFields> ev(TEvFlatRepeatedFields::Make(4, 2, 0));
        ev->Marker() = 99;
        ev->Point() = TPoint{5, 6};

        ui32 numbers[] = {3, 6, 9, 12};
        ev->Numbers().CopyFrom(numbers, std::size(numbers));
        ev->Items()[0] = TRepeatedItem{1, 2, 100};
        ev->Items()[1] = TRepeatedItem{3, 4, 200};

        UNIT_ASSERT_VALUES_EQUAL(static_cast<ui32>(ev->Marker()), 99);
        const TPoint point = ev->Point();
        UNIT_ASSERT_VALUES_EQUAL(point.X, 5);
        UNIT_ASSERT_VALUES_EQUAL(point.Y, 6);
        UNIT_ASSERT_VALUES_EQUAL(ev->NumbersSize(), 4);
        UNIT_ASSERT_VALUES_EQUAL(ev->ItemsSize(), 2);
        UNIT_ASSERT_VALUES_EQUAL(ev->EmptyItemsSize(), 0);
        UNIT_ASSERT(ev->EmptyItems().empty());

        ui32 copiedNumbers[4] = {};
        ev->Numbers().CopyTo(copiedNumbers, std::size(copiedNumbers));
        UNIT_ASSERT_VALUES_EQUAL(copiedNumbers[0], 3);
        UNIT_ASSERT_VALUES_EQUAL(copiedNumbers[3], 12);

        const TRepeatedItem first = ev->Items()[0];
        const TRepeatedItem second = ev->Items()[1];
        UNIT_ASSERT_VALUES_EQUAL(first.Left, 1);
        UNIT_ASSERT_VALUES_EQUAL(first.Right, 2);
        UNIT_ASSERT_VALUES_EQUAL(first.Score, 100);
        UNIT_ASSERT_VALUES_EQUAL(second.Left, 3);
        UNIT_ASSERT_VALUES_EQUAL(second.Right, 4);
        UNIT_ASSERT_VALUES_EQUAL(second.Score, 200);

        auto serializer = MakeHolder<TAllocChunkSerializer>();
        UNIT_ASSERT(ev->SerializeToArcadiaStream(serializer.Get()));
        auto buffers = serializer->Release(TEventSerializationInfo{});

        THolder<IEventHandle> handle(new IEventHandle(
            EvFlatRepeatedFields, 0, TActorId(), TActorId(), buffers, 0));

        TEvFlatRepeatedFields* loaded = handle->Get<TEvFlatRepeatedFields>();
        UNIT_ASSERT_VALUES_EQUAL(loaded->GetVersion(), 1);
        UNIT_ASSERT_VALUES_EQUAL(static_cast<ui32>(loaded->Marker()), 99);
        const TPoint loadedPoint = loaded->Point();
        UNIT_ASSERT_VALUES_EQUAL(loadedPoint.X, 5);
        UNIT_ASSERT_VALUES_EQUAL(loadedPoint.Y, 6);
        UNIT_ASSERT_VALUES_EQUAL(loaded->NumbersSize(), 4);
        UNIT_ASSERT_VALUES_EQUAL(loaded->ItemsSize(), 2);
        UNIT_ASSERT_VALUES_EQUAL(loaded->EmptyItemsSize(), 0);
        UNIT_ASSERT_VALUES_EQUAL(static_cast<ui32>(loaded->Numbers()[0]), 3);
        UNIT_ASSERT_VALUES_EQUAL(static_cast<ui32>(loaded->Numbers()[3]), 12);
        const TRepeatedItem loadedFirst = loaded->Items()[0];
        const TRepeatedItem loadedSecond = loaded->Items()[1];
        UNIT_ASSERT_VALUES_EQUAL(loadedFirst.Left, 1);
        UNIT_ASSERT_VALUES_EQUAL(loadedFirst.Right, 2);
        UNIT_ASSERT_VALUES_EQUAL(loadedFirst.Score, 100);
        UNIT_ASSERT_VALUES_EQUAL(loadedSecond.Left, 3);
        UNIT_ASSERT_VALUES_EQUAL(loadedSecond.Right, 4);
        UNIT_ASSERT_VALUES_EQUAL(loadedSecond.Score, 200);
        UNIT_ASSERT(loaded->EmptyItems().empty());
    }

    Y_UNIT_TEST(CreateSerializeAndLoadLatestVersion) {
        THolder<TEvFlatMessage> ev(TEvFlatMessage::Make(3));
        ev->TabletId() = 42;
        auto numbers = ev->Numbers();
        numbers[0] = 10;
        numbers[1] = 20;
        numbers[2] = 30;

        UNIT_ASSERT_VALUES_EQUAL(ev->GetVersion(), 2);
        UNIT_ASSERT(!ev->HasOldField());
        UNIT_ASSERT_VALUES_EQUAL(ev->NumbersSize(), 3);

        auto serializer = MakeHolder<TAllocChunkSerializer>();
        UNIT_ASSERT(ev->SerializeToArcadiaStream(serializer.Get()));
        auto buffers = serializer->Release(TEventSerializationInfo{});

        THolder<IEventHandle> handle(new IEventHandle(
            EvFlatMessage, 0, TActorId(), TActorId(), buffers, 0));

        TEvFlatMessage* loaded = handle->Get<TEvFlatMessage>();
        UNIT_ASSERT_VALUES_EQUAL(loaded->GetVersion(), 2);
        UNIT_ASSERT(!loaded->HasOldField());
        UNIT_ASSERT_VALUES_EQUAL(static_cast<i64>(loaded->TabletId()), 42);
        UNIT_ASSERT_VALUES_EQUAL(loaded->NumbersSize(), 3);
        UNIT_ASSERT_VALUES_EQUAL(static_cast<ui32>(loaded->Numbers()[0]), 10);
        UNIT_ASSERT_VALUES_EQUAL(static_cast<ui32>(loaded->Numbers()[1]), 20);
        UNIT_ASSERT_VALUES_EQUAL(static_cast<ui32>(loaded->Numbers()[2]), 30);
    }

    Y_UNIT_TEST(LoadOldVersion) {
        const TString data = MakeV1Buffer(101, 202, {7, 8, 9, 10});
        auto buffers = MakeIntrusive<TEventSerializedData>(data, TEventSerializationInfo{});
        THolder<IEventHandle> handle(new IEventHandle(
            EvFlatMessage, 0, TActorId(), TActorId(), buffers, 0));

        TEvFlatMessage* loaded = handle->Get<TEvFlatMessage>();
        UNIT_ASSERT_VALUES_EQUAL(loaded->GetVersion(), 1);
        UNIT_ASSERT(loaded->HasOldField());
        UNIT_ASSERT_VALUES_EQUAL(static_cast<i64>(loaded->TabletId()), 101);
        UNIT_ASSERT_VALUES_EQUAL(static_cast<i64>(loaded->OldField()), 202);
        UNIT_ASSERT_VALUES_EQUAL(loaded->NumbersSize(), 4);
        UNIT_ASSERT_VALUES_EQUAL(static_cast<ui32>(loaded->Numbers()[0]), 7);
        UNIT_ASSERT_VALUES_EQUAL(static_cast<ui32>(loaded->Numbers()[3]), 10);
    }
}
