#include "event_flat.h"
#include "event_pb.h"
#include "events.h"

#include <library/cpp/testing/unittest/registar.h>
#include <util/generic/vector.h>
#include <util/system/unaligned_mem.h>

#include <cstring>
#include <iterator>
#include <type_traits>

using namespace NActors;

enum {
    EvFlatMessage = EventSpaceBegin(TEvents::ES_PRIVATE),
    EvFlatFixedFields,
    EvFlatRepeatedFields,
    EvFlatPayloadFields,
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

    static TEvFlatMessage* Make() {
        return TBase::MakeEvent();
    }

    auto TabletId() { return TBase::template Field<TTabletIdTag>(); }
    auto TabletId() const { return TBase::template Field<TTabletIdTag>(); }

    bool HasOldField() const { return TBase::template HasField<TOldFieldTag>(); }
    auto OldField() { return TBase::template Field<TOldFieldTag>(); }
    auto OldField() const { return TBase::template Field<TOldFieldTag>(); }

    auto Numbers() { return TBase::template Array<TArrayFieldTag>(); }
    auto Numbers() const { return TBase::template Array<TArrayFieldTag>(); }
    size_t NumbersSize() const { return TBase::template GetSize<TArrayFieldTag>(); }
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

    static TEvFlatRepeatedFields* Make() {
        return TBase::MakeEvent();
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

struct TEvFlatPayloadFields : TEventFlat<TEvFlatPayloadFields> {
    using TBase = TEventFlat<TEvFlatPayloadFields>;

    static constexpr ui32 EventType = EvFlatPayloadFields;

    using TMarkerTag = TBase::FixedField<ui32, 0>;
    using TBlobTag = TBase::BytesField<1>;
    using TNumbersTag = TBase::ArrayField<ui32, 2>;

    using TSchemeV1 = TBase::Scheme<TMarkerTag, TBlobTag, TNumbersTag>;
    using TScheme = TBase::Versions<TSchemeV1>;

    friend class TEventFlat<TEvFlatPayloadFields>;

    static TEvFlatPayloadFields* Make() {
        return TBase::MakeEvent();
    }

    auto Marker() { return TBase::template Field<TMarkerTag>(); }
    auto Marker() const { return TBase::template Field<TMarkerTag>(); }

    auto Blob() { return TBase::template Bytes<TBlobTag>(); }
    auto Blob() const { return TBase::template Bytes<TBlobTag>(); }
    size_t BlobSize() const { return TBase::template GetSize<TBlobTag>(); }

    auto Numbers() { return TBase::template Array<TNumbersTag>(); }
    auto Numbers() const { return TBase::template Array<TNumbersTag>(); }
    size_t NumbersSize() const { return TBase::template GetSize<TNumbersTag>(); }
};

namespace {

    TIntrusivePtr<TEventSerializedData> MakeSerializedData(const TVector<TRope>& payloads) {
        TAllocChunkSerializer serializer;
        UNIT_ASSERT(SerializeToArcadiaStreamImpl(&serializer, payloads));
        return serializer.Release(CreateSerializationInfoImpl(0, false, payloads, 0));
    }

    TString MakeArrayPayload(const TVector<ui32>& values) {
        TString data = TString::Uninitialized(values.size() * sizeof(ui32));
        char* ptr = data.Detach();
        for (size_t i = 0; i < values.size(); ++i) {
            WriteUnaligned<ui32>(ptr + i * sizeof(ui32), values[i]);
        }
        return data;
    }

    TIntrusivePtr<TEventSerializedData> MakeV1Buffer(i64 tabletId, i64 oldField, const TVector<ui32>& values) {
        using TBase = TEvFlatMessage::TBase;
        using TScheme = TEvFlatMessage::TSchemeV1;
        TString header = TString::Uninitialized(TScheme::HeaderSize);
        char* ptr = header.Detach();
        std::memset(ptr, 0, TScheme::HeaderSize);

        WriteUnaligned<ui16>(ptr + 0, 1);
        WriteUnaligned<i64>(ptr + TScheme::template GetFixedOffset<TEvFlatMessage::TTabletIdTag>(), tabletId);
        WriteUnaligned<i64>(ptr + TScheme::template GetFixedOffset<TEvFlatMessage::TOldFieldTag>(), oldField);
        WriteUnaligned<typename TBase::TPayloadRef>(
            ptr + TScheme::template GetPayloadRefOffset<TEvFlatMessage::TArrayFieldTag>(),
            typename TBase::TPayloadRef{
                .PayloadId = values.empty() ? 0u : 1u,
                .Size = static_cast<ui32>(values.size()),
            });

        TVector<TRope> payloads;
        payloads.push_back(TRope(std::move(header)));
        payloads.push_back(TRope(MakeArrayPayload(values)));
        return MakeSerializedData(payloads);
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
        auto buffers = serializer->Release(ev->CreateSerializationInfo(false));

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
        THolder<TEvFlatRepeatedFields> ev(TEvFlatRepeatedFields::Make());
        ev->Marker() = 99;
        ev->Point() = TPoint{5, 6};

        ui32 numbers[] = {3, 6, 9, 12};
        TRepeatedItem item1{1, 2, 100};
        TRepeatedItem item2{3, 4, 200};
        ev->Numbers().Append(numbers, std::size(numbers));
        ev->Items().Append(&item1, 1);
        ev->Items().Append(&item2, 1);

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
        auto buffers = serializer->Release(ev->CreateSerializationInfo(false));

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

    Y_UNIT_TEST(BytesAndArrayPayloadFields) {
        THolder<TEvFlatPayloadFields> ev(TEvFlatPayloadFields::Make());
        ev->Marker() = 7;
        ev->Blob().Append(TRope(TString("ab")));
        ev->Blob().Append(TRope(TString("cd")));

        ui32 part1[] = {1, 2};
        ui32 part2[] = {3, 4};
        ev->Numbers().Append(part1, std::size(part1));
        ev->Numbers().Append(part2, std::size(part2));
        ev->Numbers().Resize(5);
        ev->Numbers()[1] = 22;
        ev->Numbers()[4] = 55;

        UNIT_ASSERT_VALUES_EQUAL(static_cast<ui32>(ev->Marker()), 7);
        UNIT_ASSERT_VALUES_EQUAL(ev->BlobSize(), 4);
        UNIT_ASSERT_VALUES_EQUAL(ev->Blob().Materialize(), "abcd");
        UNIT_ASSERT_VALUES_EQUAL(ev->NumbersSize(), 5);
        UNIT_ASSERT_VALUES_EQUAL(static_cast<ui32>(ev->Numbers()[0]), 1);
        UNIT_ASSERT_VALUES_EQUAL(static_cast<ui32>(ev->Numbers()[1]), 22);
        UNIT_ASSERT_VALUES_EQUAL(static_cast<ui32>(ev->Numbers()[2]), 3);
        UNIT_ASSERT_VALUES_EQUAL(static_cast<ui32>(ev->Numbers()[3]), 4);
        UNIT_ASSERT_VALUES_EQUAL(static_cast<ui32>(ev->Numbers()[4]), 55);

        auto serializer = MakeHolder<TAllocChunkSerializer>();
        UNIT_ASSERT(ev->SerializeToArcadiaStream(serializer.Get()));
        auto buffers = serializer->Release(ev->CreateSerializationInfo(false));

        THolder<IEventHandle> handle(new IEventHandle(
            EvFlatPayloadFields, 0, TActorId(), TActorId(), buffers, 0));

        TEvFlatPayloadFields* loaded = handle->Get<TEvFlatPayloadFields>();
        UNIT_ASSERT_VALUES_EQUAL(loaded->GetVersion(), 1);
        UNIT_ASSERT_VALUES_EQUAL(static_cast<ui32>(loaded->Marker()), 7);
        UNIT_ASSERT_VALUES_EQUAL(loaded->Blob().Materialize(), "abcd");
        UNIT_ASSERT_VALUES_EQUAL(loaded->NumbersSize(), 5);
        UNIT_ASSERT_VALUES_EQUAL(static_cast<ui32>(loaded->Numbers()[0]), 1);
        UNIT_ASSERT_VALUES_EQUAL(static_cast<ui32>(loaded->Numbers()[1]), 22);
        UNIT_ASSERT_VALUES_EQUAL(static_cast<ui32>(loaded->Numbers()[2]), 3);
        UNIT_ASSERT_VALUES_EQUAL(static_cast<ui32>(loaded->Numbers()[3]), 4);
        UNIT_ASSERT_VALUES_EQUAL(static_cast<ui32>(loaded->Numbers()[4]), 55);
    }

    Y_UNIT_TEST(FrontendForSingleVersionPayloadScheme) {
        THolder<TEvFlatPayloadFields> ev(TEvFlatPayloadFields::Make());
        UNIT_ASSERT(ev->IsVersion<TEvFlatPayloadFields::TSchemeV1>());

        auto frontend = ev->GetFrontend<TEvFlatPayloadFields::TSchemeV1>();
        frontend.template Field<TEvFlatPayloadFields::TMarkerTag>() = 31;
        frontend.template Bytes<TEvFlatPayloadFields::TBlobTag>().Append(TRope(TString("xy")));

        ui32 values[] = {11, 22, 33};
        frontend.template Array<TEvFlatPayloadFields::TNumbersTag>().Append(values, std::size(values));

        const TEvFlatPayloadFields& constEv = *ev;
        auto constFrontend = constEv.GetFrontend<TEvFlatPayloadFields::TSchemeV1>();
        UNIT_ASSERT_VALUES_EQUAL(static_cast<ui32>(constFrontend.template Field<TEvFlatPayloadFields::TMarkerTag>()), 31);
        UNIT_ASSERT_VALUES_EQUAL(constFrontend.template Bytes<TEvFlatPayloadFields::TBlobTag>().Materialize(), "xy");
        UNIT_ASSERT_VALUES_EQUAL(constFrontend.template GetSize<TEvFlatPayloadFields::TNumbersTag>(), 3);
        UNIT_ASSERT_VALUES_EQUAL(static_cast<ui32>(constFrontend.template Array<TEvFlatPayloadFields::TNumbersTag>()[2]), 33);
    }

    Y_UNIT_TEST(PayloadFieldAbsenceAndClear) {
        THolder<TEvFlatPayloadFields> ev(TEvFlatPayloadFields::Make());
        ev->Marker() = 13;

        UNIT_ASSERT(!ev->Blob().HasPayload());
        UNIT_ASSERT(ev->Blob().empty());
        UNIT_ASSERT_VALUES_EQUAL(ev->BlobSize(), 0);
        UNIT_ASSERT(ev->Numbers().empty());
        UNIT_ASSERT_VALUES_EQUAL(ev->NumbersSize(), 0);

        ev->Blob().Append(TRope(TString("payload")));
        ui32 values[] = {100, 200, 300};
        ev->Numbers().Append(values, std::size(values));

        UNIT_ASSERT(ev->Blob().HasPayload());
        UNIT_ASSERT_VALUES_EQUAL(ev->Blob().Materialize(), "payload");
        UNIT_ASSERT_VALUES_EQUAL(ev->NumbersSize(), 3);

        ev->Blob().Clear();
        ev->Numbers().Clear();

        UNIT_ASSERT(!ev->Blob().HasPayload());
        UNIT_ASSERT(ev->Blob().empty());
        UNIT_ASSERT_VALUES_EQUAL(ev->BlobSize(), 0);
        UNIT_ASSERT(ev->Numbers().empty());
        UNIT_ASSERT_VALUES_EQUAL(ev->NumbersSize(), 0);

        auto serializer = MakeHolder<TAllocChunkSerializer>();
        UNIT_ASSERT(ev->SerializeToArcadiaStream(serializer.Get()));
        auto buffers = serializer->Release(ev->CreateSerializationInfo(false));

        THolder<IEventHandle> handle(new IEventHandle(
            EvFlatPayloadFields, 0, TActorId(), TActorId(), buffers, 0));

        TEvFlatPayloadFields* loaded = handle->Get<TEvFlatPayloadFields>();
        UNIT_ASSERT_VALUES_EQUAL(loaded->GetVersion(), 1);
        UNIT_ASSERT_VALUES_EQUAL(static_cast<ui32>(loaded->Marker()), 13);
        UNIT_ASSERT(!loaded->Blob().HasPayload());
        UNIT_ASSERT(loaded->Blob().empty());
        UNIT_ASSERT_VALUES_EQUAL(loaded->BlobSize(), 0);
        UNIT_ASSERT(loaded->Numbers().empty());
        UNIT_ASSERT_VALUES_EQUAL(loaded->NumbersSize(), 0);
    }

    Y_UNIT_TEST(CreateSerializeAndLoadLatestVersion) {
        THolder<TEvFlatMessage> ev(TEvFlatMessage::Make());
        ev->TabletId() = 42;
        ui32 values[] = {10, 20, 30};
        ev->Numbers().Append(values, std::size(values));

        UNIT_ASSERT_VALUES_EQUAL(ev->GetVersion(), 2);
        UNIT_ASSERT(!ev->HasOldField());
        UNIT_ASSERT_VALUES_EQUAL(ev->NumbersSize(), 3);

        auto serializer = MakeHolder<TAllocChunkSerializer>();
        UNIT_ASSERT(ev->SerializeToArcadiaStream(serializer.Get()));
        auto buffers = serializer->Release(ev->CreateSerializationInfo(false));

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
        auto buffers = MakeV1Buffer(101, 202, {7, 8, 9, 10});
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

    Y_UNIT_TEST(LoadOldVersionWithEmptyPayload) {
        auto buffers = MakeV1Buffer(101, 202, {});
        THolder<IEventHandle> handle(new IEventHandle(
            EvFlatMessage, 0, TActorId(), TActorId(), buffers, 0));

        TEvFlatMessage* loaded = handle->Get<TEvFlatMessage>();
        UNIT_ASSERT_VALUES_EQUAL(loaded->GetVersion(), 1);
        UNIT_ASSERT(loaded->HasOldField());
        UNIT_ASSERT_VALUES_EQUAL(static_cast<i64>(loaded->TabletId()), 101);
        UNIT_ASSERT_VALUES_EQUAL(static_cast<i64>(loaded->OldField()), 202);
        UNIT_ASSERT(loaded->Numbers().empty());
        UNIT_ASSERT_VALUES_EQUAL(loaded->NumbersSize(), 0);
    }

    Y_UNIT_TEST(VersionedFrontendsForLatestAndOldSchemes) {
        THolder<TEvFlatMessage> ev(TEvFlatMessage::Make());
        UNIT_ASSERT(ev->IsVersion<TEvFlatMessage::TSchemeV2>());
        UNIT_ASSERT(!ev->IsVersion<TEvFlatMessage::TSchemeV1>());

        auto v2 = ev->GetFrontend<TEvFlatMessage::TSchemeV2>();
        v2.template Field<TEvFlatMessage::TTabletIdTag>() = 404;
        ui32 values[] = {9, 8, 7};
        v2.template Array<TEvFlatMessage::TArrayFieldTag>().Append(values, std::size(values));

        const TEvFlatMessage& constLatest = *ev;
        auto constV2 = constLatest.GetFrontend<TEvFlatMessage::TSchemeV2>();
        UNIT_ASSERT_VALUES_EQUAL(static_cast<i64>(constV2.template Field<TEvFlatMessage::TTabletIdTag>()), 404);
        UNIT_ASSERT_VALUES_EQUAL(constV2.template GetSize<TEvFlatMessage::TArrayFieldTag>(), 3);
        UNIT_ASSERT_VALUES_EQUAL(static_cast<ui32>(constV2.template Array<TEvFlatMessage::TArrayFieldTag>()[1]), 8);

        auto buffers = MakeV1Buffer(501, 777, {5, 6});
        THolder<IEventHandle> handle(new IEventHandle(
            EvFlatMessage, 0, TActorId(), TActorId(), buffers, 0));

        TEvFlatMessage* loaded = handle->Get<TEvFlatMessage>();
        UNIT_ASSERT(loaded->IsVersion<TEvFlatMessage::TSchemeV1>());
        UNIT_ASSERT(!loaded->IsVersion<TEvFlatMessage::TSchemeV2>());

        auto v1 = loaded->GetFrontend<TEvFlatMessage::TSchemeV1>();
        UNIT_ASSERT_VALUES_EQUAL(static_cast<i64>(v1.template Field<TEvFlatMessage::TTabletIdTag>()), 501);
        UNIT_ASSERT_VALUES_EQUAL(static_cast<i64>(v1.template Field<TEvFlatMessage::TOldFieldTag>()), 777);
        UNIT_ASSERT_VALUES_EQUAL(v1.template GetSize<TEvFlatMessage::TArrayFieldTag>(), 2);
        UNIT_ASSERT_VALUES_EQUAL(static_cast<ui32>(v1.template Array<TEvFlatMessage::TArrayFieldTag>()[0]), 5);
        UNIT_ASSERT_VALUES_EQUAL(static_cast<ui32>(v1.template Array<TEvFlatMessage::TArrayFieldTag>()[1]), 6);
    }
}
