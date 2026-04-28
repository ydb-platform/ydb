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
    EvFlatInlineFields,
    EvFlatEmpty,
    EvFlatOptionalEmpty,
    EvFlatStrictArrayFields,
    EvFlatStrictBytesFields,
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

using TFlatEventDefs = TEventFlatLayout;

struct TEvFlatEmpty;

struct TEvFlatEmptyScheme {
    using TVersions = TFlatEventDefs::Versions<>;
};

struct TEvFlatEmpty : TEventFlat<TEvFlatEmpty, TEvFlatEmptyScheme> {
    using TBase = TEventFlat<TEvFlatEmpty, TEvFlatEmptyScheme>;

    static constexpr ui32 EventType = EvFlatEmpty;

    using TScheme = TEvFlatEmptyScheme;

    friend class TEventFlat<TEvFlatEmpty, TEvFlatEmptyScheme>;

    static TEvFlatEmpty* Make() {
        return TBase::MakeEvent();
    }
};

struct TEvFlatOptionalEmpty;

struct TEvFlatOptionalEmptyScheme {
    using TMarkerTag = TFlatEventDefs::FixedField<ui32, 0>;
    using TSchemeV1 = TFlatEventDefs::Scheme<TMarkerTag>;
    using TVersions = TFlatEventDefs::Versions<TFlatEventDefs::WithEmptyVersion, TSchemeV1>;
};

struct TEvFlatOptionalEmpty : TEventFlat<TEvFlatOptionalEmpty, TEvFlatOptionalEmptyScheme> {
    using TBase = TEventFlat<TEvFlatOptionalEmpty, TEvFlatOptionalEmptyScheme>;

    static constexpr ui32 EventType = EvFlatOptionalEmpty;

    using TScheme = TEvFlatOptionalEmptyScheme;
    using TMarkerTag = TScheme::TMarkerTag;
    using TSchemeV1 = TScheme::TSchemeV1;

    friend class TEventFlat<TEvFlatOptionalEmpty, TEvFlatOptionalEmptyScheme>;

    static TEvFlatOptionalEmpty* Make() {
        return TBase::MakeEvent();
    }

    static TEvFlatOptionalEmpty* MakeNoData() {
        THolder<TEvFlatOptionalEmpty> holder(new TEvFlatOptionalEmpty());
        holder->InitializeAsNoData();
        return holder.Release();
    }

    auto Marker() { return this->template Field<TMarkerTag>(); }
    auto Marker() const { return this->template Field<TMarkerTag>(); }
};

struct TEvFlatMessage;
struct TEvFlatMessageScheme {
    using TTabletIdTag = TFlatEventDefs::FixedField<i64, 0>;
    using TOldFieldTag = TFlatEventDefs::FixedField<i64, 1>;
    using TArrayFieldTag = TFlatEventDefs::ArrayField<ui32, 2>;
    using TSchemeV1 = TFlatEventDefs::Scheme<TFlatEventDefs::WithPayloadType<ui8>,
        TTabletIdTag, TOldFieldTag, TArrayFieldTag>;
    using TSchemeV2 = TFlatEventDefs::Scheme<TFlatEventDefs::WithPayloadType<ui8>,
        TTabletIdTag, TArrayFieldTag>;
    using TVersions = TFlatEventDefs::Versions<TSchemeV1, TSchemeV2>;
};

struct TEvFlatMessage : TEventFlat<TEvFlatMessage, TEvFlatMessageScheme> {
    using TBase = TEventFlat<TEvFlatMessage, TEvFlatMessageScheme>;

    static constexpr ui32 EventType = EvFlatMessage;

    using TScheme = TEvFlatMessageScheme;
    using TTabletIdTag = TScheme::TTabletIdTag;
    using TOldFieldTag = TScheme::TOldFieldTag;
    using TArrayFieldTag = TScheme::TArrayFieldTag;
    using TSchemeV1 = TScheme::TSchemeV1;
    using TSchemeV2 = TScheme::TSchemeV2;

    friend class TEventFlat<TEvFlatMessage, TEvFlatMessageScheme>;

    static TEvFlatMessage* Make() {
        return TBase::MakeEvent();
    }

    auto TabletId() { return this->template Field<TTabletIdTag>(); }
    auto TabletId() const { return this->template Field<TTabletIdTag>(); }

    bool HasOldField() const { return this->template HasField<TOldFieldTag>(); }
    auto OldField() { return this->template Field<TOldFieldTag>(); }
    auto OldField() const { return this->template Field<TOldFieldTag>(); }

    auto Numbers() { return this->template Array<TArrayFieldTag>(); }
    auto Numbers() const { return this->template Array<TArrayFieldTag>(); }
    size_t NumbersSize() const { return this->template GetSize<TArrayFieldTag>(); }
};

struct TEvFlatFixedFields;
struct TEvFlatFixedFieldsScheme {
    using TSmallTag = TFlatEventDefs::FixedField<ui16, 0>;
    using TTabletIdTag = TFlatEventDefs::FixedField<i64, 1>;
    using TPointTag = TFlatEventDefs::FixedField<TPoint, 2>;
    using TCookieTag = TFlatEventDefs::FixedField<ui32, 3>;
    using TSchemeV1 = TFlatEventDefs::Scheme<TSmallTag, TTabletIdTag, TPointTag, TCookieTag>;
    using TVersions = TFlatEventDefs::Versions<TSchemeV1>;
};

struct TEvFlatFixedFields : TEventFlat<TEvFlatFixedFields, TEvFlatFixedFieldsScheme> {
    using TBase = TEventFlat<TEvFlatFixedFields, TEvFlatFixedFieldsScheme>;

    static constexpr ui32 EventType = EvFlatFixedFields;

    using TScheme = TEvFlatFixedFieldsScheme;
    using TSmallTag = TScheme::TSmallTag;
    using TTabletIdTag = TScheme::TTabletIdTag;
    using TPointTag = TScheme::TPointTag;
    using TCookieTag = TScheme::TCookieTag;
    using TSchemeV1 = TScheme::TSchemeV1;

    friend class TEventFlat<TEvFlatFixedFields, TEvFlatFixedFieldsScheme>;

    static TEvFlatFixedFields* Make() {
        return TBase::MakeEvent();
    }

    auto Small() { return this->template Field<TSmallTag>(); }
    auto Small() const { return this->template Field<TSmallTag>(); }

    auto TabletId() { return this->template Field<TTabletIdTag>(); }
    auto TabletId() const { return this->template Field<TTabletIdTag>(); }

    auto Point() { return this->template Field<TPointTag>(); }
    auto Point() const { return this->template Field<TPointTag>(); }

    auto Cookie() { return this->template Field<TCookieTag>(); }
    auto Cookie() const { return this->template Field<TCookieTag>(); }
};

struct TEvFlatRepeatedFields;
struct TEvFlatRepeatedFieldsScheme {
    using TMarkerTag = TFlatEventDefs::FixedField<ui32, 0>;
    using TPointTag = TFlatEventDefs::FixedField<TPoint, 1>;
    using TNumbersTag = TFlatEventDefs::ArrayField<ui32, 2>;
    using TItemsTag = TFlatEventDefs::ArrayField<TRepeatedItem, 3>;
    using TEmptyItemsTag = TFlatEventDefs::ArrayField<TRepeatedItem, 4>;
    using TSchemeV1 = TFlatEventDefs::Scheme<TFlatEventDefs::WithPayloadType<ui8>,
        TMarkerTag, TPointTag, TNumbersTag, TItemsTag, TEmptyItemsTag>;
    using TVersions = TFlatEventDefs::Versions<TSchemeV1>;
};

struct TEvFlatRepeatedFields : TEventFlat<TEvFlatRepeatedFields, TEvFlatRepeatedFieldsScheme> {
    using TBase = TEventFlat<TEvFlatRepeatedFields, TEvFlatRepeatedFieldsScheme>;

    static constexpr ui32 EventType = EvFlatRepeatedFields;

    using TScheme = TEvFlatRepeatedFieldsScheme;
    using TMarkerTag = TScheme::TMarkerTag;
    using TPointTag = TScheme::TPointTag;
    using TNumbersTag = TScheme::TNumbersTag;
    using TItemsTag = TScheme::TItemsTag;
    using TEmptyItemsTag = TScheme::TEmptyItemsTag;
    using TSchemeV1 = TScheme::TSchemeV1;

    friend class TEventFlat<TEvFlatRepeatedFields, TEvFlatRepeatedFieldsScheme>;

    static TEvFlatRepeatedFields* Make() {
        return TBase::MakeEvent();
    }

    auto Marker() { return this->template Field<TMarkerTag>(); }
    auto Marker() const { return this->template Field<TMarkerTag>(); }

    auto Point() { return this->template Field<TPointTag>(); }
    auto Point() const { return this->template Field<TPointTag>(); }

    auto Numbers() { return this->template Array<TNumbersTag>(); }
    auto Numbers() const { return this->template Array<TNumbersTag>(); }
    size_t NumbersSize() const { return this->template GetSize<TNumbersTag>(); }

    auto Items() { return this->template Array<TItemsTag>(); }
    auto Items() const { return this->template Array<TItemsTag>(); }
    size_t ItemsSize() const { return this->template GetSize<TItemsTag>(); }

    auto EmptyItems() { return this->template Array<TEmptyItemsTag>(); }
    auto EmptyItems() const { return this->template Array<TEmptyItemsTag>(); }
    size_t EmptyItemsSize() const { return this->template GetSize<TEmptyItemsTag>(); }
};

struct TEvFlatPayloadFields;
struct TEvFlatPayloadFieldsScheme {
    using TMarkerTag = TFlatEventDefs::FixedField<ui32, 0>;
    using TBlobTag = TFlatEventDefs::BytesField<1>;
    using TNumbersTag = TFlatEventDefs::ArrayField<ui32, 2>;
    using TSchemeV1 = TFlatEventDefs::Scheme<TFlatEventDefs::WithPayloadType<ui8>,
        TMarkerTag, TBlobTag, TNumbersTag>;
    using TVersions = TFlatEventDefs::Versions<TSchemeV1>;
};

struct TEvFlatPayloadFields : TEventFlat<TEvFlatPayloadFields, TEvFlatPayloadFieldsScheme> {
    using TBase = TEventFlat<TEvFlatPayloadFields, TEvFlatPayloadFieldsScheme>;

    static constexpr ui32 EventType = EvFlatPayloadFields;

    using TScheme = TEvFlatPayloadFieldsScheme;
    using TMarkerTag = TScheme::TMarkerTag;
    using TBlobTag = TScheme::TBlobTag;
    using TNumbersTag = TScheme::TNumbersTag;
    using TSchemeV1 = TScheme::TSchemeV1;

    friend class TEventFlat<TEvFlatPayloadFields, TEvFlatPayloadFieldsScheme>;

    static TEvFlatPayloadFields* Make() {
        return TBase::MakeEvent();
    }

    auto Marker() { return this->template Field<TMarkerTag>(); }
    auto Marker() const { return this->template Field<TMarkerTag>(); }

    auto Blob() { return this->template Bytes<TBlobTag>(); }
    auto Blob() const { return this->template Bytes<TBlobTag>(); }
    size_t BlobSize() const { return this->template GetSize<TBlobTag>(); }

    auto Numbers() { return this->template Array<TNumbersTag>(); }
    auto Numbers() const { return this->template Array<TNumbersTag>(); }
    size_t NumbersSize() const { return this->template GetSize<TNumbersTag>(); }
};

struct TEvFlatInlineFields;
struct TEvFlatInlineFieldsScheme {
    using TMarkerTag = TFlatEventDefs::FixedField<ui32, 0>;
    using TBlobTag = TFlatEventDefs::InlineBytesField<16, 1>;
    using TNumbersTag = TFlatEventDefs::InlineArrayField<ui32, 4, 2>;
    using TSchemeV1 = TFlatEventDefs::Scheme<TMarkerTag, TBlobTag, TNumbersTag>;
    using TVersions = TFlatEventDefs::Versions<TSchemeV1>;
};

struct TEvFlatInlineFields : TEventFlat<TEvFlatInlineFields, TEvFlatInlineFieldsScheme> {
    using TBase = TEventFlat<TEvFlatInlineFields, TEvFlatInlineFieldsScheme>;

    static constexpr ui32 EventType = EvFlatInlineFields;

    using TScheme = TEvFlatInlineFieldsScheme;
    using TMarkerTag = TScheme::TMarkerTag;
    using TBlobTag = TScheme::TBlobTag;
    using TNumbersTag = TScheme::TNumbersTag;
    using TSchemeV1 = TScheme::TSchemeV1;

    friend class TEventFlat<TEvFlatInlineFields, TEvFlatInlineFieldsScheme>;

    static TEvFlatInlineFields* Make() {
        return TBase::MakeEvent();
    }

    auto Marker() { return this->template Field<TMarkerTag>(); }
    auto Marker() const { return this->template Field<TMarkerTag>(); }

    auto Blob() { return this->template Bytes<TBlobTag>(); }
    auto Blob() const { return this->template Bytes<TBlobTag>(); }
    size_t BlobSize() const { return this->template GetSize<TBlobTag>(); }

    auto Numbers() { return this->template Array<TNumbersTag>(); }
    auto Numbers() const { return this->template Array<TNumbersTag>(); }
    size_t NumbersSize() const { return this->template GetSize<TNumbersTag>(); }
};

struct TEvFlatStrictArrayFields;
struct TEvFlatStrictArrayFieldsScheme {
    using TMarkerTag = TFlatEventDefs::FixedField<ui32, 0>;
    using TNumbersTag = TFlatEventDefs::ArrayField<ui32, 1>;
    using TSchemeV1 = TFlatEventDefs::Scheme<TFlatEventDefs::WithStrictScheme,
        TFlatEventDefs::WithPayloadType<ui8>, TMarkerTag, TNumbersTag>;
    using TVersions = TFlatEventDefs::Versions<TSchemeV1>;
};

struct TEvFlatStrictArrayFields : TEventFlat<TEvFlatStrictArrayFields, TEvFlatStrictArrayFieldsScheme> {
    using TBase = TEventFlat<TEvFlatStrictArrayFields, TEvFlatStrictArrayFieldsScheme>;

    static constexpr ui32 EventType = EvFlatStrictArrayFields;

    using TScheme = TEvFlatStrictArrayFieldsScheme;
    using TMarkerTag = TScheme::TMarkerTag;
    using TNumbersTag = TScheme::TNumbersTag;
    using TSchemeV1 = TScheme::TSchemeV1;

    friend class TEventFlat<TEvFlatStrictArrayFields, TEvFlatStrictArrayFieldsScheme>;

    static TEvFlatStrictArrayFields* Make() {
        return TBase::MakeEvent();
    }

    auto Marker() { return this->template Field<TMarkerTag>(); }
    auto Marker() const { return this->template Field<TMarkerTag>(); }

    auto Numbers() { return this->template Array<TNumbersTag>(); }
    auto Numbers() const { return this->template Array<TNumbersTag>(); }
    size_t NumbersSize() const { return this->template GetSize<TNumbersTag>(); }
};

struct TEvFlatStrictBytesFields;
struct TEvFlatStrictBytesFieldsScheme {
    using TMarkerTag = TFlatEventDefs::FixedField<ui32, 0>;
    using TBlobTag = TFlatEventDefs::BytesField<1>;
    using TSchemeV1 = TFlatEventDefs::Scheme<TFlatEventDefs::WithStrictScheme,
        TFlatEventDefs::WithPayloadType<ui8>, TMarkerTag, TBlobTag>;
    using TVersions = TFlatEventDefs::Versions<TSchemeV1>;
};

struct TEvFlatStrictBytesFields : TEventFlat<TEvFlatStrictBytesFields, TEvFlatStrictBytesFieldsScheme> {
    using TBase = TEventFlat<TEvFlatStrictBytesFields, TEvFlatStrictBytesFieldsScheme>;

    static constexpr ui32 EventType = EvFlatStrictBytesFields;

    using TScheme = TEvFlatStrictBytesFieldsScheme;
    using TMarkerTag = TScheme::TMarkerTag;
    using TBlobTag = TScheme::TBlobTag;
    using TSchemeV1 = TScheme::TSchemeV1;

    friend class TEventFlat<TEvFlatStrictBytesFields, TEvFlatStrictBytesFieldsScheme>;

    static TEvFlatStrictBytesFields* Make() {
        return TBase::MakeEvent();
    }

    auto Marker() { return this->template Field<TMarkerTag>(); }
    auto Marker() const { return this->template Field<TMarkerTag>(); }

    auto Blob() { return this->template Bytes<TBlobTag>(); }
    auto Blob() const { return this->template Bytes<TBlobTag>(); }
    size_t BlobSize() const { return this->template GetSize<TBlobTag>(); }
};

namespace {
    size_t SerializeNumberTo(char* dst, size_t number) {
        size_t pos = 0;
        do {
            dst[pos++] = static_cast<char>((number & 0x7F) | (number >= 128 ? 0x80 : 0x00));
            number >>= 7;
        } while (number);
        return pos;
    }

    TIntrusivePtr<TEventSerializedData> MakeSerializedData(TVector<TRope> payloads, TString header) {
        const ui32 framingSize = CalculateSerializedHeaderSizeImpl(payloads);
        const size_t headerSize = header.size();
        TString buffer = TString::Uninitialized(framingSize);
        char* ptr = buffer.Detach();

        if (framingSize) {
            *ptr++ = ExtendedPayloadMarker;
            ptr += SerializeNumberTo(ptr, payloads.size());
            for (const TRope& rope : payloads) {
                ptr += SerializeNumberTo(ptr, rope.GetSize());
            }
        }

        TRope rope(std::move(buffer));
        for (TRope& payload : payloads) {
            rope.Insert(rope.End(), std::move(payload));
        }
        rope.Insert(rope.End(), TRope(std::move(header)));

        return MakeIntrusive<TEventSerializedData>(std::move(rope), CreateSerializationInfoImpl(0, false, payloads, headerSize));
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
        using TScheme = TEvFlatMessage::TSchemeV1;
        TString header = TString::Uninitialized(TScheme::HeaderSize);
        char* ptr = header.Detach();
        std::memset(ptr, 0, TScheme::HeaderSize);

        WriteUnaligned<ui8>(ptr + 0, 1);
        WriteUnaligned<i64>(ptr + TScheme::template GetFixedOffset<TEvFlatMessage::TTabletIdTag>(), tabletId);
        WriteUnaligned<i64>(ptr + TScheme::template GetFixedOffset<TEvFlatMessage::TOldFieldTag>(), oldField);
        WriteUnaligned<typename TScheme::TPayloadRef>(
            ptr + TScheme::template GetPayloadRefOffset<TEvFlatMessage::TArrayFieldTag>(),
            typename TScheme::TPayloadRef{
                .PayloadId = static_cast<ui8>(values.empty() ? 0u : 1u),
            });

        TVector<TRope> payloads;
        payloads.push_back(TRope(MakeArrayPayload(values)));
        return MakeSerializedData(std::move(payloads), std::move(header));
    }

} // namespace

Y_UNIT_TEST_SUITE(TEventFlatTest) {
    Y_UNIT_TEST(EmptyEventRoundTrip) {
        THolder<TEvFlatEmpty> ev(TEvFlatEmpty::Make());

        UNIT_ASSERT_VALUES_EQUAL(ev->GetVersion(), static_cast<ui8>(0));
        UNIT_ASSERT(!ev->HasData());
        UNIT_ASSERT_VALUES_EQUAL(ev->GetSerializedSize(), 0);
        UNIT_ASSERT_VALUES_EQUAL(ev->CalculateSerializedSize(), 0);

        auto serializer = MakeHolder<TAllocChunkSerializer>();
        UNIT_ASSERT(ev->SerializeToArcadiaStream(serializer.Get()));
        auto buffers = serializer->Release(ev->CreateSerializationInfo(false));
        UNIT_ASSERT_VALUES_EQUAL(buffers->GetSize(), 0);

        THolder<IEventHandle> handle(new IEventHandle(
            EvFlatEmpty, 0, TActorId(), TActorId(), buffers, 0));

        TEvFlatEmpty* loaded = handle->Get<TEvFlatEmpty>();
        UNIT_ASSERT_VALUES_EQUAL(loaded->GetVersion(), static_cast<ui8>(0));
        UNIT_ASSERT(!loaded->HasData());
        UNIT_ASSERT_VALUES_EQUAL(loaded->GetSerializedSize(), 0);
    }

    Y_UNIT_TEST(EmptyVersionCanBeKeptAfterAddingDataVersion) {
        THolder<TEvFlatOptionalEmpty> oldEv(TEvFlatOptionalEmpty::MakeNoData());

        UNIT_ASSERT_VALUES_EQUAL(oldEv->GetVersion(), static_cast<ui8>(0));
        UNIT_ASSERT(!oldEv->HasData());
        UNIT_ASSERT(!oldEv->HasField<TEvFlatOptionalEmpty::TMarkerTag>());

        auto oldSerializer = MakeHolder<TAllocChunkSerializer>();
        UNIT_ASSERT(oldEv->SerializeToArcadiaStream(oldSerializer.Get()));
        auto oldBuffers = oldSerializer->Release(oldEv->CreateSerializationInfo(false));
        UNIT_ASSERT_VALUES_EQUAL(oldBuffers->GetSize(), 0);

        THolder<IEventHandle> oldHandle(new IEventHandle(
            EvFlatOptionalEmpty, 0, TActorId(), TActorId(), oldBuffers, 0));

        TEvFlatOptionalEmpty* loadedOld = oldHandle->Get<TEvFlatOptionalEmpty>();
        UNIT_ASSERT_VALUES_EQUAL(loadedOld->GetVersion(), static_cast<ui8>(0));
        UNIT_ASSERT(!loadedOld->HasData());
        UNIT_ASSERT(!loadedOld->HasField<TEvFlatOptionalEmpty::TMarkerTag>());

        THolder<TEvFlatOptionalEmpty> newEv(TEvFlatOptionalEmpty::Make());
        UNIT_ASSERT_VALUES_EQUAL(newEv->GetVersion(), static_cast<ui8>(1));
        UNIT_ASSERT(newEv->HasData());
        newEv->Marker() = 42;

        auto newSerializer = MakeHolder<TAllocChunkSerializer>();
        UNIT_ASSERT(newEv->SerializeToArcadiaStream(newSerializer.Get()));
        auto newBuffers = newSerializer->Release(newEv->CreateSerializationInfo(false));
        UNIT_ASSERT_VALUES_EQUAL(newBuffers->GetSize(), TEvFlatOptionalEmpty::TSchemeV1::HeaderSize);

        THolder<IEventHandle> newHandle(new IEventHandle(
            EvFlatOptionalEmpty, 0, TActorId(), TActorId(), newBuffers, 0));

        TEvFlatOptionalEmpty* loadedNew = newHandle->Get<TEvFlatOptionalEmpty>();
        UNIT_ASSERT_VALUES_EQUAL(loadedNew->GetVersion(), static_cast<ui8>(1));
        UNIT_ASSERT(loadedNew->HasData());
        UNIT_ASSERT_VALUES_EQUAL(static_cast<ui32>(loadedNew->Marker()), 42);
    }

    Y_UNIT_TEST(FixedFieldsWithTrivialStructRoundTrip) {
        THolder<TEvFlatFixedFields> ev(TEvFlatFixedFields::Make());
        ev->Small() = 17;
        ev->TabletId() = 123456789;
        ev->Point() = TPoint{11, 22};
        ev->Cookie() = 77;

        UNIT_ASSERT_VALUES_EQUAL(ev->GetVersion(), static_cast<ui8>(1));
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
        UNIT_ASSERT_VALUES_EQUAL(loaded->GetVersion(), static_cast<ui8>(1));
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
        std::memcpy(ev->Numbers().Init(std::size(numbers)), numbers, sizeof(numbers));
        auto* items = ev->Items().Init(2);
        items[0] = item1;
        items[1] = item2;

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
        UNIT_ASSERT_VALUES_EQUAL(loaded->GetVersion(), static_cast<ui8>(1));
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

        ui32 allValues[] = {1, 2, 3, 4, 0};
        std::memcpy(ev->Numbers().Init(std::size(allValues)), allValues, sizeof(allValues));
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
        UNIT_ASSERT_VALUES_EQUAL(loaded->GetVersion(), static_cast<ui8>(1));
        UNIT_ASSERT_VALUES_EQUAL(static_cast<ui32>(loaded->Marker()), 7);
        UNIT_ASSERT_VALUES_EQUAL(loaded->Blob().Materialize(), "abcd");
        UNIT_ASSERT_VALUES_EQUAL(loaded->NumbersSize(), 5);
        UNIT_ASSERT_VALUES_EQUAL(static_cast<ui32>(loaded->Numbers()[0]), 1);
        UNIT_ASSERT_VALUES_EQUAL(static_cast<ui32>(loaded->Numbers()[1]), 22);
        UNIT_ASSERT_VALUES_EQUAL(static_cast<ui32>(loaded->Numbers()[2]), 3);
        UNIT_ASSERT_VALUES_EQUAL(static_cast<ui32>(loaded->Numbers()[3]), 4);
        UNIT_ASSERT_VALUES_EQUAL(static_cast<ui32>(loaded->Numbers()[4]), 55);
    }

    Y_UNIT_TEST(InlineBytesAndInlineArrayFields) {
        THolder<TEvFlatInlineFields> ev(TEvFlatInlineFields::Make());
        ev->Marker() = 101;
        ev->Blob().Append(TRope(TString("mini")));

        ui32 values[] = {4, 8, 15, 16};
        ev->Numbers().CopyFrom(values, std::size(values));

        UNIT_ASSERT_VALUES_EQUAL(static_cast<ui32>(ev->Marker()), 101);
        UNIT_ASSERT_VALUES_EQUAL(ev->BlobSize(), 4u);
        UNIT_ASSERT_VALUES_EQUAL(ev->Blob().Materialize(), "mini");
        UNIT_ASSERT_VALUES_EQUAL(ev->NumbersSize(), 4u);
        UNIT_ASSERT_VALUES_EQUAL(static_cast<ui32>(ev->Numbers()[0]), 4u);
        UNIT_ASSERT_VALUES_EQUAL(static_cast<ui32>(ev->Numbers()[3]), 16u);

        auto serializer = MakeHolder<TAllocChunkSerializer>();
        UNIT_ASSERT(ev->SerializeToArcadiaStream(serializer.Get()));
        auto buffers = serializer->Release(ev->CreateSerializationInfo(false));

        THolder<IEventHandle> handle(new IEventHandle(
            EvFlatInlineFields, 0, TActorId(), TActorId(), buffers, 0));

        TEvFlatInlineFields* loaded = handle->Get<TEvFlatInlineFields>();
        UNIT_ASSERT_VALUES_EQUAL(static_cast<ui32>(loaded->Marker()), 101);
        UNIT_ASSERT_VALUES_EQUAL(loaded->BlobSize(), 4u);
        UNIT_ASSERT_VALUES_EQUAL(loaded->Blob().Materialize(), "mini");
        UNIT_ASSERT_VALUES_EQUAL(loaded->NumbersSize(), 4u);
        UNIT_ASSERT_VALUES_EQUAL(static_cast<ui32>(loaded->Numbers()[1]), 8u);
        UNIT_ASSERT_VALUES_EQUAL(static_cast<ui32>(loaded->Numbers()[2]), 15u);
        UNIT_ASSERT(loaded->CreateSerializationInfo(true).Sections.empty());
    }

    Y_UNIT_TEST(FrontendForSingleVersionPayloadScheme) {
        THolder<TEvFlatPayloadFields> ev(TEvFlatPayloadFields::Make());
        UNIT_ASSERT(ev->IsVersion<TEvFlatPayloadFields::TSchemeV1>());

        auto frontend = ev->GetFrontend<TEvFlatPayloadFields::TSchemeV1>();
        frontend.template Field<TEvFlatPayloadFields::TMarkerTag>() = 31;
        frontend.template Bytes<TEvFlatPayloadFields::TBlobTag>().Append(TRope(TString("xy")));

        ui32 values[] = {11, 22, 33};
        auto numbers = frontend.template Array<TEvFlatPayloadFields::TNumbersTag>();
        std::memcpy(numbers.Init(std::size(values)), values, sizeof(values));

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
        std::memcpy(ev->Numbers().Init(std::size(values)), values, sizeof(values));

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
        UNIT_ASSERT_VALUES_EQUAL(loaded->GetVersion(), static_cast<ui8>(1));
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
        std::memcpy(ev->Numbers().Init(std::size(values)), values, sizeof(values));

        UNIT_ASSERT_VALUES_EQUAL(ev->GetVersion(), static_cast<ui8>(2));
        UNIT_ASSERT(!ev->HasOldField());
        UNIT_ASSERT_VALUES_EQUAL(ev->NumbersSize(), 3);

        auto serializer = MakeHolder<TAllocChunkSerializer>();
        UNIT_ASSERT(ev->SerializeToArcadiaStream(serializer.Get()));
        auto buffers = serializer->Release(ev->CreateSerializationInfo(false));

        THolder<IEventHandle> handle(new IEventHandle(
            EvFlatMessage, 0, TActorId(), TActorId(), buffers, 0));

        TEvFlatMessage* loaded = handle->Get<TEvFlatMessage>();
        UNIT_ASSERT_VALUES_EQUAL(loaded->GetVersion(), static_cast<ui8>(2));
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
        UNIT_ASSERT_VALUES_EQUAL(loaded->GetVersion(), static_cast<ui8>(1));
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
        UNIT_ASSERT_VALUES_EQUAL(loaded->GetVersion(), static_cast<ui8>(1));
        UNIT_ASSERT(loaded->HasOldField());
        UNIT_ASSERT_VALUES_EQUAL(static_cast<i64>(loaded->TabletId()), 101);
        UNIT_ASSERT_VALUES_EQUAL(static_cast<i64>(loaded->OldField()), 202);
        UNIT_ASSERT(loaded->Numbers().empty());
        UNIT_ASSERT_VALUES_EQUAL(loaded->NumbersSize(), 0);
    }

    Y_UNIT_TEST(VersionedFrontendsForLatestAndOldSchemes) {
        THolder<TEvFlatMessage> ev(TEvFlatMessage::Make());
        UNIT_ASSERT(ev->IsVersion<TEvFlatMessage::TSchemeV2>());
        UNIT_ASSERT(ev->Is<TEvFlatMessage::TSchemeV2>());
        UNIT_ASSERT(!ev->IsVersion<TEvFlatMessage::TSchemeV1>());
        UNIT_ASSERT(!ev->Is<TEvFlatMessage::TSchemeV1>());

        auto v2 = ev->GetFrontend<TEvFlatMessage::TSchemeV2>();
        v2.template Field<TEvFlatMessage::TTabletIdTag>() = 404;
        ui32 values[] = {9, 8, 7};
        auto array = v2.template Array<TEvFlatMessage::TArrayFieldTag>();
        std::memcpy(array.Init(std::size(values)), values, sizeof(values));

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

    Y_UNIT_TEST(HasFieldUsesActualHeaderSizeWithinVersion) {
        TString header = TString::Uninitialized(sizeof(ui8) + sizeof(i64));
        char* ptr = header.Detach();
        std::memset(ptr, 0, header.size());
        WriteUnaligned<ui8>(ptr + 0, 2);
        WriteUnaligned<i64>(ptr + sizeof(ui8), 909);

        auto buffers = MakeSerializedData(TVector<TRope>{}, std::move(header));
        THolder<IEventHandle> handle(new IEventHandle(
            EvFlatMessage, 0, TActorId(), TActorId(), buffers, 0));

        TEvFlatMessage* loaded = handle->Get<TEvFlatMessage>();
        UNIT_ASSERT(loaded->Is<TEvFlatMessage::TSchemeV2>());
        UNIT_ASSERT(loaded->HasField<TEvFlatMessage::TTabletIdTag>());
        UNIT_ASSERT(!loaded->HasField<TEvFlatMessage::TArrayFieldTag>());
        UNIT_ASSERT_VALUES_EQUAL(static_cast<i64>(loaded->TabletId()), 909);
    }

    Y_UNIT_TEST(ExtendedPayloadsMapOnlyStoredPayloadFields) {
        using TScheme = TEvFlatRepeatedFields::TSchemeV1;

        constexpr size_t headerSize = TScheme::template GetPayloadRefOffset<TEvFlatRepeatedFields::TNumbersTag>()
            + sizeof(typename TScheme::TPayloadRef);

        TString header = TString::Uninitialized(headerSize);
        char* ptr = header.Detach();
        std::memset(ptr, 0, headerSize);
        WriteUnaligned<ui8>(ptr + 0, 1);
        WriteUnaligned<ui32>(ptr + TScheme::template GetFixedOffset<TEvFlatRepeatedFields::TMarkerTag>(), 123);
        WriteUnaligned<TPoint>(
            ptr + TScheme::template GetFixedOffset<TEvFlatRepeatedFields::TPointTag>(),
            TPoint{.X = 7, .Y = 8});
        WriteUnaligned<typename TScheme::TPayloadRef>(
            ptr + TScheme::template GetPayloadRefOffset<TEvFlatRepeatedFields::TNumbersTag>(),
            typename TScheme::TPayloadRef{.PayloadId = 1});

        const TVector<ui32> values = {100, 200, 300};
        auto buffers = MakeSerializedData({TRope(MakeArrayPayload(values))}, std::move(header));
        THolder<IEventHandle> handle(new IEventHandle(
            EvFlatRepeatedFields, 0, TActorId(), TActorId(), buffers, 0));

        TEvFlatRepeatedFields* loaded = handle->Get<TEvFlatRepeatedFields>();
        UNIT_ASSERT_VALUES_EQUAL(static_cast<ui32>(loaded->Marker()), 123u);
        UNIT_ASSERT_VALUES_EQUAL(loaded->NumbersSize(), values.size());
        UNIT_ASSERT_VALUES_EQUAL(static_cast<ui32>(loaded->Numbers()[0]), 100u);
        UNIT_ASSERT_VALUES_EQUAL(static_cast<ui32>(loaded->Numbers()[2]), 300u);
        UNIT_ASSERT(!loaded->HasField<TEvFlatRepeatedFields::TItemsTag>());
        UNIT_ASSERT(!loaded->HasField<TEvFlatRepeatedFields::TEmptyItemsTag>());
    }

    Y_UNIT_TEST(ArrayPayloadPreservesAlignmentLocally) {
        using TScheme = TEvFlatMessage::TSchemeV2;

        THolder<TEvFlatMessage> ev(TEvFlatMessage::Make());
        ev->TabletId() = 11;

        ui32 values[] = {10, 20, 30, 40};
        std::memcpy(ev->Numbers().Init(std::size(values)), values, sizeof(values));

        const ui32* localData = ev->ArrayData<TEvFlatMessage::TArrayFieldTag>();
        UNIT_ASSERT_VALUES_EQUAL(reinterpret_cast<uintptr_t>(localData) % alignof(ui32), 0u);

        char varint[MaxNumberBytes];
        const size_t payloadBytes = sizeof(values);
        const size_t expectedWireSize = TScheme::HeaderSize
            + SerializeNumberTo(varint, TScheme::HeaderSize)
            + SerializeNumberTo(varint, payloadBytes)
            + payloadBytes;
        UNIT_ASSERT_VALUES_EQUAL(ev->CalculateSerializedSize(), expectedWireSize);
        UNIT_ASSERT(ev->CreateSerializationInfo(true).Sections.empty());

        auto rope = ev->SerializeToRope(GetDefaultRcBufAllocator());
        UNIT_ASSERT(rope);
        UNIT_ASSERT_VALUES_EQUAL(rope->GetSize(), expectedWireSize);

        auto buffers = MakeIntrusive<TEventSerializedData>(std::move(*rope), TEventSerializationInfo{});
        THolder<IEventHandle> handle(new IEventHandle(
            EvFlatMessage, 0, TActorId(), TActorId(), buffers, 0));

        TEvFlatMessage* loaded = handle->Get<TEvFlatMessage>();
        UNIT_ASSERT_VALUES_EQUAL(static_cast<i64>(loaded->TabletId()), 11);
        UNIT_ASSERT_VALUES_EQUAL(loaded->NumbersSize(), std::size(values));
        const ui32* loadedData = loaded->ArrayData<TEvFlatMessage::TArrayFieldTag>();
        UNIT_ASSERT_VALUES_EQUAL(reinterpret_cast<uintptr_t>(loadedData) % alignof(ui32), 0u);
        UNIT_ASSERT_VALUES_EQUAL(loadedData[0], 10u);
        UNIT_ASSERT_VALUES_EQUAL(loadedData[3], 40u);

        auto serializer = MakeHolder<TAllocChunkSerializer>();
        UNIT_ASSERT(ev->SerializeToArcadiaStream(serializer.Get()));
        auto arcadiaBuffers = serializer->Release(ev->CreateSerializationInfo(false));
        UNIT_ASSERT_VALUES_EQUAL(arcadiaBuffers->GetSize(), expectedWireSize);

        THolder<IEventHandle> arcadiaHandle(new IEventHandle(
            EvFlatMessage, 0, TActorId(), TActorId(), arcadiaBuffers, 0));

        TEvFlatMessage* arcadiaLoaded = arcadiaHandle->Get<TEvFlatMessage>();
        UNIT_ASSERT_VALUES_EQUAL(static_cast<i64>(arcadiaLoaded->TabletId()), 11);
        UNIT_ASSERT_VALUES_EQUAL(arcadiaLoaded->NumbersSize(), std::size(values));
        const ui32* arcadiaData = arcadiaLoaded->ArrayData<TEvFlatMessage::TArrayFieldTag>();
        UNIT_ASSERT_VALUES_EQUAL(reinterpret_cast<uintptr_t>(arcadiaData) % alignof(ui32), 0u);
        UNIT_ASSERT_VALUES_EQUAL(arcadiaData[0], 10u);
        UNIT_ASSERT_VALUES_EQUAL(arcadiaData[3], 40u);
    }

    Y_UNIT_TEST(InlineArrayWireUsesActualHeaderSizePrefix) {
        using TScheme = TEvFlatRepeatedFields::TSchemeV1;

        constexpr size_t headerSize = TScheme::template GetPayloadRefOffset<TEvFlatRepeatedFields::TNumbersTag>()
            + sizeof(typename TScheme::TPayloadRef);

        TString header = TString::Uninitialized(headerSize);
        char* ptr = header.Detach();
        std::memset(ptr, 0, headerSize);
        WriteUnaligned<ui8>(ptr + 0, 1);
        WriteUnaligned<ui32>(ptr + TScheme::template GetFixedOffset<TEvFlatRepeatedFields::TMarkerTag>(), 123);
        WriteUnaligned<TPoint>(
            ptr + TScheme::template GetFixedOffset<TEvFlatRepeatedFields::TPointTag>(),
            TPoint{.X = 7, .Y = 8});

        ui32 values[] = {100, 200, 300};
        char varint[MaxNumberBytes];
        TString buffer = TString::Uninitialized(sizeof(ui8)
            + SerializeNumberTo(varint, headerSize)
            + headerSize - sizeof(ui8)
            + SerializeNumberTo(varint, sizeof(values))
            + sizeof(values));
        ptr = buffer.Detach();
        WriteUnaligned<ui8>(ptr, 1);
        ptr += sizeof(ui8);
        ptr += SerializeNumberTo(ptr, headerSize);
        std::memcpy(ptr, header.data() + sizeof(ui8), headerSize - sizeof(ui8));
        ptr += headerSize - sizeof(ui8);
        ptr += SerializeNumberTo(ptr, sizeof(values));
        std::memcpy(ptr, values, sizeof(values));

        auto buffers = MakeIntrusive<TEventSerializedData>(TRope(std::move(buffer)), TEventSerializationInfo{});
        THolder<IEventHandle> handle(new IEventHandle(
            EvFlatRepeatedFields, 0, TActorId(), TActorId(), buffers, 0));

        TEvFlatRepeatedFields* loaded = handle->Get<TEvFlatRepeatedFields>();
        UNIT_ASSERT_VALUES_EQUAL(static_cast<ui32>(loaded->Marker()), 123u);
        const TPoint loadedPoint = loaded->Point();
        UNIT_ASSERT_VALUES_EQUAL(loadedPoint.X, 7u);
        UNIT_ASSERT_VALUES_EQUAL(loadedPoint.Y, 8u);
        UNIT_ASSERT_VALUES_EQUAL(loaded->NumbersSize(), std::size(values));
        UNIT_ASSERT_VALUES_EQUAL(static_cast<ui32>(loaded->Numbers()[0]), 100u);
        UNIT_ASSERT_VALUES_EQUAL(static_cast<ui32>(loaded->Numbers()[2]), 300u);
        UNIT_ASSERT(!loaded->HasField<TEvFlatRepeatedFields::TItemsTag>());
        UNIT_ASSERT(!loaded->HasField<TEvFlatRepeatedFields::TEmptyItemsTag>());
    }

    Y_UNIT_TEST(StrictInlineArrayWireOmitsHeaderSizePrefix) {
        using TScheme = TEvFlatStrictArrayFields::TSchemeV1;

        THolder<TEvFlatStrictArrayFields> ev(TEvFlatStrictArrayFields::Make());
        ev->Marker() = 313;

        ui32 values[] = {21, 34, 55};
        std::memcpy(ev->Numbers().Init(std::size(values)), values, sizeof(values));

        char varint[MaxNumberBytes];
        const size_t payloadBytes = sizeof(values);
        const size_t expectedWireSize = TScheme::HeaderSize
            + SerializeNumberTo(varint, payloadBytes)
            + payloadBytes;
        UNIT_ASSERT_VALUES_EQUAL(ev->CalculateSerializedSize(), expectedWireSize);

        auto serializer = MakeHolder<TAllocChunkSerializer>();
        UNIT_ASSERT(ev->SerializeToArcadiaStream(serializer.Get()));
        auto buffers = serializer->Release(ev->CreateSerializationInfo(false));
        UNIT_ASSERT_VALUES_EQUAL(buffers->GetSize(), expectedWireSize);

        THolder<IEventHandle> handle(new IEventHandle(
            EvFlatStrictArrayFields, 0, TActorId(), TActorId(), buffers, 0));

        TEvFlatStrictArrayFields* loaded = handle->Get<TEvFlatStrictArrayFields>();
        UNIT_ASSERT_VALUES_EQUAL(static_cast<ui32>(loaded->Marker()), 313u);
        UNIT_ASSERT_VALUES_EQUAL(loaded->NumbersSize(), std::size(values));
        UNIT_ASSERT_VALUES_EQUAL(static_cast<ui32>(loaded->Numbers()[0]), 21u);
        UNIT_ASSERT_VALUES_EQUAL(static_cast<ui32>(loaded->Numbers()[2]), 55u);
    }

    Y_UNIT_TEST(BytesOnlyPayloadUsesDirectWirePayloads) {
        THolder<TEvFlatStrictBytesFields> ev(TEvFlatStrictBytesFields::Make());
        ev->Marker() = 515;
        ev->Blob().Append(TRope(TString("strict-bytes")));

        auto serializer = MakeHolder<TAllocChunkSerializer>();
        UNIT_ASSERT(ev->SerializeToArcadiaStream(serializer.Get()));
        auto buffers = serializer->Release(ev->CreateSerializationInfo(false));
        THolder<IEventHandle> handle(new IEventHandle(
            EvFlatStrictBytesFields, 0, TActorId(), TActorId(), buffers, 0));

        TEvFlatStrictBytesFields* loaded = handle->Get<TEvFlatStrictBytesFields>();
        UNIT_ASSERT_VALUES_EQUAL(static_cast<ui32>(loaded->Marker()), 515u);
        UNIT_ASSERT_VALUES_EQUAL(loaded->BlobSize(), TString("strict-bytes").size());
        UNIT_ASSERT_VALUES_EQUAL(loaded->Blob().Materialize(), "strict-bytes");
    }

    Y_UNIT_TEST(StrictBytesPayloadUsesExtendedFormat) {
        TString payload(4096, 'x');
        payload[0] = 'a';
        payload.back() = 'z';

        THolder<TEvFlatStrictBytesFields> ev(TEvFlatStrictBytesFields::Make());
        ev->Marker() = 616;
        ev->Blob().Append(TRope(payload));

        TEventSerializationInfo info = ev->CreateSerializationInfo(true);
        UNIT_ASSERT(info.IsExtendedFormat);

        auto serializer = MakeHolder<TAllocChunkSerializer>();
        UNIT_ASSERT(ev->SerializeToArcadiaStream(serializer.Get()));
        auto buffers = serializer->Release(std::move(info));

        THolder<IEventHandle> handle(new IEventHandle(
            EvFlatStrictBytesFields, 0, TActorId(), TActorId(), buffers, 0));

        TEvFlatStrictBytesFields* loaded = handle->Get<TEvFlatStrictBytesFields>();
        UNIT_ASSERT_VALUES_EQUAL(static_cast<ui32>(loaded->Marker()), 616u);
        UNIT_ASSERT_VALUES_EQUAL(loaded->BlobSize(), payload.size());
        UNIT_ASSERT_VALUES_EQUAL(loaded->Blob().Materialize(), payload);
    }

    Y_UNIT_TEST(ArrayPayloadAlignmentIsExposedInSerializationInfo) {
        THolder<TEvFlatMessage> ev(TEvFlatMessage::Make());
        ev->TabletId() = 42;

        TVector<ui32> values(4097, 7);
        std::memcpy(ev->Numbers().Init(values.size()), values.data(), values.size() * sizeof(ui32));

        const TEventSerializationInfo info = ev->CreateSerializationInfo(true);
        UNIT_ASSERT(info.IsExtendedFormat);
        UNIT_ASSERT_VALUES_EQUAL(info.Sections.size(), 3u);
        UNIT_ASSERT_VALUES_EQUAL(info.Sections[1].Alignment, alignof(ui32));
        UNIT_ASSERT_VALUES_EQUAL(info.Sections[1].Size, values.size() * sizeof(ui32));
    }

    Y_UNIT_TEST(LoadMisalignedArrayPayloadRealignsStorage) {
        using TScheme = TEvFlatMessage::TSchemeV2;

        constexpr size_t payloadBytes = 3 * sizeof(ui32);
        TRcBuf raw = TRcBuf::Uninitialized(payloadBytes + 1);
        char* rawData = raw.GetDataMut();
        std::memset(rawData, 0, payloadBytes + 1);
        WriteUnaligned<ui32>(rawData + 1, 100u);
        WriteUnaligned<ui32>(rawData + 1 + sizeof(ui32), 200u);
        WriteUnaligned<ui32>(rawData + 1 + 2 * sizeof(ui32), 300u);
        TRcBuf payloadBuf(TRcBuf::Piece, raw.GetData() + 1, payloadBytes, raw);

        TString header = TString::Uninitialized(TScheme::HeaderSize);
        char* ptr = header.Detach();
        std::memset(ptr, 0, TScheme::HeaderSize);
        WriteUnaligned<ui8>(ptr + 0, 2);
        WriteUnaligned<i64>(ptr + TScheme::template GetFixedOffset<TEvFlatMessage::TTabletIdTag>(), 501);
        WriteUnaligned<typename TScheme::TPayloadRef>(
            ptr + TScheme::template GetPayloadRefOffset<TEvFlatMessage::TArrayFieldTag>(),
            typename TScheme::TPayloadRef{.PayloadId = 1});

        auto buffers = MakeSerializedData({TRope(std::move(payloadBuf))}, std::move(header));
        THolder<IEventHandle> handle(new IEventHandle(
            EvFlatMessage, 0, TActorId(), TActorId(), buffers, 0));

        TEvFlatMessage* loaded = handle->Get<TEvFlatMessage>();
        UNIT_ASSERT_VALUES_EQUAL(static_cast<i64>(loaded->TabletId()), 501);
        UNIT_ASSERT_VALUES_EQUAL(loaded->NumbersSize(), 3u);
        UNIT_ASSERT_VALUES_EQUAL(static_cast<ui32>(loaded->Numbers()[0]), 100u);
        UNIT_ASSERT_VALUES_EQUAL(static_cast<ui32>(loaded->Numbers()[1]), 200u);
        UNIT_ASSERT_VALUES_EQUAL(static_cast<ui32>(loaded->Numbers()[2]), 300u);

        const ui32* payloadData = loaded->ArrayData<TEvFlatMessage::TArrayFieldTag>();
        UNIT_ASSERT_VALUES_EQUAL(reinterpret_cast<uintptr_t>(payloadData) % alignof(ui32), 0u);
    }
}
