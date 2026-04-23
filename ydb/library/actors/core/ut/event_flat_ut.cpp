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
    EvFlatAlignedPayloadFields,
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

struct TEvFlatMessage;
using TEvFlatMessageTTabletIdTag = TFlatEventDefs::FixedField<i64, 0>;
using TEvFlatMessageTOldFieldTag = TFlatEventDefs::FixedField<i64, 1>;
using TEvFlatMessageTArrayFieldTag = TFlatEventDefs::ArrayField<ui32, 2>;
using TEvFlatMessageTSchemeV1 = TFlatEventDefs::Scheme<TFlatEventDefs::WithPayloadType<ui8>,
    TEvFlatMessageTTabletIdTag, TEvFlatMessageTOldFieldTag, TEvFlatMessageTArrayFieldTag>;
using TEvFlatMessageTSchemeV2 = TFlatEventDefs::Scheme<TFlatEventDefs::WithPayloadType<ui8>,
    TEvFlatMessageTTabletIdTag, TEvFlatMessageTArrayFieldTag>;
using TEvFlatMessageTVersions = TFlatEventDefs::Versions<TEvFlatMessageTSchemeV1, TEvFlatMessageTSchemeV2>;

struct TEvFlatMessage : TEventFlat<TEvFlatMessage, TEvFlatMessageTVersions> {
    using TBase = TEventFlat<TEvFlatMessage, TEvFlatMessageTVersions>;

    static constexpr ui32 EventType = EvFlatMessage;

    using TTabletIdTag = TEvFlatMessageTTabletIdTag;
    using TOldFieldTag = TEvFlatMessageTOldFieldTag;
    using TArrayFieldTag = TEvFlatMessageTArrayFieldTag;
    using TSchemeV1 = TEvFlatMessageTSchemeV1;
    using TSchemeV2 = TEvFlatMessageTSchemeV2;
    using TScheme = TEvFlatMessageTVersions;

    friend class TEventFlat<TEvFlatMessage, TEvFlatMessageTVersions>;

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
using TEvFlatFixedFieldsTSmallTag = TFlatEventDefs::FixedField<ui16, 0>;
using TEvFlatFixedFieldsTTabletIdTag = TFlatEventDefs::FixedField<i64, 1>;
using TEvFlatFixedFieldsTPointTag = TFlatEventDefs::FixedField<TPoint, 2>;
using TEvFlatFixedFieldsTCookieTag = TFlatEventDefs::FixedField<ui32, 3>;
using TEvFlatFixedFieldsTSchemeV1 = TFlatEventDefs::Scheme<
    TEvFlatFixedFieldsTSmallTag, TEvFlatFixedFieldsTTabletIdTag, TEvFlatFixedFieldsTPointTag, TEvFlatFixedFieldsTCookieTag>;
using TEvFlatFixedFieldsTVersions = TFlatEventDefs::Versions<TEvFlatFixedFieldsTSchemeV1>;

struct TEvFlatFixedFields : TEventFlat<TEvFlatFixedFields, TEvFlatFixedFieldsTVersions> {
    using TBase = TEventFlat<TEvFlatFixedFields, TEvFlatFixedFieldsTVersions>;

    static constexpr ui32 EventType = EvFlatFixedFields;

    using TSmallTag = TEvFlatFixedFieldsTSmallTag;
    using TTabletIdTag = TEvFlatFixedFieldsTTabletIdTag;
    using TPointTag = TEvFlatFixedFieldsTPointTag;
    using TCookieTag = TEvFlatFixedFieldsTCookieTag;
    using TSchemeV1 = TEvFlatFixedFieldsTSchemeV1;
    using TScheme = TEvFlatFixedFieldsTVersions;

    friend class TEventFlat<TEvFlatFixedFields, TEvFlatFixedFieldsTVersions>;

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
using TEvFlatRepeatedFieldsTMarkerTag = TFlatEventDefs::FixedField<ui32, 0>;
using TEvFlatRepeatedFieldsTPointTag = TFlatEventDefs::FixedField<TPoint, 1>;
using TEvFlatRepeatedFieldsTNumbersTag = TFlatEventDefs::ArrayField<ui32, 2>;
using TEvFlatRepeatedFieldsTItemsTag = TFlatEventDefs::ArrayField<TRepeatedItem, 3>;
using TEvFlatRepeatedFieldsTEmptyItemsTag = TFlatEventDefs::ArrayField<TRepeatedItem, 4>;
using TEvFlatRepeatedFieldsTSchemeV1 = TFlatEventDefs::Scheme<TFlatEventDefs::WithPayloadType<ui8>,
    TEvFlatRepeatedFieldsTMarkerTag, TEvFlatRepeatedFieldsTPointTag, TEvFlatRepeatedFieldsTNumbersTag,
    TEvFlatRepeatedFieldsTItemsTag, TEvFlatRepeatedFieldsTEmptyItemsTag>;
using TEvFlatRepeatedFieldsTVersions = TFlatEventDefs::Versions<TEvFlatRepeatedFieldsTSchemeV1>;

struct TEvFlatRepeatedFields : TEventFlat<TEvFlatRepeatedFields, TEvFlatRepeatedFieldsTVersions> {
    using TBase = TEventFlat<TEvFlatRepeatedFields, TEvFlatRepeatedFieldsTVersions>;

    static constexpr ui32 EventType = EvFlatRepeatedFields;

    using TMarkerTag = TEvFlatRepeatedFieldsTMarkerTag;
    using TPointTag = TEvFlatRepeatedFieldsTPointTag;
    using TNumbersTag = TEvFlatRepeatedFieldsTNumbersTag;
    using TItemsTag = TEvFlatRepeatedFieldsTItemsTag;
    using TEmptyItemsTag = TEvFlatRepeatedFieldsTEmptyItemsTag;
    using TSchemeV1 = TEvFlatRepeatedFieldsTSchemeV1;
    using TScheme = TEvFlatRepeatedFieldsTVersions;

    friend class TEventFlat<TEvFlatRepeatedFields, TEvFlatRepeatedFieldsTVersions>;

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
using TEvFlatPayloadFieldsTMarkerTag = TFlatEventDefs::FixedField<ui32, 0>;
using TEvFlatPayloadFieldsTBlobTag = TFlatEventDefs::BytesField<1>;
using TEvFlatPayloadFieldsTNumbersTag = TFlatEventDefs::ArrayField<ui32, 2>;
using TEvFlatPayloadFieldsTSchemeV1 = TFlatEventDefs::Scheme<TFlatEventDefs::WithPayloadType<ui8>,
    TEvFlatPayloadFieldsTMarkerTag, TEvFlatPayloadFieldsTBlobTag, TEvFlatPayloadFieldsTNumbersTag>;
using TEvFlatPayloadFieldsTVersions = TFlatEventDefs::Versions<TEvFlatPayloadFieldsTSchemeV1>;

struct TEvFlatPayloadFields : TEventFlat<TEvFlatPayloadFields, TEvFlatPayloadFieldsTVersions> {
    using TBase = TEventFlat<TEvFlatPayloadFields, TEvFlatPayloadFieldsTVersions>;

    static constexpr ui32 EventType = EvFlatPayloadFields;

    using TMarkerTag = TEvFlatPayloadFieldsTMarkerTag;
    using TBlobTag = TEvFlatPayloadFieldsTBlobTag;
    using TNumbersTag = TEvFlatPayloadFieldsTNumbersTag;
    using TSchemeV1 = TEvFlatPayloadFieldsTSchemeV1;
    using TScheme = TEvFlatPayloadFieldsTVersions;

    friend class TEventFlat<TEvFlatPayloadFields, TEvFlatPayloadFieldsTVersions>;

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

struct TEvFlatAlignedPayloadFields : TEventFlat<TEvFlatAlignedPayloadFields, TEvFlatPayloadFieldsTVersions> {
    using TBase = TEventFlat<TEvFlatAlignedPayloadFields, TEvFlatPayloadFieldsTVersions>;

    static constexpr ui32 EventType = EvFlatAlignedPayloadFields;
    static constexpr bool UseAlignedInlinePayloadFormat = true;

    using TMarkerTag = TEvFlatPayloadFieldsTMarkerTag;
    using TBlobTag = TEvFlatPayloadFieldsTBlobTag;
    using TNumbersTag = TEvFlatPayloadFieldsTNumbersTag;
    using TSchemeV1 = TEvFlatPayloadFieldsTSchemeV1;
    using TScheme = TEvFlatPayloadFieldsTVersions;

    friend class TEventFlat<TEvFlatAlignedPayloadFields, TEvFlatPayloadFieldsTVersions>;

    static TEvFlatAlignedPayloadFields* Make() {
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
using TEvFlatInlineFieldsTMarkerTag = TFlatEventDefs::FixedField<ui32, 0>;
using TEvFlatInlineFieldsTBlobTag = TFlatEventDefs::InlineBytesField<16, 1>;
using TEvFlatInlineFieldsTNumbersTag = TFlatEventDefs::InlineArrayField<ui32, 4, 2>;
using TEvFlatInlineFieldsTSchemeV1 = TFlatEventDefs::Scheme<
    TEvFlatInlineFieldsTMarkerTag, TEvFlatInlineFieldsTBlobTag, TEvFlatInlineFieldsTNumbersTag>;
using TEvFlatInlineFieldsTVersions = TFlatEventDefs::Versions<TEvFlatInlineFieldsTSchemeV1>;

struct TEvFlatInlineFields : TEventFlat<TEvFlatInlineFields, TEvFlatInlineFieldsTVersions> {
    using TBase = TEventFlat<TEvFlatInlineFields, TEvFlatInlineFieldsTVersions>;

    static constexpr ui32 EventType = EvFlatInlineFields;

    using TMarkerTag = TEvFlatInlineFieldsTMarkerTag;
    using TBlobTag = TEvFlatInlineFieldsTBlobTag;
    using TNumbersTag = TEvFlatInlineFieldsTNumbersTag;
    using TSchemeV1 = TEvFlatInlineFieldsTSchemeV1;
    using TScheme = TEvFlatInlineFieldsTVersions;

    friend class TEventFlat<TEvFlatInlineFields, TEvFlatInlineFieldsTVersions>;

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

namespace {

    constexpr char AlignedFlatPayloadMarker = 0x08;

    size_t SerializeNumberTo(char* dst, size_t number) {
        size_t pos = 0;
        do {
            dst[pos++] = static_cast<char>((number & 0x7F) | (number >= 128 ? 0x80 : 0x00));
            number >>= 7;
        } while (number);
        return pos;
    }

    size_t DeserializeNumberFrom(TRope::TConstIterator& it, size_t& size) {
        size_t result = 0;
        size_t shift = 0;
        for (;;) {
            UNIT_ASSERT(it.Valid());
            UNIT_ASSERT_GT(size, 0u);
            const ui8 byte = static_cast<ui8>(*it.ContiguousData());
            it += 1;
            --size;
            result |= static_cast<size_t>(byte & 0x7F) << shift;
            if (!(byte & 0x80)) {
                return result;
            }
            shift += 7;
        }
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

    const char* GetSinglePayloadData(const TRope& rope, size_t payloadBytes) {
        auto it = rope.Begin();
        UNIT_ASSERT(it.Valid());

        if (*it.ContiguousData() == AlignedFlatPayloadMarker) {
            size_t size = rope.GetSize();
            it += 1;
            --size;

            const size_t payloadCount = DeserializeNumberFrom(it, size);
            UNIT_ASSERT_VALUES_EQUAL(payloadCount, 1u);

            const size_t payloadSize = DeserializeNumberFrom(it, size);
            UNIT_ASSERT_VALUES_EQUAL(payloadSize, payloadBytes);
            UNIT_ASSERT_GT(size, 0u);

            const size_t padding = static_cast<ui8>(*it.ContiguousData());
            it += 1;
            --size;
            it += padding;
        } else {
            TVector<TRope> payloads;
            if (payloadBytes) {
                payloads.push_back(TRope(TString::Uninitialized(payloadBytes)));
            } else {
                payloads.push_back(TRope{});
            }
            it += CalculateSerializedHeaderSizeImpl(payloads);
        }

        UNIT_ASSERT_C(it.ContiguousSize() >= payloadBytes,
            "contiguous size# " << it.ContiguousSize() << " payload bytes# " << payloadBytes);
        return it.ContiguousData();
    }

} // namespace

Y_UNIT_TEST_SUITE(TEventFlatTest) {
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
        UNIT_ASSERT(loaded->CreateSerializationInfo(true).Sections.size() == 1u);
    }

    Y_UNIT_TEST(AlignedInlinePayloadRoundTrip) {
        THolder<TEvFlatAlignedPayloadFields> ev(TEvFlatAlignedPayloadFields::Make());
        ev->Marker() = 202;
        ev->Blob().Set(TRope(TString("aligned")));

        ui32 values[] = {10, 20, 30, 40};
        std::memcpy(ev->Numbers().Init(std::size(values)), values, sizeof(values));

        auto serializer = MakeHolder<TAllocChunkSerializer>();
        UNIT_ASSERT(ev->SerializeToArcadiaStream(serializer.Get()));
        auto buffers = serializer->Release(ev->CreateSerializationInfo(false));

        const TRope rope = buffers->GetRope();
        auto it = rope.Begin();
        UNIT_ASSERT(it.Valid());
        UNIT_ASSERT_VALUES_EQUAL(*it.ContiguousData(), AlignedFlatPayloadMarker);

        THolder<IEventHandle> handle(new IEventHandle(
            EvFlatAlignedPayloadFields, 0, TActorId(), TActorId(), buffers, 0));

        TEvFlatAlignedPayloadFields* loaded = handle->Get<TEvFlatAlignedPayloadFields>();
        UNIT_ASSERT_VALUES_EQUAL(static_cast<ui32>(loaded->Marker()), 202u);
        UNIT_ASSERT_VALUES_EQUAL(loaded->Blob().Materialize(), "aligned");
        UNIT_ASSERT_VALUES_EQUAL(static_cast<ui32>(loaded->Numbers()[0]), 10u);
        UNIT_ASSERT_VALUES_EQUAL(static_cast<ui32>(loaded->Numbers()[3]), 40u);
    }

    Y_UNIT_TEST(AlignedInlinePayloadRoundTripFromFragmentedBuffer) {
        THolder<TEvFlatAlignedPayloadFields> ev(TEvFlatAlignedPayloadFields::Make());
        ev->Marker() = 303;
        ev->Blob().Set(TRope(TString("fragmented")));

        ui32 values[] = {1, 2, 3, 4, 5, 6, 7, 8};
        std::memcpy(ev->Numbers().Init(std::size(values)), values, sizeof(values));

        auto rope = ev->SerializeToRope(GetDefaultRcBufAllocator());
        UNIT_ASSERT(rope);

        const TString serialized = rope->ExtractUnderlyingContainerOrCopy<TString>();
        TRope fragmented;
        for (size_t offset = 0; offset < serialized.size();) {
            const size_t chunk = Min<size_t>(3 + offset % 11, serialized.size() - offset);
            fragmented.Insert(fragmented.End(), TRope(serialized.substr(offset, chunk)));
            offset += chunk;
        }

        TEventSerializationInfo info = ev->CreateSerializationInfo(false);
        TIntrusivePtr<TEventSerializedData> buffers = MakeIntrusive<TEventSerializedData>(std::move(fragmented), std::move(info));

        THolder<IEventHandle> handle(new IEventHandle(
            EvFlatAlignedPayloadFields, 0, TActorId(), TActorId(), buffers, 0));

        TEvFlatAlignedPayloadFields* loaded = handle->Get<TEvFlatAlignedPayloadFields>();
        UNIT_ASSERT_VALUES_EQUAL(static_cast<ui32>(loaded->Marker()), 303u);
        UNIT_ASSERT_VALUES_EQUAL(loaded->Blob().Materialize(), "fragmented");
        UNIT_ASSERT_VALUES_EQUAL(loaded->NumbersSize(), std::size(values));
        for (size_t i = 0; i < std::size(values); ++i) {
            UNIT_ASSERT_VALUES_EQUAL(static_cast<ui32>(loaded->Numbers()[i]), values[i]);
        }
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

    Y_UNIT_TEST(ArrayPayloadPreservesAlignmentLocally) {
        THolder<TEvFlatMessage> ev(TEvFlatMessage::Make());
        ev->TabletId() = 11;

        ui32 values[] = {10, 20, 30, 40};
        std::memcpy(ev->Numbers().Init(std::size(values)), values, sizeof(values));

        auto rope = ev->SerializeToRope(GetDefaultRcBufAllocator());
        UNIT_ASSERT(rope);

        const char* payloadData = GetSinglePayloadData(*rope, sizeof(values));
        UNIT_ASSERT_VALUES_EQUAL(reinterpret_cast<uintptr_t>(payloadData) % alignof(ui32), 0u);
    }

    Y_UNIT_TEST(ArrayPayloadAlignmentIsExposedInSerializationInfo) {
        THolder<TEvFlatMessage> ev(TEvFlatMessage::Make());
        ev->TabletId() = 42;

        TVector<ui32> values(2048, 7);
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

        auto rope = loaded->SerializeToRope(GetDefaultRcBufAllocator());
        UNIT_ASSERT(rope);

        const char* payloadData = GetSinglePayloadData(*rope, payloadBytes);
        UNIT_ASSERT_VALUES_EQUAL(reinterpret_cast<uintptr_t>(payloadData) % alignof(ui32), 0u);
    }
}
