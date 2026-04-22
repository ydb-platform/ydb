#pragma once

#include "event.h"
#include "event_pb.h"

#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/string/builder.h>
#include <util/system/type_name.h>
#include <util/system/unaligned_mem.h>

#include <cstring>
#include <limits>
#include <type_traits>
#include <utility>
#include <cstddef>

namespace NActors {
    namespace NFlatEventDetail {
        template <class... T>
        struct TLastType;

        template <class T>
        struct TLastType<T> {
            using Type = T;
        };

        template <class T, class... TRest>
        struct TLastType<T, TRest...> {
            using Type = typename TLastType<TRest...>::Type;
        };

        template <class... T>
        struct TAreDistinct : std::true_type {
        };

        template <class T, class... TRest>
        struct TAreDistinct<T, TRest...>
            : std::bool_constant<(!std::is_same_v<T, TRest> && ...) && TAreDistinct<TRest...>::value> {
        };
    } // namespace NFlatEventDetail

    /**
     * TEventFlat defines a flat binary storage format for actor events.
     *
     * Storage model:
     * - Fixed fields live inline inside a contiguous header buffer.
     * - BytesField/ArrayField payloads live out-of-line in TRope payload sections.
     * - The serialized form uses the existing extended payload framing: payload 0 is the
     *   header, payloads 1..N are field payload slots in schema order.
     *
     * Compatibility contract:
     * - The sequence of declared fields is the schema for a specific version.
     * - Fixed fields and array element types must be trivially copyable.
     * - Existing field tags must keep their meaning and binary representation across
     *   versions; incompatible layout changes require a new schema version.
     * - The format uses the native byte encoding produced by this implementation; there
     *   is no cross-endian compatibility layer.
     */
    template <class TEv>
    class TEventFlat : public IEventBase {
        static_assert(std::is_class_v<TEv>, "TEv must be a class");

    public:
        struct TPayloadRef {
            ui32 PayloadId = 0; // 0 means payload is absent
            ui32 Size = 0;      // bytes for BytesField, element count for ArrayField<T>
        };

    private:
        enum class EFieldKind {
            Fixed,
            Bytes,
            Array,
        };

    public:
        template <class T, size_t TagId>
        struct FixedField {
            static_assert(std::is_trivially_copyable_v<T>, "FixedField requires trivially copyable type");

            using TValue = T;
            static constexpr size_t Id = TagId;
            static constexpr EFieldKind Kind = EFieldKind::Fixed;
            static constexpr bool IsFixed = true;
            static constexpr bool IsPayload = false;
        };

        template <size_t TagId>
        struct BytesField {
            using TValue = char;
            static constexpr size_t Id = TagId;
            static constexpr EFieldKind Kind = EFieldKind::Bytes;
            static constexpr bool IsFixed = false;
            static constexpr bool IsPayload = true;
        };

        template <class T, size_t TagId>
        struct ArrayField {
            static_assert(std::is_trivially_copyable_v<T>, "ArrayField requires trivially copyable type");

            using TValue = T;
            static constexpr size_t Id = TagId;
            static constexpr EFieldKind Kind = EFieldKind::Array;
            static constexpr bool IsFixed = false;
            static constexpr bool IsPayload = true;
        };

        template <class T, bool IsConst>
        class TBasicView {
            using TPointer = std::conditional_t<IsConst, const char*, char*>;

        public:
            TBasicView() = default;

            explicit TBasicView(TPointer ptr)
                : Ptr(ptr)
            {}

            T Get() const {
                return ReadUnaligned<T>(Ptr);
            }

            operator T() const {
                return Get();
            }

            template <bool Enabled = !IsConst>
            std::enable_if_t<Enabled, TBasicView&> operator=(T value) {
                WriteUnaligned<T>(Ptr, value);
                return *this;
            }

        private:
            TPointer Ptr = nullptr;
        };

        template <class T>
        using TView = TBasicView<T, false>;

        template <class T>
        using TConstView = TBasicView<T, true>;

        template <bool IsConst>
        class TBasicBytesView {
            using TRopePtr = std::conditional_t<IsConst, const TRope*, TRope*>;
            using TPointer = std::conditional_t<IsConst, const char*, char*>;

        public:
            TBasicBytesView() = default;

            TBasicBytesView(TRopePtr payload, TPointer refPtr, ui32 payloadId)
                : Payload(payload)
                , RefPtr(refPtr)
                , PayloadId(payloadId)
            {}

            size_t size() const {
                return GetRef().Size;
            }

            bool empty() const {
                return size() == 0;
            }

            bool HasPayload() const {
                return GetRef().PayloadId != 0;
            }

            const TRope& Rope() const {
                return *Payload;
            }

            TString Materialize() const {
                return Payload->template ExtractUnderlyingContainerOrCopy<TString>();
            }

            template <bool Enabled = !IsConst>
            std::enable_if_t<Enabled> Clear() {
                Payload->clear();
                SetRef({});
            }

            template <bool Enabled = !IsConst>
            std::enable_if_t<Enabled> Set(TRope data) {
                *Payload = std::move(data);
                UpdateRefFromPayloadBytes();
            }

            template <bool Enabled = !IsConst>
            std::enable_if_t<Enabled> Append(TRope data) {
                if (!data) {
                    return;
                }
                Payload->Insert(Payload->End(), std::move(data));
                UpdateRefFromPayloadBytes();
            }

        private:
            TPayloadRef GetRef() const {
                return ReadUnaligned<TPayloadRef>(RefPtr);
            }

            template <bool Enabled = !IsConst>
            std::enable_if_t<Enabled> SetRef(TPayloadRef ref) {
                WriteUnaligned<TPayloadRef>(RefPtr, ref);
            }

            template <bool Enabled = !IsConst>
            std::enable_if_t<Enabled> UpdateRefFromPayloadBytes() {
                const size_t bytes = Payload->GetSize();
                Y_ENSURE(bytes <= std::numeric_limits<ui32>::max(), "Payload is too large");
                SetRef(TPayloadRef{
                    .PayloadId = bytes ? PayloadId : 0,
                    .Size = static_cast<ui32>(bytes),
                });
            }

        private:
            TRopePtr Payload = nullptr;
            TPointer RefPtr = nullptr;
            ui32 PayloadId = 0;
        };

        using TBytesView = TBasicBytesView<false>;
        using TConstBytesView = TBasicBytesView<true>;

        template <class T, bool IsConst>
        class TBasicArrayView {
            static_assert(std::is_trivially_copyable_v<T>, "Array view requires trivially copyable type");

            using TRopePtr = std::conditional_t<IsConst, const TRope*, TRope*>;
            using TPointer = std::conditional_t<IsConst, const char*, char*>;

            class TElementView {
            public:
                TElementView(TRopePtr payload, TPointer refPtr, ui32 payloadId, size_t index)
                    : Payload(payload)
                    , RefPtr(refPtr)
                    , PayloadId(payloadId)
                    , Index(index)
                {}

                operator T() const {
                    return Read();
                }

                template <bool Enabled = !IsConst>
                std::enable_if_t<Enabled, TElementView&> operator=(T value) {
                    Write(value);
                    return *this;
                }

            private:
                size_t Size() const {
                    return ReadUnaligned<TPayloadRef>(RefPtr).Size;
                }

                T Read() const {
                    Y_ENSURE(Index < Size(), "Array index " << Index << " is out of range " << Size());
                    T value;
                    auto it = Payload->Position(Index * sizeof(T));
                    TRopeUtils::Memcpy(reinterpret_cast<char*>(&value), it, sizeof(T));
                    return value;
                }

                template <bool Enabled = !IsConst>
                std::enable_if_t<Enabled> Write(T value) {
                    Y_ENSURE(Index < Size(), "Array index " << Index << " is out of range " << Size());
                    auto it = Payload->Position(Index * sizeof(T));
                    TRopeUtils::Memcpy(it, reinterpret_cast<const char*>(&value), sizeof(T));
                    TPayloadRef ref = ReadUnaligned<TPayloadRef>(RefPtr);
                    ref.PayloadId = ref.Size ? PayloadId : 0;
                    WriteUnaligned<TPayloadRef>(RefPtr, ref);
                }

            private:
                TRopePtr Payload = nullptr;
                TPointer RefPtr = nullptr;
                ui32 PayloadId = 0;
                size_t Index = 0;
            };

        public:
            TBasicArrayView() = default;

            TBasicArrayView(TRopePtr payload, TPointer refPtr, ui32 payloadId)
                : Payload(payload)
                , RefPtr(refPtr)
                , PayloadId(payloadId)
            {}

            size_t size() const {
                return GetRef().Size;
            }

            bool empty() const {
                return size() == 0;
            }

            TElementView operator[](size_t index) const {
                return TElementView(Payload, RefPtr, PayloadId, index);
            }

            T At(size_t index) const {
                return (*this)[index];
            }

            T Get(size_t index) const {
                return At(index);
            }

            void CopyTo(T* dst, size_t count) const {
                Y_ENSURE(count == size(), "Unexpected array size " << count << ", expected " << size());
                auto it = Payload->Begin();
                TRopeUtils::Memcpy(reinterpret_cast<char*>(dst), it, count * sizeof(T));
            }

            template <class F>
            void ForEach(F&& f) const {
                for (size_t i = 0; i < size(); ++i) {
                    f(At(i));
                }
            }

            template <bool Enabled = !IsConst>
            std::enable_if_t<Enabled> Set(size_t index, T value) {
                (*this)[index] = value;
            }

            template <bool Enabled = !IsConst>
            std::enable_if_t<Enabled> Clear() {
                Payload->clear();
                SetRef({});
            }

            template <bool Enabled = !IsConst>
            std::enable_if_t<Enabled> Append(const T* src, size_t count) {
                if (!count) {
                    return;
                }

                const size_t bytes = CheckedFieldBytes(count);
                TString data = TString::Uninitialized(bytes);
                std::memcpy(data.Detach(), src, bytes);
                Payload->Insert(Payload->End(), TRope(std::move(data)));

                TPayloadRef ref = GetRef();
                Y_ENSURE(ref.Size <= std::numeric_limits<ui32>::max() - count, "Array field is too large");
                ref.Size += count;
                ref.PayloadId = PayloadId;
                SetRef(ref);
            }

            template <bool Enabled = !IsConst>
            std::enable_if_t<Enabled> CopyFrom(const T* src, size_t count) {
                Clear();
                Append(src, count);
            }

            template <bool Enabled = !IsConst>
            std::enable_if_t<Enabled> Resize(size_t newSize) {
                const size_t current = size();
                if (newSize == current) {
                    return;
                }
                if (newSize == 0) {
                    Clear();
                    return;
                }
                Y_ENSURE(newSize >= current, "Shrink is not supported for flat array payloads");

                const size_t delta = newSize - current;
                const size_t bytes = CheckedFieldBytes(delta);
                TString data = TString::Uninitialized(bytes);
                std::memset(data.Detach(), 0, bytes);
                Payload->Insert(Payload->End(), TRope(std::move(data)));

                TPayloadRef ref = GetRef();
                ref.Size = static_cast<ui32>(newSize);
                ref.PayloadId = PayloadId;
                SetRef(ref);
            }

        private:
            static size_t CheckedFieldBytes(size_t count) {
                Y_ENSURE(count <= std::numeric_limits<size_t>::max() / sizeof(T), "Array field is too large");
                return count * sizeof(T);
            }

            TPayloadRef GetRef() const {
                return ReadUnaligned<TPayloadRef>(RefPtr);
            }

            template <bool Enabled = !IsConst>
            std::enable_if_t<Enabled> SetRef(TPayloadRef ref) {
                WriteUnaligned<TPayloadRef>(RefPtr, ref);
            }

        private:
            TRopePtr Payload = nullptr;
            TPointer RefPtr = nullptr;
            ui32 PayloadId = 0;
        };

        template <class T>
        using TArrayView = TBasicArrayView<T, false>;

        template <class T>
        using TConstArrayView = TBasicArrayView<T, true>;

        template <class... TFields>
        struct Scheme {
            static_assert(NFlatEventDetail::TAreDistinct<TFields...>::value, "Duplicate field tag in flat event scheme");

            static constexpr size_t FieldCount = sizeof...(TFields);
            static constexpr size_t PayloadFieldCount = (0u + ... + (TFields::IsPayload ? 1u : 0u));
            static constexpr size_t HeaderFieldsSize = (0u + ... + (TFields::IsFixed ? sizeof(typename TFields::TValue) : sizeof(TPayloadRef)));
            static constexpr size_t HeaderSize = sizeof(ui16) + HeaderFieldsSize;

            template <class TTag>
            static constexpr bool HasField = (std::is_same_v<TTag, TFields> || ...);

            template <class TTag>
            static constexpr bool HasFixedField = ((std::is_same_v<TTag, TFields> && TFields::IsFixed) || ...);

            template <class TTag>
            static constexpr bool HasPayloadField = ((std::is_same_v<TTag, TFields> && TFields::IsPayload) || ...);

            template <class TTag>
            static consteval size_t GetFixedOffset() {
                static_assert(HasFixedField<TTag>, "Field is not a fixed field in this scheme");
                return sizeof(ui16) + GetFieldOffsetImpl<TTag, TFields...>();
            }

            template <class TTag>
            static consteval size_t GetPayloadRefOffset() {
                static_assert(HasPayloadField<TTag>, "Field is not a payload field in this scheme");
                return sizeof(ui16) + GetFieldOffsetImpl<TTag, TFields...>();
            }

            template <class TTag>
            static consteval size_t GetPayloadIndex() {
                static_assert(HasPayloadField<TTag>, "Field is not a payload field in this scheme");
                return GetPayloadIndexImpl<TTag, TFields...>();
            }

            template <class F>
            static void ForEachPayloadField(F&& f) {
                (ForEachPayloadFieldImpl<TFields>(f), ...);
            }

        private:
            template <class TField>
            static consteval size_t GetStoredSize() {
                if constexpr (TField::IsFixed) {
                    return sizeof(typename TField::TValue);
                } else {
                    return sizeof(TPayloadRef);
                }
            }

            template <class TTag>
            static consteval size_t GetFieldOffsetImpl() {
                return 0;
            }

            template <class TTag, class TFirst, class... TRest>
            static consteval size_t GetFieldOffsetImpl() {
                if constexpr (std::is_same_v<TTag, TFirst>) {
                    return 0;
                } else {
                    return GetStoredSize<TFirst>() + GetFieldOffsetImpl<TTag, TRest...>();
                }
            }

            template <class TTag>
            static consteval size_t GetPayloadIndexImpl() {
                return 0;
            }

            template <class TTag, class TFirst, class... TRest>
            static consteval size_t GetPayloadIndexImpl() {
                if constexpr (std::is_same_v<TTag, TFirst>) {
                    return 0;
                } else {
                    return (TFirst::IsPayload ? 1u : 0u) + GetPayloadIndexImpl<TTag, TRest...>();
                }
            }

            template <class TField, class F>
            static void ForEachPayloadFieldImpl(F& f) {
                if constexpr (TField::IsPayload) {
                    f.template operator()<TField>();
                }
            }
        };

        template <class... TSchemes>
        struct Versions {
            static_assert(sizeof...(TSchemes) > 0, "At least one scheme version is required");

            using TLatestScheme = typename NFlatEventDetail::TLastType<TSchemes...>::Type;
            static constexpr ui16 VersionCount = sizeof...(TSchemes);

            template <class TScheme>
            static constexpr bool HasScheme = (std::is_same_v<TScheme, TSchemes> || ...);

            template <class TScheme>
            static consteval ui16 GetVersion() {
                static_assert(HasScheme<TScheme>, "Scheme does not belong to this flat event version set");
                return GetVersionImpl<TScheme, 1, TSchemes...>();
            }

            template <class F>
            static decltype(auto) Dispatch(ui16 version, F&& f) {
                return DispatchImpl<1, TSchemes...>(version, std::forward<F>(f));
            }

        private:
            template <class TScheme, ui16 Version, class TCurrent, class... TRest>
            static consteval ui16 GetVersionImpl() {
                if constexpr (std::is_same_v<TScheme, TCurrent>) {
                    return Version;
                } else {
                    static_assert(sizeof...(TRest) > 0, "Scheme does not belong to this flat event version set");
                    return GetVersionImpl<TScheme, Version + 1, TRest...>();
                }
            }

            template <ui16 Version, class TScheme, class... TRest, class F>
            static decltype(auto) DispatchImpl(ui16 version, F&& f) {
                if (version == Version) {
                    return f.template operator()<TScheme>();
                }

                if constexpr (sizeof...(TRest) > 0) {
                    return DispatchImpl<Version + 1, TRest...>(version, std::forward<F>(f));
                } else {
                    ythrow TWithBackTrace<yexception>() << "Unsupported flat event version " << version;
                }
            }
        };

    public:
        using THandle = TEventHandle<TEv>;
        using TPtr = typename THandle::TPtr;

        template <class TScheme, bool IsConst>
        class TBasicFrontend {
            using THeaderPtr = std::conditional_t<IsConst, const char*, char*>;
            using TPayloadVector = std::conditional_t<IsConst, const TVector<TRope>, TVector<TRope>>;

        public:
            TBasicFrontend() = default;

            TBasicFrontend(THeaderPtr header, TPayloadVector* payloads)
                : Header(header)
                , Payloads(payloads)
            {}

            template <class TTag>
            static constexpr bool HasField = TScheme::template HasField<TTag>;

            template <class TTag>
            auto Field() const {
                static_assert(TTag::IsFixed, "Field() is only available for fixed fields");
                static_assert(TScheme::template HasFixedField<TTag>, "Field is absent in this scheme");

                using TValue = typename TTag::TValue;
                constexpr size_t offset = TScheme::template GetFixedOffset<TTag>();
                if constexpr (IsConst) {
                    return TConstView<TValue>(Header + offset);
                } else {
                    return TView<TValue>(Header + offset);
                }
            }

            template <class TTag>
            auto Bytes() const {
                static_assert(TTag::Kind == EFieldKind::Bytes, "Bytes() is only available for bytes fields");
                static_assert(TScheme::template HasPayloadField<TTag>, "Field is absent in this scheme");

                constexpr size_t payloadIndex = TScheme::template GetPayloadIndex<TTag>();
                constexpr size_t refOffset = TScheme::template GetPayloadRefOffset<TTag>();
                if constexpr (IsConst) {
                    return TConstBytesView(&(*Payloads)[payloadIndex], Header + refOffset, payloadIndex + 1);
                } else {
                    return TBytesView(&(*Payloads)[payloadIndex], Header + refOffset, payloadIndex + 1);
                }
            }

            template <class TTag>
            auto Array() const {
                static_assert(TTag::Kind == EFieldKind::Array, "Array() is only available for array fields");
                static_assert(TScheme::template HasPayloadField<TTag>, "Field is absent in this scheme");

                using TValue = typename TTag::TValue;
                constexpr size_t payloadIndex = TScheme::template GetPayloadIndex<TTag>();
                constexpr size_t refOffset = TScheme::template GetPayloadRefOffset<TTag>();
                if constexpr (IsConst) {
                    return TConstArrayView<TValue>(&(*Payloads)[payloadIndex], Header + refOffset, payloadIndex + 1);
                } else {
                    return TArrayView<TValue>(&(*Payloads)[payloadIndex], Header + refOffset, payloadIndex + 1);
                }
            }

            template <class TTag>
            size_t GetSize() const {
                static_assert(TTag::IsPayload, "GetSize() is only available for payload-backed fields");
                static_assert(TScheme::template HasPayloadField<TTag>, "Field is absent in this scheme");
                return ReadUnaligned<TPayloadRef>(Header + TScheme::template GetPayloadRefOffset<TTag>()).Size;
            }

        private:
            THeaderPtr Header = nullptr;
            TPayloadVector* Payloads = nullptr;
        };

        template <class TScheme>
        using TFrontend = TBasicFrontend<TScheme, false>;

        template <class TScheme>
        using TConstFrontend = TBasicFrontend<TScheme, true>;

        ui32 Type() const override {
            return TEv::EventType;
        }

        TString ToStringHeader() const override {
            return TypeName<TEv>();
        }

        TString ToString() const override {
            return TStringBuilder() << TypeName<TEv>() << " { version# " << Version
                << " header# " << HeaderSize << " payloads# " << Payloads.size() << " }";
        }

        bool SerializeToArcadiaStream(TChunkSerializer* serializer) const override {
            AssertInitializedDebug();
            if (!HasPayloads()) {
                return SerializeHeaderOnlyToArcadiaStream(serializer);
            }
            const auto payloads = BuildWirePayloads();
            return SerializeToArcadiaStreamImpl(serializer, payloads);
        }

        std::optional<TRope> SerializeToRope(IRcBufAllocator* allocator) const override {
            AssertInitializedDebug();
            if (!HasPayloads()) {
                TRcBuf headerBuf = allocator->AllocRcBuf(HeaderSize, 0, 0);
                if (!headerBuf) {
                    return {};
                }
                if (HeaderSize) {
                    std::memcpy(headerBuf.GetDataMut(), HeaderData(), HeaderSize);
                }
                return TRope(std::move(headerBuf));
            }
            const auto payloads = BuildWirePayloads();
            const ui32 headerSize = CalculateSerializedHeaderSizeImpl(payloads);

            TRope result;
            if (headerSize) {
                TRcBuf headerBuf = allocator->AllocRcBuf(headerSize, 0, 0);
                if (!headerBuf) {
                    return {};
                }

                char* data = headerBuf.GetDataMut();
                auto append = [&data](const char* ptr, size_t len) {
                    std::memcpy(data, ptr, len);
                    data += len;
                    return true;
                };

                char marker = ExtendedPayloadMarker;
                append(&marker, 1);

                auto appendNumber = [&append](size_t number) {
                    char buf[MaxNumberBytes];
                    size_t pos = 0;
                    do {
                        buf[pos++] = (number & 0x7F) | (number >= 128 ? 0x80 : 0x00);
                        number >>= 7;
                    } while (number);
                    return append(buf, pos);
                };

                appendNumber(payloads.size());
                for (const TRope& rope : payloads) {
                    appendNumber(rope.GetSize());
                }

                result.Insert(result.End(), std::move(headerBuf));
            }

            for (const TRope& rope : payloads) {
                result.Insert(result.End(), TRope(rope));
            }

            return result;
        }

        bool IsSerializable() const override {
            return true;
        }

        ui32 CalculateSerializedSize() const override {
            AssertInitializedDebug();
            return CalculateSerializedSizeCached();
        }

        ui32 CalculateSerializedSizeCached() const override {
            AssertInitializedDebug();
            if (!HasPayloads()) {
                return HeaderSize;
            }
            const auto payloads = BuildWirePayloads();
            return CalculateSerializedSizeImpl(payloads, 0);
        }

        TEventSerializationInfo CreateSerializationInfo(bool allowExternalDataChannel) const override {
            AssertInitializedDebug();
            if (!HasPayloads()) {
                TEventSerializationInfo info;
                if (allowExternalDataChannel && HeaderSize) {
                    info.Sections.push_back(TEventSectionInfo{0, HeaderSize, 0, 0, true, false});
                }
                return info;
            }
            const auto payloads = BuildWirePayloads();
            return CreateSerializationInfoImpl(0, allowExternalDataChannel && AllowExternalDataChannel(), payloads, 0);
        }

        ui16 GetVersion() const {
            AssertInitializedDebug();
            return Version;
        }

        size_t GetSerializedSize() const {
            return CalculateSerializedSizeCached();
        }

        static TEv* MakeEvent() {
            using TVersions = typename TEv::TScheme;
            using TLatestScheme = typename TVersions::TLatestScheme;

            THolder<TEv> holder(new TEv());
            holder->ResetHeaderStorage(TLatestScheme::HeaderSize);
            std::memset(holder->MutableHeaderData(), 0, TLatestScheme::HeaderSize);
            holder->Payloads.resize(TLatestScheme::PayloadFieldCount);
            holder->Version = TVersions::VersionCount;
            WriteUnaligned<ui16>(holder->MutableHeaderData(), holder->Version);
            return holder.Release();
        }

        static TEv* Load(const TEventSerializedData* input) {
            Y_ENSURE(input, "Flat event buffer is null");
            Y_ENSURE(input->GetSize(), "Flat event payload is empty");

            TVector<TRope> wirePayloads;
            if (const auto& info = input->GetSerializationInfo(); info.IsExtendedFormat) {
                TRope::TConstIterator iter = input->GetBeginIter();
                size_t size = input->GetSize();
                size_t totalPayloadSize = 0;
                ParseExtendedFormatPayload(iter, size, wirePayloads, totalPayloadSize);
            } else {
                wirePayloads.push_back(input->GetRope());
            }

            Y_ENSURE(!wirePayloads.empty(), "Flat event payload set is empty");

            THolder<TEv> holder(new TEv());
            holder->AssignHeaderStorage(wirePayloads.front());
            holder->Version = ValidateHeaderStorage(holder->HeaderData(), holder->HeaderSize);

            holder->DispatchCurrentVersion([&]<class TScheme>() {
                Y_ENSURE(wirePayloads.size() == 1 + TScheme::PayloadFieldCount,
                    "Unexpected number of payloads " << wirePayloads.size() << " for flat event version " << holder->Version);
                holder->Payloads.resize(TScheme::PayloadFieldCount);
                for (size_t i = 0; i < TScheme::PayloadFieldCount; ++i) {
                    holder->Payloads[i] = std::move(wirePayloads[i + 1]);
                }
                holder->template ValidateStateForScheme<TScheme>();
            });

            return holder.Release();
        }

        template <class TTag>
        bool HasField() const {
            return DispatchCurrentVersion([&]<class TScheme>() {
                return TScheme::template HasField<TTag>;
            });
        }

        template <class TScheme>
        bool IsVersion() const {
            using TVersions = typename TEv::TScheme;
            static_assert(TVersions::template HasScheme<TScheme>, "Scheme does not belong to this flat event");
            AssertInitializedDebug();
            return Version == TVersions::template GetVersion<TScheme>();
        }

        template <class TScheme>
        TFrontend<TScheme> GetFrontend() {
            AssertFrontendVersion<TScheme>();
            return MakeFrontend<TScheme>();
        }

        template <class TScheme>
        TConstFrontend<TScheme> GetFrontend() const {
            AssertFrontendVersion<TScheme>();
            return MakeFrontend<TScheme>();
        }

        template <class TTag>
        TView<typename TTag::TValue> Field() {
            static_assert(TTag::IsFixed, "Field() is only available for fixed fields");
            return DispatchCurrentVersion([&]<class TScheme>() -> TView<typename TTag::TValue> {
                if constexpr (TScheme::template HasFixedField<TTag>) {
                    return MakeFrontend<TScheme>().template Field<TTag>();
                } else {
                    Y_ENSURE(false, "Field is absent in this event version");
                    return {};
                }
            });
        }

        template <class TTag>
        TConstView<typename TTag::TValue> Field() const {
            static_assert(TTag::IsFixed, "Field() is only available for fixed fields");
            return DispatchCurrentVersion([&]<class TScheme>() -> TConstView<typename TTag::TValue> {
                if constexpr (TScheme::template HasFixedField<TTag>) {
                    return MakeFrontend<TScheme>().template Field<TTag>();
                } else {
                    Y_ENSURE(false, "Field is absent in this event version");
                    return {};
                }
            });
        }

        template <class TTag>
        TBytesView Bytes() {
            static_assert(TTag::Kind == EFieldKind::Bytes, "Bytes() is only available for bytes fields");
            return DispatchCurrentVersion([&]<class TScheme>() -> TBytesView {
                if constexpr (TScheme::template HasPayloadField<TTag>) {
                    return MakeFrontend<TScheme>().template Bytes<TTag>();
                } else {
                    Y_ENSURE(false, "Field is absent in this event version");
                    return {};
                }
            });
        }

        template <class TTag>
        TConstBytesView Bytes() const {
            static_assert(TTag::Kind == EFieldKind::Bytes, "Bytes() is only available for bytes fields");
            return DispatchCurrentVersion([&]<class TScheme>() -> TConstBytesView {
                if constexpr (TScheme::template HasPayloadField<TTag>) {
                    return MakeFrontend<TScheme>().template Bytes<TTag>();
                } else {
                    Y_ENSURE(false, "Field is absent in this event version");
                    return {};
                }
            });
        }

        template <class TTag>
        TArrayView<typename TTag::TValue> Array() {
            static_assert(TTag::Kind == EFieldKind::Array, "Array() is only available for array fields");
            return DispatchCurrentVersion([&]<class TScheme>() -> TArrayView<typename TTag::TValue> {
                if constexpr (TScheme::template HasPayloadField<TTag>) {
                    return MakeFrontend<TScheme>().template Array<TTag>();
                } else {
                    Y_ENSURE(false, "Field is absent in this event version");
                    return {};
                }
            });
        }

        template <class TTag>
        TConstArrayView<typename TTag::TValue> Array() const {
            static_assert(TTag::Kind == EFieldKind::Array, "Array() is only available for array fields");
            return DispatchCurrentVersion([&]<class TScheme>() -> TConstArrayView<typename TTag::TValue> {
                if constexpr (TScheme::template HasPayloadField<TTag>) {
                    return MakeFrontend<TScheme>().template Array<TTag>();
                } else {
                    Y_ENSURE(false, "Field is absent in this event version");
                    return {};
                }
            });
        }

        template <class TTag>
        size_t GetSize() const {
            static_assert(TTag::IsPayload, "GetSize() is only available for payload-backed fields");
            return DispatchCurrentVersion([&]<class TScheme>() -> size_t {
                if constexpr (TScheme::template HasPayloadField<TTag>) {
                    return MakeFrontend<TScheme>().template GetSize<TTag>();
                } else {
                    Y_ENSURE(false, "Field is absent in this event version");
                    return 0;
                }
            });
        }

    protected:
        TEventFlat() = default;

    private:
        static constexpr size_t InlineHeaderCapacity = 64;

        template <class TValue>
        static size_t CheckedFieldBytes(size_t count) {
            Y_ENSURE(count <= std::numeric_limits<size_t>::max() / sizeof(TValue), "Flat event field is too large");
            return count * sizeof(TValue);
        }

        static ui16 ValidateHeaderStorage(const char* header, size_t size) {
            Y_ENSURE(size >= sizeof(ui16), "Flat event header is too short");
            return ReadUnaligned<ui16>(header);
        }

        template <class TScheme>
        void ValidateStateForScheme() const {
            Y_ENSURE(HeaderSize == TScheme::HeaderSize,
                "Unexpected flat event header size " << HeaderSize << ", expected " << TScheme::HeaderSize);
            Y_ENSURE(Payloads.size() == TScheme::PayloadFieldCount,
                "Unexpected number of flat payload slots " << Payloads.size() << ", expected " << TScheme::PayloadFieldCount);

            TScheme::ForEachPayloadField([&]<class TField>() {
                constexpr size_t payloadIndex = TScheme::template GetPayloadIndex<TField>();
                const TPayloadRef ref = GetPayloadRef<TScheme, TField>();
                const TRope& payload = Payloads[payloadIndex];
                const size_t expectedBytes = CheckedFieldBytes<typename TField::TValue>(ref.Size);

                if (ref.PayloadId == 0) {
                    Y_ENSURE(ref.Size == 0, "Absent payload must have zero size");
                    Y_ENSURE(payload.GetSize() == 0, "Absent payload slot must be empty");
                } else {
                    Y_ENSURE(ref.PayloadId == payloadIndex + 1,
                        "Unexpected payload id " << ref.PayloadId << " for slot " << payloadIndex);
                    Y_ENSURE(payload.GetSize() == expectedBytes,
                        "Payload size mismatch, payload# " << payloadIndex << " expected " << expectedBytes
                        << " got " << payload.GetSize());
                }
            });
        }

        bool AllowExternalDataChannel() const {
            size_t totalPayloadSize = 0;
            for (const TRope& payload : Payloads) {
                totalPayloadSize += payload.GetSize();
            }
            return totalPayloadSize >= 4096;
        }

        bool HasPayloads() const {
            return !Payloads.empty();
        }

        bool SerializeHeaderOnlyToArcadiaStream(TChunkSerializer* serializer) const {
            const char* data = HeaderData();
            size_t len = HeaderSize;
            while (len) {
                void* chunkData = nullptr;
                int chunkSize = 0;
                if (!serializer->Next(&chunkData, &chunkSize)) {
                    return false;
                }

                const size_t numBytesToCopy = std::min<size_t>(len, chunkSize);
                std::memcpy(chunkData, data, numBytesToCopy);
                data += numBytesToCopy;
                len -= numBytesToCopy;

                const int unused = chunkSize - static_cast<int>(numBytesToCopy);
                if (unused) {
                    serializer->BackUp(unused);
                }
            }
            return true;
        }

        TVector<TRope> BuildWirePayloads() const {
            TVector<TRope> result;
            result.reserve(1 + Payloads.size());
            result.emplace_back(BuildHeaderPayload());
            for (const TRope& payload : Payloads) {
                result.emplace_back(payload);
            }
            return result;
        }

        template <class F>
        decltype(auto) DispatchCurrentVersion(F&& f) const {
            using TVersions = typename TEv::TScheme;
            AssertInitializedDebug();
            if constexpr (TVersions::VersionCount == 1) {
                return f.template operator()<typename TVersions::TLatestScheme>();
            } else {
                return TVersions::Dispatch(Version, std::forward<F>(f));
            }
        }

        template <class TScheme, class TTag>
        TPayloadRef GetPayloadRef() const {
            return ReadUnaligned<TPayloadRef>(HeaderData() + TScheme::template GetPayloadRefOffset<TTag>());
        }

        void AssertInitializedDebug() const {
            Y_DEBUG_ABORT_UNLESS(Version != 0);
            Y_DEBUG_ABORT_UNLESS(HeaderSize >= sizeof(ui16));
            Y_DEBUG_ABORT_UNLESS(ReadUnaligned<ui16>(HeaderData()) == Version);
        }

        template <class TScheme>
        void AssertFrontendVersion() const {
            using TVersions = typename TEv::TScheme;
            static_assert(TVersions::template HasScheme<TScheme>, "Scheme does not belong to this flat event");
            AssertInitializedDebug();
            Y_DEBUG_ABORT_UNLESS(Version == TVersions::template GetVersion<TScheme>());
        }

        template <class TScheme>
        TFrontend<TScheme> MakeFrontend() {
            return TFrontend<TScheme>(MutableHeaderData(), &Payloads);
        }

        template <class TScheme>
        TConstFrontend<TScheme> MakeFrontend() const {
            return TConstFrontend<TScheme>(HeaderData(), &Payloads);
        }

        void ResetHeaderStorage(size_t size) {
            HeaderSize = size;
            if (size <= InlineHeaderCapacity) {
                HeaderStoredInline = true;
                HeaderStorage = TString();
            } else {
                HeaderStoredInline = false;
                HeaderStorage = TString::Uninitialized(size);
            }
        }

        void AssignHeaderStorage(TString&& header) {
            const size_t size = header.size();
            if (size <= InlineHeaderCapacity) {
                HeaderSize = size;
                HeaderStoredInline = true;
                if (size) {
                    std::memcpy(InlineHeader, header.data(), size);
                }
                HeaderStorage = TString();
            } else {
                HeaderStoredInline = false;
                HeaderSize = size;
                HeaderStorage = std::move(header);
            }
        }

        void AssignHeaderStorage(const TRope& header) {
            const size_t size = header.GetSize();
            if (size <= InlineHeaderCapacity) {
                HeaderSize = size;
                HeaderStoredInline = true;
                if (size) {
                    auto it = header.Begin();
                    TRopeUtils::Memcpy(InlineHeader, it, size);
                }
                HeaderStorage = TString();
            } else {
                HeaderStoredInline = false;
                HeaderStorage = header.template ExtractUnderlyingContainerOrCopy<TString>();
                HeaderSize = HeaderStorage.size();
            }
        }

        TRope BuildHeaderPayload() const {
            if (HeaderStoredInline) {
                TString header = TString::Uninitialized(HeaderSize);
                if (HeaderSize) {
                    std::memcpy(header.Detach(), InlineHeader, HeaderSize);
                }
                return TRope(std::move(header));
            }

            return TRope(TString(HeaderStorage));
        }

        char* MutableHeaderData() {
            return HeaderStoredInline ? InlineHeader : HeaderStorage.Detach();
        }

        const char* HeaderData() const {
            return HeaderStoredInline ? InlineHeader : HeaderStorage.data();
        }

    private:
        TString HeaderStorage;
        alignas(std::max_align_t) char InlineHeader[InlineHeaderCapacity] = {};
        TVector<TRope> Payloads;
        size_t HeaderSize = 0;
        ui16 Version = 0;
        bool HeaderStoredInline = true;
    };

} // namespace NActors
