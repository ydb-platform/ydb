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
        template <class T, class = void>
        struct TIsSchemeOption : std::false_type {
        };

        template <class T>
        struct TIsSchemeOption<T, std::void_t<decltype(T::IsSchemeOption)>>
            : std::bool_constant<T::IsSchemeOption> {
        };

        template <class T, class = void>
        struct THasPayloadIdType : std::false_type {
        };

        template <class T>
        struct THasPayloadIdType<T, std::void_t<typename T::TPayloadIdType>>
            : std::true_type {
        };

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

        template <class... T>
        struct TAreDistinctFields : std::true_type {
        };

        template <class T, class... TRest>
        struct TAreDistinctFields<T, TRest...>
            : std::bool_constant<
                (TIsSchemeOption<T>::value
                    ? TAreDistinctFields<TRest...>::value
                    : ((TIsSchemeOption<TRest>::value || !std::is_same_v<T, TRest>) && ...) && TAreDistinctFields<TRest...>::value)> {
        };

        template <class TDefault, class... T>
        struct TPayloadIdTypeSelector {
            using Type = TDefault;
        };

        template <class TDefault, class TFirst, bool HasPayloadIdType, class... TRest>
        struct TPayloadIdTypeSelectorImpl;

        template <class TDefault, class TFirst, class... TRest>
        struct TPayloadIdTypeSelectorImpl<TDefault, TFirst, true, TRest...> {
            using Type = typename TFirst::TPayloadIdType;
        };

        template <class TDefault, class TFirst, class... TRest>
        struct TPayloadIdTypeSelectorImpl<TDefault, TFirst, false, TRest...> {
            using Type = typename TPayloadIdTypeSelector<TDefault, TRest...>::Type;
        };

        template <class TDefault, class TFirst, class... TRest>
        struct TPayloadIdTypeSelector<TDefault, TFirst, TRest...> {
            using Type = typename TPayloadIdTypeSelectorImpl<TDefault, TFirst, THasPayloadIdType<TFirst>::value, TRest...>::Type;
        };
    } // namespace NFlatEventDetail

    /**
     * TEventFlat defines a flat binary storage format for actor events.
     *
     * Storage model:
     * - Fixed fields live inline inside a contiguous header buffer.
     * - BytesField/ArrayField payloads live out-of-line in TRope payload sections.
     * - The serialized form uses the existing extended payload framing for field payloads
     *   only; the flat header itself is serialized inline after the payload list.
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
        template <class TPayloadId>
        struct TPayloadRefBase {
            TPayloadId PayloadId = 0; // 0 means payload is absent
        };

        template <class TPayloadId>
        struct WithPayloadType {
            using TPayloadIdType = TPayloadId;
            static constexpr bool IsSchemeOption = true;
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

        template <class TPayloadId, bool IsConst>
        class TBasicBytesView {
            using TPayloadRef = TPayloadRefBase<TPayloadId>;
            using TRopePtr = std::conditional_t<IsConst, const TRope*, TRope*>;
            using TPointer = std::conditional_t<IsConst, const char*, char*>;

        public:
            TBasicBytesView() = default;

            TBasicBytesView(TRopePtr payload, TPointer refPtr, TPayloadId payloadId)
                : Payload(payload)
                , RefPtr(refPtr)
                , PayloadId(payloadId)
            {}

            size_t size() const {
                return Payload->GetSize();
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
                SetRef(TPayloadRef{
                    .PayloadId = static_cast<TPayloadId>(Payload->GetSize() ? PayloadId : 0),
                });
            }

        private:
            TRopePtr Payload = nullptr;
            TPointer RefPtr = nullptr;
            TPayloadId PayloadId = 0;
        };

        template <class TPayloadId>
        using TBytesView = TBasicBytesView<TPayloadId, false>;

        template <class TPayloadId>
        using TConstBytesView = TBasicBytesView<TPayloadId, true>;

        template <class T, class TPayloadId, bool IsConst>
        class TBasicArrayView {
            static_assert(std::is_trivially_copyable_v<T>, "Array view requires trivially copyable type");
            using TPayloadRef = TPayloadRefBase<TPayloadId>;

            using TRopePtr = std::conditional_t<IsConst, const TRope*, TRope*>;
            using TPointer = std::conditional_t<IsConst, const char*, char*>;

            class TElementView {
            public:
                TElementView(TRopePtr payload, TPointer refPtr, TPayloadId payloadId, size_t index)
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
                    const size_t bytes = Payload->GetSize();
                    Y_DEBUG_ABORT_UNLESS(bytes % sizeof(T) == 0);
                    return bytes / sizeof(T);
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
                    ref.PayloadId = Payload->GetSize() ? PayloadId : 0;
                    WriteUnaligned<TPayloadRef>(RefPtr, ref);
                }

            private:
                TRopePtr Payload = nullptr;
                TPointer RefPtr = nullptr;
                TPayloadId PayloadId = 0;
                size_t Index = 0;
            };

        public:
            TBasicArrayView() = default;

            TBasicArrayView(TRopePtr payload, TPointer refPtr, TPayloadId payloadId)
                : Payload(payload)
                , RefPtr(refPtr)
                , PayloadId(payloadId)
            {}

            size_t size() const {
                const size_t bytes = Payload->GetSize();
                Y_DEBUG_ABORT_UNLESS(bytes % sizeof(T) == 0);
                return bytes / sizeof(T);
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

                SetRef(TPayloadRef{
                    .PayloadId = static_cast<TPayloadId>(PayloadId),
                });
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

                SetRef(TPayloadRef{
                    .PayloadId = static_cast<TPayloadId>(PayloadId),
                });
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
            TPayloadId PayloadId = 0;
        };

        template <class T, class TPayloadId>
        using TArrayView = TBasicArrayView<T, TPayloadId, false>;

        template <class T, class TPayloadId>
        using TConstArrayView = TBasicArrayView<T, TPayloadId, true>;

        template <class... TItems>
        struct Scheme {
            static constexpr size_t PayloadTypeOptionCount = (0u + ... + (NFlatEventDetail::THasPayloadIdType<TItems>::value ? 1u : 0u));
            static_assert(PayloadTypeOptionCount <= 1, "Flat event scheme may specify at most one payload id type");

            using TPayloadId = typename NFlatEventDetail::TPayloadIdTypeSelector<ui16, TItems...>::Type;
            static_assert(std::is_integral_v<TPayloadId> && std::is_unsigned_v<TPayloadId>,
                "Scheme payload id type must be an unsigned integral type");

            using TPayloadRef = TPayloadRefBase<TPayloadId>;

        private:
            template <class TItem, bool IsOption = NFlatEventDetail::TIsSchemeOption<TItem>::value>
            struct TItemTraits;

            template <class TItem>
            struct TItemTraits<TItem, false> {
                static constexpr bool IsField = true;
                static constexpr bool IsFixed = TItem::IsFixed;
                static constexpr bool IsPayload = TItem::IsPayload;
                static constexpr size_t StoredSize = TItem::IsFixed ? sizeof(typename TItem::TValue) : sizeof(TPayloadRef);
            };

            template <class TItem>
            struct TItemTraits<TItem, true> {
                static constexpr bool IsField = false;
                static constexpr bool IsFixed = false;
                static constexpr bool IsPayload = false;
                static constexpr size_t StoredSize = 0;
            };

        public:
            static_assert(NFlatEventDetail::TAreDistinctFields<TItems...>::value, "Duplicate field tag in flat event scheme");
            static constexpr size_t FieldCount = (0u + ... + (TItemTraits<TItems>::IsField ? 1u : 0u));
            static constexpr size_t PayloadFieldCount = (0u + ... + (TItemTraits<TItems>::IsPayload ? 1u : 0u));
            static_assert(PayloadFieldCount <= std::numeric_limits<TPayloadId>::max(), "Too many payload fields in flat event scheme");
            static_assert(PayloadFieldCount > 0 || PayloadTypeOptionCount == 0,
                "WithPayloadType is only allowed for schemes with payload fields");
            static constexpr size_t HeaderFieldsSize = (0u + ... + TItemTraits<TItems>::StoredSize);
            static constexpr size_t HeaderSize = sizeof(ui8) + HeaderFieldsSize;

            template <class TTag>
            static constexpr bool HasField = ((TItemTraits<TItems>::IsField && std::is_same_v<TTag, TItems>) || ...);

            template <class TTag>
            static constexpr bool HasFixedField = ((TItemTraits<TItems>::IsField && std::is_same_v<TTag, TItems> && TItemTraits<TItems>::IsFixed) || ...);

            template <class TTag>
            static constexpr bool HasPayloadField = ((TItemTraits<TItems>::IsField && std::is_same_v<TTag, TItems> && TItemTraits<TItems>::IsPayload) || ...);

            template <class TTag>
            static consteval size_t GetFixedOffset() {
                static_assert(HasFixedField<TTag>, "Field is not a fixed field in this scheme");
                return sizeof(ui8) + GetFieldOffsetImpl<TTag, TItems...>();
            }

            template <class TTag>
            static consteval size_t GetPayloadRefOffset() {
                static_assert(HasPayloadField<TTag>, "Field is not a payload field in this scheme");
                return sizeof(ui8) + GetFieldOffsetImpl<TTag, TItems...>();
            }

            template <class TTag>
            static consteval size_t GetPayloadIndex() {
                static_assert(HasPayloadField<TTag>, "Field is not a payload field in this scheme");
                return GetPayloadIndexImpl<TTag, TItems...>();
            }

            template <class F>
            static void ForEachPayloadField(F&& f) {
                (ForEachPayloadFieldImpl<TItems>(f), ...);
            }

        private:
            template <class TTag>
            static consteval size_t GetFieldOffsetImpl() {
                return 0;
            }

            template <class TTag, class TFirst, class... TRest>
            static consteval size_t GetFieldOffsetImpl() {
                if constexpr (TItemTraits<TFirst>::IsField && std::is_same_v<TTag, TFirst>) {
                    return 0;
                } else {
                    return TItemTraits<TFirst>::StoredSize + GetFieldOffsetImpl<TTag, TRest...>();
                }
            }

            template <class TTag>
            static consteval size_t GetPayloadIndexImpl() {
                return 0;
            }

            template <class TTag, class TFirst, class... TRest>
            static consteval size_t GetPayloadIndexImpl() {
                if constexpr (TItemTraits<TFirst>::IsField && std::is_same_v<TTag, TFirst>) {
                    return 0;
                } else {
                    return (TItemTraits<TFirst>::IsPayload ? 1u : 0u) + GetPayloadIndexImpl<TTag, TRest...>();
                }
            }

            template <class TField, class F>
            static void ForEachPayloadFieldImpl(F& f) {
                if constexpr (TItemTraits<TField>::IsPayload) {
                    f.template operator()<TField>();
                }
            }
        };

        template <class... TSchemes>
        struct Versions {
            static_assert(sizeof...(TSchemes) > 0, "At least one scheme version is required");

            using TLatestScheme = typename NFlatEventDetail::TLastType<TSchemes...>::Type;
            static_assert(sizeof...(TSchemes) <= std::numeric_limits<ui8>::max(), "Too many flat event versions");
            static constexpr ui8 VersionCount = sizeof...(TSchemes);

            template <class TScheme>
            static constexpr bool HasScheme = (std::is_same_v<TScheme, TSchemes> || ...);

            template <class TScheme>
            static consteval ui8 GetVersion() {
                static_assert(HasScheme<TScheme>, "Scheme does not belong to this flat event version set");
                return GetVersionImpl<TScheme, 1, TSchemes...>();
            }

            template <class F>
            static decltype(auto) Dispatch(ui8 version, F&& f) {
                return DispatchImpl<1, TSchemes...>(version, std::forward<F>(f));
            }

        private:
            template <class TScheme, ui8 Version, class TCurrent, class... TRest>
            static consteval ui8 GetVersionImpl() {
                if constexpr (std::is_same_v<TScheme, TCurrent>) {
                    return Version;
                } else {
                    static_assert(sizeof...(TRest) > 0, "Scheme does not belong to this flat event version set");
                    return GetVersionImpl<TScheme, static_cast<ui8>(Version + 1), TRest...>();
                }
            }

            template <ui8 Version, class TScheme, class... TRest, class F>
            static decltype(auto) DispatchImpl(ui8 version, F&& f) {
                if (version == Version) {
                    return f.template operator()<TScheme>();
                }

                if constexpr (sizeof...(TRest) > 0) {
                    return DispatchImpl<static_cast<ui8>(Version + 1), TRest...>(version, std::forward<F>(f));
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
                    return TConstBytesView<typename TScheme::TPayloadId>(&GetPayload(payloadIndex), Header + refOffset,
                        static_cast<typename TScheme::TPayloadId>(payloadIndex + 1));
                } else {
                    return TBytesView<typename TScheme::TPayloadId>(&GetMutablePayload(payloadIndex), Header + refOffset,
                        static_cast<typename TScheme::TPayloadId>(payloadIndex + 1));
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
                    return TConstArrayView<TValue, typename TScheme::TPayloadId>(&GetPayload(payloadIndex), Header + refOffset,
                        static_cast<typename TScheme::TPayloadId>(payloadIndex + 1));
                } else {
                    return TArrayView<TValue, typename TScheme::TPayloadId>(&GetMutablePayload(payloadIndex), Header + refOffset,
                        static_cast<typename TScheme::TPayloadId>(payloadIndex + 1));
                }
            }

            template <class TTag>
            size_t GetSize() const {
                static_assert(TTag::IsPayload, "GetSize() is only available for payload-backed fields");
                static_assert(TScheme::template HasPayloadField<TTag>, "Field is absent in this scheme");
                constexpr size_t payloadIndex = TScheme::template GetPayloadIndex<TTag>();
                const size_t bytes = GetPayload(payloadIndex).GetSize();
                if constexpr (TTag::Kind == EFieldKind::Bytes) {
                    return bytes;
                } else {
                    using TValue = typename TTag::TValue;
                    Y_DEBUG_ABORT_UNLESS(bytes % sizeof(TValue) == 0);
                    return bytes / sizeof(TValue);
                }
            }

        private:
            const TRope& GetPayload(size_t index) const {
                if (index < Payloads->size()) {
                    return (*Payloads)[index];
                }

                static const TRope EmptyPayload;
                return EmptyPayload;
            }

            TRope& GetMutablePayload(size_t index) const {
                if (Payloads->size() < TScheme::PayloadFieldCount) {
                    Payloads->resize(TScheme::PayloadFieldCount);
                }
                return (*Payloads)[index];
            }

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
            return SerializeToArcadiaStreamImpl(serializer, Payloads)
                && SerializeHeaderOnlyToArcadiaStream(serializer);
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
            const ui32 headerSize = CalculateSerializedHeaderSizeImpl(Payloads);

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

                appendNumber(Payloads.size());
                for (const TRope& rope : Payloads) {
                    appendNumber(rope.GetSize());
                }

                result.Insert(result.End(), std::move(headerBuf));
            }

            for (const TRope& rope : Payloads) {
                result.Insert(result.End(), TRope(rope));
            }

            TRcBuf flatHeaderBuf = allocator->AllocRcBuf(HeaderSize, 0, 0);
            if (!flatHeaderBuf) {
                return {};
            }
            if (HeaderSize) {
                std::memcpy(flatHeaderBuf.GetDataMut(), HeaderData(), HeaderSize);
            }
            result.Insert(result.End(), std::move(flatHeaderBuf));

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
            return CalculateSerializedSizeImpl(Payloads, HeaderSize);
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
            return CreateSerializationInfoImpl(0, allowExternalDataChannel && AllowExternalDataChannel(), Payloads, HeaderSize);
        }

        ui8 GetVersion() const {
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
            if (!holder->HeaderStoredInline) {
                std::memset(holder->MutableHeaderData(), 0, TLatestScheme::HeaderSize);
            }
            holder->Version = TVersions::VersionCount;
            WriteUnaligned<ui8>(holder->MutableHeaderData(), holder->Version);
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
                Y_ENSURE(size, "Flat event header is missing");

                TString header = TString::Uninitialized(size);
                TRopeUtils::Memcpy(header.Detach(), iter, size);

                THolder<TEv> holder(new TEv());
                holder->AssignHeaderStorage(std::move(header));
                holder->Version = ValidateHeaderStorage(holder->HeaderData(), holder->HeaderSize);

                holder->DispatchCurrentVersion([&]<class TScheme>() {
                    Y_ENSURE(wirePayloads.size() == TScheme::PayloadFieldCount,
                        "Unexpected number of payloads " << wirePayloads.size() << " for flat event version " << holder->Version);
                    holder->Payloads.resize(TScheme::PayloadFieldCount);
                    for (size_t i = 0; i < TScheme::PayloadFieldCount; ++i) {
                        holder->Payloads[i] = std::move(wirePayloads[i]);
                    }
                    holder->template ValidateStateForScheme<TScheme>();
                });

                return holder.Release();
            } else {
                wirePayloads.push_back(input->GetRope());
            }

            Y_ENSURE(!wirePayloads.empty(), "Flat event payload set is empty");

            THolder<TEv> holder(new TEv());
            holder->AssignHeaderStorage(wirePayloads.front());
            holder->Version = ValidateHeaderStorage(holder->HeaderData(), holder->HeaderSize);

            holder->DispatchCurrentVersion([&]<class TScheme>() {
                Y_ENSURE(wirePayloads.size() == 1,
                    "Unexpected number of payload sections " << wirePayloads.size() << " for flat event version " << holder->Version);
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
        auto Bytes() {
            static_assert(TTag::Kind == EFieldKind::Bytes, "Bytes() is only available for bytes fields");
            return DispatchCurrentVersion([&]<class TScheme>() -> decltype(MakeFrontend<TScheme>().template Bytes<TTag>()) {
                if constexpr (TScheme::template HasPayloadField<TTag>) {
                    return MakeFrontend<TScheme>().template Bytes<TTag>();
                } else {
                    Y_ENSURE(false, "Field is absent in this event version");
                    return decltype(MakeFrontend<TScheme>().template Bytes<TTag>()){};
                }
            });
        }

        template <class TTag>
        auto Bytes() const {
            static_assert(TTag::Kind == EFieldKind::Bytes, "Bytes() is only available for bytes fields");
            return DispatchCurrentVersion([&]<class TScheme>() -> decltype(MakeFrontend<TScheme>().template Bytes<TTag>()) {
                if constexpr (TScheme::template HasPayloadField<TTag>) {
                    return MakeFrontend<TScheme>().template Bytes<TTag>();
                } else {
                    Y_ENSURE(false, "Field is absent in this event version");
                    return decltype(MakeFrontend<TScheme>().template Bytes<TTag>()){};
                }
            });
        }

        template <class TTag>
        auto Array() {
            static_assert(TTag::Kind == EFieldKind::Array, "Array() is only available for array fields");
            return DispatchCurrentVersion([&]<class TScheme>() -> decltype(MakeFrontend<TScheme>().template Array<TTag>()) {
                if constexpr (TScheme::template HasPayloadField<TTag>) {
                    return MakeFrontend<TScheme>().template Array<TTag>();
                } else {
                    Y_ENSURE(false, "Field is absent in this event version");
                    return decltype(MakeFrontend<TScheme>().template Array<TTag>()){};
                }
            });
        }

        template <class TTag>
        auto Array() const {
            static_assert(TTag::Kind == EFieldKind::Array, "Array() is only available for array fields");
            return DispatchCurrentVersion([&]<class TScheme>() -> decltype(MakeFrontend<TScheme>().template Array<TTag>()) {
                if constexpr (TScheme::template HasPayloadField<TTag>) {
                    return MakeFrontend<TScheme>().template Array<TTag>();
                } else {
                    Y_ENSURE(false, "Field is absent in this event version");
                    return decltype(MakeFrontend<TScheme>().template Array<TTag>()){};
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

        static ui8 ValidateHeaderStorage(const char* header, size_t size) {
            Y_ENSURE(size >= sizeof(ui8), "Flat event header is too short");
            return ReadUnaligned<ui8>(header);
        }

        template <class TScheme>
        void ValidateStateForScheme() const {
            Y_ENSURE(HeaderSize == TScheme::HeaderSize,
                "Unexpected flat event header size " << HeaderSize << ", expected " << TScheme::HeaderSize);
            Y_ENSURE(Payloads.empty() || Payloads.size() == TScheme::PayloadFieldCount,
                "Unexpected number of flat payload slots " << Payloads.size() << ", expected 0 or " << TScheme::PayloadFieldCount);

            TScheme::ForEachPayloadField([&]<class TField>() {
                constexpr size_t payloadIndex = TScheme::template GetPayloadIndex<TField>();
                const typename TScheme::TPayloadRef ref = GetPayloadRef<TScheme, TField>();
                static const TRope EmptyPayload;
                const TRope& payload = payloadIndex < Payloads.size() ? Payloads[payloadIndex] : EmptyPayload;

                if (ref.PayloadId == 0) {
                    Y_ENSURE(payload.GetSize() == 0, "Absent payload slot must be empty");
                } else {
                    Y_ENSURE(Payloads.size() == TScheme::PayloadFieldCount,
                        "Present payload requires allocated payload slots");
                    Y_ENSURE(ref.PayloadId == payloadIndex + 1,
                        "Unexpected payload id " << ref.PayloadId << " for slot " << payloadIndex);
                    if constexpr (TField::Kind == EFieldKind::Array) {
                        using TValue = typename TField::TValue;
                        Y_ENSURE(payload.GetSize() % sizeof(TValue) == 0,
                            "Array payload size mismatch, payload# " << payloadIndex
                            << " bytes# " << payload.GetSize() << " elemSize# " << sizeof(TValue));
                    }
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
        typename TScheme::TPayloadRef GetPayloadRef() const {
            return ReadUnaligned<typename TScheme::TPayloadRef>(HeaderData() + TScheme::template GetPayloadRefOffset<TTag>());
        }

        void AssertInitializedDebug() const {
            Y_DEBUG_ABORT_UNLESS(Version != 0);
            Y_DEBUG_ABORT_UNLESS(HeaderSize >= sizeof(ui8));
            Y_DEBUG_ABORT_UNLESS(ReadUnaligned<ui8>(HeaderData()) == Version);
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
        ui8 Version = 0;
        bool HeaderStoredInline = true;
    };

} // namespace NActors
