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
#include <cstdint>
#include <algorithm>

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

        template <class... T>
        struct TMaxHeaderSize;

        template <class T>
        struct TMaxHeaderSize<T> {
            static constexpr size_t Value = T::HeaderSize;
        };

        template <class T, class... TRest>
        struct TMaxHeaderSize<T, TRest...> {
            static constexpr size_t Tail = TMaxHeaderSize<TRest...>::Value;
            static constexpr size_t Value = T::HeaderSize > Tail ? T::HeaderSize : Tail;
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
        static constexpr char AlignedExtendedPayloadMarker = 0x08;

    public:
        static void operator delete(void* ptr) noexcept {
            ::operator delete(ptr);
        }

        static void operator delete(void* ptr, size_t) noexcept {
            ::operator delete(ptr);
        }

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
            static constexpr bool SupportsAlignedFastPath = alignof(T) <= 16;

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
                    if constexpr (SupportsAlignedFastPath) {
                        return AlignedData()[Index];
                    } else {
                        auto it = Payload->Position(Index * sizeof(T));
                        if (it.ContiguousSize() >= sizeof(T)) {
                            return ReadUnaligned<T>(it.ContiguousData());
                        }
                        T value;
                        TRopeUtils::Memcpy(reinterpret_cast<char*>(&value), it, sizeof(T));
                        return value;
                    }
                }

                template <bool Enabled = !IsConst>
                std::enable_if_t<Enabled> Write(T value) {
                    Y_ENSURE(Index < Size(), "Array index " << Index << " is out of range " << Size());
                    if constexpr (SupportsAlignedFastPath) {
                        MutableAlignedData()[Index] = value;
                    } else {
                        auto it = Payload->Position(Index * sizeof(T));
                        if (it.ContiguousSize() >= sizeof(T)) {
                            WriteUnaligned<T>(it.ContiguousDataMut(), value);
                        } else {
                            TRopeUtils::Memcpy(it, reinterpret_cast<const char*>(&value), sizeof(T));
                        }
                    }
                    TPayloadRef ref = ReadUnaligned<TPayloadRef>(RefPtr);
                    ref.PayloadId = Payload->GetSize() ? PayloadId : 0;
                    WriteUnaligned<TPayloadRef>(RefPtr, ref);
                }

                const T* AlignedData() const {
                    return TBasicArrayView::GetAlignedData(static_cast<const TRope&>(*Payload));
                }

                template <bool Enabled = !IsConst>
                std::enable_if_t<Enabled, T*> MutableAlignedData() {
                    return TBasicArrayView::GetAlignedData(*Payload);
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

            const T* Data() const {
                if constexpr (SupportsAlignedFastPath) {
                    return GetAlignedData(static_cast<const TRope&>(*Payload));
                } else {
                    static_assert(SupportsAlignedFastPath, "Data() is only available for aligned array payloads");
                    return nullptr;
                }
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
                if constexpr (SupportsAlignedFastPath) {
                    if (count) {
                        std::memcpy(dst, GetAlignedData(static_cast<const TRope&>(*Payload)), count * sizeof(T));
                    }
                } else {
                    auto it = Payload->Begin();
                    TRopeUtils::Memcpy(reinterpret_cast<char*>(dst), it, count * sizeof(T));
                }
            }

            template <class F>
            void ForEach(F&& f) const {
                if constexpr (SupportsAlignedFastPath) {
                    const size_t count = size();
                    const T* data = count ? GetAlignedData(static_cast<const TRope&>(*Payload)) : nullptr;
                    for (size_t i = 0; i < count; ++i) {
                        f(data[i]);
                    }
                } else {
                    for (size_t i = 0; i < size(); ++i) {
                        f(At(i));
                    }
                }
            }

            template <bool Enabled = !IsConst>
            std::enable_if_t<Enabled> Set(size_t index, T value) {
                (*this)[index] = value;
            }

            template <bool Enabled = !IsConst>
            std::enable_if_t<Enabled, T*> Data() {
                if constexpr (SupportsAlignedFastPath) {
                    return GetAlignedData(*Payload);
                } else {
                    static_assert(SupportsAlignedFastPath, "Data() is only available for aligned array payloads");
                    return nullptr;
                }
            }

            template <bool Enabled = !IsConst>
            std::enable_if_t<Enabled> Clear() {
                Payload->clear();
                SetRef({});
            }

            template <bool Enabled = !IsConst>
            std::enable_if_t<Enabled> CopyFrom(const T* src, size_t count) {
                if (!count) {
                    Clear();
                    return;
                }

                const size_t bytes = CheckedFieldBytes(count);
                TRcBuf data = TRcBuf(TRopeAlignedBuffer::Allocate(bytes));
                std::memcpy(data.GetDataMut(), src, bytes);
                *Payload = TRope(std::move(data));

                SetRef(TPayloadRef{
                    .PayloadId = static_cast<TPayloadId>(PayloadId),
                });
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
                const size_t currentBytes = CheckedFieldBytes(current);
                const size_t newBytes = CheckedFieldBytes(newSize);
                const size_t appendedBytes = CheckedFieldBytes(delta);
                TRcBuf data = TRcBuf(TRopeAlignedBuffer::Allocate(newBytes));
                char* dst = data.GetDataMut();
                if (currentBytes) {
                    auto it = Payload->Begin();
                    TRopeUtils::Memcpy(dst, it, currentBytes);
                }
                std::memset(dst + currentBytes, 0, appendedBytes);
                *Payload = TRope(std::move(data));

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

            static const T* GetAlignedData(const TRope& payload) {
                auto it = payload.Begin();
                if (!it.Valid()) {
                    return nullptr;
                }
                Y_DEBUG_ABORT_UNLESS(it.ContiguousSize() == payload.GetSize());
                auto* ptr = reinterpret_cast<const T*>(it.ContiguousData());
                Y_DEBUG_ABORT_UNLESS(reinterpret_cast<uintptr_t>(ptr) % alignof(T) == 0);
                return ptr;
            }

            template <bool Enabled = !IsConst>
            static std::enable_if_t<Enabled, T*> GetAlignedData(TRope& payload) {
                auto it = payload.Begin();
                if (!it.Valid()) {
                    return nullptr;
                }
                Y_DEBUG_ABORT_UNLESS(it.ContiguousSize() == payload.GetSize());
                auto* ptr = reinterpret_cast<T*>(it.ContiguousDataMut());
                Y_DEBUG_ABORT_UNLESS(reinterpret_cast<uintptr_t>(ptr) % alignof(T) == 0);
                return ptr;
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
            static constexpr size_t MaxHeaderSize = NFlatEventDetail::TMaxHeaderSize<TSchemes...>::Value;

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
            if (UseAlignedInlinePayloadFormat()) {
                return SerializeAlignedInlinePayloadToArcadiaStream(serializer);
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
            if (UseAlignedInlinePayloadFormat()) {
                return SerializeAlignedInlinePayloadToRope();
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
            if (UseAlignedInlinePayloadFormat()) {
                return CalculateAlignedInlinePayloadSerializedSize();
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
            if (UseAlignedInlinePayloadFormat()) {
                TEventSerializationInfo info;
                info.IsExtendedFormat = true;
                if (allowExternalDataChannel) {
                    info.Sections.push_back(TEventSectionInfo{0, CalculateAlignedInlinePayloadSerializedSize(), 0,
                        GetInlineBodyAlignment(), true, false});
                }
                return info;
            }

            TEventSerializationInfo info = CreateSerializationInfoImpl(0, allowExternalDataChannel && AllowExternalDataChannel(),
                Payloads, HeaderSize);
            if (!info.Sections.empty()) {
                DispatchCurrentVersion([&]<class TScheme>() {
                    size_t payloadIndex = 0;
                    TScheme::ForEachPayloadField([&]<class TField>() {
                        if constexpr (TField::Kind == EFieldKind::Array) {
                            info.Sections[1 + payloadIndex].Alignment = alignof(typename TField::TValue);
                        }
                        ++payloadIndex;
                    });
                });
            }
            return info;
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

            THolder<TEv> holder = MakeHolder();
            holder->ResetHeaderStorage(TLatestScheme::HeaderSize);
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
                if (iter.Valid() && *iter.ContiguousData() == AlignedExtendedPayloadMarker) {
                    ParseAlignedInlinePayload(iter, size, wirePayloads, totalPayloadSize);
                } else {
                    ParseExtendedFormatPayload(iter, size, wirePayloads, totalPayloadSize);
                }
                Y_ENSURE(size, "Flat event header is missing");

                THolder<TEv> holder = MakeHolder();
                holder->AssignHeaderStorage(iter, size);
                holder->Version = ValidateHeaderStorage(holder->HeaderData(), holder->HeaderSize);

                holder->DispatchCurrentVersion([&]<class TScheme>() {
                    Y_ENSURE(wirePayloads.size() == TScheme::PayloadFieldCount,
                        "Unexpected number of payloads " << wirePayloads.size() << " for flat event version " << holder->Version);
                    holder->Payloads.resize(TScheme::PayloadFieldCount);
                    for (size_t i = 0; i < TScheme::PayloadFieldCount; ++i) {
                        holder->Payloads[i] = std::move(wirePayloads[i]);
                    }
                    holder->template ValidateStateForScheme<TScheme>();
                    holder->template NormalizeArrayPayloadsForScheme<TScheme>();
                });

                return holder.Release();
            } else {
                wirePayloads.push_back(input->GetRope());
            }

            Y_ENSURE(!wirePayloads.empty(), "Flat event payload set is empty");

            THolder<TEv> holder = MakeHolder();
            holder->AssignHeaderStorage(wirePayloads.front());
            holder->Version = ValidateHeaderStorage(holder->HeaderData(), holder->HeaderSize);

            holder->DispatchCurrentVersion([&]<class TScheme>() {
                Y_ENSURE(wirePayloads.size() == 1,
                    "Unexpected number of payload sections " << wirePayloads.size() << " for flat event version " << holder->Version);
                holder->template ValidateStateForScheme<TScheme>();
                holder->template NormalizeArrayPayloadsForScheme<TScheme>();
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
        template <class TValue>
        static size_t CheckedFieldBytes(size_t count) {
            Y_ENSURE(count <= std::numeric_limits<size_t>::max() / sizeof(TValue), "Flat event field is too large");
            return count * sizeof(TValue);
        }

        static THolder<TEv> MakeHolder() {
            using TVersions = typename TEv::TScheme;
            constexpr size_t headerCapacity = TVersions::MaxHeaderSize;

            void* mem = ::operator new(sizeof(TEv) + headerCapacity);
            THolder<TEv> holder(new (mem) TEv());
            holder->BindHeaderStorage(reinterpret_cast<char*>(holder.Get()) + sizeof(TEv), headerCapacity);
            return holder;
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

        template <class TScheme>
        void NormalizeArrayPayloadsForScheme() {
            TScheme::ForEachPayloadField([&]<class TField>() {
                if constexpr (TField::Kind == EFieldKind::Array) {
                    using TValue = typename TField::TValue;
                    if constexpr (alignof(TValue) <= 16) {
                        constexpr size_t payloadIndex = TScheme::template GetPayloadIndex<TField>();
                        TRope& payload = Payloads[payloadIndex];
                        const size_t bytes = payload.GetSize();
                        if (!bytes) {
                            return;
                        }

                        auto it = payload.Begin();
                        const bool isContiguous = it.Valid() && it.ContiguousSize() == bytes;
                        const bool isAligned = isContiguous
                            && reinterpret_cast<uintptr_t>(it.ContiguousData()) % alignof(TValue) == 0;
                        if (isContiguous && isAligned) {
                            return;
                        }

                        TRcBuf data = TRcBuf(TRopeAlignedBuffer::Allocate(bytes));
                        auto src = payload.Begin();
                        TRopeUtils::Memcpy(data.GetDataMut(), src, bytes);
                        payload = TRope(std::move(data));
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

        size_t GetInlineBodyAlignment() const {
            if (!HasPayloads()) {
                return 1;
            }

            return DispatchCurrentVersion([&]<class TScheme>() -> size_t {
                size_t alignment = 1;
                TScheme::ForEachPayloadField([&]<class TField>() {
                    if constexpr (TField::Kind == EFieldKind::Array) {
                        alignment = std::max(alignment, alignof(typename TField::TValue));
                    }
                });
                return alignment;
            });
        }

        bool UseAlignedInlinePayloadFormat() const {
            return HasPayloads() && !AllowExternalDataChannel() && GetInlineBodyAlignment() > 1;
        }

        static size_t SerializeVarint(size_t num, char* buffer) {
            char* begin = buffer;
            do {
                *buffer++ = (num & 0x7F) | (num >= 128 ? 0x80 : 0x00);
                num >>= 7;
            } while (num);
            return buffer - begin;
        }

        static size_t DeserializeVarint(TRope::TConstIterator& iter, size_t& size) {
            size_t res = 0;
            size_t offset = 0;
            for (;;) {
                if (!iter.Valid()) {
                    return Max<size_t>();
                }
                const char byte = *iter.ContiguousData();
                iter += 1;
                --size;
                res |= (static_cast<size_t>(byte) & 0x7F) << offset;
                offset += 7;
                if (!(byte & 0x80)) {
                    break;
                }
            }
            return res;
        }

        struct TAlignedInlineLayout {
            ui32 HeaderSize = 0;
            ui32 TotalSize = 0;
            TVector<size_t> Paddings;
        };

        static constexpr size_t MaxInlinePayloadPadding = Max<ui8>();

        size_t GetPayloadAlignment(size_t payloadIndex) const {
            return DispatchCurrentVersion([&]<class TScheme>() -> size_t {
                size_t current = 0;
                size_t alignment = 1;
                TScheme::ForEachPayloadField([&]<class TField>() {
                    if (current == payloadIndex) {
                        if constexpr (TField::Kind == EFieldKind::Array) {
                            alignment = alignof(typename TField::TValue);
                        }
                    }
                    ++current;
                });
                return alignment;
            });
        }

        TAlignedInlineLayout BuildAlignedInlineLayout() const {
            char buf[MaxNumberBytes];

            TAlignedInlineLayout layout;
            layout.Paddings.yresize(Payloads.size());

            ui32 headerSize = 1 + SerializeVarint(Payloads.size(), buf);
            for (const TRope& rope : Payloads) {
                headerSize += SerializeVarint(rope.GetSize(), buf);
                headerSize += sizeof(ui8);
            }

            size_t offset = headerSize;
            for (size_t i = 0; i < Payloads.size(); ++i) {
                const size_t alignment = GetPayloadAlignment(i);
                const size_t padding = alignment <= 1 ? 0 : (alignment - offset % alignment) % alignment;
                Y_ENSURE(padding <= MaxInlinePayloadPadding,
                    "aligned flat event padding too large padding# " << padding << " alignment# " << alignment);
                layout.Paddings[i] = padding;
                offset += padding + Payloads[i].GetSize();
            }

            layout.HeaderSize = headerSize;
            layout.TotalSize = offset + HeaderSize;
            return layout;
        }

        ui32 CalculateAlignedInlinePayloadSerializedSize() const {
            return BuildAlignedInlineLayout().TotalSize;
        }

        bool SerializeAlignedInlinePayloadToArcadiaStream(TChunkSerializer* serializer) const {
            const TAlignedInlineLayout layout = BuildAlignedInlineLayout();
            TString framing = TString::Uninitialized(layout.HeaderSize);
            char* data = framing.Detach();
            *data++ = AlignedExtendedPayloadMarker;
            data += SerializeVarint(Payloads.size(), data);
            for (size_t i = 0; i < Payloads.size(); ++i) {
                data += SerializeVarint(Payloads[i].GetSize(), data);
                *data++ = static_cast<ui8>(layout.Paddings[i]);
            }

            if (!serializer->WriteString(&framing)) {
                return false;
            }
            for (size_t i = 0; i < Payloads.size(); ++i) {
                const size_t padding = layout.Paddings[i];
                if (padding) {
                    TString zeros = TString::Uninitialized(padding);
                    std::memset(zeros.Detach(), 0, padding);
                    if (!serializer->WriteString(&zeros)) {
                        return false;
                    }
                }
                TRope rope(Payloads[i]);
                if (!serializer->WriteRope(&rope)) {
                    return false;
                }
            }
            return SerializeHeaderOnlyToArcadiaStream(serializer);
        }

        std::optional<TRope> SerializeAlignedInlinePayloadToRope() const {
            const TAlignedInlineLayout layout = BuildAlignedInlineLayout();
            TRcBuf body = TRcBuf(TRopeAlignedBuffer::Allocate(layout.TotalSize));
            char* data = body.GetDataMut();
            *data++ = AlignedExtendedPayloadMarker;
            data += SerializeVarint(Payloads.size(), data);
            for (size_t i = 0; i < Payloads.size(); ++i) {
                data += SerializeVarint(Payloads[i].GetSize(), data);
                *data++ = static_cast<ui8>(layout.Paddings[i]);
            }
            for (size_t i = 0; i < Payloads.size(); ++i) {
                const size_t padding = layout.Paddings[i];
                if (padding) {
                    std::memset(data, 0, padding);
                    data += padding;
                }
                auto it = Payloads[i].Begin();
                TRopeUtils::Memcpy(data, it, Payloads[i].GetSize());
                data += Payloads[i].GetSize();
            }
            if (HeaderSize) {
                std::memcpy(data, HeaderData(), HeaderSize);
            }
            return TRope(std::move(body));
        }

        static void ParseAlignedInlinePayload(TRope::TConstIterator& iter, size_t& size, TVector<TRope>& payload,
                size_t& totalPayloadSize) {
            Y_ENSURE(iter.Valid() && *iter.ContiguousData() == AlignedExtendedPayloadMarker, "invalid aligned flat event");

            auto fetchRope = [&](size_t len) {
                TRope::TConstIterator begin = iter;
                iter += len;
                size -= len;
                payload.emplace_back(begin, iter);
                totalPayloadSize += len;
            };

            iter += 1;
            --size;

            const size_t numRopes = DeserializeVarint(iter, size);
            if (numRopes == Max<size_t>()) {
                Y_ENSURE(false, "invalid aligned flat event");
            }

            TStackVec<std::pair<size_t, size_t>, 16> ropeMeta;
            ropeMeta.reserve(numRopes);
            for (size_t i = 0; i < numRopes; ++i) {
                const size_t len = DeserializeVarint(iter, size);
                const size_t padding = size ? static_cast<ui8>(*iter.ContiguousData()) : Max<size_t>();
                if (size) {
                    iter += 1;
                    --size;
                }
                if (len == Max<size_t>() || padding == Max<size_t>() || size < len + padding) {
                    Y_ENSURE(false, "invalid aligned flat event len# " << len << " padding# " << padding << " size# " << size);
                }
                ropeMeta.emplace_back(len, padding);
            }

            for (const auto& [len, padding] : ropeMeta) {
                iter += padding;
                size -= padding;
                fetchRope(len);
            }
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

        void BindHeaderStorage(char* storage, size_t capacity) {
            HeaderStorage = storage;
            HeaderCapacity = capacity;
        }

        void ResetHeaderStorage(size_t size) {
            Y_DEBUG_ABORT_UNLESS(size <= HeaderCapacity);
            HeaderSize = size;
        }

        void AssignHeaderStorage(TString&& header) {
            const size_t size = header.size();
            Y_ENSURE(size <= HeaderCapacity,
                "Flat event header is too large " << size << ", capacity " << HeaderCapacity);
            HeaderSize = size;
            if (size) {
                std::memcpy(HeaderStorage, header.data(), size);
            }
        }

        void AssignHeaderStorage(const TRope& header) {
            const size_t size = header.GetSize();
            Y_ENSURE(size <= HeaderCapacity,
                "Flat event header is too large " << size << ", capacity " << HeaderCapacity);
            HeaderSize = size;
            if (size) {
                auto it = header.Begin();
                TRopeUtils::Memcpy(HeaderStorage, it, size);
            }
        }

        void AssignHeaderStorage(TRope::TConstIterator iter, size_t size) {
            Y_ENSURE(size <= HeaderCapacity,
                "Flat event header is too large " << size << ", capacity " << HeaderCapacity);
            HeaderSize = size;
            if (size) {
                TRopeUtils::Memcpy(HeaderStorage, iter, size);
            }
        }

        char* MutableHeaderData() {
            return HeaderStorage;
        }

        const char* HeaderData() const {
            return HeaderStorage;
        }

    private:
        char* HeaderStorage = nullptr;
        TVector<TRope> Payloads;
        size_t HeaderSize = 0;
        size_t HeaderCapacity = 0;
        ui8 Version = 0;
    };

} // namespace NActors
