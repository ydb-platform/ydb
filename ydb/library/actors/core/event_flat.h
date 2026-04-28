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
#include <climits>
#include <algorithm>
#include <array>
#include <optional>

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
        struct TIsVersionOption : std::false_type {
        };

        template <class T>
        struct TIsVersionOption<T, std::void_t<decltype(T::IsVersionOption)>>
            : std::bool_constant<T::IsVersionOption> {
        };

        template <class... T>
        struct TCountVersionSchemes;

        template <>
        struct TCountVersionSchemes<> {
            static constexpr size_t Value = 0;
        };

        template <class T, class... TRest>
        struct TCountVersionSchemes<T, TRest...> {
            static constexpr size_t Value = TCountVersionSchemes<TRest...>::Value + (TIsVersionOption<T>::value ? 0 : 1);
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

        template <>
        struct TLastType<> {
            using Type = void;
        };

        template <class T>
        struct TLastType<T> {
            using Type = std::conditional_t<TIsVersionOption<T>::value, void, T>;
        };

        template <class T, class... TRest>
        struct TLastType<T, TRest...> {
            using TTail = typename TLastType<TRest...>::Type;
            using Type = std::conditional_t<std::is_void_v<TTail>,
                std::conditional_t<TIsVersionOption<T>::value, void, T>,
                TTail>;
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

        template <>
        struct TMaxHeaderSize<> {
            static constexpr size_t Value = 0;
        };

        template <class T, bool IsVersionOption>
        struct TMaxHeaderSizeItem;

        template <class T>
        struct TMaxHeaderSizeItem<T, false> {
            static constexpr size_t Value = T::HeaderSize;
        };

        template <class T>
        struct TMaxHeaderSizeItem<T, true> {
            static constexpr size_t Value = 0;
        };

        template <class T>
        struct TMaxHeaderSize<T> {
            static constexpr size_t Value = TMaxHeaderSizeItem<T, TIsVersionOption<T>::value>::Value;
        };

        template <class T, class... TRest>
        struct TMaxHeaderSize<T, TRest...> {
            static constexpr size_t Tail = TMaxHeaderSize<TRest...>::Value;
            static constexpr size_t Head = TMaxHeaderSizeItem<T, TIsVersionOption<T>::value>::Value;
            static constexpr size_t Value = Head > Tail ? Head : Tail;
        };

    } // namespace NFlatEventDetail

    /**
     * TEventFlat defines a flat binary storage format for actor events.
     *
     * Storage model:
     * - Fixed fields live inline inside a contiguous header buffer.
     * - BytesField payloads live in TRope payload sections.
     * - ArrayField payloads live in contiguous aligned buffers and are copied from/to
     *   payload sections at deserialization/serialization boundaries.
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
    class TEventFlatLayout {
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

        struct WithStrictScheme {
            static constexpr bool IsSchemeOption = true;
        };

        struct WithEmptyVersion {
            static constexpr bool IsVersionOption = true;
        };

    public:
        enum class EFieldKind {
            Fixed,
            Bytes,
            Array,
            InlineBytes,
            InlineArray,
        };
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

        template <size_t MaxInlineSize, size_t TagId>
        struct InlineBytesField {
            static_assert(MaxInlineSize <= std::numeric_limits<ui16>::max(), "InlineBytesField size exceeds ui16");

            using TValue = char;
            static constexpr size_t Id = TagId;
            static constexpr size_t InlineSize = MaxInlineSize;
            static constexpr size_t StorageSize = sizeof(ui16) + MaxInlineSize;
            static constexpr EFieldKind Kind = EFieldKind::InlineBytes;
            static constexpr bool IsFixed = false;
            static constexpr bool IsPayload = false;
        };

        template <class T, size_t MaxInlineElements, size_t TagId>
        struct InlineArrayField {
            static_assert(std::is_trivially_copyable_v<T>, "InlineArrayField requires trivially copyable type");
            static_assert(MaxInlineElements <= std::numeric_limits<ui16>::max(), "InlineArrayField size exceeds ui16");

            using TValue = T;
            static constexpr size_t Id = TagId;
            static constexpr size_t MaxElements = MaxInlineElements;
            static constexpr size_t StorageSize = sizeof(ui16) + MaxInlineElements * sizeof(T);
            static constexpr EFieldKind Kind = EFieldKind::InlineArray;
            static constexpr bool IsFixed = false;
            static constexpr bool IsPayload = false;
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

        template <size_t MaxInlineSize, bool IsConst>
        class TBasicInlineBytesView {
            using TPointer = std::conditional_t<IsConst, const char*, char*>;

        public:
            TBasicInlineBytesView() = default;

            explicit TBasicInlineBytesView(TPointer ptr)
                : Ptr(ptr)
            {}

            size_t size() const {
                return ReadUnaligned<ui16>(Ptr);
            }

            bool empty() const {
                return size() == 0;
            }

            bool HasPayload() const {
                return false;
            }

            TString Materialize() const {
                return TString(Data(), size());
            }

            TRope Rope() const {
                return TRope(Materialize());
            }

            template <bool Enabled = !IsConst>
            std::enable_if_t<Enabled> Clear() {
                WriteUnaligned<ui16>(Ptr, 0);
            }

            template <bool Enabled = !IsConst>
            std::enable_if_t<Enabled> Set(TRope data) {
                const size_t bytes = data.GetSize();
                Y_ENSURE(bytes <= MaxInlineSize, "Inline bytes field overflow size# " << bytes << " capacity# " << MaxInlineSize);
                WriteUnaligned<ui16>(Ptr, static_cast<ui16>(bytes));
                if (bytes) {
                    auto it = data.Begin();
                    TRopeUtils::Memcpy(MutableData(), it, bytes);
                }
            }

            template <bool Enabled = !IsConst>
            std::enable_if_t<Enabled> Append(TRope data) {
                const size_t oldSize = size();
                const size_t bytes = data.GetSize();
                Y_ENSURE(oldSize + bytes <= MaxInlineSize,
                    "Inline bytes field overflow size# " << oldSize + bytes << " capacity# " << MaxInlineSize);
                if (bytes) {
                    auto it = data.Begin();
                    TRopeUtils::Memcpy(MutableData() + oldSize, it, bytes);
                }
                WriteUnaligned<ui16>(Ptr, static_cast<ui16>(oldSize + bytes));
            }

        private:
            const char* Data() const {
                return Ptr + sizeof(ui16);
            }

            template <bool Enabled = !IsConst>
            std::enable_if_t<Enabled, char*> MutableData() {
                return Ptr + sizeof(ui16);
            }

        private:
            TPointer Ptr = nullptr;
        };

        template <size_t MaxInlineSize>
        using TInlineBytesView = TBasicInlineBytesView<MaxInlineSize, false>;

        template <size_t MaxInlineSize>
        using TConstInlineBytesView = TBasicInlineBytesView<MaxInlineSize, true>;

        template <class T, size_t MaxInlineElements, bool IsConst>
        class TBasicInlineArrayView {
            static_assert(std::is_trivially_copyable_v<T>, "Inline array view requires trivially copyable type");
            using TPointer = std::conditional_t<IsConst, const char*, char*>;

            class TElementView {
            public:
                TElementView(TPointer ptr, size_t index)
                    : Ptr(ptr)
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
                T Read() const {
                    return ReadUnaligned<T>(Ptr + sizeof(ui16) + Index * sizeof(T));
                }

                template <bool Enabled = !IsConst>
                std::enable_if_t<Enabled> Write(T value) {
                    WriteUnaligned<T>(Ptr + sizeof(ui16) + Index * sizeof(T), value);
                }

            private:
                TPointer Ptr = nullptr;
                size_t Index = 0;
            };

        public:
            TBasicInlineArrayView() = default;

            explicit TBasicInlineArrayView(TPointer ptr)
                : Ptr(ptr)
            {}

            size_t size() const {
                return ReadUnaligned<ui16>(Ptr);
            }

            bool empty() const {
                return size() == 0;
            }

            template <bool Enabled = !IsConst>
            std::enable_if_t<Enabled, TElementView> operator[](size_t index) {
                Y_ENSURE(index < size(), "Array index " << index << " is out of range " << size());
                return TElementView(Ptr, index);
            }

            T operator[](size_t index) const {
                Y_ENSURE(index < size(), "Array index " << index << " is out of range " << size());
                return ReadUnaligned<T>(Ptr + sizeof(ui16) + index * sizeof(T));
            }

            T At(size_t index) const {
                return (*this)[index];
            }

            T Get(size_t index) const {
                return At(index);
            }

            void CopyTo(T* dst, size_t count) const {
                Y_ENSURE(count == size(), "Unexpected array size " << count << ", expected " << size());
                if (count) {
                    std::memcpy(dst, Ptr + sizeof(ui16), count * sizeof(T));
                }
            }

            template <class F>
            void ForEach(F&& f) const {
                for (size_t i = 0; i < size(); ++i) {
                    f((*this)[i]);
                }
            }

            template <bool Enabled = !IsConst>
            std::enable_if_t<Enabled> Set(size_t index, T value) {
                (*this)[index] = value;
            }

            template <bool Enabled = !IsConst>
            std::enable_if_t<Enabled> Clear() {
                WriteUnaligned<ui16>(Ptr, 0);
            }

            template <bool Enabled = !IsConst>
            std::enable_if_t<Enabled> CopyFrom(const T* src, size_t count) {
                Y_ENSURE(count <= MaxInlineElements,
                    "Inline array field overflow size# " << count << " capacity# " << MaxInlineElements);
                WriteUnaligned<ui16>(Ptr, static_cast<ui16>(count));
                if (count) {
                    std::memcpy(Ptr + sizeof(ui16), src, count * sizeof(T));
                }
            }

            template <bool Enabled = !IsConst>
            std::enable_if_t<Enabled, T*> Init(size_t newSize) {
                Y_ENSURE(newSize <= MaxInlineElements,
                    "Inline array field overflow size# " << newSize << " capacity# " << MaxInlineElements);
                WriteUnaligned<ui16>(Ptr, static_cast<ui16>(newSize));
                return nullptr;
            }

            template <bool Enabled = !IsConst>
            std::enable_if_t<Enabled> Resize(size_t newSize) {
                const size_t current = size();
                Y_ENSURE(newSize <= MaxInlineElements,
                    "Inline array field overflow size# " << newSize << " capacity# " << MaxInlineElements);
                if (newSize > current) {
                    std::memset(Ptr + sizeof(ui16) + current * sizeof(T), 0, (newSize - current) * sizeof(T));
                }
                WriteUnaligned<ui16>(Ptr, static_cast<ui16>(newSize));
            }

        private:
            TPointer Ptr = nullptr;
        };

        template <class T, size_t MaxInlineElements>
        using TInlineArrayView = TBasicInlineArrayView<T, MaxInlineElements, false>;

        template <class T, size_t MaxInlineElements>
        using TConstInlineArrayView = TBasicInlineArrayView<T, MaxInlineElements, true>;

        template <class T, class TPayloadId, bool IsConst, class TPayload = TRcBuf>
        class TBasicArrayView {
            static_assert(std::is_trivially_copyable_v<T>, "Array view requires trivially copyable type");
            using TPayloadRef = TPayloadRefBase<TPayloadId>;
            static constexpr bool SupportsAlignedFastPath = alignof(T) <= 16;

            using TPayloadPtr = std::conditional_t<IsConst, const TPayload*, TPayload*>;
            using TPointer = std::conditional_t<IsConst, const char*, char*>;

            class TElementView {
            public:
                TElementView(TPayloadPtr payload, TPointer refPtr, TPayloadId payloadId, size_t index)
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
                        return ReadUnaligned<T>(Payload->GetData() + Index * sizeof(T));
                    }
                }

                template <bool Enabled = !IsConst>
                std::enable_if_t<Enabled> Write(T value) {
                    Y_ENSURE(Index < Size(), "Array index " << Index << " is out of range " << Size());
                    if constexpr (SupportsAlignedFastPath) {
                        MutableAlignedData()[Index] = value;
                    } else {
                        WriteUnaligned<T>(Payload->GetDataMut() + Index * sizeof(T), value);
                    }
                    TPayloadRef ref = ReadUnaligned<TPayloadRef>(RefPtr);
                    ref.PayloadId = Payload->GetSize() ? PayloadId : 0;
                    WriteUnaligned<TPayloadRef>(RefPtr, ref);
                }

                const T* AlignedData() const {
                    return TBasicArrayView::GetAlignedData(*Payload);
                }

                template <bool Enabled = !IsConst>
                std::enable_if_t<Enabled, T*> MutableAlignedData() {
                    return TBasicArrayView::GetAlignedData(*Payload);
                }

            private:
                TPayloadPtr Payload = nullptr;
                TPointer RefPtr = nullptr;
                TPayloadId PayloadId = 0;
                size_t Index = 0;
            };

        public:
            TBasicArrayView() = default;

            TBasicArrayView(TPayloadPtr payload, TPointer refPtr, TPayloadId payloadId)
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
                    return GetAlignedData(*Payload);
                } else {
                    static_assert(SupportsAlignedFastPath, "Data() is only available for aligned array payloads");
                    return nullptr;
                }
            }

            template <bool Enabled = !IsConst>
            auto operator[](size_t index) -> std::enable_if_t<Enabled, std::conditional_t<SupportsAlignedFastPath, T&, TElementView>> {
                if constexpr (SupportsAlignedFastPath) {
                    return this->MutableAlignedData()[index];
                } else {
                    return TElementView(Payload, RefPtr, PayloadId, index);
                }
            }

            auto operator[](size_t index) const -> std::conditional_t<SupportsAlignedFastPath, const T&, TElementView> {
                if constexpr (SupportsAlignedFastPath) {
                    return this->AlignedData()[index];
                } else {
                    return TElementView(Payload, RefPtr, PayloadId, index);
                }
            }

            T At(size_t index) const {
                if constexpr (SupportsAlignedFastPath) {
                    return (*this)[index];
                } else {
                    return TElementView(Payload, RefPtr, PayloadId, index);
                }
            }

            T Get(size_t index) const {
                return At(index);
            }

            void CopyTo(T* dst, size_t count) const {
                Y_ENSURE(count == size(), "Unexpected array size " << count << ", expected " << size());
                if (count) {
                    std::memcpy(dst, Payload->GetData(), count * sizeof(T));
                }
            }

            template <class F>
            void ForEach(F&& f) const {
                if constexpr (SupportsAlignedFastPath) {
                    const size_t count = size();
                    const T* data = count ? GetAlignedData(*Payload) : nullptr;
                    for (size_t i = 0; i < count; ++i) {
                        f(data[i]);
                    }
                } else {
                    const size_t count = size();
                    const char* data = Payload->GetData();
                    for (size_t i = 0; i < count; ++i) {
                        f(ReadUnaligned<T>(data + i * sizeof(T)));
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
                *Payload = {};
                SetRef({});
            }

            template <bool Enabled = !IsConst>
            std::enable_if_t<Enabled> CopyFrom(const T* src, size_t count) {
                if (!count) {
                    Clear();
                    return;
                }

                const size_t bytes = CheckedFieldBytes(count);
                Payload->Allocate(bytes);
                std::memcpy(Payload->GetDataMut(), src, bytes);

                SetRef(TPayloadRef{
                    .PayloadId = static_cast<TPayloadId>(PayloadId),
                });
            }

            template <bool Enabled = !IsConst>
            std::enable_if_t<Enabled, T*> Init(size_t newSize) {
                Y_ENSURE(empty(), "Init is only supported for empty flat array payloads");
                if (!newSize) {
                    Clear();
                    return nullptr;
                }

                const size_t newBytes = CheckedFieldBytes(newSize);
                Payload->Allocate(newBytes);

                SetRef(TPayloadRef{
                    .PayloadId = static_cast<TPayloadId>(PayloadId),
                });

                if constexpr (SupportsAlignedFastPath) {
                    return MutableAlignedData();
                } else {
                    return nullptr;
                }
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
                TPayload oldPayload = *Payload;
                Payload->Allocate(newBytes);
                char* dst = Payload->GetDataMut();
                if (currentBytes) {
                    std::memcpy(dst, oldPayload.GetData(), currentBytes);
                }
                std::memset(dst + currentBytes, 0, appendedBytes);

                SetRef(TPayloadRef{
                    .PayloadId = static_cast<TPayloadId>(PayloadId),
                });
            }

        private:
            static size_t CheckedFieldBytes(size_t count) {
                Y_ENSURE(count <= std::numeric_limits<size_t>::max() / sizeof(T), "Array field is too large");
                return count * sizeof(T);
            }

        public:
            static const T* GetDirectData(const TPayload& payload) {
                if constexpr (SupportsAlignedFastPath) {
                    return GetAlignedData(payload);
                } else {
                    return nullptr;
                }
            }

            template <bool Enabled = !IsConst>
            static std::enable_if_t<Enabled, T*> GetDirectData(TPayload& payload) {
                if constexpr (SupportsAlignedFastPath) {
                    return GetAlignedData(payload);
                } else {
                    return nullptr;
                }
            }

        private:
            TPayloadRef GetRef() const {
                return ReadUnaligned<TPayloadRef>(RefPtr);
            }

            template <bool Enabled = !IsConst>
            std::enable_if_t<Enabled> SetRef(TPayloadRef ref) {
                WriteUnaligned<TPayloadRef>(RefPtr, ref);
            }

            static const T* GetAlignedData(const TPayload& payload) {
                const char* data = payload.GetData();
                if (!data) {
                    return nullptr;
                }
                auto* ptr = reinterpret_cast<const T*>(data);
                Y_DEBUG_ABORT_UNLESS(reinterpret_cast<uintptr_t>(ptr) % alignof(T) == 0);
                return ptr;
            }

            template <bool Enabled = !IsConst>
            static std::enable_if_t<Enabled, T*> GetAlignedData(TPayload& payload) {
                char* data = payload.GetDataMut();
                if (!data) {
                    return nullptr;
                }
                auto* ptr = reinterpret_cast<T*>(data);
                Y_DEBUG_ABORT_UNLESS(reinterpret_cast<uintptr_t>(ptr) % alignof(T) == 0);
                return ptr;
            }

            template <bool Enabled = !IsConst>
            std::enable_if_t<Enabled, T*> MutableAlignedData() {
                return GetAlignedData(*Payload);
            }

            const T* AlignedData() const {
                return GetAlignedData(*Payload);
            }

        private:
            TPayloadPtr Payload = nullptr;
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
            static constexpr bool IsStrict = (std::is_same_v<TItems, WithStrictScheme> || ...);

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
                static constexpr bool IsBytesPayload = TItem::IsPayload && TItem::Kind == EFieldKind::Bytes;

                static consteval size_t GetStoredSize() {
                    if constexpr (TItem::Kind == EFieldKind::Fixed) {
                        return sizeof(typename TItem::TValue);
                    } else if constexpr (TItem::Kind == EFieldKind::InlineBytes || TItem::Kind == EFieldKind::InlineArray) {
                        return TItem::StorageSize;
                    } else {
                        return sizeof(TPayloadRef);
                    }
                }

                static constexpr size_t StoredSize = GetStoredSize();
            };

            template <class TItem>
            struct TItemTraits<TItem, true> {
                static constexpr bool IsField = false;
                static constexpr bool IsFixed = false;
                static constexpr bool IsPayload = false;
                static constexpr bool IsBytesPayload = false;
                static constexpr size_t StoredSize = 0;
            };

        public:
            static_assert(NFlatEventDetail::TAreDistinctFields<TItems...>::value, "Duplicate field tag in flat event scheme");
            static constexpr size_t FieldCount = (0u + ... + (TItemTraits<TItems>::IsField ? 1u : 0u));
            static constexpr size_t PayloadFieldCount = (0u + ... + (TItemTraits<TItems>::IsPayload ? 1u : 0u));
            static constexpr size_t BytesPayloadFieldCount = (0u + ... + (TItemTraits<TItems>::IsBytesPayload ? 1u : 0u));
            static constexpr bool HasOnlyBytesPayloadFields = PayloadFieldCount > 0 && PayloadFieldCount == BytesPayloadFieldCount;
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
            static consteval size_t GetFieldOffset() {
                static_assert(HasField<TTag>, "Field is absent in this scheme");
                return GetFieldOffsetImpl<TTag, TItems...>();
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
            using TLatestScheme = typename NFlatEventDetail::TLastType<TSchemes...>::Type;
            static constexpr size_t VersionCount = NFlatEventDetail::TCountVersionSchemes<TSchemes...>::Value;
            static_assert(VersionCount <= std::numeric_limits<ui8>::max(), "Too many flat event versions");
            static constexpr size_t MaxHeaderSize = NFlatEventDetail::TMaxHeaderSize<TSchemes...>::Value;
            static constexpr bool IsEmpty = VersionCount == 0;
            static constexpr bool HasEmptyVersion = IsEmpty || (std::is_same_v<TSchemes, WithEmptyVersion> || ...);

            template <class TScheme>
            static constexpr bool HasScheme = ((!NFlatEventDetail::TIsVersionOption<TSchemes>::value && std::is_same_v<TScheme, TSchemes>) || ...);

            template <class TScheme>
            static consteval ui8 GetVersion() {
                static_assert(!IsEmpty, "Empty flat event has no scheme versions");
                static_assert(HasScheme<TScheme>, "Scheme does not belong to this flat event version set");
                return GetVersionImpl<TScheme, 1, TSchemes...>();
            }

            template <class F>
            static decltype(auto) Dispatch(ui8 version, F&& f) {
                if constexpr (IsEmpty) {
                    Y_UNUSED(version);
                    Y_UNUSED(f);
                    ythrow TWithBackTrace<yexception>() << "Empty flat event has no scheme versions";
                } else {
                    return DispatchImpl<1, TSchemes...>(version, std::forward<F>(f));
                }
            }

        private:
            template <class TScheme, ui8 Version>
            static consteval ui8 GetVersionImpl() {
                static_assert(Version == 0, "Scheme does not belong to this flat event version set");
                return 0;
            }

            template <class TScheme, ui8 Version, class TCurrent, class... TRest>
            static consteval ui8 GetVersionImpl() {
                if constexpr (NFlatEventDetail::TIsVersionOption<TCurrent>::value) {
                    return GetVersionImpl<TScheme, Version, TRest...>();
                } else if constexpr (std::is_same_v<TScheme, TCurrent>) {
                    return Version;
                } else {
                    return GetVersionImpl<TScheme, static_cast<ui8>(Version + 1), TRest...>();
                }
            }

            template <ui8 Version, class F>
            static decltype(auto) DispatchImpl(ui8 version, F&& f) {
                Y_UNUSED(f);
                ythrow TWithBackTrace<yexception>() << "Unsupported flat event version " << version;
            }

            template <ui8 Version, class TScheme, class... TRest, class F>
            static decltype(auto) DispatchImpl(ui8 version, F&& f) {
                if constexpr (NFlatEventDetail::TIsVersionOption<TScheme>::value) {
                    return DispatchImpl<Version, TRest...>(version, std::forward<F>(f));
                } else if (version == Version) {
                    return f.template operator()<TScheme>();
                } else if constexpr (sizeof...(TRest) > 0) {
                    return DispatchImpl<static_cast<ui8>(Version + 1), TRest...>(version, std::forward<F>(f));
                } else {
                    ythrow TWithBackTrace<yexception>() << "Unsupported flat event version " << version;
                }
            }
        };
    };

    template <class TEv, class TEventScheme>
    class TEventFlat : public IEventBase {
        static_assert(std::is_class_v<TEv>, "TEv must be a class");
        using TLayout = TEventFlatLayout;
        using TVersions = typename TEventScheme::TVersions;
        static constexpr bool IsEmptyEvent = TVersions::VersionCount == 0;
        static constexpr bool HasEmptyVersion = TVersions::HasEmptyVersion;

    public:
        static void operator delete(void* ptr) noexcept {
            ::operator delete(ptr);
        }

        static void operator delete(void* ptr, size_t) noexcept {
            ::operator delete(ptr);
        }

        template <class TPayloadId>
        using TPayloadRefBase = typename TLayout::template TPayloadRefBase<TPayloadId>;

        template <class TPayloadId>
        using WithPayloadType = typename TLayout::template WithPayloadType<TPayloadId>;

        template <class T, size_t TagId>
        using FixedField = typename TLayout::template FixedField<T, TagId>;

        template <size_t TagId>
        using BytesField = typename TLayout::template BytesField<TagId>;

        template <class T, size_t TagId>
        using ArrayField = typename TLayout::template ArrayField<T, TagId>;

        template <size_t MaxInlineSize, size_t TagId>
        using InlineBytesField = typename TLayout::template InlineBytesField<MaxInlineSize, TagId>;

        template <class T, size_t MaxInlineElements, size_t TagId>
        using InlineArrayField = typename TLayout::template InlineArrayField<T, MaxInlineElements, TagId>;

        template <class... TItems>
        using Scheme = typename TLayout::template Scheme<TItems...>;

        template <class... TSchemes>
        using Versions = typename TLayout::template Versions<TSchemes...>;

        using EFieldKind = typename TLayout::EFieldKind;

        template <class T>
        using TView = typename TLayout::template TView<T>;

        template <class T>
        using TConstView = typename TLayout::template TConstView<T>;

        template <class TPayloadId>
        using TBytesView = typename TLayout::template TBytesView<TPayloadId>;

        template <class TPayloadId>
        using TConstBytesView = typename TLayout::template TConstBytesView<TPayloadId>;

        template <size_t MaxInlineSize>
        using TInlineBytesView = typename TLayout::template TInlineBytesView<MaxInlineSize>;

        template <size_t MaxInlineSize>
        using TConstInlineBytesView = typename TLayout::template TConstInlineBytesView<MaxInlineSize>;

        template <class T, size_t MaxInlineElements>
        using TInlineArrayView = typename TLayout::template TInlineArrayView<T, MaxInlineElements>;

        template <class T, size_t MaxInlineElements>
        using TConstInlineArrayView = typename TLayout::template TConstInlineArrayView<T, MaxInlineElements>;

        struct TArrayPayloadStorage {
            static constexpr size_t InlineBytes = 256;
            static constexpr size_t InlineAlignment = 16;

            size_t GetSize() const {
                return Size;
            }

            const char* GetData() const {
                return Size ? (UsingInline ? InlineData.data() : Heap.GetData()) : nullptr;
            }

            char* GetDataMut() {
                return Size ? (UsingInline ? InlineData.data() : Heap.GetDataMut()) : nullptr;
            }

            void Clear() {
                Heap = {};
                Size = 0;
                UsingInline = false;
            }

            void Allocate(size_t bytes) {
                if (!bytes) {
                    Clear();
                } else if (bytes <= InlineBytes) {
                    Heap = {};
                    Size = bytes;
                    UsingInline = true;
                } else {
                    Heap = TRcBuf(TRopeAlignedBuffer::Allocate(bytes));
                    Size = bytes;
                    UsingInline = false;
                }
            }

            void MaterializeHeap() {
                if (!UsingInline || !Size) {
                    return;
                }
                TRcBuf data = TRcBuf(TRopeAlignedBuffer::Allocate(Size));
                std::memcpy(data.GetDataMut(), InlineData.data(), Size);
                Heap = std::move(data);
                UsingInline = false;
            }

            TRope ToRope() const {
                if (!Size) {
                    return {};
                }
                if (!UsingInline) {
                    return TRope(Heap);
                }

                TRcBuf data = TRcBuf(TRopeAlignedBuffer::Allocate(Size));
                std::memcpy(data.GetDataMut(), InlineData.data(), Size);
                return TRope(std::move(data));
            }

        private:
            alignas(InlineAlignment) std::array<char, InlineBytes> InlineData = {};
            TRcBuf Heap;
            size_t Size = 0;
            bool UsingInline = false;
        };

        template <class T, class TPayloadId>
        using TArrayView = typename TLayout::template TBasicArrayView<T, TPayloadId, false, TArrayPayloadStorage>;

        template <class T, class TPayloadId>
        using TConstArrayView = typename TLayout::template TBasicArrayView<T, TPayloadId, true, TArrayPayloadStorage>;

        using THandle = TEventHandle<TEv>;
        using TPtr = typename THandle::TPtr;

        template <class TScheme, bool IsConst>
        class TBasicFrontend {
            using THeaderPtr = std::conditional_t<IsConst, const char*, char*>;
            using TPayloadVector = std::conditional_t<IsConst, const TVector<TRope>, TVector<TRope>>;
            using TArrayPayloadVector = std::conditional_t<IsConst, const TVector<TArrayPayloadStorage>, TVector<TArrayPayloadStorage>>;

        public:
            TBasicFrontend() = default;

            TBasicFrontend(THeaderPtr header, size_t headerSize, TPayloadVector* payloads, TArrayPayloadVector* arrayPayloads)
                : Header(header)
                , HeaderSize(headerSize)
                , Payloads(payloads)
                , ArrayPayloads(arrayPayloads)
            {}

            template <class TTag>
            static constexpr bool HasField = TScheme::template HasField<TTag>;

            template <class TTag>
            bool HasFieldRuntime() const {
                if constexpr (!TScheme::template HasField<TTag>) {
                    return false;
                } else if constexpr (TTag::IsFixed) {
                    return Header != nullptr
                        && TScheme::template GetFixedOffset<TTag>() + sizeof(typename TTag::TValue) <= HeaderSize;
                } else if constexpr (TTag::IsPayload) {
                    return Header != nullptr
                        && TScheme::template GetPayloadRefOffset<TTag>() + sizeof(typename TScheme::TPayloadRef) <= HeaderSize;
                } else {
                    return Header != nullptr
                        && (sizeof(ui8) + TScheme::template GetFieldOffset<TTag>() + TTag::StorageSize) <= HeaderSize;
                }
            }

            template <class TTag>
            auto Field() const {
                static_assert(TTag::IsFixed, "Field() is only available for fixed fields");
                static_assert(TScheme::template HasFixedField<TTag>, "Field is absent in this scheme");
                Y_ENSURE(Header, "Flat event frontend is not initialized");
                Y_ENSURE(HasFieldRuntime<TTag>(), "Field is absent in this concrete event layout");

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
                static_assert(TTag::Kind == EFieldKind::Bytes || TTag::Kind == EFieldKind::InlineBytes,
                    "Bytes() is only available for bytes fields");
                static_assert(TScheme::template HasField<TTag>, "Field is absent in this scheme");
                Y_ENSURE(Header, "Flat event frontend is not initialized");
                Y_ENSURE(HasFieldRuntime<TTag>(), "Field is absent in this concrete event layout");

                if constexpr (TTag::Kind == EFieldKind::InlineBytes) {
                    constexpr size_t offset = sizeof(ui8) + TScheme::template GetFieldOffset<TTag>();
                    if constexpr (IsConst) {
                        return TConstInlineBytesView<TTag::InlineSize>(Header + offset);
                    } else {
                        return TInlineBytesView<TTag::InlineSize>(Header + offset);
                    }
                } else {
                    constexpr size_t payloadIndex = TScheme::template GetPayloadIndex<TTag>();
                    constexpr size_t refOffset = TScheme::template GetPayloadRefOffset<TTag>();
                    if constexpr (IsConst) {
                        return TConstBytesView<typename TScheme::TPayloadId>(&GetBytesPayload(payloadIndex), Header + refOffset,
                            static_cast<typename TScheme::TPayloadId>(payloadIndex + 1));
                    } else {
                        return TBytesView<typename TScheme::TPayloadId>(&GetMutableBytesPayload(payloadIndex), Header + refOffset,
                            static_cast<typename TScheme::TPayloadId>(payloadIndex + 1));
                    }
                }
            }

            template <class TTag>
            auto Array() const {
                static_assert(TTag::Kind == EFieldKind::Array || TTag::Kind == EFieldKind::InlineArray,
                    "Array() is only available for array fields");
                static_assert(TScheme::template HasField<TTag>, "Field is absent in this scheme");
                Y_ENSURE(Header, "Flat event frontend is not initialized");
                Y_ENSURE(HasFieldRuntime<TTag>(), "Field is absent in this concrete event layout");

                using TValue = typename TTag::TValue;
                if constexpr (TTag::Kind == EFieldKind::InlineArray) {
                    constexpr size_t offset = sizeof(ui8) + TScheme::template GetFieldOffset<TTag>();
                    if constexpr (IsConst) {
                        return TConstInlineArrayView<TValue, TTag::MaxElements>(Header + offset);
                    } else {
                        return TInlineArrayView<TValue, TTag::MaxElements>(Header + offset);
                    }
                } else {
                    constexpr size_t payloadIndex = TScheme::template GetPayloadIndex<TTag>();
                    constexpr size_t refOffset = TScheme::template GetPayloadRefOffset<TTag>();
                    if constexpr (IsConst) {
                        return TConstArrayView<TValue, typename TScheme::TPayloadId>(&GetArrayPayload(payloadIndex), Header + refOffset,
                            static_cast<typename TScheme::TPayloadId>(payloadIndex + 1));
                    } else {
                        return TArrayView<TValue, typename TScheme::TPayloadId>(&GetMutableArrayPayload(payloadIndex), Header + refOffset,
                            static_cast<typename TScheme::TPayloadId>(payloadIndex + 1));
                    }
                }
            }

            template <class TTag>
            auto ArrayData() const {
                static_assert(TTag::Kind == EFieldKind::Array, "ArrayData() is only available for array fields");
                static_assert(TScheme::template HasPayloadField<TTag>, "Field is absent in this scheme");

                using TValue = typename TTag::TValue;
                constexpr size_t payloadIndex = TScheme::template GetPayloadIndex<TTag>();
                if constexpr (IsConst) {
                    return TConstArrayView<TValue, typename TScheme::TPayloadId>::GetDirectData(GetArrayPayload(payloadIndex));
                } else {
                    return TArrayView<TValue, typename TScheme::TPayloadId>::GetDirectData(GetMutableArrayPayload(payloadIndex));
                }
            }

            template <class TTag>
            size_t ArraySize() const {
                static_assert(TTag::Kind == EFieldKind::Array || TTag::Kind == EFieldKind::InlineArray,
                    "ArraySize() is only available for array fields");
                static_assert(TScheme::template HasField<TTag>, "Field is absent in this scheme");

                if constexpr (TTag::Kind == EFieldKind::InlineArray) {
                    return this->template Array<TTag>().size();
                } else {
                    using TValue = typename TTag::TValue;
                    constexpr size_t payloadIndex = TScheme::template GetPayloadIndex<TTag>();
                    const size_t bytes = GetArrayPayload(payloadIndex).GetSize();
                    Y_DEBUG_ABORT_UNLESS(bytes % sizeof(TValue) == 0);
                    return bytes / sizeof(TValue);
                }
            }

            template <class TTag>
            size_t GetSize() const {
                static_assert(TTag::Kind == EFieldKind::Bytes || TTag::Kind == EFieldKind::Array
                        || TTag::Kind == EFieldKind::InlineBytes || TTag::Kind == EFieldKind::InlineArray,
                    "GetSize() is only available for bytes or array fields");
                static_assert(TScheme::template HasField<TTag>, "Field is absent in this scheme");

                if constexpr (TTag::Kind == EFieldKind::InlineBytes) {
                    return this->template Bytes<TTag>().size();
                } else if constexpr (TTag::Kind == EFieldKind::InlineArray) {
                    return this->template Array<TTag>().size();
                } else {
                    constexpr size_t payloadIndex = TScheme::template GetPayloadIndex<TTag>();
                    const size_t bytes = TTag::Kind == EFieldKind::Bytes
                        ? GetBytesPayload(payloadIndex).GetSize()
                        : GetArrayPayload(payloadIndex).GetSize();
                    if constexpr (TTag::Kind == EFieldKind::Bytes) {
                        return bytes;
                    } else {
                        using TValue = typename TTag::TValue;
                        Y_DEBUG_ABORT_UNLESS(bytes % sizeof(TValue) == 0);
                        return bytes / sizeof(TValue);
                    }
                }
            }

        private:
            const TRope& GetBytesPayload(size_t index) const {
                if (index < Payloads->size()) {
                    return (*Payloads)[index];
                }

                static const TRope EmptyPayload;
                return EmptyPayload;
            }

            TRope& GetMutableBytesPayload(size_t index) const {
                if (Payloads->size() < TScheme::PayloadFieldCount) {
                    Payloads->resize(TScheme::PayloadFieldCount);
                }
                return (*Payloads)[index];
            }

            const TArrayPayloadStorage& GetArrayPayload(size_t index) const {
                if (index < ArrayPayloads->size()) {
                    return (*ArrayPayloads)[index];
                }

                static const TArrayPayloadStorage EmptyPayload;
                return EmptyPayload;
            }

            TArrayPayloadStorage& GetMutableArrayPayload(size_t index) const {
                if (ArrayPayloads->size() < TScheme::PayloadFieldCount) {
                    ArrayPayloads->resize(TScheme::PayloadFieldCount);
                }
                return (*ArrayPayloads)[index];
            }

            THeaderPtr Header = nullptr;
            size_t HeaderSize = 0;
            TPayloadVector* Payloads = nullptr;
            TArrayPayloadVector* ArrayPayloads = nullptr;
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
            TStringBuilder builder;
            builder << TypeName<TEv>() << " {";
            if constexpr (!IsEmptyEvent) {
                builder << " version# " << Version;
            }
            builder << " header# " << HeaderSize << " payloads# " << GetPayloadSlotCount() << " }";
            return builder;
        }

        bool SerializeToArcadiaStream(TChunkSerializer* serializer) const override {
            AssertInitializedDebug();
            if constexpr (IsEmptyEvent) {
                return SerializeHeaderOnlyToArcadiaStream(serializer);
            } else {
                if (!HasData()) {
                    return SerializeHeaderOnlyToArcadiaStream(serializer);
                }
                if (!HasPayloads()) {
                    return SerializeHeaderOnlyToArcadiaStream(serializer);
                }
                if (const auto inlineWire = TryBuildInlineArrayWire()) {
                    return SerializeInlineArrayWireToArcadiaStream(serializer, *inlineWire);
                }
                if (CanUsePayloadsAsWirePayloads()) {
                    return SerializeToArcadiaStreamImpl(serializer, Payloads)
                        && SerializeHeaderOnlyToArcadiaStream(serializer);
                }
                const TVector<TRope> wirePayloads = BuildWirePayloads();
                return SerializeToArcadiaStreamImpl(serializer, wirePayloads)
                    && SerializeHeaderOnlyToArcadiaStream(serializer);
            }
        }

        std::optional<TRope> SerializeToRope(IRcBufAllocator* allocator) const override {
            AssertInitializedDebug();
            if constexpr (IsEmptyEvent) {
                Y_UNUSED(allocator);
                return TRope();
            } else {
                if (!HasData()) {
                    Y_UNUSED(allocator);
                    return TRope();
                }
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
                if (const auto inlineWire = TryBuildInlineArrayWire()) {
                    const size_t size = inlineWire->GetSize();
                    TRcBuf buffer = allocator->AllocRcBuf(size, 0, 0);
                    if (!buffer) {
                        return {};
                    }
                    WriteInlineArrayWireToBuffer(buffer.GetDataMut(), *inlineWire);
                    return TRope(std::move(buffer));
                }
                if (CanUsePayloadsAsWirePayloads()) {
                    return SerializeToRopeWithWirePayloads(Payloads, allocator);
                }
                const TVector<TRope> wirePayloads = BuildWirePayloads();
                return SerializeToRopeWithWirePayloads(wirePayloads, allocator);
            }
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
            if constexpr (IsEmptyEvent) {
                return 0;
            } else {
                if (!HasData()) {
                    return 0;
                }
                if (!HasPayloads()) {
                    return HeaderSize;
                }
                if (const auto inlineWire = TryDescribeInlineArrayWire()) {
                    return inlineWire->GetSize(HeaderSize);
                }
                if (CanUsePayloadsAsWirePayloads()) {
                    return CalculateSerializedSizeImpl(Payloads, HeaderSize);
                }
                const TVector<TRope> wirePayloads = BuildWirePayloads();
                return CalculateSerializedSizeImpl(wirePayloads, HeaderSize);
            }
        }

        TEventSerializationInfo CreateSerializationInfo(bool allowExternalDataChannel) const override {
            AssertInitializedDebug();
            if constexpr (IsEmptyEvent) {
                Y_UNUSED(allowExternalDataChannel);
                return {};
            } else {
                if (!HasData()) {
                    Y_UNUSED(allowExternalDataChannel);
                    return {};
                }
                if (!HasPayloads()) {
                    TEventSerializationInfo info;
                    if (allowExternalDataChannel && HeaderSize >= ExternalDataChannelMinBytes) {
                        info.Sections.push_back(TEventSectionInfo{0, HeaderSize, 0, 0, true, false});
                    }
                    return info;
                }
                if (TryDescribeInlineArrayWire()) {
                    return {};
                }
                if (CanUsePayloadsAsWirePayloads()) {
                    return CreateSerializationInfoImpl(0, allowExternalDataChannel && AllowExternalDataChannel(),
                        Payloads, HeaderSize);
                }
                const TVector<TRope> wirePayloads = BuildWirePayloads();
                TEventSerializationInfo info = CreateSerializationInfoImpl(0, allowExternalDataChannel && AllowExternalDataChannel(),
                    wirePayloads, HeaderSize);
                if (!info.Sections.empty()) {
                    DispatchCurrentVersion([&]<class TScheme>() {
                        size_t wireIndex = 0;
                        TScheme::ForEachPayloadField([&]<class TField>() {
                            if (!HasStoredField<TScheme, TField>()) {
                                return;
                            }
                            if constexpr (TField::Kind == EFieldKind::Array) {
                                info.Sections[1 + wireIndex].Alignment = alignof(typename TField::TValue);
                            }
                            ++wireIndex;
                        });
                    });
                }
                return info;
            }
        }

        ui8 GetVersion() const {
            AssertInitializedDebug();
            return Version;
        }

        bool HasData() const {
            AssertInitializedDebug();
            return Version != 0;
        }

        size_t GetSerializedSize() const {
            return CalculateSerializedSizeCached();
        }

        template <class TScheme = typename TVersions::TLatestScheme>
        static TEv* MakeEvent() {
            THolder<TEv> holder = MakeHolder();
            holder->template InitializeAsVersion<TScheme>();
            return holder.Release();
        }

        static TEv* Load(const TEventSerializedData* input) {
            Y_ENSURE(input, "Flat event buffer is null");
            if (!input->GetSize()) {
                Y_ENSURE(HasEmptyVersion, "Flat event payload is empty");
                THolder<TEv> holder = MakeHolder();
                holder->InitializeAsNoData();
                return holder.Release();
            }

            if constexpr (IsEmptyEvent) {
                Y_ENSURE(false, "Empty flat event payload is not empty");
                return nullptr;
            } else {
                TVector<TRope> wirePayloads;
                if (const auto& info = input->GetSerializationInfo(); info.IsExtendedFormat) {
                    TRope::TConstIterator iter = input->GetBeginIter();
                    size_t size = input->GetSize();
                    size_t totalPayloadSize = 0;
                    ParseExtendedFormatPayload(iter, size, wirePayloads, totalPayloadSize);
                    Y_ENSURE(size, "Flat event header is missing");

                    THolder<TEv> holder = MakeHolder();
                    holder->AssignHeaderStorage(iter, size);
                    holder->Version = ValidateHeaderStorage(holder->HeaderData(), holder->HeaderSize);

                    holder->DispatchCurrentVersion([&]<class TScheme>() {
                        Y_ENSURE(wirePayloads.size() <= TScheme::PayloadFieldCount,
                            "Unexpected number of payloads " << wirePayloads.size() << " for flat event version " << holder->Version);
                        holder->template AssignWirePayloadsForScheme<TScheme>(wirePayloads);
                        holder->template ValidateStateForScheme<TScheme>();
                    });

                    return holder.Release();
                }

                THolder<TEv> holder = MakeHolder();
                const TRope& wire = input->GetRope();
                const ui8 version = ReadWireVersion(wire);
                holder->Version = version;

                DispatchVersion(version, [&]<class TScheme>() {
                    const size_t size = wire.GetSize();
                    if (size <= TScheme::HeaderSize) {
                        holder->AssignHeaderStorage(wire);
                        holder->Version = ValidateHeaderStorage(holder->HeaderData(), holder->HeaderSize);
                    } else {
                        holder->template AssignInlineArrayWireForScheme<TScheme>(wire);
                    }
                    holder->template ValidateStateForScheme<TScheme>();
                });

                return holder.Release();
            }
        }

        template <class TTag>
        bool HasField() const {
            if constexpr (IsEmptyEvent) {
                return false;
            } else {
                if (!HasData()) {
                    return false;
                }
                return DispatchCurrentVersion([&]<class TScheme>() {
                    return HasStoredField<TScheme, TTag>();
                });
            }
        }

        template <class TScheme>
        bool IsVersion() const {
            static_assert(TVersions::template HasScheme<TScheme>, "Scheme does not belong to this flat event");
            AssertInitializedDebug();
            return Version == TVersions::template GetVersion<TScheme>();
        }

        template <class TScheme>
        bool Is() const {
            return IsVersion<TScheme>();
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
                    Y_ENSURE((HasStoredField<TScheme, TTag>()), "Field is absent in this concrete event layout");
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
                    Y_ENSURE((HasStoredField<TScheme, TTag>()), "Field is absent in this concrete event layout");
                    return MakeFrontend<TScheme>().template Field<TTag>();
                } else {
                    Y_ENSURE(false, "Field is absent in this event version");
                    return {};
                }
            });
        }

        template <class TTag>
        auto Bytes() {
            static_assert(TTag::Kind == EFieldKind::Bytes || TTag::Kind == EFieldKind::InlineBytes,
                "Bytes() is only available for bytes fields");
            return DispatchCurrentVersion([&]<class TScheme>() -> decltype(MakeFrontend<TScheme>().template Bytes<TTag>()) {
                if constexpr (TScheme::template HasField<TTag>) {
                    Y_ENSURE((HasStoredField<TScheme, TTag>()), "Field is absent in this concrete event layout");
                    return MakeFrontend<TScheme>().template Bytes<TTag>();
                } else {
                    Y_ENSURE(false, "Field is absent in this event version");
                    return decltype(MakeFrontend<TScheme>().template Bytes<TTag>()){};
                }
            });
        }

        template <class TTag>
        auto Bytes() const {
            static_assert(TTag::Kind == EFieldKind::Bytes || TTag::Kind == EFieldKind::InlineBytes,
                "Bytes() is only available for bytes fields");
            return DispatchCurrentVersion([&]<class TScheme>() -> decltype(MakeFrontend<TScheme>().template Bytes<TTag>()) {
                if constexpr (TScheme::template HasField<TTag>) {
                    Y_ENSURE((HasStoredField<TScheme, TTag>()), "Field is absent in this concrete event layout");
                    return MakeFrontend<TScheme>().template Bytes<TTag>();
                } else {
                    Y_ENSURE(false, "Field is absent in this event version");
                    return decltype(MakeFrontend<TScheme>().template Bytes<TTag>()){};
                }
            });
        }

        template <class TTag>
        auto Array() {
            static_assert(TTag::Kind == EFieldKind::Array || TTag::Kind == EFieldKind::InlineArray,
                "Array() is only available for array fields");
            return DispatchCurrentVersion([&]<class TScheme>() -> decltype(MakeFrontend<TScheme>().template Array<TTag>()) {
                if constexpr (TScheme::template HasField<TTag>) {
                    Y_ENSURE((HasStoredField<TScheme, TTag>()), "Field is absent in this concrete event layout");
                    return MakeFrontend<TScheme>().template Array<TTag>();
                } else {
                    Y_ENSURE(false, "Field is absent in this event version");
                    return decltype(MakeFrontend<TScheme>().template Array<TTag>()){};
                }
            });
        }

        template <class TTag>
        auto Array() const {
            static_assert(TTag::Kind == EFieldKind::Array || TTag::Kind == EFieldKind::InlineArray,
                "Array() is only available for array fields");
            return DispatchCurrentVersion([&]<class TScheme>() -> decltype(MakeFrontend<TScheme>().template Array<TTag>()) {
                if constexpr (TScheme::template HasField<TTag>) {
                    Y_ENSURE((HasStoredField<TScheme, TTag>()), "Field is absent in this concrete event layout");
                    return MakeFrontend<TScheme>().template Array<TTag>();
                } else {
                    Y_ENSURE(false, "Field is absent in this event version");
                    return decltype(MakeFrontend<TScheme>().template Array<TTag>()){};
                }
            });
        }

        template <class TTag>
        auto ArrayData() {
            static_assert(TTag::Kind == EFieldKind::Array, "ArrayData() is only available for array fields");
            return DispatchCurrentVersion([&]<class TScheme>() -> decltype(MakeFrontend<TScheme>().template ArrayData<TTag>()) {
                if constexpr (TScheme::template HasPayloadField<TTag>) {
                    Y_ENSURE((HasStoredField<TScheme, TTag>()), "Field is absent in this concrete event layout");
                    return MakeFrontend<TScheme>().template ArrayData<TTag>();
                } else {
                    Y_ENSURE(false, "Field is absent in this event version");
                    return decltype(MakeFrontend<TScheme>().template ArrayData<TTag>()){};
                }
            });
        }

        template <class TTag>
        auto ArrayData() const {
            static_assert(TTag::Kind == EFieldKind::Array, "ArrayData() is only available for array fields");
            return DispatchCurrentVersion([&]<class TScheme>() -> decltype(MakeFrontend<TScheme>().template ArrayData<TTag>()) {
                if constexpr (TScheme::template HasPayloadField<TTag>) {
                    Y_ENSURE((HasStoredField<TScheme, TTag>()), "Field is absent in this concrete event layout");
                    return MakeFrontend<TScheme>().template ArrayData<TTag>();
                } else {
                    Y_ENSURE(false, "Field is absent in this event version");
                    return decltype(MakeFrontend<TScheme>().template ArrayData<TTag>()){};
                }
            });
        }

        template <class TTag>
        size_t ArraySize() const {
            static_assert(TTag::Kind == EFieldKind::Array || TTag::Kind == EFieldKind::InlineArray,
                "ArraySize() is only available for array fields");
            return DispatchCurrentVersion([&]<class TScheme>() -> size_t {
                if constexpr (TScheme::template HasField<TTag>) {
                    Y_ENSURE((HasStoredField<TScheme, TTag>()), "Field is absent in this concrete event layout");
                    return MakeFrontend<TScheme>().template ArraySize<TTag>();
                } else {
                    Y_ENSURE(false, "Field is absent in this event version");
                    return 0;
                }
            });
        }

        template <class TTag>
        size_t GetSize() const {
            static_assert(TTag::Kind == EFieldKind::Bytes || TTag::Kind == EFieldKind::Array
                    || TTag::Kind == EFieldKind::InlineBytes || TTag::Kind == EFieldKind::InlineArray,
                "GetSize() is only available for bytes or array fields");
            return DispatchCurrentVersion([&]<class TScheme>() -> size_t {
                if constexpr (TScheme::template HasField<TTag>) {
                    Y_ENSURE((HasStoredField<TScheme, TTag>()), "Field is absent in this concrete event layout");
                    return MakeFrontend<TScheme>().template GetSize<TTag>();
                } else {
                    Y_ENSURE(false, "Field is absent in this event version");
                    return 0;
                }
            });
        }

    protected:
        TEventFlat() = default;

        template <class TScheme = typename TVersions::TLatestScheme>
        void InitializeAsVersion() {
            if constexpr (IsEmptyEvent) {
                InitializeAsNoData();
            } else {
                static_assert(TVersions::template HasScheme<TScheme>, "Scheme does not belong to this flat event");
                ResetHeaderStorage(TScheme::HeaderSize);
                Version = TVersions::template GetVersion<TScheme>();
                WriteUnaligned<ui8>(MutableHeaderData(), Version);
            }
        }

        void InitializeAsNoData() {
            Y_ENSURE(HasEmptyVersion, "Flat event does not allow empty payload");
            ResetHeaderStorage(0);
            Payloads.clear();
            ArrayPayloads.clear();
            Version = 0;
        }

    private:
        template <class TValue>
        static size_t CheckedFieldBytes(size_t count) {
            Y_ENSURE(count <= std::numeric_limits<size_t>::max() / sizeof(TValue), "Flat event field is too large");
            return count * sizeof(TValue);
        }

        static THolder<TEv> MakeHolder() {
            return THolder<TEv>(new TEv());
        }

        static ui8 ValidateHeaderStorage(const char* header, size_t size) {
            Y_ENSURE(size >= sizeof(ui8), "Flat event header is too short");
            const ui8 version = ReadUnaligned<ui8>(header);
            Y_ENSURE(version >= 1 && version <= TVersions::VersionCount,
                "Unsupported flat event version " << static_cast<size_t>(version));
            return version;
        }

        template <class TScheme>
        void ValidateStateForScheme() const {
            Y_ENSURE(HeaderSize >= sizeof(ui8), "Flat event header is too short");
            if constexpr (TScheme::IsStrict) {
                Y_ENSURE(HeaderSize == TScheme::HeaderSize,
                    "Unexpected strict flat event header size " << HeaderSize << ", expected " << TScheme::HeaderSize);
            } else {
                Y_ENSURE(HeaderSize <= TScheme::HeaderSize,
                    "Unexpected flat event header size " << HeaderSize << ", expected at most " << TScheme::HeaderSize);
            }
            Y_ENSURE(Payloads.empty() || Payloads.size() <= TScheme::PayloadFieldCount,
                "Unexpected number of flat payload slots " << Payloads.size() << ", expected 0.." << TScheme::PayloadFieldCount);
            Y_ENSURE(ArrayPayloads.empty() || ArrayPayloads.size() <= TScheme::PayloadFieldCount,
                "Unexpected number of flat array payload slots " << ArrayPayloads.size() << ", expected 0.." << TScheme::PayloadFieldCount);

            TScheme::ForEachPayloadField([&]<class TField>() {
                constexpr size_t payloadIndex = TScheme::template GetPayloadIndex<TField>();
                const typename TScheme::TPayloadRef ref = HasStoredField<TScheme, TField>()
                    ? GetPayloadRef<TScheme, TField>()
                    : typename TScheme::TPayloadRef{};
                static const TRope EmptyBytesPayload;
                static const TArrayPayloadStorage EmptyArrayPayload;
                const TRope& bytesPayload = payloadIndex < Payloads.size() ? Payloads[payloadIndex] : EmptyBytesPayload;
                const TArrayPayloadStorage& arrayPayload = payloadIndex < ArrayPayloads.size() ? ArrayPayloads[payloadIndex] : EmptyArrayPayload;

                if constexpr (TField::Kind == EFieldKind::Bytes) {
                    Y_ENSURE(arrayPayload.GetSize() == 0, "Bytes field must not use array payload storage");
                    if (ref.PayloadId == 0) {
                        Y_ENSURE(bytesPayload.GetSize() == 0, "Absent payload slot must be empty");
                    } else {
                        Y_ENSURE(Payloads.size() > payloadIndex,
                            "Present bytes payload requires allocated payload slots");
                        Y_ENSURE(ref.PayloadId == payloadIndex + 1,
                            "Unexpected payload id " << ref.PayloadId << " for slot " << payloadIndex);
                    }
                } else {
                    using TValue = typename TField::TValue;
                    Y_ENSURE(bytesPayload.GetSize() == 0, "Array field must not use bytes payload storage");
                    if (ref.PayloadId == 0) {
                        Y_ENSURE(arrayPayload.GetSize() == 0, "Absent payload slot must be empty");
                    } else {
                        Y_ENSURE(ArrayPayloads.size() > payloadIndex,
                            "Present array payload requires allocated payload slots");
                        Y_ENSURE(ref.PayloadId == payloadIndex + 1,
                            "Unexpected payload id " << ref.PayloadId << " for slot " << payloadIndex);
                        Y_ENSURE(arrayPayload.GetSize() % sizeof(TValue) == 0,
                            "Array payload size mismatch, payload# " << payloadIndex
                            << " bytes# " << arrayPayload.GetSize() << " elemSize# " << sizeof(TValue));
                        if constexpr (alignof(TValue) <= 16) {
                            Y_ENSURE(arrayPayload.GetData() == nullptr
                                    || reinterpret_cast<uintptr_t>(arrayPayload.GetData()) % alignof(TValue) == 0,
                                "Array payload alignment mismatch, payload# " << payloadIndex
                                << " alignment# " << alignof(TValue));
                        }
                    }
                }
            });
        }

        template <class TScheme>
        void AssignWirePayloadsForScheme(TVector<TRope>& wirePayloads) {
            Payloads.resize(TScheme::PayloadFieldCount);
            ArrayPayloads.resize(TScheme::PayloadFieldCount);

            TScheme::ForEachPayloadField([&]<class TField>() {
                constexpr size_t payloadIndex = TScheme::template GetPayloadIndex<TField>();
                if constexpr (TField::Kind == EFieldKind::Bytes) {
                    Payloads[payloadIndex] = std::move(wirePayloads[payloadIndex]);
                    ArrayPayloads[payloadIndex] = {};
                } else {
                    const TRope& wirePayload = wirePayloads[payloadIndex];
                    const size_t bytes = wirePayload.GetSize();
                    using TValue = typename TField::TValue;
                    Y_ENSURE(bytes % sizeof(TValue) == 0,
                        "Array payload size mismatch, payload# " << payloadIndex
                        << " bytes# " << bytes << " elemSize# " << sizeof(TValue));
                    if (bytes) {
                        ArrayPayloads[payloadIndex].Allocate(bytes);
                        auto src = wirePayload.Begin();
                        TRopeUtils::Memcpy(ArrayPayloads[payloadIndex].GetDataMut(), src, bytes);
                    } else {
                        ArrayPayloads[payloadIndex].Clear();
                    }
                    Payloads[payloadIndex] = {};
                }
            });
        }

        template <class TScheme>
        void AssignInlineArrayWireForScheme(const TRope& wire) {
            const size_t size = wire.GetSize();
            Y_ENSURE(size > TScheme::HeaderSize,
                "Flat inline array wire is too short " << size << ", expected more than " << TScheme::HeaderSize);

            auto iter = wire.Begin();
            ui8 wireVersion = 0;
            TRopeUtils::Memcpy(reinterpret_cast<char*>(&wireVersion), iter, sizeof(wireVersion));
            iter += sizeof(wireVersion);
            size_t tailSize = size - sizeof(wireVersion);

            size_t headerSize = TScheme::HeaderSize;
            if constexpr (!TScheme::IsStrict) {
                headerSize = ReadNumberFromRope(iter, tailSize);
                Y_ENSURE(headerSize >= sizeof(ui8),
                    "Flat inline array wire header is too short " << headerSize);
                Y_ENSURE(headerSize <= TScheme::HeaderSize,
                    "Flat inline array wire header is too large " << headerSize << ", expected at most " << TScheme::HeaderSize);
            }
            Y_ENSURE(headerSize - sizeof(ui8) <= tailSize,
                "Flat inline array wire is truncated in header, header# " << headerSize << " tail# " << tailSize);

            ResetHeaderStorage(headerSize);
            WriteUnaligned<ui8>(MutableHeaderData(), wireVersion);
            if (headerSize > sizeof(ui8)) {
                TRopeUtils::Memcpy(MutableHeaderData() + sizeof(ui8), iter, headerSize - sizeof(ui8));
                iter += headerSize - sizeof(ui8);
                tailSize -= headerSize - sizeof(ui8);
            }
            Version = ValidateHeaderStorage(HeaderData(), HeaderSize);
            Y_ENSURE(Version == TVersions::template GetVersion<TScheme>(),
                "Flat inline array wire version mismatch");

            Payloads.resize(TScheme::PayloadFieldCount);
            ArrayPayloads.resize(TScheme::PayloadFieldCount);

            TScheme::ForEachPayloadField([&]<class TField>() {
                if (!HasStoredField<TScheme, TField>()) {
                    return;
                }

                constexpr size_t payloadIndex = TScheme::template GetPayloadIndex<TField>();
                if constexpr (TField::Kind == EFieldKind::Bytes) {
                    const typename TScheme::TPayloadRef ref = GetPayloadRef<TScheme, TField>();
                    Y_ENSURE(ref.PayloadId == 0, "Bytes payload cannot be stored in inline array wire");
                    Payloads[payloadIndex] = {};
                    ArrayPayloads[payloadIndex] = {};
                } else {
                    using TValue = typename TField::TValue;
                    typename TScheme::TPayloadRef ref = GetPayloadRef<TScheme, TField>();
                    Y_ENSURE(ref.PayloadId == 0, "Array payload ref must be empty in inline array wire");
                    const size_t bytes = ReadNumberFromRope(iter, tailSize);
                    Y_ENSURE(bytes % sizeof(TValue) == 0,
                        "Array payload size mismatch, payload# " << payloadIndex
                        << " bytes# " << bytes << " elemSize# " << sizeof(TValue));
                    Y_ENSURE(bytes <= tailSize,
                        "Flat inline array wire is truncated, payload# " << payloadIndex
                        << " bytes# " << bytes << " tail# " << tailSize);

                    Payloads[payloadIndex] = {};
                    if (bytes) {
                        ArrayPayloads[payloadIndex].Allocate(bytes);
                        TRopeUtils::Memcpy(ArrayPayloads[payloadIndex].GetDataMut(), iter, bytes);
                        iter += bytes;
                        tailSize -= bytes;
                        ref.PayloadId = static_cast<typename TScheme::TPayloadId>(payloadIndex + 1);
                    } else {
                        ArrayPayloads[payloadIndex].Clear();
                        ref.PayloadId = 0;
                    }
                    WriteUnaligned<typename TScheme::TPayloadRef>(
                        MutableHeaderData() + TScheme::template GetPayloadRefOffset<TField>(),
                        ref);
                }
            });

            Y_ENSURE(tailSize == 0, "Flat inline array wire has trailing bytes " << tailSize);
        }

        template <class TScheme, class TTag>
        static consteval size_t GetStoredFieldEndOffset() {
            if constexpr (TTag::IsFixed) {
                return TScheme::template GetFixedOffset<TTag>() + sizeof(typename TTag::TValue);
            } else if constexpr (TTag::IsPayload) {
                return TScheme::template GetPayloadRefOffset<TTag>() + sizeof(typename TScheme::TPayloadRef);
            } else {
                return sizeof(ui8) + TScheme::template GetFieldOffset<TTag>() + TTag::StorageSize;
            }
        }

        template <class TScheme, class TTag>
        bool HasStoredField() const {
            if constexpr (!TScheme::template HasField<TTag>) {
                return false;
            } else if constexpr (TScheme::IsStrict) {
                return true;
            } else {
                return HeaderSize >= GetStoredFieldEndOffset<TScheme, TTag>();
            }
        }

        bool AllowExternalDataChannel() const {
            size_t totalPayloadSize = 0;
            for (const TRope& payload : Payloads) {
                totalPayloadSize += payload.GetSize();
            }
            for (const TArrayPayloadStorage& payload : ArrayPayloads) {
                totalPayloadSize += payload.GetSize();
            }
            return totalPayloadSize >= ExternalDataChannelMinBytes;
        }

        bool HasPayloads() const {
            return !Payloads.empty() || !ArrayPayloads.empty();
        }

        size_t GetPayloadSlotCount() const {
            return std::max(Payloads.size(), ArrayPayloads.size());
        }

        struct TInlineArrayWireLayout {
            size_t DataBytes = 0;
            size_t TailBytes = 0;
            bool HasHeaderSize = true;

            ui32 GetSize(size_t headerSize) const {
                return static_cast<ui32>(headerSize + (HasHeaderSize ? GetNumberWireSize(headerSize) : 0) + TailBytes);
            }
        };

        struct TInlineArrayWire {
            struct TArrayEntry {
                const TArrayPayloadStorage* Payload = nullptr;
                size_t Bytes = 0;
            };

            TString Header;
            TVector<TArrayEntry> Arrays;
            TInlineArrayWireLayout Layout;

            ui32 GetSize() const {
                return Layout.GetSize(Header.size());
            }
        };

        static ui8 ReadWireVersion(const TRope& wire) {
            static_assert(!IsEmptyEvent, "Empty flat event has no wire version");
            Y_ENSURE(wire.GetSize() >= sizeof(ui8), "Flat event header is too short");
            ui8 version = 0;
            auto iter = wire.Begin();
            TRopeUtils::Memcpy(reinterpret_cast<char*>(&version), iter, sizeof(version));
            Y_ENSURE(version >= 1 && version <= TVersions::VersionCount,
                "Unsupported flat event version " << static_cast<size_t>(version));
            return version;
        }

        template <class F>
        static decltype(auto) DispatchVersion(ui8 version, F&& f) {
            if constexpr (IsEmptyEvent) {
                Y_UNUSED(version);
                Y_UNUSED(f);
                ythrow TWithBackTrace<yexception>() << "Empty flat event has no scheme versions";
            } else if constexpr (TVersions::VersionCount == 1) {
                Y_ENSURE(version == 1, "Unsupported flat event version " << static_cast<size_t>(version));
                return f.template operator()<typename TVersions::TLatestScheme>();
            } else {
                return TVersions::Dispatch(version, std::forward<F>(f));
            }
        }

        std::optional<TInlineArrayWireLayout> TryDescribeInlineArrayWire() const {
            if constexpr (IsEmptyEvent) {
                return std::nullopt;
            } else {
                if (!HasData()) {
                    return std::nullopt;
                }
                return DispatchCurrentVersion([&]<class TScheme>() -> std::optional<TInlineArrayWireLayout> {
                    return TryDescribeInlineArrayWireForScheme<TScheme>();
                });
            }
        }

        template <class TScheme>
        std::optional<TInlineArrayWireLayout> TryDescribeInlineArrayWireForScheme() const {
            TInlineArrayWireLayout layout;
            layout.HasHeaderSize = !TScheme::IsStrict;

            bool ok = true;
            TScheme::ForEachPayloadField([&]<class TField>() {
                if (!ok || !HasStoredField<TScheme, TField>()) {
                    return;
                }

                constexpr size_t payloadIndex = TScheme::template GetPayloadIndex<TField>();
                const typename TScheme::TPayloadRef ref = GetPayloadRef<TScheme, TField>();

                if constexpr (TField::Kind == EFieldKind::Bytes) {
                    const size_t bytes = payloadIndex < Payloads.size() ? Payloads[payloadIndex].GetSize() : 0;
                    if (ref.PayloadId != 0 || bytes != 0) {
                        ok = false;
                    }
                } else {
                    const size_t bytes = payloadIndex < ArrayPayloads.size() ? ArrayPayloads[payloadIndex].GetSize() : 0;
                    if (ref.PayloadId == 0) {
                        if (bytes != 0) {
                            ok = false;
                        }
                        layout.TailBytes += GetNumberWireSize(0);
                        return;
                    }

                    using TValue = typename TField::TValue;
                    if (payloadIndex >= ArrayPayloads.size()
                            || ref.PayloadId != payloadIndex + 1
                            || bytes == 0
                            || bytes % sizeof(TValue) != 0) {
                        ok = false;
                        return;
                    }

                    if (bytes > InlineArrayWireMaxBytes
                            || layout.DataBytes > InlineArrayWireMaxBytes - bytes) {
                        ok = false;
                        return;
                    }

                    layout.DataBytes += bytes;
                    layout.TailBytes += GetNumberWireSize(bytes) + bytes;
                }
            });

            if (!ok || layout.DataBytes == 0 || layout.GetSize(HeaderSize) <= TScheme::HeaderSize) {
                return std::nullopt;
            }
            return layout;
        }

        std::optional<TInlineArrayWire> TryBuildInlineArrayWire() const {
            if constexpr (IsEmptyEvent) {
                return std::nullopt;
            } else {
                if (!HasData()) {
                    return std::nullopt;
                }
                return DispatchCurrentVersion([&]<class TScheme>() -> std::optional<TInlineArrayWire> {
                    const auto layout = TryDescribeInlineArrayWireForScheme<TScheme>();
                    if (!layout) {
                        return std::nullopt;
                    }

                    TInlineArrayWire wire;
                    wire.Layout = *layout;
                    wire.Header = TString(HeaderData(), HeaderSize);
                    wire.Arrays.reserve(TScheme::PayloadFieldCount);
                    char* headerData = wire.Header.Detach();

                    TScheme::ForEachPayloadField([&]<class TField>() {
                        if (!HasStoredField<TScheme, TField>()) {
                            return;
                        }

                        constexpr size_t payloadIndex = TScheme::template GetPayloadIndex<TField>();
                        const typename TScheme::TPayloadRef ref = GetPayloadRef<TScheme, TField>();
                        char* refPtr = headerData + TScheme::template GetPayloadRefOffset<TField>();

                        if constexpr (TField::Kind != EFieldKind::Bytes) {
                            const size_t bytes = payloadIndex < ArrayPayloads.size() ? ArrayPayloads[payloadIndex].GetSize() : 0;
                            WriteUnaligned<typename TScheme::TPayloadRef>(refPtr, {});
                            if (ref.PayloadId == 0) {
                                wire.Arrays.push_back(typename TInlineArrayWire::TArrayEntry{});
                                return;
                            }

                            wire.Arrays.push_back(typename TInlineArrayWire::TArrayEntry{&ArrayPayloads[payloadIndex], bytes});
                        }
                    });

                    return wire;
                });
            }
        }

        static void WriteInlineArrayWireToBuffer(char* dst, const TInlineArrayWire& wire) {
            Y_DEBUG_ABORT_UNLESS(!wire.Header.empty());
            std::memcpy(dst, wire.Header.data(), sizeof(ui8));
            dst += sizeof(ui8);
            if (wire.Layout.HasHeaderSize) {
                dst += WriteNumberTo(dst, wire.Header.size());
            }
            if (wire.Header.size() > sizeof(ui8)) {
                std::memcpy(dst, wire.Header.data() + sizeof(ui8), wire.Header.size() - sizeof(ui8));
                dst += wire.Header.size() - sizeof(ui8);
            }
            for (const typename TInlineArrayWire::TArrayEntry& entry : wire.Arrays) {
                dst += WriteNumberTo(dst, entry.Bytes);
                if (entry.Bytes) {
                    std::memcpy(dst, entry.Payload->GetData(), entry.Bytes);
                    dst += entry.Bytes;
                }
            }
        }

        static size_t GetNumberWireSize(size_t number) {
            size_t size = 0;
            ForEachNumberWireByte(number, [&size](size_t, ui8) {
                ++size;
            });
            return size;
        }

        static size_t WriteNumberTo(char* dst, size_t number) {
            return ForEachNumberWireByte(number, [dst](size_t pos, ui8 byte) {
                dst[pos] = static_cast<char>(byte);
            });
        }

        template <typename TConsumer>
        static size_t ForEachNumberWireByte(size_t number, TConsumer&& consumer) {
            size_t pos = 0;
            do {
                const ui8 byte = static_cast<ui8>((number & 0x7F) | (number >= 128 ? 0x80 : 0x00));
                consumer(pos++, byte);
                number >>= 7;
            } while (number);
            return pos;
        }

        static size_t ReadNumberFromRope(TRope::TConstIterator& iter, size_t& size) {
            size_t number = 0;
            size_t shift = 0;
            for (;;) {
                Y_ENSURE(size, "Flat inline array wire is truncated in size prefix");
                Y_ENSURE(iter.Valid(), "Flat inline array wire iterator is invalid");
                Y_ENSURE(shift < sizeof(size_t) * CHAR_BIT, "Flat inline array wire size prefix is too long");
                const ui8 byte = static_cast<ui8>(*iter.ContiguousData());
                iter += 1;
                --size;
                number |= static_cast<size_t>(byte & 0x7F) << shift;
                if ((byte & 0x80) == 0) {
                    return number;
                }
                shift += 7;
            }
        }

        static bool SerializeInlineArrayWireToArcadiaStream(TChunkSerializer* serializer, const TInlineArrayWire& wire) {
            Y_DEBUG_ABORT_UNLESS(!wire.Header.empty());
            if (!WriteRawToArcadiaStream(serializer, wire.Header.data(), sizeof(ui8))) {
                return false;
            }

            if (wire.Layout.HasHeaderSize) {
                char headerSizeBuf[MaxNumberBytes];
                const size_t headerSizeLen = WriteNumberTo(headerSizeBuf, wire.Header.size());
                if (!WriteRawToArcadiaStream(serializer, headerSizeBuf, headerSizeLen)) {
                    return false;
                }
            }

            if (wire.Header.size() > sizeof(ui8) && !WriteRawToArcadiaStream(
                    serializer,
                    wire.Header.data() + sizeof(ui8),
                    wire.Header.size() - sizeof(ui8))) {
                return false;
            }

            for (const typename TInlineArrayWire::TArrayEntry& entry : wire.Arrays) {
                char sizeBuf[MaxNumberBytes];
                const size_t sizeLen = WriteNumberTo(sizeBuf, entry.Bytes);
                if (!WriteRawToArcadiaStream(serializer, sizeBuf, sizeLen)) {
                    return false;
                }
                if (entry.Bytes && !WriteRawToArcadiaStream(serializer, entry.Payload->GetData(), entry.Bytes)) {
                    return false;
                }
            }

            return true;
        }

        bool CanUsePayloadsAsWirePayloads() const {
            if constexpr (IsEmptyEvent) {
                return false;
            } else {
                if (!HasData()) {
                    return false;
                }
                return DispatchCurrentVersion([&]<class TScheme>() {
                    if constexpr (!TScheme::HasOnlyBytesPayloadFields) {
                        return false;
                    } else {
                        if (Payloads.size() != TScheme::PayloadFieldCount) {
                            return false;
                        }
                        bool ok = true;
                        TScheme::ForEachPayloadField([&]<class TField>() {
                            ok = ok && HasStoredField<TScheme, TField>();
                        });
                        return ok;
                    }
                });
            }
        }

        std::optional<TRope> SerializeToRopeWithWirePayloads(const TVector<TRope>& wirePayloads, IRcBufAllocator* allocator) const {
            const ui32 headerSize = CalculateSerializedHeaderSizeImpl(wirePayloads);

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
                    return append(buf, WriteNumberTo(buf, number));
                };

                appendNumber(wirePayloads.size());
                for (const TRope& rope : wirePayloads) {
                    appendNumber(rope.GetSize());
                }

                result.Insert(result.End(), std::move(headerBuf));
            }

            for (const TRope& rope : wirePayloads) {
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

        TVector<TRope> BuildWirePayloads() const {
            TVector<TRope> wirePayloads;
            if constexpr (IsEmptyEvent) {
                return wirePayloads;
            } else {
                if (!HasData()) {
                    return wirePayloads;
                }
                DispatchCurrentVersion([&]<class TScheme>() {
                    wirePayloads.reserve(TScheme::PayloadFieldCount);
                    TScheme::ForEachPayloadField([&]<class TField>() {
                        constexpr size_t payloadIndex = TScheme::template GetPayloadIndex<TField>();
                        if (!HasStoredField<TScheme, TField>()) {
                            return;
                        }
                        if constexpr (TField::Kind == EFieldKind::Bytes) {
                            wirePayloads.push_back(payloadIndex < Payloads.size() ? TRope(Payloads[payloadIndex]) : TRope());
                        } else {
                            if (payloadIndex < ArrayPayloads.size()) {
                                const_cast<TArrayPayloadStorage&>(ArrayPayloads[payloadIndex]).MaterializeHeap();
                                wirePayloads.push_back(ArrayPayloads[payloadIndex].ToRope());
                            } else {
                                wirePayloads.push_back(TRope());
                            }
                        }
                    });
                });
                return wirePayloads;
            }
        }

        bool SerializeHeaderOnlyToArcadiaStream(TChunkSerializer* serializer) const {
            return WriteRawToArcadiaStream(serializer, HeaderData(), HeaderSize);
        }

        static bool WriteRawToArcadiaStream(TChunkSerializer* serializer, const char* data, size_t len) {
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
            AssertInitializedDebug();
            if constexpr (IsEmptyEvent) {
                Y_UNUSED(f);
                ythrow TWithBackTrace<yexception>() << "Empty flat event has no scheme versions";
            } else if constexpr (TVersions::VersionCount == 1) {
                Y_ENSURE(Version == 1, "Flat event has no data");
                return f.template operator()<typename TVersions::TLatestScheme>();
            } else {
                Y_ENSURE(Version != 0, "Flat event has no data");
                return TVersions::Dispatch(Version, std::forward<F>(f));
            }
        }

        template <class TScheme, class TTag>
        typename TScheme::TPayloadRef GetPayloadRef() const {
            return ReadUnaligned<typename TScheme::TPayloadRef>(HeaderData() + TScheme::template GetPayloadRefOffset<TTag>());
        }

        void AssertInitializedDebug() const {
            if constexpr (HasEmptyVersion) {
                if (Version == 0) {
                    Y_DEBUG_ABORT_UNLESS(HeaderSize == 0);
                    Y_DEBUG_ABORT_UNLESS(!HasPayloads());
                    return;
                }
            }
            if constexpr (IsEmptyEvent) {
                Y_DEBUG_ABORT_UNLESS(Version == 0);
                Y_DEBUG_ABORT_UNLESS(HeaderSize == 0);
                Y_DEBUG_ABORT_UNLESS(!HasPayloads());
            } else {
                Y_DEBUG_ABORT_UNLESS(Version != 0);
                Y_DEBUG_ABORT_UNLESS(HeaderSize >= sizeof(ui8));
                Y_DEBUG_ABORT_UNLESS(ReadUnaligned<ui8>(HeaderData()) == Version);
            }
        }

        template <class TScheme>
        void AssertFrontendVersion() const {
            static_assert(TVersions::template HasScheme<TScheme>, "Scheme does not belong to this flat event");
            AssertInitializedDebug();
            Y_DEBUG_ABORT_UNLESS(Version == TVersions::template GetVersion<TScheme>());
        }

        template <class TScheme>
        TFrontend<TScheme> MakeFrontend() {
            return TFrontend<TScheme>(MutableHeaderData(), HeaderSize, &Payloads, &ArrayPayloads);
        }

        template <class TScheme>
        TConstFrontend<TScheme> MakeFrontend() const {
            return TConstFrontend<TScheme>(HeaderData(), HeaderSize, &Payloads, &ArrayPayloads);
        }

        void ResetHeaderStorage(size_t size) {
            Y_DEBUG_ABORT_UNLESS(size <= InlineHeaderStorage.size());
            HeaderSize = size;
        }

        void AssignHeaderStorage(TString&& header) {
            const size_t size = header.size();
            Y_ENSURE(size <= InlineHeaderStorage.size(),
                "Flat event header is too large " << size << ", capacity " << InlineHeaderStorage.size());
            HeaderSize = size;
            if (size) {
                std::memcpy(InlineHeaderStorage.data(), header.data(), size);
            }
        }

        void AssignHeaderStorage(const TRope& header) {
            const size_t size = header.GetSize();
            Y_ENSURE(size <= InlineHeaderStorage.size(),
                "Flat event header is too large " << size << ", capacity " << InlineHeaderStorage.size());
            HeaderSize = size;
            if (size) {
                auto it = header.Begin();
                TRopeUtils::Memcpy(InlineHeaderStorage.data(), it, size);
            }
        }

        void AssignHeaderStorage(TRope::TConstIterator iter, size_t size) {
            Y_ENSURE(size <= InlineHeaderStorage.size(),
                "Flat event header is too large " << size << ", capacity " << InlineHeaderStorage.size());
            HeaderSize = size;
            if (size) {
                TRopeUtils::Memcpy(InlineHeaderStorage.data(), iter, size);
            }
        }

        char* MutableHeaderData() {
            return InlineHeaderStorage.data();
        }

        const char* HeaderData() const {
            return InlineHeaderStorage.data();
        }

    private:
        std::array<char, TVersions::MaxHeaderSize> InlineHeaderStorage = {};
        TVector<TRope> Payloads;
        TVector<TArrayPayloadStorage> ArrayPayloads;
        size_t HeaderSize = 0;
        ui8 Version = 0;

        static constexpr size_t ExternalDataChannelMinBytes = 8192;
        static constexpr size_t InlineArrayWireMaxBytes = 16 * 1024;
    };

} // namespace NActors
