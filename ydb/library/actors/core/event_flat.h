#pragma once

#include "event.h"
#include "event_pb.h"

#include <util/generic/string.h>
#include <util/string/builder.h>
#include <util/system/type_name.h>
#include <util/system/unaligned_mem.h>

#include <cstring>
#include <limits>
#include <type_traits>
#include <utility>

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

    template <class TEv>
    class TEventFlat : public IEventBase {
        static_assert(std::is_class_v<TEv>, "TEv must be a class");

        enum class EFieldKind {
            Fixed,
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
            static constexpr bool IsVariable = false;
        };

        template <class T, size_t TagId>
        struct ArrayField {
            static_assert(std::is_trivially_copyable_v<T>, "ArrayField requires trivially copyable type");

            using TValue = T;
            static constexpr size_t Id = TagId;
            static constexpr EFieldKind Kind = EFieldKind::Array;
            static constexpr bool IsFixed = false;
            static constexpr bool IsVariable = true;
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

        template <class T, bool IsConst>
        class TBasicArrayView {
            using TPointer = std::conditional_t<IsConst, const char*, char*>;

        public:
            TBasicArrayView() = default;

            TBasicArrayView(TPointer ptr, size_t size)
                : Ptr(ptr)
                , Size(size)
            {}

            size_t size() const {
                return Size;
            }

            bool empty() const {
                return Size == 0;
            }

            TBasicView<T, IsConst> operator[](size_t index) const {
                Y_ENSURE(index < Size, "Array index " << index << " is out of range " << Size);
                return TBasicView<T, IsConst>(Ptr + index * sizeof(T));
            }

            T Get(size_t index) const {
                return (*this)[index].Get();
            }

            template <bool Enabled = !IsConst>
            std::enable_if_t<Enabled> Set(size_t index, T value) {
                (*this)[index] = value;
            }

            template <bool Enabled = !IsConst>
            std::enable_if_t<Enabled> CopyFrom(const T* src, size_t count) {
                Y_ENSURE(count == Size, "Unexpected array size " << count << ", expected " << Size);
                for (size_t i = 0; i < Size; ++i) {
                    WriteUnaligned<T>(Ptr + i * sizeof(T), src[i]);
                }
            }

            void CopyTo(T* dst, size_t count) const {
                Y_ENSURE(count == Size, "Unexpected array size " << count << ", expected " << Size);
                for (size_t i = 0; i < Size; ++i) {
                    dst[i] = ReadUnaligned<T>(Ptr + i * sizeof(T));
                }
            }

        private:
            TPointer Ptr = nullptr;
            size_t Size = 0;
        };

        template <class T>
        using TArrayView = TBasicArrayView<T, false>;

        template <class T>
        using TConstArrayView = TBasicArrayView<T, true>;

        template <class... TFields>
        struct Scheme {
            static_assert(NFlatEventDetail::TAreDistinct<TFields...>::value, "Duplicate field tag in flat event scheme");

            static constexpr size_t FieldCount = sizeof...(TFields);
            static constexpr size_t VariableFieldCount = (0u + ... + (TFields::IsVariable ? 1u : 0u));
            static constexpr size_t FixedFieldsSize = (0u + ... + (TFields::IsFixed ? sizeof(typename TFields::TValue) : 0u));
            static constexpr size_t FixedPartSize = sizeof(ui16) + FixedFieldsSize + VariableFieldCount * sizeof(ui32);

            template <class TTag>
            static constexpr bool HasField = (std::is_same_v<TTag, TFields> || ...);

            template <class TTag>
            static constexpr bool HasFixedField = ((std::is_same_v<TTag, TFields> && TFields::IsFixed) || ...);

            template <class TTag>
            static constexpr bool HasVariableField = ((std::is_same_v<TTag, TFields> && TFields::IsVariable) || ...);

            template <class TTag>
            static consteval size_t GetFixedOffset() {
                static_assert(HasFixedField<TTag>, "Field is not a fixed field in this scheme");
                return sizeof(ui16) + GetFixedOffsetImpl<TTag, TFields...>();
            }

            template <class TTag>
            static consteval size_t GetVariableMetaOffset() {
                static_assert(HasVariableField<TTag>, "Field is not an array field in this scheme");
                return sizeof(ui16) + FixedFieldsSize + GetVariableIndexImpl<TTag, TFields...>() * sizeof(ui32);
            }

            template <class F>
            static void ForEachVariableField(F&& f) {
                (ForEachVariableFieldImpl<TFields>(f), ...);
            }

        private:
            template <class TTag>
            static consteval size_t GetFixedOffsetImpl() {
                return 0;
            }

            template <class TTag, class TFirst, class... TRest>
            static consteval size_t GetFixedOffsetImpl() {
                if constexpr (std::is_same_v<TTag, TFirst>) {
                    return 0;
                } else {
                    return (TFirst::IsFixed ? sizeof(typename TFirst::TValue) : 0u) + GetFixedOffsetImpl<TTag, TRest...>();
                }
            }

            template <class TTag>
            static consteval size_t GetVariableIndexImpl() {
                return 0;
            }

            template <class TTag, class TFirst, class... TRest>
            static consteval size_t GetVariableIndexImpl() {
                if constexpr (std::is_same_v<TTag, TFirst>) {
                    return 0;
                } else {
                    return (TFirst::IsVariable ? 1u : 0u) + GetVariableIndexImpl<TTag, TRest...>();
                }
            }

            template <class TField, class F>
            static void ForEachVariableFieldImpl(F& f) {
                if constexpr (TField::IsVariable) {
                    f.template operator()<TField>();
                }
            }
        };

        template <class... TSchemes>
        struct Versions {
            static_assert(sizeof...(TSchemes) > 0, "At least one scheme version is required");

            using TLatestScheme = typename NFlatEventDetail::TLastType<TSchemes...>::Type;
            static constexpr ui16 VersionCount = sizeof...(TSchemes);

            template <class F>
            static decltype(auto) Dispatch(ui16 version, F&& f) {
                return DispatchImpl<1, TSchemes...>(version, std::forward<F>(f));
            }

        private:
            template <ui16 Version, class TScheme, class... TRest, class F>
            static decltype(auto) DispatchImpl(ui16 version, F&& f) {
                if (version == Version) {
                    return f.template operator()<TScheme>();
                }

                if constexpr (sizeof...(TRest) > 0) {
                    return DispatchImpl<Version + 1, TRest...>(version, std::forward<F>(f));
                } else {
                    Y_ENSURE(false, "Unsupported flat event version " << version);
                }
            }
        };

        template <class TTag>
        struct TRepeatedFieldSize {
            using TFieldTag = TTag;

            explicit TRepeatedFieldSize(size_t size)
                : Size(size)
            {}

            size_t Size;
        };

    public:
        using THandle = TEventHandle<TEv>;
        using TPtr = typename THandle::TPtr;

        ui32 Type() const override {
            return TEv::EventType;
        }

        TString ToStringHeader() const override {
            return TypeName<TEv>();
        }

        TString ToString() const override {
            return TStringBuilder() << TypeName<TEv>() << " { version# " << Version << " size# " << Storage.size() << " }";
        }

        bool SerializeToArcadiaStream(TChunkSerializer* serializer) const override {
            return serializer->WriteString(&Storage);
        }

        bool IsSerializable() const override {
            return true;
        }

        ui32 CalculateSerializedSize() const override {
            return Storage.size();
        }

        ui32 CalculateSerializedSizeCached() const override {
            return Storage.size();
        }

        ui16 GetVersion() const {
            return Version;
        }

        size_t GetSerializedSize() const {
            return Storage.size();
        }

        template <class... TSizes>
        static TEv* MakeEvent(TSizes... sizes) {
            using TVersions = typename TEv::TScheme;
            using TLatestScheme = typename TVersions::TLatestScheme;

            static_assert(sizeof...(TSizes) == TLatestScheme::VariableFieldCount, "Unexpected number of repeated field sizes");
            static_assert(((requires {
                typename std::decay_t<TSizes>::TFieldTag;
                std::declval<std::decay_t<TSizes>>().Size;
            }) && ...), "Unsupported MakeEvent argument");
            static_assert((TLatestScheme::template HasVariableField<typename std::decay_t<TSizes>::TFieldTag> && ...),
                "MakeEvent argument doesn't match array field in latest scheme");

            const size_t totalSize = ComputeTotalSize<TLatestScheme>(sizes...);

            THolder<TEv> holder(new TEv());
            holder->Storage = TString::Uninitialized(totalSize);
            char* data = holder->Storage.Detach();
            memset(data, 0, totalSize);

            holder->Version = TVersions::VersionCount;
            WriteUnaligned<ui16>(data, holder->Version);

            size_t metaOffset = sizeof(ui16) + TLatestScheme::FixedFieldsSize;
            TLatestScheme::ForEachVariableField([&]<class TField>() {
                const size_t count = ExtractRepeatedFieldSize<TField>(sizes...);
                Y_ENSURE(count <= std::numeric_limits<ui32>::max(), "Repeated field size is too large");
                WriteUnaligned<ui32>(data + metaOffset, static_cast<ui32>(count));
                metaOffset += sizeof(ui32);
            });

            return holder.Release();
        }

        static TEv* Load(const TEventSerializedData* input) {
            Y_ENSURE(input, "Flat event buffer is null");

            TString storage = input->GetString();
            const ui16 version = ValidateStorage(storage);

            THolder<TEv> holder(new TEv());
            holder->Storage = std::move(storage);
            holder->Version = version;
            return holder.Release();
        }

        template <class TTag>
        bool HasField() const {
            using TVersions = typename TEv::TScheme;
            return TVersions::Dispatch(Version, []<class TScheme>() {
                return TScheme::template HasField<TTag>;
            });
        }

        template <class TTag>
        TView<typename TTag::TValue> Field() {
            static_assert(TTag::IsFixed, "Field() is only available for fixed fields");

            using TValue = typename TTag::TValue;
            const size_t offset = DispatchCurrentVersion([&]<class TScheme>() {
                if constexpr (TScheme::template HasFixedField<TTag>) {
                    return TScheme::template GetFixedOffset<TTag>();
                } else {
                    Y_ENSURE(false, "Field is absent in this event version");
                    return size_t{0};
                }
            });

            return TView<TValue>(MutableData() + offset);
        }

        template <class TTag>
        TConstView<typename TTag::TValue> Field() const {
            static_assert(TTag::IsFixed, "Field() is only available for fixed fields");

            using TValue = typename TTag::TValue;
            const size_t offset = DispatchCurrentVersion([&]<class TScheme>() {
                if constexpr (TScheme::template HasFixedField<TTag>) {
                    return TScheme::template GetFixedOffset<TTag>();
                } else {
                    Y_ENSURE(false, "Field is absent in this event version");
                    return size_t{0};
                }
            });

            return TConstView<TValue>(Data() + offset);
        }

        template <class TTag>
        TArrayView<typename TTag::TValue> Array() {
            static_assert(TTag::IsVariable, "Array() is only available for array fields");

            using TValue = typename TTag::TValue;
            const size_t offset = DispatchCurrentVersion([&]<class TScheme>() {
                if constexpr (TScheme::template HasVariableField<TTag>) {
                    return GetVariablePayloadOffset<TScheme, TTag>();
                } else {
                    Y_ENSURE(false, "Field is absent in this event version");
                    return size_t{0};
                }
            });

            return TArrayView<TValue>(MutableData() + offset, GetSize<TTag>());
        }

        template <class TTag>
        TConstArrayView<typename TTag::TValue> Array() const {
            static_assert(TTag::IsVariable, "Array() is only available for array fields");

            using TValue = typename TTag::TValue;
            const size_t offset = DispatchCurrentVersion([&]<class TScheme>() {
                if constexpr (TScheme::template HasVariableField<TTag>) {
                    return GetVariablePayloadOffset<TScheme, TTag>();
                } else {
                    Y_ENSURE(false, "Field is absent in this event version");
                    return size_t{0};
                }
            });

            return TConstArrayView<TValue>(Data() + offset, GetSize<TTag>());
        }

        template <class TTag>
        size_t GetSize() const {
            static_assert(TTag::IsVariable, "GetSize() is only available for array fields");

            return DispatchCurrentVersion([&]<class TScheme>() -> size_t {
                if constexpr (TScheme::template HasVariableField<TTag>) {
                    return GetStoredCount<TScheme, TTag>();
                } else {
                    Y_ENSURE(false, "Field is absent in this event version");
                    return 0;
                }
            });
        }

    protected:
        TEventFlat() = default;

    private:
        template <class TScheme, class... TSizes>
        static size_t ComputeTotalSize(TSizes... sizes) {
            size_t total = 0;
            total = TScheme::FixedPartSize;

            TScheme::ForEachVariableField([&]<class TField>() {
                const size_t count = ExtractRepeatedFieldSize<TField>(sizes...);
                const size_t fieldBytes = CheckedFieldBytes<typename TField::TValue>(count);
                Y_ENSURE(total <= std::numeric_limits<size_t>::max() - fieldBytes, "Flat event is too large");
                total += fieldBytes;
            });

            return total;
        }

        template <class T, class... TSizes>
        static size_t ExtractRepeatedFieldSize(TSizes... sizes) {
            size_t result = 0;
            bool found = false;

            auto extract = [&](const auto& item) {
                using TArg = std::decay_t<decltype(item)>;
                if constexpr (requires { typename TArg::TFieldTag; item.Size; }) {
                    if constexpr (std::is_same_v<typename TArg::TFieldTag, T>) {
                        result = item.Size;
                        found = true;
                    }
                }
            };

            (extract(sizes), ...);
            Y_ENSURE(found, "Missing repeated field size");
            return result;
        }

        template <class TValue>
        static size_t CheckedFieldBytes(size_t count) {
            Y_ENSURE(count <= std::numeric_limits<size_t>::max() / sizeof(TValue), "Flat event field is too large");
            return count * sizeof(TValue);
        }

        static ui16 ValidateStorage(const TString& storage) {
            Y_ENSURE(storage.size() >= sizeof(ui16), "Flat event payload is too short");

            const ui16 version = ReadUnaligned<ui16>(storage.data());
            using TVersions = typename TEv::TScheme;

            TVersions::Dispatch(version, [&]<class TScheme>() {
                ValidateStorageForScheme<TScheme>(storage);
            });

            return version;
        }

        template <class TScheme>
        static void ValidateStorageForScheme(const TString& storage) {
            Y_ENSURE(storage.size() >= TScheme::FixedPartSize, "Flat event payload is too short for this scheme");

            size_t expected = TScheme::FixedPartSize;
            const char* data = storage.data();

            TScheme::ForEachVariableField([&]<class TField>() {
                const ui32 count = ReadUnaligned<ui32>(data + TScheme::template GetVariableMetaOffset<TField>());
                const size_t fieldBytes = CheckedFieldBytes<typename TField::TValue>(count);
                Y_ENSURE(expected <= std::numeric_limits<size_t>::max() - fieldBytes, "Flat event payload is too large");
                expected += fieldBytes;
            });

            Y_ENSURE(expected == storage.size(),
                "Flat event payload size mismatch, expected " << expected << " got " << storage.size());
        }

        template <class F>
        decltype(auto) DispatchCurrentVersion(F&& f) const {
            using TVersions = typename TEv::TScheme;
            return TVersions::Dispatch(Version, std::forward<F>(f));
        }

        template <class TScheme, class TTag>
        ui32 GetStoredCount() const {
            return ReadUnaligned<ui32>(Data() + TScheme::template GetVariableMetaOffset<TTag>());
        }

        template <class TScheme, class TTag>
        size_t GetVariablePayloadOffset() const {
            size_t offset = TScheme::FixedPartSize;
            bool found = false;

            TScheme::ForEachVariableField([&]<class TField>() {
                if (found) {
                    return;
                }

                if constexpr (std::is_same_v<TField, TTag>) {
                    found = true;
                } else {
                    offset += CheckedFieldBytes<typename TField::TValue>(GetStoredCount<TScheme, TField>());
                }
            });

            Y_ENSURE(found, "Array field is absent in this event version");
            return offset;
        }

        char* MutableData() {
            return Storage.Detach();
        }

        const char* Data() const {
            return Storage.data();
        }

    private:
        TString Storage;
        ui16 Version = 0;
    };

} // namespace NActors
