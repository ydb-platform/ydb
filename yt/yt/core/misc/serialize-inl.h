#ifndef SERIALIZE_INL_H_
#error "Direct inclusion of this file is not allowed, include serialize.h"
// For the sake of sane code completion.
#include "serialize.h"
#endif

#include "collection_helpers.h"
#include "maybe_inf.h"

#include <yt/yt/core/phoenix/concepts.h>

#include <yt/yt/core/yson/string.h>

#include <library/cpp/yt/small_containers/compact_vector.h>
#include <library/cpp/yt/small_containers/compact_flat_map.h>
#include <library/cpp/yt/small_containers/compact_set.h>

#include <library/cpp/yt/containers/enum_indexed_array.h>

#include <optional>
#include <variant>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <class TInput>
void ReadRef(TInput& input, TMutableRef ref)
{
    auto bytesLoaded = input.Load(ref.Begin(), ref.Size());
    if (bytesLoaded != ref.Size()) {
        TCrashOnDeserializationErrorGuard::OnError();
        THROW_ERROR_EXCEPTION("Premature end-of-stream")
            << TErrorAttribute("bytes_loaded", bytesLoaded)
            << TErrorAttribute("bytes_expected", ref.Size());
    }
}

inline void ReadRef(const char*& ptr, TMutableRef ref)
{
    memcpy(ref.Begin(), ptr, ref.Size());
    ptr += ref.Size();
}

template <class TOutput>
void WriteRef(TOutput& output, TRef ref)
{
    output.Write(ref.Begin(), ref.Size());
}

inline void WriteRef(char*& ptr, TRef ref)
{
    memcpy(ptr, ref.Begin(), ref.Size());
    ptr += ref.Size();
}

template <class TOutput, class T>
void WritePod(TOutput& output, const T& obj)
{
    static_assert(TTypeTraits<T>::IsPod || std::is_trivial_v<T>, "T must be a pod-type.");
    static_assert(
        std::has_unique_object_representations_v<T> ||
        std::is_same_v<T, float> ||
        std::is_same_v<T, double>,
        "T must have unique representations.");

    output.Write(&obj, sizeof(obj));
}

template <class T>
void WritePod(char*& ptr, const T& obj)
{
    static_assert(TTypeTraits<T>::IsPod || std::is_trivial_v<T>, "T must be a pod-type.");
    memcpy(ptr, &obj, sizeof(obj));
    ptr += sizeof(obj);
}

template <class TOutput>
void WriteZeroes(TOutput& output, size_t count)
{
    size_t bytesWritten = 0;
    while (bytesWritten < count) {
        size_t bytesToWrite = Min(ZeroBufferSize, count - bytesWritten);
        output.Write(ZeroBuffer.data(), bytesToWrite);
        bytesWritten += bytesToWrite;
    }
    YT_VERIFY(bytesWritten == count);
}

inline void WriteZeroes(char*& ptr, size_t count)
{
    memset(ptr, 0, count);
    ptr += count;
}

template <class TOutput>
void WritePadding(TOutput& output, size_t sizeToPad)
{
    output.Write(ZeroBuffer.data(), AlignUpSpace(sizeToPad, SerializationAlignment));
}

inline void WritePadding(char*& ptr, size_t sizeToPad)
{
    size_t count = AlignUpSpace(sizeToPad, SerializationAlignment);
    memset(ptr, 0, count);
    ptr += count;
}

template <class TInput>
void ReadPadding(TInput& input, size_t sizeToPad)
{
    auto bytesToSkip = AlignUpSpace(sizeToPad, SerializationAlignment);
    auto bytesSkipped = input.Skip(bytesToSkip);
    if (bytesSkipped != bytesToSkip) {
        TCrashOnDeserializationErrorGuard::OnError();
        THROW_ERROR_EXCEPTION("Premature end-of-stream")
            << TErrorAttribute("bytes_skipped", bytesSkipped)
            << TErrorAttribute("bytes_expected", bytesToSkip);
    }
}

inline void ReadPadding(const char*& ptr, size_t sizeToPad)
{
    ptr += AlignUpSpace(sizeToPad, SerializationAlignment);
}

template <class TInput, class T>
void ReadPod(TInput& input, T& obj)
{
    static_assert(TTypeTraits<T>::IsPod || std::is_trivial_v<T>, "T must be a pod-type.");
    ReadRef(input, TMutableRef::FromPod(obj));
}

template <class T>
void ReadPod(const char*& ptr, T& obj)
{
    static_assert(TTypeTraits<T>::IsPod || std::is_trivial_v<T>, "T must be a pod-type.");
    memcpy(&obj, ptr, sizeof(obj));
    ptr += sizeof(obj);
}

////////////////////////////////////////////////////////////////////////////////

template <class T>
TSharedRef PackRefs(const T& parts)
{
    size_t size = 0;

    // Number of bytes to hold vector size.
    size += sizeof(i32);
    // Number of bytes to hold ref sizes.
    size += sizeof(i64) * parts.size();
    // Number of bytes to hold refs.
    for (const auto& ref : parts) {
        size += ref.Size();
    }

    struct TPackedRefsTag { };
    auto result = TSharedMutableRef::Allocate<TPackedRefsTag>(size, {.InitializeStorage = false});
    TMemoryOutput output(result.Begin(), result.Size());

    WritePod(output, static_cast<i32>(parts.size()));
    for (const auto& ref : parts) {
        WritePod(output, static_cast<i64>(ref.Size()));
        WriteRef(output, ref);
    }

    return result;
}

template <class T>
void UnpackRefs(const TSharedRef& packedRef, T* parts);

template <class T>
void UnpackRefs(const TSharedRef& packedRef, T* parts);

template <class TTag, class TParts>
TSharedRef MergeRefsToRef(const TParts& parts)
{
    size_t size = GetByteSize(parts);
    auto mergedRef = TSharedMutableRef::Allocate<TTag>(size, {.InitializeStorage = false});
    MergeRefsToRef(parts, mergedRef);
    return mergedRef;
}

template <class TParts>
void MergeRefsToRef(const TParts& parts, TMutableRef dst)
{
    char* current = dst.Begin();
    for (const auto& part : parts) {
        std::copy(part.Begin(), part.End(), current);
        current += part.Size();
    }
    YT_VERIFY(current == dst.End());
}

template <class TParts>
TString MergeRefsToString(const TParts& parts)
{
    size_t size = GetByteSize(parts);
    TString packedString;
    packedString.reserve(size);
    for (const auto& part : parts) {
        packedString.append(part.Begin(), part.End());
    }
    return packedString;
}

template <class T>
void UnpackRefs(const TSharedRef& packedRef, T* parts)
{
    TMemoryInput input(packedRef.Begin(), packedRef.Size());

    i32 size = 0;
    ReadPod(input, size);
    if (size < 0) {
        TCrashOnDeserializationErrorGuard::OnError();
        THROW_ERROR_EXCEPTION("Packed ref size is negative")
            << TErrorAttribute("size", size);
    }

    parts->clear();
    parts->reserve(size);

    for (int index = 0; index < size; ++index) {
        i64 partSize = 0;
        ReadPod(input, partSize);
        if (partSize < 0) {
            TCrashOnDeserializationErrorGuard::OnError();
            THROW_ERROR_EXCEPTION("A part of a packed ref has negative size")
                << TErrorAttribute("index", index)
                << TErrorAttribute("size", partSize);
        }
        if (packedRef.End() - input.Buf() < partSize) {
            TCrashOnDeserializationErrorGuard::OnError();
            THROW_ERROR_EXCEPTION("A part of a packed ref is too large")
                << TErrorAttribute("index", index)
                << TErrorAttribute("size", partSize)
                << TErrorAttribute("bytes_left", packedRef.End() - input.Buf());
        }

        parts->push_back(packedRef.Slice(input.Buf(), input.Buf() + partSize));

        input.Skip(partSize);
    }

    if (input.Buf() < packedRef.End()) {
        TCrashOnDeserializationErrorGuard::OnError();
        THROW_ERROR_EXCEPTION("Packed ref is too large")
            << TErrorAttribute("extra_bytes", packedRef.End() - input.Buf());
    }
}

inline std::vector<TSharedRef> UnpackRefs(const TSharedRef& packedRef)
{
    std::vector<TSharedRef> parts;
    UnpackRefs(packedRef, &parts);
    return parts;
}

////////////////////////////////////////////////////////////////////////////////

Y_FORCE_INLINE void TSaveContextStream::Write(const void* buf, size_t len)
{
    if (Y_LIKELY(BufferRemaining_ >= len)) {
        ::memcpy(BufferPtr_, buf, len);
        BufferPtr_ += len;
        BufferRemaining_ -= len;
    } else {
        WriteSlow(buf, len);
    }
}

////////////////////////////////////////////////////////////////////////////////

Y_FORCE_INLINE TSaveContextStream* TStreamSaveContext::GetOutput()
{
    return &Output_;
}

Y_FORCE_INLINE int TStreamSaveContext::GetVersion() const
{
    return Version_;
}

////////////////////////////////////////////////////////////////////////////////

Y_FORCE_INLINE size_t TLoadContextStream::Load(void* buf, size_t len)
{
    if (Y_LIKELY(BufferRemaining_ >= len)) {
        ::memcpy(buf, BufferPtr_, len);
        BufferPtr_ += len;
        BufferRemaining_ -= len;
        return len;
    } else {
        return LoadSlow(buf, len);
    }
}

////////////////////////////////////////////////////////////////////////////////

Y_FORCE_INLINE TLoadContextStream* TStreamLoadContext::GetInput()
{
    return &Input_;
}

////////////////////////////////////////////////////////////////////////////////

inline TEntitySerializationKey TEntityStreamSaveContext::GenerateSerializationKey()
{
    YT_VERIFY(!ParentContext_);
    return TEntitySerializationKey(SerializationKeyIndex_++);
}

template <class T>
TEntitySerializationKey TEntityStreamSaveContext::RegisterRawEntity(T* entity)
{
    if (ParentContext_) {
        return GetOrCrash(ParentContext_->RawPtrs_, entity);
    }

    if (auto it = RawPtrs_.find(entity)) {
        return it->second;
    }
    auto key = GenerateSerializationKey();
    EmplaceOrCrash(RawPtrs_, entity, key);
    return InlineKey;
}

template <class T>
TEntitySerializationKey TEntityStreamSaveContext::RegisterRefCountedEntity(const TIntrusivePtr<T>& entity)
{
    if (ParentContext_) {
        return GetOrCrash(ParentContext_->RefCountedPtrs_, entity);
    }

    if (auto it = RefCountedPtrs_.find(entity)) {
        return it->second;
    }

    auto key = GenerateSerializationKey();
    EmplaceOrCrash(RefCountedPtrs_, entity, key);
    return InlineKey;
}

////////////////////////////////////////////////////////////////////////////////

template <class T>
inline TEntitySerializationKey TEntityStreamLoadContext::RegisterRawEntity(T* entity)
{
    YT_VERIFY(!ParentContext_);
    auto key = TEntitySerializationKey(std::ssize(RawPtrs_));
    RawPtrs_.push_back(entity);
    return key;
}

template <class T>
TEntitySerializationKey TEntityStreamLoadContext::RegisterRefCountedEntity(const TIntrusivePtr<T>& entity)
{
    YT_VERIFY(!ParentContext_);
    auto* ptr = entity.Get();
    RefCountedPtrs_.push_back(entity);
    return RegisterRawEntity(ptr);
}

template <class T>
T* TEntityStreamLoadContext::GetRawEntity(TEntitySerializationKey key) const
{
    if (ParentContext_) {
        return ParentContext_->GetRawEntity<T>(key);
    }

    auto index = key.Underlying();
    YT_ASSERT(index >= 0);
    YT_ASSERT(index < std::ssize(RawPtrs_));
    return static_cast<T*>(RawPtrs_[index]);
}

template <class T>
TIntrusivePtr<T> TEntityStreamLoadContext::GetRefCountedEntity(TEntitySerializationKey key) const
{
    return TIntrusivePtr<T>(GetRawEntity<T>(key));
}

////////////////////////////////////////////////////////////////////////////////

template <class TSaveContext, class TLoadContext, class TSnapshotVersion>
TCustomPersistenceContext<TSaveContext, TLoadContext, TSnapshotVersion>::TCustomPersistenceContext(TSaveContext& saveContext)
    : SaveContext_(&saveContext)
{ }

template <class TSaveContext, class TLoadContext, class TSnapshotVersion>
TCustomPersistenceContext<TSaveContext, TLoadContext, TSnapshotVersion>::TCustomPersistenceContext(TLoadContext& loadContext)
    : LoadContext_(&loadContext)
{ }

template <class TSaveContext, class TLoadContext, class TSnapshotVersion>
bool TCustomPersistenceContext<TSaveContext, TLoadContext, TSnapshotVersion>::IsSave() const
{
    return SaveContext_ != nullptr;
}

template <class TSaveContext, class TLoadContext, class TSnapshotVersion>
TSaveContext& TCustomPersistenceContext<TSaveContext, TLoadContext, TSnapshotVersion>::SaveContext() const
{
    YT_ASSERT(SaveContext_);
    return *SaveContext_;
}

template <class TSaveContext, class TLoadContext, class TSnapshotVersion>
bool TCustomPersistenceContext<TSaveContext, TLoadContext, TSnapshotVersion>::IsLoad() const
{
    return LoadContext_ != nullptr;
}

template <class TSaveContext, class TLoadContext, class TSnapshotVersion>
TLoadContext& TCustomPersistenceContext<TSaveContext, TLoadContext, TSnapshotVersion>::LoadContext() const
{
    YT_ASSERT(LoadContext_);
    return *LoadContext_;
}

template <class TSaveContext, class TLoadContext, class TSnapshotVersion>
template <class TOtherContext>
TCustomPersistenceContext<TSaveContext, TLoadContext, TSnapshotVersion>::operator TOtherContext() const
{
    return IsSave() ? TOtherContext(*SaveContext_) : TOtherContext(*LoadContext_);
}

template <class TSaveContext, class TLoadContext, class TSnapshotVersion>
TSnapshotVersion TCustomPersistenceContext<TSaveContext, TLoadContext, TSnapshotVersion>::GetVersion() const
{
    return IsSave() ? SaveContext().GetVersion() : LoadContext().GetVersion();
}

////////////////////////////////////////////////////////////////////////////////

template <class T, class C, class... TArgs>
void Save(C& context, const T& value, TArgs&&... args)
{
    TSerializerTraits<T, C>::TSerializer::Save(context, value, std::forward<TArgs>(args)...);
}

template <class T, class C, class... TArgs>
void Load(C& context, T& value, TArgs&&... args)
{
    TSerializerTraits<T, C>::TSerializer::Load(context, value, std::forward<TArgs>(args)...);
}

template <class T, class C, class... TArgs>
T Load(C& context, TArgs&&... args)
{
    T value{};
    Load(context, value, std::forward<TArgs>(args)...);
    return value;
}

template <class T, class C, class... TArgs>
T LoadSuspended(C& context, TArgs&&... args)
{
    SERIALIZATION_DUMP_SUSPEND(context) {
        return Load<T, C, TArgs...>(context, std::forward<TArgs>(args)...);
    }
}

template <class S, class T, class C>
void Persist(const C& context, T& value)
{
    if (context.IsSave()) {
        S::Save(context.SaveContext(), value);
    } else if (context.IsLoad()) {
        S::Load(context.LoadContext(), value);
    } else {
        YT_ABORT();
    }
}

struct TDefaultSerializer;

template <class T, class C>
void Persist(const C& context, T& value)
{
    Persist<TDefaultSerializer, T, C>(context, value);
}

////////////////////////////////////////////////////////////////////////////////

template <class T>
struct TPersistMemberTraits
{
    template <class S>
    struct TSignatureCracker
    { };

    template <class U, class C>
    struct TSignatureCracker<void (U::*)(C&)>
    {
        using TContext = C;
    };

    using TContext = typename TSignatureCracker<decltype(&T::Persist)>::TContext;
};

////////////////////////////////////////////////////////////////////////////////
// Simple types

struct TValueBoundSerializer
{
    template <class T, class C, class = void>
    struct TSaver
    { };

    template <class T, class C, class = void>
    struct TLoader
    { };

    template <class T, class C>
        requires (NPhoenix2::SupportsPhoenix2<T> || !NPhoenix2::SupportsPersist<T, C>)
    struct TSaver<
        T,
        C,
        decltype(std::declval<const T&>().Save(std::declval<C&>()), void())
        >
    {
        static void Do(C& context, const T& value)
        {
            value.Save(context);
        }
    };

    template <class T, class C>
        requires (NPhoenix2::SupportsPhoenix2<T> || !NPhoenix2::SupportsPersist<T, C>)
    struct TLoader<
        T,
        C,
        decltype(std::declval<T&>().Load(std::declval<C&>()), void())>
    {
        static void Do(C& context, T& value)
        {
            value.Load(context);
        }
    };

    template <class T, class C>
        requires (!NPhoenix2::SupportsPhoenix2<T>)
    struct TSaver<
        T,
        C,
        decltype(std::declval<T&>().Persist(std::declval<C&>()), void())>
    {
        static void Do(C& context, const T& value)
        {
            const_cast<T&>(value).Persist(context);
        }
    };

    template <class T, class C>
        requires (!NPhoenix2::SupportsPhoenix2<T>)
    struct TLoader<
        T,
        C,
        decltype(std::declval<T&>().Persist(std::declval<C&>()), void())>
    {
        static void Do(C& context, T& value)
        {
            value.Persist(context);
        }
    };


    template <class T, class C>
    static void Save(C& context, const T& value)
    {
        // If you see an "ambiguous partial specializations of 'TSaver'" error then
        // you've probably defined both Persist and Save methods. Remove one.
        //
        // If you see a "no member named 'Do' in 'TSaver'" then
        //  - either you're missing both Persist and Save methods on T
        //  - or the method arguments are wrong (hint: Persist takes a const ref).
        TSaver<T, C>::Do(context, value);
    }

    template <class T, class C>
    static void Load(C& context, T& value)
    {
        // If you see an "ambiguous partial specializations of TLoader" error then
        // you've probably defined both Persist and Load methods. Remove one.
        //
        // If you see a "no member named 'Do' in 'TLoader'" then
        //  - either you're missing both Persist and Load methods on T
        //  - or the method arguments are wrong (hint: Persist takes a const ref).
        TLoader<T, C>::Do(context, value);
    }
};

struct TDefaultSerializer
{
    template <class T, class C>
    static void Save(C& context, const T& value)
    {
        NYT::Save(context, value);
    }

    template <class T, class C>
    static void Load(C& context, T& value)
    {
        NYT::Load(context, value);
    }
};

struct TRangeSerializer
{
    template <class C>
    Y_FORCE_INLINE static void Save(C& context, TRef value)
    {
        auto* output = context.GetOutput();
        output->Write(value.Begin(), value.Size());
    }

    template <class C>
    Y_FORCE_INLINE static void Load(C& context, const TMutableRef& value)
    {
        auto* input = context.GetInput();
        ReadRef(*input, value);
        SERIALIZATION_DUMP_WRITE(context, "raw[%v] %v", value.Size(), DumpRangeToHex(value));
    }
};

struct TPodSerializer
{
    template <class T, class C>
    Y_FORCE_INLINE static void Save(C& context, const T& value)
    {
        TRangeSerializer::Save(context, TRef::FromPod(value));
    }

    template <class T, class C>
    Y_FORCE_INLINE static void Load(C& context, T& value)
    {
        SERIALIZATION_DUMP_SUSPEND(context) {
            TRangeSerializer::Load(context, TMutableRef::FromPod(value));
        }
        TSerializationDumpPodWriter<T>::Do(context, value);
    }
};

struct TSizeSerializer
{
    template <class C>
    static void Save(C& context, size_t value)
    {
        ui32 fixedValue = static_cast<ui32>(value);
        TPodSerializer::Save(context, fixedValue);
    }

    template <class C>
    static void Load(C& context, size_t& value)
    {
        ui32 fixedValue = 0;
        TPodSerializer::Load(context, fixedValue);
        value = static_cast<size_t>(fixedValue);
    }


    // Helpers.
    template <class C>
    static size_t Load(C& context)
    {
        size_t value = 0;
        Load(context, value);
        return value;
    }

    template <class C>
    static size_t LoadSuspended(C& context)
    {
        SERIALIZATION_DUMP_SUSPEND(context) {
            return Load(context);
        }
    }
};

struct TSharedRefSerializer
{
    template <class C>
    static void Save(C& context, const TSharedRef& value)
    {
        TSizeSerializer::Save(context, value.Size());
        auto* output = context.GetOutput();
        output->Write(value.Begin(), value.Size());
    }

    template <class C>
    static void Load(C& context, TSharedRef& value)
    {
        return Load(context, value, TDefaultSharedBlobTag());
    }

    template <class C, class TTag>
    static void Load(C& context, TSharedRef& value, TTag)
    {
        size_t size = TSizeSerializer::LoadSuspended(context);
        auto mutableValue = TSharedMutableRef::Allocate<TTag>(size, {.InitializeStorage = false});

        auto* input = context.GetInput();
        ReadRef(*input, mutableValue);
        value = mutableValue;

        SERIALIZATION_DUMP_WRITE(context, "TSharedRef %v", DumpRangeToHex(value));
    }
};

struct TSharedRefArraySerializer
{
    template <class C>
    static void Save(C& context, const TSharedRefArray& value)
    {
        TSizeSerializer::Save(context, value.Size());

        for (const auto& part : value) {
            NYT::Save(context, part);
        }
    }

    template <class C>
    static void Load(C& context, TSharedRefArray& value)
    {
        size_t size = TSizeSerializer::LoadSuspended(context);
        std::vector<TSharedRef> parts(size);

        SERIALIZATION_DUMP_WRITE(context, "TSharedRefArray[%v]", size);
        SERIALIZATION_DUMP_INDENT(context) {
            for (int index = 0; index < static_cast<int>(size); ++index) {
                SERIALIZATION_DUMP_SUSPEND(context) {
                    NYT::Load(context, parts[index]);
                }
                SERIALIZATION_DUMP_WRITE(context, "%v => %v", index, DumpRangeToHex(parts[index]));
            }
        }

        value = TSharedRefArray(std::move(parts), TSharedRefArray::TMoveParts{});
    }
};

struct TEnumSerializer
{
    template <class T, class C>
    static void Save(C& context, const T& value)
    {
        NYT::Save(context, static_cast<i32>(value));
    }

    template <class T, class C>
    static void Load(C& context, T& value)
    {
        SERIALIZATION_DUMP_SUSPEND(context) {
            value = T(NYT::Load<i32>(context));
        }

        SERIALIZATION_DUMP_WRITE(context, "%v %v", TEnumTraits<T>::GetTypeName(), value);
    }
};

struct TStringSerializer
{
    template <class T, class C>
    static void Save(C& context, const T& value)
    {
        TSizeSerializer::Save(context, value.size());

        TRangeSerializer::Save(context, TRef::FromStringBuf(value));
    }

    template <class T, class C>
    static void Load(C& context, T& value)
    {
        size_t size = TSizeSerializer::LoadSuspended(context);
        value.resize(size);

        SERIALIZATION_DUMP_SUSPEND(context) {
            TRangeSerializer::Load(context, TMutableRef::FromString(value));
        }

        SERIALIZATION_DUMP_WRITE(context, "string %Qv", value);
    }
};

template <class TUnderlyingSerializer = TDefaultSerializer>
struct TOptionalSerializer
{
    template <class T, class C>
    static void Save(C& context, const std::optional<T>& optional)
    {
        using NYT::Save;

        Save(context, optional.operator bool());

        if (optional) {
            TUnderlyingSerializer::Save(context, *optional);
        }
    }

    template <class T, class C>
    static void Load(C& context, std::optional<T>& optional)
    {
        using NYT::Load;

        bool hasValue = LoadSuspended<bool>(context);

        if (hasValue) {
            T temp{};
            TUnderlyingSerializer::Load(context, temp);
            optional = std::move(temp);
        } else {
            optional.reset();
            SERIALIZATION_DUMP_WRITE(context, "null");
        }
    }
};

template <size_t Index, class... Ts>
struct TVariantSerializerTraits;

template <size_t Index, class T, class... Ts>
struct TVariantSerializerTraits<Index, T, Ts...>
{
    template <class C, class V>
    static void Save(C& context, const V& variant)
    {
        if (Index == variant.index()) {
            NYT::Save(context, std::get<Index>(variant));
        } else {
            TVariantSerializerTraits<Index + 1, Ts...>::Save(context, variant);
        }
    }

    template <class C, class V>
    static void Load(C& context, size_t index, V& variant)
    {
        if (Index == index) {
            variant = V(std::in_place_index_t<Index>());
            NYT::Load(context, std::get<Index>(variant));
        } else {
            TVariantSerializerTraits<Index + 1, Ts...>::Load(context, index, variant);
        }
    }
};

template <size_t Index>
struct TVariantSerializerTraits<Index>
{
    template <class C, class V>
    static void Save(C& /*context*/, const V& /*variant*/)
    {
        // Invalid variant index.
        YT_ABORT();
    }

    template <class C, class V>
    static void Load(C& /*context*/, size_t /*index*/, V& /*variant*/)
    {
        // Invalid variant index.
        YT_ABORT();
    }
};

struct TVariantSerializer
{
    template <class... Ts, class C>
    static void Save(C& context, const std::variant<Ts...>& variant)
    {
        NYT::Save<ui32>(context, variant.index());
        TVariantSerializerTraits<0, Ts...>::Save(context, variant);
    }

    template <class... Ts, class C>
    static void Load(C& context, std::variant<Ts...>& variant)
    {
        size_t index = NYT::Load<ui32>(context);
        TVariantSerializerTraits<0, Ts...>::Load(context, index, variant);
    }
};

template <class TUnderlyingSerializer = TDefaultSerializer>
struct TAtomicSerializer
{
    template <class T, class C>
    static void Save(C& context, const std::atomic<T>& value)
    {
        TUnderlyingSerializer::Save(context, value.load());
    }

    template <class T, class C>
    static void Load(C& context, std::atomic<T>& value)
    {
        T temp;
        TUnderlyingSerializer::Load(context, temp);
        value.store(std::move(temp));
    }
};

////////////////////////////////////////////////////////////////////////////////
// Sorters

template <class T, class C>
class TNoopSorter
{
public:
    using TIterator = typename T::const_iterator;

    explicit TNoopSorter(const T& any)
        : Any_(any)
    { }

    TIterator begin()
    {
        return Any_.begin();
    }

    TIterator end()
    {
        return Any_.end();
    }

private:
    const T& Any_;

};

template <class C>
struct TValueSorterComparer
{
    template <class TIterator>
    static bool Compare(TIterator lhs, TIterator rhs)
    {
        using T = typename std::remove_const<typename std::remove_reference<decltype(*lhs)>::type>::type;
        using TComparer = typename TSerializerTraits<T, C>::TComparer;
        return TComparer::Compare(*lhs, *rhs);
    }
};

template <class C>
struct TKeySorterComparer
{
    template <class TIterator>
    static bool Compare(TIterator lhs, TIterator rhs)
    {
        using T = typename std::remove_const<typename std::remove_reference<decltype(lhs->first)>::type>::type;
        using TComparer = typename TSerializerTraits<T, C>::TComparer;
        return TComparer::Compare(lhs->first, rhs->first);
    }
};

template <class C>
struct TKeyValueSorterComparer
{
    template <class TIterator>
    static bool Compare(TIterator lhs, TIterator rhs)
    {
        using TKey = typename std::remove_const<typename std::remove_reference<decltype(lhs->first)>::type>::type;
        using TValue = typename std::remove_const<typename std::remove_reference<decltype(lhs->second)>::type>::type;
        using TKeyComparer = typename TSerializerTraits<TKey, C>::TComparer;
        using TValueComparer = typename TSerializerTraits<TValue, C>::TComparer;
        if (TKeyComparer::Compare(lhs->first, rhs->first)) {
            return true;
        }
        if (TKeyComparer::Compare(rhs->first, lhs->first)) {
            return false;
        }
        return TValueComparer::Compare(lhs->second, rhs->second);
    }
};

template <class T, class Q>
class TCollectionSorter
{
public:
    using TIterator = typename T::const_iterator;
    using TIterators = TCompactVector<TIterator, 16>;

    class TIteratorWrapper
    {
    public:
        TIteratorWrapper(const TIterators* iterators, size_t index)
            : Iterators_(iterators)
            , Index_(index)
        { }

        const typename T::value_type& operator * ()
        {
            return *((*Iterators_)[Index_]);
        }

        TIteratorWrapper& operator ++ ()
        {
            ++Index_;
            return *this;
        }

        bool operator == (const TIteratorWrapper& other) const
        {
            return Index_ == other.Index_;
        }

    private:
        const TIterators* const Iterators_;
        size_t Index_;
    };

    explicit TCollectionSorter(const T& set)
    {
        Iterators_.reserve(set.size());
        for (auto it = set.begin(); it != set.end(); ++it) {
            Iterators_.push_back(it);
        }

        std::sort(
            Iterators_.begin(),
            Iterators_.end(),
            [] (TIterator lhs, TIterator rhs) {
                return Q::Compare(lhs, rhs);
            });
    }

    TIteratorWrapper begin() const
    {
        return TIteratorWrapper(&Iterators_, 0);
    }

    TIteratorWrapper end() const
    {
        return TIteratorWrapper(&Iterators_, Iterators_.size());
    }

private:
    TIterators Iterators_;
};

struct TSortedTag { };
struct TUnsortedTag { };

template <class T, class C, class TTag>
struct TSorterSelector
{ };

template <class T, class C>
struct TSorterSelector<T, C, TUnsortedTag>
{
    using TSorter = TNoopSorter<T, C>;
};

template <class T, class C>
struct TSorterSelector<std::vector<T>, C, TSortedTag>
{
    using TSorter = TCollectionSorter<std::vector<T>, TValueSorterComparer<C>>;
};

template <class T, class C, size_t N>
struct TSorterSelector<TCompactVector<T, N>, C, TSortedTag>
{
    using TSorter = TCollectionSorter<TCompactVector<T, N>, TValueSorterComparer<C>>;
};

template <class C, class... T>
struct TSorterSelector<std::set<T...>, C, TSortedTag>
{
    using TSorter = TNoopSorter<std::set<T...>, C>;
};

template <class C, class... T>
struct TSorterSelector<std::map<T...>, C, TSortedTag>
{
    using TSorter = TNoopSorter<std::map<T...>, C>;
};

template <class C, class... T>
struct TSorterSelector<std::unordered_set<T...>, C, TSortedTag>
{
    using TSorter = TCollectionSorter<std::unordered_set<T...>, TValueSorterComparer<C>>;
};

template <class C, class... T>
struct TSorterSelector<THashSet<T...>, C, TSortedTag>
{
    using TSorter = TCollectionSorter<THashSet<T...>, TValueSorterComparer<C>>;
};

template <class C, class T, size_t N, class Q>
struct TSorterSelector<TCompactSet<T, N, Q>, C, TSortedTag>
{
    using TSorter = TNoopSorter<TCompactSet<T, N, Q>, C>;
};

template <class C, class... T>
struct TSorterSelector<std::unordered_multiset<T...>, C, TSortedTag>
{
    using TSorter = TCollectionSorter<std::unordered_multiset<T...>, TValueSorterComparer<C>>;
};

template <class C, class... T>
struct TSorterSelector<THashMultiSet<T...>, C, TSortedTag>
{
    using TSorter = TCollectionSorter<THashMultiSet<T...>, TValueSorterComparer<C>>;
};

template <class C, class... T>
struct TSorterSelector<std::unordered_map<T...>, C, TSortedTag>
{
    using TSorter = TCollectionSorter<std::unordered_map<T...>, TKeySorterComparer<C>>;
};

template <class C, class... T>
struct TSorterSelector<THashMap<T...>, C, TSortedTag>
{
    using TSorter = TCollectionSorter<THashMap<T...>, TKeySorterComparer<C>>;
};

template <class K, class V, size_t N, class C>
struct TSorterSelector<TCompactFlatMap<K, V, N>, C, TSortedTag>
{
    using TSorter = TCollectionSorter<TCompactFlatMap<K, V, N>, TKeySorterComparer<C>>;
};

template <class C, class... T>
struct TSorterSelector<std::unordered_multimap<T...>, C, TSortedTag>
{
    using TSorter = TCollectionSorter<std::unordered_map<T...>, TKeyValueSorterComparer<C>>;
};

template <class C, class... T>
struct TSorterSelector<THashMultiMap<T...>, C, TSortedTag>
{
    using TSorter = TCollectionSorter<THashMultiMap<T...>, TKeyValueSorterComparer<C>>;
};

////////////////////////////////////////////////////////////////////////////////
// Ordered collections

template <
    class TItemSerializer = TDefaultSerializer,
    class TSortTag = TUnsortedTag
>
struct TVectorSerializer
{
    template <class TVectorType, class C>
    static void Save(C& context, const TVectorType& objects)
    {
        TSizeSerializer::Save(context, objects.size());

        typename TSorterSelector<TVectorType, C, TSortTag>::TSorter sorter(objects);
        for (const auto& object : sorter) {
            TItemSerializer::Save(context, object);
        }
    }

    template <class TVectorType, class C>
    static void Load(C& context, TVectorType& objects)
    {
        size_t size = TSizeSerializer::LoadSuspended(context);
        objects.resize(size);

        SERIALIZATION_DUMP_WRITE(context, "vector[%v]", size);
        SERIALIZATION_DUMP_INDENT(context) {
            for (size_t index = 0; index != size; ++index) {
                SERIALIZATION_DUMP_WRITE(context, "%v =>", index);
                SERIALIZATION_DUMP_INDENT(context) {
                    TItemSerializer::Load(context, objects[index]);
                }
            }
        }
    }
};

template <
    class TItemSerializer = TDefaultSerializer,
    class TSortTag = TUnsortedTag
>
struct TOptionalVectorSerializer
{
    template <class TVectorType, class C>
    static void Save(C& context, const std::unique_ptr<TVectorType>& objects)
    {
        if (objects) {
            TVectorSerializer<TItemSerializer, TSortTag>::Save(context, *objects);
        } else {
            TSizeSerializer::Save(context, 0);
        }
    }

    template <class TVectorType, class C>
    static void Load(C& context, std::unique_ptr<TVectorType>& objects)
    {
        size_t size = TSizeSerializer::LoadSuspended(context);
        if (size == 0) {
            objects.reset();
            return;
        }

        objects.reset(new TVectorType());
        objects->resize(size);

        SERIALIZATION_DUMP_WRITE(context, "vector[%v]", size);
        SERIALIZATION_DUMP_INDENT(context) {
            for (size_t index = 0; index != size; ++index) {
                SERIALIZATION_DUMP_WRITE(context, "%v =>", index);
                SERIALIZATION_DUMP_INDENT(context) {
                    TItemSerializer::Load(context, (*objects)[index]);
                }
            }
        }
    }
};

template <class TItemSerializer = TDefaultSerializer>
struct TListSerializer
{
    template <class TListType, class C>
    static void Save(C& context, const TListType& objects)
    {
        TSizeSerializer::Save(context, objects.size());

        for (const auto& object : objects) {
            TItemSerializer::Save(context, object);
        }
    }

    template <class TListType, class C>
    static void Load(C& context, TListType& objects)
    {
        size_t size = TSizeSerializer::LoadSuspended(context);
        objects.clear();

        SERIALIZATION_DUMP_WRITE(context, "list[%v]", size);
        SERIALIZATION_DUMP_INDENT(context) {
            for (size_t index = 0; index != size; ++index) {
                typename TListType::value_type obj;
                SERIALIZATION_DUMP_WRITE(context, "%v =>", index);
                SERIALIZATION_DUMP_INDENT(context) {
                    TItemSerializer::Load(context, obj);
                }
                objects.push_back(obj);
            }
        }
    }
};

template <class TItemSerializer = TDefaultSerializer>
struct TArraySerializer
{
    template <class TArray, class C>
    static void Save(C& context, const TArray& objects)
    {
        TSizeSerializer::Save(context, objects.size());

        for (const auto& object : objects) {
            TItemSerializer::Save(context, object);
        }
    }

    template <
        class C,
        class T,
        std::size_t N,
        template <typename U, std::size_t S> class TArray
    >
    static void Load(C& context, TArray<T,N>& objects)
    {
        size_t size = TSizeSerializer::LoadSuspended(context);
        if (size > N) {
            TCrashOnDeserializationErrorGuard::OnError();
            THROW_ERROR_EXCEPTION("Array size is too large: expected <= %v, actual %v",
                N,
                size);
        }

        SERIALIZATION_DUMP_WRITE(context, "array[%v]", size);
        SERIALIZATION_DUMP_INDENT(context) {
            for (size_t index = 0; index != size; ++index) {
                SERIALIZATION_DUMP_WRITE(context, "%v =>", index);
                SERIALIZATION_DUMP_INDENT(context) {
                    TItemSerializer::Load(context, objects[index]);
                }
            }
        }
    }
};

template <class TItemSerializer = TDefaultSerializer>
struct TOptionalListSerializer
{
    template <class TListType, class C>
    static void Save(C& context, const std::unique_ptr<TListType>& objects)
    {
        using NYT::Save;
        if (objects) {
            TListSerializer<TItemSerializer>::Save(context, *objects);
        } else {
            TSizeSerializer::Save(context, 0);
        }
    }

    template <class TListType, class C>
    static void Load(C& context, std::unique_ptr<TListType>& objects)
    {
        size_t size = TSizeSerializer::LoadSuspended(context);

        SERIALIZATION_DUMP_WRITE(context, "list[%v]", size);

        if (size == 0) {
            objects.reset();
            return;
        }

        objects.reset(new TListType());

        SERIALIZATION_DUMP_INDENT(context) {
            for (size_t index = 0; index != size; ++index) {
                typename TListType::value_type obj;
                SERIALIZATION_DUMP_WRITE(context, "%v =>", index);
                SERIALIZATION_DUMP_INDENT(context) {
                    TItemSerializer::Load(context, obj);
                }
                objects->push_back(obj);
            }
        }
    }
};

template <class TItemSerializer = TDefaultSerializer>
struct TEnumIndexedArraySerializer
{
    template <class E, class T, class C, E Min, E Max>
    static void Save(C& context, const TEnumIndexedArray<E, T, Min, Max>& vector)
    {
        using NYT::Save;

        auto keys = TEnumTraits<E>::GetDomainValues();
        size_t count = 0;
        for (auto key : keys) {
            if (!vector.IsValidIndex(key)) {
                continue;
            }
            ++count;
        }

        TSizeSerializer::Save(context, count);

        for (auto key : keys) {
            if (!vector.IsValidIndex(key)) {
                continue;
            }
            Save(context, key);
            TItemSerializer::Save(context, vector[key]);
        }
    }

    template <class E, class T, class C, E Min, E Max>
    static void Load(C& context, TEnumIndexedArray<E, T, Min, Max>& vector)
    {
        if constexpr (std::is_copy_assignable_v<T>) {
            std::fill(vector.begin(), vector.end(), T());
        } else {
            // Use move assignment.
            for (auto& item : vector) {
                item = T();
            }
        }

        size_t size = TSizeSerializer::LoadSuspended(context);

        SERIALIZATION_DUMP_WRITE(context, "vector[%v]", size);
        SERIALIZATION_DUMP_INDENT(context) {
            for (size_t index = 0; index != size; ++index) {
                auto key = LoadSuspended<E>(context);
                SERIALIZATION_DUMP_WRITE(context, "%v =>", key);
                SERIALIZATION_DUMP_INDENT(context) {
                    if (!vector.IsValidIndex(key)) {
                        T dummy;
                        TItemSerializer::Load(context, dummy);
                    } else {
                        TItemSerializer::Load(context, vector[key]);
                    }
                }
            }
        }
    }
};

////////////////////////////////////////////////////////////////////////////////
// Possibly unordered collections

template <
    class TItemSerializer = TDefaultSerializer,
    class TSortTag = TSortedTag
>
struct TSetSerializer
{
    template <class TSetType, class C>
    static void Save(C& context, const TSetType& set)
    {
        TSizeSerializer::Save(context, set.size());

        typename TSorterSelector<TSetType, C, TSortTag>::TSorter sorter(set);
        for (const auto& item : sorter) {
            TItemSerializer::Save(context, item);
        }
    }

    template <class TSetType, class C>
    static void Load(C& context, TSetType& set)
    {
        using TKey = typename TSetType::key_type;

        size_t size = TSizeSerializer::LoadSuspended(context);

        set.clear();

        SERIALIZATION_DUMP_WRITE(context, "set[%v]", size);
        SERIALIZATION_DUMP_INDENT(context) {
            for (size_t index = 0; index < size; ++index) {
                SERIALIZATION_DUMP_WRITE(context, "%v =>", index);

                TKey key;
                SERIALIZATION_DUMP_INDENT(context) {
                    TItemSerializer::Load(context, key);
                }

                InsertOrCrash(set, key);
            }
        }
    }
};

template <
    class TItemSerializer = TDefaultSerializer,
    class TSortTag = TSortedTag
>
struct TMultiSetSerializer
{
    template <class TSetType, class C>
    static void Save(C& context, const TSetType& set)
    {
        TSetSerializer<TItemSerializer, TSortTag>::Save(context, set);
    }

    template <class TSetType, class C>
    static void Load(C& context, TSetType& set)
    {
        using TKey = typename TSetType::key_type;

        size_t size = TSizeSerializer::LoadSuspended(context);

        set.clear();

        SERIALIZATION_DUMP_WRITE(context, "multiset[%v]", size);
        SERIALIZATION_DUMP_INDENT(context) {
            for (size_t index = 0; index < size; ++index) {
                SERIALIZATION_DUMP_WRITE(context, "%v =>", index);

                TKey key;
                SERIALIZATION_DUMP_INDENT(context) {
                    TItemSerializer::Load(context, key);
                }

                set.emplace(std::move(key));
            }
        }
    }
};

template <
    class TItemSerializer = TDefaultSerializer,
    class TSortTag = TSortedTag
>
struct TOptionalSetSerializer
{
    template <class TSetType, class C>
    static void Save(C& context, const std::unique_ptr<TSetType>& set)
    {
        if (set) {
            TSetSerializer<TItemSerializer, TSortTag>::Save(context, *set);
        } else {
            TSizeSerializer::Save(context, 0);
        }
    }

    template <class TSetType, class C>
    static void Load(C& context, std::unique_ptr<TSetType>& set)
    {
        using TKey = typename TSetType::key_type;

        size_t size = TSizeSerializer::LoadSuspended(context);

        SERIALIZATION_DUMP_WRITE(context, "set[%v]", size);

        if (size == 0) {
            set.reset();
            return;
        }

        set.reset(new TSetType());

        SERIALIZATION_DUMP_INDENT(context) {
            for (size_t index = 0; index < size; ++index) {
                SERIALIZATION_DUMP_WRITE(context, "%v =>", index);

                TKey key;
                SERIALIZATION_DUMP_INDENT(context) {
                    TItemSerializer::Load(context, key);
                }

                InsertOrCrash(*set, key);
            }
        }
    }
};

template <
    class TKeySerializer = TDefaultSerializer,
    class TValueSerializer = TDefaultSerializer,
    class TSortTag = TSortedTag
>
struct TMapSerializer
{
    template <class TMapType, class C>
    static void Save(C& context, const TMapType& map)
    {
        TSizeSerializer::Save(context, map.size());

        typename TSorterSelector<TMapType, C, TSortTag>::TSorter sorter(map);
        for (const auto& [key, value] : sorter) {
            TKeySerializer::Save(context, key);
            TValueSerializer::Save(context, value);
        }
    }

    template <class TMapType, class C>
    static void Load(C& context, TMapType& map)
    {
        size_t size = TSizeSerializer::LoadSuspended(context);

        SERIALIZATION_DUMP_WRITE(context, "map[%v]", size);

        map.clear();

        SERIALIZATION_DUMP_INDENT(context) {
            for (size_t index = 0; index < size; ++index) {
                typename TMapType::key_type key{};
                TKeySerializer::Load(context, key);

                SERIALIZATION_DUMP_WRITE(context, "=>");

                typename TMapType::mapped_type value{};
                SERIALIZATION_DUMP_INDENT(context) {
                    TValueSerializer::Load(context, value);
                }

                EmplaceOrCrash(map, std::move(key), std::move(value));
            }
        }
    }
};

template <
    class TKeySerializer = TDefaultSerializer,
    class TValueSerializer = TDefaultSerializer,
    class TSortTag = TSortedTag
>
struct TMultiMapSerializer
{
    template <class TMapType, class C>
    static void Save(C& context, const TMapType& map)
    {
        TMapSerializer<
            TDefaultSerializer,
            TDefaultSerializer,
            TSortTag
        >::Save(context, map);
    }

    template <class TMapType, class C>
    static void Load(C& context, TMapType& map)
    {
        size_t size = TSizeSerializer::LoadSuspended(context);

        SERIALIZATION_DUMP_WRITE(context, "multimap[%v]", size);

        map.clear();

        for (size_t index = 0; index < size; ++index) {
            typename TMapType::key_type key{};
            TKeySerializer::Load(context, key);

            SERIALIZATION_DUMP_WRITE(context, "=>");

            typename TMapType::mapped_type value{};
            SERIALIZATION_DUMP_INDENT(context) {
                TValueSerializer::Load(context, value);
            }

            map.emplace(std::move(key), std::move(value));
        }
    }
};

template <class T, size_t Size = std::tuple_size<T>::value>
struct TTupleSerializer;

template <class T>
struct TTupleSerializer<T, 0U>
{
    template<class C>
    static void Save(C&, const T&) {}

    template<class C>
    static void Load(C&, T&) {}
};

template <class T, size_t Size>
struct TTupleSerializer
{
    template<class C>
    static void Save(C& context, const T& tuple)
    {
        TTupleSerializer<T, Size - 1U>::Save(context, tuple);
        NYT::Save(context, std::get<Size - 1U>(tuple));
    }

    template<class C>
    static void Load(C& context, T& tuple)
    {
        TTupleSerializer<T, Size - 1U>::Load(context, tuple);
        NYT::Load(context, std::get<Size - 1U>(tuple));
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TValueBoundComparer
{
    template <class T>
    static bool Compare(const T& lhs, const T& rhs)
    {
        return lhs < rhs;
    }
};

template <class T, class C, class>
struct TSerializerTraits
{
    using TSerializer = TValueBoundSerializer;
    using TComparer = TValueBoundComparer;
};

////////////////////////////////////////////////////////////////////////////////

template <class TUnderlyingSerializer = TDefaultSerializer>
struct TUniquePtrSerializer
{
    template <class T, class C>
    static void Save(C& context, const std::unique_ptr<T>& ptr)
    {
        using NYT::Save;
        if (ptr) {
            Save(context, true);
            TUnderlyingSerializer::Save(context, *ptr);
        } else {
            Save(context, false);
        }
    }

    template <class T, class C>
    static void Load(C& context, std::unique_ptr<T>& ptr)
    {
        if (LoadSuspended<bool>(context)) {
            ptr = std::make_unique<T>();
            TUnderlyingSerializer::Load(context, *ptr);
        } else {
            ptr.reset();
            SERIALIZATION_DUMP_WRITE(context, "nullptr");
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

template <class TUnderlyingSerializer = TDefaultSerializer>
struct TNonNullableIntrusivePtrSerializer
{
    template <class T, class C>
    static void Save(C& context, const TIntrusivePtr<T>& ptr)
    {
        using NYT::Save;
        TUnderlyingSerializer::Save(context, *ptr);
    }

    template <class T, class C>
    static void Load(C& context, TIntrusivePtr<T>& ptr)
    {
        ptr = New<T>();
        TUnderlyingSerializer::Load(context, *ptr);
    }
};

////////////////////////////////////////////////////////////////////////////////

template <class TUnderlyingSerializer = TDefaultSerializer>
struct TNullableIntrusivePtrSerializer
{
    template <class T, class C>
    static void Save(C& context, const TIntrusivePtr<T>& ptr)
    {
        using NYT::Save;

        Save(context, ptr.operator bool());

        if (ptr) {
            TUnderlyingSerializer::Save(context, *ptr);
        }
    }

    template <class T, class C>
    static void Load(C& context, TIntrusivePtr<T>& ptr)
    {
        using NYT::Load;

        auto hasValue = LoadSuspended<bool>(context);

        if (hasValue) {
            ptr = New<T>();
            TUnderlyingSerializer::Load(context, *ptr);
        } else {
            ptr.Reset();
            SERIALIZATION_DUMP_WRITE(context, "nullptr");
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

template <class C>
struct TSerializerTraits<TSharedRef, C, void>
{
    using TSerializer = TSharedRefSerializer;
};

template <class C>
struct TSerializerTraits<TSharedRefArray, C, void>
{
    using TSerializer = TSharedRefArraySerializer;
};

template <class T, class C>
struct TSerializerTraits<
    T,
    C,
    typename std::enable_if<NMpl::TIsPod<T>::value && !std::is_pointer<T>::value>::type
>
{
    using TSerializer = TPodSerializer;
    using TComparer = TValueBoundComparer;
};

template <class T, class C>
struct TSerializerTraits<
    T,
    C,
    typename TEnumTraits<T>::TType
>
{
    using TSerializer = TEnumSerializer;
    using TComparer = TValueBoundComparer;
};

template <class C>
struct TSerializerTraits<TString, C, void>
{
    using TSerializer = TStringSerializer;
    using TComparer = TValueBoundComparer;
};

template <class C>
struct TSerializerTraits<std::string, C, void>
{
    using TSerializer = TStringSerializer;
    using TComparer = TValueBoundComparer;
};

// For save only.
template <class C>
struct TSerializerTraits<TStringBuf, C, void>
{
    using TSerializer = TStringSerializer;
    using TComparer = TValueBoundComparer;
};

template <class T, class C>
struct TSerializerTraits<std::optional<T>, C, void>
{
    using TSerializer = TOptionalSerializer<>;
    using TComparer = TValueBoundComparer;
};

template <class... Ts, class C>
struct TSerializerTraits<std::variant<Ts...>, C, void>
{
    using TSerializer = TVariantSerializer;
};

template <class T, class C>
struct TSerializerTraits<std::atomic<T>, C, void>
{
    using TSerializer = TAtomicSerializer<>;
};

template <class T, class A, class C>
struct TSerializerTraits<std::vector<T, A>, C, void>
{
    using TSerializer = TVectorSerializer<>;
};

template <class T, size_t N, class C>
struct TSerializerTraits<TCompactVector<T, N>, C, void>
{
    using TSerializer = TVectorSerializer<>;
};

template <class T, std::size_t size, class C>
struct TSerializerTraits<std::array<T, size>, C, void>
{
    using TSerializer = TArraySerializer<>;
};

template <class T, class A, class C>
struct TSerializerTraits<std::list<T, A>, C, void>
{
    using TSerializer = TListSerializer<>;
};

template <class T, class C>
struct TSerializerTraits<std::deque<T>, C, void>
{
    using TSerializer = TListSerializer<>;
};

template <class T, class Q, class A, class C>
struct TSerializerTraits<std::set<T, Q, A>, C, void>
{
    using TSerializer = TSetSerializer<>;
};
template <class T, class H, class P, class A, class C>
struct TSerializerTraits<std::unordered_set<T, H, P, A>, C, void>
{
    using TSerializer = TSetSerializer<>;
};

template <class T, class H, class E, class A, class C>
struct TSerializerTraits<THashSet<T, H, E, A>, C, void>
{
    using TSerializer = TSetSerializer<>;
};

template <class T, size_t N, class Q, class C>
struct TSerializerTraits<TCompactSet<T, N, Q>, C, void>
{
    using TSerializer = TSetSerializer<>;
};

template <class T, class C>
struct TSerializerTraits<THashMultiSet<T>, C, void>
{
    using TSerializer = TMultiSetSerializer<>;
};

template <class T, class A, class C>
struct TSerializerTraits<std::unique_ptr<std::vector<T, A>>, C, void>
{
    using TSerializer = TOptionalVectorSerializer<>;
};

template <class T, size_t size, class C>
struct TSerializerTraits<std::unique_ptr<TCompactVector<T, size>>, C, void>
{
    using TSerializer = TOptionalVectorSerializer<>;
};

template <class T, class A, class C>
struct TSerializerTraits<std::unique_ptr<std::list<T, A>>, C, void>
{
    using TSerializer = TOptionalListSerializer<>;
};

template <class T, class Q, class A, class C>
struct TSerializerTraits<std::unique_ptr<std::set<T, Q, A>>, C, void>
{
    using TSerializer = TOptionalSetSerializer<>;
};

template <class T, class H, class P, class A, class C>
struct TSerializerTraits<std::unique_ptr<std::unordered_set<T, H, P, A>>, C, void>
{
    using TSerializer = TOptionalSetSerializer<>;
};

template <class T, class H, class E, class A, class C>
struct TSerializerTraits<std::unique_ptr<THashSet<T, H, E, A>>, C, void>
{
    using TSerializer = TOptionalSetSerializer<>;
};

template <class K, class V, class Q, class A, class C>
struct TSerializerTraits<std::map<K, V, Q, A>, C, void>
{
    using TSerializer = TMapSerializer<>;
};

template <class K, class V, class H, class P, class A, class C>
struct TSerializerTraits<std::unordered_map<K, V, H, P, A>, C, void>
{
    using TSerializer = TMapSerializer<>;
};

template <class K, class V, class Q, class A, class C>
struct TSerializerTraits<THashMap<K, V, Q, A>, C, void>
{
    using TSerializer = TMapSerializer<>;
};

template <class K, class V, size_t N, class C>
struct TSerializerTraits<TCompactFlatMap<K, V, N>, C, void>
{
    using TSerializer = TMapSerializer<>;
};

template <class K, class V, class Q, class A, class C>
struct TSerializerTraits<std::multimap<K, V, Q, A>, C, void>
{
    using TSerializer = TMultiMapSerializer<>;
};

template <class K, class V, class C>
struct TSerializerTraits<THashMultiMap<K, V>, C, void>
{
    using TSerializer = TMultiMapSerializer<>;
};

template <class E, class T, class C, E Min, E Max>
struct TSerializerTraits<TEnumIndexedArray<E, T, Min, Max>, C, void>
{
    using TSerializer = TEnumIndexedArraySerializer<>;
};

template <class F, class S, class C>
struct TSerializerTraits<std::pair<F, S>, C, typename std::enable_if<!NMpl::TIsPod<std::pair<F, S>>::value>::type>
{
    using TSerializer = TTupleSerializer<std::pair<F, S>>;
    using TComparer = TValueBoundComparer;
};

template <class T, class TTag, class C>
struct TSerializerTraits<TStrongTypedef<T, TTag>, C, void>
{
    struct TSerializer
    {
        static void Save(C& context, const TStrongTypedef<T, TTag>& value)
        {
            NYT::Save(context, value.Underlying());
        }

        static void Load(C& context, TStrongTypedef<T, TTag>& value)
        {
            NYT::Load(context, value.Underlying());
        }
    };

    using TComparer = TValueBoundComparer;
};

template <class T, class C>
struct TSerializerTraits<TMaybeInf<T>, C, void>
{
    struct TSerializer
    {
        static void Save(C& context, TMaybeInf<T> value)
        {
            NYT::Save(context, value.UnsafeToUnderlying());
        }

        static void Load(C& context, TMaybeInf<T>& value)
        {
            value.UnsafeAssign(NYT::Load<T>(context));
        }
    };
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
