#pragma once

#include "guid.h"
#include "mpl.h"
#include "object_pool.h"
#include "serialize.h"

#include <yt/yt/core/compression/public.h>

#include <yt/yt_proto/yt/core/misc/proto/guid.pb.h>
#include <yt/yt_proto/yt/core/misc/proto/protobuf_helpers.pb.h>

#include <library/cpp/yt/memory/ref.h>

#include <library/cpp/yt/misc/optional.h>
#include <library/cpp/yt/misc/preprocessor.h>
#include <library/cpp/yt/misc/strong_typedef.h>
#include <library/cpp/yt/misc/static_initializer.h>

#include <library/cpp/yt/memory/range.h>

#include <google/protobuf/duration.pb.h>
#include <google/protobuf/message.h>
#include <google/protobuf/repeated_field.h>
#include <google/protobuf/timestamp.pb.h>

#include <google/protobuf/io/zero_copy_stream_impl_lite.h>

#include <type_traits>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <class T>
struct TProtoTraits
{ };

////////////////////////////////////////////////////////////////////////////////

void ToProto(::google::protobuf::uint64* serialized, TDuration original);
void FromProto(TDuration* original, ::google::protobuf::uint64 serialized);

void ToProto(::google::protobuf::Duration* serialized, TDuration original);
void FromProto(TDuration* original, ::google::protobuf::Duration serialized);

template <>
struct TProtoTraits<TDuration>
{
    using TSerialized = TDuration::TValue; // ui64
};

////////////////////////////////////////////////////////////////////////////////

void ToProto(::google::protobuf::uint64* serialized, TInstant original);
void FromProto(TInstant* original, ::google::protobuf::uint64 serialized);

void ToProto(::google::protobuf::uint64* serialized, TInstant original);
void FromProto(TInstant* original, ::google::protobuf::uint64 serialized);

void ToProto(::google::protobuf::Timestamp* serialized, TInstant original);
void FromProto(TInstant* original, ::google::protobuf::Timestamp serialized);

template <>
struct TProtoTraits<TInstant>
{
    using TSerialized = TInstant::TValue; // ui64
};

////////////////////////////////////////////////////////////////////////////////

template <class T>
typename std::enable_if<std::is_convertible_v<T*, ::google::protobuf::MessageLite*>, void>::type ToProto(
    T* serialized,
    const T& original);
template <class T>
typename std::enable_if<std::is_convertible_v<T*, ::google::protobuf::MessageLite*>, void>::type FromProto(
    T* original,
    const T& serialized);

////////////////////////////////////////////////////////////////////////////////

template <class T>
    requires TEnumTraits<T>::IsEnum && (!TEnumTraits<T>::IsBitEnum)
void ToProto(int* serialized, T original);
template <class T>
    requires TEnumTraits<T>::IsEnum && (!TEnumTraits<T>::IsBitEnum)
void FromProto(T* original, int serialized);

template <class T>
    requires TEnumTraits<T>::IsEnum && (!TEnumTraits<T>::IsBitEnum)
struct TProtoTraits<T>
{
    using TSerialized = int;
};

////////////////////////////////////////////////////////////////////////////////

template <class T>
    requires TEnumTraits<T>::IsBitEnum
void ToProto(ui64* serialized, T original);
template <class T>
    requires TEnumTraits<T>::IsBitEnum
void FromProto(T* original, ui64 serialized);

template <class T>
    requires TEnumTraits<T>::IsBitEnum
struct TProtoTraits<T>
{
    using TSerialized = ui64;
};

////////////////////////////////////////////////////////////////////////////////

template <class TSerialized, class TOriginalArray, class... TArgs>
void ToProto(
    ::google::protobuf::RepeatedPtrField<TSerialized>* serializedArray,
    const TOriginalArray& originalArray,
    TArgs&&... args);

template <class TSerialized, class TOriginalArray>
void ToProto(
    ::google::protobuf::RepeatedField<TSerialized>* serializedArray,
    const TOriginalArray& originalArray);

template <class TKey, class TValue, class TSerializedKey, class TSerializedValue>
void ToProto(
    ::google::protobuf::Map<TSerializedKey, TSerializedValue>* serializedMap,
    const THashMap<TKey, TValue>& originalMap);

template <class TOriginalArray, class TSerialized, class... TArgs>
void FromProto(
    TOriginalArray* originalArray,
    const ::google::protobuf::RepeatedPtrField<TSerialized>& serializedArray,
    TArgs&&... args);

template <class TOriginalArray, class TSerialized>
void FromProto(
    TOriginalArray* originalArray,
    const ::google::protobuf::RepeatedField<TSerialized>& serializedArray);

template <class TOriginal, class TSerialized>
void CheckedHashSetFromProto(
    THashSet<TOriginal>* originalHashSet,
    const ::google::protobuf::RepeatedPtrField<TSerialized>& serializedHashSet);

template <class TOriginal, class TSerialized>
void CheckedHashSetFromProto(
    THashSet<TOriginal>* originalHashSet,
    const ::google::protobuf::RepeatedField<TSerialized>& serializedHashSet);

template <class TKey, class TValue, class TSerializedKey, class TSerializedValue>
void FromProto(
    THashMap<TKey, TValue>* originalMap,
    const ::google::protobuf::Map<TSerializedKey, TSerializedValue>& serializedMap);

////////////////////////////////////////////////////////////////////////////////

template <class TSerialized, class T, class TTag>
void FromProto(TStrongTypedef<T, TTag>* original, const TSerialized& serialized);

template <class TSerialized, class T, class TTag>
void ToProto(TSerialized* serialized, const TStrongTypedef<T, TTag>& original);

template <class T, class TTag>
struct TProtoTraits<TStrongTypedef<T, TTag>>
{
    using TSerialized = T;
};

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

struct TToProtoAutoDerivedSerializedTag
{ };

template <class TSerialized, class TOriginal>
struct TToProtoResult
{
    using T = TSerialized;
};

template <class TOriginal>
struct TToProtoResult<TToProtoAutoDerivedSerializedTag, TOriginal>
{
    using T = typename TProtoTraits<TOriginal>::TSerialized;
};

//! A simple heuristic to distinguish between `ToProto(original)` and `ToProto(&original, serialized)`.
template <class T>
concept CToProtoOriginal = !std::is_pointer_v<T>;

} // namespace NDetail

template <class TSerialized = NYT::NDetail::TToProtoAutoDerivedSerializedTag, NYT::NDetail::CToProtoOriginal TOriginal, class... TArgs>
auto ToProto(const TOriginal& original, TArgs&&... args);

template <class TOriginal, class TSerialized, class... TArgs>
TOriginal FromProto(const TSerialized& serialized, TArgs&&... args);

////////////////////////////////////////////////////////////////////////////////

#pragma pack(push, 4)

struct TEnvelopeFixedHeader
{
    ui32 EnvelopeSize;
    ui32 MessageSize;
};

#pragma pack(pop)

////////////////////////////////////////////////////////////////////////////////

//! Serializes a protobuf message to pre-allocated TMutableRef.
//! The caller is responsible for providing #ref of a suitable size.
//! Fails on error.
void SerializeProtoToRef(
    const google::protobuf::MessageLite& message,
    TMutableRef ref,
    bool partial = true);

//! Serializes a protobuf message to TSharedRef.
//! Fails on error.
TSharedRef SerializeProtoToRef(
    const google::protobuf::MessageLite& message,
    bool partial = true);

//! Serializes a protobuf message to TString.
//! Fails on error.
TString SerializeProtoToString(
    const google::protobuf::MessageLite& message,
    bool partial = true);

//! Deserializes a chunk of memory into a protobuf message.
//! Returns |true| iff everything went well.
bool TryDeserializeProto(
    google::protobuf::MessageLite* message,
    TRef data);

//! Deserializes a chunk of memory into a protobuf message.
//! Fails on error.
void DeserializeProto(
    google::protobuf::MessageLite* message,
    TRef data);

//! Serializes a given protobuf message and wraps it with envelope.
//! Optionally compresses the serialized message.
//! Fails on error.
TSharedRef SerializeProtoToRefWithEnvelope(
    const google::protobuf::MessageLite& message,
    NCompression::ECodec codecId = NCompression::ECodec::None,
    bool partial = true);

//! \see SerializeProtoToRefWithEnvelope
TString SerializeProtoToStringWithEnvelope(
    const google::protobuf::MessageLite& message,
    NCompression::ECodec codecId = NCompression::ECodec::None,
    bool partial = true);

//! Unwraps a chunk of memory obtained from #SerializeProtoToRefWithEnvelope
//! and deserializes it into a protobuf message.
//! Returns |true| iff everything went well.
bool TryDeserializeProtoWithEnvelope(
    google::protobuf::MessageLite* message,
    TRef data);

//! Unwraps a chunk of memory obtained from #SerializeProtoToRefWithEnvelope
//! and deserializes it into a protobuf message.
//! Fails on error.
void DeserializeProtoWithEnvelope(
    google::protobuf::MessageLite* message,
    TRef data);

//! Serializes a given protobuf message.
//! Optionally compresses the serialized message.
//! Fails on error.
TSharedRef SerializeProtoToRefWithCompression(
    const google::protobuf::MessageLite& message,
    NCompression::ECodec codecId = NCompression::ECodec::None,
    bool partial = true);

//! Unwraps a chunk of memory obtained from #SerializeProtoToRefWithCompression,
//! decompresses it with a given codec and deserializes it into a protobuf message.
//! Returns |true| iff everything went well.
bool TryDeserializeProtoWithCompression(
    google::protobuf::MessageLite* message,
    TRef data,
    NCompression::ECodec codecId = NCompression::ECodec::None);

//! Unwraps a chunk of memory obtained from #SerializeProtoToRefWithCompression,
//! decompresses it with a given codec and deserializes it into a protobuf message.
//! Fails on error.
void DeserializeProtoWithCompression(
    google::protobuf::MessageLite* message,
    TRef data,
    NCompression::ECodec codecId = NCompression::ECodec::None);

TSharedRef PushEnvelope(const TSharedRef& data, NCompression::ECodec codec);
TSharedRef PushEnvelope(const TSharedRef& data);
TSharedRef PopEnvelope(const TSharedRef& data);

////////////////////////////////////////////////////////////////////////////////

template <std::derived_from<::google::protobuf::MessageLite> T>
void FormatValue(TStringBuilderBase* builder, const T& message, TStringBuf /*spec*/);

////////////////////////////////////////////////////////////////////////////////

struct TBinaryProtoSerializer
{
    //! Serializes a given protobuf message into a given stream.
    //! Throws an exception in case of error.
    static void Save(TStreamSaveContext& context, const ::google::protobuf::Message& message);

    //! Reads from a given stream protobuf message.
    //! Throws an exception in case of error.
    static void Load(TStreamLoadContext& context, ::google::protobuf::Message& message);
};

template <class T, class C>
struct TSerializerTraits<
    T,
    C,
    typename std::enable_if_t<std::is_convertible_v<T&, ::google::protobuf::Message&>>>
{
    using TSerializer = TBinaryProtoSerializer;
};

////////////////////////////////////////////////////////////////////////////////

/*
 *  YT Extension Set is a collection of |(tag, data)| pairs.
 *
 *  Here |tag| is a unique integer identifier and |data| is a protobuf-serialized
 *  embedded message.
 *
 *  In contrast to native Protobuf Extensions, ours are deserialized on-demand.
 */

//! Used to obtain an integer tag for a given type.
/*!
 *  Specialized versions of this traits are generated with |DECLARE_PROTO_EXTENSION|.
 */
template <class T>
struct TProtoExtensionTag;

#define DECLARE_PROTO_EXTENSION(type, tag) \
    template <> \
    struct TProtoExtensionTag<type> \
    { \
        static constexpr i32 Value = tag; \
    };

struct TProtobufExtensionDescriptor
{
    const google::protobuf::Descriptor* MessageDescriptor;
    const int Tag;
    const std::string Name;
};

struct IProtobufExtensionRegistry
{
    using TRegisterAction = std::function<void()>;

    //! This method is assumed to be called during static initialization only.
    //! We defer running actions until static protobuf descriptors are ready.
    //! Accessing type descriptors during static initialization phase may cause UB.
    virtual void AddAction(TRegisterAction action) = 0;

    //! Registers protobuf extension for further conversions. Do not call
    //! this method explicitly; use REGISTER_PROTO_EXTENSION macro that defers invocation
    //! until static protobuf descriptors are ready.
    virtual void RegisterDescriptor(const TProtobufExtensionDescriptor& descriptor) = 0;

    //! Finds a descriptor by tag value.
    virtual const TProtobufExtensionDescriptor* FindDescriptorByTag(int tag) = 0;

    //! Finds a descriptor by name.
    virtual const TProtobufExtensionDescriptor* FindDescriptorByName(const std::string& name) = 0;

    //! Returns the singleton instance.
    static IProtobufExtensionRegistry* Get();
};

#define REGISTER_PROTO_EXTENSION(type, tag, name) \
    YT_STATIC_INITIALIZER( \
        NYT::IProtobufExtensionRegistry::Get()->AddAction([] { \
            const auto* descriptor = type::default_instance().GetDescriptor(); \
            ::NYT::IProtobufExtensionRegistry::Get()->RegisterDescriptor({ \
                .MessageDescriptor = descriptor, \
                .Tag = tag, \
                .Name = #name, \
            });\
        }));

//! Finds and deserializes an extension of the given type. Fails if no matching
//! extension is found.
template <class T>
T GetProtoExtension(const NYT::NProto::TExtensionSet& extensions);

// Returns |true| iff an extension of a given type is present.
template <class T>
bool HasProtoExtension(const NYT::NProto::TExtensionSet& extensions);

//! Finds and deserializes an extension of the given type. Returns null if no matching
//! extension is found.
template <class T>
std::optional<T> FindProtoExtension(const NYT::NProto::TExtensionSet& extensions);

//! Serializes and stores an extension.
//! Overwrites any extension with the same tag (if exists).
template <class T>
void SetProtoExtension(NProto::TExtensionSet* extensions, const T& value);
template <class TProto, class TValue>
std::enable_if_t<!std::is_same_v<TProto, TValue>, void>
SetProtoExtension(NProto::TExtensionSet* extensions, const TValue& value);

//! Tries to remove the extension.
//! Returns |true| iff the proper extension is found.
template <class T>
bool RemoveProtoExtension(NProto::TExtensionSet* extensions);

//! Filters extensions leaving only those matching #tags set.
void FilterProtoExtensions(
    NYT::NProto::TExtensionSet* target,
    const NYT::NProto::TExtensionSet& source,
    const THashSet<int>& tags);
void FilterProtoExtensions(
    NYT::NProto::TExtensionSet* inplace,
    const THashSet<int>& tags);
NYT::NProto::TExtensionSet FilterProtoExtensions(
    const NYT::NProto::TExtensionSet& source,
    const THashSet<int>& tags);

////////////////////////////////////////////////////////////////////////////////

THashSet<int> GetExtensionTagSet(const NYT::NProto::TExtensionSet& source);
std::optional<std::string> FindExtensionName(int tag);

////////////////////////////////////////////////////////////////////////////////

//! Wrapper that makes proto message ref-counted.
template <class TProto>
class TRefCountedProto
    : public TRefCounted
    , public TProto
{
public:
    TRefCountedProto() = default;
    TRefCountedProto(const TRefCountedProto<TProto>& other);
    TRefCountedProto(TRefCountedProto<TProto>&& other);
    explicit TRefCountedProto(const TProto& other);
    explicit TRefCountedProto(TProto&& other);
    ~TRefCountedProto();

    i64 GetSize() const;

private:
    size_t ExtraSpace_ = 0;

    void RegisterExtraSpace();
    void UnregisterExtraSpace();
};

////////////////////////////////////////////////////////////////////////////////

google::protobuf::Timestamp GetProtoNow();

////////////////////////////////////////////////////////////////////////////////

//! This macro may be used to extract std::optional<T> from protobuf message
//! field. Macro accepts desired target type as optional third parameter.
//! Usage:
//!     // Get as is.
//!     int instantInt = YT_OPTIONAL_FROM_PROTO(message, instant);
//!     // Get with conversion.
//!     TInstant instant = YT_OPTIONAL_FROM_PROTO(message, instant, TInstant);
#define YT_OPTIONAL_FROM_PROTO(message, field, ...) \
    (((message).has_##field()) \
        ? std::optional(YT_OPTIONAL_FROM_PROTO_CONVERT(__VA_ARGS__)((message).field())) \
        : std::nullopt)

#define YT_OPTIONAL_TO_PROTO(message, field, original) \
    do {\
        if (original.has_value()) {\
            ToProto((message)->mutable_##field(), *original);\
        } else {\
            (message)->clear_##field();\
        }\
    } while (false)

#define YT_OPTIONAL_SET_PROTO(message, field, original) \
    do {\
        /* Avoid unnecessary computation if <original> is a return value of a call*/\
        const auto& originalRef = (original);\
        if (originalRef.has_value()) {\
            (message)->set_##field(ToProto(*originalRef));\
        } else {\
            (message)->clear_##field();\
        }\
    } while (false)

// TODO(cherepashka): to remove after std::optional::and_then is here.
//! This macro may be used to extract std::optional<T> from protobuf message field of type T and to apply some function to value if it is present.
#define YT_APPLY_PROTO_OPTIONAL(message, field, function) (((message).has_##field()) ? std::make_optional(function((message).field())) : std::nullopt)

////////////////////////////////////////////////////////////////////////////////

constexpr bool IsVanillaProtobuf = std::same_as<TProtoStringType, std::string>;
constexpr bool IsArcadiaProtobuf = std::same_as<TProtoStringType, TString>;

// If this assert fails, something went very wrong. Refer to a comment above.
static_assert(IsVanillaProtobuf || IsArcadiaProtobuf, "Unknown protobuf string type");

////////////////////////////////////////////////////////////////////////////////

class TProtobufInputStream
    : public ::google::protobuf::io::CopyingInputStream
{
public:
    explicit TProtobufInputStream(IInputStream* stream);

    int Read(void* buffer, int size) override;

    // Arcadia-style streams throw errors instead of returning -1,
    // so we intercept these errors and store them in a flag.
    bool HasError() const;

private:
    IInputStream* const Stream_;

    bool HasError_ = false;
};

class TProtobufInputStreamAdaptor
    : public TProtobufInputStream
    , public ::google::protobuf::io::CopyingInputStreamAdaptor
{
public:
    explicit TProtobufInputStreamAdaptor(IInputStream* stream);
};

////////////////////////////////////////////////////////////////////////////////

class TProtobufOutputStream
    : public ::google::protobuf::io::CopyingOutputStream
{
public:
    explicit TProtobufOutputStream(IOutputStream* stream);

    bool Write(const void* buffer, int size) override;

    // Arcadia-style streams throw errors instead of returning -1,
    // so we intercept these errors and store them in a flag.
    bool HasError() const;

private:
    IOutputStream* const Stream_;

    bool HasError_ = false;
};

class TProtobufOutputStreamAdaptor
    : public TProtobufOutputStream
    , public ::google::protobuf::io::CopyingOutputStreamAdaptor
{
public:
    explicit TProtobufOutputStreamAdaptor(IOutputStream* stream);
};

class TProtobufZeroCopyOutputStream
    : public ::google::protobuf::io::ZeroCopyOutputStream
{
public:
    explicit TProtobufZeroCopyOutputStream(IZeroCopyOutput* stream);

    bool Next(void** data, int* size) override;
    void BackUp(int count) override;
    int64_t ByteCount() const override;

    void ThrowOnError() const;

private:
    IZeroCopyOutput* const Stream_;

    std::exception_ptr Error_;
    i64 ByteCount_ = 0;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

namespace google::protobuf {

////////////////////////////////////////////////////////////////////////////////

template <class T>
void FormatValue(
    NYT::TStringBuilderBase* builder,
    const ::google::protobuf::RepeatedField<T>& collection,
    TStringBuf /*spec*/);

template <class T>
void FormatValue(
    NYT::TStringBuilderBase* builder,
    const ::google::protobuf::RepeatedPtrField<T>& collection,
    TStringBuf /*spec*/);

////////////////////////////////////////////////////////////////////////////////

} // namespace google::protobuf

#define PROTOBUF_HELPERS_INL_H_
#include "protobuf_helpers-inl.h"
#undef PROTOBUF_HELPERS_INL_H_
