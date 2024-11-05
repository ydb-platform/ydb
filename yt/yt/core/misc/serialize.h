#pragma once

#include "public.h"
#include "error.h"
#include "mpl.h"
#include "property.h"
#include "serialize_dump.h"
#include "maybe_inf.h"

#include <library/cpp/yt/assert/assert.h>

#include <library/cpp/yt/memory/ref.h>

#include <library/cpp/yt/misc/strong_typedef.h>

#include <util/stream/buffered.h>
#include <util/stream/file.h>
#include <util/stream/zerocopy_output.h>

#include <util/system/align.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

//! Alignment size; measured in bytes and must be a power of two.
constexpr size_t SerializationAlignment = 8;
static_assert(
    (SerializationAlignment & (SerializationAlignment - 1)) == 0,
    "SerializationAlignment should be a power of two");

//! The size of the zero buffer used by #WriteZeroes and #WritePadding.
constexpr size_t ZeroBufferSize = 64_KB;
static_assert(
    ZeroBufferSize >= SerializationAlignment,
    "ZeroBufferSize < SerializationAlignment");
extern const std::array<ui8, ZeroBufferSize> ZeroBuffer;

////////////////////////////////////////////////////////////////////////////////

//! When active, causes the process on crash on a deserialization error is
//! encountered. (The default is to throw an exception.)
class TCrashOnDeserializationErrorGuard
{
public:
    TCrashOnDeserializationErrorGuard();
    ~TCrashOnDeserializationErrorGuard();

    TCrashOnDeserializationErrorGuard(const TCrashOnDeserializationErrorGuard&) = delete;
    TCrashOnDeserializationErrorGuard(TCrashOnDeserializationErrorGuard&&) = delete;

    static void OnError();
};

////////////////////////////////////////////////////////////////////////////////

template <class TInput>
void ReadRef(TInput& input, TMutableRef ref);
void ReadRef(const char*& ptr, TMutableRef ref);
template <class TOutput>
void WriteRef(TOutput& output, TRef ref);
void WriteRef(char*& ptr, TRef ref);

template <class TInput, class T>
void ReadPod(TInput& input, T& obj);
template <class TInput, class T>
void ReadPod(const char*& ptr, T& obj);
template <class TOutput, class T>
void WritePod(TOutput& output, const T& obj);
template <class T>
void WritePod(char*& ptr, const T& obj);

template <class TOutput>
void WriteZeroes(TOutput& output, size_t count);
void WriteZeroes(char*& ptr, size_t count);

template <class TOutput>
void WritePadding(TOutput& output, size_t sizeToPad);
void WritePadding(char*& ptr, size_t sizeToPad);

template <class TInput>
void ReadPadding(TInput& input, size_t sizeToPad);
void ReadPadding(const char*& ptr, size_t sizeToPad);

template <class T>
TSharedRef PackRefs(const T& parts);
template <class T>
void UnpackRefs(const TSharedRef& packedRef, T* parts);
std::vector<TSharedRef> UnpackRefs(const TSharedRef& packedRef);

template <class TTag, class TParts>
TSharedRef MergeRefsToRef(const TParts& parts);
template <class TParts>
void MergeRefsToRef(const TParts& parts, TMutableRef dst);
template <class TParts>
TString MergeRefsToString(const TParts& parts);

void AssertSerializationAligned(i64 byteSize);
void VerifySerializationAligned(i64 byteSize);

////////////////////////////////////////////////////////////////////////////////

class TSaveContextStream
{
public:
    explicit TSaveContextStream(IOutputStream* output);
    explicit TSaveContextStream(IZeroCopyOutput* output);

    void Write(const void* buf, size_t len);
    void FlushBuffer();

private:
    std::optional<TBufferedOutput> BufferedOutput_;
    IZeroCopyOutput* const Output_;

    char* BufferPtr_ = nullptr;
    size_t BufferRemaining_ = 0;

    void WriteSlow(const void* buf, size_t len);
};

////////////////////////////////////////////////////////////////////////////////

class TStreamSaveContext
{
public:
    explicit TStreamSaveContext(
        IOutputStream* output,
        int version = 0);
    explicit TStreamSaveContext(
        IZeroCopyOutput* output,
        int version = 0);

    virtual ~TStreamSaveContext() = default;

    TSaveContextStream* GetOutput();
    int GetVersion() const;

    void Finish();

protected:
    TSaveContextStream Output_;
    const int Version_;
};

////////////////////////////////////////////////////////////////////////////////

class TLoadContextStream
{
public:
    explicit TLoadContextStream(IInputStream* input);
    explicit TLoadContextStream(IZeroCopyInput* input);

    size_t Load(void* buf, size_t len);
    void ClearBuffer();

private:
    IInputStream* const Input_ = nullptr;
    IZeroCopyInput* const ZeroCopyInput_ = nullptr;

    char* BufferPtr_ = nullptr;
    size_t BufferRemaining_ = 0;

    size_t LoadSlow(void* buf, size_t len);
};

////////////////////////////////////////////////////////////////////////////////

class TStreamLoadContext
{
public:
    DEFINE_BYVAL_RW_PROPERTY(int, Version);
    DEFINE_BYREF_RW_PROPERTY(TSerializationDumper, Dumper);

public:
    explicit TStreamLoadContext(IInputStream* input);
    explicit TStreamLoadContext(IZeroCopyInput* input);

    virtual ~TStreamLoadContext() = default;

    TLoadContextStream* GetInput();

protected:
    TLoadContextStream Input_;
};

////////////////////////////////////////////////////////////////////////////////

template <class TSaveContext, class TLoadContext, class TSnapshotVersion>
class TCustomPersistenceContext
{
public:
    // Deliberately not explicit.
    TCustomPersistenceContext(TSaveContext& saveContext);
    TCustomPersistenceContext(TLoadContext& loadContext);

    bool IsSave() const;
    TSaveContext& SaveContext() const;

    bool IsLoad() const;
    TLoadContext& LoadContext() const;

    template <class TOtherContext>
    operator TOtherContext() const;

    TSnapshotVersion GetVersion() const;

private:
    TSaveContext* const SaveContext_ = nullptr;
    TLoadContext* const LoadContext_ = nullptr;
};

////////////////////////////////////////////////////////////////////////////////

YT_DEFINE_STRONG_TYPEDEF(TEntitySerializationKey, i32);
constexpr auto NullEntitySerializationKey = TEntitySerializationKey(-1);

////////////////////////////////////////////////////////////////////////////////

class TEntityStreamSaveContext
    : public TStreamSaveContext
{
public:
    using TStreamSaveContext::TStreamSaveContext;

    TEntityStreamSaveContext(
        IZeroCopyOutput* output,
        const TEntityStreamSaveContext* parentContext);

    TEntitySerializationKey GenerateSerializationKey();

    static constexpr TEntitySerializationKey InlineKey = TEntitySerializationKey(-3);

    template <class T>
    TEntitySerializationKey RegisterRawEntity(T* entity);
    template <class T>
    TEntitySerializationKey RegisterRefCountedEntity(const TIntrusivePtr<T>& entity);

private:
    const TEntityStreamSaveContext* const ParentContext_ = nullptr;

    int SerializationKeyIndex_ = 0;
    THashMap<void*, TEntitySerializationKey> RawPtrs_;
    THashMap<TRefCountedPtr, TEntitySerializationKey> RefCountedPtrs_;
};

////////////////////////////////////////////////////////////////////////////////

class TEntityStreamLoadContext
    : public TStreamLoadContext
{
public:
    using TStreamLoadContext::TStreamLoadContext;

    TEntityStreamLoadContext(
        IZeroCopyInput* input,
        const TEntityStreamLoadContext* parentContext);

    template <class T>
    TEntitySerializationKey RegisterRawEntity(T* entity);
    template <class T>
    TEntitySerializationKey RegisterRefCountedEntity(const TIntrusivePtr<T>& entity);

    template <class T>
    T* GetRawEntity(TEntitySerializationKey key) const;
    template <class T>
    TIntrusivePtr<T> GetRefCountedEntity(TEntitySerializationKey key) const;

private:
    const TEntityStreamLoadContext* const ParentContext_ = nullptr;

    std::vector<void*> RawPtrs_;
    std::vector<TIntrusivePtr<TRefCounted>> RefCountedPtrs_;
};

////////////////////////////////////////////////////////////////////////////////

template <class T, class C, class... TArgs>
void Save(C& context, const T& value, TArgs&&... args);

template <class T, class C, class... TArgs>
void Load(C& context, T& value, TArgs&&... args);

template <class T, class C, class... TArgs>
T Load(C& context, TArgs&&... args);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

#define SERIALIZE_INL_H_
#include "serialize-inl.h"
#undef SERIALIZE_INL_H_

