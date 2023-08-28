#include "serialize.h"

#include <yt/yt/core/concurrency/fls.h>

#include <library/cpp/yt/assert/assert.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

const std::array<ui8, ZeroBufferSize> ZeroBuffer{};

////////////////////////////////////////////////////////////////////////////////

void AssertSerializationAligned(i64 byteSize)
{
    YT_ASSERT(AlignUpSpace<i64>(byteSize, SerializationAlignment) == 0);
}

void VerifySerializationAligned(i64 byteSize)
{
    YT_VERIFY(AlignUpSpace<i64>(byteSize, SerializationAlignment) == 0);
}

////////////////////////////////////////////////////////////////////////////////

namespace {

int& CrashOnErrorDepth()
{
    static NConcurrency::TFlsSlot<int> Slot;
    return *Slot;
}

} // namespace

TCrashOnDeserializationErrorGuard::TCrashOnDeserializationErrorGuard()
{
    ++CrashOnErrorDepth();
}

TCrashOnDeserializationErrorGuard::~TCrashOnDeserializationErrorGuard()
{
    YT_VERIFY(--CrashOnErrorDepth() >= 0);
}

void TCrashOnDeserializationErrorGuard::OnError()
{
    YT_VERIFY(CrashOnErrorDepth() == 0);
}

////////////////////////////////////////////////////////////////////////////////

TSaveContextStream::TSaveContextStream(IOutputStream* output)
    : BufferedOutput_(output)
    , Output_(&*BufferedOutput_)
{ }

TSaveContextStream::TSaveContextStream(IZeroCopyOutput* output)
    : Output_(output)
{ }

void TSaveContextStream::FlushBuffer()
{
    Output_->Undo(BufferRemaining_);
    BufferPtr_ = nullptr;
    BufferRemaining_ = 0;
    Output_->Flush();
    if (BufferedOutput_) {
        BufferedOutput_->Flush();
    }
}

void TSaveContextStream::WriteSlow(const void* buf, size_t len)
{
    auto bufPtr = static_cast<const char*>(buf);
    auto toWrite = len;
    while (toWrite > 0) {
        if (BufferRemaining_ == 0) {
            BufferRemaining_ = Output_->Next(&BufferPtr_);
        }
        YT_ASSERT(BufferRemaining_ > 0);
        auto toCopy = std::min(toWrite, BufferRemaining_);
        ::memcpy(BufferPtr_, bufPtr, toCopy);
        BufferPtr_ += toCopy;
        BufferRemaining_ -= toCopy;
        bufPtr += toCopy;
        toWrite -= toCopy;
    }
}

////////////////////////////////////////////////////////////////////////////////

TStreamSaveContext::TStreamSaveContext(
    IOutputStream* output,
    int version)
    : Output_(output)
    , Version_(version)
{ }

TStreamSaveContext::TStreamSaveContext(
    IZeroCopyOutput* output,
    int version)
    : Output_(output)
    , Version_(version)
{ }

void TStreamSaveContext::Finish()
{
    Output_.FlushBuffer();
}

////////////////////////////////////////////////////////////////////////////////

TLoadContextStream::TLoadContextStream(IInputStream* input)
    : Input_(input)
{ }

TLoadContextStream::TLoadContextStream(IZeroCopyInput* input)
    : ZeroCopyInput_(input)
{ }

void TLoadContextStream::ClearBuffer()
{
    if (BufferRemaining_ > 0) {
        BufferPtr_ = nullptr;
        BufferRemaining_ = 0;
    }
}

size_t TLoadContextStream::LoadSlow(void* buf, size_t len)
{
    if (ZeroCopyInput_) {
        auto bufPtr = static_cast<char*>(buf);
        auto toRead = len;
        while (toRead > 0) {
            if (BufferRemaining_ == 0) {
                BufferRemaining_ = ZeroCopyInput_->Next(&BufferPtr_);
                if (BufferRemaining_ == 0) {
                    break;
                }
            }
            YT_ASSERT(BufferRemaining_ > 0);
            auto toCopy = std::min(toRead, BufferRemaining_);
            ::memcpy(bufPtr, BufferPtr_, toCopy);
            BufferPtr_ += toCopy;
            BufferRemaining_ -= toCopy;
            bufPtr += toCopy;
            toRead -= toCopy;
        }
        return len - toRead;
    } else {
        return Input_->Load(buf, len);
    }
}

////////////////////////////////////////////////////////////////////////////////

TStreamLoadContext::TStreamLoadContext(IInputStream* input)
    : Input_(input)
{ }

TStreamLoadContext::TStreamLoadContext(IZeroCopyInput* input)
    : Input_(input)
{ }

////////////////////////////////////////////////////////////////////////////////

TEntityStreamSaveContext::TEntityStreamSaveContext(
    IZeroCopyOutput* output,
    const TEntityStreamSaveContext* parentContext)
    : TStreamSaveContext(
        output,
        parentContext->GetVersion())
    , ParentContext_(parentContext)
{ }

////////////////////////////////////////////////////////////////////////////////

TEntityStreamLoadContext::TEntityStreamLoadContext(
    IZeroCopyInput* input,
    const TEntityStreamLoadContext* parentContext)
    : TStreamLoadContext(input)
    , ParentContext_(parentContext)
{
    SetVersion(ParentContext_->GetVersion());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

