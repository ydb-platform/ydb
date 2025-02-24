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

TLoadContextStream::TLoadContextStream(
    TStreamLoadContext* context,
    IInputStream* input)
    : Context_(context)
    , Input_(input)
{ }

TLoadContextStream::TLoadContextStream(
    TStreamLoadContext* context,
    IZeroCopyInput* input)
    : Context_(context)
    , Input_(input)
{ }

void TLoadContextStream::SkipToCheckpoint()
{
    if (BufferRemaining_ == 0) {
        return;
    }

    if (!ScopeStack_.empty()) {
        THROW_ERROR_EXCEPTION("Cannot skip to checkpoint when scope stack is not empty");
    }

    BufferPtr_ = nullptr;
    BufferRemaining_ = 0;
}

void TLoadContextStream::UpdateTopmostScopeChecksum(void* buf, size_t len)
{
    auto& scope = ScopeStack_.back();
    scope.CurrentChecksum = GetChecksum(TRef(buf, len), scope.CurrentChecksum);
}

void TLoadContextStream::UpdateScopesChecksum()
{
    if (Context_->Dumper().IsChecksumDumpActive() && !ScopeStack_.empty()) {
        auto& topmostScope = ScopeStack_.back();
        UpdateTopmostScopeChecksum(
            topmostScope.CurrentChecksumPtr,
            BufferPtr_ - topmostScope.CurrentChecksumPtr);
        UpdateScopesCurrentChecksumPtr();
    }
}

void TLoadContextStream::UpdateScopesCurrentChecksumPtr()
{
    if (Context_->Dumper().IsChecksumDumpActive() && !ScopeStack_.empty()) {
        for (auto& scope : ScopeStack_) {
            scope.CurrentChecksumPtr = BufferPtr_;
        }
    }
}

size_t TLoadContextStream::LoadSlow(void* buf_, size_t len)
{
    size_t bytesRead = 0;
    if (ZeroCopyInput_) {
        auto buf = static_cast<char*>(buf_);
        auto bytesToRead = len;
        while (bytesToRead > 0) {
            if (BufferRemaining_ == 0) {
                UpdateScopesChecksum();

                BufferRemaining_ = ZeroCopyInput_->Next(&BufferPtr_);
                if (BufferRemaining_ == 0) {
                    break;
                }

                UpdateScopesCurrentChecksumPtr();
            }

            auto bytesToCopy = std::min(bytesToRead, BufferRemaining_);
            ::memcpy(buf, BufferPtr_, bytesToCopy);

            BufferPtr_ += bytesToCopy;
            BufferRemaining_ -= bytesToCopy;
            buf += bytesToCopy;
            bytesToRead -= bytesToCopy;
            bytesRead += bytesToCopy;
        }
        return len - bytesToRead;
    } else {
        bytesRead = Input_->Load(buf_, len);
        if (Context_->Dumper().IsChecksumDumpActive() && !ScopeStack_.empty()) {
            UpdateTopmostScopeChecksum(buf_, bytesRead);
        }
    }
    return bytesRead;
}

void TLoadContextStream::BeginScope(TStringBuf name)
{
    UpdateScopesChecksum();

    ScopeStack_.push_back({
        .ScopeNameLength = name.size(),
    });

    CurrentScopePath_ += "/";
    CurrentScopePath_ += name;

    if (Context_->Dumper().IsChecksumDumpActive()) {
        ScopeStack_.back().CurrentChecksumPtr = BufferPtr_;
    }
}

void TLoadContextStream::EndScope()
{
    UpdateScopesChecksum();

    YT_VERIFY(!ScopeStack_.empty());
    auto& topmostScope = ScopeStack_.back();

    if (Context_->Dumper().IsChecksumDumpActive()) {
        for (int index = 0; index < std::ssize(ScopeStack_) - 1; ++index) {
            auto& scope = ScopeStack_[index];
            scope.CurrentChecksum = GetChecksum(TRef::FromPod(topmostScope.CurrentChecksum), scope.CurrentChecksum);
        }

        Context_->Dumper().WriteChecksum(CurrentScopePath_, topmostScope.CurrentChecksum);
    }

    CurrentScopePath_.resize(CurrentScopePath_.size() - topmostScope.ScopeNameLength - 1);
    ScopeStack_.pop_back();
}

////////////////////////////////////////////////////////////////////////////////

TStreamLoadContext::TStreamLoadContext(IInputStream* input)
    : Input_(this, input)
{ }

TStreamLoadContext::TStreamLoadContext(IZeroCopyInput* input)
    : Input_(this, input)
{ }

void TStreamLoadContext::BeginScope(TStringBuf name)
{
    Input_.BeginScope(name);
}

void TStreamLoadContext::EndScope()
{
    Input_.EndScope();
}

////////////////////////////////////////////////////////////////////////////////

TStreamLoadContextScopeGuard::TStreamLoadContextScopeGuard(
    TStreamLoadContext& context,
    TStringBuf name)
    : Context_(context)
{
    YT_VERIFY(!name.empty());
    context.BeginScope(name);
}

TStreamLoadContextScopeGuard::~TStreamLoadContextScopeGuard()
{
    Context_.EndScope();
}

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

