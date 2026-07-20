#include "tagged_payload.h"
#include "private.h"

#include <library/cpp/yt/assert/assert.h>

#include <library/cpp/yt/misc/tls.h>

#include <util/system/compiler.h>
#include <util/system/types.h>

#include <util/generic/bitops.h>

#include <algorithm>

namespace NYT::NLogging {

////////////////////////////////////////////////////////////////////////////////

namespace {

struct TPerThreadCache;

YT_DEFINE_THREAD_LOCAL(TPerThreadCache*, Cache);
YT_DEFINE_THREAD_LOCAL(bool, CacheDestroyed);

struct TPerThreadCache
{
    TSharedMutableRef Chunk;
    size_t ChunkOffset = 0;

    ~TPerThreadCache()
    {
        TTaggedPayloadBuilder::DisablePerThreadCache();
    }

    static YT_PREVENT_TLS_CACHING TPerThreadCache* GetCache()
    {
        auto& cache = Cache();
        [[likely]] if (cache) {
            return cache;
        }
        if (CacheDestroyed()) {
            return nullptr;
        }
        static thread_local TPerThreadCache CacheData;
        cache = &CacheData;
        return cache;
    }
};

TSharedMutableRef AllocateChunk(size_t size)
{
    return TSharedMutableRef::Allocate<::NYT::NLogging::NDetail::TMessageBufferTag>(size, {.InitializeStorage = false});
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

void TTaggedPayloadBuilder::DisablePerThreadCache()
{
    Cache() = nullptr;
    CacheDestroyed() = true;
}

void TTaggedPayloadBuilder::DoReset()
{
    Buffer_.Reset();
}

void TTaggedPayloadBuilder::DoReserve(size_t newCapacity)
{
    auto oldLength = GetLength();
    newCapacity = FastClp2(newCapacity);

    auto newChunkSize = std::max(ChunkSize, newCapacity);
    // Hold the old buffer until the data is copied.
    auto oldBuffer = std::move(Buffer_);
    auto* cache = TPerThreadCache::GetCache();
    [[likely]] if (cache) {
        auto oldCapacity = End_ - Begin_;
        auto deltaCapacity = newCapacity - oldCapacity;
        if (End_ == cache->Chunk.Begin() + cache->ChunkOffset &&
            cache->ChunkOffset + deltaCapacity <= cache->Chunk.Size())
        {
            // Resize inplace.
            Buffer_ = cache->Chunk.Slice(cache->ChunkOffset - oldCapacity, cache->ChunkOffset + deltaCapacity);
            cache->ChunkOffset += deltaCapacity;
            End_ = Begin_ + newCapacity;
            return;
        }

        [[unlikely]] if (cache->ChunkOffset + newCapacity > cache->Chunk.Size()) {
            cache->Chunk = AllocateChunk(newChunkSize);
            cache->ChunkOffset = 0;
        }

        Buffer_ = cache->Chunk.Slice(cache->ChunkOffset, cache->ChunkOffset + newCapacity);
        cache->ChunkOffset += newCapacity;
    } else {
        Buffer_ = AllocateChunk(newChunkSize);
        newCapacity = newChunkSize;
    }
    if (oldLength > 0) {
        ::memcpy(Buffer_.Begin(), Begin_, oldLength);
    }
    Begin_ = Buffer_.Begin();
    End_ = Begin_ + newCapacity;
}

////////////////////////////////////////////////////////////////////////////////

void TTaggedPayloadWriter::DisablePerThreadCache()
{
    TTaggedPayloadBuilder::DisablePerThreadCache();
}

////////////////////////////////////////////////////////////////////////////////

TTaggedPayloadReader::TTaggedPayloadReader(const TTaggedLogEventPayload& payload)
    : Current_(payload.Underlying().Begin())
    , End_(payload.Underlying().End())
{ }

TStringBuf TTaggedPayloadReader::ReadMessage()
{
    YT_ASSERT(!MessageRead_);
    MessageRead_ = true;
    return ReadString();
}

std::optional<TTaggedPayloadReader::TTag> TTaggedPayloadReader::TryReadTag()
{
    YT_ASSERT(MessageRead_);
    if (Current_ == End_) {
        return std::nullopt;
    }
    auto keySize = ReadLength();
    bool wellKnown = (keySize & WellKnownTagFlag) != 0;
    auto key = ReadBytes(keySize & ~WellKnownTagFlag);
    auto value = ReadString();
    return TTag{.IsWellKnown = wellKnown, .Key = key, .Value = value};
}

ui32 TTaggedPayloadReader::ReadLength()
{
    YT_ASSERT(Current_ + sizeof(ui32) <= End_);
    ui32 size;
    ::memcpy(&size, Current_, sizeof(size));
    Current_ += sizeof(size);
    return size;
}

TStringBuf TTaggedPayloadReader::ReadBytes(ui32 size)
{
    YT_ASSERT(Current_ + size <= End_);
    TStringBuf result(Current_, size);
    Current_ += size;
    return result;
}

TStringBuf TTaggedPayloadReader::ReadString()
{
    return ReadBytes(ReadLength());
}

////////////////////////////////////////////////////////////////////////////////

TTaggedLogEventPayload MakeTaggedPayloadFromMessage(TStringBuf message)
{
    TTaggedPayloadWriter writer;
    writer.BeginMessage()->AppendString(message);
    writer.EndMessage();
    return writer.Finish();
}

TStringBuf GetMessageFromTaggedPayload(const TTaggedLogEventPayload& payload)
{
    return TTaggedPayloadReader(payload).ReadMessage();
}

std::string FormatTaggedPayload(const TTaggedLogEventPayload& payload)
{
    TTaggedPayloadReader reader(payload);
    std::string result(reader.ReadMessage());
    // Well-known tags (e.g. an error) are always written last -- the fluent |.With| chain
    // enforces this at the type level (see #TWellKnownTaggedLoggingGuard) -- so they render
    // after the |(...)| group on trailing lines in a single pass.
    bool parenOpen = false;
    while (auto tag = reader.TryReadTag()) {
        if (tag->IsWellKnown) {
            if (parenOpen) {
                result += ')';
                parenOpen = false;
            }
            result += '\n';
            result += tag->Value;
        } else {
            result += parenOpen ? ", " : " (";
            parenOpen = true;
            result += tag->Key;
            result += ": ";
            result += tag->Value;
        }
    }
    if (parenOpen) {
        result += ')';
    }
    return result;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NLogging
