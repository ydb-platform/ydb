#include "logger.h"

#include <library/cpp/yt/assert/assert.h>

#include <library/cpp/yt/cpu_clock/clock.h>

#include <library/cpp/yt/misc/thread_name.h>

#include <util/system/compiler.h>
#include <util/system/thread.h>

namespace NYT::NLogging {

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

void OnCriticalLogEvent(
    const TLogger& logger,
    const TLogEvent& event)
{
    if (event.Level == ELogLevel::Fatal ||
        event.Level == ELogLevel::Alert && logger.GetAbortOnAlert())
    {
        fprintf(stderr, "*** Aborting on critical log event\n");
        fwrite(event.MessageRef.begin(), 1, event.MessageRef.size(), stderr);
        fprintf(stderr, "\n");
        YT_ABORT();
    }
}

TSharedRef TMessageStringBuilder::Flush()
{
    return Buffer_.Slice(0, GetLength());
}

void TMessageStringBuilder::DoReset()
{
    Buffer_.Reset();
}

struct TPerThreadCache;

YT_DEFINE_THREAD_LOCAL(TPerThreadCache*, Cache);
YT_DEFINE_THREAD_LOCAL(bool, CacheDestroyed);

struct TPerThreadCache
{
    TSharedMutableRef Chunk;
    size_t ChunkOffset = 0;

    ~TPerThreadCache()
    {
        TMessageStringBuilder::DisablePerThreadCache();
    }

    static YT_PREVENT_TLS_CACHING TPerThreadCache* GetCache()
    {
        auto& cache = Cache();
        if (Y_LIKELY(cache)) {
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

void TMessageStringBuilder::DisablePerThreadCache()
{
    Cache() = nullptr;
    CacheDestroyed() = true;
}

void TMessageStringBuilder::DoReserve(size_t newCapacity)
{
    auto oldLength = GetLength();
    newCapacity = FastClp2(newCapacity);

    auto newChunkSize = std::max(ChunkSize, newCapacity);
    // Hold the old buffer until the data is copied.
    auto oldBuffer = std::move(Buffer_);
    auto* cache = TPerThreadCache::GetCache();
    if (Y_LIKELY(cache)) {
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

        if (Y_UNLIKELY(cache->ChunkOffset + newCapacity > cache->Chunk.Size())) {
            cache->Chunk = TSharedMutableRef::Allocate<TMessageBufferTag>(newChunkSize, {.InitializeStorage = false});
            cache->ChunkOffset = 0;
        }

        Buffer_ = cache->Chunk.Slice(cache->ChunkOffset, cache->ChunkOffset + newCapacity);
        cache->ChunkOffset += newCapacity;
    } else {
        Buffer_ = TSharedMutableRef::Allocate<TMessageBufferTag>(newChunkSize, {.InitializeStorage = false});
        newCapacity = newChunkSize;
    }
    if (oldLength > 0) {
        ::memcpy(Buffer_.Begin(), Begin_, oldLength);
    }
    Begin_ = Buffer_.Begin();
    End_ = Begin_ + newCapacity;
}

} // namespace NDetail

////////////////////////////////////////////////////////////////////////////////

Y_WEAK TLoggingContext GetLoggingContext()
{
    return {
        .Instant = GetCpuInstant(),
        .ThreadId = TThread::CurrentThreadId(),
        .ThreadName = GetCurrentThreadName(),
    };
}

Y_WEAK ILogManager* GetDefaultLogManager()
{
    return nullptr;
}

////////////////////////////////////////////////////////////////////////////////

YT_DEFINE_THREAD_LOCAL(ELogLevel, ThreadMinLogLevel, ELogLevel::Minimum);

void SetThreadMinLogLevel(ELogLevel minLogLevel)
{
    ThreadMinLogLevel() = minLogLevel;
}

ELogLevel GetThreadMinLogLevel()
{
    return ThreadMinLogLevel();
}

////////////////////////////////////////////////////////////////////////////////

YT_DEFINE_THREAD_LOCAL(std::string, ThreadMessageTag);

void SetThreadMessageTag(std::string messageTag)
{
    ThreadMessageTag() = std::move(messageTag);
}

std::string& GetThreadMessageTag()
{
    return ThreadMessageTag();
}

////////////////////////////////////////////////////////////////////////////////

TLogger::TLogger(ILogManager* logManager, TStringBuf categoryName)
    : LogManager_(logManager)
    , Category_(LogManager_ ? LogManager_->GetCategory(categoryName) : nullptr)
    , MinLevel_(LogManager_ ? LoggerDefaultMinLevel : NullLoggerMinLevel)
{ }

TLogger::TLogger(TStringBuf categoryName)
    : TLogger(GetDefaultLogManager(), categoryName)
{ }

TLogger::operator bool() const
{
    return LogManager_;
}

const TLoggingCategory* TLogger::GetCategory() const
{
    return Category_;
}

void TLogger::UpdateCategory() const
{
    LogManager_->UpdateCategory(const_cast<TLoggingCategory*>(Category_));
}

bool TLogger::GetAbortOnAlert() const
{
    return LogManager_->GetAbortOnAlert();
}

bool TLogger::IsEssential() const
{
    return Essential_;
}

void TLogger::UpdateStaticAnchor(
    TLoggingAnchor* anchor,
    std::atomic<bool>* anchorRegistered,
    ::TSourceLocation sourceLocation,
    TStringBuf message) const
{
    if (!anchorRegistered->exchange(true)) {
        LogManager_->RegisterStaticAnchor(anchor, sourceLocation, message);
    }
    LogManager_->UpdateAnchor(anchor);
}

void TLogger::UpdateDynamicAnchor(TLoggingAnchor* anchor) const
{
    LogManager_->UpdateAnchor(anchor);
}

void TLogger::Write(TLogEvent&& event) const
{
    LogManager_->Enqueue(std::move(event));
}

void TLogger::AddRawTag(const std::string& tag)
{
    auto* state = GetMutableCoWState();
    if (!state->Tag.empty()) {
        state->Tag += ", ";
    }
    state->Tag += tag;
}

TLogger TLogger::WithRawTag(const std::string& tag) const &
{
    auto result = *this;
    result.AddRawTag(tag);
    return result;
}

TLogger TLogger::WithRawTag(const std::string& tag) &&
{
    AddRawTag(tag);
    return std::move(*this);
}

TLogger TLogger::WithEssential(bool essential) const &
{
    auto result = *this;
    result.Essential_ = essential;
    return result;
}

TLogger TLogger::WithEssential(bool essential) &&
{
    Essential_ = essential;
    return std::move(*this);
}

void TLogger::AddStructuredValidator(TStructuredValidator validator)
{
    auto* state = GetMutableCoWState();
    state->StructuredValidators.push_back(std::move(validator));
}

TLogger TLogger::WithStructuredValidator(TStructuredValidator validator) const &
{
    auto result = *this;
    result.AddStructuredValidator(std::move(validator));
    return result;
}

TLogger TLogger::WithStructuredValidator(TStructuredValidator validator) &&
{
    AddStructuredValidator(std::move(validator));
    return std::move(*this);
}

TLogger TLogger::WithMinLevel(ELogLevel minLevel) const &
{
    auto result = *this;
    if (result) {
        result.MinLevel_ = minLevel;
    }
    return result;
}

TLogger TLogger::WithMinLevel(ELogLevel minLevel) &&
{
    if (*this) {
        MinLevel_ = minLevel;
    }
    return std::move(*this);
}

const std::string& TLogger::GetTag() const
{
    static const std::string emptyResult;
    return CoWState_ ? CoWState_->Tag : emptyResult;
}

const TLogger::TStructuredTags& TLogger::GetStructuredTags() const
{
    static const TStructuredTags emptyResult;
    return CoWState_ ? CoWState_->StructuredTags : emptyResult;
}

const TLogger::TStructuredValidators& TLogger::GetStructuredValidators() const
{
    static const TStructuredValidators emptyResult;
    return CoWState_ ? CoWState_->StructuredValidators : emptyResult;
}

TLogger::TCoWState* TLogger::GetMutableCoWState()
{
    if (!CoWState_ || GetRefCounter(CoWState_.get())->GetRefCount() > 1) {
        auto uniquelyOwnedState = New<TCoWState>();
        if (CoWState_) {
            *uniquelyOwnedState = *CoWState_;
        }
        CoWState_ = StaticPointerCast<const TCoWState>(std::move(uniquelyOwnedState));
    }
    return const_cast<TCoWState*>(CoWState_.get());
}

void TLogger::ResetCoWState()
{
    CoWState_.Reset();
}

////////////////////////////////////////////////////////////////////////////////

void LogStructuredEvent(
    const TLogger& logger,
    NYson::TYsonString message,
    ELogLevel level)
{
    YT_VERIFY(message.GetType() == NYson::EYsonType::MapFragment);

    if (!logger.GetStructuredValidators().empty()) {
        auto samplingRate = logger.GetCategory()->StructuredValidationSamplingRate.load();
        auto p = RandomNumber<double>();
        if (p < samplingRate) {
            for (const auto& validator : logger.GetStructuredValidators()) {
                validator(message);
            }
        }
    }

    auto loggingContext = GetLoggingContext();
    auto event = NDetail::CreateLogEvent(
        loggingContext,
        logger,
        level);
    event.MessageKind = ELogMessageKind::Structured;
    event.MessageRef = message.ToSharedRef();
    event.Family = ELogFamily::Structured;
    logger.Write(std::move(event));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NLogging
