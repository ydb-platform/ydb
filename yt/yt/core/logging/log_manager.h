#pragma once

#include "log.h"

#include <yt/yt/core/tracing/public.h>

#include <yt/yt/core/ytree/public.h>

namespace NYT::NLogging {

////////////////////////////////////////////////////////////////////////////////

struct TLogWriterCacheKey
{
    TStringBuf Category;
    ELogLevel LogLevel;
    ELogFamily Family;
};

bool operator == (const TLogWriterCacheKey& lhs, const TLogWriterCacheKey& rhs);

////////////////////////////////////////////////////////////////////////////////

class TLogManager
    : public ILogManager
{
public:
    friend struct TLocalQueueReclaimer;

    ~TLogManager();

    static TLogManager* Get();

    void Configure(TLogManagerConfigPtr config, bool sync = true);
    bool IsDefaultConfigured();

    void ConfigureFromEnv();
    bool IsConfiguredFromEnv();

    void Shutdown();

    const TLoggingCategory* GetCategory(TStringBuf categoryName) override;
    void UpdateCategory(TLoggingCategory* category) override;

    void RegisterStaticAnchor(
        TLoggingAnchor* position,
        ::TSourceLocation sourceLocation,
        TStringBuf anchorMessage) override;
    TLoggingAnchor* RegisterDynamicAnchor(TString anchorMessage);
    void UpdateAnchor(TLoggingAnchor* position) override;

    void RegisterWriterFactory(const TString& typeName, const ILogWriterFactoryPtr& factory);
    void UnregisterWriterFactory(const TString& typeName);

    int GetVersion() const;
    bool GetAbortOnAlert() const override;

    void Enqueue(TLogEvent&& event) override;

    void Reopen();
    void EnableReopenOnSighup();

    void SuppressRequest(NTracing::TRequestId requestId);

    void Synchronize(TInstant deadline = TInstant::Max());

    double GetBacklogQueueFillFraction() const;

private:
    TLogManager();

    DECLARE_LEAKY_SINGLETON_FRIEND()

    class TImpl;
    const TIntrusivePtr<TImpl> Impl_;
};

////////////////////////////////////////////////////////////////////////////////

//! Sets the minimum logging level for all messages in current fiber.
class TFiberMinLogLevelGuard
    : private TMoveOnly
{
public:
    explicit TFiberMinLogLevelGuard(ELogLevel minLogLevel);
    ~TFiberMinLogLevelGuard();

private:
    const ELogLevel OldMinLogLevel_;
};

////////////////////////////////////////////////////////////////////////////////

//! Sets the minimum logging level for all messages in current fiber.
class TFiberMessageTagGuard
    : private TMoveOnly
{
public:
    explicit TFiberMessageTagGuard(std::string messageTag);

    // For use with std::optional in tests.
    TFiberMessageTagGuard(TFiberMessageTagGuard&& other);
    TFiberMessageTagGuard& operator=(TFiberMessageTagGuard&& other) = delete;

    ~TFiberMessageTagGuard();

private:
    // NB: Keeping it non-const to allow moving from in the dtor.
    std::string OldMessageTag_;
    bool Active_ = true;
};
////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NLogging

template <>
struct TSingletonTraits<NYT::NLogging::TLogManager>
{
    enum
    {
        Priority = 2048
    };
};

template <>
struct THash<NYT::NLogging::TLogWriterCacheKey>
{
    size_t operator () (const NYT::NLogging::TLogWriterCacheKey& obj) const
    {
        size_t hash = 0;
        NYT::HashCombine(hash, THash<TString>()(obj.Category));
        NYT::HashCombine(hash, static_cast<size_t>(obj.LogLevel));
        NYT::HashCombine(hash, static_cast<size_t>(obj.Family));
        return hash;
    }
};
