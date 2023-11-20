#include "yt_log.h"

#include "logger.h"

#include <util/generic/guid.h>

#include <util/system/mutex.h>

namespace NYT {

using namespace NLogging;

////////////////////////////////////////////////////////////////////////////////

namespace {

class TLogManager
    : public ILogManager
{
public:
    static constexpr TStringBuf CategoryName = "Wrapper";

public:
    void RegisterStaticAnchor(
        TLoggingAnchor* anchor,
        ::TSourceLocation sourceLocation,
        TStringBuf anchorMessage) override
    {
        if (anchor->Registered.exchange(true)) {
            return;
        }

        anchor->Enabled.store(true);

        auto guard = Guard(Mutex_);
        anchor->SourceLocation = sourceLocation;
        anchor->AnchorMessage = anchorMessage;
    }

    void UpdateAnchor(TLoggingAnchor* /*position*/) override
    { }

    void Enqueue(TLogEvent&& event) override
    {
        auto message = TString(event.MessageRef.ToStringBuf());
        LogMessage(
            ToImplLevel(event.Level),
            ::TSourceLocation(event.SourceFile, event.SourceLine),
            "%.*s",
            event.MessageRef.size(),
            event.MessageRef.begin());
    }

    const TLoggingCategory* GetCategory(TStringBuf categoryName) override
    {
        Y_ABORT_UNLESS(categoryName == CategoryName);
        return &Category_;
    }

    void UpdateCategory(TLoggingCategory* /*category*/) override
    {
        Y_ABORT();
    }

    bool GetAbortOnAlert() const override
    {
        return false;
    }

private:
    static ILogger::ELevel ToImplLevel(ELogLevel level)
    {
        switch (level) {
            case ELogLevel::Minimum:
            case ELogLevel::Trace:
            case ELogLevel::Debug:
                return ILogger::ELevel::DEBUG;
            case ELogLevel::Info:
                return ILogger::ELevel::INFO;
            case ELogLevel::Warning:
            case ELogLevel::Error:
                return ILogger::ELevel::ERROR;
            case ELogLevel::Alert:
            case ELogLevel::Fatal:
            case ELogLevel::Maximum:
                return ILogger::ELevel::FATAL;
        }
    }

    static void LogMessage(ILogger::ELevel level, const ::TSourceLocation& sourceLocation, const char* format, ...)
    {
        va_list args;
        va_start(args, format);
        GetLogger()->Log(level, sourceLocation, format, args);
        va_end(args);
    }

private:
    ::TMutex Mutex_;
    std::atomic<int> ActualVersion_{1};
    const TLoggingCategory Category_{
        .Name{CategoryName},
        .MinPlainTextLevel{ELogLevel::Minimum},
        .CurrentVersion{1},
        .ActualVersion = &ActualVersion_,
    };
};

TLogManager LogManager;

} // namespace

////////////////////////////////////////////////////////////////////////////////

TLogger Logger(&LogManager, TLogManager::CategoryName);

////////////////////////////////////////////////////////////////////////////////

void FormatValue(TStringBuilderBase* builder, const TGUID& value, TStringBuf /*format*/)
{
    builder->AppendString(GetGuidAsString(value));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
