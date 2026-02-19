#include "logging.h"

#include <ydb/core/nbs/cloud/storage/core/libs/common/thread.h>

#include <library/cpp/unified_agent_client/client.h>
#include <library/cpp/unified_agent_client/backend.h>
#include <library/cpp/unified_agent_client/enum.h>

#include <ydb/library/actors/core/log.h>
#include <ydb/library/actors/prof/tag.h>

#include <util/datetime/base.h>
#include <util/generic/deque.h>
#include <util/generic/map.h>
#include <util/string/builder.h>
#include <util/system/atexit.h>
#include <util/system/event.h>
#include <util/system/hostname.h>
#include <util/system/thread.h>
#include <util/thread/lfstack.h>

namespace NYdb::NBS {

using namespace NUnifiedAgent;

namespace {

////////////////////////////////////////////////////////////////////////////////

class TComponentLogBackend final
    : public TLogBackend
{
private:
    const TString Component;
    const std::shared_ptr<TLogBackend> Backend;
    const IAsyncLoggerPtr AsyncLogger;
    const TLogSettings LogSettings;

public:
    TComponentLogBackend(
            TString component,
            std::shared_ptr<TLogBackend> backend,
            IAsyncLoggerPtr asyncLogger,
            const TLogSettings& logSettings)
        : Component(std::move(component))
        , Backend(std::move(backend))
        , AsyncLogger(std::move(asyncLogger))
        , LogSettings(logSettings)
    {}

    void WriteData(const TLogRecord& rec) override
    {
        if (rec.Priority > LogSettings.FiltrationLevel) {
            return;
        }

        auto ts = TInstant::Now();

        TStringBuilder logRecord;
        if (LogSettings.UseLocalTimestamps) {
            logRecord << FormatLocalTimestamp(ts);
        } else {
            logRecord << ts;
        }

        logRecord
            << " :" << Component
            << " "  << PriorityToString(rec.Priority)
            << ": " << TrimWhitespaces({ rec.Data, rec.Len });

        if (!LogSettings.SuppressNewLine) {
            logRecord << Endl;
        }

        if (AsyncLogger) {
            AsyncLogger->Enqueue(Backend, rec.Priority, std::move(logRecord));
        } else {
            Backend->WriteData(
                TLogRecord(rec.Priority, logRecord.data(), logRecord.size()));
        }
    }

    void ReopenLog() override
    {
        Backend->ReopenLog();
    }

    void ReopenLogNoFlush() override
    {
        Backend->ReopenLogNoFlush();
    }

    ELogPriority FiltrationLevel() const override
    {
        return LogSettings.FiltrationLevel;
    }

private:
    static const char* PriorityToString(ELogPriority priority)
    {
        switch (priority) {
            case TLOG_EMERG:
                return "EMERG";
            case TLOG_ALERT:
                return "ALERT";
            case TLOG_CRIT:
                return "CRIT";
            case TLOG_ERR:
                return "ERROR";
            case TLOG_WARNING:
                return "WARN";
            case TLOG_NOTICE:
                return "NOTICE";
            case TLOG_INFO:
                return "INFO";
            case TLOG_DEBUG:
                return "DEBUG";
            case TLOG_RESOURCES:
                return "TRACE";
            default:
                return "UNKNOWN";
        }
    }

    static TString FormatLocalTimestamp(TInstant time)
    {
        struct tm localTime;
        time.LocalTime(&localTime);
        return Strftime("%Y-%m-%d-%H-%M-%S", &localTime);
    }

    static bool IsWhitespace(char ch)
    {
        return ch == 0 || ch == ' ' || ch == '\t' || ch == '\n' || ch == '\r';
    }

    static TStringBuf TrimWhitespaces(TStringBuf s)
    {
        size_t len = s.length();
        while (len > 0 && IsWhitespace(s[len - 1])) {
            --len;
        }
        return s.SubStr(0, len);
    }
};

////////////////////////////////////////////////////////////////////////////////

class TMultiLogBackend final
    : public TLogBackend
{
private:
    const TVector<std::shared_ptr<TLogBackend>> Backends;

public:
    TMultiLogBackend(TVector<std::shared_ptr<TLogBackend>> backends)
        : Backends(std::move(backends))
    {}

    void WriteData(const TLogRecord& rec) override
    {
        for (const auto& backend: Backends) {
            backend->WriteData(rec);
        }
    }

    void ReopenLog() override
    {
        for (const auto& backend: Backends) {
            backend->ReopenLog();
        }
    }

    void ReopenLogNoFlush() override
    {
        for (const auto& backend: Backends) {
            backend->ReopenLogNoFlush();
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

class TLoggingService final
    : public ILoggingService
{
private:
    const std::shared_ptr<TLogBackend> Backend;
    const TLogSettings LogSettings;

public:
    TLoggingService(
            std::shared_ptr<TLogBackend> backend,
            const TLogSettings& logSettings)
        : Backend(std::move(backend))
        , LogSettings(logSettings)
    {}

    void Start() override
    {
        // nothing to do
    }

    void Stop() override
    {
        // nothing to do
    }

    TLog CreateLog(const TString& component) override
    {
        return CreateComponentLog(component, Backend, nullptr, LogSettings);
    }
};

////////////////////////////////////////////////////////////////////////////////

class TAsyncLogger final
    : public IAsyncLogger
    , public ISimpleThread
{
    struct TItem
    {
        std::shared_ptr<TLogBackend> Backend;
        ELogPriority LogPriority;
        TString LogRecord;
    };

private:
    TLockFreeStack<TItem> Queue;
    TAutoEvent ThreadPark;
    TAtomic ShouldStop = 0;
    TDeque<TItem> Items;

public:
    ~TAsyncLogger()
    {
        Stop();
    }

    void Start() override
    {
        ISimpleThread::Start();
    }

    void Stop() override
    {
        AtomicSet(ShouldStop, 1);
        ThreadPark.Signal();

        ISimpleThread::Join();
    }

    void Enqueue(
        std::shared_ptr<TLogBackend> backend,
        ELogPriority logPriority,
        TString logRecord) override
    {
        Queue.Enqueue({
            std::move(backend),
            logPriority,
            std::move(logRecord)
        });

        ThreadPark.Signal();
    }

private:
    void* ThreadProc() override
    {
        if (AtomicGet(ShouldStop) || ExitStarted()) {
            return nullptr;
        }

        ::NYdb::NBS::SetCurrentThreadName("Logger");
        NProfiling::TMemoryTagScope tagScope("STORAGE_THREAD_LOGGER");

        while (!AtomicGet(ShouldStop)) {
            WriteData();

            ThreadPark.Wait();
        }

        WriteData();

        return nullptr;
    }

    void WriteData()
    {
        Queue.DequeueAllSingleConsumer(&Items);

        for (auto it = Items.rbegin(); it != Items.rend(); ++it) {
            it->Backend->WriteData({
                it->LogPriority,
                it->LogRecord.data(),
                it->LogRecord.size()});
        }

        Items.clear();
    }
};

////////////////////////////////////////////////////////////////////////////////

class TStorageRecordConverter final
    : public IRecordConverter
{
private:
    const bool StripTrailingNewLine;
    const TString SyslogIdentifier;

public:
    TStorageRecordConverter(
            bool stripTrailingNewLine,
            const TString& syslogIdentifier)
        : StripTrailingNewLine(stripTrailingNewLine)
        , SyslogIdentifier(syslogIdentifier)
    {
    }

    TClientMessage Convert(const TLogRecord& rec) const override
    {
        TStringBuf data {rec.Data, rec.Len};

        if (StripTrailingNewLine && data.ends_with('\n')) {
            data.remove_suffix(1);
        }

        return {
            ToString(data),
            THashMap<TString, TString> {
                {"_priority", NameOf(rec.Priority)},
                {"SYSLOG_IDENTIFIER", SyslogIdentifier},
                {"HOSTNAME", FQDNHostName()}
            }
        };
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

static const TMap<TString, ELogPriority> LogLevels = {
    { "error",    TLOG_ERR       },
    { "warn",     TLOG_WARNING   },
    { "info",     TLOG_INFO      },
    { "debug",    TLOG_DEBUG     },
    { "trace",    TLOG_RESOURCES }
};

TMaybe<ELogPriority> GetLogLevel(const TString& levelStr)
{
    auto it = LogLevels.find(levelStr);
    if (it != LogLevels.end()) {
        return it->second;
    }
    return {};
}

ILoggingServicePtr CreateLoggingService(
    const TString& logName,
    const TLogSettings& logSettings)
{
    auto backend = CreateLogBackend(logName);

    if (backend && logSettings.BackendFileName) {
        backend = NActors::CreateCompositeLogBackend(
            {backend, new TFileLogBackend(logSettings.BackendFileName)});
    }

    return std::make_shared<TLoggingService>(
        std::shared_ptr<TLogBackend>(backend.Release()),
        logSettings);
}

ILoggingServicePtr CreateLoggingService(
    std::shared_ptr<TLogBackend> backend,
    const TLogSettings& logSettings)
{
    return std::make_shared<TLoggingService>(
        std::move(backend),
        logSettings);
}

TLog CreateComponentLog(
    TString component,
    std::shared_ptr<TLogBackend> backend,
    IAsyncLoggerPtr asyncLogger,
    const TLogSettings& logSettings)
{
    return {
        MakeHolder<TComponentLogBackend>(
            std::move(component),
            std::move(backend),
            std::move(asyncLogger),
            logSettings)
    };
}

std::shared_ptr<TLogBackend> CreateMultiLogBackend(
    TVector<std::shared_ptr<TLogBackend>> backends)
{
    return std::make_shared<TMultiLogBackend>(std::move(backends));
}

IAsyncLoggerPtr CreateAsyncLogger()
{
    return std::make_shared<TAsyncLogger>();
}

ILoggingServicePtr CreateUnifiedAgentLoggingService(
    ILoggingServicePtr logging,
    const TString& endpoint,
    const TString& syslogService)
{
    if (endpoint.empty()) {
        return logging;
    }

    auto backend = MakeLogBackend(
        TClientParameters {endpoint},
        TSessionParameters {},
        MakeHolder<TStorageRecordConverter>(true, syslogService)
    );

    return CreateLoggingService(
        std::shared_ptr<TLogBackend>(backend.Release()),
        TLogSettings{}
    );

}

}   // namespace NYdb::NBS
