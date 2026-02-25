#include "log.h"

#include "format.h"
#include "fwd_backend.h"

#include <yql/essentials/utils/log/proto/logger_config.pb.h>
#include <yql/essentials/utils/backtrace/backtrace.h>
#include <yql/essentials/utils/yql_panic.h>

#include <library/cpp/logger/stream.h>
#include <library/cpp/logger/system.h>
#include <library/cpp/logger/composite.h>

#include <util/datetime/systime.h>
#include <util/generic/strbuf.h>
#include <util/stream/format.h>
#include <util/system/getpid.h>
#include <util/system/mutex.h>
#include <util/system/progname.h>
#include <util/system/thread.i>

#include <cstdio>
#include <ctime>

namespace {

TMutex g_InitLoggerMutex;
int g_LoggerInitialized = 0;

class TLimitedLogBackend final: public TLogBackend {
public:
    TLimitedLogBackend(TAutoPtr<TLogBackend> b, TAtomic& flag, ui64 limit) noexcept
        : Backend_(b)
        , Flag_(flag)
        , Limit_(limit)
    {
    }

    ~TLimitedLogBackend() final {
    }

    void ReopenLog() final {
        Backend_->ReopenLog();
    }

    void WriteData(const TLogRecord& rec) final {
        const auto remaining = AtomicGet(Limit_);
        const bool final = remaining > 0 && AtomicSub(Limit_, rec.Len) <= 0;
        if (remaining > 0 || rec.Priority <= TLOG_WARNING) {
            Backend_->WriteData(rec);
        }
        if (final) {
            AtomicSet(Flag_, 1);
        }
    }

private:
    THolder<TLogBackend> Backend_;
    TAtomic& Flag_;
    TAtomic Limit_;
};

class TEmergencyLogOutput: public IOutputStream {
public:
    TEmergencyLogOutput()
        : Current_(Buf_.begin())
        , End_(Buf_.end())
    {
    }

    ~TEmergencyLogOutput() override {
    }

private:
    inline size_t Avail() const noexcept {
        return End_ - Current_;
    }

    void DoFlush() override {
        if (Current_ != Buf_.data()) {
            NYql::NLog::YqlLogger().Write(TLOG_EMERG, Buf_.data(), Current_ - Buf_.data());
            Current_ = Buf_.data();
        }
    }

    void DoWrite(const void* buf, size_t len) override {
        len = Min(len, Avail());
        if (len) {
            char* end = Current_ + len;
            memcpy(Current_, buf, len);
            Current_ = end;
        }
    }

private:
    std::array<char, 1 << 20> Buf_;
    char* Current_;
    char* const End_;
};

TEmergencyLogOutput EMERGENCY_LOG_OUT;

void LogBacktraceOnSignal(int signum) {
    if (NYql::NLog::IsYqlLoggerInitialized()) {
        EMERGENCY_LOG_OUT <<
#ifdef _win_
            signum
#else
            strsignal(signum)
#endif
                          << TStringBuf(" (pid=") << GetPID() << TStringBuf("): ");
        NYql::NBacktrace::KikimrBackTraceFormatImpl(&EMERGENCY_LOG_OUT);
        EMERGENCY_LOG_OUT.Flush();
    }
}

// Conversions between NYql::NProto::TLoggingConfig enums and NYql::NLog enums

NYql::NLog::ELevel ConvertLevel(NYql::NProto::TLoggingConfig::ELevel level) {
    using namespace NYql::NProto;
    using namespace NYql::NLog;
    switch (level) {
        case TLoggingConfig::FATAL:
            return ELevel::FATAL;
        case TLoggingConfig::ERROR:
            return ELevel::ERROR;
        case TLoggingConfig::WARN:
            return ELevel::WARN;
        case TLoggingConfig::INFO:
            return ELevel::INFO;
        case TLoggingConfig::DEBUG:
            return ELevel::DEBUG;
        case TLoggingConfig::TRACE:
            return ELevel::TRACE;
    }

    ythrow yexception() << "unknown log level: "
                        << TLoggingConfig::ELevel_Name(level);
}

NYql::NLog::EComponent ConvertComponent(NYql::NProto::TLoggingConfig::EComponent c) {
    using namespace NYql::NProto;
    using namespace NYql::NLog;
    switch (c) {
        case TLoggingConfig::DEFAULT:
            return EComponent::Default;
        case TLoggingConfig::CORE:
            return EComponent::Core;
        case TLoggingConfig::CORE_EVAL:
            return EComponent::CoreEval;
        case TLoggingConfig::CORE_PEEPHOLE:
            return EComponent::CorePeepHole;
        case TLoggingConfig::CORE_EXECUTION:
            return EComponent::CoreExecution;
        case TLoggingConfig::SQL:
            return EComponent::Sql;
        case TLoggingConfig::PROVIDER_COMMON:
            return EComponent::ProviderCommon;
        case TLoggingConfig::PROVIDER_CONFIG:
            return EComponent::ProviderConfig;
        case TLoggingConfig::PROVIDER_RESULT:
            return EComponent::ProviderResult;
        case TLoggingConfig::PROVIDER_YT:
            return EComponent::ProviderYt;
        case TLoggingConfig::PROVIDER_KIKIMR:
            return EComponent::ProviderKikimr;
        case TLoggingConfig::PROVIDER_KQP:
            return EComponent::ProviderKqp;
        case TLoggingConfig::PROVIDER_RTMR:
            return EComponent::ProviderRtmr;
        case TLoggingConfig::PERFORMANCE:
            return EComponent::Perf;
        case TLoggingConfig::NET:
            return EComponent::Net;
        case TLoggingConfig::PROVIDER_STAT:
            return EComponent::ProviderStat;
        case TLoggingConfig::PROVIDER_SOLOMON:
            return EComponent::ProviderSolomon;
        case TLoggingConfig::PROVIDER_DQ:
            return EComponent::ProviderDq;
        case TLoggingConfig::PROVIDER_CLICKHOUSE:
            return EComponent::ProviderClickHouse;
        case TLoggingConfig::PROVIDER_YDB:
            return EComponent::ProviderYdb;
        case TLoggingConfig::PROVIDER_PQ:
            return EComponent::ProviderPq;
        case TLoggingConfig::PROVIDER_S3:
            return EComponent::ProviderS3;
        case TLoggingConfig::CORE_DQ:
            return EComponent::CoreDq;
        case TLoggingConfig::HTTP_GATEWAY:
            return EComponent::HttpGateway;
        case TLoggingConfig::PROVIDER_GENERIC:
            return EComponent::ProviderGeneric;
        case TLoggingConfig::PROVIDER_PG:
            return EComponent::ProviderPg;
        case TLoggingConfig::PROVIDER_PURE:
            return EComponent::ProviderPure;
        case TLoggingConfig::FAST_MAP_REDUCE:
            return EComponent::FastMapReduce;
        case TLoggingConfig::PROVIDER_YTFLOW:
            return EComponent::ProviderYtflow;
    }

    ythrow yexception() << "unknown log component: "
                        << TLoggingConfig::EComponent_Name(c);
}

TString ConvertDestinationType(NYql::NProto::TLoggingConfig::ELogTo c) {
    switch (c) {
        case NYql::NProto::TLoggingConfig::STDOUT:
            return "cout";
        case NYql::NProto::TLoggingConfig::STDERR:
            return "cerr";
        case NYql::NProto::TLoggingConfig::CONSOLE:
            return "console";
        default: {
            ythrow yexception() << "unsupported ELogTo destination in Convert";
        }
    }

    ythrow yexception() << "unknown ELogTo destination";
}

NYql::NProto::TLoggingConfig::TLogDestination CreateLogDestination(const TString& c) {
    NYql::NProto::TLoggingConfig::TLogDestination destination;
    if (c == "cout") {
        destination.SetType(NYql::NProto::TLoggingConfig::STDOUT);
    } else if (c == "cerr") {
        destination.SetType(NYql::NProto::TLoggingConfig::STDERR);
    } else if (c == "console") {
        destination.SetType(NYql::NProto::TLoggingConfig::CONSOLE);
    } else {
        destination.SetType(NYql::NProto::TLoggingConfig::FILE);
        destination.SetTarget(c);
    }
    return destination;
}

NYql::NLog::TFormatter Formatter(const NYql::NProto::TLoggingConfig& config) {
    switch (config.GetFormat().Format_case()) {
        case NYql::NProto::TLoggingConfig_TFormat::kLegacyFormat:
            return NYql::NLog::LegacyFormat;
        case NYql::NProto::TLoggingConfig_TFormat::kJsonFormat:
            return NYql::NLog::JsonFormat;
        case NYql::NProto::TLoggingConfig_TFormat::FORMAT_NOT_SET:
            return NYql::NLog::LegacyFormat;
    }
}

} // namespace

namespace NYql::NLog {

namespace NImpl {

TString GetThreadId() {
#ifdef _unix_
    return ToString(Hex(SystemCurrentThreadIdImpl()));
#else
    return ToString(SystemCurrentThreadIdImpl());
#endif
}

TString GetLocalTime() {
    TStringStream time;
    NYql::NLog::WriteLocalTime(&time);
    return std::move(time.Str());
}

} // namespace NImpl

void WriteLocalTime(IOutputStream* out) {
    struct timeval now;
    gettimeofday(&now, nullptr);

    struct tm tm;
    time_t seconds = static_cast<time_t>(now.tv_sec);
    localtime_r(&seconds, &tm);

    std::array<char, sizeof("2016-01-02 03:04:05.006")> buf;
    int n = strftime(buf.data(), sizeof(buf), "%Y-%m-%d %H:%M:%S.", &tm);
    snprintf(buf.data() + n, sizeof(buf) - n, "%03" PRIu32, static_cast<ui32>(now.tv_usec) / 1000);

    out->Write(buf.data(), sizeof(buf) - 1);
}

TYqlLog::TYqlLog()
    : TLog()
    , ProcName_()
    , ProcId_()
    , WriteTruncMsg_(0)
{
}

TYqlLog::TYqlLog(const TString& logType, const TComponentLevels& levels)
    : TLog(logType)
    , ProcName_(GetProgramName())
    , ProcId_(GetPID())
    , WriteTruncMsg_(0)
{
    for (size_t component = 0; component < levels.size(); ++component) {
        SetComponentLevel(TComponentHelpers::FromInt(component), levels[component]);
    }
}

TYqlLog::TYqlLog(TAutoPtr<TLogBackend> backend, const TComponentLevels& levels)
    : TLog(backend)
    , ProcName_(GetProgramName())
    , ProcId_(GetPID())
    , WriteTruncMsg_(0)
{
    for (size_t component = 0; component < levels.size(); ++component) {
        SetComponentLevel(TComponentHelpers::FromInt(component), levels[component]);
    }
}

void TYqlLog::UpdateProcInfo(const TString& procName) {
    ProcName_ = procName;
    ProcId_ = GetPID();
}

TAutoPtr<TLogElement> TYqlLog::CreateLogElement(
    EComponent component, ELevel level,
    TStringBuf file, int line) const {
    if (/* const bool writeMsg = */ AtomicCas(&WriteTruncMsg_, 0, 1)) {
        TLogElement fatal(this, TLevelHelpers::ToLogPriority(ELevel::FATAL));
        Contextify(fatal, EComponent::Default, ELevel::FATAL, __FILE__, __LINE__);
        fatal << "Log is truncated by limit";
    }

    auto element = MakeHolder<TLogElement>(this, TLevelHelpers::ToLogPriority(level));
    Contextify(*element, component, level, file, line);
    return element.Release();
}

void TYqlLog::Contextify(TLogElement& element, EComponent component, ELevel level, TStringBuf file, int line) const {
    const auto action = [&](std::pair<TString, TString> pair) {
        element.With(std::move(pair.first), std::move(pair.second));
    };
    Contextify(action, component, level, file, line);
}

void TYqlLog::Contextify(TLogRecord& record, EComponent component, ELevel level, TStringBuf file, int line) const {
    const auto action = [&](std::pair<TString, TString> pair) {
        record.MetaFlags.emplace_back(std::move(pair.first), std::move(pair.second));
    };
    Contextify(action, component, level, file, line);
}

void TYqlLog::SetMaxLogLimit(ui64 limit) {
    THolder<TLogBackend> backend = TLog::ReleaseBackend();

    auto* forwarding = dynamic_cast<TForwardingLogBackend*>(backend.Get());
    if (!forwarding) {
        TLog::ResetBackend(THolder(new TLimitedLogBackend(backend, WriteTruncMsg_, limit)));
        return;
    }

    TAutoPtr<TLogBackend> child = forwarding->GetChild();
    TAutoPtr<TLogBackend> limited = new TLimitedLogBackend(child, WriteTruncMsg_, limit);
    forwarding->SetChild(limited);
    TLog::ResetBackend(std::move(backend));
}

void InitLogger(const TString& logType, bool startAsDaemon) {
    NProto::TLoggingConfig config;
    *config.AddLogDest() = CreateLogDestination(logType);

    InitLogger(config, startAsDaemon);
}

void InitLogger(const NProto::TLoggingConfig& config, bool startAsDaemon) {
    with_lock (g_InitLoggerMutex) {
        ++g_LoggerInitialized;
        if (g_LoggerInitialized > 1) {
            return;
        }

        TComponentLevels levels;
        if (config.HasAllComponentsLevel()) {
            levels.fill(ConvertLevel(config.GetAllComponentsLevel()));
        } else {
            levels.fill(ELevel::INFO);
        }

        for (const auto& cmpLevel : config.GetLevels()) {
            auto component = ConvertComponent(cmpLevel.GetC());
            auto level = ConvertLevel(cmpLevel.GetL());
            levels[TComponentHelpers::ToInt(component)] = level;
        }
        TLoggerOperator<TYqlLog>::Set(new TYqlLog("null", levels));

        std::vector<THolder<TLogBackend>> backends;

        // Set stderr log destination if none was described in config
        if (config.LogDestSize() == 0) {
            backends.emplace_back(CreateLogBackend("cerr", LOG_MAX_PRIORITY, false));
        }

        for (const auto& logDest : config.GetLogDest()) {
            // Generate the backend we need and temporary store it
            switch (logDest.GetType()) {
                case NProto::TLoggingConfig::STDERR:
                case NProto::TLoggingConfig::STDOUT:
                case NProto::TLoggingConfig::CONSOLE: {
                    if (!startAsDaemon) {
                        backends.emplace_back(CreateLogBackend(ConvertDestinationType(logDest.GetType()), LOG_MAX_PRIORITY, false));
                    }
                    break;
                }
                case NProto::TLoggingConfig::FILE: {
                    backends.emplace_back(CreateLogBackend(logDest.GetTarget(), LOG_MAX_PRIORITY, false));
                    break;
                }
                case NProto::TLoggingConfig::SYSLOG: {
                    backends.emplace_back(MakeHolder<TSysLogBackend>(GetProgramName().data(), TSysLogBackend::TSYSLOG_LOCAL1));
                    break;
                }
                default: {
                    break;
                }
            }
        }

        THolder<TLogBackend> backend;
        if (backends.size() == 1) {
            backend = std::move(backends[0]);
        } else if (backends.size() > 1) {
            auto compositeBackend = MakeHolder<TCompositeLogBackend>();
            for (auto& backend : backends) {
                compositeBackend->AddLogBackend(std::move(backend));
            }

            backend = std::move(compositeBackend);
        }

        if (!backend) {
            return;
        }

        auto& logger = TLoggerOperator<TYqlLog>::Log();
        logger.ResetBackend(MakeFormattingLogBackend(
            Formatter(config),
            config.GetFormat().GetIsStrict(),
            std::move(backend)));
    }
    NYql::NBacktrace::AddAfterFatalCallback([](int signo) { LogBacktraceOnSignal(signo); });
}

void InitLogger(TAutoPtr<TLogBackend> backend, TFormatter formatter, bool isStrictFormatting) {
    with_lock (g_InitLoggerMutex) {
        ++g_LoggerInitialized;
        if (g_LoggerInitialized > 1) {
            return;
        }

        backend = MakeFormattingLogBackend(std::move(formatter), isStrictFormatting, std::move(backend));

        TComponentLevels levels;
        levels.fill(ELevel::INFO);
        TLoggerOperator<TYqlLog>::Set(new TYqlLog(backend, levels));
    }
    NYql::NBacktrace::AddAfterFatalCallback([](int signo) { LogBacktraceOnSignal(signo); });
}

void InitLogger(IOutputStream* out, TFormatter formatter, bool isStrictFormatting) {
    InitLogger(new TStreamLogBackend(out), std::move(formatter), isStrictFormatting);
}

void CleanupLogger() {
    with_lock (g_InitLoggerMutex) {
        --g_LoggerInitialized;
        if (g_LoggerInitialized > 0) {
            return;
        }

        TLoggerOperator<TYqlLog>::Set(new TYqlLog());
    }
}

void ReopenLog() {
    with_lock (g_InitLoggerMutex) {
        TLoggerOperator<TYqlLog>::Log().ReopenLog();
    }
}

} // namespace NYql::NLog

/**
 * creates default YQL logger writing to /dev/null
 */
template <>
NYql::NLog::TYqlLog* CreateDefaultLogger<NYql::NLog::TYqlLog>() {
    NYql::NLog::TComponentLevels levels;
    levels.fill(NYql::NLog::ELevel::INFO);
    return new NYql::NLog::TYqlLog("null", levels);
}
