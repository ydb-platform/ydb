#include "log.h"

#include <ydb/library/yql/utils/log/proto/logger_config.pb.h>

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

#include <stdio.h>
#include <time.h>

static TMutex g_InitLoggerMutex;
static int g_LoggerInitialized = 0;

namespace {

class TLimitedLogBackend final : public TLogBackend {
public:
    TLimitedLogBackend(TAutoPtr<TLogBackend> b, TAtomic& flag, ui64 limit) noexcept
        : Backend(b)
        , Flag(flag)
        , Limit(limit)
    {
    }

    ~TLimitedLogBackend() final {
    }

    void ReopenLog() final {
        Backend->ReopenLog();
    }

    void WriteData(const TLogRecord& rec) final {
        const auto remaining = AtomicGet(Limit);
        const bool final = remaining > 0 && AtomicSub(Limit, rec.Len) <= 0;
        if (remaining > 0 || rec.Priority <= TLOG_WARNING) {
            Backend->WriteData(rec);
        }
        if (final) {
            AtomicSet(Flag, 1);
        }
    }

private:
    THolder<TLogBackend> Backend;
    TAtomic& Flag;
    TAtomic Limit;
};

// Conversions between NYql::NProto::TLoggingConfig enums and NYql::NLog enums

NYql::NLog::ELevel ConvertLevel(NYql::NProto::TLoggingConfig::ELevel level) {
    using namespace NYql::NProto;
    using namespace NYql::NLog;
    switch (level) {
    case TLoggingConfig::FATAL: return ELevel::FATAL;
    case TLoggingConfig::ERROR: return ELevel::ERROR;
    case TLoggingConfig::WARN: return ELevel::WARN;
    case TLoggingConfig::INFO: return ELevel::INFO;
    case TLoggingConfig::DEBUG: return ELevel::DEBUG;
    case TLoggingConfig::TRACE: return ELevel::TRACE;
    }

    ythrow yexception() << "unknown log level: "
            << TLoggingConfig::ELevel_Name(level);
}

NYql::NLog::EComponent ConvertComponent(NYql::NProto::TLoggingConfig::EComponent c) {
    using namespace NYql::NProto;
    using namespace NYql::NLog;
    switch (c) {
    case TLoggingConfig::DEFAULT: return EComponent::Default;
    case TLoggingConfig::CORE: return EComponent::Core;
    case TLoggingConfig::CORE_EVAL: return EComponent::CoreEval;
    case TLoggingConfig::CORE_PEEPHOLE: return EComponent::CorePeepHole;
    case TLoggingConfig::CORE_EXECUTION: return EComponent::CoreExecution;
    case TLoggingConfig::SQL: return EComponent::Sql;
    case TLoggingConfig::PROVIDER_COMMON: return EComponent::ProviderCommon;
    case TLoggingConfig::PROVIDER_CONFIG: return EComponent::ProviderConfig;
    case TLoggingConfig::PROVIDER_RESULT: return EComponent::ProviderResult;
    case TLoggingConfig::PROVIDER_YT: return EComponent::ProviderYt;
    case TLoggingConfig::PROVIDER_KIKIMR: return EComponent::ProviderKikimr;
    case TLoggingConfig::PROVIDER_KQP: return EComponent::ProviderKqp;
    case TLoggingConfig::PROVIDER_RTMR: return EComponent::ProviderRtmr;
    case TLoggingConfig::PERFORMANCE: return EComponent::Perf;
    case TLoggingConfig::NET: return EComponent::Net;
    case TLoggingConfig::PROVIDER_STAT: return EComponent::ProviderStat;
    case TLoggingConfig::PROVIDER_SOLOMON: return EComponent::ProviderSolomon;
    case TLoggingConfig::PROVIDER_DQ: return EComponent::ProviderDq;
    case TLoggingConfig::PROVIDER_CLICKHOUSE: return EComponent::ProviderClickHouse;
    case TLoggingConfig::PROVIDER_YDB: return EComponent::ProviderYdb;
    case TLoggingConfig::PROVIDER_PQ: return EComponent::ProviderPq;
    case TLoggingConfig::PROVIDER_S3: return EComponent::ProviderS3;
    case TLoggingConfig::CORE_DQ: return EComponent::CoreDq;
    case TLoggingConfig::HTTP_GATEWAY: return EComponent::HttpGateway;
    case TLoggingConfig::PROVIDER_GENERIC: return EComponent::ProviderGeneric;
    case TLoggingConfig::PROVIDER_PG: return EComponent::ProviderPg;
    }

    ythrow yexception() << "unknown log component: "
            << TLoggingConfig::EComponent_Name(c);
}

TString ConvertDestinationType(NYql::NProto::TLoggingConfig::ELogTo c) {
    switch (c) {
    case NYql::NProto::TLoggingConfig::STDOUT: return "cout";
    case NYql::NProto::TLoggingConfig::STDERR: return "cerr";
    case NYql::NProto::TLoggingConfig::CONSOLE: return "console";
    default : {
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

} // namspace

namespace NYql {
namespace NLog {

void WriteLocalTime(IOutputStream* out) {
    struct timeval now;
    gettimeofday(&now, nullptr);

    struct tm tm;
    time_t seconds = static_cast<time_t>(now.tv_sec);
    localtime_r(&seconds, &tm);

    char buf[sizeof("2016-01-02 03:04:05.006")];
    int n = strftime(buf, sizeof(buf), "%Y-%m-%d %H:%M:%S.", &tm);
    snprintf(buf + n, sizeof(buf) - n, "%03" PRIu32, static_cast<ui32>(now.tv_usec) / 1000);

    out->Write(buf, sizeof(buf) - 1);
}

/**
 * TYqlLogElement
 * automaticaly adds new line char
 */
class TYqlLogElement: public TLogElement {
public:
    TYqlLogElement(const TLog* parent, ELevel level)
        : TLogElement(parent, ELevelHelpers::ToLogPriority(level))
    {
    }

    ~TYqlLogElement() {
        *this << '\n';
    }
};

TYqlLog::TYqlLog()
    : TLog()
    , ProcName_()
    , ProcId_()
    , WriteTruncMsg_(0) {}

TYqlLog::TYqlLog(const TString& logType, const TComponentLevels& levels)
    : TLog(logType)
    , ProcName_(GetProgramName())
    , ProcId_(GetPID())
    , WriteTruncMsg_(0)
{
    for (size_t component = 0; component < levels.size(); ++component) {
        SetComponentLevel(EComponentHelpers::FromInt(component), levels[component]);
    }
}

TYqlLog::TYqlLog(TAutoPtr<TLogBackend> backend, const TComponentLevels& levels)
    : TLog(backend)
    , ProcName_(GetProgramName())
    , ProcId_(GetPID())
    , WriteTruncMsg_(0)
{
    for (size_t component = 0; component < levels.size(); ++component) {
        SetComponentLevel(EComponentHelpers::FromInt(component), levels[component]);
    }
}

void TYqlLog::UpdateProcInfo(const TString& procName) {
    ProcName_ = procName;
    ProcId_ = GetPID();
}

TAutoPtr<TLogElement> TYqlLog::CreateLogElement(
        EComponent component, ELevel level,
        TStringBuf file, int line) const
{
    const bool writeMsg = AtomicCas(&WriteTruncMsg_, 0, 1);
    auto element = MakeHolder<TYqlLogElement>(this, writeMsg ? ELevel::FATAL : level);
    if (writeMsg) {
        WriteLogPrefix(element.Get(), EComponent::Default, ELevel::FATAL, __FILE__, __LINE__);
        *element << "Log is truncated by limit\n";
        *element << ELevelHelpers::ToLogPriority(level);
    }

    WriteLogPrefix(element.Get(), component, level, file, line);
    return element.Release();
}

void TYqlLog::WriteLogPrefix(IOutputStream* out, EComponent component, ELevel level, TStringBuf file, int line) const {
    // LOG FORMAT:
    //     {datetime} {level} {procname}(pid={pid}, tid={tid}) [{component}] {source_location}: {message}\n
    //

    WriteLocalTime(out);
    *out << ' '
             << ELevelHelpers::ToString(level) << ' '
             << ProcName_ << TStringBuf("(pid=") << ProcId_
             << TStringBuf(", tid=")
#ifdef _unix_
             << Hex(SystemCurrentThreadIdImpl())
#else
             << SystemCurrentThreadIdImpl()
#endif
             << TStringBuf(") [") << EComponentHelpers::ToString(component)
             << TStringBuf("] ")
             << file.RAfter(LOCSLASH_C) << ':' << line << TStringBuf(": ");
}

void TYqlLog::SetMaxLogLimit(ui64 limit) {
    auto backend = TLog::ReleaseBackend();
    TLog::ResetBackend(THolder(new TLimitedLogBackend(backend, WriteTruncMsg_, limit)));
}

void InitLogger(const TString& logType, bool startAsDaemon) {
    NProto::TLoggingConfig config;
    *config.AddLogDest() = CreateLogDestination(logType);

    InitLogger(config, startAsDaemon);
}

void InitLogger(const NProto::TLoggingConfig& config, bool startAsDaemon) {
    with_lock(g_InitLoggerMutex) {
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

        for (const auto& cmpLevel: config.GetLevels()) {
            auto component = ConvertComponent(cmpLevel.GetC());
            auto level = ConvertLevel(cmpLevel.GetL());
            levels[EComponentHelpers::ToInt(component)] = level;
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

        // Combine created backends and set them for logger
        auto& logger = TLoggerOperator<TYqlLog>::Log();
        if (backends.size() == 1) {
            logger.ResetBackend(std::move(backends[0]));
        } else if (backends.size() > 1) {
            THolder<TCompositeLogBackend> compositeBackend = MakeHolder<TCompositeLogBackend>();
            for (auto& backend : backends) {
                compositeBackend->AddLogBackend(std::move(backend));
            }
            logger.ResetBackend(std::move(compositeBackend));
        }
    }
}

void InitLogger(TAutoPtr<TLogBackend> backend) {
    with_lock(g_InitLoggerMutex) {
        ++g_LoggerInitialized;
        if (g_LoggerInitialized > 1) {
            return;
        }

        TComponentLevels levels;
        levels.fill(ELevel::INFO);
        TLoggerOperator<TYqlLog>::Set(new TYqlLog(backend, levels));
    }
}

void InitLogger(IOutputStream* out) {
    InitLogger(new TStreamLogBackend(out));
}

void CleanupLogger() {
    with_lock(g_InitLoggerMutex) {
        --g_LoggerInitialized;
        if (g_LoggerInitialized > 0) {
            return;
        }

        TLoggerOperator<TYqlLog>::Set(new TYqlLog());
    }
}

void ReopenLog() {
    with_lock(g_InitLoggerMutex) {
        TLoggerOperator<TYqlLog>::Log().ReopenLog();
    }
}

} // namespace NLog
} // namespace NYql

/**
 * creates default YQL logger writing to /dev/null
 */
template <>
NYql::NLog::TYqlLog* CreateDefaultLogger<NYql::NLog::TYqlLog>() {
    NYql::NLog::TComponentLevels levels;
    levels.fill(NYql::NLog::ELevel::INFO);
    return new NYql::NLog::TYqlLog("null", levels);
}
