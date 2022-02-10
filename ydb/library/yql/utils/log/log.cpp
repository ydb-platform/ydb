#include "log.h"

#include <library/cpp/logger/stream.h>
#include <library/cpp/logger/system.h>

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


} // namspace 
 
namespace NYql {
namespace NLog { 

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
    with_lock(g_InitLoggerMutex) {
        ++g_LoggerInitialized;
        if (g_LoggerInitialized > 1) {
            return;
        }
 
        TComponentLevels levels;
        levels.fill(ELevel::INFO);
 
        if (startAsDaemon && (
                TStringBuf("console") == logType ||
                TStringBuf("cout") == logType ||
                TStringBuf("cerr") == logType))
        {
            TLoggerOperator<TYqlLog>::Set(new TYqlLog("null", levels));
            return;
        }

        if (TStringBuf("syslog") == logType) {
            auto backend = MakeHolder<TSysLogBackend>(
                        GetProgramName().data(), TSysLogBackend::TSYSLOG_LOCAL1);
            auto& logger = TLoggerOperator<TYqlLog>::Log();
            logger.ResetBackend(THolder(backend.Release()));
        } else {
            TLoggerOperator<TYqlLog>::Set(new TYqlLog(logType, levels));
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
