#pragma once

#include "log_component.h"
#include "log_level.h"
#include "context.h"
#include "profile.h"

#include <library/cpp/logger/global/common.h>

#include <library/cpp/deprecated/atomic/atomic.h>
#include <util/stream/output.h>
#include <util/generic/strbuf.h>

#include <array>


#define YQL_LOG_IMPL(logger, component, level, preprocessor, file, line) \
    logger.NeedToLog(component, level) && NPrivateGlobalLogger::TEatStream() | \
        (*preprocessor::Preprocess(logger.CreateLogElement(component, level, file, line)))

#define YQL_LOG_IF_IMPL(logger, component, level, preprocessor, condition, file, line) \
    logger.NeedToLog(component, level) && (condition) && NPrivateGlobalLogger::TEatStream() | \
        (*preprocessor::Preprocess(logger.CreateLogElement(component, level, file, line)))

// with component logger

#define YQL_CLOG_PREP(level, component, preprocessor) YQL_LOG_IMPL(\
    ::NYql::NLog::YqlLogger(), \
    ::NYql::NLog::EComponent::component, \
    ::NYql::NLog::ELevel::level, \
    preprocessor, \
    __FILE__, __LINE__)

#define YQL_CLOG(level, component) \
    YQL_CLOG_PREP(level, component, ::NYql::NLog::TContextPreprocessor)

#define YQL_CLOG_ACTIVE(level, component) ::NYql::NLog::YqlLogger().NeedToLog( \
    ::NYql::NLog::EComponent::component, \
    ::NYql::NLog::ELevel::level)

// with component/level values logger

#define YQL_CVLOG_PREP(level, component, preprocessor) YQL_LOG_IMPL(\
    ::NYql::NLog::YqlLogger(), \
    component, \
    level, \
    preprocessor, \
    __FILE__, __LINE__)

#define YQL_CVLOG(level, component) \
    YQL_CVLOG_PREP(level, component, ::NYql::NLog::TContextPreprocessor)

#define YQL_CVLOG_ACTIVE(level, component) ::NYql::NLog::YqlLogger().NeedToLog( \
    component, \
    level)

// default logger

#define YQL_LOG_PREP(level, preprocessor) \
    YQL_CLOG_PREP(level, Default, preprocessor)

#define YQL_LOG(level) \
    YQL_LOG_PREP(level, ::NYql::NLog::TContextPreprocessor)

#define YQL_LOG_ACTIVE(level) YQL_CLOG_ACTIVE(level, Default)

// conditional logger

#define YQL_CLOG_PREP_IF(level, component, preprocessor, condition) YQL_LOG_IF_IMPL(\
    ::NYql::NLog::YqlLogger(), \
    ::NYql::NLog::EComponent::component, \
    ::NYql::NLog::ELevel::level, \
    preprocessor, \
    condition, \
    __FILE__, __LINE__)

#define YQL_CLOG_IF(level, component, condition) \
    YQL_CLOG_PREP_IF(level, component, ::NYql::NLog::TContextPreprocessor, condition)

#define YQL_LOG_PREP_IF(level, preprocessor, condition) \
    YQL_CLOG_PREP_IF(level, Default, preprocessor, condition)

#define YQL_LOG_IF(level, condition) \
    YQL_LOG_PREP_IF(level, ::NYql::NLog::TContextPreprocessor, condition)


namespace NYql {

namespace NProto {
    class TLoggingConfig;
} // NProto

namespace NLog {

using TComponentLevels =
        std::array<ELevel, EComponentHelpers::ToInt(EComponent::MaxValue)>;

void WriteLocalTime(IOutputStream* out);

/**
 * @brief Component based logger frontend.
 */
class TYqlLog: public TLog {
public:
    TYqlLog();
    TYqlLog(const TString& logType, const TComponentLevels& levels);
    TYqlLog(TAutoPtr<TLogBackend> backend, const TComponentLevels& levels);

    // XXX: not thread-safe
    void UpdateProcInfo(const TString& procName);

    ELevel GetComponentLevel(EComponent component) const {
        return ELevelHelpers::FromInt(AtomicGet(ComponentLevels_[EComponentHelpers::ToInt(component)]));
    }

    void SetComponentLevel(EComponent component, ELevel level) {
        AtomicSet(ComponentLevels_[EComponentHelpers::ToInt(component)], ELevelHelpers::ToInt(level));
    }

    bool NeedToLog(EComponent component, ELevel level) const {
        return ELevelHelpers::Lte(level, GetComponentLevel(component));
    }

    void SetMaxLogLimit(ui64 limit);

    TAutoPtr<TLogElement> CreateLogElement(EComponent component, ELevel level, TStringBuf file, int line) const;

    void WriteLogPrefix(IOutputStream* out, EComponent component, ELevel level, TStringBuf file, int line) const;

private:
    TString ProcName_;
    pid_t ProcId_;
    std::array<TAtomic, EComponentHelpers::ToInt(EComponent::MaxValue)> ComponentLevels_{0};
    mutable TAtomic WriteTruncMsg_;
};

/**
 * @brief returns reference to YQL logger instance.
 */
inline TYqlLog& YqlLogger() {
    return static_cast<TYqlLog&>(TLoggerOperator<TYqlLog>::Log());
}

/**
 * @brief returns true it YQL logger already initialized.
 */
inline bool IsYqlLoggerInitialized() {
    return TLoggerOperator<TYqlLog>::Usage();
}

/**
 * @brief Initialize logger with selected backend type.
 *
 * @param log - one of { syslog, console, cout, cerr, null, /path/to/file }
 * @param startAsDaemon - true if process is demonized
 */
void InitLogger(const TString& log, bool startAsDaemon = false);

/**
 * @brief Initialize logger with backends described in config.
*/
void InitLogger(const NProto::TLoggingConfig& loggingConfig, bool startAsDaemon = false);

/**
 * @brief Initialize logger with concrete backend.
 *
 * @param backend - logger backend
 */
void InitLogger(TAutoPtr<TLogBackend> backend);

/**
 * @brief Initialize logger with concrete output stream.
 *
 * @param out - output stream
 */
void InitLogger(IOutputStream* out);

void CleanupLogger();

void ReopenLog();

class YqlLoggerScope {
public:
    YqlLoggerScope(const TString& log, bool startAsDaemon = false) { InitLogger(log, startAsDaemon); }
    YqlLoggerScope(TAutoPtr<TLogBackend> backend) { InitLogger(backend); }
    YqlLoggerScope(IOutputStream* out) { InitLogger(out); }

    ~YqlLoggerScope() { CleanupLogger(); }
};

} // namespace NLog
} // namespace NYql

template <>
NYql::NLog::TYqlLog* CreateDefaultLogger<NYql::NLog::TYqlLog>();
