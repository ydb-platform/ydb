#include "profile.h"
#include "log.h"

#include <util/stream/format.h>


#define YQL_PERF_LOG(level, file, line) YQL_LOG_IMPL( \
    ::NYql::NLog::YqlLogger(), ::NYql::NLog::EComponent::Perf, level, \
    ::NYql::NLog::TContextPreprocessor, file, line)


namespace NYql {
namespace NLog {

TProfilingScope::~TProfilingScope() {
    if (Name_ == nullptr) {
        return;
    }

    double elapsed = static_cast<double>(::MicroSeconds() - StartedAt_);
    TStringBuf unit("us");
    if (elapsed > 1000000) {
        elapsed /= 1000000;
        unit = TStringBuf("s");
    } else if (elapsed > 1000) {
        elapsed /= 1000;
        unit = TStringBuf("ms");
    }

    auto doLog = [&]() {
        YQL_PERF_LOG(Level_, File_, Line_)
                << TStringBuf("Execution of [") << Name_
                << TStringBuf("] took ") << Prec(elapsed, 3) << unit;
    };

    if (!LogCtxPath_.first.empty() || !LogCtxPath_.second.empty()) {
        YQL_LOG_CTX_ROOT_SESSION_SCOPE(LogCtxPath_);
        doLog();
    } else {
        doLog();
    }
}

} // namspace NLog
} // namspace NYql
