#include "log.h"

#include <util/datetime/systime.h>
#include <util/generic/yexception.h>
#include <util/system/guard.h>
#include <util/system/mutex.h>

using namespace NReverseGeocoder;

static size_t const TIMESTAMP_LIMIT = 32;

class TLogger {
public:
    static TLogger& Inst() {
        static TLogger logger;
        return logger;
    }

    void Setup(IOutputStream& out, ELogLevel level) {
        Out_ = &out;
        Level_ = level;
    }

    void Write(ELogLevel level, const char* message) {
        if (level <= Level_) {
            TGuard<TMutex> Lock(Lock_);
            Out_->Write(message, strlen(message));
        }
    }

    IOutputStream& OutputStream() const {
        return *Out_;
    }

    ELogLevel Level() const {
        return Level_;
    }

private:
    TLogger()
        : Out_()
        , Level_(LOG_LEVEL_DISABLE)
    {
    }

    IOutputStream* Out_;
    ELogLevel Level_;
    TMutex Lock_;
};

ELogLevel NReverseGeocoder::LogLevel() {
    return TLogger::Inst().Level();
}

void NReverseGeocoder::LogSetup(IOutputStream& out, ELogLevel level) {
    TLogger::Inst().Setup(out, level);
}

IOutputStream& NReverseGeocoder::LogOutputStream() {
    return TLogger::Inst().OutputStream();
}

static const char* T(char* buffer) {
    struct timeval timeVal;
    gettimeofday(&timeVal, nullptr);

    struct tm timeInfo;
    const time_t sec = timeVal.tv_sec;
    localtime_r(&sec, &timeInfo);

    snprintf(buffer, TIMESTAMP_LIMIT, "%02d:%02d:%02d.%06d",
             timeInfo.tm_hour, timeInfo.tm_min, timeInfo.tm_sec, (int)timeVal.tv_usec);

    return buffer;
}

void NReverseGeocoder::LogWrite(ELogLevel level, const char* message) {
    if (level > LogLevel())
        return;

    static const char* A[LOG_LEVEL_COUNT] = {
        "",         // LOG_LEVEL_DISABLE
        "\033[90m", // LOG_LEVEL_ERROR
        "\033[90m", // LOG_LEVEL_WARNING
        "\033[90m", // LOG_LEVEL_INFO
        "\033[90m", // LOG_LEVEL_DEBUG
    };

    static const char* B[LOG_LEVEL_COUNT] = {
        "",                       // LOG_LEVEL_DISABLE
        "\033[31;1mError\033[0m", // LOG_LEVEL_ERROR
        "\033[33;1mWarn\033[0m",  // LOG_LEVEL_WARNING
        "\033[32;1mInfo\033[0m",  // LOG_LEVEL_INFO
        "Debug",                  // LOG_LEVEL_DEBUG
    };

    static const char* C[LOG_LEVEL_COUNT] = {
        "",          // LOG_LEVEL_DISABLE
        "\n",        // LOG_LEVEL_ERROR
        "\n",        // LOG_LEVEL_WARNING
        "\n",        // LOG_LEVEL_INFO
        "\033[0m\n", // LOG_LEVEL_DEBUG
    };

    char buffer[LOG_MESSAGE_LIMIT], tbuffer[TIMESTAMP_LIMIT];
    // Ignore logger snprintf errors.
    snprintf(buffer, LOG_MESSAGE_LIMIT, "%s(%s) %s: %s%s",
             A[level], T(tbuffer), B[level], message, C[level]);

    TLogger::Inst().Write(level, buffer);
}
