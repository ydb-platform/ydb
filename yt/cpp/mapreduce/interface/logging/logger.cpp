#include "logger.h"

#include <util/datetime/base.h>

#include <util/stream/file.h>
#include <util/stream/format.h>
#include <util/stream/printf.h>
#include <util/stream/str.h>

#include <util/system/mutex.h>
#include <util/system/rwlock.h>
#include <util/system/thread.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

static TStringBuf StripFileName(TStringBuf path) {
    TStringBuf l, r;
    if (path.TryRSplit('/', l, r) || path.TryRSplit('\\', l, r)) {
        return r;
    } else {
        return path;
    }
}

static char GetLogLevelCode(ILogger::ELevel level) {
    switch (level) {
        case ILogger::FATAL: return 'F';
        case ILogger::ERROR: return 'E';
        case ILogger::INFO: return 'I';
        case ILogger::DEBUG: return 'D';
    }
    Y_UNREACHABLE();
}

////////////////////////////////////////////////////////////////////////////////

class TNullLogger
    : public ILogger
{
public:
    void Log(ELevel level, const TSourceLocation& sourceLocation, const char* format, va_list args) override
    {
        Y_UNUSED(level);
        Y_UNUSED(sourceLocation);
        Y_UNUSED(format);
        Y_UNUSED(args);
    }
};

////////////////////////////////////////////////////////////////////////////////

class TLoggerBase
    : public ILogger
{
public:
    TLoggerBase(ELevel cutLevel)
        : CutLevel_(cutLevel)
    { }

    virtual void OutputLine(const TString& line) = 0;

    void Log(ELevel level, const TSourceLocation& sourceLocation, const char* format, va_list args) override
    {
        if (level > CutLevel_) {
            return;
        }

        TStringStream stream;
        stream << TInstant::Now().ToStringLocal()
            << " " << GetLogLevelCode(level)
            << " [" << Hex(TThread::CurrentThreadId(), HF_FULL) << "] ";
        Printf(stream, format, args);
        stream << " - " << StripFileName(sourceLocation.File) << ':' << sourceLocation.Line << Endl;

        TGuard<TMutex> guard(Mutex_);
        OutputLine(stream.Str());
    }

private:
    ELevel CutLevel_;
    TMutex Mutex_;
};

////////////////////////////////////////////////////////////////////////////////

class TStdErrLogger
    : public TLoggerBase
{
public:
    TStdErrLogger(ELevel cutLevel)
        : TLoggerBase(cutLevel)
    { }

    void OutputLine(const TString& line) override
    {
        Cerr << line;
    }
};

ILoggerPtr CreateStdErrLogger(ILogger::ELevel cutLevel)
{
    return new TStdErrLogger(cutLevel);
}

////////////////////////////////////////////////////////////////////////////////

class TFileLogger
    : public TLoggerBase
{
public:
    TFileLogger(ELevel cutLevel, const TString& path, bool append)
        : TLoggerBase(cutLevel)
        , Stream_(TFile(path, OpenAlways | WrOnly | Seq | (append ? ForAppend : EOpenMode())))
    { }

    void OutputLine(const TString& line) override
    {
        Stream_ << line;
    }

private:
    TUnbufferedFileOutput Stream_;
};

ILoggerPtr CreateFileLogger(ILogger::ELevel cutLevel, const TString& path, bool append)
{
    return new TFileLogger(cutLevel, path, append);
}
////////////////////////////////////////////////////////////////////////////////

class TBufferedFileLogger
    : public TLoggerBase
{
public:
    TBufferedFileLogger(ELevel cutLevel, const TString& path, bool append)
        : TLoggerBase(cutLevel)
        , Stream_(TFile(path, OpenAlways | WrOnly | Seq | (append ? ForAppend : EOpenMode())))
    { }

    void OutputLine(const TString& line) override
    {
        Stream_ << line;
    }

private:
    TFileOutput Stream_;
};

ILoggerPtr CreateBufferedFileLogger(ILogger::ELevel cutLevel, const TString& path, bool append)
{
    return new TBufferedFileLogger(cutLevel, path, append);
}

////////////////////////////////////////////////////////////////////////////////

static TRWMutex LoggerMutex;
static ILoggerPtr Logger;

struct TLoggerInitializer
{
    TLoggerInitializer()
    {
        Logger = new TNullLogger;
    }
} LoggerInitializer;

void SetLogger(ILoggerPtr logger)
{
    auto guard = TWriteGuard(LoggerMutex);
    if (logger) {
        Logger = logger;
    } else {
        Logger = new TNullLogger;
    }
}

ILoggerPtr GetLogger()
{
    auto guard = TReadGuard(LoggerMutex);
    return Logger;
}

////////////////////////////////////////////////////////////////////////////////

}

