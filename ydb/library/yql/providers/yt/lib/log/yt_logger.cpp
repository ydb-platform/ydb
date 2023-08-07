#include <ydb/library/yql/providers/yt/lib/init_yt_api/init.h>

#include <ydb/library/yql/utils/log/log.h>

#include <yt/cpp/mapreduce/interface/logging/logger.h>

#include <util/stream/printf.h>
#include <util/stream/str.h>
#include <util/stream/file.h>
#include <util/system/spinlock.h>
#include <util/system/guard.h>
#include <util/system/file.h>
#include <util/generic/mem_copy.h>


namespace NYql {

class TGlobalLoggerImpl: public NYT::ILogger
{
public:
    TGlobalLoggerImpl(int cutLevel, size_t debugLogBufferSize, const TString& debugLogFile, bool debugLogAlwaysWrite)
        : CutLevel_(cutLevel)
        , BufferSize_(debugLogBufferSize)
        , DebugLogFile_(debugLogFile)
        , DebugLogAlwaysWrite_(debugLogAlwaysWrite)
    {
        if (BufferSize_ && DebugLogFile_) {
            Buffer_ = TArrayHolder<char>(new char[BufferSize_]);
        }
    }

    ~TGlobalLoggerImpl() {
        FlushYtDebugLog();
    }

    void FlushYtDebugLog() {
        THolder<char, TDeleteArray> buffer;
        with_lock(BufferLock_) {
            buffer.Swap(Buffer_);
        }
        if (buffer) {
            try {
                TUnbufferedFileOutput out(TFile(DebugLogFile_, OpenAlways | WrOnly | Seq | ForAppend | NoReuse));
                if (BufferFull_ && BufferWritePos_ < BufferSize_) {
                    out.Write(buffer.Get() + BufferWritePos_, BufferSize_ - BufferWritePos_);
                }
                if (BufferWritePos_ > 0) {
                    out.Write(buffer.Get(), BufferWritePos_);
                }
            } catch (...) {
                YQL_CLOG(ERROR, ProviderYt) << CurrentExceptionMessage();
            }
        }
    }

    void DropYtDebugLog() {
        if (DebugLogAlwaysWrite_) {
            FlushYtDebugLog();
            return;
        }
        with_lock(BufferLock_) {
            Buffer_.Destroy();
        }
    }

    void Log(ELevel level, const TSourceLocation& sl, const char* format, va_list args) override {
        NLog::ELevel yqlLevel = NLog::ELevel::TRACE;
        switch (level) {
        case FATAL:
            yqlLevel = NLog::ELevel::FATAL;
            break;
        case ERROR:
            yqlLevel = NLog::ELevel::ERROR;
            break;
        case INFO:
            yqlLevel = NLog::ELevel::INFO;
            break;
        case DEBUG:
            yqlLevel = NLog::ELevel::DEBUG;
            break;
        }
        const bool needLog = int(level) <= CutLevel_ && NLog::YqlLogger().NeedToLog(NLog::EComponent::ProviderYt, yqlLevel);
        with_lock(BufferLock_) {
            if (!needLog && !Buffer_) {
                return;
            }
        }

        TStringStream stream;
        NLog::YqlLogger().WriteLogPrefix(&stream, NLog::EComponent::ProviderYt, yqlLevel, sl.File, sl.Line);
        NLog::OutputLogCtx(&stream, true);
        Printf(stream, format, args);
        stream << Endl;

        if (needLog) {
            NLog::YqlLogger().Write(NLog::ELevelHelpers::ToLogPriority(yqlLevel), stream.Str().data(), stream.Str().length());
        }
        with_lock(BufferLock_) {
            if (Buffer_) {
                const char* ptr = stream.Str().data();
                size_t remaining = stream.Str().length();
                while (remaining) {
                    const size_t write = Min(remaining, BufferSize_ - BufferWritePos_);
                    MemCopy(Buffer_.Get() + BufferWritePos_, ptr, write);
                    ptr += write;
                    BufferWritePos_ += write;
                    remaining -= write;
                    if (BufferWritePos_ >= BufferSize_) {
                        BufferWritePos_ = 0;
                        BufferFull_ = true;
                    }
                }
            }
        }
    }

private:
    int CutLevel_;
    THolder<char, TDeleteArray> Buffer_;
    size_t BufferSize_ = 0;
    size_t BufferWritePos_ = 0;
    bool BufferFull_ = false;
    TSpinLock BufferLock_;
    TString DebugLogFile_;
    const bool DebugLogAlwaysWrite_;
};

void SetYtLoggerGlobalBackend(int level, size_t debugLogBufferSize, const TString& debugLogFile, bool debugLogAlwaysWrite) {
    // Important to initialize YT API before setting logger. Otherwise YT API initialization will rest it to default
    InitYtApiOnce();
    if (level >= 0 || (debugLogBufferSize && debugLogFile)) {
        NYT::SetLogger(new TGlobalLoggerImpl(level, debugLogBufferSize, debugLogFile, debugLogAlwaysWrite));
    } else {
        NYT::SetLogger(NYT::ILoggerPtr());
    }
}

void FlushYtDebugLog() {
    auto logger = NYT::GetLogger();
    if (auto yqlLogger = dynamic_cast<TGlobalLoggerImpl*>(logger.Get())) {
        yqlLogger->FlushYtDebugLog();
    }
}

void DropYtDebugLog() {
    auto logger = NYT::GetLogger();
    if (auto yqlLogger = dynamic_cast<TGlobalLoggerImpl*>(logger.Get())) {
        yqlLogger->DropYtDebugLog();
    }
}

} // NYql
