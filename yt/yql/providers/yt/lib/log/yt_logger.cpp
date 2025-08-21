#include "yt_logger.h"

#include <yt/yql/providers/yt/lib/init_yt_api/init.h>

#include <yql/essentials/utils/log/log.h>
#include <yql/essentials/utils/log/format.h>
#include <yql/essentials/utils/backtrace/backtrace.h>

#include <yt/cpp/mapreduce/interface/logging/logger.h>

#include <library/cpp/malloc/api/malloc.h>

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
                TFileHandle fh(DebugLogFile_, OpenAlways | WrOnly | Seq | ForAppend | NoReuse);

                auto write = [&](const void* buf, size_t numBytes) {
                    const ui8* ubuf = (const ui8*)buf;

                    while (numBytes) {
                        const i32 reallyWritten = fh.Write(ubuf, numBytes);

                        if (reallyWritten < 0) {
                            Cerr << "can't write " << numBytes << " bytes to " << DebugLogFile_ << Endl;
                            return false;
                        }

                        ubuf += reallyWritten;
                        numBytes -= reallyWritten;
                    }
                    return true;
                };

                bool success = true;
                if (BufferFull_ && BufferWritePos_ < BufferSize_) {
                    success = write(buffer.Get() + BufferWritePos_, BufferSize_ - BufferWritePos_);
                }
                if (success && BufferWritePos_ > 0) {
                    write(buffer.Get(), BufferWritePos_);
                }
            } catch (const std::exception& e) {
                Cerr << e.what() << Endl;
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

        TString message;
        {
            TStringStream out;
            Printf(out, format, args);
            message = std::move(out.Str());
        }

        TString path;
        {
            TStringStream out;
            NLog::OutputLogCtx(&out, false);
            path = std::move(out.Str());
        }

        TLogRecord record(NLog::ELevelHelpers::ToLogPriority(yqlLevel), message.data(), message.length());
        NLog::YqlLogger().Contextify(record, NLog::EComponent::ProviderYt, yqlLevel, sl.File, sl.Line);
        record.MetaFlags.emplace_back(NLog::ToStringBuf(NLog::EContextKey::Path), std::move(path));

        if (needLog) {
            ELogPriority level = NLog::ELevelHelpers::ToLogPriority(yqlLevel);
            NLog::YqlLogger().Write(level, record.Data, record.Len, record.MetaFlags);
        }

        with_lock(BufferLock_) {
            if (Buffer_) {
                // Log format can be distinct from that is YqlLogger,
                // but we do not care as it is a debug buffer.
                TString out = NLog::LegacyFormat(record);

                const char* ptr = out.data();
                size_t remaining = out.length();
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
        NYql::NBacktrace::AddAfterFatalCallback([](int ){ NYql::FlushYtDebugLog(); });
    } else {
        NYT::SetLogger(NYT::ILoggerPtr());
    }
}

void FlushYtDebugLog() {
    if (!NMalloc::IsAllocatorCorrupted) {
        auto logger = NYT::GetLogger();
        if (auto yqlLogger = dynamic_cast<TGlobalLoggerImpl*>(logger.Get())) {
            yqlLogger->FlushYtDebugLog();
        }
    }
}

void DropYtDebugLog() {
    auto logger = NYT::GetLogger();
    if (auto yqlLogger = dynamic_cast<TGlobalLoggerImpl*>(logger.Get())) {
        yqlLogger->DropYtDebugLog();
    }
}

} // NYql
