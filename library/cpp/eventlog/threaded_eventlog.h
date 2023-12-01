#pragma once

#include "eventlog.h"

#include <util/generic/string.h>
#include <util/thread/pool.h>

class TThreadedEventLog: public TEventLogWithSlave {
public:
    class TWrapper;
    using TOverflowCallback = std::function<void(TWrapper& wrapper)>;

    enum class EDegradationResult {
        ShouldWrite,
        ShouldDrop,
    };
    using TDegradationCallback = std::function<EDegradationResult(float fillFactor)>;

public:
    TThreadedEventLog(
        IEventLog& parentLog,
        size_t threadCount,
        size_t queueSize,
        TOverflowCallback cb,
        TDegradationCallback degradationCallback = {})
        : TEventLogWithSlave(parentLog)
        , LogSaver(TThreadPoolParams().SetThreadName("ThreadedEventLog"))
        , ThreadCount(threadCount)
        , QueueSize(queueSize)
        , OverflowCallback(std::move(cb))
        , DegradationCallback(std::move(degradationCallback))
    {
        Init();
    }

    TThreadedEventLog(
        const TEventLogPtr& parentLog,
        size_t threadCount,
        size_t queueSize,
        TOverflowCallback cb,
        TDegradationCallback degradationCallback = {})
        : TEventLogWithSlave(parentLog)
        , LogSaver(TThreadPoolParams().SetThreadName("ThreadedEventLog"))
        , ThreadCount(threadCount)
        , QueueSize(queueSize)
        , OverflowCallback(std::move(cb))
        , DegradationCallback(std::move(degradationCallback))
    {
        Init();
    }

    TThreadedEventLog(IEventLog& parentLog)
        : TThreadedEventLog(parentLog, 1, 0, TOverflowCallback())
    {
    }

    TThreadedEventLog(const TEventLogPtr& parentLog)
        : TThreadedEventLog(parentLog, 1, 0, TOverflowCallback())
    {
    }

    ~TThreadedEventLog() override {
        try {
            LogSaver.Stop();
        } catch (...) {
        }
    }

    void ReopenLog() override {
        TEventLogWithSlave::ReopenLog();
    }

    void CloseLog() override {
        LogSaver.Stop();
        TEventLogWithSlave::CloseLog();
    }

    void WriteFrame(TBuffer& buffer,
                    TEventTimestamp startTimestamp,
                    TEventTimestamp endTimestamp,
                    TWriteFrameCallbackPtr writeFrameCallback = nullptr,
                    TLogRecord::TMetaFlags metaFlags = {}) override {
        float fillFactor = 0.0f;
        if (Y_LIKELY(LogSaver.GetMaxQueueSize() > 0)) {
            fillFactor = static_cast<float>(LogSaver.Size()) / LogSaver.GetMaxQueueSize();
        }

        EDegradationResult status = EDegradationResult::ShouldWrite;
        if (DegradationCallback) {
            status = DegradationCallback(fillFactor);
        }
        if (Y_UNLIKELY(status == EDegradationResult::ShouldDrop)) {
            return;
        }

        THolder<TWrapper> wrapped;
        wrapped.Reset(new TWrapper(buffer, startTimestamp, endTimestamp, Slave(), writeFrameCallback, std::move(metaFlags)));

        if (LogSaver.Add(wrapped.Get())) {
            Y_UNUSED(wrapped.Release());
        } else if (OverflowCallback) {
            OverflowCallback(*wrapped);
        }
    }

private:
    void Init() {
        LogSaver.Start(ThreadCount, QueueSize);
    }

public:
    class TWrapper: public IObjectInQueue {
    public:
        TWrapper(TBuffer& buffer,
                 TEventTimestamp startTimestamp,
                 TEventTimestamp endTimestamp,
                 IEventLog& slave,
                 TWriteFrameCallbackPtr writeFrameCallback = nullptr,
                 TLogRecord::TMetaFlags metaFlags = {})
            : StartTimestamp(startTimestamp)
            , EndTimestamp(endTimestamp)
            , Slave(&slave)
            , WriteFrameCallback(writeFrameCallback)
            , MetaFlags(std::move(metaFlags))
        {
            Buffer.Swap(buffer);
        }

        void Process(void*) override {
            THolder<TWrapper> holder(this);

            WriteFrame();
        }

        void WriteFrame() {
            Slave->WriteFrame(Buffer, StartTimestamp, EndTimestamp, WriteFrameCallback, std::move(MetaFlags));
        }

    private:
        TBuffer Buffer;
        TEventTimestamp StartTimestamp;
        TEventTimestamp EndTimestamp;
        IEventLog* Slave;
        TWriteFrameCallbackPtr WriteFrameCallback;
        TLogRecord::TMetaFlags MetaFlags;
    };

private:
    TThreadPool LogSaver;
    const size_t ThreadCount;
    const size_t QueueSize;
    const TOverflowCallback OverflowCallback;
    const TDegradationCallback DegradationCallback;
};
