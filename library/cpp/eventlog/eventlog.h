#pragma once

#include "eventlog_int.h"
#include "event_field_output.h"
#include "events_extension.h"

#include <library/cpp/blockcodecs/codecs.h>
#include <library/cpp/logger/all.h>

#include <google/protobuf/message.h>

#include <util/datetime/base.h>
#include <util/generic/ptr.h>
#include <util/generic/string.h>
#include <util/stream/output.h>
#include <util/stream/buffer.h>
#include <util/stream/str.h>
#include <util/system/mutex.h>
#include <util/stream/output.h>
#include <util/system/env.h>
#include <util/system/unaligned_mem.h>
#include <util/ysaveload.h>

#include <cstdlib>

namespace NJson {
    class TJsonWriter;
}

class IEventLog;

class TEvent : public TThrRefBase {
public:
    enum class TOutputFormat {
        TabSeparated,
        TabSeparatedRaw, // disables escaping
        Json
    };

    struct TOutputOptions {
        TOutputFormat OutputFormat = TOutputFormat::TabSeparated;
        // Dump some fields (e.g. timestamp) in more human-readable format
        bool HumanReadable = false;

        TOutputOptions(TOutputFormat outputFormat = TOutputFormat::TabSeparated)
            : OutputFormat(outputFormat)
        {
        }

        TOutputOptions(TOutputFormat outputFormat, bool humanReadable)
            : OutputFormat(outputFormat)
            , HumanReadable(humanReadable)
        {
        }
    };

    struct TEventState {
        TEventTimestamp FrameStartTime = 0;
        TEventTimestamp PrevEventTime = 0;
        TEventState() {
        }
    };

    TEvent(TEventClass c, TEventTimestamp t)
        : Class(c)
        , Timestamp(t)
    {
    }

    virtual ~TEvent() = default;

    // Note, that descendants MUST have Save() & Load() methods to alter
    // only its new variables, not the base class!
    virtual void Save(IOutputStream& out) const = 0;
    virtual void SaveToBuffer(TBufferOutput& out) const {
        Save(out);
    }

    // Note, that descendants MUST have Save() & Load() methods to alter
    // only its new variables, not the base class!
    virtual void Load(IInputStream& i) = 0;

    virtual TStringBuf GetName() const = 0;
    virtual const NProtoBuf::Message* GetProto() const = 0;

    void Print(IOutputStream& out, const TOutputOptions& options = TOutputOptions(), const TEventState& eventState = TEventState()) const;
    void PrintHeader(IOutputStream& out, const TOutputOptions& options, const TEventState& eventState) const;

    TString ToString() const {
        TStringStream buff;
        Print(buff);
        return buff.Str();
    }

    void FullSaveToBuffer(TBufferOutput& buf) const {
        SaveMessageHeader(buf);
        this->SaveToBuffer(buf);
    }

    void FullSave(IOutputStream& o) const {
        SaveMessageHeader(o);
        this->Save(o);
    }

    void FullLoad(IInputStream& i) {
        ::Load(&i, Timestamp);
        ::Load(&i, Class);
        this->Load(i);
    }

    template <class T>
    const T* Get() const {
        return static_cast<const T*>(this->GetProto());
    }

    TEventClass Class;
    TEventTimestamp Timestamp;
    ui32 FrameId = 0;

private:
    void SaveMessageHeader(IOutputStream& out) const {
        ::Save(&out, Timestamp);
        ::Save(&out, Class);
    }

    virtual void DoPrint(IOutputStream& out, EFieldOutputFlags flags) const = 0;
    virtual void DoPrintJson(NJson::TJsonWriter& jsonWriter) const = 0;

    void PrintJsonHeader(NJson::TJsonWriter& jsonWriter) const;
};

using TEventPtr = TIntrusivePtr<TEvent>;
using TConstEventPtr = TIntrusiveConstPtr<TEvent>;

class IEventProcessor {
public:
    virtual void SetOptions(const TEvent::TOutputOptions& options) {
        Options_ = options;
    }
    virtual void ProcessEvent(const TEvent* ev) = 0;
    virtual bool CheckedProcessEvent(const TEvent* ev) {
        ProcessEvent(ev);
        return true;
    }
    virtual ~IEventProcessor() = default;

protected:
    TEvent::TOutputOptions Options_;
};

class IEventFactory {
public:
    virtual TEvent* CreateLogEvent(TEventClass c) = 0;
    virtual TEventLogFormat CurrentFormat() = 0;
    virtual TEventClass ClassByName(TStringBuf name) const = 0;
    virtual TEventClass EventClassBegin() const = 0;
    virtual TEventClass EventClassEnd() const = 0;
    virtual ~IEventFactory() = default;
};

class TUnknownEvent: public TEvent {
public:
    TUnknownEvent(TEventTimestamp ts, TEventClass cls)
        : TEvent(cls, ts)
    {
    }

    ~TUnknownEvent() override = default;

    void Save(IOutputStream& /* o */) const override {
        ythrow yexception() << "TUnknownEvent cannot be saved";
    }

    void Load(IInputStream& /* i */) override {
        ythrow yexception() << "TUnknownEvent cannot be loaded";
    }

    TStringBuf GetName() const override;

private:
    void DoPrint(IOutputStream& out, EFieldOutputFlags) const override {
        out << GetName() << "\t" << (size_t)Class;
    }

    void DoPrintJson(NJson::TJsonWriter& jsonWriter) const override;

    const NProtoBuf::Message* GetProto() const override;
};

class TEndOfFrameEvent: public TEvent {
public:
    enum {
        EventClass = 0
    };

    TEndOfFrameEvent(TEventTimestamp ts)
        : TEvent(TEndOfFrameEvent::EventClass, ts)
    {
    }

    ~TEndOfFrameEvent() override = default;

    void Save(IOutputStream& o) const override {
        (void)o;
        ythrow yexception() << "TEndOfFrameEvent cannot be saved";
    }

    void Load(IInputStream& i) override {
        (void)i;
        ythrow yexception() << "TEndOfFrameEvent cannot be loaded";
    }

    TStringBuf GetName() const override;

private:
    void DoPrint(IOutputStream& out, EFieldOutputFlags) const override {
        out << GetName();
    }
    void DoPrintJson(NJson::TJsonWriter& jsonWriter) const override;

    const NProtoBuf::Message* GetProto() const override;
};

class ILogFrameEventVisitor {
public:
    virtual ~ILogFrameEventVisitor() = default;

    virtual void Visit(const TEvent& event) = 0;
};

class IWriteFrameCallback : public TAtomicRefCount<IWriteFrameCallback> {
public:
    virtual ~IWriteFrameCallback() = default;

    virtual void OnAfterCompress(const TBuffer& compressedFrame, TEventTimestamp startTimestamp, TEventTimestamp endTimestamp) = 0;
};

using TWriteFrameCallbackPtr = TIntrusivePtr<IWriteFrameCallback>;

class TEventLogFrame {
public:
    TEventLogFrame(bool needAlwaysSafeAdd = false, TWriteFrameCallbackPtr writeFrameCallback = nullptr);
    TEventLogFrame(IEventLog& parentLog, bool needAlwaysSafeAdd = false, TWriteFrameCallbackPtr writeFrameCallback = nullptr);
    TEventLogFrame(IEventLog* parentLog, bool needAlwaysSafeAdd = false, TWriteFrameCallbackPtr writeFrameCallback = nullptr);

    virtual ~TEventLogFrame() = default;

    void Flush();
    void SafeFlush();

    void ForceDump() {
        ForceDump_ = true;
    }

    template <class T>
    inline void LogEvent(const T& ev) {
        if (NeedAlwaysSafeAdd_) {
            SafeLogEvent(ev);
        } else {
            UnSafeLogEvent(ev);
        }
    }

    template <class T>
    inline void LogEvent(TEventTimestamp timestamp, const T& ev) {
        if (NeedAlwaysSafeAdd_) {
            SafeLogEvent(timestamp, ev);
        } else {
            UnSafeLogEvent(timestamp, ev);
        }
    }

    template <class T>
    inline void UnSafeLogEvent(const T& ev) {
        if (!IsEventIgnored(ev.ID))
            LogProtobufEvent(ev.ID, ev);
    }

    template <class T>
    inline void UnSafeLogEvent(TEventTimestamp timestamp, const T& ev) {
        if (!IsEventIgnored(ev.ID))
            LogProtobufEvent(timestamp, ev.ID, ev);
    }

    template <class T>
    inline void SafeLogEvent(const T& ev) {
        if (!IsEventIgnored(ev.ID)) {
            TGuard<TMutex> g(Mtx_);
            LogProtobufEvent(ev.ID, ev);
        }
    }

    template <class T>
    inline void SafeLogEvent(TEventTimestamp timestamp, const T& ev) {
        if (!IsEventIgnored(ev.ID)) {
            TGuard<TMutex> g(Mtx_);
            LogProtobufEvent(timestamp, ev.ID, ev);
        }
    }

    void VisitEvents(ILogFrameEventVisitor& visitor, IEventFactory* eventFactory);

    inline bool IsEventIgnored(size_t eventId) const {
        Y_UNUSED(eventId); // in future we might want to selectively discard only some kinds of messages
        return !IsDebugModeEnabled() && EvLog_ == nullptr && !ForceDump_;
    }

    void Enable(IEventLog& evLog) {
        EvLog_ = &evLog;
    }

    void Disable() {
        EvLog_ = nullptr;
    }

    void SetNeedAlwaysSafeAdd(bool val) {
        NeedAlwaysSafeAdd_ = val;
    }

    void SetWriteFrameCallback(TWriteFrameCallbackPtr writeFrameCallback) {
        WriteFrameCallback_ = writeFrameCallback;
    }

    void AddMetaFlag(const TString& key, const TString& value) {
        if (NeedAlwaysSafeAdd_) {
            TGuard<TMutex> g(Mtx_);
            MetaFlags_.emplace_back(key, value);
        } else {
            MetaFlags_.emplace_back(key, value);
        }
    }

protected:
    void LogProtobufEvent(size_t eventId, const NProtoBuf::Message& ev);
    void LogProtobufEvent(TEventTimestamp timestamp, size_t eventId, const NProtoBuf::Message& ev);

private:
    static bool IsDebugModeEnabled() {
        static struct TSelector {
            bool Flag;

            TSelector()
                : Flag(GetEnv("EVLOG_DEBUG") == TStringBuf("1"))
            {
            }
        } selector;

        return selector.Flag;
    }

    template <class T>
    void DebugDump(const T& ev);

    // T must be a descendant of NEvClass::TEvent
    template <class T>
    inline void LogEventImpl(const T& ev) {
        if (EvLog_ != nullptr || ForceDump_) {
            TBuffer& b = Buf_.Buffer();
            size_t lastSize = b.size();
            ::Save(&Buf_, ui32(0));
            ev.FullSaveToBuffer(Buf_);
            WriteUnaligned<ui32>(b.data() + lastSize, (ui32)(b.size() - lastSize));
            AddEvent(ev.Timestamp);
        }

        if (IsDebugModeEnabled()) {
            DebugDump(ev);
        }
    }

    void AddEvent(TEventTimestamp timestamp);
    void DoInit();

private:
    TBufferOutput Buf_;
    TEventTimestamp StartTimestamp_, EndTimestamp_;
    IEventLog* EvLog_;
    TMutex Mtx_;
    bool NeedAlwaysSafeAdd_;
    bool ForceDump_;
    TWriteFrameCallbackPtr WriteFrameCallback_;
    TLogRecord::TMetaFlags MetaFlags_;
    friend class TEventRecord;
};

class TSelfFlushLogFrame: public TEventLogFrame, public TAtomicRefCount<TSelfFlushLogFrame> {
public:
    TSelfFlushLogFrame(bool needAlwaysSafeAdd = false, TWriteFrameCallbackPtr writeFrameCallback = nullptr);
    TSelfFlushLogFrame(IEventLog& parentLog, bool needAlwaysSafeAdd = false, TWriteFrameCallbackPtr writeFrameCallback = nullptr);
    TSelfFlushLogFrame(IEventLog* parentLog, bool needAlwaysSafeAdd = false, TWriteFrameCallbackPtr writeFrameCallback = nullptr);

    virtual ~TSelfFlushLogFrame();
};

using TSelfFlushLogFramePtr = TIntrusivePtr<TSelfFlushLogFrame>;

class IEventLog: public TAtomicRefCount<IEventLog> {
public:
    class IErrorCallback {
    public:
        virtual ~IErrorCallback() {
        }

        virtual void OnWriteError() = 0;
    };

    class ISuccessCallback {
    public:
        virtual ~ISuccessCallback() {
        }

        virtual void OnWriteSuccess(const TBuffer& frameData) = 0;
    };

    virtual ~IEventLog();

    virtual void ReopenLog() = 0;
    virtual void CloseLog() = 0;
    virtual void Flush() = 0;
    virtual void SetErrorCallback(IErrorCallback*) {
    }
    virtual void SetSuccessCallback(ISuccessCallback*) {
    }

    template <class T>
    void LogEvent(const T& ev) {
        TEventLogFrame frame(*this);
        frame.LogEvent(ev);
        frame.Flush();
    }

    virtual bool HasNullBackend() const = 0;

    virtual void WriteFrame(TBuffer& buffer, 
                            TEventTimestamp startTimestamp, 
                            TEventTimestamp endTimestamp, 
                            TWriteFrameCallbackPtr writeFrameCallback = nullptr, 
                            TLogRecord::TMetaFlags metaFlags = {}) = 0;
};

struct TEventLogBackendOptions {
    bool UseSyncPageCacheBackend = false;
    size_t SyncPageCacheBackendBufferSize = 0;
    size_t SyncPageCacheBackendMaxPendingSize = 0;
};

class TEventLog: public IEventLog {
public:
    /*
         * Параметр contentformat указывает формат контента лога, например какие могут в логе
         * встретится классы событий, какие параметры у этих событий, и пр. Старший байт параметра
         * должен быть нулевым.
         */
    TEventLog(const TString& fileName, TEventLogFormat contentFormat, const TEventLogBackendOptions& backendOpts, TMaybe<TEventLogFormat> logFormat);
    TEventLog(const TString& fileName, TEventLogFormat contentFormat, const TEventLogBackendOptions& backendOpts = {});
    TEventLog(const TLog& log, TEventLogFormat contentFormat, TEventLogFormat logFormat = COMPRESSED_LOG_FORMAT_V4);
    TEventLog(TEventLogFormat contentFormat, TEventLogFormat logFormat = COMPRESSED_LOG_FORMAT_V4);

    ~TEventLog() override;

    void ReopenLog() override;
    void CloseLog() override;
    void Flush() override;
    void SetErrorCallback(IErrorCallback* errorCallback) override {
        ErrorCallback_ = errorCallback;
    }
    void SetSuccessCallback(ISuccessCallback* successCallback) override {
        SuccessCallback_ = successCallback;
    }

    template <class T>
    void LogEvent(const T& ev) {
        TEventLogFrame frame(*this);
        frame.LogEvent(ev);
        frame.Flush();
    }

    bool HasNullBackend() const override {
        return HasNullBackend_;
    }

    void WriteFrame(TBuffer& buffer, 
                    TEventTimestamp startTimestamp, 
                    TEventTimestamp endTimestamp,
                    TWriteFrameCallbackPtr writeFrameCallback = nullptr, 
                    TLogRecord::TMetaFlags metaFlags = {}) override;

private:
    mutable TLog Log_;
    TEventLogFormat ContentFormat_;
    const TEventLogFormat LogFormat_;
    bool HasNullBackend_;
    const NBlockCodecs::ICodec* const Lz4hcCodec_;
    const NBlockCodecs::ICodec* const ZstdCodec_;
    IErrorCallback* ErrorCallback_ = nullptr;
    ISuccessCallback* SuccessCallback_ = nullptr;
};

using TEventLogPtr = TIntrusivePtr<IEventLog>;

class TEventLogWithSlave: public IEventLog {
public:
    TEventLogWithSlave(IEventLog& parentLog)
        : Slave_(&parentLog)
    {
    }

    TEventLogWithSlave(const TEventLogPtr& parentLog)
        : SlavePtr_(parentLog)
        , Slave_(SlavePtr_.Get())
    {
    }

    ~TEventLogWithSlave() override {
        try {
            Slave().Flush();
        } catch (...) {
        }
    }

    void Flush() override {
        Slave().Flush();
    }

    void ReopenLog() override {
        return Slave().ReopenLog();
    }
    void CloseLog() override {
        return Slave().CloseLog();
    }

    bool HasNullBackend() const override {
        return Slave().HasNullBackend();
    }

    void WriteFrame(TBuffer& buffer, 
                    TEventTimestamp startTimestamp, 
                    TEventTimestamp endTimestamp, 
                    TWriteFrameCallbackPtr writeFrameCallback = nullptr,
                    TLogRecord::TMetaFlags metaFlags = {}) override {
        Slave().WriteFrame(buffer, startTimestamp, endTimestamp, writeFrameCallback, std::move(metaFlags));
    }

    void SetErrorCallback(IErrorCallback* errorCallback) override {
        Slave().SetErrorCallback(errorCallback);
    }

    void SetSuccessCallback(ISuccessCallback* successCallback) override {
        Slave().SetSuccessCallback(successCallback);
    }

protected:
    inline IEventLog& Slave() const {
        return *Slave_;
    }

private:
    TEventLogPtr SlavePtr_;
    IEventLog* Slave_ = nullptr;
};

extern TAtomic eventlogFrameCounter;

class TProtobufEventProcessor: public IEventProcessor {
public:
    void ProcessEvent(const TEvent* ev) override final {
        ProcessEvent(ev, &Cout);
    }

    void ProcessEvent(const TEvent* ev, IOutputStream *out) {
        UpdateEventState(ev);
        DoProcessEvent(ev, out);
        EventState_.PrevEventTime = ev->Timestamp;
    }
protected:
    virtual void DoProcessEvent(const TEvent * ev, IOutputStream *out) {
        ev->Print(*out, Options_, EventState_);
        (*out) << Endl;
    }
    ui32 CurrentFrameId_ = Max<ui32>();
    TEvent::TEventState EventState_;

private:
    void UpdateEventState(const TEvent *ev) {
        if (ev->FrameId != CurrentFrameId_) {
            EventState_.FrameStartTime = ev->Timestamp;
            EventState_.PrevEventTime = ev->Timestamp;
            CurrentFrameId_ = ev->FrameId;
        }
    }
};

class TProtobufEventFactory: public IEventFactory {
public:
    TProtobufEventFactory(NProtoBuf::TEventFactory* factory = NProtoBuf::TEventFactory::Instance())
        : EventFactory_(factory)
    {
    }

    TEvent* CreateLogEvent(TEventClass c) override;

    TEventLogFormat CurrentFormat() override {
        return 0;
    }

    TEventClass ClassByName(TStringBuf name) const override;

    TEventClass EventClassBegin() const override;

    TEventClass EventClassEnd() const override;

    ~TProtobufEventFactory() override = default;

private:
    NProtoBuf::TEventFactory* EventFactory_;
};

THolder<TEvent> MakeProtobufLogEvent(TEventTimestamp ts, TEventClass eventId, google::protobuf::Message& ev);

namespace NEvClass {
    IEventFactory* Factory();
    IEventProcessor* Processor();
}
