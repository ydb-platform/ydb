#include <util/datetime/base.h>
#include <util/stream/zlib.h>
#include <util/stream/length.h>
#include <util/generic/buffer.h>
#include <util/generic/yexception.h>
#include <util/digest/murmur.h>
#include <util/generic/singleton.h>
#include <util/generic/function.h>
#include <util/stream/output.h>
#include <util/stream/format.h>
#include <util/stream/null.h>

#include <google/protobuf/messagext.h>

#include "eventlog.h"
#include "events_extension.h"
#include "evdecoder.h"
#include "logparser.h"
#include <library/cpp/eventlog/proto/internal.pb.h>

#include <library/cpp/json/json_writer.h>
#include <library/cpp/protobuf/json/proto2json.h>


TAtomic eventlogFrameCounter = 0;

namespace {

    const NProtobufJson::TProto2JsonConfig PROTO_2_JSON_CONFIG = NProtobufJson::TProto2JsonConfig()
        .SetMissingRepeatedKeyMode(NProtobufJson::TProto2JsonConfig::MissingKeyDefault)
        .AddStringTransform(MakeIntrusive<NProtobufJson::TBase64EncodeBytesTransform>());

    ui32 GenerateFrameId() {
        return ui32(AtomicAdd(eventlogFrameCounter, 1));
    }

    inline const NProtoBuf::Message* UnknownEventMessage() {
        return Singleton<NEventLogInternal::TUnknownEvent>();
    }

} // namespace

void TEvent::Print(IOutputStream& out, const TOutputOptions& options, const TEventState& eventState) const {
    if (options.OutputFormat == TOutputFormat::TabSeparatedRaw) {
        PrintHeader(out, options, eventState);
        DoPrint(out, {});
    } else if (options.OutputFormat == TOutputFormat::TabSeparated) {
        PrintHeader(out, options, eventState);
        DoPrint(
            out,
            EFieldOutputFlags{} | EFieldOutputFlag::EscapeNewLine | EFieldOutputFlag::EscapeBackSlash);
    } else if (options.OutputFormat == TOutputFormat::Json) {
        NJson::TJsonWriterConfig jsonWriterConfig;
        jsonWriterConfig.FormatOutput = 0;
        NJson::TJsonWriter jsonWriter(&out, jsonWriterConfig);

        jsonWriter.OpenMap();
        PrintJsonHeader(jsonWriter);
        DoPrintJson(jsonWriter);
        jsonWriter.CloseMap();
    }
}

void TEvent::PrintHeader(IOutputStream& out, const TOutputOptions& options, const TEventState& eventState) const {
    if (options.HumanReadable) {
        out << TInstant::MicroSeconds(Timestamp).ToString() << "\t";
        if (Timestamp >= eventState.FrameStartTime)
            out << "+" << HumanReadable(TDuration::MicroSeconds(Timestamp - eventState.FrameStartTime));
        else  // a bug somewhere? anyway, let's handle it in a nice fashion
            out << "-" << HumanReadable(TDuration::MicroSeconds(eventState.FrameStartTime - Timestamp));

        if (Timestamp >= eventState.PrevEventTime)
            out << " (+" << HumanReadable(TDuration::MicroSeconds(Timestamp - eventState.PrevEventTime)) << ")";
        // else: these events are async and out-of-order, relative time diff makes no sense, skip it

        out << "\tF# " << FrameId << '\t';
    } else {
        out << static_cast<TEventTimestamp>(Timestamp);
        out << '\t' << FrameId << '\t';
    }
}

void TEvent::PrintJsonHeader(NJson::TJsonWriter& jsonWriter) const {
    jsonWriter.Write("Timestamp", Timestamp);
    jsonWriter.Write("FrameId", FrameId);
}

class TProtobufEvent: public TEvent {
public:
    TProtobufEvent(TEventTimestamp t, size_t eventId, const NProtoBuf::Message& msg)
        : TEvent(eventId, t)
        , Message_(&msg)
        , EventFactory_(NProtoBuf::TEventFactory::Instance())
    {
    }

    TProtobufEvent()
        : TEvent(0, 0)
        , EventFactory_(NProtoBuf::TEventFactory::Instance())
    {
    }

    explicit TProtobufEvent(ui32 id, NProtoBuf::TEventFactory* eventFactory = NProtoBuf::TEventFactory::Instance())
        : TEvent(id, 0)
        , EventFactory_(eventFactory)
    {
        InnerMsg_.Reset(EventFactory_->CreateEvent(Class));
        Message_ = InnerMsg_.Get();
    }

    ui32 Id() const {
        return Class;
    }

    void Load(IInputStream& in) override {
        if (!!InnerMsg_) {
            InnerMsg_->ParseFromArcadiaStream(&in);
        } else {
            TransferData(&in, &Cnull);
        }
    }

    void Save(IOutputStream& out) const override {
        Message_->SerializeToArcadiaStream(&out);
    }

    void SaveToBuffer(TBufferOutput& buf) const override {
        size_t messageSize = Message_->ByteSize();
        size_t before = buf.Buffer().Size();
        buf.Buffer().Advance(messageSize);
        Y_PROTOBUF_SUPPRESS_NODISCARD Message_->SerializeToArray(buf.Buffer().Data() + before, messageSize);
    }

    TStringBuf GetName() const override {
        return EventFactory_->NameById(Id());
    }

private:
    void DoPrint(IOutputStream& out, EFieldOutputFlags flags) const override {
        EventFactory_->PrintEvent(Id(), Message_, out, flags);
    }
    void DoPrintJson(NJson::TJsonWriter& jsonWriter) const override {
        jsonWriter.OpenMap("EventBody");
        jsonWriter.Write("Type", GetName());

        jsonWriter.Write("Fields");
        NProtobufJson::Proto2Json(*GetProto(), jsonWriter, PROTO_2_JSON_CONFIG);

        jsonWriter.CloseMap();
    }

    const NProtoBuf::Message* GetProto() const override {
        if (Message_) {
            return Message_;
        }

        return UnknownEventMessage();
    }

private:
    const NProtoBuf::Message* Message_ = nullptr;
    NProtoBuf::TEventFactory* EventFactory_;
    THolder<NProtoBuf::Message> InnerMsg_;

    friend class TEventLogFrame;
};

void TEventLogFrame::LogProtobufEvent(size_t eventId, const NProtoBuf::Message& ev) {
    TProtobufEvent event(Now().MicroSeconds(), eventId, ev);

    LogEventImpl(event);
}

void TEventLogFrame::LogProtobufEvent(TEventTimestamp timestamp, size_t eventId, const NProtoBuf::Message& ev) {
    TProtobufEvent event(timestamp, eventId, ev);

    LogEventImpl(event);
}

template <>
void TEventLogFrame::DebugDump(const TProtobufEvent& ev) {
    static TMutex lock;

    with_lock (lock) {
        Cerr << ev.Timestamp << "\t" << ev.GetName() << "\t";
        ev.GetProto()->PrintJSON(Cerr);
        Cerr << Endl;
    }
}

#pragma pack(push, 1)
struct TFrameHeaderData {
    char SyncField[COMPRESSED_LOG_FRAME_SYNC_DATA.size()];
    TCompressedFrameBaseHeader Header;
    TCompressedFrameHeader2 HeaderEx;
};
#pragma pack(pop)

TEventLogFrame::TEventLogFrame(IEventLog& parentLog, bool needAlwaysSafeAdd, TWriteFrameCallbackPtr writeFrameCallback)
    : EvLog_(parentLog.HasNullBackend() ? nullptr : &parentLog)
    , NeedAlwaysSafeAdd_(needAlwaysSafeAdd)
    , ForceDump_(false)
    , WriteFrameCallback_(std::move(writeFrameCallback))
{
    DoInit();
}

TEventLogFrame::TEventLogFrame(IEventLog* parentLog, bool needAlwaysSafeAdd, TWriteFrameCallbackPtr writeFrameCallback)
    : EvLog_(parentLog)
    , NeedAlwaysSafeAdd_(needAlwaysSafeAdd)
    , ForceDump_(false)
    , WriteFrameCallback_(std::move(writeFrameCallback))
{
    if (EvLog_ && EvLog_->HasNullBackend()) {
        EvLog_ = nullptr;
    }

    DoInit();
}

TEventLogFrame::TEventLogFrame(bool needAlwaysSafeAdd, TWriteFrameCallbackPtr writeFrameCallback)
    : EvLog_(nullptr)
    , NeedAlwaysSafeAdd_(needAlwaysSafeAdd)
    , ForceDump_(false)
    , WriteFrameCallback_(std::move(writeFrameCallback))
{
    DoInit();
}

void TEventLogFrame::Flush() {
    if (EvLog_ == nullptr)
        return;

    TBuffer& buf = Buf_.Buffer();

    if (buf.Empty()) {
        return;
    }

    EvLog_->WriteFrame(buf, StartTimestamp_, EndTimestamp_, WriteFrameCallback_, std::move(MetaFlags_));

    DoInit();

    return;
}

void TEventLogFrame::SafeFlush() {
    TGuard<TMutex> g(Mtx_);
    Flush();
}

void TEventLogFrame::AddEvent(TEventTimestamp timestamp) {
    if (timestamp < StartTimestamp_) {
        StartTimestamp_ = timestamp;
    }

    if (timestamp > EndTimestamp_) {
        EndTimestamp_ = timestamp;
    }
}

void TEventLogFrame::DoInit() {
    Buf_.Buffer().Clear();

    StartTimestamp_ = (TEventTimestamp)-1;
    EndTimestamp_ = 0;
}

void TEventLogFrame::VisitEvents(ILogFrameEventVisitor& visitor, IEventFactory* eventFactory) {
    const auto doVisit = [this, &visitor, eventFactory]() {
        TBuffer& buf = Buf_.Buffer();

        TBufferInput bufferInput(buf);
        TLengthLimitedInput limitedInput(&bufferInput, buf.size());

        TEventFilter EventFilter(false);

        while (limitedInput.Left()) {
            THolder<TEvent> event = DecodeEvent(limitedInput, true, 0, &EventFilter, eventFactory);

            visitor.Visit(*event);
        }
    };
    if (NeedAlwaysSafeAdd_) {
        TGuard<TMutex> g(Mtx_);
        doVisit();
    } else {
        doVisit();
    }
}

TSelfFlushLogFrame::TSelfFlushLogFrame(IEventLog& parentLog, bool needAlwaysSafeAdd, TWriteFrameCallbackPtr writeFrameCallback)
    : TEventLogFrame(parentLog, needAlwaysSafeAdd, std::move(writeFrameCallback))
{
}

TSelfFlushLogFrame::TSelfFlushLogFrame(IEventLog* parentLog, bool needAlwaysSafeAdd, TWriteFrameCallbackPtr writeFrameCallback)
    : TEventLogFrame(parentLog, needAlwaysSafeAdd, std::move(writeFrameCallback))
{
}

TSelfFlushLogFrame::TSelfFlushLogFrame(bool needAlwaysSafeAdd, TWriteFrameCallbackPtr writeFrameCallback)
    : TEventLogFrame(needAlwaysSafeAdd, std::move(writeFrameCallback))
{
}

TSelfFlushLogFrame::~TSelfFlushLogFrame() {
    try {
        Flush();
    } catch (...) {
    }
}

IEventLog::~IEventLog() {
}

static THolder<TLogBackend> ConstructBackend(const TString& fileName, const TEventLogBackendOptions& backendOpts) {
    try {
        THolder<TLogBackend> backend;
        if (backendOpts.UseSyncPageCacheBackend) {
            backend = MakeHolder<TSyncPageCacheFileLogBackend>(fileName, backendOpts.SyncPageCacheBackendBufferSize, backendOpts.SyncPageCacheBackendMaxPendingSize);
        } else {
            backend = MakeHolder<TFileLogBackend>(fileName);
        }
        return MakeHolder<TReopenLogBackend>(std::move(backend));
    } catch (...) {
        Cdbg << "Warning: Cannot open event log '" << fileName << "': " << CurrentExceptionMessage() << "." << Endl;
    }

    return MakeHolder<TNullLogBackend>();
}

TEventLog::TEventLog(const TString& fileName, TEventLogFormat contentFormat, const TEventLogBackendOptions& backendOpts, TMaybe<TEventLogFormat> logFormat)
    : Log_(ConstructBackend(fileName, backendOpts))
    , ContentFormat_(contentFormat)
    , LogFormat_(logFormat.Defined() ? *logFormat : COMPRESSED_LOG_FORMAT_V4)
    , HasNullBackend_(Log_.IsNullLog())
    , Lz4hcCodec_(NBlockCodecs::Codec("lz4hc"))
    , ZstdCodec_(NBlockCodecs::Codec("zstd_1"))
{
    Y_ENSURE(LogFormat_ == COMPRESSED_LOG_FORMAT_V4 || LogFormat_ == COMPRESSED_LOG_FORMAT_V5);

    if (contentFormat & 0xff000000) {
        ythrow yexception() << "wrong compressed event log content format code (" << contentFormat << ")";
    }
}

TEventLog::TEventLog(const TString& fileName, TEventLogFormat contentFormat, const TEventLogBackendOptions& backendOpts)
    : TEventLog(fileName, contentFormat, backendOpts, COMPRESSED_LOG_FORMAT_V4)
{
}

TEventLog::TEventLog(const TLog& log, TEventLogFormat contentFormat, TEventLogFormat logFormat)
    : Log_(log)
    , ContentFormat_(contentFormat)
    , LogFormat_(logFormat)
    , HasNullBackend_(Log_.IsNullLog())
    , Lz4hcCodec_(NBlockCodecs::Codec("lz4hc"))
    , ZstdCodec_(NBlockCodecs::Codec("zstd_1"))
{
    if (contentFormat & 0xff000000) {
        ythrow yexception() << "wrong compressed event log content format code (" << contentFormat << ")";
    }
}

TEventLog::TEventLog(TEventLogFormat contentFormat, TEventLogFormat logFormat)
    : Log_(MakeHolder<TNullLogBackend>())
    , ContentFormat_(contentFormat)
    , LogFormat_(logFormat)
    , HasNullBackend_(true)
    , Lz4hcCodec_(NBlockCodecs::Codec("lz4hc"))
    , ZstdCodec_(NBlockCodecs::Codec("zstd_1"))
{
    if (contentFormat & 0xff000000) {
        ythrow yexception() << "wrong compressed event log content format code (" << contentFormat << ")";
    }
}

TEventLog::~TEventLog() {
}

void TEventLog::ReopenLog() {
    Log_.ReopenLog();
}

void TEventLog::CloseLog() {
    Log_.CloseLog();
}

void TEventLog::Flush() {
}

namespace {
    class TOnExceptionAction {
    public:
        TOnExceptionAction(std::function<void()>&& f)
            : F_(std::move(f))
        {
        }

        ~TOnExceptionAction() {
            if (F_ && UncaughtException()) {
                try {
                    F_();
                } catch (...) {
                }
            }
        }

    private:
        std::function<void()> F_;
    };
}

void TEventLog::WriteFrame(TBuffer& buffer,
                           TEventTimestamp startTimestamp,
                           TEventTimestamp endTimestamp,
                           TWriteFrameCallbackPtr writeFrameCallback,
                           TLogRecord::TMetaFlags metaFlags) {
    Y_ENSURE(LogFormat_ == COMPRESSED_LOG_FORMAT_V4 || LogFormat_ == COMPRESSED_LOG_FORMAT_V5);

    TBuffer& b1 = buffer;

    size_t maxCompressedLength = (LogFormat_ == COMPRESSED_LOG_FORMAT_V4) ? b1.Size() + 256 : ZstdCodec_->MaxCompressedLength(b1);

    // Reserve enough memory to minimize reallocs
    TBufferOutput outbuf(sizeof(TFrameHeaderData) + maxCompressedLength);
    TBuffer& b2 = outbuf.Buffer();
    b2.Proceed(sizeof(TFrameHeaderData));

    {
        TFrameHeaderData& hdr = *reinterpret_cast<TFrameHeaderData*>(b2.data());

        memcpy(hdr.SyncField, COMPRESSED_LOG_FRAME_SYNC_DATA.data(), COMPRESSED_LOG_FRAME_SYNC_DATA.size());
        hdr.Header.Format = (LogFormat_ << 24) | (ContentFormat_ & 0xffffff);
        hdr.Header.FrameId = GenerateFrameId();
        hdr.HeaderEx.UncompressedDatalen = (ui32)b1.Size();
        hdr.HeaderEx.StartTimestamp = startTimestamp;
        hdr.HeaderEx.EndTimestamp = endTimestamp;
        hdr.HeaderEx.PayloadChecksum = 0;
        hdr.HeaderEx.CompressorVersion = 0;
    }

    if (LogFormat_ == COMPRESSED_LOG_FORMAT_V4) {
        TBuffer encoded(b1.Size() + sizeof(TFrameHeaderData) + 256);
        Lz4hcCodec_->Encode(b1, encoded);

        TZLibCompress compr(&outbuf, ZLib::ZLib, 6, 2048);
        compr.Write(encoded.data(), encoded.size());
        compr.Finish();
    } else {
        b2.Advance(ZstdCodec_->Compress(b1, b2.Pos()));
    }

    {
        const size_t k = sizeof(TCompressedFrameBaseHeader) + COMPRESSED_LOG_FRAME_SYNC_DATA.size();
        TFrameHeaderData& hdr = *reinterpret_cast<TFrameHeaderData*>(b2.data());
        hdr.Header.Length = static_cast<ui32>(b2.size() - k);
        hdr.HeaderEx.PayloadChecksum = MurmurHash<ui32>(b2.data() + sizeof(TFrameHeaderData), b2.size() - sizeof(TFrameHeaderData));

        const size_t n = sizeof(TFrameHeaderData) - (COMPRESSED_LOG_FRAME_SYNC_DATA.size() + sizeof(hdr.HeaderEx.HeaderChecksum));
        hdr.HeaderEx.HeaderChecksum = MurmurHash<ui32>(b2.data() + COMPRESSED_LOG_FRAME_SYNC_DATA.size(), n);
    }

    const TBuffer& frameData = outbuf.Buffer();

    TOnExceptionAction actionCallback([this] {
        if (ErrorCallback_) {
            ErrorCallback_->OnWriteError();
        }
    });

    if (writeFrameCallback) {
        writeFrameCallback->OnAfterCompress(frameData, startTimestamp, endTimestamp);
    }

    Log_.Write(frameData.Data(), frameData.Size(), std::move(metaFlags));
    if (SuccessCallback_) {
        SuccessCallback_->OnWriteSuccess(frameData);
    }
}

TEvent* TProtobufEventFactory::CreateLogEvent(TEventClass c) {
    return new TProtobufEvent(c, EventFactory_);
}

TEventClass TProtobufEventFactory::ClassByName(TStringBuf name) const {
    return EventFactory_->IdByName(name);
}

TEventClass TProtobufEventFactory::EventClassBegin() const {
    const auto& items = EventFactory_->FactoryItems();

    if (items.empty()) {
        return static_cast<TEventClass>(0);
    }

    return static_cast<TEventClass>(items.begin()->first);
}

TEventClass TProtobufEventFactory::EventClassEnd() const {
    const auto& items = EventFactory_->FactoryItems();

    if (items.empty()) {
        return static_cast<TEventClass>(0);
    }

    return static_cast<TEventClass>(items.rbegin()->first + 1);
}

namespace NEvClass {
    IEventFactory* Factory() {
        return Singleton<TProtobufEventFactory>();
    }

    IEventProcessor* Processor() {
        return Singleton<TProtobufEventProcessor>();
    }
}

const NProtoBuf::Message* TUnknownEvent::GetProto() const {
    return UnknownEventMessage();
}

TStringBuf TUnknownEvent::GetName() const {
    return TStringBuf("UnknownEvent");
}

void TUnknownEvent::DoPrintJson(NJson::TJsonWriter& jsonWriter) const {
    jsonWriter.OpenMap("EventBody");
    jsonWriter.Write("Type", GetName());
    jsonWriter.Write("EventId", (size_t)Class);
    jsonWriter.CloseMap();
}

TStringBuf TEndOfFrameEvent::GetName() const {
    return TStringBuf("EndOfFrame");
}

const NProtoBuf::Message* TEndOfFrameEvent::GetProto() const {
    return Singleton<NEventLogInternal::TEndOfFrameEvent>();
}

void TEndOfFrameEvent::DoPrintJson(NJson::TJsonWriter& jsonWriter) const {
    jsonWriter.OpenMap("EventBody");
    jsonWriter.Write("Type", GetName());
    jsonWriter.OpenMap("Fields");
    jsonWriter.CloseMap();
    jsonWriter.CloseMap();
}

THolder<TEvent> MakeProtobufLogEvent(TEventTimestamp ts, TEventClass eventId, google::protobuf::Message& ev) {
    return MakeHolder<TProtobufEvent>(ts, eventId, ev);
}
