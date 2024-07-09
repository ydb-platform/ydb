#include "log.h"

static_assert(int(NActors::NLog::PRI_EMERG) == int(::TLOG_EMERG), "expect int(NActors::NLog::PRI_EMERG) == int(::TLOG_EMERG)");
static_assert(int(NActors::NLog::PRI_ALERT) == int(::TLOG_ALERT), "expect int(NActors::NLog::PRI_ALERT) == int(::TLOG_ALERT)");
static_assert(int(NActors::NLog::PRI_CRIT) == int(::TLOG_CRIT), "expect int(NActors::NLog::PRI_CRIT) == int(::TLOG_CRIT)");
static_assert(int(NActors::NLog::PRI_ERROR) == int(::TLOG_ERR), "expect int(NActors::NLog::PRI_ERROR) == int(::TLOG_ERR)");
static_assert(int(NActors::NLog::PRI_WARN) == int(::TLOG_WARNING), "expect int(NActors::NLog::PRI_WARN) == int(::TLOG_WARNING)");
static_assert(int(NActors::NLog::PRI_NOTICE) == int(::TLOG_NOTICE), "expect int(NActors::NLog::PRI_NOTICE) == int(::TLOG_NOTICE)");
static_assert(int(NActors::NLog::PRI_INFO) == int(::TLOG_INFO), "expect int(NActors::NLog::PRI_INFO) == int(::TLOG_INFO)");
static_assert(int(NActors::NLog::PRI_DEBUG) == int(::TLOG_DEBUG), "expect int(NActors::NLog::PRI_DEBUG) == int(::TLOG_DEBUG)");
static_assert(int(NActors::NLog::PRI_TRACE) == int(::TLOG_RESOURCES), "expect int(NActors::NLog::PRI_TRACE) == int(::TLOG_RESOURCES)");

namespace {
    struct TRecordWithNewline {
        ELogPriority Priority;
        TTempBuf Buf;

        TRecordWithNewline(const TLogRecord& rec)
            : Priority(rec.Priority)
            , Buf(rec.Len + 1)
        {
            Buf.Append(rec.Data, rec.Len);
            *Buf.Proceed(1) = '\n';
        }

        operator TLogRecord() const {
            return TLogRecord(Priority, Buf.Data(), Buf.Filled());
        }
    };
}

namespace NActors {
    TLoggerActor::TLoggerActor(TIntrusivePtr<NLog::TSettings> settings,
                               TAutoPtr<TLogBackend> logBackend,
                               TIntrusivePtr<NMonitoring::TDynamicCounters> counters)
        : TActor(&TLoggerActor::StateFunc)
        , Settings(settings)
        , LogBackend(logBackend.Release())
        , Metrics(std::make_unique<TLoggerCounters>(counters))
        , LogBuffer(*Metrics, *Settings)
    {
    }

    TLoggerActor::TLoggerActor(TIntrusivePtr<NLog::TSettings> settings,
                               std::shared_ptr<TLogBackend> logBackend,
                               TIntrusivePtr<NMonitoring::TDynamicCounters> counters)
        : TActor(&TLoggerActor::StateFunc)
        , Settings(settings)
        , LogBackend(logBackend)
        , Metrics(std::make_unique<TLoggerCounters>(counters))
        , LogBuffer(*Metrics, *Settings)
    {
    }

    TLoggerActor::TLoggerActor(TIntrusivePtr<NLog::TSettings> settings,
                               TAutoPtr<TLogBackend> logBackend,
                               std::shared_ptr<NMonitoring::TMetricRegistry> metrics)
        : TActor(&TLoggerActor::StateFunc)
        , Settings(settings)
        , LogBackend(logBackend.Release())
        , Metrics(std::make_unique<TLoggerMetrics>(metrics))
        , LogBuffer(*Metrics, *Settings)
    {
    }

    TLoggerActor::TLoggerActor(TIntrusivePtr<NLog::TSettings> settings,
                               std::shared_ptr<TLogBackend> logBackend,
                               std::shared_ptr<NMonitoring::TMetricRegistry> metrics)
        : TActor(&TLoggerActor::StateFunc)
        , Settings(settings)
        , LogBackend(logBackend)
        , Metrics(std::make_unique<TLoggerMetrics>(metrics))
        , LogBuffer(*Metrics, *Settings)
    {
    }

    TLoggerActor::~TLoggerActor() {
    }

    void TLoggerActor::Log(TInstant time, NLog::EPriority priority, NLog::EComponent component, const char* c, ...) {
        Metrics->IncDirectMsgs();
        if (Settings && Settings->Satisfies(priority, component, 0ull)) {
            va_list params;
            va_start(params, c);
            TString formatted;
            vsprintf(formatted, c, params);

            auto ok = OutputRecord(time, NLog::EPrio(priority), component, formatted);
            Y_UNUSED(ok);
            va_end(params);
        }
    }

    void TLoggerActor::Throttle(const NLog::TSettings& settings) {
        // throttling via Sleep was removed since it causes unexpected
        // incidents when users try to set AllowDrop=false.
        Y_UNUSED(settings);
    }

    void TLoggerActor::FlushLogBufferMessage() {
        if (!LogBuffer.IsEmpty()) {
            NLog::TEvLog *log = LogBuffer.Pop();
            if (!OutputRecord(log)) {
                BecomeDefunct();
            }
            delete log;
        }
    }

    void TLoggerActor::FlushLogBufferMessageEvent(TFlushLogBuffer::TPtr& ev, const NActors::TActorContext& ctx) {
        Y_UNUSED(ev);
        FlushLogBufferMessage();

        ui64 ignoredCount = LogBuffer.GetIgnoredCount();
        if (ignoredCount > 0) {
            NLog::EPrio prio = LogBuffer.GetIgnoredHighestPrio();
            TString message = Sprintf("Logger overflow! Ignored %" PRIu64 "  log records with priority [%s] or lower!", ignoredCount, PriorityToString(prio));
            if (!OutputRecord(ctx.Now(), NActors::NLog::EPrio::Error, Settings->LoggerComponent, message)) {
                BecomeDefunct();
            }
            LogBuffer.ClearIgnoredCount();
        }

        if (!LogBuffer.IsEmpty()) {
            ctx.Send(ctx.SelfID, ev->Release().Release());
        }
    }

    void TLoggerActor::WriteMessageStat(const NLog::TEvLog& ev) {
        Metrics->IncActorMsgs();

        const auto prio = ev.Level.ToPrio();

        switch (prio) {
            case ::NActors::NLog::EPrio::Alert:
                Metrics->IncAlertMsgs();
                break;
            case ::NActors::NLog::EPrio::Emerg:
                Metrics->IncEmergMsgs();
                break;
            default:
                break;
        }

    }

    void TLoggerActor::HandleLogEvent(NLog::TEvLog::TPtr& ev, const NActors::TActorContext& ctx) {
        i64 delayMillisec = (ctx.Now() - ev->Get()->Stamp).MilliSeconds();
        WriteMessageStat(*ev->Get());
        if (Settings->AllowDrop) {
            if (PassedCount > 10 && delayMillisec > (i64)Settings->TimeThresholdMs || !LogBuffer.IsEmpty() || LogBuffer.CheckLogIgnoring()) {
                if (LogBuffer.IsEmpty() && !LogBuffer.CheckLogIgnoring()) {
                    ctx.Send(ctx.SelfID, new TFlushLogBuffer());
                }
                LogBuffer.AddLog(ev->Release().Release());
                PassedCount = 0;

                if (delayMillisec < (i64)Settings->TimeThresholdMs && !LogBuffer.CheckLogIgnoring()) {
                    FlushLogBufferMessage();
                }
                return;
            }

            PassedCount++;
        }

        if (!OutputRecord(ev->Get())) {
            BecomeDefunct();
        }
    }

    void TLoggerActor::BecomeDefunct() {
        Become(&TThis::StateDefunct);
        Schedule(WakeupInterval, new TEvents::TEvWakeup);
    }

    void TLoggerActor::HandleLogComponentLevelRequest(TLogComponentLevelRequest::TPtr& ev, const NActors::TActorContext& ctx) {
        Metrics->IncLevelRequests();
        TString explanation;
        int code = Settings->SetLevel(ev->Get()->Priority, ev->Get()->Component, explanation);
        ctx.Send(ev->Sender, new TLogComponentLevelResponse(code, explanation));
    }

    void TLoggerActor::RenderComponentPriorities(IOutputStream& str) {
        using namespace NLog;
        HTML(str) {
            TAG(TH4) {
                str << "Priority Settings for the Components";
            }
            TABLE_SORTABLE_CLASS("table") {
                TABLEHEAD() {
                    TABLER() {
                        TABLEH() {
                            str << "Component";
                        }
                        TABLEH() {
                            str << "Level";
                        }
                        TABLEH() {
                            str << "Sampling Level";
                        }
                        TABLEH() {
                            str << "Sampling Rate";
                        }
                    }
                }
                TABLEBODY() {
                    for (EComponent i = Settings->MinVal; i < Settings->MaxVal; i++) {
                        auto name = Settings->ComponentName(i);
                        if (!*name)
                            continue;
                        NLog::TComponentSettings componentSettings = Settings->GetComponentSettings(i);

                        TABLER() {
                            TABLED() {
                                str << "<a href='logger?c=" << i << "'>" << name << "</a>";
                            }
                            TABLED() {
                                str << PriorityToString(EPrio(componentSettings.Raw.X.Level));
                            }
                            TABLED() {
                                str << PriorityToString(EPrio(componentSettings.Raw.X.SamplingLevel));
                            }
                            TABLED() {
                                str << componentSettings.Raw.X.SamplingRate;
                            }
                        }
                    }
                }
            }
        }
    }

    /*
     * Logger INFO:
     * 1. Current priority settings from components
     * 2. Number of log messages (via actors events, directly)
     * 3. Number of messages per components, per priority
     * 4. Log level changes (last N changes)
     */
    void TLoggerActor::HandleMonInfo(NMon::TEvHttpInfo::TPtr& ev, const TActorContext& ctx) {
        const auto& params = ev->Get()->Request.GetParams();
        NLog::EComponent component = NLog::InvalidComponent;
        NLog::EPriority priority = NLog::PRI_DEBUG;
        NLog::EPriority samplingPriority = NLog::PRI_DEBUG;
        ui32 samplingRate = 0;
        bool hasComponent = false;
        bool hasPriority = false;
        bool hasSamplingPriority = false;
        bool hasSamplingRate = false;
        bool hasAllowDrop = false;
        int allowDrop = 0;
        if (params.Has("c")) {
            if (TryFromString(params.Get("c"), component) && (component == NLog::InvalidComponent || Settings->IsValidComponent(component))) {
                hasComponent = true;
                if (params.Has("p")) {
                    int rawPriority;
                    if (TryFromString(params.Get("p"), rawPriority) && NLog::TSettings::IsValidPriority((NLog::EPriority)rawPriority)) {
                        priority = (NLog::EPriority)rawPriority;
                        hasPriority = true;
                    }
                }
                if (params.Has("sp")) {
                    int rawPriority;
                    if (TryFromString(params.Get("sp"), rawPriority) && NLog::TSettings::IsValidPriority((NLog::EPriority)rawPriority)) {
                        samplingPriority = (NLog::EPriority)rawPriority;
                        hasSamplingPriority = true;
                    }
                }
                if (params.Has("sr")) {
                    if (TryFromString(params.Get("sr"), samplingRate)) {
                        hasSamplingRate = true;
                    }
                }
            }
        }
        if (params.Has("allowdrop")) {
            if (TryFromString(params.Get("allowdrop"), allowDrop)) {
                hasAllowDrop = true;
            }
        }

        TStringStream str;
        if (hasComponent && !hasPriority && !hasSamplingPriority && !hasSamplingRate) {
            NLog::TComponentSettings componentSettings = Settings->GetComponentSettings(component);
            ui32 samplingRate = componentSettings.Raw.X.SamplingRate;
            HTML(str) {
                DIV_CLASS("row") {
                    DIV_CLASS("col-md-12") {
                        TAG(TH4) {
                            str << "Current log settings for " << Settings->ComponentName(component) << Endl;
                        }
                        UL() {
                            LI() {
                                str << "Priority: "
                                    << NLog::PriorityToString(NLog::EPrio(componentSettings.Raw.X.Level));
                            }
                            LI() {
                                str << "Sampling priority: "
                                    << NLog::PriorityToString(NLog::EPrio(componentSettings.Raw.X.SamplingLevel));
                            }
                            LI() {
                                str << "Sampling rate: "
                                    << samplingRate;
                            }
                        }
                    }
                }

                DIV_CLASS("row") {
                    DIV_CLASS("col-md-12") {
                        TAG(TH4) {
                            str << "Change priority" << Endl;
                        }
                        UL() {
                            for (int p = NLog::PRI_EMERG; p <= NLog::PRI_TRACE; ++p) {
                                LI() {
                                    str << "<a href='logger?c=" << component << "&p=" << p << "'>"
                                        << NLog::PriorityToString(NLog::EPrio(p)) << "</a>";
                                }
                            }
                        }
                        TAG(TH4) {
                            str << "Change sampling priority" << Endl;
                        }
                        UL() {
                            for (int p = NLog::PRI_EMERG; p <= NLog::PRI_TRACE; ++p) {
                                LI() {
                                    str << "<a href='logger?c=" << component << "&sp=" << p << "'>"
                                        << NLog::PriorityToString(NLog::EPrio(p)) << "</a>";
                                }
                            }
                        }
                        TAG(TH4) {
                            str << "Change sampling rate" << Endl;
                        }
                        str << "<form method=\"GET\">" << Endl;
                        str << "Rate: <input type=\"number\" name=\"sr\" value=\"" << samplingRate << "\"/>" << Endl;
                        str << "<input type=\"hidden\" name=\"c\" value=\"" << component << "\">" << Endl;
                        str << "<input class=\"btn btn-primary\" type=\"submit\" value=\"Change\"/>" << Endl;
                        str << "</form>" << Endl;
                        TAG(TH4) {
                            str << "<a href='logger'>Cancel</a>" << Endl;
                        }
                    }
                }
            }

        } else {
            TString explanation;
            if (hasComponent && hasPriority) {
                Settings->SetLevel(priority, component, explanation);
            }
            if (hasComponent && hasSamplingPriority) {
                Settings->SetSamplingLevel(samplingPriority, component, explanation);
            }
            if (hasComponent && hasSamplingRate) {
                Settings->SetSamplingRate(samplingRate, component, explanation);
            }
            if (hasAllowDrop) {
                Settings->SetAllowDrop(allowDrop);
            }

            HTML(str) {
                if (!explanation.empty()) {
                    DIV_CLASS("row") {
                        DIV_CLASS("col-md-12 alert alert-info") {
                            str << explanation;
                        }
                    }
                }

                DIV_CLASS("row") {
                    DIV_CLASS("col-md-8") {
                        RenderComponentPriorities(str);
                    }
                    DIV_CLASS("col-md-4") {
                        TAG(TH4) {
                            str << "Change priority for all components";
                        }
                        TABLE_CLASS("table table-condensed") {
                            TABLEHEAD() {
                                TABLER() {
                                    TABLEH() {
                                        str << "Priority";
                                    }
                                }
                            }
                            TABLEBODY() {
                                for (int p = NLog::PRI_EMERG; p <= NLog::PRI_TRACE; ++p) {
                                    TABLER() {
                                        TABLED() {
                                            str << "<a href = 'logger?c=-1&p=" << p << "'>"
                                                << NLog::PriorityToString(NLog::EPrio(p)) << "</a>";
                                        }
                                    }
                                }
                            }
                        }
                        TAG(TH4) {
                            str << "Change sampling priority for all components";
                        }
                        TABLE_CLASS("table table-condensed") {
                            TABLEHEAD() {
                                TABLER() {
                                    TABLEH() {
                                        str << "Priority";
                                    }
                                }
                            }
                            TABLEBODY() {
                                for (int p = NLog::PRI_EMERG; p <= NLog::PRI_TRACE; ++p) {
                                    TABLER() {
                                        TABLED() {
                                            str << "<a href = 'logger?c=-1&sp=" << p << "'>"
                                                << NLog::PriorityToString(NLog::EPrio(p)) << "</a>";
                                        }
                                    }
                                }
                            }
                        }
                        TAG(TH4) {
                            str << "Change sampling rate for all components";
                        }
                        str << "<form method=\"GET\">" << Endl;
                        str << "Rate: <input type=\"number\" name=\"sr\" value=\"0\"/>" << Endl;
                        str << "<input type=\"hidden\" name=\"c\" value=\"-1\">" << Endl;
                        str << "<input class=\"btn btn-primary\" type=\"submit\" value=\"Change\"/>" << Endl;
                        str << "</form>" << Endl;
                        TAG(TH4) {
                            str << "Drop log entries in case of overflow: "
                                << (Settings->AllowDrop ? "Enabled" : "Disabled");
                        }
                        str << "<form method=\"GET\">" << Endl;
                        str << "<input type=\"hidden\" name=\"allowdrop\" value=\"" << (Settings->AllowDrop ? "0" : "1") << "\"/>" << Endl;
                        str << "<input class=\"btn btn-primary\" type=\"submit\" value=\"" << (Settings->AllowDrop ? "Disable" : "Enable") << "\"/>" << Endl;
                        str << "</form>" << Endl;
                    }
                }
                Metrics->GetOutputHtml(str);
            }
        }

        ctx.Send(ev->Sender, new NMon::TEvHttpInfoRes(str.Str()));
    }

    constexpr size_t TimeBufSize = 512;

    bool TLoggerActor::OutputRecord(NLog::TEvLog *evLog) noexcept {
        return OutputRecord(evLog->Stamp, evLog->Level.ToPrio(), evLog->Component, evLog->Line);
    }

    bool TLoggerActor::OutputRecord(TInstant time, NLog::EPrio priority, NLog::EComponent component,
                                    const TString& formatted) noexcept try {
        const auto logPrio = ::ELogPriority(ui16(priority));

        char buf[TimeBufSize];
        switch (Settings->Format) {
            case NActors::NLog::TSettings::PLAIN_FULL_FORMAT: {
                TStringBuilder logRecord;
                if (Settings->UseLocalTimestamps) {
                    logRecord << FormatLocalTimestamp(time, buf);
                } else {
                    logRecord << time;
                }
                logRecord
                    << Settings->MessagePrefix
                    << " :" << Settings->ComponentName(component)
                    << " "  << PriorityToString(priority)
                    << ": " << formatted;
                LogBackend->WriteData(
                    TLogRecord(logPrio, logRecord.data(), logRecord.size()));
            } break;

            case NActors::NLog::TSettings::PLAIN_SHORT_FORMAT: {
                TStringBuilder logRecord;
                logRecord
                    << Settings->ComponentName(component)
                    << ": " << formatted;
                LogBackend->WriteData(
                    TLogRecord(logPrio, logRecord.data(), logRecord.size()));
            } break;

            case NActors::NLog::TSettings::JSON_FORMAT: {
                NJsonWriter::TBuf json;
                json.BeginObject()
                    .WriteKey("@timestamp")
                    .WriteString(Settings->UseLocalTimestamps ? FormatLocalTimestamp(time, buf) : time.ToString().data())
                    .WriteKey("microseconds")
                    .WriteULongLong(time.MicroSeconds())
                    .WriteKey("host")
                    .WriteString(Settings->ShortHostName)
                    .WriteKey("cluster")
                    .WriteString(Settings->ClusterName)
                    .WriteKey("priority")
                    .WriteString(PriorityToString(priority))
                    .WriteKey("npriority")
                    .WriteInt((int)priority)
                    .WriteKey("component")
                    .WriteString(Settings->ComponentName(component))
                    .WriteKey("tag")
                    .WriteString("KIKIMR")
                    .WriteKey("revision")
                    .WriteInt(GetProgramSvnRevision())
                    .WriteKey("message")
                    .WriteString(formatted)
                    .EndObject();
                auto logRecord = json.Str();
                LogBackend->WriteData(
                    TLogRecord(logPrio, logRecord.data(), logRecord.size()));
            } break;
        }

        return true;
    } catch (...) {
        return false;
    }

    void TLoggerActor::HandleLogEventDrop(const NLog::TEvLog::TPtr& ev) {
        WriteMessageStat(*ev->Get());
        Metrics->IncDroppedMsgs();
    }

    void TLoggerActor::HandleWakeup() {
        Become(&TThis::StateFunc);
    }

    const char* TLoggerActor::FormatLocalTimestamp(TInstant time, char* buf) {
        struct tm localTime;
        time.LocalTime(&localTime);
        int r = strftime(buf, TimeBufSize, "%Y-%m-%d-%H-%M-%S", &localTime);
        Y_ABORT_UNLESS(r != 0);
        return buf;
    }

    TAutoPtr<TLogBackend> CreateSysLogBackend(const TString& ident,
                                              bool logPError, bool logCons) {
        int flags = 0;
        if (logPError)
            flags |= TSysLogBackend::LogPerror;
        if (logCons)
            flags |= TSysLogBackend::LogCons;

        return new TSysLogBackend(ident.data(), TSysLogBackend::TSYSLOG_LOCAL1, flags);
    }

    class TStderrBackend: public TLogBackend {
    public:
        TStderrBackend() {
        }
        void WriteData(const TLogRecord& rec) override {
#ifdef _MSC_VER
            if (IsDebuggerPresent()) {
                TString x;
                x.reserve(rec.Len + 2);
                x.append(rec.Data, rec.Len);
                x.append('\n');
                OutputDebugString(x.c_str());
            }
#endif
            bool isOk = false;
            do {
                try {
                    TRecordWithNewline r(rec);
                    Cerr.Write(r.Buf.Data(), r.Buf.Filled());
                    isOk = true;
                } catch (TSystemError err) {
                    // Interrupted system call
                    Y_UNUSED(err);
                }
            } while (!isOk);
        }

        void ReopenLog() override {
        }

    private:
        const TString Indent;
    };

    class TLineFileLogBackend: public TFileLogBackend {
    public:
        TLineFileLogBackend(const TString& path)
            : TFileLogBackend(path)
        {
        }

        // Append newline after every record
        void WriteData(const TLogRecord& rec) override {
            TFileLogBackend::WriteData(TRecordWithNewline(rec));
        }
    };

    class TCompositeLogBackend: public TLogBackend {
    public:
        TCompositeLogBackend(TVector<TAutoPtr<TLogBackend>>&& underlyingBackends)
            : UnderlyingBackends(std::move(underlyingBackends))
        {
        }

        void WriteData(const TLogRecord& rec) override {
            for (auto& b: UnderlyingBackends) {
                b->WriteData(rec);
            }
        }

        void ReopenLog() override {
        }

    private:
        TVector<TAutoPtr<TLogBackend>> UnderlyingBackends;
    };

    TAutoPtr<TLogBackend> CreateStderrBackend() {
        return new TStderrBackend();
    }

    TAutoPtr<TLogBackend> CreateFileBackend(const TString& fileName) {
        return new TLineFileLogBackend(fileName);
    }

    TAutoPtr<TLogBackend> CreateNullBackend() {
        return new TNullLogBackend();
    }

    TAutoPtr<TLogBackend> CreateCompositeLogBackend(TVector<TAutoPtr<TLogBackend>>&& underlyingBackends) {
        return new TCompositeLogBackend(std::move(underlyingBackends));
    }

    class TLogContext: TNonCopyable {
    private:
        class TStackedContext {
        private:
            const TLogContextGuard* Guard;
            std::optional<NLog::EComponent> Component;
            mutable std::optional<TString> CurrentHeader;
        public:
            TStackedContext(const TLogContextGuard* guard, std::optional<NLog::EComponent>&& component)
                : Guard(guard)
                , Component(std::move(component))
            {

            }

            ui64 GetId() const {
                return Guard->GetId();
            }

            const std::optional<NLog::EComponent>& GetComponent() const {
                return Component;
            }

            TString GetCurrentHeader() const {
                if (!CurrentHeader) {
                    CurrentHeader = Guard->GetResult();
                }
                return *CurrentHeader;
            }
        };

        std::vector<TStackedContext> Stack;
    public:
        void Push(const TLogContextGuard& context) {
            std::optional<NLog::EComponent> component;
            if (Stack.empty() || context.GetComponent()) {
                component = context.GetComponent();
            } else {
                component = Stack.back().GetComponent();
            }
            Stack.emplace_back(&context, std::move(component));
        }

        void Pop(const TLogContextGuard& context) {
            Y_ABORT_UNLESS(Stack.size() && Stack.back().GetId() == context.GetId());
            Stack.pop_back();
        }

        std::optional<NLog::EComponent> GetCurrentComponent() const {
            if (Stack.empty()) {
                return {};
            }
            return Stack.back().GetComponent();
        }

        TString GetCurrentHeader() {
            TStringBuilder sb;
            for (auto&& i : Stack) {
                sb << i.GetCurrentHeader();
            }
            return sb;
        }
    };

    namespace {
        Y_POD_THREAD(ui64) GuardId;
        Y_THREAD(TLogContext) TlsLogContext;

    }

    TLogContextGuard::~TLogContextGuard() {
        TlsLogContext.Get().Pop(*this);
    }

    TLogContextGuard::TLogContextGuard(const TLogContextBuilder& builder)
        : TBase(builder.GetResult())
        , Component(builder.GetComponent())
        , Id(++GuardId)
    {
        TlsLogContext.Get().Push(*this);
    }

    int TLogContextGuard::GetCurrentComponent(const ::NActors::NLog::EComponent defComponent) {
        return TlsLogContext.Get().GetCurrentComponent().value_or(defComponent);
    }

    TLogRecordConstructor::TLogRecordConstructor() {
        TBase::WriteDirectly(TlsLogContext.Get().GetCurrentHeader());
    }

    TFormattedRecordWriter::TFormattedRecordWriter(::NActors::NLog::EPriority priority, ::NActors::NLog::EComponent component)
        : ActorContext(NActors::TlsActivationContext ? &NActors::TlsActivationContext->AsActorContext() : nullptr)
        , Priority(priority)
        , Component(component) {
        TBase::WriteDirectly(TlsLogContext.Get().GetCurrentHeader());
    }

    TFormattedRecordWriter::~TFormattedRecordWriter() {
        if (ActorContext) {
            ::NActors::MemLogAdapter(*ActorContext, Priority, Component, TBase::GetResult());
        } else {
            Cerr << "FALLBACK_ACTOR_LOGGING;priority=" << Priority << ";component=" << Component << ";" << TBase::GetResult() << Endl;
        }
    }

    TVerifyFormattedRecordWriter::TVerifyFormattedRecordWriter(const TString& conditionText)
        : ConditionText(conditionText) {
        TBase::WriteDirectly(TlsLogContext.Get().GetCurrentHeader());
        TBase::Write("verification", ConditionText);

    }

    TVerifyFormattedRecordWriter::~TVerifyFormattedRecordWriter() {
        const TString data = TBase::GetResult();
        Y_ABORT("%s", data.data());
    }

    TEnsureFormattedRecordWriter::TEnsureFormattedRecordWriter(const TString& conditionText)
        : ConditionText(conditionText) {
        TBase::WriteDirectly(TlsLogContext.Get().GetCurrentHeader());
        TBase::Write("verification", ConditionText);

    }

    TEnsureFormattedRecordWriter::~TEnsureFormattedRecordWriter() noexcept(false) {
        const TString data = TBase::GetResult();
        if (NActors::TlsActivationContext) {
            ::NActors::MemLogAdapter(NActors::TlsActivationContext->AsActorContext(), NLog::EPriority::PRI_ERROR, 0, data);
        } else {
            Cerr << "FALLBACK_EXCEPTION_LOGGING;component=EXCEPTION;" << data << Endl;
        }
        if (!std::uncaught_exceptions()) {
            Y_ENSURE(false, data.data());
        } else {
            Y_ABORT("%s", data.data());
        }
    }

}
