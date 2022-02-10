#include "log.h"
#include "log_settings.h"

#include <library/cpp/monlib/service/pages/templates.h>

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

    class TLoggerCounters : public ILoggerMetrics {
    public:
        TLoggerCounters(TIntrusivePtr<NMonitoring::TDynamicCounters> counters)
            : DynamicCounters(counters)
        {
            ActorMsgs_ = DynamicCounters->GetCounter("ActorMsgs", true);
            DirectMsgs_ = DynamicCounters->GetCounter("DirectMsgs", true);
            LevelRequests_ = DynamicCounters->GetCounter("LevelRequests", true);
            IgnoredMsgs_ = DynamicCounters->GetCounter("IgnoredMsgs", true);
            DroppedMsgs_ = DynamicCounters->GetCounter("DroppedMsgs", true);

            AlertMsgs_ = DynamicCounters->GetCounter("AlertMsgs", true);
            EmergMsgs_ = DynamicCounters->GetCounter("EmergMsgs", true);
        }

        ~TLoggerCounters() = default;

        void IncActorMsgs() override {
            ++*ActorMsgs_;
        }
        void IncDirectMsgs() override {
            ++*DirectMsgs_;
        }
        void IncLevelRequests() override {
            ++*LevelRequests_;
        }
        void IncIgnoredMsgs() override {
            ++*IgnoredMsgs_;
        }
        void IncAlertMsgs() override {
            ++*AlertMsgs_;
        }
        void IncEmergMsgs() override {
            ++*EmergMsgs_;
        }
        void IncDroppedMsgs() override {
            DroppedMsgs_->Inc();
        };

        void GetOutputHtml(IOutputStream& str) override {
            HTML(str) {
                DIV_CLASS("row") {
                    DIV_CLASS("col-md-12") {
                        H4() {
                            str << "Counters" << Endl;
                        }
                        DynamicCounters->OutputHtml(str);
                    }
                }
            }
        }

    private:
        NMonitoring::TDynamicCounters::TCounterPtr ActorMsgs_;
        NMonitoring::TDynamicCounters::TCounterPtr DirectMsgs_;
        NMonitoring::TDynamicCounters::TCounterPtr LevelRequests_;
        NMonitoring::TDynamicCounters::TCounterPtr IgnoredMsgs_;
        NMonitoring::TDynamicCounters::TCounterPtr AlertMsgs_;
        NMonitoring::TDynamicCounters::TCounterPtr EmergMsgs_;
        // Dropped while the logger backend was unavailable
        NMonitoring::TDynamicCounters::TCounterPtr DroppedMsgs_;

        TIntrusivePtr<NMonitoring::TDynamicCounters> DynamicCounters;
    };

    class TLoggerMetrics : public ILoggerMetrics {
    public:
        TLoggerMetrics(std::shared_ptr<NMonitoring::TMetricRegistry> metrics)
            : Metrics(metrics)
        {
            ActorMsgs_ = Metrics->Rate(NMonitoring::TLabels{{"sensor", "logger.actor_msgs"}});
            DirectMsgs_ = Metrics->Rate(NMonitoring::TLabels{{"sensor", "logger.direct_msgs"}});
            LevelRequests_ = Metrics->Rate(NMonitoring::TLabels{{"sensor", "logger.level_requests"}});
            IgnoredMsgs_ = Metrics->Rate(NMonitoring::TLabels{{"sensor", "logger.ignored_msgs"}});
            DroppedMsgs_ = Metrics->Rate(NMonitoring::TLabels{{"sensor", "logger.dropped_msgs"}});

            AlertMsgs_ = Metrics->Rate(NMonitoring::TLabels{{"sensor", "logger.alert_msgs"}});
            EmergMsgs_ = Metrics->Rate(NMonitoring::TLabels{{"sensor", "logger.emerg_msgs"}});
        }

        ~TLoggerMetrics() = default;

        void IncActorMsgs() override {
            ActorMsgs_->Inc();
        }
        void IncDirectMsgs() override {
            DirectMsgs_->Inc();
        }
        void IncLevelRequests() override {
            LevelRequests_->Inc();
        }
        void IncIgnoredMsgs() override {
            IgnoredMsgs_->Inc();
        }
        void IncAlertMsgs() override {
            AlertMsgs_->Inc();
        }
        void IncEmergMsgs() override {
            EmergMsgs_->Inc();
        }
        void IncDroppedMsgs() override {
            DroppedMsgs_->Inc();
        };

        void GetOutputHtml(IOutputStream& str) override {
            HTML(str) {
                DIV_CLASS("row") {
                    DIV_CLASS("col-md-12") {
                        H4() {
                            str << "Metrics" << Endl;
                        }
                        // TODO: Now, TMetricRegistry does not have the GetOutputHtml function
                    }
                }
            }
        }

    private:
        NMonitoring::TRate* ActorMsgs_;
        NMonitoring::TRate* DirectMsgs_;
        NMonitoring::TRate* LevelRequests_;
        NMonitoring::TRate* IgnoredMsgs_;
        NMonitoring::TRate* AlertMsgs_;
        NMonitoring::TRate* EmergMsgs_;
        // Dropped while the logger backend was unavailable
        NMonitoring::TRate* DroppedMsgs_;

        std::shared_ptr<NMonitoring::TMetricRegistry> Metrics;
    };

    TAtomic TLoggerActor::IsOverflow = 0;

    TLoggerActor::TLoggerActor(TIntrusivePtr<NLog::TSettings> settings,
                               TAutoPtr<TLogBackend> logBackend,
                               TIntrusivePtr<NMonitoring::TDynamicCounters> counters)
        : TActor(&TLoggerActor::StateFunc)
        , Settings(settings)
        , LogBackend(logBackend.Release())
        , Metrics(std::make_unique<TLoggerCounters>(counters))
    {
    }

    TLoggerActor::TLoggerActor(TIntrusivePtr<NLog::TSettings> settings,
                               std::shared_ptr<TLogBackend> logBackend,
                               TIntrusivePtr<NMonitoring::TDynamicCounters> counters)
        : TActor(&TLoggerActor::StateFunc)
        , Settings(settings)
        , LogBackend(logBackend)
        , Metrics(std::make_unique<TLoggerCounters>(counters))
    {
    }

    TLoggerActor::TLoggerActor(TIntrusivePtr<NLog::TSettings> settings,
                               TAutoPtr<TLogBackend> logBackend,
                               std::shared_ptr<NMonitoring::TMetricRegistry> metrics)
        : TActor(&TLoggerActor::StateFunc)
        , Settings(settings)
        , LogBackend(logBackend.Release())
        , Metrics(std::make_unique<TLoggerMetrics>(metrics))
    {
    }

    TLoggerActor::TLoggerActor(TIntrusivePtr<NLog::TSettings> settings,
                               std::shared_ptr<TLogBackend> logBackend,
                               std::shared_ptr<NMonitoring::TMetricRegistry> metrics)
        : TActor(&TLoggerActor::StateFunc)
        , Settings(settings)
        , LogBackend(logBackend)
        , Metrics(std::make_unique<TLoggerMetrics>(metrics))
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
        if (AtomicGet(IsOverflow))
            Sleep(settings.ThrottleDelay);
    }

    void TLoggerActor::LogIgnoredCount(TInstant now) {
        TString message = Sprintf("Ignored IgnoredCount# %" PRIu64 " log records due to logger overflow!", IgnoredCount);
        if (!OutputRecord(now, NActors::NLog::EPrio::Error, Settings->LoggerComponent, message)) {
            BecomeDefunct();
        }
    }

    void TLoggerActor::HandleIgnoredEvent(TLogIgnored::TPtr& ev, const NActors::TActorContext& ctx) {
        Y_UNUSED(ev);
        LogIgnoredCount(ctx.Now());
        IgnoredCount = 0;
        PassedCount = 0;
    }

    void TLoggerActor::HandleIgnoredEventDrop() {
        // logger backend is unavailable, just ignore
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
            // Disable throttling if it was enabled previously
            if (AtomicGet(IsOverflow))
                AtomicSet(IsOverflow, 0);

            // Check if some records have to be dropped
            if ((PassedCount > 10 && delayMillisec > (i64)Settings->TimeThresholdMs) || IgnoredCount > 0) {
                Metrics->IncIgnoredMsgs();
                if (IgnoredCount == 0) {
                    ctx.Send(ctx.SelfID, new TLogIgnored());
                }
                ++IgnoredCount;
                PassedCount = 0;
                return;
            }
            PassedCount++;
        } else {
            // Enable of disable throttling depending on the load
            if (delayMillisec > (i64)Settings->TimeThresholdMs && !AtomicGet(IsOverflow))
                AtomicSet(IsOverflow, 1);
            else if (delayMillisec <= (i64)Settings->TimeThresholdMs && AtomicGet(IsOverflow))
                AtomicSet(IsOverflow, 0);
        }

        const auto prio = ev->Get()->Level.ToPrio();
        if (!OutputRecord(ev->Get()->Stamp, prio, ev->Get()->Component, ev->Get()->Line)) {
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
            H4() {
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
                        H4() {
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
                        H4() {
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
                        H4() {
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
                        H4() {
                            str << "Change sampling rate" << Endl;
                        }
                        str << "<form method=\"GET\">" << Endl;
                        str << "Rate: <input type=\"number\" name=\"sr\" value=\"" << samplingRate << "\"/>" << Endl;
                        str << "<input type=\"hidden\" name=\"c\" value=\"" << component << "\">" << Endl;
                        str << "<input class=\"btn btn-primary\" type=\"submit\" value=\"Change\"/>" << Endl;
                        str << "</form>" << Endl;
                        H4() {
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
                    DIV_CLASS("col-md-6") {
                        RenderComponentPriorities(str);
                    }
                    DIV_CLASS("col-md-6") {
                        H4() {
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
                        H4() {
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
                        H4() {
                            str << "Change sampling rate for all components";
                        }
                        str << "<form method=\"GET\">" << Endl;
                        str << "Rate: <input type=\"number\" name=\"sr\" value=\"0\"/>" << Endl;
                        str << "<input type=\"hidden\" name=\"c\" value=\"-1\">" << Endl;
                        str << "<input class=\"btn btn-primary\" type=\"submit\" value=\"Change\"/>" << Endl;
                        str << "</form>" << Endl;
                        H4() {
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
        Y_VERIFY(r != 0);
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
}
