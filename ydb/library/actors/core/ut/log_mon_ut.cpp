#include <ydb/library/actors/core/log.h>
#include <ydb/library/actors/testlib/test_runtime.h>

#include <library/cpp/testing/unittest/registar.h>

using namespace NActors;
using namespace NActors::NLog;

namespace {
    const TString& ComponentName(int component) {
        static const TString names[] = {"LOGGER", "QUERY", "STORAGE", "TRANSPORT"};
        return names[component >= 4 ? component - 2 : component];
    }

    class TCaptureBackend: public TLogBackend {
    public:
        TVector<TString> Records;
        bool Fail = false;

        void WriteData(const TLogRecord& record) override {
            if (Fail) {
                ythrow yexception() << "backend unavailable";
            }
            Records.emplace_back(record.Data, record.Len);
        }

        void ReopenLog() override {}
    };

    class TMonitorRequest: public NMonitoring::TMonService2HttpRequest {
    public:
        explicit TMonitorRequest(const TString& query)
            : TMonService2HttpRequest(nullptr, nullptr, nullptr, nullptr, "/logger", nullptr)
            , Params(query)
        {}

        const TCgiParameters& GetParams() const override {
            return Params;
        }

    private:
        const TCgiParameters Params;
    };

    struct TFixture {
        TFixture() {
            Settings = MakeIntrusive<TSettings>(TActorId(), 0, PRI_INFO, PRI_DEBUG, 0u);
            Settings->Append(0, 1, ComponentName);
            Settings->Append(4, 5, ComponentName); // Real components separated by unnamed slots.
            Settings->SetAllowDrop(false);
            Settings->Format = TSettings::PLAIN_SHORT_FORMAT;
            Settings->LogSourceLocation = false;
            Runtime.Initialize();
            Logger = Runtime.Register(new TLoggerActor(Settings, Backend, MakeIntrusive<NMonitoring::TDynamicCounters>()));
            Settings->LoggerActorId = Logger;
            Edge = Runtime.AllocateEdgeActor();
            Runtime.SetScheduledEventFilter([](auto&&, auto&&, auto&&, auto) { return false; });
        }

        TString Request(const TString& query = {}) {
            // TEvHttpInfo borrows the request; keep it alive until the reply.
            TMonitorRequest request(query);
            Runtime.Send(new IEventHandle(Logger, Edge, new NMon::TEvHttpInfo(request)));
            auto response = Runtime.GrabEdgeEvent<NMon::TEvHttpInfoRes>(Edge, TDuration::Seconds(5));
            UNIT_ASSERT(response);
            UNIT_ASSERT(response->Get()->GetContentType() == NMon::IEvHttpInfoRes::Html);
            TStringStream html;
            response->Get()->Output(html);
            return html.Str();
        }

        TSettings* LoggerSettings() { return Settings.Get(); }
        void Send(TAutoPtr<IEventHandle> event) { Runtime.Send(event); }

        void CheckLog(EPriority priority, int component, bool accepted) {
            const auto count = Backend->Records.size();
            LOG_LOG_S(*this, priority, component, "monitor-test");
            UNIT_ASSERT_VALUES_EQUAL(Backend->Records.size(), count + accepted);
            if (accepted) {
                UNIT_ASSERT_VALUES_EQUAL(Backend->Records.back(),
                    TStringBuilder() << ComponentName(component) << ": monitor-test");
            }
        }

        void CheckSettings(int component, EPriority level, EPriority samplingLevel, ui32 rate, const TString& query = {}) {
            const auto settings = Settings->GetComponentSettings(component).Raw.X;
            UNIT_ASSERT_VALUES_EQUAL_C(settings.Level, level, query);
            UNIT_ASSERT_VALUES_EQUAL_C(settings.SamplingLevel, samplingLevel, query);
            UNIT_ASSERT_VALUES_EQUAL_C(settings.SamplingRate, rate, query);
        }

        TTestActorRuntimeBase Runtime;
        TIntrusivePtr<TSettings> Settings;
        std::shared_ptr<TCaptureBackend> Backend = std::make_shared<TCaptureBackend>();
        TActorId Logger;
        TActorId Edge;
    };
}

Y_UNIT_TEST_SUITE(TLoggerMonitoringTest) {
    Y_UNIT_TEST(OverviewListsRegisteredComponentsAndControls) {
        TFixture env;
        const auto html = env.Request();
        UNIT_ASSERT_STRING_CONTAINS(html, "Priority Settings for the Components");
        for (int component : {0, 1, 4, 5}) {
            UNIT_ASSERT_STRING_CONTAINS(html, TStringBuilder() << "logger?c=" << component << "'>" << ComponentName(component));
            env.CheckSettings(component, PRI_INFO, PRI_DEBUG, 0);
        }
        for (int hole : {2, 3}) {
            UNIT_ASSERT(!html.Contains(TStringBuilder() << "logger?c=" << hole << "'>"));
        }
        UNIT_ASSERT_STRING_CONTAINS(html, "logger?c=-1&p=0");
        UNIT_ASSERT_STRING_CONTAINS(html, "logger?c=-1&sp=8");
        UNIT_ASSERT_STRING_CONTAINS(html, "name=\"c\" value=\"-1\"");
        UNIT_ASSERT_STRING_CONTAINS(html, "Drop log entries in case of overflow: Disabled");
        UNIT_ASSERT_STRING_CONTAINS(html, "name=\"allowdrop\" value=\"1\"");
        UNIT_ASSERT_STRING_CONTAINS(html, "Counters");
    }

    Y_UNIT_TEST(ComponentPageDisplaysCurrentSettingsWithoutChangingThem) {
        TFixture env;
        env.Request("c=1&p=4&sp=8&sr=7");
        const auto html = env.Request("c=1");
        UNIT_ASSERT_STRING_CONTAINS(html, "Current log settings for QUERY");
        UNIT_ASSERT_STRING_CONTAINS(html, "Priority: WARN");
        UNIT_ASSERT_STRING_CONTAINS(html, "Sampling priority: TRACE");
        UNIT_ASSERT_STRING_CONTAINS(html, "Sampling rate: 7");
        UNIT_ASSERT_STRING_CONTAINS(html, "name=\"sr\" value=\"7\"");
        UNIT_ASSERT_STRING_CONTAINS(html, "name=\"c\" value=\"1\"");
        for (int priority = PRI_EMERG; priority <= PRI_TRACE; ++priority) {
            UNIT_ASSERT_STRING_CONTAINS(html, TStringBuilder() << "logger?c=1&p=" << priority);
            UNIT_ASSERT_STRING_CONTAINS(html, TStringBuilder() << "logger?c=1&sp=" << priority);
        }
        env.CheckSettings(1, PRI_WARN, PRI_TRACE, 7);
        env.CheckSettings(0, PRI_INFO, PRI_DEBUG, 0);
        env.CheckSettings(4, PRI_INFO, PRI_DEBUG, 0);
    }

    Y_UNIT_TEST(ComponentChangesAffectSubsequentLoggingOnlyForThatComponent) {
        TFixture env;
        env.CheckLog(PRI_DEBUG, 1, false);
        const auto changed = env.Request("c=1&p=7");
        UNIT_ASSERT_STRING_CONTAINS(changed, "Priority for the component QUERY has been changed from INFO to DEBUG");
        env.CheckLog(PRI_DEBUG, 1, true);
        env.CheckLog(PRI_DEBUG, 4, false);
        env.Request("c=1&p=4&sp=8&sr=1");
        env.CheckSettings(1, PRI_WARN, PRI_TRACE, 1);
        env.CheckLog(PRI_TRACE, 1, true); // Sampling at rate one is deterministic, including key zero.
        env.CheckLog(PRI_TRACE, 4, false);
        const auto disabled = env.Request("c=1&sr=0");
        UNIT_ASSERT_STRING_CONTAINS(disabled, "Sampling rate for the component QUERY has been changed from 1 to 0");
        env.CheckLog(PRI_WARN, 1, true);
        env.CheckLog(PRI_INFO, 1, false);
    }

    Y_UNIT_TEST(GlobalChangesApplyAcrossComponentRanges) {
        TFixture env;
        const auto html = env.Request("c=-1&p=4&sp=7&sr=1");
        UNIT_ASSERT_STRING_CONTAINS(html, "Sampling rate for all components has been changed to 1");
        for (int component : {0, 1, 4, 5}) {
            env.CheckSettings(component, PRI_WARN, PRI_DEBUG, 1);
            env.CheckLog(PRI_WARN, component, true);
            env.CheckLog(PRI_DEBUG, component, true);
            env.CheckLog(PRI_TRACE, component, false);
        }
        const auto disabled = env.Request("c=-1&sr=0");
        UNIT_ASSERT_STRING_CONTAINS(disabled, "Sampling rate for all components has been changed to 0");
        for (int component : {0, 1, 4, 5}) {
            env.CheckLog(PRI_WARN, component, true);
            env.CheckLog(PRI_DEBUG, component, false);
        }
    }

    Y_UNIT_TEST(InvalidParametersDoNotMutateSettings) {
        TFixture env;
        for (const TString& query : {
                "p=7&sp=8&sr=1", "c=text&p=7&sp=8&sr=1", "c=2&p=7&sp=8&sr=1",
                "c=-2&p=7", "c=6&p=7", "c=2147483648&p=7",
                "c=1&p=text", "c=1&p=-1", "c=1&p=9", "c=1&p=2147483648",
                "c=1&sp=text", "c=1&sp=-1", "c=1&sp=9", "c=1&sp=2147483648",
                "c=1&sr=text", "c=1&sr=-1", "c=1&sr=4294967296", "allowdrop=text"}) {
            const auto html = env.Request(query);
            UNIT_ASSERT_C(!html.Contains("has been changed"), query);
            for (int component : {0, 1, 4, 5}) {
                env.CheckSettings(component, PRI_INFO, PRI_DEBUG, 0, query);
            }
            UNIT_ASSERT_C(!env.Settings->AllowDrop, query);
        }
        env.CheckLog(PRI_INFO, 1, true);
        env.CheckLog(PRI_DEBUG, 1, false);
        // Invalid fields do not prevent an independently valid field from being applied.
        const auto html = env.Request("c=1&p=9&sp=8&sr=1");
        UNIT_ASSERT_STRING_CONTAINS(html, "Sampling rate for the component QUERY has been changed from 0 to 1");
        env.CheckSettings(1, PRI_INFO, PRI_TRACE, 1);
        env.CheckLog(PRI_TRACE, 1, true);
        env.CheckLog(PRI_TRACE, 4, false);
    }

    Y_UNIT_TEST(AllowDropCanBeToggledIndependentlyOfComponentSelection) {
        TFixture env;
        const auto enabled = env.Request("c=2&p=7&allowdrop=1");
        UNIT_ASSERT(env.Settings->AllowDrop);
        UNIT_ASSERT_STRING_CONTAINS(enabled, "Drop log entries in case of overflow: Enabled");
        UNIT_ASSERT_STRING_CONTAINS(enabled, "name=\"allowdrop\" value=\"0\"");
        env.CheckSettings(1, PRI_INFO, PRI_DEBUG, 0);
        const auto disabled = env.Request("allowdrop=0");
        UNIT_ASSERT(!env.Settings->AllowDrop);
        UNIT_ASSERT_STRING_CONTAINS(disabled, "Drop log entries in case of overflow: Disabled");
        UNIT_ASSERT_STRING_CONTAINS(disabled, "name=\"allowdrop\" value=\"1\"");
    }

    Y_UNIT_TEST(MonitoringStillRespondsWhileBackendIsUnavailable) {
        TFixture env;
        env.Backend->Fail = true;
        LOG_INFO_S(env, 1, "failed-write");
        UNIT_ASSERT(env.Backend->Records.empty());
        const auto html = env.Request("c=1&p=7");
        UNIT_ASSERT_STRING_CONTAINS(html, "Priority for the component QUERY has been changed from INFO to DEBUG");
        env.CheckSettings(1, PRI_DEBUG, PRI_DEBUG, 0);
        UNIT_ASSERT_STRING_CONTAINS(env.Request(), "Counters");
        env.Backend->Fail = false;
        env.Runtime.Send(new IEventHandle(env.Logger, {}, new TEvents::TEvWakeup));
        env.CheckLog(PRI_DEBUG, 1, true);
    }
}
