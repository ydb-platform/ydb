#include <library/cpp/json/json_reader.h>
#include <library/cpp/testing/unittest/registar.h>
#include <util/system/env.h>
#include <ydb/core/base/tablet_pipecache.h>
#include <ydb/core/mon/mon.h>
#include <ydb/core/persqueue/ut/common/pq_ut_common.h>
#include <ydb/core/persqueue/public/counters/percentile_counter.h>
#include <ydb/core/persqueue/pqtablet/common/constants.h>
#include <ydb/core/persqueue/pqtablet/partition/partition.h>
#include <ydb/core/quoter/public/quoter.h>
#include <ydb/core/sys_view/service/sysview_service.h>
#include <ydb/core/testlib/fake_scheme_shard.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>
#include <ydb/core/keyvalue/keyvalue_events.h>
#include <ydb/public/api/protos/draft/persqueue_error_codes.pb.h>
#include <ydb/core/protos/grpc_pq_old.pb.h>

#include <regex>

namespace NKikimr::NPQ {

namespace {

static constexpr const char* EMPTY_COUNTERS = "<pre></pre>";

TVector<std::pair<ui64, TString>> TestData() {
    TVector<std::pair<ui64, TString>> data;
    TString s{32, 'c'};
    // FIXME: replace magic numbers and add VERIFY on sizes
    const ui32 pp = 8 + 4 + 2 + 9;
    for (ui32 i = 0; i < 10; ++i) {
        data.push_back({i + 1, s.substr(pp)});
    }
    return data;
}

struct THttpRequest : NMonitoring::IHttpRequest {
    HTTP_METHOD Method;
    TCgiParameters CgiParameters;
    THttpHeaders HttpHeaders;

    THttpRequest(HTTP_METHOD method)
        : Method(method)
    {
        CgiParameters.emplace("type", TTabletTypes::TypeToStr(TTabletTypes::PersQueue));
        CgiParameters.emplace("json", "");
    }

    ~THttpRequest() {}

    const char* GetURI() const override {
        return "";
    }

    const char* GetPath() const override {
        return "";
    }

    const TCgiParameters& GetParams() const override {
        return CgiParameters;
    }

    const TCgiParameters& GetPostParams() const override {
        return CgiParameters;
    }

    TStringBuf GetPostContent() const override {
        return TStringBuf();
    }

    HTTP_METHOD GetMethod() const override {
        return Method;
    }

    const THttpHeaders& GetHeaders() const override {
        return HttpHeaders;
    }

    TString GetRemoteAddr() const override {
        return TString();
    }
};

} // anonymous namespace
Y_UNIT_TEST_SUITE(PQCountersSimple) {

Y_UNIT_TEST(Partition) {
    TTestContext tc;
    TFinalizer finalizer(tc);
    bool activeZone{false};
    tc.Prepare("", [](TTestActorRuntime&) {}, activeZone, false, true);
    tc.Runtime->SetScheduledLimit(100);

    PQTabletPrepare({}, {}, tc);
    CmdWrite(0, "sourceid0", TestData(), tc, false, {}, true);
    CmdWrite(0, "sourceid1", TestData(), tc, false);
    CmdWrite(0, "sourceid2", TestData(), tc, false);
    CmdWrite(0, "sourceid1", TestData(), tc, false);
    CmdWrite(0, "sourceid2", TestData(), tc, false);
    PQGetPartInfo(0, 30, tc);


    {
        auto counters = tc.Runtime->GetAppData(0).Counters;
        auto dbGroup = GetServiceCounters(counters, "pqproxy");
        TStringStream countersStr;
        dbGroup->OutputHtml(countersStr);
        TString referenceCounters = NResource::Find(TStringBuf("counters_pqproxy.html"));

        // FILE* ofs = fopen("~/counters_pqproxy.html.actual", "w");
        // if (ofs) {
        //     fwrite(countersStr.Str().data(), 1, countersStr.Str().size(), ofs);
        //     fputs("\n", ofs);
        //     fclose(ofs);
        // } else {
        //     Cerr << "Failed to open output file for writing: counters_pqproxy.html.actual" << Endl;
        // }

        Cerr << "ACTUAL:" << Endl << countersStr.Str() << Endl << "END" << Endl;

        UNIT_ASSERT_VALUES_EQUAL(countersStr.Str() + "\n", referenceCounters);
    }

    {
        auto counters = tc.Runtime->GetAppData(0).Counters;
        auto dbGroup = GetServiceCounters(counters, "datastreams");
        TStringStream countersStr;
        dbGroup->OutputHtml(countersStr);
        UNIT_ASSERT_VALUES_EQUAL(countersStr.Str(), EMPTY_COUNTERS);
    }
}

struct TPartitionLevelMetricsTestParameters {
    bool EnableMetricsLevel = false;
    bool FirstClassCitizen = false;
    std::optional<TString> MonitoringProjectId;
};

void PartitionLevelMetrics(TPartitionLevelMetricsTestParameters p) {
    const TString caseDescr = TStringBuilder()
                              << "EnableMetricsLevel=" << p.EnableMetricsLevel << ", "
                              << "FirstClassCitizen=" << p.FirstClassCitizen << ", "
                              << "MonitoringProjectId=" << p.MonitoringProjectId;
    Cerr << (TStringBuilder() << "Run PartitionLevelMetrics(" <<caseDescr << ")\n");
    TString referenceDir = p.FirstClassCitizen ? "first_class_citizen" : "federation";
    if (p.MonitoringProjectId.has_value() && !p.MonitoringProjectId->empty()) {
        referenceDir += "_with_monitoring_project_id";
    }
    TTestContext tc;
    TFinalizer finalizer(tc);
    bool activeZone{false};
    tc.Prepare("", [](TTestActorRuntime&) {}, activeZone, p.FirstClassCitizen, true);
    tc.Runtime->SetScheduledLimit(100);

    tc.Runtime->GetAppData(0).FeatureFlags.SetEnableMetricsLevel(p.EnableMetricsLevel);

    TTabletPreparationParameters parameters{
        .metricsLevel = METRICS_LEVEL_OBJECT,
        .monitoringProjectId = p.MonitoringProjectId,
    };

    PQTabletPrepare(parameters, {}, tc);
    CmdWrite(0, "sourceid0", TestData(), tc, false, {}, true);
    CmdWrite(0, "sourceid1", TestData(), tc, false);
    CmdWrite(0, "sourceid2", TestData(), tc, false);
    CmdWrite(1, "sourceid1", TestData(), tc, false);
    CmdWrite(1, "sourceid2", TestData(), tc, false);
    PQGetPartInfo(0, 30, tc);

    auto zeroUnreliableValues = [](std::string counters) {
        // Some counters end up with a different value each run.
        // To simplify testing we set such values to 0.
        auto names = TVector<std::string>{
            "WriteTimeLagMsByLastWrite",
            "WriteTimeLagMsByCommittedPerPartition",
            "TimeSinceLastReadMsPerPartition",
            "WriteTimeLagMsByLastReadPerPartition",
            "milliseconds",  // For FirstClassCitizen
        };
        for (const auto& name : names) {
            counters = std::regex_replace(counters, std::regex(name + ": \\d+"), name + ": 0");
        }
        return counters;
    };

    auto getCountersHtml = [&tc](const TString& group = "topics_per_partition", bool skipAddedLabels = true) {
        auto counters = tc.Runtime->GetAppData(0).Counters;
        auto dbGroup = GetServiceCounters(counters, group, skipAddedLabels);
        TStringStream countersStr;
        dbGroup->OutputHtml(countersStr);
        return countersStr.Str();
    };

    TString counters = getCountersHtml();
    Cerr << "XXXXX before write: " << counters << Endl;
    TString referenceCounters = NResource::Find(TStringBuilder() << referenceDir << "_turned_off.html");
    UNIT_ASSERT_C(counters + "\n" == referenceCounters || counters == EMPTY_COUNTERS, caseDescr);

    {
        // Turn on per partition counters, check counters.

        parameters.metricsLevel = METRICS_LEVEL_DETAILED;
        PQTabletPrepare(parameters, {}, tc);

        // partition, sourceId, data, text
        CmdWrite({ .Partition = 0, .SourceId = "sourceid3", .Data = TestData(), .TestContext = tc, .Error = false });
        CmdWrite({ .Partition = 0, .SourceId = "sourceid4", .Data = TestData(), .TestContext = tc, .Error = false });
        CmdWrite({ .Partition = 0, .SourceId = "sourceid5", .Data = TestData(), .TestContext = tc, .Error = false });
        CmdWrite({ .Partition = 0, .SourceId = "sourceid3", .Data = TestData(), .TestContext = tc, .Error = false });
        CmdWrite({ .Partition = 1, .SourceId = "sourceid4", .Data = TestData(), .TestContext = tc, .Error = false });
        CmdWrite({ .Partition = 1, .SourceId = "sourceid5", .Data = TestData(), .TestContext = tc, .Error = false });
        CmdWrite({ .Partition = 1, .SourceId = "sourceid4", .Data = TestData(), .TestContext = tc, .Error = false });

        std::string counters = getCountersHtml();
        Cerr << "after write: " << counters << "\n";
        Cerr << "after write zeroed: " << zeroUnreliableValues(counters) << "\n";
        TString referenceCounters = NResource::Find(TStringBuilder() << referenceDir << "_after_write.html");
        Cerr << "referenceCounters: " << referenceCounters << "\n";
        counters = zeroUnreliableValues(counters) + (p.EnableMetricsLevel ? "\n" : "");
        UNIT_ASSERT_VALUES_EQUAL_C(counters, p.EnableMetricsLevel ? referenceCounters : EMPTY_COUNTERS, caseDescr);
    }

    {
        // Read messages from different partitions.

        TString sessionId = "session1";
        TString user = "user1";
        TPQCmdReadSettings readSettings{
            /*session=*/ sessionId,
            /*partition=*/ 0,
            /*offset=*/ 0,
            /*count=*/ 2,
            /*size=*/ 16_MB,
            /*resCount=*/ 1,
        };
        readSettings.PartitionSessionId = 1;
        readSettings.User = user;

        {
            // Partition 0
            TPQCmdSettings sessionSettings{0, user, sessionId};
            sessionSettings.PartitionSessionId = 1;
            sessionSettings.KeepPipe = true;
            readSettings.Pipe = CmdCreateSession(sessionSettings, tc);
            BeginCmdRead(readSettings, tc);
            TAutoPtr<IEventHandle> handle;
            auto* result = tc.Runtime->GrabEdgeEvent<TEvPersQueue::TEvResponse>(handle);
            UNIT_ASSERT_C(result->Record.GetPartitionResponse().HasCmdReadResult(), LabeledOutput(caseDescr, result->Record.GetPartitionResponse().DebugString()));
        }

        {
            // Partition 1
            TPQCmdSettings sessionSettings{1, user, sessionId};
            sessionSettings.PartitionSessionId = 2;
            sessionSettings.KeepPipe = true;
            readSettings.Pipe = CmdCreateSession(sessionSettings, tc);
            readSettings.Partition = 1;
            readSettings.Count = 17;
            BeginCmdRead(readSettings, tc);
            TAutoPtr<IEventHandle> handle;
            auto* result = tc.Runtime->GrabEdgeEvent<TEvPersQueue::TEvResponse>(handle);
            UNIT_ASSERT_C(result->Record.GetPartitionResponse().HasCmdReadResult(), LabeledOutput(caseDescr, result->Record.GetPartitionResponse().DebugString()));
        }

        TString counters = getCountersHtml();
        TString referenceCounters = NResource::Find(TStringBuilder() << referenceDir << "_after_read.html");
        counters = zeroUnreliableValues(counters) + (p.EnableMetricsLevel ? "\n" : "");
        Cerr << "XXXXX after read: " << counters << "\n";
        UNIT_ASSERT_VALUES_EQUAL_C(counters, p.EnableMetricsLevel ? referenceCounters : EMPTY_COUNTERS, caseDescr);
    }

    {
        // Disable per partition counters, the counters should be empty.

        parameters.metricsLevel = METRICS_LEVEL_OBJECT;
        PQTabletPrepare(parameters, {}, tc);
        TString counters = getCountersHtml();
        TString referenceCounters = NResource::Find(TStringBuilder() << referenceDir << "_turned_off.html");
        counters = zeroUnreliableValues(counters) + (p.EnableMetricsLevel ? "\n" : "");
        Cerr << "XXXXX after read counters disabled: " << counters << "\n";
        UNIT_ASSERT_VALUES_EQUAL_C(counters, p.EnableMetricsLevel ? referenceCounters : EMPTY_COUNTERS, caseDescr);
    }
}

Y_UNIT_TEST(PartitionLevelMetrics) {
    for (bool enableMetricsLevel : {false, true}) {
        for (bool firstClassCitizen : {false, true}) {
            for (std::optional<TString> monitoringProjectId : TVector<std::optional<TString>>{std::nullopt, "", "first-monitoring-project-id"}) {
                PartitionLevelMetrics({
                    .EnableMetricsLevel = enableMetricsLevel,
                    .FirstClassCitizen = firstClassCitizen,
                    .MonitoringProjectId = monitoringProjectId,
                });
            }
        }
    }
}

class TFilterCounters: public NMonitoring::ICountableConsumer {
private:
    TString GetFromPath(const TConstArrayRef<TStringBuf> names) const {
        for (const auto& [n, v] : Path) {
            for (const auto& name : names) {
                if (n == name) {
                    return v;
                }
            }
        }
        return "";
    }

    void OnCounter(const TString& labelName, const TString& labelValue, const NMonitoring::TCounterForPtr* counter) override {
        Y_UNUSED(labelName);
        if (!FindPtr(SensorNames, labelValue)) {
            return;
        }
        if (GetFromPath({"consumer"}) != ConsumerName && GetFromPath({"ConsumerPath"}) != "shared/" + ConsumerName) {
            return;
        }

        NJson::TJsonValue& value = Result[GetFromPath({"monitoring_project_id"})][FromString<int>(GetFromPath({"partition_id", "Partition"}))];
        Y_ASSERT(!value.IsDefined());
        value = counter->Val();
    }

    void OnHistogram(const TString& labelName, const TString& labelValue, NMonitoring::IHistogramSnapshotPtr snapshot, bool derivative) override {
        Y_UNUSED(labelName, labelValue, snapshot, derivative);
    }

    void OnGroupBegin(const TString& labelName, const TString& labelValue, const NMonitoring::TDynamicCounters* group) override {
        Y_UNUSED(group);
        Path.emplace_back(labelName, labelValue);
    }

    void OnGroupEnd(const TString& labelName, const TString& labelValue, const NMonitoring::TDynamicCounters* group) override {
        Y_UNUSED(group);
        Y_ASSERT(Path.back().first == labelName);
        Y_ASSERT(Path.back().second == labelValue);
        Path.pop_back();
    }
public:

    NMonitoring::TCountableBase::EVisibility Visibility() const override {
        return NMonitoring::TCountableBase::EVisibility::Public;
    }

    explicit TFilterCounters(TConstArrayRef<TStringBuf> sensorNames)
        : SensorNames(sensorNames.begin(), sensorNames.end())
    {
    }

    const NJson::TJsonValue& GetResult() const {
        return Result;
    }

private:
    const TVector<TString> SensorNames;
    const TString ConsumerName = "user1";
    NJson::TJsonValue Result;
    TVector<std::pair<TString, TString>> Path;
};

struct TConsumerDetailedPartitionLevelMetricsTestParameters {
    bool EnableMetricsLevel = false;
    bool FirstClassCitizen = false;
    ui32 PartitionMetricsLevel = METRICS_LEVEL_OBJECT;
    std::optional<TString> PartitionMonitoringProjectId;
    std::optional<TString> ConsumersMonitoringProjectId;
};

void ConsumerDetailedMetrics(const TConsumerDetailedPartitionLevelMetricsTestParameters p) {
    const TString caseDescr = TStringBuilder()
                              << "EnableMetricsLevel=" << p.EnableMetricsLevel << ", "
                              << "FirstClassCitizen=" << p.FirstClassCitizen << ", "
                              << "PartitionMetricsLevel=" << p.PartitionMetricsLevel << ", "
                              << "PartitionMonitoringProjectId=" << p.PartitionMonitoringProjectId << ", "
                              << "ConsumersMonitoringProjectId=" << p.ConsumersMonitoringProjectId;
    Cerr << (TStringBuilder() << "Run PartitionLevelMetrics(" << caseDescr << ")\n");
    TTestContext tc;
    TFinalizer finalizer(tc);
    bool activeZone{false};
    tc.Prepare("", [](TTestActorRuntime&) {}, activeZone, p.FirstClassCitizen, true);
    tc.Runtime->SetScheduledLimit(100);

    tc.Runtime->GetAppData(0).FeatureFlags.SetEnableMetricsLevel(p.EnableMetricsLevel);

    TTabletPreparationParameters parameters{
        .metricsLevel = p.PartitionMetricsLevel,
        .monitoringProjectId = p.PartitionMonitoringProjectId,
    };

    TVector<TConsumerPreparationParameters> consumers{
        TConsumerPreparationParameters{
            .Name = "user1",
            .MetricsLevel = METRICS_LEVEL_OBJECT,
            .MonitoringProjectId = p.ConsumersMonitoringProjectId,
        },
    };
    PQTabletPrepare(parameters, consumers, *tc.Runtime, tc.TabletId, tc.Edge);

    CmdWrite(0, "sourceid0", TestData(), tc, false, {}, true);
    CmdWrite(0, "sourceid1", TestData(), tc, false);
    CmdWrite(0, "sourceid2", TestData(), tc, false);
    CmdWrite(1, "sourceid1", TestData(), tc, false);
    CmdWrite(1, "sourceid2", TestData(), tc, false);
    PQGetPartInfo(0, 30, tc);

    auto zeroUnreliableValues = [](std::string counters) {
        // Some counters end up with a different value each run.
        // To simplify testing we set such values to 0.
        auto names = TVector<std::string>{
            "WriteTimeLagMsByLastWrite",
            "WriteTimeLagMsByCommittedPerPartition",
            "TimeSinceLastReadMsPerPartition",
            "WriteTimeLagMsByLastReadPerPartition",
            "milliseconds", // For FirstClassCitizen
        };
        for (const auto& name : names) {
            counters = std::regex_replace(counters, std::regex(name + ": \\d+"), name + ": 000");
        }
        return counters;
    };

    const TVector<TStringBuf> MessageLagByCommittedPerPartition{{!p.FirstClassCitizen ? "MessageLagByCommittedPerPartition"sv : "topic.partition.committed_lag_messages"sv}};
    const TVector<TStringBuf> BytesRead{{!p.FirstClassCitizen ? "BytesReadPerPartition"sv : "topic.partition.read.bytes"sv}};
    const TVector<TStringBuf> MessagesRead{{!p.FirstClassCitizen ? "MessagesReadPerPartition"sv : "topic.partition.read.messages"sv}};

    auto getCountersObj = [&tc, &zeroUnreliableValues](const TStringBuf stage, const TString& group = "topics_per_partition", bool skipAddedLabels = false) {
        auto counters = tc.Runtime->GetAppData(0).Counters;
        auto dbGroup = GetServiceCounters(counters, group, skipAddedLabels);
        TString rep;
        TStringOutput countersStr{rep};
        dbGroup->OutputPlainText(countersStr);
        rep = zeroUnreliableValues(rep);
        Cerr << (TStringBuilder() << "XXXXX " << stage << ":\n"
                                  << rep << "\n");
        return dbGroup;
    };
    // collect sensor's value for all monitoring_project_id
    auto getSensorJsonStr = [](const auto& dbGroup, TConstArrayRef<TStringBuf> sensorNames) -> std::string {
        if (dbGroup == nullptr) {
            return "nullptr";
        }
        TFilterCounters fc{sensorNames};
        dbGroup->Accept("", "", fc);
        TString result = NJson::WriteJson(fc.GetResult(), false, true, true);
        Cerr << (TStringBuilder() << "Sensor: " << sensorNames.at(0) << " " << result << "\n");
        return result;
    };
    // where partition detailed metrics stored
    auto partitionTargetMonitoringProjectId = [&]() -> TMaybe<std::string> {
        if (!p.EnableMetricsLevel) {
            return Nothing();
        }
        if (p.PartitionMetricsLevel == METRICS_LEVEL_DETAILED) {
            return p.PartitionMonitoringProjectId.value_or("");
        }
        return Nothing();
    };
    // where consumer detailed metrics stored
    auto consumerTargetMonitoringProjectId = [&]() -> TMaybe<std::string> {
        if (!p.EnableMetricsLevel) {
            return Nothing();
        }
        if (!p.ConsumersMonitoringProjectId.value_or("").empty() && consumers.at(0).MetricsLevel == METRICS_LEVEL_DETAILED) {
            return p.ConsumersMonitoringProjectId.value();
        } else if (p.PartitionMetricsLevel == METRICS_LEVEL_DETAILED) {
            return Nothing();
        }
        return Nothing();
    };

    auto formExpectedJson = [&](const int part0Value, const int part1Value) -> std::string {
        NJson::TJsonArray valueVec{{part0Value, part1Value}};
        NJson::TJsonValue json;
        if (partitionTargetMonitoringProjectId().Defined()) {
            json[*partitionTargetMonitoringProjectId()] = valueVec;
        }
        if (consumerTargetMonitoringProjectId().Defined()) {
            json[*consumerTargetMonitoringProjectId()] = valueVec;
        }
        return NJson::WriteJson(json, false, true, true);
    };

    {
        const auto counters = getCountersObj("before enable");
        UNIT_ASSERT_VALUES_EQUAL_C(getSensorJsonStr(counters, MessageLagByCommittedPerPartition), formExpectedJson(0, 0), caseDescr);
        UNIT_ASSERT_VALUES_EQUAL_C(getSensorJsonStr(counters, BytesRead), formExpectedJson(0, 0), caseDescr);
        UNIT_ASSERT_VALUES_EQUAL_C(getSensorJsonStr(counters, MessagesRead), formExpectedJson(0, 0), caseDescr);
    }

    {
        // Turn on per partition counters for consumer, check counters.
        consumers.at(0).MetricsLevel = METRICS_LEVEL_DETAILED;
        PQTabletPrepare(parameters, consumers, *tc.Runtime, tc.TabletId, tc.Edge);

        const auto counters = getCountersObj("after enable");
        UNIT_ASSERT_VALUES_EQUAL_C(getSensorJsonStr(counters, MessageLagByCommittedPerPartition), formExpectedJson(30, 20), caseDescr);
        UNIT_ASSERT_VALUES_EQUAL_C(getSensorJsonStr(counters, BytesRead), formExpectedJson(0, 0), caseDescr);
        UNIT_ASSERT_VALUES_EQUAL_C(getSensorJsonStr(counters, MessagesRead), formExpectedJson(0, 0), caseDescr);
    }

    {
        // Read messages from different partitions.

        TString sessionId = "session1";
        TString user = "user1";
        TPQCmdReadSettings readSettings{
            /*session=*/sessionId,
            /*partition=*/0,
            /*offset=*/0,
            /*count=*/2,
            /*size=*/16_MB,
            /*resCount=*/1,
        };
        readSettings.PartitionSessionId = 1;
        readSettings.User = user;

        {
            // Partition 0
            TPQCmdSettings sessionSettings{0, user, sessionId};
            sessionSettings.PartitionSessionId = 1;
            sessionSettings.KeepPipe = true;
            readSettings.Pipe = CmdCreateSession(sessionSettings, tc);
            BeginCmdRead(readSettings, tc);
            TAutoPtr<IEventHandle> handle;
            auto* result = tc.Runtime->GrabEdgeEvent<TEvPersQueue::TEvResponse>(handle);
            UNIT_ASSERT_C(result->Record.GetPartitionResponse().HasCmdReadResult(), LabeledOutput(caseDescr, result->Record.GetPartitionResponse().DebugString()));
        }

        {
            // Partition 1
            TPQCmdSettings sessionSettings{1, user, sessionId};
            sessionSettings.PartitionSessionId = 2;
            sessionSettings.KeepPipe = true;
            readSettings.Pipe = CmdCreateSession(sessionSettings, tc);
            readSettings.Partition = 1;
            readSettings.Count = 17;
            BeginCmdRead(readSettings, tc);
            TAutoPtr<IEventHandle> handle;
            auto* result = tc.Runtime->GrabEdgeEvent<TEvPersQueue::TEvResponse>(handle);
            UNIT_ASSERT_C(result->Record.GetPartitionResponse().HasCmdReadResult(), LabeledOutput(caseDescr, result->Record.GetPartitionResponse().DebugString()));
        }

        const auto counters = getCountersObj("after read");
        UNIT_ASSERT_VALUES_EQUAL_C(getSensorJsonStr(counters, MessageLagByCommittedPerPartition), formExpectedJson(30, 20), caseDescr);
        UNIT_ASSERT_VALUES_EQUAL_C(getSensorJsonStr(counters, BytesRead), formExpectedJson(106, 693), caseDescr);
        UNIT_ASSERT_VALUES_EQUAL_C(getSensorJsonStr(counters, MessagesRead), formExpectedJson(2, 17), caseDescr);
    }

    {
        // Disable per partition counters on consumer, the counters should be empty if partition's metrics disabled
        consumers.at(0).MetricsLevel = METRICS_LEVEL_OBJECT;
        PQTabletPrepare(parameters, consumers, *tc.Runtime, tc.TabletId, tc.Edge);

        const auto counters = getCountersObj("after disable");
        UNIT_ASSERT_VALUES_EQUAL_C(getSensorJsonStr(counters, MessageLagByCommittedPerPartition), formExpectedJson(30, 20), caseDescr);
        UNIT_ASSERT_VALUES_EQUAL_C(getSensorJsonStr(counters, BytesRead), formExpectedJson(106, 693), caseDescr);
        UNIT_ASSERT_VALUES_EQUAL_C(getSensorJsonStr(counters, MessagesRead), formExpectedJson(2, 17), caseDescr);
    }
}

    struct TConsumerDetailedMetricsTestRegistration {
        TConsumerDetailedMetricsTestRegistration() {
            for (bool enableMetricsLevel : {false, true}) { // global settings
                for (bool firstClassCitizen : {false, true}) {
                    for (ui32 partitionMetricsLevel : {METRICS_LEVEL_OBJECT, METRICS_LEVEL_DETAILED}) {
                        for (std::optional<TString> partitionMonitoringProjectId : TVector<std::optional<TString>>{std::nullopt, "", "foo", "bar"}) {
                            for (std::optional<TString> consumerMonitoringProjectId : TVector<std::optional<TString>>{std::nullopt, "", "foo"}) {
                                if (partitionMonitoringProjectId == "foo" && consumerMonitoringProjectId != "foo") {
                                    continue; // duplicated case
                                }
                                auto nameMPI = [](const std::optional<TString>& v) -> std::string {
                                    return !v.has_value()
                                               ? "noset"
                                           : v->empty()
                                               ? "empty"
                                               : ToString(TStringBuf{*v}.Before('-'));
                                };
                                auto nameLevel = [](const ui32 level) -> std::string {
                                    return level == METRICS_LEVEL_DETAILED
                                               ? "detailed"
                                           : level == METRICS_LEVEL_OBJECT
                                               ? "object"
                                               : ToString(level);
                                };
                                TString testName = std::format("ConsumerDetailedMetrics__{}__{}__partitionMetricsLevel={}__partitionMPI={}__consumerMPI={}",
                                                               enableMetricsLevel ? "Enabled" : "Disabled",
                                                               firstClassCitizen ? "firstClassCitizen" : "federation",
                                                               nameLevel(partitionMetricsLevel),
                                                               nameMPI(partitionMonitoringProjectId),
                                                               nameMPI(consumerMonitoringProjectId));
                                Names.push_back(testName);
                                TCurrentTest::AddTest(Names.back().c_str(), [=](NUnitTest::TTestContext&) {
                                    ConsumerDetailedMetrics(TConsumerDetailedPartitionLevelMetricsTestParameters{
                                        .EnableMetricsLevel = enableMetricsLevel,
                                        .FirstClassCitizen = firstClassCitizen,
                                        .PartitionMetricsLevel = partitionMetricsLevel,
                                        .PartitionMonitoringProjectId = partitionMonitoringProjectId,
                                        .ConsumersMonitoringProjectId = consumerMonitoringProjectId,
                                    });
                                }, false);
                            }
                        }
                    }
                }
            }
        }

        TDeque<TString> Names;
    };
    static const TConsumerDetailedMetricsTestRegistration TestRegistration;

    // Test that changing monitoring project ID updates counters for both consumers and write operations
    Y_UNIT_TEST(MonitoringProjectIdChange) {
        for (bool firstClassCitizen : {false, true}) {
            Cerr << "Run MonitoringProjectIdChange(FirstClassCitizen=" << firstClassCitizen << ")\n";

            TTestContext tc;
            TFinalizer finalizer(tc);
            bool activeZone{false};
            tc.Prepare("", [](TTestActorRuntime&) {}, activeZone, firstClassCitizen, true);
            tc.Runtime->SetScheduledLimit(100);

            tc.Runtime->GetAppData(0).FeatureFlags.SetEnableMetricsLevel(true);

            const TString firstProjectId = "first-monitoring-project-id";
            const TString secondProjectId = "second-monitoring-project-id";
            const TString consumer = "test-consumer";

            // Initial setup with first monitoring project ID and a consumer
            TTabletPreparationParameters parameters{
                .metricsLevel = METRICS_LEVEL_DETAILED,
                .monitoringProjectId = firstProjectId,
            };

            PQTabletPrepare(parameters, {{consumer, false}}, tc);

            // Write some data to populate write counters
            CmdWrite({.Partition = 0, .SourceId = "sourceid0", .Data = TestData(), .TestContext = tc, .Error = false, .IsFirst = true});
            CmdWrite({.Partition = 0, .SourceId = "sourceid1", .Data = TestData(), .TestContext = tc, .Error = false});

            // Helper to read messages and populate consumer counters
            auto doRead = [&tc, &consumer](const TString& sessionId, ui64 partitionSessionId) {
                TPQCmdSettings sessionSettings{0, consumer, sessionId};
                sessionSettings.PartitionSessionId = partitionSessionId;
                TPQCmdReadSettings readSettings{
                    /*session=*/sessionId,
                    /*partition=*/0,
                    /*offset=*/0,
                    /*count=*/2,
                    /*size=*/16_MB,
                    /*resCount=*/1,
                };
                readSettings.PartitionSessionId = partitionSessionId;
                readSettings.User = consumer;
                readSettings.Pipe = CmdCreateSession(sessionSettings, tc);
                BeginCmdRead(readSettings, tc);
                TAutoPtr<IEventHandle> handle;
                auto* result = tc.Runtime->GrabEdgeEvent<TEvPersQueue::TEvResponse>(handle);
                UNIT_ASSERT_C(result->Record.GetPartitionResponse().HasCmdReadResult(), result->Record.GetPartitionResponse().DebugString());
                CmdKillSession(0, consumer, sessionId, tc);
            };

            // Read messages to populate consumer counters
            doRead("session", 1);

            // Verify counters exist under first monitoring project ID
            auto getCountersGroup = [&tc, firstClassCitizen](const TString& monitoringProjectId) -> ::NMonitoring::TDynamicCounterPtr {
                auto counters = tc.Runtime->GetAppData(0).Counters;
                auto group = counters->GetSubgroup("counters", "topics_per_partition");
                group = group->GetSubgroup("host", firstClassCitizen ? "" : "cluster");
                if (!monitoringProjectId.empty()) {
                    group = group->GetSubgroup("monitoring_project_id", monitoringProjectId);
                }
                return group;
            };

            {
                auto group = getCountersGroup(firstProjectId);
                TStringStream countersStr;
                group->OutputHtml(countersStr);
                Cerr << "Counters under first project ID:\n"
                     << countersStr.Str() << "\n";
                // Verify counters exist by checking the group is not empty
                UNIT_ASSERT_C(!countersStr.Str().empty() && countersStr.Str() != "<pre></pre>",
                              "Expected counters under first monitoring project ID");
            }

            // Change monitoring project ID
            parameters.monitoringProjectId = secondProjectId;
            PQTabletPrepare(parameters, {{consumer, false}}, tc);

            // Write more data to populate new counters under the new project ID
            CmdWrite({.Partition = 0, .SourceId = "sourceid2", .Data = TestData(), .TestContext = tc, .Error = false});
            CmdWrite({.Partition = 0, .SourceId = "sourceid3", .Data = TestData(), .TestContext = tc, .Error = false});

            // Read again to populate consumer counters under new project ID
            doRead("session", 2);

            // Verify counters now exist under second monitoring project ID
            {
                auto group = getCountersGroup(secondProjectId);
                TStringStream countersStr;
                group->OutputHtml(countersStr);
                Cerr << "Counters under second project ID:\n"
                     << countersStr.Str() << "\n";
                UNIT_ASSERT_C(!countersStr.Str().empty() && countersStr.Str() != "<pre></pre>",
                              "Expected counters under second monitoring project ID");
            }
        }
    }

    Y_UNIT_TEST(PartitionWriteQuota) {
        TTestContext tc;

        TFinalizer finalizer(tc);
        bool activeZone{false};
        tc.Prepare("", [](TTestActorRuntime&) {}, activeZone, false, true);
        tc.Runtime->SetScheduledLimit(100);
        tc.Runtime->GetAppData(0).PQConfig.MutableQuotingConfig()->SetEnableQuoting(true);

        PQTabletPrepare({.partitions = 1, .writeSpeed = 30_KB}, {}, tc);
        TVector<std::pair<ui64, TString>> data;
        TString s{32_KB, 'c'};
        data.push_back({1, s});
        tc.Runtime->SetObserverFunc(
            [&](TAutoPtr<IEventHandle>& ev) {
                if (ev->CastAsLocal<TEvQuota::TEvRequest>()) {
                    Cerr << "Captured kesus quota request event from " << ev->Sender.ToString() << Endl;
                    tc.Runtime->Send(new IEventHandle(
                        ev->Sender, TActorId{},
                        new TEvQuota::TEvClearance(TEvQuota::TEvClearance::EResult::Success), 0, ev->Cookie));
                    return TTestActorRuntimeBase::EEventAction::DROP;
                }
                return TTestActorRuntimeBase::EEventAction::PROCESS;
            });
        for (auto i = 0u; i < 6; i++) {
            CmdWrite(0, "sourceid0", data, tc);
            data[0].first++;
        }

        {
            auto counters = tc.Runtime->GetAppData(0).Counters;
            Y_ABORT_UNLESS(counters);
            auto dbGroup = GetServiceCounters(counters, "pqproxy");

            auto quotaWait = dbGroup->FindSubgroup("subsystem", "partitionWriteQuotaWait")
                                 ->FindSubgroup("Account", "total")
                                 ->FindSubgroup("Producer", "total")
                                 ->FindSubgroup("Topic", "total")
                                 ->FindSubgroup("TopicPath", "total")
                                 ->FindSubgroup("OriginDC", "cluster");
            auto histogram = quotaWait->FindSubgroup("sensor", "PartitionWriteQuotaWaitOriginal");
            TStringStream histogramStr;
            histogram->OutputHtml(histogramStr);
            Cerr << "**** Total histogram: **** \n " << histogramStr.Str() << "**** **** **** ****" << Endl;
            auto instant = histogram->FindNamedCounter("Interval", "0ms")->Val();
            auto oneSec = histogram->FindNamedCounter("Interval", "1000ms")->Val();
            auto twoSec = histogram->FindNamedCounter("Interval", "2500ms")->Val();
            UNIT_ASSERT_VALUES_EQUAL(oneSec + twoSec, 5);
            UNIT_ASSERT(twoSec >= 2);
            UNIT_ASSERT(oneSec >= 1);
            UNIT_ASSERT(instant >= 1);
        }
    }

    Y_UNIT_TEST(PartitionFirstClass) {
        TTestContext tc;
        TFinalizer finalizer(tc);
        bool activeZone{false};
        tc.Prepare("", [](TTestActorRuntime&) {}, activeZone, true, true);
        tc.Runtime->SetScheduledLimit(100);

        PQTabletPrepare({}, {}, tc);
        CmdWrite(0, "sourceid0", TestData(), tc, false, {}, true);
        CmdWrite(0, "sourceid1", TestData(), tc, false);
        CmdWrite(0, "sourceid2", TestData(), tc, false);
        CmdWrite(0, "sourceid0", TestData(), tc, false);
        PQGetPartInfo(0, 30, tc);

        {
            auto counters = tc.Runtime->GetAppData(0).Counters;
            auto dbGroup = GetServiceCounters(counters, "pqproxy");
            TStringStream countersStr;
            dbGroup->OutputHtml(countersStr);
            TString referenceCounters = NResource::Find(TStringBuf("counters_pqproxy_firstclass.html"));

            Cerr << "COUNTERS: " << countersStr.Str() << Endl;

            UNIT_ASSERT_VALUES_EQUAL(countersStr.Str() + "\n", referenceCounters);
        }

        {
            auto counters = tc.Runtime->GetAppData(0).Counters;
            auto dbGroup = GetServiceCounters(counters, "datastreams");

            TStringStream countersStr;
            dbGroup->OutputHtml(countersStr);

            const TString referenceCounters = NResource::Find(TStringBuf("counters_datastreams.html"));
            UNIT_ASSERT_VALUES_EQUAL(countersStr.Str() + "\n", referenceCounters);
        }
    }

    Y_UNIT_TEST(SupportivePartitionCountersPersist) {
        TTestContext tc;

        TFinalizer finalizer(tc);
        bool activeZone{false};
        tc.Prepare("", [](TTestActorRuntime&) {}, activeZone, false, true);
        tc.Runtime->SetScheduledLimit(100);
        tc.Runtime->GetAppData(0).PQConfig.MutableQuotingConfig()->SetEnableQuoting(true);

        PQTabletPrepare({.partitions = 1, .writeSpeed = 30_KB}, {}, tc);
        TVector<std::pair<ui64, TString>> data;
        TString s{32_KB, 'c'};
        data.push_back({1, s});
        tc.Runtime->SetObserverFunc(
            [&](TAutoPtr<IEventHandle>& ev) {
                if (ev->CastAsLocal<TEvQuota::TEvRequest>()) {
                    Cerr << "Captured kesus quota request event from " << ev->Sender.ToString() << Endl;
                    tc.Runtime->Send(new IEventHandle(
                        ev->Sender, TActorId{},
                        new TEvQuota::TEvClearance(TEvQuota::TEvClearance::EResult::Success), 0, ev->Cookie));
                    return TTestActorRuntimeBase::EEventAction::DROP;
                } else if (auto* msg = ev->CastAsLocal<TEvKeyValue::TEvRequest>()) {
                    Cerr << "Captured TEvRequest, cmd write size: " << msg->Record.CmdWriteSize() << Endl;
                    for (auto& w : msg->Record.GetCmdWrite()) {
                        if (w.GetKey().StartsWith("J")) {
                            NKikimrPQ::TPartitionMeta meta;
                            bool res = meta.ParseFromString(w.GetValue());
                            UNIT_ASSERT(res);
                            UNIT_ASSERT(meta.HasCounterData());
                            Cerr << "Write meta: " << meta.GetCounterData().ShortDebugString() << Endl;
                        }
                    }
                    return TTestActorRuntimeBase::EEventAction::PROCESS;
                }
                return TTestActorRuntimeBase::EEventAction::PROCESS;
            });
        for (auto i = 0u; i < 6; i++) {
            CmdWrite(0, "sourceid0", data, tc);
            data[0].first++;
        }
        PQGetPartInfo(0, 6, tc);
    }
} // Y_UNIT_TEST_SUITE(PQCountersSimple)

Y_UNIT_TEST_SUITE(PQCountersLabeled) {
    void CompareJsons(const TString& inputStr, const TString& referenceStr) {
        NJson::TJsonValue referenceJson;
        UNIT_ASSERT(NJson::ReadJsonTree(TStringBuf(referenceStr), &referenceJson));

        NJson::TJsonValue inputJson;
        UNIT_ASSERT(NJson::ReadJsonTree(TStringBuf(inputStr), &inputJson));

        Cerr << "Expected: " << referenceStr << Endl;
        Cerr << "Result: " << inputStr << Endl;

        // Run time of test differs as well as counters below.
        // We  set it to 5000 and then compare with reference string.
        auto getByPath = [](const NJson::TJsonValue& msg, TStringBuf path) {
            NJson::TJsonValue ret;
            UNIT_ASSERT_C(msg.GetValueByPath(path, ret), path);
            return ret.GetStringSafe();
        };

        for (auto& sensor : inputJson["sensors"].GetArraySafe()) {
            if (getByPath(sensor, "kind") == "GAUGE" &&
                (getByPath(sensor, "labels.sensor") == "PQ/TimeSinceLastReadMs" ||
                 getByPath(sensor, "labels.sensor") == "PQ/PartitionLifeTimeMs" ||
                 getByPath(sensor, "labels.sensor") == "PQ/TotalTimeLagMsByLastRead" ||
                 getByPath(sensor, "labels.sensor") == "PQ/WriteTimeLagMsByLastReadOld")) {
                sensor.SetValueByPath("value", 5000);
            } else if (getByPath(sensor, "kind") == "GAUGE" &&
                       (getByPath(sensor, "labels.sensor") == "PQ/WriteTimeLagMsByLastRead" ||
                        getByPath(sensor, "labels.sensor") == "PQ/WriteTimeLagMsByLastWrite")) {
                sensor.SetValueByPath("value", 30);
            }
        }

        Cerr << "Test diff count : " << inputJson["sensors"].GetArraySafe().size()
             << " " << referenceJson["sensors"].GetArraySafe().size() << Endl;

        ui64 inCount = inputJson["sensors"].GetArraySafe().size();
        ui64 refCount = referenceJson["sensors"].GetArraySafe().size();
        for (ui64 i = 0; i < inCount && i < refCount; ++i) {
            auto& in = inputJson["sensors"].GetArraySafe()[i];
            auto& ref = referenceJson["sensors"].GetArraySafe()[i];
            UNIT_ASSERT_VALUES_EQUAL_C(in["labels"], ref["labels"], TStringBuilder() << " at pos #" << i);
        }
        if (inCount > refCount) {
            UNIT_ASSERT_C(false, inputJson["sensors"].GetArraySafe()[refCount].GetStringRobust());
        } else if (refCount > inCount) {
            UNIT_ASSERT_C(false, referenceJson["sensors"].GetArraySafe()[inCount].GetStringRobust());
        }

        // UNIT_ASSERT_VALUES_EQUAL(referenceJson, inputJson);
    }

    Y_UNIT_TEST(Partition) {
        SetEnv("FAST_UT", "1");
        TTestContext tc;
        RunTestWithReboots(tc.TabletIds, [&]() { return tc.InitialEventsFilter.Prepare(); }, [&](const TString& dispatchName, std::function<void(TTestActorRuntime&)> setup, bool& activeZone) {
            TFinalizer finalizer(tc);
            tc.Prepare(dispatchName, setup, activeZone, false, true, true);
            tc.Runtime->SetScheduledLimit(1000);

            PQTabletPrepare({}, {}, tc);

            IActor* actor = CreateTabletCountersAggregator(false);
            auto aggregatorId = tc.Runtime->Register(actor);
            tc.Runtime->EnableScheduleForActor(aggregatorId);

            CmdWrite(0, "sourceid0", TestData(), tc, false, {}, true);
            CmdWrite(0, "sourceid1", TestData(), tc, false);
            CmdWrite(0, "sourceid2", TestData(), tc, false);
            PQGetPartInfo(0, 30, tc);

            {
                TDispatchOptions options;
                options.FinalEvents.emplace_back(TEvTabletCounters::EvTabletAddLabeledCounters);
                auto processedCountersEvent = tc.Runtime->DispatchEvents(options);
                UNIT_ASSERT_VALUES_EQUAL(processedCountersEvent, true);
            }

            IActor* actorX = CreateClusterLabeledCountersAggregatorActor(tc.Edge, TTabletTypes::PersQueue);
            tc.Runtime->Register(actorX);

            TAutoPtr<IEventHandle> handle;
            TEvTabletCounters::TEvTabletLabeledCountersResponse* result;
            result = tc.Runtime->GrabEdgeEvent<TEvTabletCounters::TEvTabletLabeledCountersResponse>(handle);
            UNIT_ASSERT(result);

            THttpRequest httpReq(HTTP_METHOD_GET);
            NMonitoring::TMonService2HttpRequest monReq(nullptr, &httpReq, nullptr, nullptr, "", nullptr);
            tc.Runtime->Send(new IEventHandle(aggregatorId, tc.Edge, new NMon::TEvHttpInfo(monReq)));

            TAutoPtr<IEventHandle> handle1;
            auto resp = tc.Runtime->GrabEdgeEvent<NMon::TEvHttpInfoRes>(handle1);
            const TString countersStr = ((NMon::TEvHttpInfoRes*)resp)->Answer.substr(sizeof("HTTP/1.1 200 Ok Content-Type: application/json Connection: Close "));
            const TString referenceStr = NResource::Find(TStringBuf("counters_labeled.json"));
            CompareJsons(countersStr, referenceStr);
        });
    }

    Y_UNIT_TEST(PartitionFirstClass) {
        SetEnv("FAST_UT", "1");
        TTestContext tc;
        RunTestWithReboots(tc.TabletIds, [&]() { return tc.InitialEventsFilter.Prepare(); }, [&](const TString& dispatchName, std::function<void(TTestActorRuntime&)> setup, bool& activeZone) {
        TFinalizer finalizer(tc);
        activeZone = false;
        bool dbRegistered{false};
        bool labeledCountersReceived =false ;

        tc.Prepare(dispatchName, setup, activeZone, true, true, true);
        tc.Runtime->SetScheduledLimit(10000);

        tc.Runtime->SetObserverFunc([&](TAutoPtr<IEventHandle>& event) {
            if (event->GetTypeRewrite() == NSysView::TEvSysView::EvRegisterDbCounters) {
                auto database = event.Get()->Get<NSysView::TEvSysView::TEvRegisterDbCounters>()->Database;
                UNIT_ASSERT_VALUES_EQUAL(database, "/Root/PQ");
                dbRegistered = true;
            } else if (event->GetTypeRewrite() == TEvTabletCounters::EvTabletAddLabeledCounters) {
                labeledCountersReceived = true;
            }
            return TTestActorRuntime::DefaultObserverFunc(event);
        });
        PQTabletPrepare({.deleteTime=3600, .writeSpeed = 100_KB, .meteringMode = NKikimrPQ::TPQTabletConfig::METERING_MODE_REQUEST_UNITS}, {{"client", true}}, tc);
        TFakeSchemeShardState::TPtr state{new TFakeSchemeShardState()};
        ui64 ssId = 325;
        BootFakeSchemeShard(*tc.Runtime, ssId, state);

        PQBalancerPrepare("topic", {{0, {tc.TabletId, 1}}}, ssId, tc);

        IActor* actor = CreateTabletCountersAggregator(false);
        auto aggregatorId = tc.Runtime->Register(actor);
        tc.Runtime->EnableScheduleForActor(aggregatorId);

        CmdWrite(0, "sourceid0", TestData(), tc, false, {}, true);
        CmdWrite(0, "sourceid1", TestData(), tc, false);
        CmdWrite(0, "sourceid2", TestData(), tc, false);
        PQGetPartInfo(0, 30, tc);

        {
            TDispatchOptions options;
            options.FinalEvents.emplace_back(TEvTabletCounters::EvTabletAddLabeledCounters);
            tc.Runtime->DispatchEvents(options);
        }
        //UNIT_ASSERT(labeledCountersReceived);

        {
            NSchemeCache::TDescribeResult::TPtr result = new NSchemeCache::TDescribeResult{};
            result->SetPath("/Root");
            TVector<TString> attrs = {"folder_id", "cloud_id", "database_id"};
            for (auto& attr : attrs) {
                auto ua = result->MutablePathDescription()->AddUserAttributes();
                ua->SetKey(attr);
                ua->SetValue(attr);
            }
            NSchemeCache::TDescribeResult::TCPtr cres = result;
            auto event = MakeHolder<TEvTxProxySchemeCache::TEvWatchNotifyUpdated>(0, "/Root", TPathId{}, cres);
            TActorId pipeClient = tc.Runtime->ConnectToPipe(tc.BalancerTabletId, tc.Edge, 0, GetPipeConfigWithRetries());
            tc.Runtime->SendToPipe(tc.BalancerTabletId, tc.Edge, event.Release(), 0, GetPipeConfigWithRetries(), pipeClient);

            TDispatchOptions options;
            options.FinalEvents.emplace_back(TEvTxProxySchemeCache::EvWatchNotifyUpdated);
            auto processedCountersEvent = tc.Runtime->DispatchEvents(options);
            UNIT_ASSERT_VALUES_EQUAL(processedCountersEvent, true);
        }
        {
            TDispatchOptions options;
            options.FinalEvents.emplace_back(TEvPersQueue::EvPeriodicTopicStats);
            auto processedCountersEvent = tc.Runtime->DispatchEvents(options);
            UNIT_ASSERT_VALUES_EQUAL(processedCountersEvent, true);
        }
        {
            auto counters = tc.Runtime->GetAppData(0).Counters;
            auto dbGroup = GetServiceCounters(counters, "topics_serverless", false);

            auto group = dbGroup->GetSubgroup("host", "")
                                ->GetSubgroup("database", "/Root")
                                ->GetSubgroup("cloud_id", "cloud_id")
                                ->GetSubgroup("folder_id", "folder_id")
                                ->GetSubgroup("database_id", "database_id")->GetSubgroup("topic", "topic");
            group->GetNamedCounter("name", "topic.partition.uptime_milliseconds_min", false)->Set(30000);
            group->GetNamedCounter("name", "topic.partition.write.lag_milliseconds_max", false)->Set(600);
            group->GetNamedCounter("name", "topic.partition.uptime_milliseconds_min", false)->Set(30000);
            group->GetNamedCounter("name", "topic.partition.write.lag_milliseconds_max", false)->Set(600);
            group = group->GetSubgroup("consumer", "client");
            group->GetNamedCounter("name", "topic.partition.end_to_end_lag_milliseconds_max", false)->Set(30000);
            group->GetNamedCounter("name", "topic.partition.read.idle_milliseconds_max", false)->Set(30000);
            group->GetNamedCounter("name", "topic.partition.write.lag_milliseconds_max", false)->Set(200);

            TStringStream countersStr;
            dbGroup->OutputHtml(countersStr);
            const TString referenceCounters = NResource::Find(TStringBuf("counters_topics.html"));
            Cerr << "REF: " << referenceCounters << "\n";
            Cerr << "COUNTERS: " << countersStr.Str() << "\n";
            UNIT_ASSERT_VALUES_EQUAL(countersStr.Str() + "\n", referenceCounters);
        } });
    }

    void CheckLabeledCountersResponse(TTestContext & tc, ui32 count, TVector<TString> mustHave = {}) {
        IActor* actor = CreateClusterLabeledCountersAggregatorActor(tc.Edge, TTabletTypes::PersQueue);
        tc.Runtime->Register(actor);

        TAutoPtr<IEventHandle> handle;
        TEvTabletCounters::TEvTabletLabeledCountersResponse* result;
        result = tc.Runtime->GrabEdgeEvent<TEvTabletCounters::TEvTabletLabeledCountersResponse>(handle);
        UNIT_ASSERT(result);

        THashSet<TString> groups;

        Cerr << "NEW ANS:\n";
        for (ui32 i = 0; i < result->Record.LabeledCountersByGroupSize(); ++i) {
            auto& c = result->Record.GetLabeledCountersByGroup(i);
            groups.insert(c.GetGroup());
            Cerr << "ANS GROUP " << c.GetGroup() << "\n";
        }
        UNIT_ASSERT_VALUES_EQUAL(groups.size(), count);
        for (auto& g : mustHave) {
            Cerr << "CHECKING GROUP " << g << "\n";
            UNIT_ASSERT(groups.contains(g));
        }
    }

    Y_UNIT_TEST(ImportantFlagSwitching) {
        const TString topicName = "rt3.dc1--asdfgs--topic";

        TTestContext tc;
        RunTestWithReboots(tc.TabletIds, [&]() { return tc.InitialEventsFilter.Prepare(); }, [&](const TString& dispatchName, std::function<void(TTestActorRuntime&)> setup, bool& activeZone) {
        TFinalizer finalizer(tc);
        tc.Prepare(dispatchName, setup, activeZone);
        activeZone = false;
        tc.Runtime->SetScheduledLimit(1000);

        auto MakeTopics = [&] (const TVector<TString>& users) {
            TVector<TString> res;
            for (const auto& u : users) {
                res.emplace_back(NKikimr::JoinPath({u, topicName}));
            }
            return res;
        };

        PQTabletPrepare({}, {}, tc);
        {
            TDispatchOptions options;
            options.FinalEvents.emplace_back(TEvTabletCounters::EvTabletAddLabeledCounters);
            tc.Runtime->DispatchEvents(options);
        }
        // Topic counters only
        CheckLabeledCountersResponse(tc, 8);

        // Topic counters + important
        PQTabletPrepare({}, {{"user", true}}, tc);
        {
            TDispatchOptions options;
            options.FinalEvents.emplace_back(TEvTabletCounters::EvTabletAddLabeledCounters);
            tc.Runtime->DispatchEvents(options);
        }
        CheckLabeledCountersResponse(tc, 8, MakeTopics({"user/1"}));

        PQTabletPrepare({}, {}, tc);
        {
            TDispatchOptions options;
            options.FinalEvents.emplace_back(TEvTabletCounters::EvTabletAddLabeledCounters);
            tc.Runtime->DispatchEvents(options);
        }
        {
            TDispatchOptions options;
            options.FinalEvents.emplace_back(TEvTabletCounters::EvTabletAddLabeledCounters);
            tc.Runtime->DispatchEvents(options);
        }
        // Topic counters + not important
        CheckLabeledCountersResponse(tc, 8, MakeTopics({"user/0"}));

        // Topic counters + not important
        PQTabletPrepare({}, {{"user", true}, {"user2", true}}, tc);
        {
            TDispatchOptions options;
            options.FinalEvents.emplace_back(TEvTabletCounters::EvTabletAddLabeledCounters);
            tc.Runtime->DispatchEvents(options);
        }
        {
            TDispatchOptions options;
            options.FinalEvents.emplace_back(TEvTabletCounters::EvTabletAddLabeledCounters);
            tc.Runtime->DispatchEvents(options);
        }
        CheckLabeledCountersResponse(tc, 11, MakeTopics({"user/1", "user2/1"}));

        PQTabletPrepare({}, {{"user", true}, {"user2", false}}, tc);
        for (ui32 i = 0 ; i < 2; ++i){
            TDispatchOptions options;
            options.FinalEvents.emplace_back(TEvTabletCounters::EvTabletAddLabeledCounters);
            tc.Runtime->DispatchEvents(options);
        }

        CheckLabeledCountersResponse(tc, 12, MakeTopics({"user/1", "user2/0"}));

        PQTabletPrepare({}, {{"user", true}}, tc);
        for (ui32 i = 0 ; i < 2; ++i){
            TDispatchOptions options;
            options.FinalEvents.emplace_back(TEvTabletCounters::EvTabletAddLabeledCounters);
            tc.Runtime->DispatchEvents(options);
        }

        CheckLabeledCountersResponse(tc, 8, MakeTopics({"user/1"})); });
    }

    Y_UNIT_TEST(PartitionKeyCompaction) {
        SetEnv("FAST_UT", "1");
        TTestContext tc;
        RunTestWithReboots(tc.TabletIds, [&]() { return tc.InitialEventsFilter.Prepare(); }, [&](const TString& dispatchName, std::function<void(TTestActorRuntime&)> setup, bool& activeZone) {
            TFinalizer finalizer(tc);
            activeZone = false;
            bool dbRegistered = false;

            tc.EnableDetailedPQLog = true;
            tc.Prepare(dispatchName, setup, activeZone, true, true, true);

            tc.Runtime->GetAppData(0).FeatureFlags.SetEnableTopicCompactificationByKey(true);
            tc.Runtime->GetAppData(0).PQConfig.MutableCompactionConfig()->SetBlobsSize(10_MB);
            tc.Runtime->SetScheduledLimit(10000);

            tc.Runtime->SetObserverFunc([&](TAutoPtr<IEventHandle>& event) {
                if (event->GetTypeRewrite() == NSysView::TEvSysView::EvRegisterDbCounters) {
                    auto database = event.Get()->Get<NSysView::TEvSysView::TEvRegisterDbCounters>()->Database;
                    UNIT_ASSERT_VALUES_EQUAL(database, "/Root/PQ");
                    dbRegistered = true;
                }
                return TTestActorRuntime::DefaultObserverFunc(event);
            });
            PQTabletPrepare({.deleteTime = 3600, .writeSpeed = 2_MB, .enableCompactificationByKey = true}, {}, tc);

            TFakeSchemeShardState::TPtr state{new TFakeSchemeShardState()};
            ui64 ssId = 325;
            BootFakeSchemeShard(*tc.Runtime, ssId, state);

            auto balancerParams = TBalancerParams::FromContext("topic", {{0, {tc.TabletId, 1}}}, ssId, tc);
            balancerParams.EnableKeyCompaction = true;
            PQBalancerPrepare(balancerParams);

            IActor* actor = CreateTabletCountersAggregator(false);
            auto aggregatorId = tc.Runtime->Register(actor);
            tc.Runtime->EnableScheduleForActor(aggregatorId);
            TString s{5_MB, 'c'};
            ui64 currentOffset = 0;
            auto writeData = [&](const TString& key, ui32 count) {
                TVector<std::pair<ui64, TString>> data;
                for (auto i = 0u; i < count; ++i) {
                    NKikimrPQClient::TDataChunk proto;
                    proto.SetSeqNo(i + 1);
                    proto.SetData(s);
                    auto* msgMeta = proto.AddMessageMeta();
                    msgMeta->set_key("__key");
                    msgMeta->set_value(key);
                    TString dataChunkStr;
                    bool res = proto.SerializeToString(&dataChunkStr);
                    Y_ABORT_UNLESS(res);
                    data.push_back({i + 1, dataChunkStr});
                }
                CmdWrite(0, "sourceid0", std::move(data), tc, false, {}, false, "", -1, currentOffset, false, false, true);
                currentOffset += count;
            };
            writeData("key1", 1);
            writeData("key2", 1);
            writeData("key3", 2);
            writeData("key4", 2);
            writeData("key2", 2);

            i64 expectedOffset = 4;
            i64 consumerOffset = -1;
            while (consumerOffset < expectedOffset) {
                consumerOffset = CmdGetOffset(0, CLIENTID_COMPACTION_CONSUMER, Nothing(), tc);
                Cerr << "Got compacter offset = " << consumerOffset << Endl;
            }
            UNIT_ASSERT(consumerOffset >= expectedOffset);

            {
                NSchemeCache::TDescribeResult::TPtr result = new NSchemeCache::TDescribeResult{};
                result->SetPath("/Root");
                TVector<TString> attrs = {"folder_id", "cloud_id", "database_id"};
                for (auto& attr : attrs) {
                    auto ua = result->MutablePathDescription()->AddUserAttributes();
                    ua->SetKey(attr);
                    ua->SetValue(attr);
                }
                NSchemeCache::TDescribeResult::TCPtr cres = result;
                auto event = MakeHolder<TEvTxProxySchemeCache::TEvWatchNotifyUpdated>(0, "/Root", TPathId{}, cres);
                TActorId pipeClient = tc.Runtime->ConnectToPipe(tc.BalancerTabletId, tc.Edge, 0, GetPipeConfigWithRetries());
                tc.Runtime->SendToPipe(tc.BalancerTabletId, tc.Edge, event.Release(), 0, GetPipeConfigWithRetries(), pipeClient);

                TDispatchOptions options;
                options.FinalEvents.emplace_back(TEvTxProxySchemeCache::EvWatchNotifyUpdated);
                auto processedCountersEvent = tc.Runtime->DispatchEvents(options);
                UNIT_ASSERT_VALUES_EQUAL(processedCountersEvent, true);
            }
            {
                TDispatchOptions options;
                options.FinalEvents.emplace_back(TEvPersQueue::EvPeriodicTopicStats);
                auto processedCountersEvent = tc.Runtime->DispatchEvents(options);
                UNIT_ASSERT_VALUES_EQUAL(processedCountersEvent, true);
            }

            auto counters = tc.Runtime->GetAppData(0).Counters;
            {
                auto dbGroup = GetServiceCounters(counters, "topics_serverless", false);
                auto group = dbGroup->GetSubgroup("host", "")
                                 ->GetSubgroup("database", "/Root")
                                 ->GetSubgroup("cloud_id", "cloud_id")
                                 ->GetSubgroup("folder_id", "folder_id")
                                 ->GetSubgroup("database_id", "database_id")
                                 ->GetSubgroup("topic", "topic");

                group->GetNamedCounter("name", "topic.partition.uptime_milliseconds_min", false)->Set(30000);
                group->GetNamedCounter("name", "topic.partition.write.lag_milliseconds_max", false)->Set(600);
                group->GetNamedCounter("name", "topic.partition.uptime_milliseconds_min", false)->Set(30000);
                group->GetNamedCounter("name", "topic.partition.write.lag_milliseconds_max", false)->Set(600);
                group->GetNamedCounter("name", "topic.partition.read.throttled_microseconds_max", false)->Set(2000);
                group = group->GetSubgroup("consumer", "__ydb_compaction_consumer");
                group->GetNamedCounter("name", "topic.partition.write.lag_milliseconds_max", false)->Set(200);
                group->GetNamedCounter("name", "topic.partition.end_to_end_lag_milliseconds_max", false)->Set(30000);
                group->GetNamedCounter("name", "topic.partition.read.throttled_microseconds_max", false)->Set(2000);
                group->GetNamedCounter("name", "topic.partition.read.idle_milliseconds_max", false)->Set(300);
                group->GetNamedCounter("name", "topic.partition.read.lag_milliseconds_max", false)->Set(300);

                TStringStream countersStr;
                dbGroup->OutputHtml(countersStr);
                const TString referenceCounters = NResource::Find(TStringBuf("counters_topics_extended.html"));
                Cerr << "REF: " << referenceCounters << "\n";
                Cerr << "COUNTERS: " << countersStr.Str() << "\n";
                UNIT_ASSERT_VALUES_EQUAL(countersStr.Str() + "\n", referenceCounters);
            }
            // {
            //     auto dbGroup = GetServiceCounters(counters, "datastreams_serverless");
            //     auto topicGroup = dbGroup->GetSubgroup("topic", "topic");
            //     UNIT_ASSERT_VALUES_EQUAL(
            //         topicGroup->GetNamedCounter("name", "topic.key_compaction.read_cycles_complete_total", false)->Val(),
            //         4
            //     );
            //     UNIT_ASSERT_VALUES_EQUAL(
            //         topicGroup->GetNamedCounter("name", "topic.key_compaction.write_cycles_complete_total", false)->Val(),
            //         3
            //     );
            // }
        });
    }

    Y_UNIT_TEST(PartitionBlobCompactionCounters) {
        SetEnv("FAST_UT", "1");
        TTestContext tc;
        RunTestWithReboots(tc.TabletIds, [&]() { return tc.InitialEventsFilter.Prepare(); }, [&](const TString& dispatchName, std::function<void(TTestActorRuntime&)> setup, bool& activeZone) {
        TFinalizer finalizer(tc);
        activeZone = false;
        bool dbRegistered = false;

        tc.EnableDetailedPQLog = true;
        tc.Prepare(dispatchName, setup, activeZone, true, true, true);

        tc.Runtime->GetAppData(0).PQConfig.MutableCompactionConfig()->SetBlobsSize(10_MB);
        tc.Runtime->SetScheduledLimit(10000);

        tc.Runtime->SetObserverFunc([&](TAutoPtr<IEventHandle>& event) {
            if (event->GetTypeRewrite() == NSysView::TEvSysView::EvRegisterDbCounters) {
                auto database = event.Get()->Get<NSysView::TEvSysView::TEvRegisterDbCounters>()->Database;
                UNIT_ASSERT_VALUES_EQUAL(database, "/Root/PQ");
                dbRegistered = true;
            } else if (event->GetTypeRewrite() == TEvPQ::EEv::EvRunCompaction) {
                Cerr << "===Dropped TEvRunCompaction with blobs count: " << event->Get<TEvPQ::TEvRunCompaction>()->BlobsCount << Endl;
                return TTestActorRuntime::EEventAction::DROP;
            }
            return TTestActorRuntime::DefaultObserverFunc(event);
        });
        PQTabletPrepare({.deleteTime = 3600, .writeSpeed = 2_MB, .enableCompactificationByKey = false}, {}, tc);

        TFakeSchemeShardState::TPtr state{new TFakeSchemeShardState()};
        ui64 ssId = 325;
        BootFakeSchemeShard(*tc.Runtime, ssId, state);

        auto balancerParams = TBalancerParams::FromContext("topic", {{0, {tc.TabletId, 1}}}, ssId, tc);
        balancerParams.EnableKeyCompaction = false;
        PQBalancerPrepare(balancerParams);

        IActor* actor = CreateTabletCountersAggregator(false);
        auto aggregatorId = tc.Runtime->Register(actor);
        tc.Runtime->EnableScheduleForActor(aggregatorId);
        TString s{5_MB, 'c'};
        ui64 currentOffset = 0;
        auto writeData = [&](ui32 count) {
            TVector<std::pair<ui64, TString>> data;
            for (auto i = 0u; i < count; ++i) {
                data.push_back({i + 1, s});
            }
            CmdWrite(0, "sourceid0", std::move(data), tc, false, {}, false, "", -1, currentOffset, false, false, true);
            currentOffset += count;
        };
        writeData(3);
        writeData(3);

        {
            NSchemeCache::TDescribeResult::TPtr result = new NSchemeCache::TDescribeResult{};
            result->SetPath("/Root");
            TVector<TString> attrs = {"folder_id", "cloud_id", "database_id"};
            for (auto& attr : attrs) {
                auto ua = result->MutablePathDescription()->AddUserAttributes();
                ua->SetKey(attr);
                ua->SetValue(attr);
            }
            NSchemeCache::TDescribeResult::TCPtr cres = result;
            auto event = MakeHolder<TEvTxProxySchemeCache::TEvWatchNotifyUpdated>(0, "/Root", TPathId{}, cres);
            TActorId pipeClient = tc.Runtime->ConnectToPipe(tc.BalancerTabletId, tc.Edge, 0, GetPipeConfigWithRetries());
            tc.Runtime->SendToPipe(tc.BalancerTabletId, tc.Edge, event.Release(), 0, GetPipeConfigWithRetries(), pipeClient);

            TDispatchOptions options;
            options.FinalEvents.emplace_back(TEvTxProxySchemeCache::EvWatchNotifyUpdated);
            options.FinalEvents.emplace_back(TEvPQ::EEv::EvRunCompaction);
            auto processedCountersEvent = tc.Runtime->DispatchEvents(options);
            UNIT_ASSERT(processedCountersEvent);
        }
        {
            TDispatchOptions options;
            options.FinalEvents.emplace_back(TEvPersQueue::EvPeriodicTopicStats);
            auto processedCountersEvent = tc.Runtime->DispatchEvents(options);
            UNIT_ASSERT_VALUES_EQUAL(processedCountersEvent, true);
        }

        auto counters = tc.Runtime->GetAppData(0).Counters;
        {
            auto dbGroup = GetServiceCounters(counters, "topics_serverless", false);
            auto group = dbGroup->GetSubgroup("host", "")
                             ->GetSubgroup("database", "/Root")
                             ->GetSubgroup("cloud_id", "cloud_id")
                             ->GetSubgroup("folder_id", "folder_id")
                             ->GetSubgroup("database_id", "database_id")
                             ->GetSubgroup("topic", "topic");


            TStringStream countersStr;
            dbGroup->OutputHtml(countersStr);
            Cerr << "COUNTERS: " << countersStr.Str() << "\n";

            UNIT_ASSERT_VALUES_EQUAL(
                group->FindNamedCounter("name", "topic.partition.blobs.uncompacted_count_max")->Val(), 4);
            UNIT_ASSERT_VALUES_EQUAL(
                group->FindNamedCounter("name", "topic.partition.blobs.uncompacted_bytes_max")->Val(), 31461348);
            UNIT_ASSERT_GE(group->FindNamedCounter("name", "topic.partition.blobs.compaction_lag_milliseconds_max")->Val(), 1000);
        } });
    }

    Y_UNIT_TEST(NewConsumersCountersAppear) {
        TTestContext tc;
        tc.InitialEventsFilter.Prepare();

        TFinalizer finalizer(tc);
        bool activeZone = false;
        bool dbRegistered{false};
        bool labeledCountersReceived = false;

        tc.Prepare("", [](TTestActorRuntime&) {}, activeZone, true, true, true);

        tc.Runtime->SetScheduledLimit(5000);

        tc.Runtime->SetObserverFunc([&](TAutoPtr<IEventHandle>& event) {
            if (event->GetTypeRewrite() == NSysView::TEvSysView::EvRegisterDbCounters) {
                auto database = event.Get()->Get<NSysView::TEvSysView::TEvRegisterDbCounters>()->Database;
                UNIT_ASSERT_VALUES_EQUAL(database, "/Root/PQ");
                dbRegistered = true;
            } else if (event->GetTypeRewrite() == TEvTabletCounters::EvTabletAddLabeledCounters) {
                labeledCountersReceived = true;
            }
            return TTestActorRuntime::DefaultObserverFunc(event);
        });
        PQTabletPrepare({.deleteTime = 3600, .writeSpeed = 100_KB, .meteringMode = NKikimrPQ::TPQTabletConfig::METERING_MODE_REQUEST_UNITS}, {{"client", true}}, tc);
        TFakeSchemeShardState::TPtr state{new TFakeSchemeShardState()};
        ui64 ssId = 325;
        BootFakeSchemeShard(*tc.Runtime, ssId, state);

        PQBalancerPrepare("topic", {{0, {tc.TabletId, 1}}}, ssId, tc, false, false, {"user1", "user2"});

        IActor* actor = CreateTabletCountersAggregator(false);
        auto aggregatorId = tc.Runtime->Register(actor);
        tc.Runtime->EnableScheduleForActor(aggregatorId);

        CmdWrite(0, "sourceid0", TestData(), tc, false, {}, true);
        CmdWrite(0, "sourceid1", TestData(), tc, false);
        CmdWrite(0, "sourceid2", TestData(), tc, false);
        PQGetPartInfo(0, 30, tc);

        {
            NSchemeCache::TDescribeResult::TPtr result = new NSchemeCache::TDescribeResult{};
            result->SetPath("/Root");
            TVector<TString> attrs = {"folder_id", "cloud_id", "database_id"};
            for (auto& attr : attrs) {
                auto ua = result->MutablePathDescription()->AddUserAttributes();
                ua->SetKey(attr);
                ua->SetValue(attr);
            }
            NSchemeCache::TDescribeResult::TCPtr cres = result;
            auto event = MakeHolder<TEvTxProxySchemeCache::TEvWatchNotifyUpdated>(0, "/Root", TPathId{}, cres);
            TActorId pipeClient = tc.Runtime->ConnectToPipe(tc.BalancerTabletId, tc.Edge, 0, GetPipeConfigWithRetries());
            tc.Runtime->SendToPipe(tc.BalancerTabletId, tc.Edge, event.Release(), 0, GetPipeConfigWithRetries(), pipeClient);

            TDispatchOptions options;
            options.FinalEvents.emplace_back(TEvTxProxySchemeCache::EvWatchNotifyUpdated);
            auto processedCountersEvent = tc.Runtime->DispatchEvents(options);
            UNIT_ASSERT_VALUES_EQUAL(processedCountersEvent, true);
        }
        {
            TDispatchOptions options;
            options.FinalEvents.emplace_back(TEvPersQueue::EvPeriodicTopicStats);
            auto processedCountersEvent = tc.Runtime->DispatchEvents(options);
            UNIT_ASSERT_VALUES_EQUAL(processedCountersEvent, true);
        }

        {
            auto counters = tc.Runtime->GetAppData(0).Counters;
            auto dbGroup = GetServiceCounters(counters, "topics_serverless", false);

            auto group = dbGroup->GetSubgroup("host", "")
                             ->GetSubgroup("database", "/Root")
                             ->GetSubgroup("cloud_id", "cloud_id")
                             ->GetSubgroup("folder_id", "folder_id")
                             ->GetSubgroup("database_id", "database_id")
                             ->GetSubgroup("topic", "topic");
            for (const auto& user : {"client", "user1", "user2"}) {
                auto consumerSG = group->FindSubgroup("consumer", user);
                UNIT_ASSERT_C(consumerSG, user);
            }
        }
        PQBalancerPrepare("topic", {{0, {tc.TabletId, 1}}}, ssId, tc, false, false, {"user3", "user2"});
        {
            TDispatchOptions options;
            options.FinalEvents.emplace_back(TEvPersQueue::EvPeriodicTopicStats);
            auto processedCountersEvent = tc.Runtime->DispatchEvents(options);
            UNIT_ASSERT_VALUES_EQUAL(processedCountersEvent, true);
        }

        {
            auto counters = tc.Runtime->GetAppData(0).Counters;
            auto dbGroup = GetServiceCounters(counters, "topics_serverless", false);

            auto group = dbGroup->GetSubgroup("host", "")
                             ->GetSubgroup("database", "/Root")
                             ->GetSubgroup("cloud_id", "cloud_id")
                             ->GetSubgroup("folder_id", "folder_id")
                             ->GetSubgroup("database_id", "database_id")
                             ->GetSubgroup("topic", "topic");
            for (const auto& user : {"user2", "user3"}) {
                auto consumerSG = group->FindSubgroup("consumer", user);
                UNIT_ASSERT_C(consumerSG, user);
            }
        }
    }

} // Y_UNIT_TEST_SUITE(PQCountersLabeled)

Y_UNIT_TEST_SUITE(TMultiBucketCounter) {
void CheckBucketsValues(const TVector<std::pair<double, ui64>>& actual, const TVector<std::pair<double, ui64>>& expected) {
    UNIT_ASSERT_VALUES_EQUAL(actual.size(), expected.size());
    for (auto i = 0u; i < expected.size(); ++i) {
        UNIT_ASSERT_VALUES_EQUAL(actual[i].second, expected[i].second);
        UNIT_ASSERT_C(abs(actual[i].first - expected[i].first) < 0.0001, TStringBuilder() << actual[i].first << "-" << expected[i].first);
    }
}

Y_UNIT_TEST(InsertAndUpdate) {
    TMultiBucketCounter counter({100, 200, 500, 1000, 5000}, 5, 0);
    counter.Insert(19, 3);
    counter.Insert(15, 1);
    counter.Insert(17, 1);

    counter.Insert(100, 1);
    counter.Insert(50001, 5);

    CheckBucketsValues(counter.GetValues(), {{(19*3 + 15 + 17) / 5.0, 5}, {100, 1}, {50001, 5}});

    counter.UpdateTimestamp(50);

    CheckBucketsValues(counter.GetValues(), {{50.0 + (19*3 + 15 + 17) / 5.0, 5}, {150, 1}, {50051, 5}});
    counter.Insert(190, 1);
    counter.Insert(155, 1);

    CheckBucketsValues(counter.GetValues(), {{50.0 + (19*3 + 15 + 17) / 5.0, 5}, {152.5, 2}, {190, 1}, {50051, 5}});

    counter.UpdateTimestamp(1050);

    CheckBucketsValues(counter.GetValues(), {{(1067.8 * 5 + 1152.5 * 2 + 1190) / 8, 8}, {51051, 5}});
}

Y_UNIT_TEST(ManyCounters) {
    TVector<ui64> buckets = {100, 200, 500, 1000, 2000, 5000};
    ui64 multiplier = 20;
    TMultiBucketCounter counter(buckets, multiplier, 0);
    for (auto i = 1u; i <= 5000; i++) {
        counter.Insert(1, 1);
        counter.UpdateTimestamp(i);
    }
    counter.Insert(1, 1);

    const auto& values = counter.GetValues(true);
    ui64 prev = 0;
    for (auto i = 0u; i < buckets.size(); i++) {
        ui64 sum = 0u;
        for (auto j = 0u; j < multiplier; j++) {
            sum += values[i * multiplier + j].second;
        }
        Cerr << "Bucket: " << buckets[i] << " elems count: " << sum << Endl;
        ui64 bucketSize = buckets[i] - prev;
        prev = buckets[i];
        i64 diff = sum - bucketSize;
        UNIT_ASSERT(std::abs(diff) < (i64)(bucketSize / 10));
    }
}

} // Y_UNIT_TEST_SUITE(TMultiBucketCounter)

} // namespace NKikimr::NPQ
