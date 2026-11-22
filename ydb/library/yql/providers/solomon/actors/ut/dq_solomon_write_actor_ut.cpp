#include "ut_helpers.h"

#include <ydb/library/services/services.pb.h>
#include <ydb/library/testlib/solomon_helpers/solomon_emulator_helpers.h>
#include <ydb/library/yql/providers/solomon/common/constants.h>

#include <library/cpp/json/json_reader.h>
#include <library/cpp/testing/unittest/registar.h>
#include <util/generic/algorithm.h>
#include <util/generic/yexception.h>
#include <yql/essentials/minikql/computation/mkql_computation_node_impl.h>
#include <yql/essentials/minikql/mkql_string_util.h>
#include <yql/essentials/utils/yql_panic.h>

namespace NYql::NDq {

using namespace NKikimr::NMiniKQL;
using namespace NTestUtils;

constexpr TDuration WaitTimeout = TDuration::Seconds(10);

namespace {

class TBatchingCredentialsProvider final: public NYdb::ICredentialsProvider {
public:
    bool Fail = false;

    std::string GetAuthInfo() const override {
        if (Fail) {
            ythrow yexception() << "Test credentials failure";
        }
        return "test-token";
    }

    bool IsValid() const override {
        return true;
    }
};

class TBatchingCredentialsFactory final: public IStructuredTokenCredentialsFactory {
    class TProviderFactory final: public NYdb::ICredentialsProviderFactory {
    public:
        explicit TProviderFactory(std::shared_ptr<TBatchingCredentialsProvider> provider)
            : Provider(std::move(provider))
        {
        }

        std::shared_ptr<NYdb::ICredentialsProvider> CreateProvider() const override {
            return Provider;
        }

    private:
        const std::shared_ptr<TBatchingCredentialsProvider> Provider;
    };

public:
    explicit TBatchingCredentialsFactory(std::shared_ptr<TBatchingCredentialsProvider> provider)
        : Provider(std::move(provider))
    {
    }

    std::shared_ptr<NYdb::ICredentialsProviderFactory> Create(const TString&, bool) override {
        return std::make_shared<TProviderFactory>(Provider);
    }

private:
    const std::shared_ptr<TBatchingCredentialsProvider> Provider;
};

class TTrackedRow final: public TComputationValue<TTrackedRow> {
public:
    TTrackedRow(TMemoryUsageInfo* memInfo, NUdf::TUnboxedValue row, ui64& destroyed)
        : TComputationValue<TTrackedRow>(memInfo)
        , Row(std::move(row))
        , Destroyed(destroyed)
    {
    }

    ~TTrackedRow() override {
        ++Destroyed;
    }

private:
    NUdf::TUnboxedValue GetElement(ui32 index) const override {
        return Row.GetElement(index);
    }

    const NUdf::TUnboxedValue Row;
    ui64& Destroyed;
};

struct TBatchingCallbacks final: IDqComputeActorAsyncOutput::ICallbacks {
    ui64 Finished = 0;
    ui64 Resumed = 0;
    TVector<ui64> Checkpoints;
    TVector<TIssues> Errors;
    TVector<NDqProto::StatusIds::StatusCode> ErrorCodes;

    void ResumeExecution(EResumeSource) override {
        ++Resumed;
    }

    void OnAsyncOutputError(ui64, const TIssues& issues, NDqProto::StatusIds::StatusCode code) override {
        Errors.push_back(issues);
        ErrorCodes.push_back(code);
    }

    void OnAsyncOutputStateSaved(TSinkState&&, ui64, const NDqProto::TCheckpoint& checkpoint) override {
        Checkpoints.push_back(checkpoint.GetId());
    }

    void OnAsyncOutputStateCommitted(ui64, const NDqProto::TCheckpoint&) override {
    }

    void OnAsyncOutputFinished(ui64) override {
        ++Finished;
    }
};

struct TBatchingFixture {
    TBatchingCallbacks Callbacks;
    TVector<THolder<NActors::IEventHandle>> Requests;
    NActors::TActorId ComputeId;
    NActors::TActorId SinkId;
    NSo::NProto::ESolomonClusterType ClusterType;
    ui32 Sensors;
    ui64 NextRow = 0;
    ui64 ValueOffset = 0;
    bool SameLabels = false;
    bool InvalidLabel = false;
    bool TrackRows = false;
    ui64 DestroyedRows = 0;
    size_t ExpectedErrors = 0;
    const std::shared_ptr<TBatchingCredentialsProvider> Credentials = std::make_shared<TBatchingCredentialsProvider>();
    // Destroy the runtime before the callbacks and captured requests it borrows.
    NActors::TTestActorRuntimeBase Runtime;

    explicit TBatchingFixture(
        NSo::NProto::ESolomonClusterType clusterType = NSo::NProto::CT_SOLOMON, ui32 sensors = 1, i64 freeSpace = DqSolomonDefaultFreeSpace)
        : ClusterType(clusterType)
        , Sensors(sensors)
    {
        Runtime.Initialize();
        ComputeId = Runtime.Register(new TFakeActor(std::make_shared<TAsyncInputPromises>(), std::make_shared<TAsyncOutputPromises>()));
        Runtime.GetLogSettings(0)->Append(
            NKikimrServices::EServiceKikimr_MIN, NKikimrServices::EServiceKikimr_MAX, NKikimrServices::EServiceKikimr_Name);
        Runtime.SetEventFilter([this](auto&, TAutoPtr<NActors::IEventHandle>& event) {
            if (event->GetTypeRewrite() == NHttp::TEvHttpProxy::TEvHttpOutgoingRequest::EventType && event->Sender != SinkId) {
                Requests.emplace_back(event.Release());
                return true;
            }
            return false;
        });

        Execute([&](TFakeActor& compute) {
            NSo::NProto::TDqSolomonShard settings;
            settings.SetEndpoint("localhost:1");
            settings.SetProject("project");
            settings.SetCluster("cluster");
            settings.SetService("service");
            settings.SetClusterType(ClusterType);
            auto& scheme = *settings.MutableScheme();
            scheme.MutableTimestamp()->SetIndex(0);
            scheme.MutableTimestamp()->SetDataTypeId(NUdf::TDataType<NUdf::TTimestamp>::Id);
            auto& label = *scheme.AddLabels();
            label.SetKey("label");
            label.SetIndex(1);
            label.SetDataTypeId(NUdf::TDataType<char*>::Id);
            for (ui32 i = 0; i < Sensors; ++i) {
                auto& sensor = *scheme.AddSensors();
                sensor.SetKey(TStringBuilder() << "sensor" << i);
                sensor.SetIndex(i + 2);
                sensor.SetDataTypeId(NUdf::TDataType<ui64>::Id);
            }
            auto counters = MakeIntrusive<NMonitoring::TDynamicCounters>();
            auto [output, actor] =
                CreateDqSolomonWriteActor(std::move(settings), 0, TCollectStatsLevel::None, "batching-test", 0, {}, &Callbacks, counters,
                    std::make_shared<TBatchingCredentialsFactory>(Credentials),
                    std::shared_ptr<TScopedAlloc>(&compute.Alloc, [](TScopedAlloc*) {
                    }), freeSpace);
            compute.InitAsyncOutput(output, actor);
            SinkId = actor->SelfId();
        });
    }

    ~TBatchingFixture() {
        // The fixture borrows the fake compute actor's allocator. Release retained
        // MiniKQL rows before the runtime can destroy that actor.
        Stop();
        Runtime.SimulateSleep(TDuration::MilliSeconds(1));
    }

    void Stop() {
        Execute([](TFakeActor& compute) {
            compute.Terminate();
        });
    }

    void Execute(TCallback callback) {
        auto promise = NThreading::NewPromise();
        std::exception_ptr error;
        Runtime.Send(new NActors::IEventHandle(ComputeId, {}, new TEvPrivate::TEvExecute(promise, std::move(callback), error)));
        NActors::TDispatchOptions options;
        options.CustomFinalCondition = [&] {
            return promise.GetFuture().HasValue();
        };
        Runtime.DispatchEvents(options, WaitTimeout);
        UNIT_ASSERT(promise.GetFuture().HasValue());
        if (error) {
            std::rethrow_exception(error);
        }
    }

    void Pump() {
        Runtime.SimulateSleep(TDuration::MilliSeconds(1));
        UNIT_ASSERT_VALUES_EQUAL(Callbacks.Errors.size(), ExpectedErrors);
    }

    void Write(ui32 rows, const TMaybe<NDqProto::TCheckpoint>& checkpoint = Nothing(), bool finished = false, i64 dataSize = -1) {
        Execute([&](TFakeActor& compute) {
            TUnboxedValueBatch batch;
            for (ui32 i = 0; i < rows; ++i, ++NextRow) {
                NUdf::TUnboxedValue* items = nullptr;
                NUdf::TUnboxedValue value = compute.GetHolderFactory().CreateDirectArrayHolder(Sensors + 2, items);
                items[0] = NUdf::TUnboxedValuePod(TInstant::Seconds(NextRow + 1).MicroSeconds());
                items[1] = MakeString(SameLabels ? TString("same") : ToString(NextRow));
                if (InvalidLabel) {
                    items[1] = MakeString(TString("\xff"));
                }
                for (ui32 sensor = 0; sensor < Sensors; ++sensor) {
                    items[sensor + 2] = NUdf::TUnboxedValuePod(ValueOffset + NextRow * Sensors + sensor);
                }
                if (TrackRows) {
                    batch.emplace_back(compute.GetHolderFactory().Create<TTrackedRow>(std::move(value), DestroyedRows));
                } else {
                    batch.emplace_back(std::move(value));
                }
            }
            compute.DqAsyncOutput->SendData(std::move(batch), dataSize < 0 ? rows * 100 : dataSize, checkpoint, finished);
        });
        Pump();
    }

    const NHttp::TEvHttpProxy::TEvHttpOutgoingRequest& Request(size_t index) const {
        UNIT_ASSERT(index < Requests.size());
        return *Requests[index]->Get<NHttp::TEvHttpProxy::TEvHttpOutgoingRequest>();
    }

    TVector<ui64> Values(size_t index) const {
        const auto& request = Request(index);
        UNIT_ASSERT(request.AllowConnectionReuse);
        NJson::TJsonValue json;
        UNIT_ASSERT(NJson::ReadJsonTree(TString(request.Request->Body), &json, true));
        const auto& metrics = json[ClusterType == NSo::NProto::CT_MONITORING ? "metrics" : "sensors"].GetArray();
        TVector<ui64> values;
        for (const auto& metric : metrics) {
            if (metric.Has("timeseries")) {
                for (const auto& point : metric["timeseries"].GetArray()) {
                    values.push_back(point["value"].GetUIntegerRobust());
                }
            } else {
                values.push_back(metric["value"].GetUIntegerRobust());
            }
        }
        Sort(values);
        return values;
    }

    void AssertRequest(size_t index, ui64 first, ui64 count) const {
        NJson::TJsonValue json;
        UNIT_ASSERT(NJson::ReadJsonTree(TString(Request(index).Request->Body), &json, true));
        const bool cloud = ClusterType == NSo::NProto::CT_MONITORING;
        for (const auto& metric : json[cloud ? "metrics" : "sensors"].GetArray()) {
            UNIT_ASSERT_VALUES_EQUAL(metric[cloud ? "type" : "kind"].GetString(), "IGAUGE");
            auto checkPoint = [&](const NJson::TJsonValue& point) {
                const ui64 value = point["value"].GetUIntegerRobust();
                const ui64 row = (value - ValueOffset) / Sensors;
                const TString sensor = TStringBuilder() << "sensor" << (value - ValueOffset) % Sensors;
                UNIT_ASSERT_VALUES_EQUAL((cloud ? metric["name"] : metric["labels"]["name"]).GetString(), sensor);
                UNIT_ASSERT_VALUES_EQUAL(metric["labels"]["label"].GetString(), SameLabels ? TString("same") : ToString(row));
                const auto expectedTime = TInstant::Seconds(row + 1);
                if (cloud) {
                    UNIT_ASSERT_VALUES_EQUAL(TInstant::ParseIso8601(point["ts"].GetString()), expectedTime);
                } else {
                    UNIT_ASSERT_VALUES_EQUAL(point["ts"].GetUIntegerRobust(), expectedTime.Seconds());
                }
            };
            if (metric.Has("timeseries")) {
                for (const auto& point : metric["timeseries"].GetArray()) {
                    checkPoint(point);
                }
            } else {
                checkPoint(metric);
            }
        }
        const auto values = Values(index);
        UNIT_ASSERT_VALUES_EQUAL(values.size(), count);
        for (ui64 i = 0; i < count; ++i) {
            UNIT_ASSERT_VALUES_EQUAL(values[i], first + i);
        }
    }

    void Reply(size_t index, TStringBuf status, const TString& body) {
        UNIT_ASSERT(index < Requests.size());
        auto handle = std::move(Requests[index]);
        Requests.erase(Requests.begin() + index);
        const auto& request = *handle->Get<NHttp::TEvHttpProxy::TEvHttpOutgoingRequest>();
        NHttp::THttpOutgoingResponsePtr response = new NHttp::THttpOutgoingResponse(nullptr, "HTTP", "1.1", status, "Test response");
        response->SetBody(body);
        Runtime.Send(new NActors::IEventHandle(handle->Sender, handle->Recipient,
            new NHttp::TEvHttpProxy::TEvHttpIncomingResponse(request.Request, response->Reverse(request.Request))));
        Pump();
    }

    void Ack(size_t index = 0) {
        const auto count = Values(index).size();
        const TString body = TStringBuilder() << "{\""
                                              << (ClusterType == NSo::NProto::CT_MONITORING ? "writtenMetricsCount" : "sensorsProcessed")
                                              << "\":" << count << "}";
        Reply(index, "200", body);
    }

    void AssertError(NDqProto::StatusIds::StatusCode code, TStringBuf message) const {
        UNIT_ASSERT_VALUES_EQUAL(Callbacks.Errors.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(Callbacks.ErrorCodes.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(Callbacks.ErrorCodes.front(), code);
        UNIT_ASSERT_STRING_CONTAINS(Callbacks.Errors.front().ToString(), message);
    }

    i64 FreeSpace() {
        i64 result = 0;
        Execute([&](TFakeActor& compute) {
            result = compute.DqAsyncOutput->GetFreeSpace();
        });
        return result;
    }

    void FillSlots() {
        for (ui32 i = 0; i < 3; ++i) {
            Write(1);
        }
        UNIT_ASSERT_VALUES_EQUAL(Requests.size(), 3);
        for (ui32 i = 0; i < 3; ++i) {
            AssertRequest(i, i * Sensors, Sensors);
        }
    }

    void Drain() {
        while (!Requests.empty()) {
            Ack();
        }
    }
};

void TestQueuedWritesAreMerged(NSo::NProto::ESolomonClusterType clusterType) {
    TBatchingFixture fixture(clusterType);
    fixture.FillSlots();
    fixture.SameLabels = true;
    for (ui32 i = 0; i < 7; ++i) {
        fixture.Write(1);
    }
    UNIT_ASSERT_VALUES_EQUAL(fixture.Requests.size(), 3);
    fixture.Ack();
    UNIT_ASSERT_VALUES_EQUAL(fixture.Requests.size(), 3);
    fixture.AssertRequest(2, 3, 7);
    fixture.Drain();
    UNIT_ASSERT_VALUES_EQUAL(fixture.FreeSpace(), DqSolomonDefaultFreeSpace);

    // With no outstanding requests, a partial batch is still sent immediately.
    fixture.Write(1);
    UNIT_ASSERT_VALUES_EQUAL(fixture.Requests.size(), 1);
    fixture.AssertRequest(0, 10, 1);
    fixture.Drain();
}

void TestWriteBigBatch(bool isCloud) {
    const int batchSize = 7500;
    const TSolomonLocation location = { .ProjectId = "cloudId1", .FolderId = "folderId1", .Service = "custom", .IsCloud = isCloud };
    CleanupSolomon(location);

    TFakeCASetup setup;
    InitAsyncOutput(setup, BuildSolomonShardSettings(isCloud));

    auto issue = setup.AsyncOutputPromises->Issue.GetFuture();
    setup.AsyncOutputWrite([](NKikimr::NMiniKQL::THolderFactory& holderFactory) {
        TUnboxedValueBatch res;
        for (int i = 0; i < batchSize; i++) {
            res.emplace_back(
                CreateStruct(holderFactory, { NUdf::TUnboxedValuePod(static_cast<NUdf::TDataType<NUdf::TTimestamp>::TLayout>(i + 200000)),
                                                NKikimr::NMiniKQL::MakeString(std::to_string(i)), NUdf::TUnboxedValuePod(678) }));
        }

        return res;
    });
    UNIT_ASSERT_C(!issue.Wait(WaitTimeout), issue.GetValue().ToString());

    const auto metrics = GetSolomonMetrics(location);
    UNIT_ASSERT_VALUES_EQUAL(GetMetricsCount(metrics), batchSize);
}

void TestWriteDateTimestamp(bool isCloud) {
    const ui16 days = 19000;   // 2022-01-08
    const TInstant expected = TInstant::Days(days);
    const TSolomonLocation location = { .ProjectId = "cloudId1", .FolderId = "folderId1", .Service = "custom", .IsCloud = isCloud };
    CleanupSolomon(location);

    TFakeCASetup setup;
    InitAsyncOutput(setup, BuildSolomonShardSettings(isCloud, NUdf::TDataType<NUdf::TDate>::Id));

    auto issue = setup.AsyncOutputPromises->Issue.GetFuture();
    setup.AsyncOutputWrite([&](NKikimr::NMiniKQL::THolderFactory& holderFactory) {
        TUnboxedValueBatch res;
        res.emplace_back(
            CreateStruct(holderFactory, { NUdf::TUnboxedValuePod(days), NKikimr::NMiniKQL::MakeString("123"), NUdf::TUnboxedValuePod(678) }));
        return res;
    });
    TString metrics;
    const TInstant deadline = TInstant::Now() + WaitTimeout;
    while (GetMetricsCount(metrics = GetSolomonMetrics(location)) == 0 && !issue.HasValue() && TInstant::Now() < deadline) {
        Sleep(TDuration::MilliSeconds(50));
    }
    UNIT_ASSERT_C(!issue.HasValue(), issue.GetValue().ToString());

    NJson::TJsonValue json;
    UNIT_ASSERT_C(NJson::ReadJsonTree(metrics, &json), metrics);
    UNIT_ASSERT_VALUES_EQUAL_C(json.GetArray().size(), 1, metrics);

    const auto& ts = json.GetArray()[0]["ts"];
    if (ts.IsString()) {
        UNIT_ASSERT_VALUES_EQUAL_C(TInstant::ParseIso8601(ts.GetString()), expected, metrics);
    } else {
        UNIT_ASSERT_VALUES_EQUAL_C(TInstant::Seconds(ts.GetUIntegerRobust()), expected, metrics);
    }
}

}   // anonymous namespace

Y_UNIT_TEST_SUITE(TDqSolomonWriteActorTest) {
    Y_UNIT_TEST(OutOfOrderRepliesPreserveCheckpointBoundary) {
        for (const auto clusterType : { NSo::NProto::CT_SOLOMON, NSo::NProto::CT_MONIUM, NSo::NProto::CT_MONITORING }) {
            TBatchingFixture fixture(clusterType);
            fixture.FillSlots();
            fixture.Write(2, CreateCheckpoint(1));
            fixture.Write(3, Nothing(), true);

            fixture.Ack(2);
            fixture.AssertRequest(2, 3, 2);
            fixture.Ack(0);
            // The merged pre-checkpoint request completes before the last
            // original request. Neither the barrier nor finish can advance yet.
            fixture.Ack(1);
            UNIT_ASSERT_VALUES_EQUAL(fixture.Requests.size(), 1);
            UNIT_ASSERT(fixture.Callbacks.Checkpoints.empty());
            UNIT_ASSERT_VALUES_EQUAL(fixture.Callbacks.Finished, 0);
            fixture.AssertRequest(0, 1, 1);

            fixture.Ack(0);
            UNIT_ASSERT_VALUES_EQUAL(fixture.Callbacks.Checkpoints.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(fixture.Callbacks.Checkpoints[0], 1);
            UNIT_ASSERT_VALUES_EQUAL(fixture.Callbacks.Finished, 0);
            UNIT_ASSERT_VALUES_EQUAL(fixture.Requests.size(), 1);
            fixture.AssertRequest(0, 5, 3);
            fixture.Drain();
            UNIT_ASSERT_VALUES_EQUAL(fixture.Callbacks.Finished, 1);
            UNIT_ASSERT_VALUES_EQUAL(fixture.FreeSpace(), DqSolomonDefaultFreeSpace);
        }
    }

    Y_UNIT_TEST(FinishWaitsForAllOutOfOrderReplies) {
        TBatchingFixture fixture;
        fixture.FillSlots();
        fixture.Write(2);
        fixture.Write(3, Nothing(), true);
        fixture.Ack(2);
        fixture.AssertRequest(2, 3, 5);
        fixture.Ack(0);
        fixture.Ack(1);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Callbacks.Finished, 0);
        fixture.AssertRequest(0, 1, 1);
        fixture.Ack(0);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Callbacks.Finished, 1);
        UNIT_ASSERT(fixture.Requests.empty());
        UNIT_ASSERT_VALUES_EQUAL(fixture.FreeSpace(), DqSolomonDefaultFreeSpace);
    }

    Y_UNIT_TEST(RetryableErrorKeepsQueuedRowsUntilRetrySucceeds) {
        TBatchingFixture fixture;
        fixture.FillSlots();
        fixture.TrackRows = true;
        fixture.Write(2);
        fixture.Write(3, Nothing(), true);
        const i64 freeSpace = fixture.FreeSpace();
        const TString originalBody(fixture.Request(0).Request->Body);
        const auto sender = fixture.Requests[0]->Sender;
        fixture.Runtime.EnableScheduleForActor(sender, true);
        fixture.ExpectedErrors = 1;
        fixture.Reply(0, "500", "retry later");
        fixture.AssertError(NDqProto::StatusIds::UNSPECIFIED, "500");
        UNIT_ASSERT_VALUES_EQUAL(fixture.FreeSpace(), freeSpace);
        UNIT_ASSERT_VALUES_EQUAL(fixture.DestroyedRows, 0);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Callbacks.Finished, 0);

        fixture.Runtime.SimulateSleep(TDuration::MilliSeconds(250));
        fixture.Pump();
        UNIT_ASSERT_VALUES_EQUAL(fixture.Requests.size(), 3);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Requests[2]->Sender, sender);
        UNIT_ASSERT_VALUES_EQUAL(TString(fixture.Request(2).Request->Body), originalBody);
        fixture.AssertRequest(2, 0, 1);
        fixture.Ack(2);
        fixture.AssertRequest(2, 3, 5);
        UNIT_ASSERT_VALUES_EQUAL(fixture.DestroyedRows, 5);
        fixture.Drain();
        UNIT_ASSERT_VALUES_EQUAL(fixture.Callbacks.Finished, 1);
        UNIT_ASSERT_VALUES_EQUAL(fixture.FreeSpace(), DqSolomonDefaultFreeSpace);
    }

    Y_UNIT_TEST(TerminalHttpErrorRestoresInflightSpaceAndReportsFailure) {
        TBatchingFixture fixture;
        fixture.FillSlots();
        fixture.TrackRows = true;
        fixture.Write(10, CreateCheckpoint(1), true);
        const i64 freeSpace = fixture.FreeSpace();
        const i64 bodySize = fixture.Request(0).Request->Body.size();
        fixture.ExpectedErrors = 1;
        fixture.Reply(0, "401", "unauthorized");
        fixture.AssertError(NDqProto::StatusIds::EXTERNAL_ERROR, "401");
        UNIT_ASSERT_VALUES_EQUAL(fixture.FreeSpace(), freeSpace + bodySize);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Requests.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(fixture.DestroyedRows, 0);
        UNIT_ASSERT(fixture.Callbacks.Checkpoints.empty());
        UNIT_ASSERT_VALUES_EQUAL(fixture.Callbacks.Finished, 0);
        fixture.Stop();
        UNIT_ASSERT_VALUES_EQUAL(fixture.DestroyedRows, 10);
        fixture.Pump();
        UNIT_ASSERT_VALUES_EQUAL(fixture.Requests.size(), 2);
    }

    Y_UNIT_TEST(MalformedResponseReportsFailureWithRowsBehindCheckpoint) {
        for (const auto clusterType : { NSo::NProto::CT_SOLOMON, NSo::NProto::CT_MONIUM, NSo::NProto::CT_MONITORING }) {
            TBatchingFixture fixture(clusterType);
            fixture.FillSlots();
            fixture.Write(0, CreateCheckpoint(1));
            fixture.TrackRows = true;
            fixture.Write(10, Nothing(), true);
            const i64 freeSpace = fixture.FreeSpace();
            const i64 bodySize = fixture.Request(2).Request->Body.size();
            fixture.ExpectedErrors = 1;
            fixture.Reply(2, "200", "{invalid-json");
            fixture.AssertError(NDqProto::StatusIds::EXTERNAL_ERROR, "Invalid monitoring response");
            UNIT_ASSERT_VALUES_EQUAL(fixture.FreeSpace(), freeSpace + bodySize);
            UNIT_ASSERT_VALUES_EQUAL(fixture.Requests.size(), 2);
            UNIT_ASSERT_VALUES_EQUAL(fixture.DestroyedRows, 0);
            UNIT_ASSERT(fixture.Callbacks.Checkpoints.empty());
            UNIT_ASSERT_VALUES_EQUAL(fixture.Callbacks.Finished, 0);
            fixture.Stop();
            UNIT_ASSERT_VALUES_EQUAL(fixture.DestroyedRows, 10);
        }
    }

    Y_UNIT_TEST(CredentialsErrorReleasesEncodedBatchReservation) {
        TBatchingFixture fixture;
        fixture.FillSlots();
        fixture.TrackRows = true;
        fixture.Write(10, CreateCheckpoint(1), true);
        const i64 remainingBytes = fixture.Request(1).Request->Body.size() + fixture.Request(2).Request->Body.size();
        fixture.Credentials->Fail = true;
        fixture.ExpectedErrors = 1;
        fixture.Ack();
        fixture.AssertError(NDqProto::StatusIds::EXTERNAL_ERROR, "Failed to get auth token: Test credentials failure");
        UNIT_ASSERT_VALUES_EQUAL(fixture.FreeSpace(), DqSolomonDefaultFreeSpace - remainingBytes);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Requests.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(fixture.DestroyedRows, 10);
        UNIT_ASSERT(fixture.Callbacks.Checkpoints.empty());
        UNIT_ASSERT_VALUES_EQUAL(fixture.Callbacks.Finished, 0);
        fixture.Stop();
        fixture.Pump();
        UNIT_ASSERT_VALUES_EQUAL(fixture.DestroyedRows, 10);
    }

    Y_UNIT_TEST(EncodingErrorReportsFailureWithoutLeakingQueuedRows) {
        for (const auto clusterType : { NSo::NProto::CT_SOLOMON, NSo::NProto::CT_MONIUM, NSo::NProto::CT_MONITORING }) {
            TBatchingFixture fixture(clusterType);
            fixture.FillSlots();
            fixture.TrackRows = true;
            fixture.InvalidLabel = true;
            fixture.Write(10, CreateCheckpoint(1), true);
            const i64 remainingBytes = fixture.Request(1).Request->Body.size() + fixture.Request(2).Request->Body.size();
            fixture.ExpectedErrors = 1;
            fixture.Ack();
            fixture.AssertError(NDqProto::StatusIds::EXTERNAL_ERROR, "Error while encoding solomon metrics");
            UNIT_ASSERT_STRING_CONTAINS(fixture.Callbacks.Errors[0].ToString(), "UTF-8");
            UNIT_ASSERT_VALUES_EQUAL(fixture.FreeSpace(), DqSolomonDefaultFreeSpace - remainingBytes);
            UNIT_ASSERT_VALUES_EQUAL(fixture.Requests.size(), 2);
            UNIT_ASSERT_VALUES_EQUAL(fixture.DestroyedRows, 10);
            UNIT_ASSERT(fixture.Callbacks.Checkpoints.empty());
            UNIT_ASSERT_VALUES_EQUAL(fixture.Callbacks.Finished, 0);
            fixture.Stop();
            fixture.Pump();
            UNIT_ASSERT_VALUES_EQUAL(fixture.DestroyedRows, 10);
        }
    }

    Y_UNIT_TEST(EmptyBatchCheckpointIsSavedWithoutHttpRequests) {
        TBatchingFixture fixture;
        fixture.Write(0, CreateCheckpoint(1));
        UNIT_ASSERT(fixture.Requests.empty());
        UNIT_ASSERT_VALUES_EQUAL(fixture.Callbacks.Checkpoints.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Callbacks.Checkpoints[0], 1);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Callbacks.Finished, 0);
        UNIT_ASSERT_VALUES_EQUAL(fixture.FreeSpace(), DqSolomonDefaultFreeSpace);
    }

    Y_UNIT_TEST(ConsecutiveCheckpointsPreserveOrderWithQueuedRows) {
        TBatchingFixture fixture;
        fixture.FillSlots();
        fixture.Write(1);
        fixture.Write(0, CreateCheckpoint(1));
        fixture.Write(0, CreateCheckpoint(2));
        fixture.Write(2, Nothing(), true);
        fixture.Ack(2);
        fixture.AssertRequest(2, 3, 1);
        fixture.Ack(0);
        fixture.Ack(0);
        UNIT_ASSERT(fixture.Callbacks.Checkpoints.empty());
        UNIT_ASSERT_VALUES_EQUAL(fixture.Requests.size(), 1);
        fixture.Ack(0);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Callbacks.Checkpoints.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Callbacks.Checkpoints[0], 1);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Callbacks.Checkpoints[1], 2);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Requests.size(), 1);
        fixture.AssertRequest(0, 4, 2);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Callbacks.Finished, 0);
        fixture.Drain();
        UNIT_ASSERT_VALUES_EQUAL(fixture.Callbacks.Finished, 1);
        UNIT_ASSERT_VALUES_EQUAL(fixture.FreeSpace(), DqSolomonDefaultFreeSpace);
    }

    Y_UNIT_TEST(CheckpointAndFinishWaitForAllPendingRequests) {
        for (const auto clusterType : { NSo::NProto::CT_SOLOMON, NSo::NProto::CT_MONIUM, NSo::NProto::CT_MONITORING }) {
            for (const ui32 rows : { 0U, 2U, 1200U }) {
                TBatchingFixture fixture(clusterType);
                fixture.FillSlots();
                fixture.Write(rows, CreateCheckpoint(1), true);
                fixture.Ack(2);
                fixture.Ack(0);
                UNIT_ASSERT(fixture.Callbacks.Checkpoints.empty());
                UNIT_ASSERT_VALUES_EQUAL(fixture.Callbacks.Finished, 0);
                fixture.Ack(0);
                if (rows) {
                    UNIT_ASSERT(fixture.Callbacks.Checkpoints.empty());
                    UNIT_ASSERT_VALUES_EQUAL(fixture.Callbacks.Finished, 0);
                    fixture.AssertRequest(0, 3, Min<ui32>(rows, 1000));
                    if (rows > 1000) {
                        fixture.AssertRequest(1, 1003, rows - 1000);
                    }
                }
                fixture.Drain();
                UNIT_ASSERT_VALUES_EQUAL(fixture.Callbacks.Checkpoints.size(), 1);
                UNIT_ASSERT_VALUES_EQUAL(fixture.Callbacks.Checkpoints[0], 1);
                UNIT_ASSERT_VALUES_EQUAL(fixture.Callbacks.Finished, 1);
                UNIT_ASSERT_VALUES_EQUAL(fixture.FreeSpace(), DqSolomonDefaultFreeSpace);
            }
        }
    }

    Y_UNIT_TEST(MergedRequestsDoNotSplitRowsAtNonDivisibleMetricLimit) {
        for (const auto clusterType : { NSo::NProto::CT_SOLOMON, NSo::NProto::CT_MONIUM, NSo::NProto::CT_MONITORING }) {
            TBatchingFixture fixture(clusterType, 3);
            fixture.FillSlots();
            fixture.Write(200);
            fixture.Write(200);
            fixture.Ack();
            fixture.AssertRequest(2, 9, 999);
            fixture.Ack();
            fixture.AssertRequest(2, 1008, 201);
            fixture.Drain();
            UNIT_ASSERT_VALUES_EQUAL(fixture.FreeSpace(), DqSolomonDefaultFreeSpace);
        }
    }

    Y_UNIT_TEST(StopReleasesPartiallyConsumedBatch) {
        for (const auto clusterType : { NSo::NProto::CT_SOLOMON, NSo::NProto::CT_MONIUM, NSo::NProto::CT_MONITORING }) {
            TBatchingFixture fixture(clusterType);
            fixture.FillSlots();
            fixture.TrackRows = true;
            fixture.Write(1200);
            UNIT_ASSERT_VALUES_EQUAL(fixture.DestroyedRows, 0);
            fixture.Ack();
            fixture.AssertRequest(2, 3, 1000);
            UNIT_ASSERT_VALUES_EQUAL(fixture.DestroyedRows, 1000);
            fixture.Stop();
            UNIT_ASSERT_VALUES_EQUAL(fixture.DestroyedRows, 1200);
            fixture.Pump();
            UNIT_ASSERT_VALUES_EQUAL(fixture.Requests.size(), 3);
            UNIT_ASSERT_VALUES_EQUAL(fixture.Callbacks.Finished, 0);
            UNIT_ASSERT_VALUES_EQUAL(fixture.DestroyedRows, 1200);
        }
    }

    Y_UNIT_TEST(QueuedWritesAreMergedSolomon) {
        TestQueuedWritesAreMerged(NSo::NProto::CT_SOLOMON);
    }

    Y_UNIT_TEST(QueuedWritesAreMergedMonium) {
        TestQueuedWritesAreMerged(NSo::NProto::CT_MONIUM);
    }

    Y_UNIT_TEST(QueuedWritesAreMergedMonitoring) {
        TestQueuedWritesAreMerged(NSo::NProto::CT_MONITORING);
    }

    Y_UNIT_TEST(MergedRequestsPreserveUnsignedValues) {
        for (const auto clusterType : { NSo::NProto::CT_SOLOMON, NSo::NProto::CT_MONIUM, NSo::NProto::CT_MONITORING }) {
            TBatchingFixture fixture(clusterType);
            fixture.FillSlots();
            fixture.ValueOffset = Max<ui64>() - fixture.NextRow - 1;
            fixture.Write(1);
            fixture.Write(1);
            fixture.Ack();
            fixture.AssertRequest(2, Max<ui64>() - 1, 2);
            fixture.Drain();
        }
    }

    Y_UNIT_TEST(MergedRequestsRespectMetricLimit) {
        TBatchingFixture fixture(NSo::NProto::CT_SOLOMON, 2);
        fixture.FillSlots();
        fixture.Write(300);
        fixture.Write(200);
        fixture.Write(1);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Requests.size(), 3);
        fixture.Ack();
        UNIT_ASSERT_VALUES_EQUAL(fixture.Requests.size(), 3);
        fixture.AssertRequest(2, 6, 1000);
        fixture.Ack();
        UNIT_ASSERT_VALUES_EQUAL(fixture.Requests.size(), 3);
        fixture.AssertRequest(2, 1006, 2);
        fixture.Drain();
        UNIT_ASSERT_VALUES_EQUAL(fixture.FreeSpace(), DqSolomonDefaultFreeSpace);
    }

    Y_UNIT_TEST(MaxSensorCountRespectsMetricLimit) {
        constexpr ui32 sensorsPerRow = NSo::NConstants::MaxSensorsPerRow;
        constexpr ui32 rowsPerRequest = 1000 / sensorsPerRow;
        constexpr ui64 metricsPerRequest = rowsPerRequest * sensorsPerRow;
        constexpr ui64 firstValue = 3 * sensorsPerRow;
        for (const auto clusterType : { NSo::NProto::CT_SOLOMON, NSo::NProto::CT_MONIUM, NSo::NProto::CT_MONITORING }) {
            TBatchingFixture fixture(clusterType, sensorsPerRow);
            fixture.FillSlots();
            fixture.Write(rowsPerRequest + 1);
            UNIT_ASSERT_VALUES_EQUAL(fixture.Requests.size(), 3);
            fixture.Ack();
            UNIT_ASSERT_VALUES_EQUAL(fixture.Requests.size(), 3);
            fixture.AssertRequest(2, firstValue, metricsPerRequest);
            fixture.Ack();
            UNIT_ASSERT_VALUES_EQUAL(fixture.Requests.size(), 3);
            fixture.AssertRequest(2, firstValue + metricsPerRequest, sensorsPerRow);
            fixture.Drain();
            UNIT_ASSERT_VALUES_EQUAL(fixture.FreeSpace(), DqSolomonDefaultFreeSpace);
        }
    }

    Y_UNIT_TEST(PartialInputBatchIsKeptForNextRequest) {
        for (const auto clusterType : { NSo::NProto::CT_SOLOMON, NSo::NProto::CT_MONIUM, NSo::NProto::CT_MONITORING }) {
            TBatchingFixture fixture(clusterType);
            fixture.FillSlots();
            fixture.Write(600);
            fixture.Write(600);
            fixture.Ack();
            fixture.AssertRequest(2, 3, 1000);
            fixture.Ack();
            fixture.AssertRequest(2, 1003, 200);
            fixture.Drain();
            UNIT_ASSERT_VALUES_EQUAL(fixture.FreeSpace(), DqSolomonDefaultFreeSpace);
        }
    }

    Y_UNIT_TEST(LargeInputBatchIsDrainedAcrossReplies) {
        TBatchingFixture fixture;
        fixture.FillSlots();
        fixture.Write(2505);
        fixture.Ack();
        fixture.AssertRequest(2, 3, 1000);
        fixture.Ack();
        fixture.AssertRequest(2, 1003, 1000);
        fixture.Ack();
        fixture.AssertRequest(2, 2003, 505);
        fixture.Drain();
        UNIT_ASSERT_VALUES_EQUAL(fixture.FreeSpace(), DqSolomonDefaultFreeSpace);
    }

    Y_UNIT_TEST(StopWithPendingBatches) {
        TBatchingFixture fixture;
        fixture.FillSlots();
        fixture.TrackRows = true;
        fixture.Write(10);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Requests.size(), 3);
        UNIT_ASSERT_VALUES_EQUAL(fixture.DestroyedRows, 0);
        fixture.Stop();
        UNIT_ASSERT_VALUES_EQUAL(fixture.DestroyedRows, 10);
        fixture.Pump();
        UNIT_ASSERT_VALUES_EQUAL(fixture.Callbacks.Finished, 0);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Requests.size(), 3);
        UNIT_ASSERT_VALUES_EQUAL(fixture.DestroyedRows, 10);
    }

    Y_UNIT_TEST(OmittedInputSizeStillAppliesBackpressure) {
        constexpr i64 budget = 512;
        TBatchingFixture fixture(NSo::NProto::CT_SOLOMON, 1, budget);
        fixture.FillSlots();
        for (ui32 i = 0; i < 10; ++i) {
            fixture.Write(1, Nothing(), false, 0);
        }
        UNIT_ASSERT(fixture.FreeSpace() < 0);
        fixture.Ack();
        fixture.AssertRequest(2, 3, 10);
        fixture.Drain();
        UNIT_ASSERT_VALUES_EQUAL(fixture.FreeSpace(), budget);
    }

    Y_UNIT_TEST(MergingStopsAtCheckpoint) {
        TBatchingFixture fixture;
        fixture.FillSlots();
        fixture.Write(1);
        fixture.Write(1, CreateCheckpoint(1));
        fixture.Write(3);
        fixture.Ack();
        UNIT_ASSERT_VALUES_EQUAL(fixture.Requests.size(), 3);
        fixture.AssertRequest(2, 3, 2);
        fixture.Ack();
        fixture.Ack();
        UNIT_ASSERT(fixture.Callbacks.Checkpoints.empty());
        UNIT_ASSERT_VALUES_EQUAL(fixture.Requests.size(), 1);
        fixture.Ack();
        UNIT_ASSERT_VALUES_EQUAL(fixture.Callbacks.Checkpoints.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Callbacks.Checkpoints[0], 1);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Requests.size(), 1);
        fixture.AssertRequest(0, 5, 3);
        fixture.Drain();
        UNIT_ASSERT_VALUES_EQUAL(fixture.FreeSpace(), DqSolomonDefaultFreeSpace);
    }

    Y_UNIT_TEST(FinishWaitsForMergedRequest) {
        TBatchingFixture fixture;
        fixture.FillSlots();
        fixture.Write(2);
        fixture.Write(3, Nothing(), true);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Callbacks.Finished, 0);
        fixture.Ack();
        fixture.AssertRequest(2, 3, 5);
        fixture.Ack();
        fixture.Ack();
        UNIT_ASSERT_VALUES_EQUAL(fixture.Callbacks.Finished, 0);
        fixture.Ack();
        UNIT_ASSERT_VALUES_EQUAL(fixture.Callbacks.Finished, 1);
        UNIT_ASSERT(fixture.Requests.empty());
        UNIT_ASSERT_VALUES_EQUAL(fixture.FreeSpace(), DqSolomonDefaultFreeSpace);
    }

    Y_UNIT_TEST(MergingPreservesBackpressureAccounting) {
        constexpr i64 budget = 512;
        TBatchingFixture fixture(NSo::NProto::CT_SOLOMON, 1, budget);
        fixture.FillSlots();
        const i64 beforeQueue = fixture.FreeSpace();
        const i64 acknowledgedBytes = fixture.Request(0).Request->Body.size();
        fixture.SameLabels = true;
        for (ui32 i = 0; i < 10; ++i) {
            fixture.Write(1);
        }
        UNIT_ASSERT(fixture.FreeSpace() < 0);
        fixture.Ack();
        fixture.AssertRequest(2, 3, 10);
        const i64 mergedBytes = fixture.Request(2).Request->Body.size();
        UNIT_ASSERT_VALUES_EQUAL(fixture.FreeSpace(), beforeQueue + acknowledgedBytes - mergedBytes);
        fixture.Drain();
        UNIT_ASSERT_VALUES_EQUAL(fixture.FreeSpace(), budget);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Callbacks.Resumed, 1);
    }

    Y_UNIT_TEST(BackpressureNotificationWaitsForBatchEncoding) {
        for (const auto clusterType : { NSo::NProto::CT_SOLOMON, NSo::NProto::CT_MONIUM, NSo::NProto::CT_MONITORING }) {
            for (const ui32 rows : { 10U, 2505U }) {
                const i64 budget = Max<i64>(512, rows * 50);
                TBatchingFixture fixture(clusterType, 1, budget);
                fixture.FillSlots();
                const i64 acknowledgedBytes = fixture.Request(0).Request->Body.size();
                // The first ACK temporarily makes FreeSpace positive. Encoding
                // the queued rows consumes it again, including when only part
                // of a large input batch fits into the next request.
                const i64 dataSize = fixture.FreeSpace() + acknowledgedBytes / 2;
                UNIT_ASSERT(dataSize > 0);
                fixture.Write(rows, Nothing(), false, dataSize);
                UNIT_ASSERT(fixture.FreeSpace() < 0);
                UNIT_ASSERT_VALUES_EQUAL(fixture.Callbacks.Resumed, 0);

                fixture.Ack();
                UNIT_ASSERT_VALUES_EQUAL(fixture.Requests.size(), 3);
                fixture.AssertRequest(2, 3, Min<ui32>(rows, 1000));
                UNIT_ASSERT(fixture.FreeSpace() < 0);
                // Do not wake the producer while the new request still keeps
                // it blocked; a later ACK must deliver the actual notification.
                UNIT_ASSERT_VALUES_EQUAL(fixture.Callbacks.Resumed, 0);

                fixture.Drain();
                UNIT_ASSERT_VALUES_EQUAL(fixture.FreeSpace(), budget);
                UNIT_ASSERT_VALUES_EQUAL(fixture.Callbacks.Resumed, 1);

                fixture.Write(1);
                UNIT_ASSERT_VALUES_EQUAL(fixture.Requests.size(), 1);
                fixture.AssertRequest(0, 3 + rows, 1);
                fixture.Drain();
                UNIT_ASSERT_VALUES_EQUAL(fixture.FreeSpace(), budget);
                UNIT_ASSERT_VALUES_EQUAL(fixture.Callbacks.Resumed, 1);
            }
        }
    }

    Y_UNIT_TEST(TestWriteDateTimestampMonitoring) {
        TestWriteDateTimestamp(true);
    }

    Y_UNIT_TEST(TestWriteDateTimestampSolomon) {
        TestWriteDateTimestamp(false);
    }

    Y_UNIT_TEST(TestWriteFormat) {
        const TSolomonLocation location = { .ProjectId = "cloudId1", .FolderId = "folderId1", .Service = "custom", .IsCloud = true };
        CleanupSolomon(location);

        TFakeCASetup setup;
        InitAsyncOutput(setup, BuildSolomonShardSettings(true));

        auto issue = setup.AsyncOutputPromises->Issue.GetFuture();
        setup.AsyncOutputWrite([](NKikimr::NMiniKQL::THolderFactory& holderFactory) {
            TUnboxedValueBatch res;
            res.emplace_back(
                CreateStruct(holderFactory, { NUdf::TUnboxedValuePod(static_cast<NUdf::TDataType<NUdf::TTimestamp>::TLayout>(1624811684)),
                                                NKikimr::NMiniKQL::MakeString("123"), NUdf::TUnboxedValuePod(678) }));
            return res;
        });
        UNIT_ASSERT_C(!issue.Wait(WaitTimeout), issue.GetValue().ToString());

        const auto metrics = GetSolomonMetrics(location);
        const auto expected = R"([
  {
    "labels": [
      [
        "label1",
        "123"
      ],
      [
        "name",
        "sensor1"
      ]
    ],
    "ts": "1970-01-01T00:27:04.811684Z",
    "value": 678
  }
])";
        UNIT_ASSERT_STRINGS_EQUAL(metrics, expected);
    }

    Y_UNIT_TEST(TestWriteBigBatchMonitoring) {
        TestWriteBigBatch(true);
    }

    Y_UNIT_TEST(TestWriteBigBatchSolomon) {
        TestWriteBigBatch(false);
    }

    Y_UNIT_TEST(TestWriteWithTimeseries) {
        const int batchSize = 10;
        const TSolomonLocation location = { .ProjectId = "cloudId1", .FolderId = "folderId1", .Service = "custom", .IsCloud = true };
        CleanupSolomon(location);

        TFakeCASetup setup;
        InitAsyncOutput(setup, BuildSolomonShardSettings(true));

        auto issue = setup.AsyncOutputPromises->Issue.GetFuture();
        setup.AsyncOutputWrite([](NKikimr::NMiniKQL::THolderFactory& holderFactory) {
            TUnboxedValueBatch res;

            for (int i = 0; i < batchSize; i++) {
                res.emplace_back(
                    CreateStruct(holderFactory, { NUdf::TUnboxedValuePod(static_cast<NUdf::TDataType<NUdf::TTimestamp>::TLayout>(i + 200000)),
                                                    NKikimr::NMiniKQL::MakeString("123"), NUdf::TUnboxedValuePod(678) }));
            }

            return res;
        });
        UNIT_ASSERT_C(!issue.Wait(WaitTimeout), issue.GetValue().ToString());

        const auto metrics = GetSolomonMetrics(location);
        UNIT_ASSERT_VALUES_EQUAL(GetMetricsCount(metrics), batchSize);
    }

    Y_UNIT_TEST(TestCheckpoints) {
        const int batchSize = 2400;
        TFakeCASetup setup;

        const TSolomonLocation location = { .ProjectId = "cloudId1", .FolderId = "folderId1", .Service = "custom", .IsCloud = true };
        CleanupSolomon(location);
        InitAsyncOutput(setup, BuildSolomonShardSettings(true));

        auto stateSaved = setup.AsyncOutputPromises->StateSaved.GetFuture();
        setup.AsyncOutputWrite([](NKikimr::NMiniKQL::THolderFactory& holderFactory) {
            TUnboxedValueBatch res;

            for (int i = 0; i < batchSize; i++) {
                res.emplace_back(
                    CreateStruct(holderFactory, { NUdf::TUnboxedValuePod(static_cast<NUdf::TDataType<NUdf::TTimestamp>::TLayout>(i + 200000)),
                                                    NKikimr::NMiniKQL::MakeString(std::to_string(i)), NUdf::TUnboxedValuePod(678) }));
            }

            return res;
        }, CreateCheckpoint(1));
        UNIT_ASSERT(stateSaved.Wait(WaitTimeout));

        const auto metrics = GetSolomonMetrics(location);
        UNIT_ASSERT_VALUES_EQUAL(GetMetricsCount(metrics), batchSize);
    }

    Y_UNIT_TEST(TestShouldReturnAfterCheckpoint) {
        TFakeCASetup setup;

        const TSolomonLocation location = { .ProjectId = "cloudId1", .FolderId = "folderId1", .Service = "custom", .IsCloud = true };
        CleanupSolomon(location);
        InitAsyncOutput(setup, BuildSolomonShardSettings(true));

        auto stateSaved = setup.AsyncOutputPromises->StateSaved.GetFuture();
        setup.AsyncOutputWrite([](NKikimr::NMiniKQL::THolderFactory& holderFactory) {
            TUnboxedValueBatch res;
            res.emplace_back(CreateStruct(holderFactory, { NUdf::TUnboxedValuePod(static_cast<NUdf::TDataType<NUdf::TTimestamp>::TLayout>(
                                                               200000)), NKikimr::NMiniKQL::MakeString("abc"), NUdf::TUnboxedValuePod(678) }));
            return res;
        }, CreateCheckpoint(1));
        UNIT_ASSERT(stateSaved.Wait(WaitTimeout));

        auto issue = setup.AsyncOutputPromises->Issue.GetFuture();
        setup.AsyncOutputWrite([](NKikimr::NMiniKQL::THolderFactory& holderFactory) {
            TUnboxedValueBatch res;
            res.emplace_back(CreateStruct(holderFactory, { NUdf::TUnboxedValuePod(static_cast<NUdf::TDataType<NUdf::TTimestamp>::TLayout>(
                                                               200001)), NKikimr::NMiniKQL::MakeString("cba"), NUdf::TUnboxedValuePod(678) }));
            return res;
        });
        UNIT_ASSERT_C(!issue.Wait(WaitTimeout), issue.GetValue().ToString());

        const auto metrics = GetSolomonMetrics(location);
        UNIT_ASSERT_VALUES_EQUAL(GetMetricsCount(metrics), 2);
    }
}

}   // namespace NYql::NDq
