#include "ut_helpers.h"

#include <ydb/core/base/backtrace.h>
#include <ydb/core/testlib/basics/appdata.h>
#include <ydb/library/testlib/common/test_utils.h>
#include <ydb/library/yql/providers/pq/gateway/native/yql_pq_gateway.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/datastreams/datastreams.h>

#include <yql/essentials/minikql/mkql_string_util.h>
#include <yql/essentials/providers/common/proto/gateways_config.pb.h>

#include <util/system/env.h>

namespace NYql::NDq {

NYql::NPq::NProto::TDqPqTopicSource BuildPqTopicSourceSettings(
    TString topic,
    TMaybe<TDuration> watermarksPeriod,
    TDuration lateArrivalDelay,
    bool idlePartitionsEnabled)
{
    NYql::NPq::NProto::TDqPqTopicSource settings;
    settings.SetTopicPath(topic);
    settings.SetConsumerName(DefaultPqConsumer);
    settings.SetEndpoint(GetDefaultPqEndpoint());
    settings.MutableToken()->SetName("token");
    settings.SetDatabase(GetDefaultPqDatabase());
    settings.SetRowType("[StructType; [[dt; [DataType; Uint64]]; [value; [DataType; String]]]]");
    settings.AddColumns("dt");
    settings.AddColumns("value");
    settings.AddColumnTypes("[DataType; Uint64]");
    settings.AddColumnTypes("[DataType; String]");
    if (watermarksPeriod) {
        settings.MutableWatermarks()->SetEnabled(true);
        settings.MutableWatermarks()->SetGranularityUs(watermarksPeriod->MicroSeconds());
    }
    settings.MutableWatermarks()->SetIdlePartitionsEnabled(idlePartitionsEnabled);
    settings.MutableWatermarks()->SetLateArrivalDelayUs(lateArrivalDelay.MicroSeconds());

    auto* disposition = settings.mutable_disposition()->mutable_from_time()->mutable_timestamp();
    disposition->set_seconds(0);
    disposition->set_nanos(0);

    return settings;
}

NYql::NPq::NProto::TDqPqTopicSink BuildPqTopicSinkSettings(TString topic) {
    NYql::NPq::NProto::TDqPqTopicSink settings;
    settings.SetTopicPath(topic);
    settings.SetEndpoint(GetDefaultPqEndpoint());
    settings.SetDatabase(GetDefaultPqDatabase());
    settings.SetClusterType(NPq::NProto::DataStreams);
    settings.MutableToken()->SetName("token");

    return settings;
}

TPqIoTestFixture::TPqIoTestFixture() {
    NTestUtils::SetupSignalHandlers();
}

TPqIoTestFixture::~TPqIoTestFixture() {
    CaSetup = nullptr;
    Driver.Stop(true);
}

void TPqIoTestFixture::InitAsyncOutput(
    NPq::NProto::TDqPqTopicSink&& settings,
    i64 freeSpace)
{
    const THashMap<TString, TString> secureParams;

    TPqGatewayServices pqServices(
            Driver,
            nullptr,
            nullptr,
            std::make_shared<TPqGatewayConfig>(),
            nullptr
        );

    CaSetup->Execute([&](TFakeActor& actor) {
        auto [dqAsyncOutput, dqAsyncOutputAsActor] = CreateDqPqWriteActor(
            std::move(settings),
            0,
            NYql::NDq::TCollectStatsLevel::None,
            "query_1",
            0,
            secureParams,
            Driver,
            nullptr,
            &actor.GetAsyncOutputCallbacks(),
            MakeIntrusive<NMonitoring::TDynamicCounters>(),
            CreatePqNativeGateway(std::move(pqServices)),
            true,
            freeSpace,
            true);

        actor.InitAsyncOutput(dqAsyncOutput, dqAsyncOutputAsActor);
    });
}

TString GetDefaultPqEndpoint() {
    auto endpoint = GetEnv("YDB_ENDPOINT");
    UNIT_ASSERT_C(endpoint, "Yds recipe is expected");
    return endpoint;
}

TString GetDefaultPqDatabase() {
    auto database = GetEnv("YDB_DATABASE");
    UNIT_ASSERT_C(database, "Yds recipe is expected");
    return database;
}

extern const TString DefaultPqConsumer = "test_client";

void PQWrite(
    const std::vector<TString>& messages,
    const TString& topic,
    const TString& endpoint)
{
    auto config = NYdb::TDriverConfig()
        .SetEndpoint(endpoint)
        .SetDatabase(GetDefaultPqDatabase())
        .SetLog(std::unique_ptr<TLogBackend>(CreateLogBackend("cerr").Release()));
    NYdb::TDriver driver(config);
    NYdb::NTopic::TTopicClient client(driver);

    auto settings = NYdb::NTopic::TWriteSessionSettings()
        .Path(topic)
        .MessageGroupId("src_id")
        .Codec(NYdb::NTopic::ECodec::RAW);
    auto session = client.CreateSimpleBlockingWriteSession(settings);
    for (const auto& message : messages) {
        UNIT_ASSERT_C(session->Write(message), "Failed to write message with body \"" << message << "\" to topic " << topic);
        Cerr << "Message '" << message << "' was written into topic '" << topic << "'" << Endl;
    }

    session->Close(); // Wait until all data would be written into PQ.
    driver.Stop(true);
}

std::vector<TString> PQReadUntil(
    const TString& topic,
    ui64 size,
    const TString& endpoint,
    TDuration timeout)
{
    NYdb::TDriverConfig cfg;
    cfg.SetEndpoint(endpoint);
    cfg.SetDatabase(GetDefaultPqDatabase());
    cfg.SetLog(std::unique_ptr<TLogBackend>(CreateLogBackend("cerr").Release()));
    NYdb::TDriver driver(cfg);
    NYdb::NTopic::TTopicClient client(driver);
    NYdb::NTopic::TReadSessionSettings sessionSettings;
    sessionSettings
        .AppendTopics(std::string{topic})
        .ConsumerName(DefaultPqConsumer);

    auto promise = NThreading::NewPromise();
    std::vector<TString> result;

    sessionSettings.EventHandlers_.SimpleDataHandlers([&](NYdb::NTopic::TReadSessionEvent::TDataReceivedEvent& ev) {
        for (const auto& message : ev.GetMessages()) {
            result.emplace_back(message.GetData());
        }
        if (result.size() >= size) {
            promise.SetValue();
        }
    }, false, false);

    std::shared_ptr<NYdb::NTopic::IReadSession> session = client.CreateReadSession(sessionSettings);
    UNIT_ASSERT(promise.GetFuture().Wait(timeout));
    session->Close(TDuration::Zero());
    session = nullptr;
    driver.Stop(true);
    return result;
}

void PQCreateStream(const TString& streamName)
{
    NYdb::TDriverConfig cfg;
    cfg.SetEndpoint(GetDefaultPqEndpoint());
    cfg.SetDatabase(GetDefaultPqDatabase());
    cfg.SetLog(std::unique_ptr<TLogBackend>(CreateLogBackend("cerr").Release()));
    NYdb::TDriver driver(cfg);

    NYdb::NDataStreams::V1::TDataStreamsClient client = NYdb::NDataStreams::V1::TDataStreamsClient(
        driver,
        NYdb::TCommonClientSettings().Database(GetDefaultPqDatabase()));

    auto result = client.CreateStream(streamName,
        NYdb::NDataStreams::V1::TCreateStreamSettings().ShardCount(1).RetentionPeriodHours(1)).ExtractValueSync();
    UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), NYdb::EStatus::SUCCESS, result.GetIssues().ToString());
    UNIT_ASSERT_VALUES_EQUAL(result.IsTransportError(), false);

    AddReadRule(driver, streamName);
    driver.Stop(true);
}

void AddReadRule(NYdb::TDriver& driver, const TString& streamName) {
    NYdb::NTopic::TTopicClient client(driver);

   auto alterTopicSettings =
        NYdb::NTopic::TAlterTopicSettings()
            .BeginAddConsumer(DefaultPqConsumer)
            .SetSupportedCodecs(
                {
                    NYdb::NTopic::ECodec::RAW
                })
            .EndAddConsumer();
    auto result = client.AlterTopic(streamName, alterTopicSettings).ExtractValueSync();

    UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), NYdb::EStatus::SUCCESS, result.GetIssues().ToString());
    UNIT_ASSERT_VALUES_EQUAL(result.IsTransportError(), false);
}

std::vector<TMessage> UVPairParser(const NUdf::TUnboxedValue& item) {
    UNIT_ASSERT_VALUES_EQUAL(item.GetListLength(), 2);
    auto stringElement = item.GetElement(1);
    return { {item.GetElement(0).Get<ui64>(), TString(stringElement.AsStringRef())} };
}

std::vector<TString> UVParser(const NUdf::TUnboxedValue& item) {
    return { TString(item.AsStringRef()) };
}

void TPqIoTestFixture::AsyncOutputWrite(std::vector<TString> data, TMaybe<NDqProto::TCheckpoint> checkpoint) {
    CaSetup->AsyncOutputWrite([data](NKikimr::NMiniKQL::THolderFactory& factory) {
        NKikimr::NMiniKQL::TUnboxedValueBatch batch;
        for (const auto& item : data) {
            NUdf::TUnboxedValue* unboxedValueForData = nullptr;
            batch.emplace_back(factory.CreateDirectArrayHolder(1, unboxedValueForData));
            unboxedValueForData[0] = NKikimr::NMiniKQL::MakeString(NUdf::TStringRef(item.data(), item.size()));
        }

        return batch;
    }, checkpoint);
}
}
