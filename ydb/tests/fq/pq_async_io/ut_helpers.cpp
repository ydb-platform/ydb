#include "ut_helpers.h"

#include <ydb/library/yql/minikql/mkql_string_util.h>

#include <ydb/core/testlib/basics/appdata.h>

#include <util/system/env.h>

#include <condition_variable>
#include <thread>

namespace NYql::NDq {

using namespace NActors;

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
    if (watermarksPeriod) {
        settings.MutableWatermarks()->SetEnabled(true);
        settings.MutableWatermarks()->SetGranularityUs(watermarksPeriod->MicroSeconds());
    }
    settings.MutableWatermarks()->SetIdlePartitionsEnabled(idlePartitionsEnabled);
    settings.MutableWatermarks()->SetLateArrivalDelayUs(lateArrivalDelay.MicroSeconds());

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
}

TPqIoTestFixture::~TPqIoTestFixture() {
    CaSetup = nullptr;
    Driver.Stop(true);
}

void TPqIoTestFixture::InitSource(
    NYql::NPq::NProto::TDqPqTopicSource&& settings,
    i64 freeSpace)
{
    CaSetup->Execute([&](TFakeActor& actor) {
        NPq::NProto::TDqReadTaskParams params;
        auto* partitioninigParams = params.MutablePartitioningParams();
        partitioninigParams->SetTopicPartitionsCount(1);
        partitioninigParams->SetEachTopicPartitionGroupId(0);
        partitioninigParams->SetDqPartitionsCount(1);

        TString serializedParams;
        Y_PROTOBUF_SUPPRESS_NODISCARD params.SerializeToString(&serializedParams);

        const THashMap<TString, TString> secureParams;
        const THashMap<TString, TString> taskParams { {"pq", serializedParams} };

        auto [dqSource, dqSourceAsActor] = CreateDqPqReadActor(
            std::move(settings),
            0,
            NYql::NDq::TCollectStatsLevel::None,
            "query_1",
            0,
            secureParams,
            taskParams,
            Driver,
            nullptr,
            actor.SelfId(),
            actor.GetHolderFactory(),
            MakeIntrusive<NMonitoring::TDynamicCounters>(),
            freeSpace);

        actor.InitAsyncInput(dqSource, dqSourceAsActor);
    });
}

void TPqIoTestFixture::InitRdSource(
    NYql::NPq::NProto::TDqPqTopicSource&& settings,
    i64 freeSpace)
{
    CaSetup->Execute([&](TFakeActor& actor) {
        NPq::NProto::TDqReadTaskParams params;
        auto* partitioninigParams = params.MutablePartitioningParams();
        partitioninigParams->SetTopicPartitionsCount(1);
        partitioninigParams->SetEachTopicPartitionGroupId(0);
        partitioninigParams->SetDqPartitionsCount(1);

        TString serializedParams;
        Y_PROTOBUF_SUPPRESS_NODISCARD params.SerializeToString(&serializedParams);

        const THashMap<TString, TString> secureParams;
        const THashMap<TString, TString> taskParams { {"pq", serializedParams} };

        auto [dqSource, dqSourceAsActor] = CreateDqPqRdReadActor(
            std::move(settings),
            0,
            NYql::NDq::TCollectStatsLevel::None,
            "query_1",
            0,
            secureParams,
            taskParams,
            nullptr,                // credentialsFactory
            actor.SelfId(),         // computeActorId
            actor.GetHolderFactory(),
            //MakeIntrusive<NMonitoring::TDynamicCounters>(), // TODO
            freeSpace);

        actor.InitAsyncInput(dqSource, dqSourceAsActor);
    });
}

void TPqIoTestFixture::InitAsyncOutput(
    NPq::NProto::TDqPqTopicSink&& settings,
    i64 freeSpace)
{
    const THashMap<TString, TString> secureParams;

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
            freeSpace);

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
    const std::vector<TString>& sequence,
    const TString& topic,
    const TString& endpoint)
{
    NYdb::TDriverConfig cfg;
    cfg.SetEndpoint(endpoint);
    cfg.SetDatabase(GetDefaultPqDatabase());
    cfg.SetLog(CreateLogBackend("cerr"));
    NYdb::TDriver driver(cfg);
    NYdb::NPersQueue::TPersQueueClient client(driver);
    NYdb::NPersQueue::TWriteSessionSettings sessionSettings;
    sessionSettings
        .Path(topic)
        .MessageGroupId("src_id")
        .Codec(NYdb::NPersQueue::ECodec::RAW);
    auto session = client.CreateSimpleBlockingWriteSession(sessionSettings);
    for (const TString& data : sequence) {
        UNIT_ASSERT_C(session->Write(data), "Failed to write message with body \"" << data << "\" to topic " << topic);
        Cerr << "Message '" << data << "' was written into topic '" << topic << "'" << Endl;
    }
    session->Close(); // Wait until all data would be written into PQ.
    session = nullptr;
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
    cfg.SetLog(CreateLogBackend("cerr"));
    NYdb::TDriver driver(cfg);
    NYdb::NPersQueue::TPersQueueClient client(driver);
    NYdb::NPersQueue::TReadSessionSettings sessionSettings;
    sessionSettings
        .AppendTopics(topic)
        .ConsumerName(DefaultPqConsumer)
        .DisableClusterDiscovery(true);

    auto promise = NThreading::NewPromise();
    std::vector<TString> result;

    sessionSettings.EventHandlers_.SimpleDataHandlers([&](NYdb::NPersQueue::TReadSessionEvent::TDataReceivedEvent& ev) {
        for (const auto& message : ev.GetMessages()) {
            result.emplace_back(message.GetData());
        }
        if (result.size() >= size) {
            promise.SetValue();
        }
    }, false, false);

    std::shared_ptr<NYdb::NPersQueue::IReadSession> session = client.CreateReadSession(sessionSettings);
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
    cfg.SetLog(CreateLogBackend("cerr"));
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
    NYdb::NPersQueue::TPersQueueClient client(driver);

    auto result = client.AddReadRule(
            streamName,
            NYdb::NPersQueue::TAddReadRuleSettings()
                .ReadRule(
                    NYdb::NPersQueue::TReadRuleSettings()
                        .ConsumerName(DefaultPqConsumer)
                        .ServiceType("yandex-query")
                        .SupportedCodecs({
                            NYdb::NPersQueue::ECodec::RAW
                        })
                )
        ).ExtractValueSync();
    UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), NYdb::EStatus::SUCCESS, result.GetIssues().ToString());
    UNIT_ASSERT_VALUES_EQUAL(result.IsTransportError(), false);
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
            unboxedValueForData[0] = NKikimr::NMiniKQL::MakeString(NUdf::TStringRef(item.Data(), item.Size()));
        }

        return batch;
    }, checkpoint);
}
}
