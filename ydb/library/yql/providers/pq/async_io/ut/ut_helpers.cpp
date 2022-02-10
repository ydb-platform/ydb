#include "ut_helpers.h" 
 
#include <ydb/library/yql/minikql/mkql_string_util.h>
 
#include <ydb/core/testlib/basics/appdata.h>
 
#include <util/system/env.h> 
 
#include <condition_variable> 
#include <thread> 
 
namespace NYql::NDq { 
 
using namespace NActors; 
 
NYql::NPq::NProto::TDqPqTopicSource BuildPqTopicSourceSettings(TString topic) { 
    NYql::NPq::NProto::TDqPqTopicSource settings; 
    settings.SetTopicPath(topic); 
    settings.SetConsumerName(DefaultPqConsumer); 
    settings.SetEndpoint(GetDefaultPqEndpoint()); 
    settings.MutableToken()->SetName("token"); 
 
    return settings; 
} 
 
NYql::NPq::NProto::TDqPqTopicSink BuildPqTopicSinkSettings(TString topic) { 
    NYql::NPq::NProto::TDqPqTopicSink settings; 
    settings.SetTopicPath(topic); 
    settings.SetEndpoint(GetDefaultPqEndpoint()); 
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
            "query_1", 
            secureParams, 
            taskParams, 
            Driver,
            nullptr, 
            &actor.GetSourceCallbacks(), 
            actor.GetHolderFactory(), 
            freeSpace); 
 
        actor.InitSource(dqSource, dqSourceAsActor); 
    }); 
} 
 
void TPqIoTestFixture::InitSink(
    NPq::NProto::TDqPqTopicSink&& settings, 
    i64 freeSpace) 
{ 
    const THashMap<TString, TString> secureParams; 
 
    CaSetup->Execute([&](TFakeActor& actor) {
        auto [dqSink, dqSinkAsActor] = CreateDqPqWriteActor( 
            std::move(settings), 
            0, 
            "query_1", 
            secureParams, 
            Driver,
            nullptr, 
            &actor.GetSinkCallbacks(), 
            freeSpace); 
 
        actor.InitSink(dqSink, dqSinkAsActor); 
    }); 
} 
 
TString GetDefaultPqEndpoint() { 
    auto port = GetEnv("LOGBROKER_PORT"); 
    UNIT_ASSERT_C(port, "Logbroker recipe is expected"); 
    return TStringBuilder() << "localhost:" << port; 
} 
 
extern const TString DefaultPqConsumer = "test_client"; 
 
void PQWrite( 
    const std::vector<TString>& sequence, 
    const TString& topic, 
    const TString& endpoint) 
{ 
    NYdb::TDriverConfig cfg; 
    cfg.SetEndpoint(endpoint); 
    cfg.SetDatabase("/Root"); 
    cfg.SetLog(CreateLogBackend("cerr")); 
    NYdb::TDriver driver(cfg); 
    NYdb::NPersQueue::TPersQueueClient client(driver); 
    NYdb::NPersQueue::TWriteSessionSettings sessionSettings; 
    sessionSettings 
        .Path(topic) 
        .MessageGroupId("src_id"); 
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
    cfg.SetDatabase("/Root"); 
    cfg.SetLog(CreateLogBackend("cerr")); 
    NYdb::TDriver driver(cfg); 
    NYdb::NPersQueue::TPersQueueClient client(driver); 
    NYdb::NPersQueue::TReadSessionSettings sessionSettings; 
    sessionSettings 
        .AppendTopics(topic) 
        .ConsumerName(DefaultPqConsumer); 
 
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
 
std::vector<TString> UVParser(const NUdf::TUnboxedValue& item) { 
    return { TString(item.AsStringRef()) }; 
} 
 
void TPqIoTestFixture::SinkWrite(std::vector<TString> data, TMaybe<NDqProto::TCheckpoint> checkpoint) {
    CaSetup->SinkWrite([data](NKikimr::NMiniKQL::THolderFactory& factory) {
        NKikimr::NMiniKQL::TUnboxedValueVector batch; 
        batch.reserve(data.size()); 
        for (const auto& item : data) { 
            NUdf::TUnboxedValue* unboxedValueForData = nullptr;
            batch.emplace_back(factory.CreateDirectArrayHolder(1, unboxedValueForData)); 
            unboxedValueForData[0] = NKikimr::NMiniKQL::MakeString(NUdf::TStringRef(item.Data(), item.Size()));
        } 
 
        return batch; 
    }, checkpoint); 
} 
} 
