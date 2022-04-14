#pragma once
#include "test_server.h"
#include <kikimr/persqueue/sdk/deprecated/cpp/v2/types.h>
#include <ydb/library/persqueue/topic_parser_public/topic_parser.h>
#include <library/cpp/logger/log.h>
#include <util/system/tempfile.h>

#define TEST_CASE_NAME (this->Name_)

namespace NPersQueue {
class SDKTestSetup {
protected:
    TString TestCaseName;

    THolder<TTempFileHandle> NetDataFile;
    THashMap<TString, NKikimr::NPersQueueTests::TPQTestClusterInfo> DataCenters;
    TString LocalDC = "dc1";
    TTestServer Server = TTestServer(false /* don't start */);

    TLog Log = TLog("cerr");

    TPQLibSettings PQLibSettings;
    THolder<TPQLib> PQLib;

public:
    SDKTestSetup(const TString& testCaseName, bool start = true)
        : TestCaseName(testCaseName)
    {
        InitOptions();
        if (start) {
            Start();
        }
    }

    void InitOptions() {
        Log.SetFormatter([testCaseName = TestCaseName](ELogPriority priority, TStringBuf message) {
            return TStringBuilder() << TInstant::Now() << " :" << testCaseName << " " << priority << ": " << message << Endl;
        });
        Server.GrpcServerOptions.SetGRpcShutdownDeadline(TDuration::Max());
        // Default TTestServer value for 'MaxReadCookies' is 10. With this value the tests are flapping with two errors:
        // 1. 'got more than 10 unordered cookies to commit 12'
        // 2. 'got more than 10 uncommitted reads'
        Server.ServerSettings.PQConfig.Clear();
        Server.ServerSettings.PQConfig.SetEnabled(true);
        Server.ServerSettings.PQConfig.SetRemoteClusterEnabledDelaySec(1);
        Server.ServerSettings.PQConfig.SetCloseClientSessionWithEnabledRemotePreferredClusterDelaySec(1);
        Server.ServerSettings.PQClusterDiscoveryConfig.SetEnabled(true);
        SetNetDataViaFile("::1/128\t" + GetLocalCluster());

        auto seed = TInstant::Now().MicroSeconds();
        // This makes failing randomized tests (for example with NUnitTest::RandomString(size, std::rand()) calls) reproducable
        Log << TLOG_INFO << "Random seed for debugging is " << seed;
        std::srand(seed);
    }

    void Start(bool waitInit = true, bool addBrokenDatacenter = !std::getenv("PERSQUEUE_GRPC_API_V1_ENABLED")) {
        Server.StartServer(false);
        //Server.EnableLogs({NKikimrServices::PQ_WRITE_PROXY, NKikimrServices::PQ_READ_PROXY});
        Server.AnnoyingClient->InitRoot();
        if (DataCenters.empty()) {
            THashMap<TString, NKikimr::NPersQueueTests::TPQTestClusterInfo> dataCenters;
            dataCenters.emplace("dc1", NKikimr::NPersQueueTests::TPQTestClusterInfo{TStringBuilder() << "localhost:" << Server.GrpcPort, true});
            if (addBrokenDatacenter) {
                dataCenters.emplace("dc2", NKikimr::NPersQueueTests::TPQTestClusterInfo{"dc2.logbroker.yandex.net", false});
            }
            Server.AnnoyingClient->InitDCs(dataCenters);
        } else {
            Server.AnnoyingClient->InitDCs(DataCenters, LocalDC);
        }
        Server.AnnoyingClient->InitSourceIds();
        CreateTopic(GetTestTopic(), GetLocalCluster());
        if (waitInit) {
            Server.WaitInit(GetTestTopic());
        }
        PQLibSettings.DefaultLogger = MakeIntrusive<TCerrLogger>(TLOG_DEBUG);
        PQLib = MakeHolder<TPQLib>(PQLibSettings);
    }

    THolder<TPQLib>& GetPQLib() {
        return PQLib;
    }

    const TPQLibSettings& GetPQLibSettings() const {
        return PQLibSettings;
    }

    TString GetTestTopic() const {
        return "topic1";
    }

    TString GetTestClient() const {
        return "test-reader";
    }

    TString GetTestMessageGroupId() const {
        return "test-message-group-id";
    }

    TString GetLocalCluster() const {
        return LocalDC;
    }


    NGrpc::TServerOptions& GetGrpcServerOptions() {
        return Server.GrpcServerOptions;
    }

    void SetNetDataViaFile(const TString& netDataTsv) {
        NetDataFile = MakeHolder<TTempFileHandle>("netData.tsv");
        NetDataFile->Write(netDataTsv.Data(), netDataTsv.Size());
        NetDataFile->FlushData();
        Server.ServerSettings.NetClassifierConfig.SetNetDataFilePath(NetDataFile->Name());
    }

    TProducerSettings GetProducerSettings() const {
        TProducerSettings producerSettings;
        producerSettings.Topic = GetTestTopic();
        producerSettings.SourceId = GetTestMessageGroupId();
        producerSettings.Server = TServerSetting{"localhost", Server.GrpcPort};
        producerSettings.Server.Database = "/Root";
        return producerSettings;
    }

    TConsumerSettings GetConsumerSettings() const {
        TConsumerSettings consumerSettings;
        consumerSettings.Topics = {GetTestTopic()};
        consumerSettings.ClientId = GetTestClient();
        consumerSettings.Server = TServerSetting{"localhost", Server.GrpcPort};
        consumerSettings.UseV2RetryPolicyInCompatMode = true;
        return consumerSettings;
    }

    TLog& GetLog() {
        return Log;
    }


    template <class TConsumerOrProducer>
    void Start(const THolder<TConsumerOrProducer>& obj) {
        auto startFuture = obj->Start();
        const auto& initResponse = startFuture.GetValueSync();
        UNIT_ASSERT_C(!initResponse.Response.HasError(), "Failed to start: " << initResponse.Response);
    }

    THolder<IConsumer> StartConsumer(const TConsumerSettings& settings) {
        THolder<IConsumer> consumer = GetPQLib()->CreateConsumer(settings);
        Start(consumer);
        return consumer;
    }

    THolder<IConsumer> StartConsumer() {
        return StartConsumer(GetConsumerSettings());
    }

    THolder<IProducer> StartProducer(const TProducerSettings& settings) {
        THolder<IProducer> producer = GetPQLib()->CreateProducer(settings);
        Start(producer);
        return producer;
    }

    THolder<IProducer> StartProducer() {
        return StartProducer(GetProducerSettings());
    }

    void WriteToTopic(const TVector<TString>& data, IProducer* producer = nullptr) {
        THolder<IProducer> localProducer;
        if (!producer) {
            localProducer = StartProducer();
            producer = localProducer.Get();
        }
        TVector<NThreading::TFuture<TProducerCommitResponse>> resps;
        for (const TString& d : data) {
            Log << TLOG_INFO << "WriteToTopic: " << d;
            resps.push_back(producer->Write(d));
        }
        for (NThreading::TFuture<TProducerCommitResponse>& r : resps) {
            UNIT_ASSERT_C(!r.GetValueSync().Response.HasError(), r.GetValueSync().Response.GetError());
        }
    }

    // Read set of sequences from topic
    void ReadFromTopic(const TVector<TVector<TString>>& data, bool commit = true, IConsumer* consumer = nullptr) {
        THolder<IConsumer> localConsumer;
        if (!consumer) {
            localConsumer = StartConsumer();
            consumer = localConsumer.Get();
        }
        TVector<size_t> positions(data.size()); // Initialy zeroes.

        int wholeCount = 0;
        for (const TVector<TString>& seq : data) {
            wholeCount += seq.size();
        }

        TSet<ui64> cookies;

        auto processCommit = [&](const TConsumerMessage& resp) {
            Log << TLOG_INFO << "ReadFromTopic. Committed: " << resp.Response.GetCommit();
            for (ui64 cookie : resp.Response.GetCommit().GetCookie()) {
                UNIT_ASSERT_VALUES_EQUAL(cookies.erase(cookie), 1);
            }
        };

        while (wholeCount > 0) {
            auto event = consumer->GetNextMessage();
            const auto& resp = event.GetValueSync();
            UNIT_ASSERT_C(!resp.Response.HasError(), resp.Response);
            if (!resp.Response.HasData()) {
                if (resp.Response.HasCommit()) {
                    processCommit(resp);
                }
                continue;
            }
            Log << TLOG_INFO << "ReadFromTopic. Data: " << resp.Response.GetData();
            UNIT_ASSERT(cookies.insert(resp.Response.GetData().GetCookie()).second);
            for (const auto& batch : resp.Response.GetData().GetMessageBatch()) {
                // find proper sequence
                const TString& firstData = batch.GetMessage(0).GetData();
                size_t seqIndex = 0;
                for (; seqIndex < positions.size(); ++seqIndex) {
                    if (positions[seqIndex] >= data[seqIndex].size()) { // Already seen.
                        continue;
                    }
                    size_t& seqPos = positions[seqIndex];
                    const TString& expectedData = data[seqIndex][seqPos];
                    if (expectedData == firstData) {
                        UNIT_ASSERT(batch.MessageSize() <= data[seqIndex].size() - positions[seqIndex]);
                        ++seqPos;
                        --wholeCount;
                        // Match.
                        for (size_t msgIndex = 1; msgIndex < batch.MessageSize(); ++msgIndex, ++seqPos, --wholeCount) {
                            UNIT_ASSERT_STRINGS_EQUAL(batch.GetMessage(msgIndex).GetData(), data[seqIndex][seqPos]);
                        }
                        break;
                    }
                }
                UNIT_ASSERT_LT_C(seqIndex, positions.size(), resp.Response);
            }

            if (commit) {
                consumer->Commit({resp.Response.GetData().GetCookie()});
            }
        }
        while (commit && !cookies.empty()) {
            auto event = consumer->GetNextMessage();
            const auto& resp = event.GetValueSync();
            UNIT_ASSERT_C(!resp.Response.HasError(), resp.Response);
            if (!resp.Response.HasCommit()) {
                continue;
            }
            processCommit(resp);
        }
        UNIT_ASSERT_VALUES_EQUAL(wholeCount, 0);
    }

    void SetSingleDataCenter(const TString& name = "dc1") {
        UNIT_ASSERT(
                DataCenters.insert(std::make_pair(
                        name,
                        NKikimr::NPersQueueTests::TPQTestClusterInfo{TStringBuilder() << "localhost:" << Server.GrpcPort, true}
                )).second
        );
        LocalDC = name;
    }

    void AddDataCenter(const TString& name, const TString& address, bool enabled = true, bool setSelfAsDc = true) {
        if (DataCenters.empty() && setSelfAsDc) {
            SetSingleDataCenter();
        }
        NKikimr::NPersQueueTests::TPQTestClusterInfo info{
            address,
            enabled
        };
        UNIT_ASSERT(DataCenters.insert(std::make_pair(name, info)).second);
    }

    void AddDataCenter(const TString& name, const SDKTestSetup& cluster, bool enabled = true, bool setSelfAsDc = true) {
        AddDataCenter(name, TStringBuilder() << "localhost:" << cluster.Server.GrpcPort, enabled, setSelfAsDc);
    }

    void EnableDataCenter(const TString& name) {
        auto iter = DataCenters.find(name);
        UNIT_ASSERT(iter != DataCenters.end());
        Server.AnnoyingClient->UpdateDcEnabled(name, true);
    }
    void DisableDataCenter(const TString& name) {
        auto iter = DataCenters.find(name);
        UNIT_ASSERT(iter != DataCenters.end());
        Server.AnnoyingClient->UpdateDcEnabled(name, false);
    }

    void ShutdownGRpc() {
        Server.ShutdownGRpc();
    }

    void EnableGRpc() {
        Server.EnableGRpc();
        Server.WaitInit(GetTestTopic());
    }

    void KickTablets() {
        for (ui32 i = 0; i < Server.CleverServer->StaticNodes(); i++) {
            Server.AnnoyingClient->MarkNodeInHive(Server.CleverServer->GetRuntime(), i, false);
            Server.AnnoyingClient->KickNodeInHive(Server.CleverServer->GetRuntime(), i);
        }
    }
    void AllowTablets() {
        for (ui32 i = 0; i < Server.CleverServer->StaticNodes(); i++) {
            Server.AnnoyingClient->MarkNodeInHive(Server.CleverServer->GetRuntime(), i, true);
        }
    }

    void CreateTopic(const TString& topic, const TString& cluster, size_t partitionsCount = 1) {
        Server.AnnoyingClient->CreateTopicNoLegacy(BuildFullTopicName(topic, cluster), partitionsCount);
    }

    void KillPqrb(const TString& topic, const TString& cluster) {
        auto describeResult = Server.AnnoyingClient->Ls(TStringBuilder() << "/Root/PQ/" << BuildFullTopicName(topic, cluster));
        UNIT_ASSERT_C(describeResult->Record.GetPathDescription().HasPersQueueGroup(), describeResult->Record);
        Server.AnnoyingClient->KillTablet(*Server.CleverServer, describeResult->Record.GetPathDescription().GetPersQueueGroup().GetBalancerTabletID());
    }
};
}
