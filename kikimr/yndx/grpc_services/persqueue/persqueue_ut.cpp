#include "ut/definitions.h"
#include <ydb/core/base/appdata.h>
#include <ydb/core/testlib/test_pq_client.h>
#include <ydb/core/client/server/grpc_proxy_status.h>
#include <ydb/core/protos/grpc_pq_old.pb.h>
#include <ydb/core/persqueue/writer/source_id_encoding.h>
#include <kikimr/persqueue/sdk/deprecated/cpp/v2/ut_utils/test_pqlib.h>
#include <kikimr/persqueue/sdk/deprecated/cpp/v2/ut_utils/test_server.h>

#include <ydb/library/aclib/aclib.h>
#include <ydb/library/persqueue/tests/counters.h>

#include <library/cpp/testing/unittest/tests_data.h>
#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/http/io/stream.h>
#include <google/protobuf/text_format.h>

#include <util/string/join.h>
#include <util/string/builder.h>

#include <grpc++/client_context.h>
#include <grpc++/create_channel.h>

#include <kikimr/persqueue/sdk/deprecated/cpp/v2/persqueue.h>

namespace NKikimr {
namespace NPersQueueTests {

using namespace Tests;
using namespace NKikimrClient;
using namespace NPersQueue;
using namespace NPersQueue::NTests;
using namespace NThreading;
using namespace NNetClassifier;

static TString FormNetData() {
    return "10.99.99.224/32\tSAS\n"
           "::1/128\tVLA\n";
}

TAutoPtr<IEventHandle> GetClassifierUpdate(TServer& server, const TActorId sender) {
    auto& actorSystem = *server.GetRuntime();
    actorSystem.Send(
            new IEventHandle(MakeNetClassifierID(), sender,
            new TEvNetClassifier::TEvSubscribe()
        ));

    TAutoPtr<IEventHandle> handle;
    actorSystem.GrabEdgeEvent<NNetClassifier::TEvNetClassifier::TEvClassifierUpdate>(handle);

    UNIT_ASSERT(handle);
    UNIT_ASSERT_VALUES_EQUAL(handle->Recipient, sender);

    return handle;
}

THolder<TTempFileHandle> CreateNetDataFile(const TString& content) {
    auto netDataFile = MakeHolder<TTempFileHandle>("data.tsv");

    netDataFile->Write(content.Data(), content.Size());
    netDataFile->FlushData();

    return netDataFile;
}


Y_UNIT_TEST_SUITE(TPersQueueTest2) {
    void PrepareForGrpcNoDC(TFlatMsgBusPQClient& annoyingClient) {
        annoyingClient.SetNoConfigMode();
        annoyingClient.FullInit();
        annoyingClient.InitUserRegistry();
        annoyingClient.MkDir("/Root", "account1");
        annoyingClient.MkDir("/Root/PQ", "account1");
        annoyingClient.CreateTopicNoLegacy("/Root/PQ/rt3.db--topic1", 5, false);
        annoyingClient.CreateTopicNoLegacy("/Root/PQ/account1/topic1", 5, false, true, Nothing(), {"user1", "user2"});
        annoyingClient.CreateTopicNoLegacy("/Root/account2/topic2", 5);
    }
    Y_UNIT_TEST(TestGrpcWriteNoDC) {
        TTestServer server(false);
        server.ServerSettings.PQConfig.SetTopicsAreFirstClassCitizen(true);
        server.ServerSettings.PQConfig.SetRoot("/Rt2/PQ");
        server.ServerSettings.PQConfig.SetDatabase("/Root");
        server.StartServer();

        UNIT_ASSERT_VALUES_EQUAL(NMsgBusProxy::MSTATUS_OK,
                                        server.AnnoyingClient->AlterUserAttributes("/", "Root", {{"folder_id", "somefolder"}, {"cloud_id", "somecloud"}, {"database_id", "root"}}));


        PrepareForGrpcNoDC(*server.AnnoyingClient);
        auto writer = MakeDataWriter(server, "source1");

        writer.Write("/Root/account2/topic2", {"valuevaluevalue1"}, true, "topic1@" BUILTIN_ACL_DOMAIN);
        writer.Write("/Root/PQ/account1/topic1", {"valuevaluevalue1"}, true, "topic1@" BUILTIN_ACL_DOMAIN);

        NACLib::TDiffACL acl;
        acl.AddAccess(NACLib::EAccessType::Allow, NACLib::UpdateRow, "topic1@" BUILTIN_ACL_DOMAIN);
        acl.AddAccess(NACLib::EAccessType::Allow, NACLib::SelectRow, "topic1@" BUILTIN_ACL_DOMAIN);

        server.AnnoyingClient->ModifyACL("/Root/account2", "topic2", acl.SerializeAsString());
        server.AnnoyingClient->ModifyACL("/Root/PQ/account1", "topic1", acl.SerializeAsString());

        Sleep(TDuration::Seconds(5));

        writer.Write("/Root/account2/topic2", {"valuevaluevalue1"}, false, "topic1@" BUILTIN_ACL_DOMAIN);

        writer.Write("/Root/PQ/account1/topic1", {"valuevaluevalue1"}, false, "topic1@" BUILTIN_ACL_DOMAIN);
        writer.Write("/Root/PQ/account1/topic1", {"valuevaluevalue2"}, false, "topic1@" BUILTIN_ACL_DOMAIN);

        writer.Read("/Root/PQ/account1/topic1", "user1", "topic1@" BUILTIN_ACL_DOMAIN, false, false, false, true);

        writer.Write("Root/PQ/account1/topic1", {"valuevaluevalue2"}, false, "topic1@" BUILTIN_ACL_DOMAIN); //TODO /Root remove

        writer.Read("Root/PQ/account1/topic1", "user1", "topic1@" BUILTIN_ACL_DOMAIN, false, false, false, true);
    }
}
Y_UNIT_TEST_SUITE(TPersQueueTest) {

    Y_UNIT_TEST(SetupLockSession2) {
        TTestServer server(false);
        server.GrpcServerOptions.SetMaxMessageSize(130_MB);
        server.StartServer();
        server.EnableLogs({ NKikimrServices::PQ_READ_PROXY });
        server.EnableLogs({ NKikimrServices::PERSQUEUE }, NActors::NLog::PRI_INFO);
        server.AnnoyingClient->CreateTopic("rt3.dc1--acc--topic1", 1);
        server.AnnoyingClient->CreateTopic("rt3.dc2--acc--topic1", 1);
        server.AnnoyingClient->CreateTopic(DEFAULT_TOPIC_NAME, 1);
        server.AnnoyingClient->CreateConsumer("user");

        auto writer = MakeDataWriter(server);

        TTestPQLib PQLib(server);
        auto [consumer, startResult] = PQLib.CreateConsumer({"acc/topic1"}, "user", 1, true);
        Cerr << startResult.Response << "\n";
        for (ui32 i = 0; i < 2; ++i) {
            auto msg = consumer->GetNextMessage();
            msg.Wait();
            Cerr << "Response: " << msg.GetValue().Response << "\n";
            UNIT_ASSERT(msg.GetValue().Response.HasLock());
            UNIT_ASSERT(msg.GetValue().Response.GetLock().GetTopic() == "rt3.dc1--acc--topic1" || msg.GetValue().Response.GetLock().GetTopic() == "rt3.dc2--acc--topic1");
            UNIT_ASSERT(msg.GetValue().Response.GetLock().GetPartition() == 0);
        }
        auto msg = consumer->GetNextMessage();
        UNIT_ASSERT(!msg.Wait(TDuration::Seconds(1)));
        server.AnnoyingClient->AlterTopic("rt3.dc2--acc--topic1", 2);
        msg.Wait();
        UNIT_ASSERT(msg.GetValue().Response.HasLock());
        UNIT_ASSERT(msg.GetValue().Response.GetLock().GetTopic() == "rt3.dc2--acc--topic1");
        UNIT_ASSERT(msg.GetValue().Response.GetLock().GetPartition() == 1);
    }



    Y_UNIT_TEST(SetupLockSession) {
        TTestServer server;

        server.EnableLogs({ NKikimrServices::PQ_READ_PROXY });
        server.EnableLogs({ NKikimrServices::PERSQUEUE }, NActors::NLog::PRI_INFO);

        server.AnnoyingClient->CreateTopic("rt3.dc1--acc--topic1", 1);
        server.AnnoyingClient->CreateTopic(DEFAULT_TOPIC_NAME, 1);
        server.AnnoyingClient->CreateConsumer("user");

        auto writer = MakeDataWriter(server);

        std::shared_ptr<grpc::Channel> Channel_;
        std::unique_ptr<NKikimrClient::TGRpcServer::Stub> Stub_;
        std::unique_ptr<NPersQueue::PersQueueService::Stub> StubP_;

        Channel_ = grpc::CreateChannel("localhost:" + ToString(server.GrpcPort), grpc::InsecureChannelCredentials());
        Stub_ = NKikimrClient::TGRpcServer::NewStub(Channel_);

        ui64 proxyCookie = 0;

        {
            grpc::ClientContext context;
            NKikimrClient::TChooseProxyRequest request;
            NKikimrClient::TResponse response;
            auto status = Stub_->ChooseProxy(&context, request, &response);
            UNIT_ASSERT(status.ok());
            Cerr << response << "\n";
            UNIT_ASSERT(response.GetStatus() == NMsgBusProxy::MSTATUS_OK);
            proxyCookie = response.GetProxyCookie();
            Channel_ = grpc::CreateChannel("[" + response.GetProxyName() + "]:" + ToString(server.GrpcPort), grpc::InsecureChannelCredentials());
            StubP_ = NPersQueue::PersQueueService::NewStub(Channel_);
        }


        grpc::ClientContext rcontext;
        auto readStream = StubP_->ReadSession(&rcontext);
        UNIT_ASSERT(readStream);

        // init read session
        {
            TReadRequest  req;
            TReadResponse resp;

            req.MutableInit()->AddTopics("acc/topic1");

            req.MutableInit()->SetClientId("user");
            req.MutableInit()->SetClientsideLocksAllowed(true);
            req.MutableInit()->SetProxyCookie(proxyCookie);
            req.MutableInit()->SetProtocolVersion(TReadRequest::ReadParamsInInit);
            req.MutableInit()->SetMaxReadMessagesCount(3);

            if (!readStream->Write(req)) {
                ythrow yexception() << "write fail";
            }
            UNIT_ASSERT(readStream->Read(&resp));
            UNIT_ASSERT(resp.HasInit());
            //send some reads
            req.Clear();
            req.MutableRead();
            for (ui32 i = 0; i < 10; ++i) {
                if (!readStream->Write(req)) {
                    ythrow yexception() << "write fail";
                }
            }
        }

        {
            TReadRequest  req;
            TReadResponse resp;

            //lock partition
            UNIT_ASSERT(readStream->Read(&resp));
            UNIT_ASSERT(resp.HasLock());
            UNIT_ASSERT_VALUES_EQUAL(resp.GetLock().GetTopic(), "rt3.dc1--acc--topic1");
            UNIT_ASSERT(resp.GetLock().GetPartition() == 0);

            req.Clear();
            req.MutableStartRead()->SetTopic(resp.GetLock().GetTopic());
            req.MutableStartRead()->SetPartition(resp.GetLock().GetPartition());
            req.MutableStartRead()->SetReadOffset(10);
            req.MutableStartRead()->SetGeneration(resp.GetLock().GetGeneration());
            if (!readStream->Write(req)) {
                ythrow yexception() << "write fail";
            }

        }

        //Write some data
        writer.Write("acc/topic1", "valuevaluevalue1");
        Sleep(TDuration::Seconds(15)); //force wait data
        writer.Write("acc/topic1", "valuevaluevalue2");
        writer.Write("acc/topic1", "valuevaluevalue3");
        writer.Write("acc/topic1", "valuevaluevalue4");

        //check read results
        TReadResponse resp;
        for (ui32 i = 10; i < 16; ++i) {
            UNIT_ASSERT(readStream->Read(&resp));
            UNIT_ASSERT_C(resp.HasBatchedData(), resp);
            UNIT_ASSERT(resp.GetBatchedData().PartitionDataSize() == 1);
            UNIT_ASSERT(resp.GetBatchedData().GetPartitionData(0).BatchSize() == 1);
            UNIT_ASSERT(resp.GetBatchedData().GetPartitionData(0).GetBatch(0).MessageDataSize() == 1);
            UNIT_ASSERT(resp.GetBatchedData().GetPartitionData(0).GetBatch(0).GetMessageData(0).GetOffset() == i);
        }
        //TODO: restart here readSession and read from position 10
        {
            TReadRequest  req;
            TReadResponse resp;

            req.MutableCommit()->AddCookie(1);

            if (!readStream->Write(req)) {
                ythrow yexception() << "write fail";
            }
            UNIT_ASSERT(readStream->Read(&resp));
            UNIT_ASSERT(resp.HasCommit());
        }
    }


    void SetupWriteSessionImpl(bool rr) {
        TTestServer server(PQSettings(0, 2, rr));

        server.EnableLogs({ NKikimrServices::PQ_WRITE_PROXY });

        server.AnnoyingClient->CreateTopic(DEFAULT_TOPIC_NAME, 10);
        auto writer = MakeDataWriter(server);

        ui32 p = writer.Write(SHORT_TOPIC_NAME, "valuevaluevalue1");

        server.AnnoyingClient->AlterTopic(DEFAULT_TOPIC_NAME, 15);

        ui32 pp = writer.Write(SHORT_TOPIC_NAME, "valuevaluevalue2");
        UNIT_ASSERT_VALUES_EQUAL(p, pp);

        writer.WriteBatch(SHORT_TOPIC_NAME, {"1", "2", "3", "4", "5"});

        writer.Write("topic2", "valuevaluevalue1", true);

        p = writer.InitSession("sid1", 2, true);
        pp = writer.InitSession("sid1", 0, true);

        UNIT_ASSERT(p = pp);
        UNIT_ASSERT(p == 1);

        {
            p = writer.InitSession("sidx", 0, true);
            pp = writer.InitSession("sidx", 0, true);

            UNIT_ASSERT(p == pp);
        }

        writer.InitSession("sid1", 3, false);

        //check round robin;
        TMap<ui32, ui32> ss;
        for (ui32 i = 0; i < 15*5; ++i) {
            ss[writer.InitSession("sid_rand_" + ToString<ui32>(i), 0, true)]++;
        }
        for (auto &s : ss) {
            Cerr << s.first << " " << s.second << "\n";
            if (rr) {
                UNIT_ASSERT(s.second >= 4 && s.second <= 6);
            }
        }
    }

    Y_UNIT_TEST(SetupWriteSession) {
        SetupWriteSessionImpl(false);
        SetupWriteSessionImpl(true);
    }

    Y_UNIT_TEST(SetupWriteSessionOnDisabledCluster) {
        TTestServer server;
        server.EnableLogs({ NKikimrServices::PERSQUEUE, NKikimrServices::PQ_WRITE_PROXY});

        server.AnnoyingClient->CreateTopic(DEFAULT_TOPIC_NAME, 10);

        auto writer = MakeDataWriter(server);

        server.AnnoyingClient->DisableDC();

        Sleep(TDuration::Seconds(5));
        writer.Write(SHORT_TOPIC_NAME, "valuevaluevalue1", true);
    }

    Y_UNIT_TEST(CloseActiveWriteSessionOnClusterDisable) {
        TTestServer server;
        server.AnnoyingClient->CreateTopic(DEFAULT_TOPIC_NAME, 10);
        server.EnableLogs({ NKikimrServices::PQ_WRITE_PROXY });

        auto writer = MakeDataWriter(server);

        TTestPQLib PQLib(server);
        auto [producer, res] = PQLib.CreateProducer(SHORT_TOPIC_NAME, "123", {}, ECodec::RAW);

        NThreading::TFuture<NPersQueue::TError> isDead = producer->IsDead();
        server.AnnoyingClient->DisableDC();
        isDead.Wait();
        UNIT_ASSERT_EQUAL(isDead.GetValue().GetCode(), NPersQueue::NErrorCode::CLUSTER_DISABLED);
    }

    Y_UNIT_TEST(BadSids) {
        TTestServer server;
        server.AnnoyingClient->CreateTopic(DEFAULT_TOPIC_NAME, 10);
        server.EnableLogs({ NKikimrServices::PQ_WRITE_PROXY });

        auto writer = MakeDataWriter(server);
        TTestPQLib PQLib(server);
        auto runSidTest = [&](const TString& srcId, bool shouldFail = true) {
            auto[producer, res] = PQLib.CreateProducer(SHORT_TOPIC_NAME, srcId);
            if (shouldFail) {
                UNIT_ASSERT(res.Response.HasError());
            } else {
                UNIT_ASSERT(res.Response.HasInit());
            }
        };

        runSidTest("base64:a***");
        runSidTest("base64:aa==");
        runSidTest("base64:a");
        runSidTest("base64:aa", false);
    }

    Y_UNIT_TEST(ReadFromSeveralPartitions) {
        TTestServer server;
        server.AnnoyingClient->CreateTopic(DEFAULT_TOPIC_NAME, 10);
        auto writer = MakeDataWriter(server, "source1");

        std::shared_ptr<grpc::Channel> Channel_;
        std::unique_ptr<NKikimrClient::TGRpcServer::Stub> Stub_;
        std::unique_ptr<NPersQueue::PersQueueService::Stub> StubP_;

        Channel_ = grpc::CreateChannel("localhost:" + ToString(server.GrpcPort), grpc::InsecureChannelCredentials());
        Stub_ = NKikimrClient::TGRpcServer::NewStub(Channel_);

        ui64 proxyCookie = 0;

        {
            grpc::ClientContext context;
            NKikimrClient::TChooseProxyRequest request;
            NKikimrClient::TResponse response;
            auto status = Stub_->ChooseProxy(&context, request, &response);
            UNIT_ASSERT(status.ok());
            Cerr << response << "\n";
            UNIT_ASSERT(response.GetStatus() == NMsgBusProxy::MSTATUS_OK);
            proxyCookie = response.GetProxyCookie();
            Channel_ = grpc::CreateChannel("[" + response.GetProxyName() + "]:" + ToString(server.GrpcPort), grpc::InsecureChannelCredentials());
            StubP_ = NPersQueue::PersQueueService::NewStub(Channel_);
        }


        //Write some data
        writer.Write(SHORT_TOPIC_NAME, "valuevaluevalue1");

        auto writer2 = MakeDataWriter(server, "source2");
        writer2.Write(SHORT_TOPIC_NAME, "valuevaluevalue2");

        grpc::ClientContext rcontext;
        auto readStream = StubP_->ReadSession(&rcontext);
        UNIT_ASSERT(readStream);

        // init read session
        {
            TReadRequest  req;
            TReadResponse resp;

            req.MutableInit()->AddTopics(SHORT_TOPIC_NAME);

            req.MutableInit()->SetClientId("user");
            req.MutableInit()->SetProxyCookie(proxyCookie);
            req.MutableInit()->SetProtocolVersion(TReadRequest::ReadParamsInInit);
            req.MutableInit()->SetMaxReadMessagesCount(1000);

            if (!readStream->Write(req)) {
                ythrow yexception() << "write fail";
            }
            UNIT_ASSERT(readStream->Read(&resp));
            UNIT_ASSERT(resp.HasInit());

            //send some reads
            Sleep(TDuration::Seconds(5));
            for (ui32 i = 0; i < 10; ++i) {
                req.Clear();
                req.MutableRead();

                if (!readStream->Write(req)) {
                    ythrow yexception() << "write fail";
                }
            }
        }

        //check read results
        TReadResponse resp;
        for (ui32 i = 0; i < 1; ++i) {
            UNIT_ASSERT(readStream->Read(&resp));
            UNIT_ASSERT_C(resp.HasBatchedData(), resp);
            UNIT_ASSERT(resp.GetBatchedData().PartitionDataSize() == 2);
        }
    }


    void SetupReadSessionTest(bool useBatching) {
        TTestServer server;
        server.EnableLogs({ NKikimrServices::PQ_READ_PROXY });

        server.AnnoyingClient->CreateTopic(DEFAULT_TOPIC_NAME, 2);
        server.AnnoyingClient->CreateTopic("rt3.dc2--topic1", 2);

        auto writer = MakeDataWriter(server, "source1");

        writer.Write(SHORT_TOPIC_NAME, "valuevaluevalue0");
        writer.Write(SHORT_TOPIC_NAME, "valuevaluevalue1");
        writer.Write(SHORT_TOPIC_NAME, "valuevaluevalue2");
        writer.Write(SHORT_TOPIC_NAME, "valuevaluevalue3");
        writer.Write(SHORT_TOPIC_NAME, "valuevaluevalue4");
        writer.Write(SHORT_TOPIC_NAME, "valuevaluevalue5");
        writer.Write(SHORT_TOPIC_NAME, "valuevaluevalue6");
        writer.Write(SHORT_TOPIC_NAME, "valuevaluevalue7");
        writer.Write(SHORT_TOPIC_NAME, "valuevaluevalue8");
        writer.Write(SHORT_TOPIC_NAME, "valuevaluevalue9");

        writer.Read(SHORT_TOPIC_NAME, "user", "", false, false, useBatching);
    }

    Y_UNIT_TEST(SetupReadSession) {
        SetupReadSessionTest(false);
    }

    Y_UNIT_TEST(SetupReadSessionWithBatching) {
        SetupReadSessionTest(true);
    }

    void ClosesSessionOnReadSettingsChangeTest(bool initReadSettingsInInitRequest) {
        TTestServer server;
        server.EnableLogs({ NKikimrServices::PQ_READ_PROXY });

        server.AnnoyingClient->CreateTopic(DEFAULT_TOPIC_NAME, 2);
        server.AnnoyingClient->CreateTopic("rt3.dc2--topic1", 2);

        auto writer = MakeDataWriter(server, "source1");
        writer.Write(SHORT_TOPIC_NAME, "valuevaluevalue0");

        // Reading code
        std::shared_ptr<grpc::Channel> Channel_;
        std::unique_ptr<NKikimrClient::TGRpcServer::Stub> Stub_;
        std::unique_ptr<NPersQueue::PersQueueService::Stub> StubP_;

        Channel_ = grpc::CreateChannel("localhost:" + ToString(server.GrpcPort), grpc::InsecureChannelCredentials());
        Stub_ = NKikimrClient::TGRpcServer::NewStub(Channel_);

        ui64 proxyCookie = 0;

            {
                grpc::ClientContext context;
                NKikimrClient::TChooseProxyRequest request;
                NKikimrClient::TResponse response;
                auto status = Stub_->ChooseProxy(&context, request, &response);
                UNIT_ASSERT(status.ok());
                Cerr << response << "\n";
                UNIT_ASSERT(response.GetStatus() == NMsgBusProxy::MSTATUS_OK);
                proxyCookie = response.GetProxyCookie();
                Channel_ = grpc::CreateChannel("[" + response.GetProxyName() + "]:" + ToString(server.GrpcPort), grpc::InsecureChannelCredentials());
                StubP_ = NPersQueue::PersQueueService::NewStub(Channel_);
            }

        grpc::ClientContext rcontext;
        auto readStream = StubP_->ReadSession(&rcontext);
        UNIT_ASSERT(readStream);

        // init read session
        {
            TReadRequest  req;
            TReadResponse resp;

            req.MutableInit()->AddTopics(SHORT_TOPIC_NAME);

            req.MutableInit()->SetClientId("user");
            req.MutableInit()->SetProxyCookie(proxyCookie);
            if (initReadSettingsInInitRequest) {
                req.MutableInit()->SetProtocolVersion(TReadRequest::ReadParamsInInit);
                req.MutableInit()->SetMaxReadMessagesCount(1);
            }

            if (!readStream->Write(req)) {
                ythrow yexception() << "write fail";
            }
            UNIT_ASSERT(readStream->Read(&resp));
            UNIT_ASSERT(resp.HasInit());

            if (!initReadSettingsInInitRequest) {
                // send first read
                req.Clear();
                req.MutableRead()->SetMaxCount(1);
                if (!readStream->Write(req)) {
                    ythrow yexception() << "write fail";
                }
                UNIT_ASSERT(readStream->Read(&resp));
                UNIT_ASSERT_C(resp.HasData(), resp);
            }

            // change settings
            req.Clear();
            req.MutableRead()->SetMaxCount(42);
            if (!readStream->Write(req)) {
                ythrow yexception() << "write fail";
            }
            UNIT_ASSERT(readStream->Read(&resp));
            UNIT_ASSERT(resp.HasError());
        }
    }

    Y_UNIT_TEST(WriteSessionClose) {

        TTestServer server;
        server.EnableLogs({ NKikimrServices::PQ_WRITE_PROXY });

        server.AnnoyingClient->CreateTopic(DEFAULT_TOPIC_NAME, 2);
        server.AnnoyingClient->CreateTopic("rt3.dc2--topic1", 2);

        auto writer = MakeDataWriter(server, "source1");
        writer.Write(SHORT_TOPIC_NAME, "valuevaluevalue0");

        // Reading code
        std::shared_ptr<grpc::Channel> Channel_;
        std::unique_ptr<NKikimrClient::TGRpcServer::Stub> Stub_;
        std::unique_ptr<NPersQueue::PersQueueService::Stub> StubP_;

        Channel_ = grpc::CreateChannel("localhost:" + ToString(server.GrpcPort), grpc::InsecureChannelCredentials());
        Stub_ = NKikimrClient::TGRpcServer::NewStub(Channel_);

        ui64 proxyCookie = 0;

        {
            grpc::ClientContext context;
            NKikimrClient::TChooseProxyRequest request;
            NKikimrClient::TResponse response;
            auto status = Stub_->ChooseProxy(&context, request, &response);
            UNIT_ASSERT(status.ok());
            Cerr << response << "\n";
            UNIT_ASSERT(response.GetStatus() == NMsgBusProxy::MSTATUS_OK);
            proxyCookie = response.GetProxyCookie();
            Channel_ = grpc::CreateChannel("[" + response.GetProxyName() + "]:" + ToString(server.GrpcPort), grpc::InsecureChannelCredentials());
            StubP_ = NPersQueue::PersQueueService::NewStub(Channel_);
        }

        // init write session
        for (ui32 i = 0; i < 2; ++i){
            grpc::ClientContext rcontext;

            auto writeStream = StubP_->WriteSession(&rcontext);
            UNIT_ASSERT(writeStream);

            TWriteRequest  req;

            req.MutableInit()->SetTopic(SHORT_TOPIC_NAME);

            req.MutableInit()->SetSourceId("user");
            req.MutableInit()->SetProxyCookie(proxyCookie);
            if (i == 0)
                continue;
            if (!writeStream->Write(req)) {
                ythrow yexception() << "write fail";
            }
        }
    }

    Y_UNIT_TEST(ClosesSessionOnReadSettingsChange) {
        ClosesSessionOnReadSettingsChangeTest(false);
    }

    Y_UNIT_TEST(ClosesSessionOnReadSettingsChangeWithInit) {
        ClosesSessionOnReadSettingsChangeTest(true);
    }

    Y_UNIT_TEST(WriteExisting) {
        TTestServer server;
        server.EnableLogs({ NKikimrServices::FLAT_TX_SCHEMESHARD });

        server.AnnoyingClient->CreateTopic(DEFAULT_TOPIC_NAME, 2);

        {
            THolder<NMsgBusProxy::TBusPersQueue> request = TRequestDescribePQ().GetRequest({});

            NKikimrClient::TResponse response;

            auto channel = grpc::CreateChannel("localhost:"+ToString(server.GrpcPort), grpc::InsecureChannelCredentials());
            auto stub(NKikimrClient::TGRpcServer::NewStub(channel));
            grpc::ClientContext context;
            auto status = stub->PersQueueRequest(&context, request->Record, &response);

            UNIT_ASSERT(status.ok());
        }

        server.AnnoyingClient->WriteToPQ(
                DEFAULT_TOPIC_NAME, 1, "abacaba", 1, "valuevaluevalue1", "", ETransport::GRpc
        );
        server.AnnoyingClient->WriteToPQ(DEFAULT_TOPIC_NAME, 1, "abacaba", 2, "valuevaluevalue1", "", ETransport::GRpc);
    }

    Y_UNIT_TEST(WriteExistingBigValue) {
        TTestServer server(PQSettings(0, 2).SetDomainName("Root"));
        server.AnnoyingClient->CreateTopic(DEFAULT_TOPIC_NAME, 2, 8_MB, 86400, 100000);

        server.EnableLogs({ NKikimrServices::FLAT_TX_SCHEMESHARD });

        TInstant now(Now());

        server.AnnoyingClient->WriteToPQ(DEFAULT_TOPIC_NAME, 1, "abacaba", 1, TString(1000000, 'a'));
        server.AnnoyingClient->WriteToPQ(DEFAULT_TOPIC_NAME, 1, "abacaba", 2, TString(1, 'a'));
        UNIT_ASSERT(TInstant::Now() - now > TDuration::MilliSeconds(5990)); //speed limit is 200kb/s and burst is 200kb, so to write 1mb it will take at least 4 seconds
    }

    Y_UNIT_TEST(WriteEmptyData) {
        TTestServer server(PQSettings(0, 2).SetDomainName("Root"));
        server.AnnoyingClient->CreateTopic(DEFAULT_TOPIC_NAME, 2);

        // empty data and sourecId
        server.AnnoyingClient->WriteToPQ(DEFAULT_TOPIC_NAME, 1, "", 1, "", "", ETransport::MsgBus, NMsgBusProxy::MSTATUS_ERROR);
        server.AnnoyingClient->WriteToPQ(DEFAULT_TOPIC_NAME, 1, "a", 1, "", "", ETransport::MsgBus, NMsgBusProxy::MSTATUS_ERROR);
        server.AnnoyingClient->WriteToPQ(DEFAULT_TOPIC_NAME, 1, "", 1, "a", "", ETransport::MsgBus, NMsgBusProxy::MSTATUS_ERROR);
        server.AnnoyingClient->WriteToPQ(DEFAULT_TOPIC_NAME, 1, "a", 1, "a", "", ETransport::MsgBus, NMsgBusProxy::MSTATUS_OK);
    }


    Y_UNIT_TEST(WriteNonExistingPartition) {
        TTestServer server(PQSettings(0, 2).SetDomainName("Root"));
        server.AnnoyingClient->CreateTopic(DEFAULT_TOPIC_NAME, 2);

        server.EnableLogs({ NKikimrServices::FLAT_TX_SCHEMESHARD });

        server.AnnoyingClient->WriteToPQ(
                DEFAULT_TOPIC_NAME, 100500, "abacaba", 1, "valuevaluevalue1", "",
                ETransport::MsgBus, NMsgBusProxy::MSTATUS_ERROR, NMsgBusProxy::MSTATUS_ERROR
        );
    }

    Y_UNIT_TEST(WriteNonExistingTopic) {
        TTestServer server(PQSettings(0, 2).SetDomainName("Root"));
        server.AnnoyingClient->CreateTopic(DEFAULT_TOPIC_NAME, 2);

        server.EnableLogs({ NKikimrServices::FLAT_TX_SCHEMESHARD });

        server.AnnoyingClient->WriteToPQ("rt3.dc1--topic1000", 1, "abacaba", 1, "valuevaluevalue1", "", ETransport::MsgBus, NMsgBusProxy::MSTATUS_ERROR, NMsgBusProxy::MSTATUS_ERROR);
    }

    Y_UNIT_TEST(SchemeshardRestart) {
        TTestServer server;
        server.AnnoyingClient->CreateTopic(DEFAULT_TOPIC_NAME, 2);
        TString topic2 = "rt3.dc1--topic2";
        server.AnnoyingClient->CreateTopic(topic2, 2);

        // force topic1 into cache and establish pipe from cache to schemeshard
        server.AnnoyingClient->WriteToPQ(DEFAULT_TOPIC_NAME, 1, "abacaba", 1, "valuevaluevalue1");

        server.EnableLogs({ NKikimrServices::FLAT_TX_SCHEMESHARD, NKikimrServices::PQ_METACACHE });

        server.AnnoyingClient->RestartSchemeshard(server.CleverServer->GetRuntime());
        server.AnnoyingClient->WriteToPQ(topic2, 1, "abacaba", 1, "valuevaluevalue1");
    }

    Y_UNIT_TEST(WriteAfterAlter) {
        TTestServer server(PQSettings(0).SetDomainName("Root").SetNodeCount(2));
        server.AnnoyingClient->CreateTopic(DEFAULT_TOPIC_NAME, 2);

        server.EnableLogs({ NKikimrServices::FLAT_TX_SCHEMESHARD, NKikimrServices::PQ_METACACHE });


        server.AnnoyingClient->WriteToPQ(
                DEFAULT_TOPIC_NAME, 5, "abacaba", 1, "valuevaluevalue1", "",
                ETransport::MsgBus, NMsgBusProxy::MSTATUS_ERROR,  NMsgBusProxy::MSTATUS_ERROR
        );

        server.AnnoyingClient->AlterTopic(DEFAULT_TOPIC_NAME, 10);
        Sleep(TDuration::Seconds(1));
        server.AnnoyingClient->WriteToPQ(DEFAULT_TOPIC_NAME, 5, "abacaba", 1, "valuevaluevalue1");
        server.AnnoyingClient->WriteToPQ(
                DEFAULT_TOPIC_NAME, 15, "abacaba", 1, "valuevaluevalue1", "",
                ETransport::MsgBus, NMsgBusProxy::MSTATUS_ERROR,  NMsgBusProxy::MSTATUS_ERROR
        );

        server.AnnoyingClient->AlterTopic(DEFAULT_TOPIC_NAME, 20);
        Sleep(TDuration::Seconds(1));
        server.AnnoyingClient->WriteToPQ(DEFAULT_TOPIC_NAME, 5, "abacaba", 1, "valuevaluevalue1");
        server.AnnoyingClient->WriteToPQ(DEFAULT_TOPIC_NAME, 15, "abacaba", 1, "valuevaluevalue1");
    }

    Y_UNIT_TEST(Delete) {
        TTestServer server(PQSettings(0).SetDomainName("Root").SetNodeCount(2));
        server.EnableLogs({ NKikimrServices::FLAT_TX_SCHEMESHARD });

        // Delete non-existing
        server.AnnoyingClient->DeleteTopic2(DEFAULT_TOPIC_NAME, NPersQueue::NErrorCode::UNKNOWN_TOPIC);

        server.AnnoyingClient->CreateTopic(DEFAULT_TOPIC_NAME, 2);

        // Delete existing
        server.AnnoyingClient->DeleteTopic2(DEFAULT_TOPIC_NAME);

        // Double delete - "What Is Dead May Never Die"
        server.AnnoyingClient->DeleteTopic2(DEFAULT_TOPIC_NAME, NPersQueue::NErrorCode::UNKNOWN_TOPIC);

        // Resurrect deleted topic
        server.AnnoyingClient->CreateTopic(DEFAULT_TOPIC_NAME, 2);
        server.AnnoyingClient->DeleteTopic2(DEFAULT_TOPIC_NAME);
    }

    Y_UNIT_TEST(WriteAfterDelete) {
        TTestServer server(PQSettings(0).SetDomainName("Root").SetNodeCount(2));
        server.AnnoyingClient->CreateTopic(DEFAULT_TOPIC_NAME, 3);

        server.AnnoyingClient->WriteToPQ(DEFAULT_TOPIC_NAME, 1, "abacaba", 1, "valuevaluevalue1");

        server.EnableLogs({ NKikimrServices::FLAT_TX_SCHEMESHARD, NKikimrServices::PQ_METACACHE });

        server.AnnoyingClient->DeleteTopic2(DEFAULT_TOPIC_NAME);
        server.AnnoyingClient->WriteToPQ(
                DEFAULT_TOPIC_NAME, 1, "abacaba", 2, "valuevaluevalue1", "",
                ETransport::MsgBus, NMsgBusProxy::MSTATUS_ERROR, NMsgBusProxy::MSTATUS_ERROR
        );
        server.AnnoyingClient->WriteToPQ(
                DEFAULT_TOPIC_NAME, 2, "abacaba", 1, "valuevaluevalue1", "",
                ETransport::MsgBus, NMsgBusProxy::MSTATUS_ERROR, NMsgBusProxy::MSTATUS_ERROR
        );
    }

    Y_UNIT_TEST(WriteAfterCreateDeleteCreate) {
        TTestServer server(PQSettings(0).SetDomainName("Root").SetNodeCount(2));
        TString topic = "rt3.dc1--johnsnow";
        server.AnnoyingClient->CreateTopic(topic, 2);

        server.AnnoyingClient->WriteToPQ(topic, 1, "abacaba", 1, "valuevaluevalue1");
        server.AnnoyingClient->WriteToPQ(topic, 3, "abacaba", 1, "valuevaluevalue1", "", ETransport::MsgBus, NMsgBusProxy::MSTATUS_ERROR, NMsgBusProxy::MSTATUS_ERROR);
        server.EnableLogs({ NKikimrServices::FLAT_TX_SCHEMESHARD, NKikimrServices::PQ_METACACHE });

        server.AnnoyingClient->DeleteTopic2(topic);

        server.AnnoyingClient->CreateTopic(topic, 4);

        // Write to topic, cache must be updated by CreateTopic
        server.AnnoyingClient->WriteToPQ(topic, 1, "abacaba", 1, "valuevaluevalue1");
        // Write to partition that didn't exist in the old topic
        server.AnnoyingClient->WriteToPQ(topic, 3, "abacaba", 1, "valuevaluevalue1");
    }

    Y_UNIT_TEST(GetOffsetsAfterDelete) {
        TTestServer server(PQSettings(0).SetDomainName("Root").SetNodeCount(2));
        TString topic2 = "rt3.dc1--topic2";
        server.AnnoyingClient->CreateTopic(DEFAULT_TOPIC_NAME, 3);
        server.AnnoyingClient->CreateTopic(topic2, 3);

        server.AnnoyingClient->WriteToPQ(DEFAULT_TOPIC_NAME, 1, "abacaba", 1, "valuevaluevalue1");
        server.EnableLogs({ NKikimrServices::FLAT_TX_SCHEMESHARD, NKikimrServices::PQ_METACACHE });

        server.AnnoyingClient->DeleteTopic2(DEFAULT_TOPIC_NAME);

        // Get offsets from deleted topic
        server.AnnoyingClient->GetPartOffset( {
                                          {DEFAULT_TOPIC_NAME, {1,2}}
                                      }, 0, 0, false);

        // Get offsets from multiple topics
        server.AnnoyingClient->GetPartOffset( {
                                          {DEFAULT_TOPIC_NAME, {1,2}},
                                          {topic2, {1,2}},
                                      }, 0, 0, false);
    }


    Y_UNIT_TEST(GetOffsetsAfterCreateDeleteCreate) {
        TTestServer server(PQSettings(0).SetDomainName("Root").SetNodeCount(2));
        TString topic2 = "rt3.dc1--topic2";
        server.AnnoyingClient->CreateTopic(DEFAULT_TOPIC_NAME, 3);
        server.AnnoyingClient->CreateTopic(topic2, 3);

        server.AnnoyingClient->WriteToPQ(DEFAULT_TOPIC_NAME, 1, "abacaba", 1, "valuevaluevalue1");

        server.EnableLogs({ NKikimrServices::FLAT_TX_SCHEMESHARD, NKikimrServices::PQ_METACACHE });

        server.AnnoyingClient->DeleteTopic2(DEFAULT_TOPIC_NAME);
        Sleep(TDuration::Seconds(1));

        server.AnnoyingClient->CreateTopic(DEFAULT_TOPIC_NAME, 4);
        Sleep(TDuration::Seconds(1));

        // Get offsets from multiple topics
        server.AnnoyingClient->GetPartOffset( {
                                          {DEFAULT_TOPIC_NAME, {1,2}},
                                          {topic2, {1}},
                                      }, 3, 0, true);
    }

    Y_UNIT_TEST(BigRead) {
        TTestServer server;
        server.AnnoyingClient->CreateTopic(DEFAULT_TOPIC_NAME, 1, 8_MB, 86400, 20000000, "user", 2000000);

        server.EnableLogs( { NKikimrServices::FLAT_TX_SCHEMESHARD });

        TString value(1_MB, 'x');
        for (ui32 i = 0; i < 32; ++i)
            server.AnnoyingClient->WriteToPQ({DEFAULT_TOPIC_NAME, 0, "source1", i}, value);

        // trying to read small PQ messages in a big messagebus event
        auto info = server.AnnoyingClient->ReadFromPQ({DEFAULT_TOPIC_NAME, 0, 0, 32, "user"}, 23, "", NMsgBusProxy::MSTATUS_OK); //will read 21mb
        UNIT_ASSERT_VALUES_EQUAL(info.BlobsFromDisk, 0);
        UNIT_ASSERT_VALUES_EQUAL(info.BlobsFromCache, 4);

        TInstant now(TInstant::Now());
        info = server.AnnoyingClient->ReadFromPQ({DEFAULT_TOPIC_NAME, 0, 0, 32, "user"}, 23, "", NMsgBusProxy::MSTATUS_OK); //will read 21mb
        TDuration dur = TInstant::Now() - now;
        UNIT_ASSERT_C(dur > TDuration::Seconds(7) && dur < TDuration::Seconds(20), "dur = " << dur); //speed limit is 2000kb/s and burst is 2000kb, so to read 24mb it will take at least 11 seconds

        server.AnnoyingClient->GetPartStatus({}, 1, true);

    }

    // expects that L2 size is 32Mb
    Y_UNIT_TEST(Cache) {
        TTestServer server;
        server.AnnoyingClient->CreateTopic(DEFAULT_TOPIC_NAME, 1, 8_MB);

        server.EnableLogs({ NKikimrServices::FLAT_TX_SCHEMESHARD });

        TString value(1_MB, 'x');
        for (ui32 i = 0; i < 32; ++i)
            server.AnnoyingClient->WriteToPQ({DEFAULT_TOPIC_NAME, 0, "source1", i}, value);

        auto info0 = server.AnnoyingClient->ReadFromPQ({DEFAULT_TOPIC_NAME, 0, 0, 16, "user"}, 16);
        auto info16 = server.AnnoyingClient->ReadFromPQ({DEFAULT_TOPIC_NAME, 0, 16, 16, "user"}, 16);

        UNIT_ASSERT_VALUES_EQUAL(info0.BlobsFromCache, 3);
        UNIT_ASSERT_VALUES_EQUAL(info16.BlobsFromCache, 2);
        UNIT_ASSERT_VALUES_EQUAL(info0.BlobsFromDisk + info16.BlobsFromDisk, 0);

        for (ui32 i = 0; i < 8; ++i)
            server.AnnoyingClient->WriteToPQ({DEFAULT_TOPIC_NAME, 0, "source1", 32+i}, value);

        info0 = server.AnnoyingClient->ReadFromPQ({DEFAULT_TOPIC_NAME, 0, 0, 16, "user"}, 16);
        info16 = server.AnnoyingClient->ReadFromPQ({DEFAULT_TOPIC_NAME, 0, 16, 16, "user"}, 16);

        ui32 fromDisk = info0.BlobsFromDisk + info16.BlobsFromDisk;
        ui32 fromCache = info0.BlobsFromCache + info16.BlobsFromCache;
        UNIT_ASSERT(fromDisk > 0);
        UNIT_ASSERT(fromDisk < 5);
        UNIT_ASSERT(fromCache > 0);
        UNIT_ASSERT(fromCache < 5);
    }

    Y_UNIT_TEST(CacheHead) {
        TTestServer server;
        server.AnnoyingClient->CreateTopic(DEFAULT_TOPIC_NAME, 1, 6_MB);
        server.EnableLogs({ NKikimrServices::FLAT_TX_SCHEMESHARD });

        ui64 seqNo = 0;
        for (ui32 blobSizeKB = 256; blobSizeKB < 4096; blobSizeKB *= 2) {
            static const ui32 maxEventKB = 24_KB;
            ui32 blobSize = blobSizeKB * 1_KB;
            ui32 count = maxEventKB / blobSizeKB;
            count -= count%2;
            ui32 half = count/2;

            ui64 offset = seqNo;
            TString value(blobSize, 'a');
            for (ui32 i = 0; i < count; ++i)
                server.AnnoyingClient->WriteToPQ({DEFAULT_TOPIC_NAME, 0, "source1", seqNo++}, value);

            auto info_half1 = server.AnnoyingClient->ReadFromPQ({DEFAULT_TOPIC_NAME, 0, offset, half, "user1"}, half);
            auto info_half2 = server.AnnoyingClient->ReadFromPQ({DEFAULT_TOPIC_NAME, 0, offset, half, "user1"}, half);

            UNIT_ASSERT(info_half1.BlobsFromCache > 0);
            UNIT_ASSERT(info_half2.BlobsFromCache > 0);
            UNIT_ASSERT_VALUES_EQUAL(info_half1.BlobsFromDisk, 0);
            UNIT_ASSERT_VALUES_EQUAL(info_half2.BlobsFromDisk, 0);
        }
    }

    Y_UNIT_TEST(SameOffset) {
        TTestServer server;
        TString topic2 = "rt3.dc1--topic2";
        server.AnnoyingClient->CreateTopic(DEFAULT_TOPIC_NAME, 1, 6_MB);
        server.AnnoyingClient->CreateTopic(topic2, 1, 6_MB);

        server.EnableLogs({ NKikimrServices::FLAT_TX_SCHEMESHARD });

        ui32 valueSize = 128;
        TString value1(valueSize, 'a');
        TString value2(valueSize, 'b');
        server.AnnoyingClient->WriteToPQ({DEFAULT_TOPIC_NAME, 0, "source1", 0}, value1);
        server.AnnoyingClient->WriteToPQ({topic2, 0, "source1", 0}, value2);

        // avoid reading from head
        TString mb(1_MB, 'x');
        for (ui32 i = 1; i < 16; ++i) {
            server.AnnoyingClient->WriteToPQ({DEFAULT_TOPIC_NAME, 0, "source1", i}, mb);
            server.AnnoyingClient->WriteToPQ({topic2, 0, "source1", i}, mb);
        }

        auto info1 = server.AnnoyingClient->ReadFromPQ({DEFAULT_TOPIC_NAME, 0, 0, 1, "user1"}, 1);
        auto info2 = server.AnnoyingClient->ReadFromPQ({topic2, 0, 0, 1, "user1"}, 1);

        UNIT_ASSERT_VALUES_EQUAL(info1.BlobsFromCache, 1);
        UNIT_ASSERT_VALUES_EQUAL(info2.BlobsFromCache, 1);
        UNIT_ASSERT_VALUES_EQUAL(info1.Values.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(info2.Values.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(info1.Values[0].size(), valueSize);
        UNIT_ASSERT_VALUES_EQUAL(info2.Values[0].size(), valueSize);
        UNIT_ASSERT(info1.Values[0] == value1);
        UNIT_ASSERT(info2.Values[0] == value2);
    }


    Y_UNIT_TEST(FetchRequest) {
        TTestServer server;
        TString topic2 = "rt3.dc1--topic2";
        server.AnnoyingClient->CreateTopic(DEFAULT_TOPIC_NAME, 10);
        server.AnnoyingClient->CreateTopic(topic2, 10);

        ui32 valueSize = 128;
        TString value1(valueSize, 'a');
        TString value2(valueSize, 'b');
        server.AnnoyingClient->WriteToPQ({topic2, 5, "source1", 0}, value2);
        server.AnnoyingClient->WriteToPQ({DEFAULT_TOPIC_NAME, 1, "source1", 0}, value1);
        server.AnnoyingClient->WriteToPQ({DEFAULT_TOPIC_NAME, 1, "source1", 1}, value2);

        server.EnableLogs({ NKikimrServices::FLAT_TX_SCHEMESHARD });
        TInstant tm(TInstant::Now());
        server.AnnoyingClient->FetchRequestPQ(
                { {topic2, 5, 0, 400}, {DEFAULT_TOPIC_NAME, 1, 0, 400}, {DEFAULT_TOPIC_NAME, 3, 0, 400} },
                400, 1000000
        );
        UNIT_ASSERT((TInstant::Now() - tm).Seconds() < 1);
        tm = TInstant::Now();
        server.AnnoyingClient->FetchRequestPQ({{topic2, 5, 1, 400}}, 400, 5000);
        UNIT_ASSERT((TInstant::Now() - tm).Seconds() > 2);
        server.AnnoyingClient->FetchRequestPQ(
                { {topic2, 5, 0, 400}, {DEFAULT_TOPIC_NAME, 1, 0, 400}, {DEFAULT_TOPIC_NAME, 3, 0, 400} },
                1, 1000000
        );
        server.AnnoyingClient->FetchRequestPQ(
                { {topic2, 5, 500, 400}, {topic2, 4, 0, 400}, {DEFAULT_TOPIC_NAME, 1, 0, 400} },
                400, 1000000
        );
    }

    Y_UNIT_TEST(ChooseProxy) {
        TTestServer server;
        server.AnnoyingClient->ChooseProxy(ETransport::GRpc);
    }


    Y_UNIT_TEST(Init) {
        TTestServer server(PQSettings(0).SetDomainName("Root").SetNodeCount(2));
        TString topic2 = "rt3.dc1--topic2";
        TString topic3 = "rt3.dc1--topic3";

        if (!true) {
            server.EnableLogs({
                NKikimrServices::FLAT_TX_SCHEMESHARD,
                NKikimrServices::TX_DATASHARD,
                NKikimrServices::HIVE,
                NKikimrServices::PERSQUEUE,
                NKikimrServices::TABLET_MAIN,
                NKikimrServices::BS_PROXY_DISCOVER,
                NKikimrServices::PIPE_CLIENT,
                NKikimrServices::PQ_METACACHE });
        }

        server.AnnoyingClient->DescribeTopic({});
        server.AnnoyingClient->TestCase({}, 0, 0, true);

        server.AnnoyingClient->CreateTopic(DEFAULT_TOPIC_NAME, 10);
        server.AnnoyingClient->AlterTopic(DEFAULT_TOPIC_NAME, 20);
        server.AnnoyingClient->CreateTopic(topic2, 25);

        server.AnnoyingClient->WriteToPQ(DEFAULT_TOPIC_NAME, 5, "abacaba", 1, "valuevaluevalue1");
        server.AnnoyingClient->WriteToPQ(DEFAULT_TOPIC_NAME, 5, "abacaba", 2, "valuevaluevalue2");
        server.AnnoyingClient->WriteToPQ(DEFAULT_TOPIC_NAME, 5, "abacabae", 1, "valuevaluevalue3");
        server.AnnoyingClient->ReadFromPQ(DEFAULT_TOPIC_NAME, 5, 0, 10, 3);

        server.AnnoyingClient->SetClientOffsetPQ(DEFAULT_TOPIC_NAME, 5, 2);

        server.AnnoyingClient->TestCase({{DEFAULT_TOPIC_NAME, {5}}}, 1, 1, true);
        server.AnnoyingClient->TestCase({{DEFAULT_TOPIC_NAME, {0}}}, 1, 0, true);
        server.AnnoyingClient->TestCase({{DEFAULT_TOPIC_NAME, {}}}, 20, 1, true);
        server.AnnoyingClient->TestCase({{DEFAULT_TOPIC_NAME, {5, 5}}}, 0, 0, false);
        server.AnnoyingClient->TestCase({{DEFAULT_TOPIC_NAME, {111}}}, 0, 0, false);
        server.AnnoyingClient->TestCase({}, 45, 1, true);
        server.AnnoyingClient->TestCase({{topic3, {}}}, 0, 0, false);
        server.AnnoyingClient->TestCase({{DEFAULT_TOPIC_NAME, {}}, {topic3, {}}}, 0, 0, false);
        server.AnnoyingClient->TestCase({{DEFAULT_TOPIC_NAME, {}}, {topic2, {}}}, 45, 1, true);
        server.AnnoyingClient->TestCase({{DEFAULT_TOPIC_NAME, {0, 3, 5}}, {topic2, {1, 4, 6, 8}}}, 7, 1, true);

        server.AnnoyingClient->DescribeTopic({DEFAULT_TOPIC_NAME});
        server.AnnoyingClient->DescribeTopic({topic2});
        server.AnnoyingClient->DescribeTopic({topic2, DEFAULT_TOPIC_NAME});
        server.AnnoyingClient->DescribeTopic({});
        server.AnnoyingClient->DescribeTopic({topic3}, true);
    }


    Y_UNIT_TEST(DescribeAfterDelete) {
        TTestServer server(PQSettings(0).SetDomainName("Root").SetNodeCount(2));
        server.EnableLogs({ NKikimrServices::PQ_METACACHE });
        TString topic2 = "rt3.dc1--topic2";

        server.AnnoyingClient->DescribeTopic({});
        server.AnnoyingClient->CreateTopic(DEFAULT_TOPIC_NAME, 10);
        server.AnnoyingClient->CreateTopic(topic2, 10);
        server.AnnoyingClient->DescribeTopic({});

        server.AnnoyingClient->DeleteTopic2(DEFAULT_TOPIC_NAME);

        server.AnnoyingClient->DescribeTopic({});
        server.AnnoyingClient->GetClientInfo({}, "user", true);
        server.AnnoyingClient->GetClientInfo({topic2}, "user", true);
        Sleep(TDuration::Seconds(2));
        server.AnnoyingClient->TestCase({{DEFAULT_TOPIC_NAME, {}}, {topic2, {}}}, 10, 0, false);
        server.AnnoyingClient->TestCase({{DEFAULT_TOPIC_NAME, {}}, {topic2, {}}, {"rt3.dc1--topic3", {}}}, 10, 0, false);
    }

    Y_UNIT_TEST(DescribeAfterDelete2) {
        TTestServer server(PQSettings(0).SetDomainName("Root").SetNodeCount(2));
        server.EnableLogs({ NKikimrServices::PQ_METACACHE });
        TString topic3 = "rt3.dc1--topic3";

        server.AnnoyingClient->CreateTopic(topic3, 10);
        Sleep(TDuration::Seconds(3)); //for invalidation of cache
        server.AnnoyingClient->TestCase({{"rt3.dc1--topic4", {}}}, 10, 0, false); //will force caching of topic3
        server.AnnoyingClient->DeleteTopic2(topic3);
        server.AnnoyingClient->DescribeTopic({topic3}, true); //describe will fail
        server.AnnoyingClient->DescribeTopic({topic3}, true); //describe will fail
    }


    void WaitResolveSuccess(TTestServer& server, TString topic, ui32 numParts) {
        const TInstant start = TInstant::Now();
        while (true) {
            TAutoPtr<NMsgBusProxy::TBusPersQueue> request(new NMsgBusProxy::TBusPersQueue);
            auto req = request->Record.MutableMetaRequest();
            auto partOff = req->MutableCmdGetPartitionLocations();
            auto treq = partOff->AddTopicRequest();
            treq->SetTopic(topic);
            for (ui32 i = 0; i < numParts; ++i)
                treq->AddPartition(i);

            TAutoPtr<NBus::TBusMessage> reply;
            NBus::EMessageStatus status = server.AnnoyingClient->SyncCall(request, reply);
            UNIT_ASSERT_VALUES_EQUAL(status, NBus::MESSAGE_OK);
            const NMsgBusProxy::TBusResponse* response = dynamic_cast<NMsgBusProxy::TBusResponse*>(reply.Get());
            UNIT_ASSERT(response);
            if (response->Record.GetStatus() == NMsgBusProxy::MSTATUS_OK)
                break;
            UNIT_ASSERT(TInstant::Now() - start < ::DEFAULT_DISPATCH_TIMEOUT);
            Sleep(TDuration::MilliSeconds(10));
        }
    }

    Y_UNIT_TEST(WhenDisableNodeAndCreateTopic_ThenAllPartitionsAreOnOtherNode) {
        // Arrange
        TTestServer server(PQSettings(0).SetDomainName("Root").SetNodeCount(2));
        server.EnableLogs({ NKikimrServices::HIVE });
        TString unused = "rt3.dc1--unusedtopic";
        // Just to make sure that HIVE has started
        server.AnnoyingClient->CreateTopic(unused, 1);
        WaitResolveSuccess(server, unused, 1);

        // Act
        // Disable node #0
        server.AnnoyingClient->MarkNodeInHive(server.CleverServer->GetRuntime(), 0, false);
        server.AnnoyingClient->CreateTopic(DEFAULT_TOPIC_NAME, 3);
        WaitResolveSuccess(server, DEFAULT_TOPIC_NAME, 3);

        // Assert that all partitions are on node #1
        const ui32 node1Id = server.CleverServer->GetRuntime()->GetNodeId(1);
        UNIT_ASSERT_VALUES_EQUAL(
            server.AnnoyingClient->GetPartLocation({{DEFAULT_TOPIC_NAME, {0, 1}}}, 2, true),
            TVector<ui32>({node1Id, node1Id})
        );
    }

    void PrepareForGrpc(TTestServer& server) {
        server.AnnoyingClient->CreateTopic(DEFAULT_TOPIC_NAME, 2);
        server.AnnoyingClient->InitUserRegistry();
    }
    void PrepareForFstClass(TFlatMsgBusPQClient& annoyingClient) {
        annoyingClient.SetNoConfigMode();
        annoyingClient.FullInit({});
        annoyingClient.InitUserRegistry();
        annoyingClient.MkDir("/Root", "account1");
        annoyingClient.CreateTopicNoLegacy(FC_TOPIC_PATH, 5);
    }

    Y_UNIT_TEST(CheckACLForGrpcWrite) {
        TTestServer server;
        PrepareForGrpc(server);

        auto writer = MakeDataWriter(server, "source1");

        writer.Write(SHORT_TOPIC_NAME, "valuevaluevalue1", true, "topic1@" BUILTIN_ACL_DOMAIN);

        NACLib::TDiffACL acl;
        acl.AddAccess(NACLib::EAccessType::Allow, NACLib::UpdateRow, "topic1@" BUILTIN_ACL_DOMAIN);
        server.AnnoyingClient->ModifyACL("/Root/PQ", DEFAULT_TOPIC_NAME, acl.SerializeAsString());

        Sleep(TDuration::Seconds(2));

        auto writer2 = MakeDataWriter(server, "source1");
        writer2.Write(SHORT_TOPIC_NAME, "valuevaluevalue1", false, "topic1@" BUILTIN_ACL_DOMAIN);
    }

    void PrepareForGrpcNoDC(TFlatMsgBusPQClient& annoyingClient) {
        annoyingClient.SetNoConfigMode();
        annoyingClient.FullInit();
        annoyingClient.InitUserRegistry();
        annoyingClient.MkDir("/Root", "account1");
        annoyingClient.MkDir("/Root/PQ", "account1");
        annoyingClient.CreateTopicNoLegacy("/Root/PQ/rt3.db--topic1", 5, false);
        annoyingClient.CreateTopicNoLegacy("/Root/PQ/account1/topic1", 5, false, true, {}, {"user1", "user2"});
        annoyingClient.CreateTopicNoLegacy("/Root/account2/topic2", 5);
   }

    Y_UNIT_TEST(TestGrpcWriteNoDC) {
        TTestServer server(false);
        server.ServerSettings.PQConfig.SetTopicsAreFirstClassCitizen(true);
        server.ServerSettings.PQConfig.SetRoot("/Rt2/PQ");
        server.ServerSettings.PQConfig.SetDatabase("/Root");
        server.StartServer();

        UNIT_ASSERT_VALUES_EQUAL(NMsgBusProxy::MSTATUS_OK,
                                        server.AnnoyingClient->AlterUserAttributes("/", "Root", {{"folder_id", "somefolder"}, {"cloud_id", "somecloud"}, {"database_id", "root"}}));


        PrepareForGrpcNoDC(*server.AnnoyingClient);
        auto writer = MakeDataWriter(server, "source1");

        writer.Write("/Root/account2/topic2", {"valuevaluevalue1"}, true, "topic1@" BUILTIN_ACL_DOMAIN);
        writer.Write("/Root/PQ/account1/topic1", {"valuevaluevalue1"}, true, "topic1@" BUILTIN_ACL_DOMAIN);

        NACLib::TDiffACL acl;
        acl.AddAccess(NACLib::EAccessType::Allow, NACLib::UpdateRow, "topic1@" BUILTIN_ACL_DOMAIN);
        acl.AddAccess(NACLib::EAccessType::Allow, NACLib::SelectRow, "topic1@" BUILTIN_ACL_DOMAIN);

        server.AnnoyingClient->ModifyACL("/Root/account2", "topic2", acl.SerializeAsString());
        server.AnnoyingClient->ModifyACL("/Root/PQ/account1", "topic1", acl.SerializeAsString());

        Sleep(TDuration::Seconds(5));

        writer.Write("/Root/account2/topic2", {"valuevaluevalue1"}, false, "topic1@" BUILTIN_ACL_DOMAIN);

        writer.Write("/Root/PQ/account1/topic1", {"valuevaluevalue1"}, false, "topic1@" BUILTIN_ACL_DOMAIN);
        writer.Write("/Root/PQ/account1/topic1", {"valuevaluevalue2"}, false, "topic1@" BUILTIN_ACL_DOMAIN);

        writer.Read("/Root/PQ/account1/topic1", "user1", "topic1@" BUILTIN_ACL_DOMAIN, false, false, false, true);

        writer.Write("Root/PQ/account1/topic1", {"valuevaluevalue2"}, false, "topic1@" BUILTIN_ACL_DOMAIN); //TODO /Root remove

        writer.Read("Root/PQ/account1/topic1", "user1", "topic1@" BUILTIN_ACL_DOMAIN, false, false, false, true);
    }

    Y_UNIT_TEST(CheckACLForGrpcRead) {
        TTestServer server(PQSettings(0, 1));
        PrepareForGrpc(server);
        TString topic2 = "rt3.dc1--topic2";
        TString topic2ShortName = "topic2";
        server.AnnoyingClient->CreateTopic(
                topic2, 1, 8_MB, 86400, 20000000, "", 200000000, {"user1", "user2"}
        );
        server.EnableLogs({NKikimrServices::PERSQUEUE}, NActors::NLog::PRI_INFO);

        server.AnnoyingClient->CreateConsumer("user1");
        server.AnnoyingClient->CreateConsumer("user2");
        server.AnnoyingClient->CreateConsumer("user5");
        server.AnnoyingClient->GrantConsumerAccess("user1", "user2@" BUILTIN_ACL_DOMAIN);
        server.AnnoyingClient->GrantConsumerAccess("user1", "user3@" BUILTIN_ACL_DOMAIN);

        server.AnnoyingClient->GrantConsumerAccess("user1", "1@" BUILTIN_ACL_DOMAIN);
        server.AnnoyingClient->GrantConsumerAccess("user2", "2@" BUILTIN_ACL_DOMAIN);
        server.AnnoyingClient->GrantConsumerAccess("user5", "1@" BUILTIN_ACL_DOMAIN);
        server.AnnoyingClient->GrantConsumerAccess("user5", "2@" BUILTIN_ACL_DOMAIN);

        auto writer = MakeDataWriter(server, "source1");

        NACLib::TDiffACL acl;
        acl.AddAccess(NACLib::EAccessType::Allow, NACLib::SelectRow, "1@" BUILTIN_ACL_DOMAIN);
        acl.AddAccess(NACLib::EAccessType::Allow, NACLib::SelectRow, "2@" BUILTIN_ACL_DOMAIN);
        acl.AddAccess(NACLib::EAccessType::Allow, NACLib::SelectRow, "user1@" BUILTIN_ACL_DOMAIN);
        acl.AddAccess(NACLib::EAccessType::Allow, NACLib::SelectRow, "user2@" BUILTIN_ACL_DOMAIN);
        server.AnnoyingClient->ModifyACL("/Root/PQ", topic2, acl.SerializeAsString());

        Sleep(TDuration::Seconds(2));

        auto ticket1 = "1@" BUILTIN_ACL_DOMAIN;
        auto ticket2 = "2@" BUILTIN_ACL_DOMAIN;

        writer.Read(topic2ShortName, "user1", ticket1, false, false, false, true);

        writer.Read(topic2ShortName, "user1", "user2@" BUILTIN_ACL_DOMAIN, false, false, false, true);
        writer.Read(topic2ShortName, "user1", "user3@" BUILTIN_ACL_DOMAIN, true, false, false, true); //for topic
        writer.Read(topic2ShortName, "user1", "user1@" BUILTIN_ACL_DOMAIN, true, false, false, true); //for consumer

        writer.Read(topic2ShortName, "user2", ticket1, true, false, false, true);
        writer.Read(topic2ShortName, "user2", ticket2, false, false, false, true);

        writer.Read(topic2ShortName, "user5", ticket1, true, false, false, true);
        writer.Read(topic2ShortName, "user5", ticket2, true, false, false, true);

        acl.Clear();
        acl.AddAccess(NACLib::EAccessType::Allow, NACLib::SelectRow, "user3@" BUILTIN_ACL_DOMAIN);
        server.AnnoyingClient->ModifyACL("/Root/PQ", topic2, acl.SerializeAsString());

        Sleep(TDuration::Seconds(2));
        writer.Read(topic2ShortName, "user1", "user3@" BUILTIN_ACL_DOMAIN, false, true, false, true);
    }



    Y_UNIT_TEST(CheckKillBalancer) {
        TTestServer server;
        server.EnableLogs({ NKikimrServices::PQ_WRITE_PROXY, NKikimrServices::PQ_READ_PROXY});
        server.EnableLogs({ NKikimrServices::PERSQUEUE}, NActors::NLog::PRI_INFO);

        PrepareForGrpc(server);

        auto writer = MakeDataWriter(server, "source1");
        TTestPQLib PQLib(server);
        auto [consumer, p] = PQLib.CreateConsumer({SHORT_TOPIC_NAME}, "shared/user", {}, true);
        Cerr << p.Response << "\n";

        auto msg = consumer->GetNextMessage();
        msg.Wait();
        Cerr << msg.GetValue().Response << "\n";
        UNIT_ASSERT(msg.GetValue().Type == EMT_LOCK);
        auto pp = msg.GetValue().ReadyToRead;
        pp.SetValue(TLockInfo());

        msg = consumer->GetNextMessage();
        msg.Wait();

        Cerr << msg.GetValue().Response << "\n";
        UNIT_ASSERT(msg.GetValue().Type == EMT_LOCK);
        pp = msg.GetValue().ReadyToRead;
        pp.SetValue(TLockInfo());

        msg = consumer->GetNextMessage();

        //TODO: make here infly commits - check results

        server.AnnoyingClient->RestartBalancerTablet(server.CleverServer->GetRuntime(), DEFAULT_TOPIC_NAME);
        Cerr << "Balancer killed\n";

        msg.Wait();
        Cerr << msg.GetValue().Response << "\n";
        UNIT_ASSERT(msg.GetValue().Type == EMT_RELEASE);
        UNIT_ASSERT(!msg.GetValue().Response.GetRelease().GetCanCommit());

        msg = consumer->GetNextMessage();
        msg.Wait();
        Cerr << msg.GetValue().Response << "\n";
        UNIT_ASSERT(msg.GetValue().Type == EMT_RELEASE);
        UNIT_ASSERT(!msg.GetValue().Response.GetRelease().GetCanCommit());

        msg = consumer->GetNextMessage();
        msg.Wait();
        Cerr << msg.GetValue().Response << "\n";
        UNIT_ASSERT(msg.GetValue().Type == EMT_LOCK);

        msg = consumer->GetNextMessage();
        msg.Wait();
        Cerr << msg.GetValue().Response << "\n";
        UNIT_ASSERT(msg.GetValue().Type == EMT_LOCK);


        msg = consumer->GetNextMessage();
        bool res = msg.Wait(TDuration::Seconds(1));
        Y_VERIFY(!res); //no read signalled or lock signals
    }

    Y_UNIT_TEST(TestWriteStat) {
        auto testWriteStat = [](const TString& originallyProvidedConsumerName,
                                const TString& consumerName,
                                const TString& consumerPath) {
            auto checkCounters = [](auto monPort, const TString& session,
                                    const std::set<std::string>& canonicalSensorNames,
                                    const TString& clientDc, const TString& originDc,
                                    const TString& client, const TString& consumerPath) {
                NJson::TJsonValue counters;
                if (clientDc.empty() && originDc.empty()) {
                    counters = GetClientCountersLegacy(monPort, "pqproxy", session, client, consumerPath);
                } else {
                    counters = GetCountersLegacy(monPort, "pqproxy", session, "account/topic1",
                                                 clientDc, originDc, client, consumerPath);
                }
                const auto sensors = counters["sensors"].GetArray();
                std::set<std::string> sensorNames;
                std::transform(sensors.begin(), sensors.end(),
                               std::inserter(sensorNames, sensorNames.begin()),
                               [](auto& el) {
                                   return el["labels"]["sensor"].GetString();
                               });
                auto equal = sensorNames == canonicalSensorNames;
                UNIT_ASSERT(equal);
            };

            auto settings = PQSettings(0, 1, true, "10");
            TTestServer server(settings, false);
            server.PrepareNetDataFile(FormNetData());
            server.StartServer();
            server.EnableLogs({ NKikimrServices::PQ_WRITE_PROXY, NKikimrServices::NET_CLASSIFIER });

            const auto monPort = TPortManager().GetPort();
            auto Counters = server.CleverServer->GetGRpcServerRootCounters();
            NActors::TMon Monitoring({monPort, "localhost", 3, "root", "localhost", {}, {}, {}});
            Monitoring.RegisterCountersPage("counters", "Counters", Counters);
            Monitoring.Start();

            auto sender = server.CleverServer->GetRuntime()->AllocateEdgeActor();

            GetClassifierUpdate(*server.CleverServer, sender); //wait for initializing

            server.AnnoyingClient->CreateTopic("rt3.dc1--account--topic1", 10, 10000, 10000, 2000);

            auto writer = MakeDataWriter(server);
            TTestPQLib PQLib(server);
            auto [producer, res] = PQLib.CreateProducer("account--topic1", "base64:AAAAaaaa____----12", {}, ECodec::RAW);

            Cerr << res.Response << "\n";
            TInstant st(TInstant::Now());
            for (ui32 i = 1; i <= 5; ++i) {
                auto f = producer->Write(i,  TString(2000, 'a'));
                f.Wait();
                Cerr << f.GetValue().Response << "\n";
                if (i == 5) {
                    UNIT_ASSERT(TInstant::Now() - st > TDuration::Seconds(3));
                    UNIT_ASSERT(f.GetValue().Response.GetAck().GetStat().GetPartitionQuotedTimeMs() <=
                                f.GetValue().Response.GetAck().GetStat().GetTotalTimeInPartitionQueueMs() + 100);
                }
            }

            checkCounters(server.CleverServer->GetRuntime()->GetMonPort(),
                          "writeSession",
                          {
                              "BytesWrittenOriginal",
                              "CompactedBytesWrittenOriginal",
                              "MessagesWrittenOriginal",
                              "UncompressedBytesWrittenOriginal"
                          },
                          "", "cluster", "", ""
                          );

            checkCounters(monPort,
                          "writeSession",
                          {
                              "BytesInflight",
                              "BytesInflightTotal",
                              "Errors",
                              "SessionsActive",
                              "SessionsCreated",
                              "WithoutAuth"
                          },
                          "", "cluster", "", ""
                          );
            {
                auto [consumer, res] = PQLib.CreateConsumer({"account/topic1"}, originallyProvidedConsumerName,
                                                            {}, false);
                Cerr << res.Response << "\n";
                auto msg = consumer->GetNextMessage();
                msg.Wait();
                Cerr << msg.GetValue().Response << "\n";
                UNIT_ASSERT(msg.GetValue().Type == EMT_DATA);

                checkCounters(monPort,
                              "readSession",
                              {
                                  "Commits",
                                  "PartitionsErrors",
                                  "PartitionsInfly",
                                  "PartitionsLocked",
                                  "PartitionsReleased",
                                  "PartitionsToBeLocked",
                                  "PartitionsToBeReleased",
                                  "WaitsForData"
                              },
                              "", "cluster", "", ""
                              );

                checkCounters(monPort,
                              "readSession",
                              {
                                  "BytesInflight",
                                  "Errors",
                                  "PipeReconnects",
                                  "SessionsActive",
                                  "SessionsCreated",
                                  "PartsPerSession",
                                  "SessionsWithOldBatchingVersion",
                                  "WithoutAuth"
                              },
                              "", "", consumerName, consumerPath
                              );

                checkCounters(server.CleverServer->GetRuntime()->GetMonPort(),
                              "readSession",
                              {
                                  "BytesReadFromDC"
                              },
                              "Vla", "", "", ""
                              );

                checkCounters(server.CleverServer->GetRuntime()->GetMonPort(),
                              "readSession",
                              {
                                  "BytesRead",
                                  "MessagesRead"
                              },
                              "", "Dc1", "", ""
                              );

                checkCounters(server.CleverServer->GetRuntime()->GetMonPort(),
                              "readSession",
                              {
                                  "BytesRead",
                                  "MessagesRead"
                              },
                              "", "cluster", "", ""
                              );

                checkCounters(server.CleverServer->GetRuntime()->GetMonPort(),
                              "readSession",
                              {
                                  "BytesRead",
                                  "MessagesRead"
                              },
                              "", "cluster", "", ""
                              );

                checkCounters(server.CleverServer->GetRuntime()->GetMonPort(),
                              "readSession",
                              {
                                  "BytesRead",
                                  "MessagesRead"
                              },
                              "", "Dc1", consumerName, consumerPath
                              );
            }
        };

        testWriteStat("some@random@consumer", "some@random@consumer", "some/random/consumer");
        testWriteStat("some@user", "some@user", "some/user");
        testWriteStat("shared@user", "shared@user", "shared/user");
        testWriteStat("shared/user", "user", "shared/user");
        testWriteStat("user", "user", "shared/user");
        testWriteStat("some@random/consumer", "some@random@consumer", "some/random/consumer");
        testWriteStat("/some/user", "some@user", "some/user");
    }


    Y_UNIT_TEST(TestWriteStat1stClass) {
        auto testWriteStat1stClass = [](const TString& consumerPath) {
            const auto folderId = "somefolder";
            const auto cloudId = "somecloud";
            const auto databaseId = "root";
            const TString fullTopicName{"/Root/account/topic1"};
            const TString topicName{"account/topic1"};

            auto checkCounters =
                [cloudId, folderId, databaseId](auto monPort,
                                                const std::set<std::string>& canonicalSensorNames,
                                                const TString& stream, const TString& consumer,
                                                const TString& host, const TString& shard) {
                    auto counters = GetCounters1stClass(monPort, "datastreams", cloudId, databaseId,
                                                        folderId, stream, consumer, host, shard);
                    const auto sensors = counters["sensors"].GetArray();
                    std::set<std::string> sensorNames;
                    std::transform(sensors.begin(), sensors.end(),
                                   std::inserter(sensorNames, sensorNames.begin()),
                                   [](auto& el) {
                                       return el["labels"]["name"].GetString();
                                   });
                    auto equal = sensorNames == canonicalSensorNames;
                    UNIT_ASSERT(equal);
                };

            auto settings = PQSettings(0, 1, true, "10");
            TTestServer server(settings, false);
            server.PrepareNetDataFile(FormNetData());
            server.ServerSettings.PQConfig.SetTopicsAreFirstClassCitizen(true);
            server.StartServer();
            server.EnableLogs({ NKikimrServices::PQ_WRITE_PROXY, NKikimrServices::NET_CLASSIFIER });

            UNIT_ASSERT_VALUES_EQUAL(NMsgBusProxy::MSTATUS_OK,
                                     server.AnnoyingClient->AlterUserAttributes("/", "Root",
                                                                                {{"folder_id", folderId},
                                                                                 {"cloud_id", cloudId},
                                                                                 {"database_id", databaseId}}));

            const auto monPort = TPortManager().GetPort();
            auto Counters = server.CleverServer->GetGRpcServerRootCounters();
            NActors::TMon Monitoring({monPort, "localhost", 3, "root", "localhost", {}, {}, {}});
            Monitoring.RegisterCountersPage("counters", "Counters", Counters);
            Monitoring.Start();

            auto sender = server.CleverServer->GetRuntime()->AllocateEdgeActor();

            GetClassifierUpdate(*server.CleverServer, sender); //wait for initializing

            server.AnnoyingClient->SetNoConfigMode();
            server.AnnoyingClient->FullInit();
            server.AnnoyingClient->InitUserRegistry();
            server.AnnoyingClient->MkDir("/Root", "account");
            server.AnnoyingClient->CreateTopicNoLegacy(fullTopicName, 5);

            NYdb::TDriverConfig driverCfg;
            driverCfg.SetEndpoint(TStringBuilder() << "localhost:" << server.GrpcPort)
                .SetLog(CreateLogBackend("cerr", ELogPriority::TLOG_DEBUG)).SetDatabase("/Root");

            auto ydbDriver = MakeHolder<NYdb::TDriver>(driverCfg);
            auto persQueueClient = MakeHolder<NYdb::NPersQueue::TPersQueueClient>(*ydbDriver);

            {
                auto res = persQueueClient->AddReadRule(fullTopicName,
                                                        NYdb::NPersQueue::TAddReadRuleSettings().ReadRule(NYdb::NPersQueue::TReadRuleSettings().ConsumerName(consumerPath)));
                res.Wait();
                UNIT_ASSERT(res.GetValue().IsSuccess());
            }

            auto writer = MakeDataWriter(server);
            TTestPQLib PQLib(server);
            auto [producer, res] = PQLib.CreateProducer(fullTopicName, "base64:AAAAaaaa____----12", {},
                                                        ECodec::RAW);

            Cerr << res.Response << "\n";
            for (ui32 i = 1; i <= 5; ++i) {
                auto f = producer->Write(i,  TString(2000, 'a'));
                f.Wait();
                Cerr << f.GetValue().Response << "\n";

            }

            {
                auto [consumer, res] = PQLib.CreateConsumer({topicName}, consumerPath, {},
                                                            false);
                Cerr << res.Response << "\n";
                auto msg = consumer->GetNextMessage();
                msg.Wait();
                Cerr << msg.GetValue().Response << "\n";

                checkCounters(monPort,
                              {
                                  "stream.internal_read.commits_per_second",
                                  "stream.internal_read.partitions_errors_per_second",
                                  "stream.internal_read.partitions_locked",
                                  "stream.internal_read.partitions_locked_per_second",
                                  "stream.internal_read.partitions_released_per_second",
                                  "stream.internal_read.partitions_to_be_locked",
                                  "stream.internal_read.partitions_to_be_released",
                                  "stream.internal_read.waits_for_data",
                                  "stream.internal_write.bytes_proceeding",
                                  "stream.internal_write.bytes_proceeding_total",
                                  "stream.internal_write.errors_per_second",
                                  "stream.internal_write.sessions_active",
                                  "stream.internal_write.sessions_created_per_second",
                                  "stream.internal_write.sessions_without_auth"
                              },
                              topicName, "", "", ""
                              );

                checkCounters(monPort,
                              {
                                  "stream.internal_read.commits_per_second",
                                  "stream.internal_read.partitions_errors_per_second",
                                  "stream.internal_read.partitions_locked",
                                  "stream.internal_read.partitions_locked_per_second",
                                  "stream.internal_read.partitions_released_per_second",
                                  "stream.internal_read.partitions_to_be_locked",
                                  "stream.internal_read.partitions_to_be_released",
                                  "stream.internal_read.waits_for_data",
                              },
                              topicName, consumerPath, "", ""
                              );

                checkCounters(server.CleverServer->GetRuntime()->GetMonPort(),
                              {
                                  "stream.internal_read.time_lags_milliseconds",
                                  "stream.incoming_bytes_per_second",
                                  "stream.incoming_records_per_second",
                                  "stream.internal_write.bytes_per_second",
                                  "stream.internal_write.compacted_bytes_per_second",
                                  "stream.internal_write.partition_write_quota_wait_milliseconds",
                                  "stream.internal_write.record_size_bytes",
                                  "stream.internal_write.records_per_second",
                                  "stream.internal_write.time_lags_milliseconds",
                                  "stream.internal_write.uncompressed_bytes_per_second",
                                  "stream.await_operating_milliseconds",
                                  "stream.internal_write.buffer_brimmed_duration_ms",
                                  "stream.internal_read.bytes_per_second",
                                  "stream.internal_read.records_per_second",
                                  "stream.outgoing_bytes_per_second",
                                  "stream.outgoing_records_per_second",
                              },
                              topicName, "", "", ""
                              );

                checkCounters(server.CleverServer->GetRuntime()->GetMonPort(),
                              {
                                  "stream.internal_read.time_lags_milliseconds",
                                  "stream.await_operating_milliseconds",
                                  "stream.internal_read.bytes_per_second",
                                  "stream.internal_read.records_per_second",
                                  "stream.outgoing_bytes_per_second",
                                  "stream.outgoing_records_per_second",
                              },
                              topicName, consumerPath, "", ""
                              );

                checkCounters(server.CleverServer->GetRuntime()->GetMonPort(),
                              {
                                  "stream.await_operating_milliseconds"
                              },
                              topicName, consumerPath, "1", ""
                              );

                checkCounters(server.CleverServer->GetRuntime()->GetMonPort(),
                              {
                                  "stream.internal_write.buffer_brimmed_duration_ms"
                              },
                              topicName, "", "1", ""
                              );
            }
        };
        testWriteStat1stClass("some@random@consumer");
        testWriteStat1stClass("user1");
    }


    Y_UNIT_TEST(TestUnorderedCommit) {
        TTestServer server;
        server.AnnoyingClient->CreateTopic(DEFAULT_TOPIC_NAME, 3);
        server.EnableLogs({ NKikimrServices::PQ_READ_PROXY });

        auto writer = MakeDataWriter(server);
        TTestPQLib PQLib(server);

        for (ui32 i = 1; i <= 3; ++i) {
            auto [producer, res] = PQLib.CreateProducer(SHORT_TOPIC_NAME, "123" + ToString<ui32>(i), i, ECodec::RAW);
            UNIT_ASSERT(res.Response.HasInit());
            auto f = producer->Write(i,  TString(10, 'a'));
            f.Wait();
        }
        auto [consumer, res] = PQLib.CreateConsumer({SHORT_TOPIC_NAME}, "user", 1, false, false);
        Cerr << res.Response << "\n";
        for (ui32 i = 1; i <= 3; ++i) {
            auto msg = consumer->GetNextMessage();
            msg.Wait();
            Cerr << msg.GetValue().Response << "\n";
            UNIT_ASSERT(msg.GetValue().Response.HasData());
            UNIT_ASSERT(msg.GetValue().Response.GetData().GetCookie() == i);
        }
        auto msg = consumer->GetNextMessage();
        consumer->Commit({2});
        UNIT_ASSERT(!msg.Wait(TDuration::Seconds(1)));
        consumer->Commit({1});
        msg.Wait();
        Cerr << msg.GetValue().Response << "\n";
        UNIT_ASSERT(msg.GetValue().Response.HasCommit());
        UNIT_ASSERT(msg.GetValue().Response.GetCommit().CookieSize() == 2);
        UNIT_ASSERT(msg.GetValue().Response.GetCommit().GetCookie(0) == 1);
        UNIT_ASSERT(msg.GetValue().Response.GetCommit().GetCookie(1) == 2);
        consumer->Commit({3});
        msg = consumer->GetNextMessage();
        UNIT_ASSERT(!msg.Wait(TDuration::MilliSeconds(500)));
        msg.Wait();
        Cerr << msg.GetValue().Response << "\n";
        UNIT_ASSERT(msg.GetValue().Response.HasCommit());
        UNIT_ASSERT(msg.GetValue().Response.GetCommit().CookieSize() == 1);
        UNIT_ASSERT(msg.GetValue().Response.GetCommit().GetCookie(0) == 3);
        consumer->Commit({4}); //not existed cookie
        msg = consumer->GetNextMessage();
        msg.Wait();
        Cerr << msg.GetValue().Response << "\n";
        UNIT_ASSERT(msg.GetValue().Response.HasError());
    }

    Y_UNIT_TEST(TestMaxReadCookies) {
        TTestServer server;
        server.AnnoyingClient->CreateTopic(DEFAULT_TOPIC_NAME, 1);
        server.EnableLogs({ NKikimrServices::PQ_READ_PROXY });
        auto writer = MakeDataWriter(server);

        TTestPQLib PQLib(server);
        {
            auto [producer, res] = PQLib.CreateProducer(SHORT_TOPIC_NAME, "123");
            for (ui32 i = 1; i <= 11; ++i) {
                auto f = producer->Write(i,  TString(10, 'a'));
                f.Wait();
            }
        }
        auto [consumer, res] = PQLib.CreateConsumer({SHORT_TOPIC_NAME}, "user", 1, false);
        Cerr << res.Response << "\n";
        for (ui32 i = 1; i <= 11; ++i) {
            auto msg = consumer->GetNextMessage();
            msg.Wait();
            Cerr << msg.GetValue().Response << "\n";
            UNIT_ASSERT(msg.GetValue().Response.HasData());
            UNIT_ASSERT(msg.GetValue().Response.GetData().GetCookie() == i);
        }
        for (ui32 i = 11; i >= 1; --i) {
            consumer->Commit({i});
            if (i == 5) {
                server.AnnoyingClient->GetClientInfo({DEFAULT_TOPIC_NAME}, "user", true);
            }
        }
        Cerr << "cookies committed\n";
        auto msg = consumer->GetNextMessage();
        msg.Wait();
        Cerr << msg.GetValue().Response << "\n";
        UNIT_ASSERT(msg.GetValue().Response.HasError());
    }

    Y_UNIT_TEST(TestWriteSessionsConflicts) {
        TTestServer server;
        server.AnnoyingClient->CreateTopic(DEFAULT_TOPIC_NAME, 1);
        auto writer = MakeDataWriter(server);
        server.EnableLogs({ NKikimrServices::PQ_WRITE_PROXY });

        TTestPQLib PQLib(server);
        auto [producer, res] = PQLib.CreateProducer(SHORT_TOPIC_NAME, "123");
        auto dead = producer->IsDead();

        auto [producer2, res2] = PQLib.CreateProducer(SHORT_TOPIC_NAME, "123");

        UNIT_ASSERT(dead.Wait(TDuration::Seconds(1)));
        UNIT_ASSERT(!producer2->IsDead().Wait(TDuration::Seconds(1)));
    }

    Y_UNIT_TEST(TestWriteSessionNaming) {
        TTestServer server;
        server.EnableLogs({ NKikimrServices::PQ_WRITE_PROXY });


        TTestPQLib PQLib(server);
        while (true) {
            auto [producer, res] = PQLib.CreateProducer("//", "123");
            Cerr << res.Response << "\n";
            UNIT_ASSERT(res.Response.HasError());
            if (res.Response.GetError().GetCode() != NPersQueue::NErrorCode::INITIALIZING)
                break;
            Sleep(TDuration::Seconds(1));
        }
    }


    Y_UNIT_TEST(TestRelocks) {
        TTestServer server;
        server.AnnoyingClient->CreateTopic(DEFAULT_TOPIC_NAME, 1);

        server.EnableLogs({ NKikimrServices::PQ_READ_PROXY });

        auto writer = MakeDataWriter(server);

        TIntrusivePtr<TCerrLogger> logger = new TCerrLogger(DEBUG_LOG_LEVEL);
        TPQLib PQLib;

        TConsumerSettings ss;
        ss.ClientId = "user";
        ss.Server = TServerSetting{"localhost", server.GrpcPort};
        ss.Topics.push_back(SHORT_TOPIC_NAME);
        ss.UseLockSession = true;
        ss.MaxCount = 1;

        auto consumer = PQLib.CreateConsumer(ss, logger, false);
        auto p = consumer->Start();
        for (ui32 i = 0; i < 30; ++i) {
            server.CleverServer->GetRuntime()->ResetScheduledCount();
            server.AnnoyingClient->RestartPartitionTablets(server.CleverServer->GetRuntime(), DEFAULT_TOPIC_NAME);
            Sleep(TDuration::MilliSeconds(1));
        }
        p.Wait();
        Cerr << p.GetValue().Response << "\n";
        auto msg = consumer->GetNextMessage();
        msg.Wait();
        Cerr << msg.GetValue().Response << "\n";
        UNIT_ASSERT(msg.GetValue().Response.HasLock());
        UNIT_ASSERT(msg.GetValue().Response.GetLock().GetTopic() == DEFAULT_TOPIC_NAME);
        UNIT_ASSERT(msg.GetValue().Response.GetLock().GetPartition() == 0);
    }

    Y_UNIT_TEST(TestLockErrors) {
        TTestServer server;
        server.AnnoyingClient->CreateTopic(DEFAULT_TOPIC_NAME, 1);
        server.EnableLogs({ NKikimrServices::PQ_READ_PROXY });

        auto writer = MakeDataWriter(server);

        TTestPQLib PQLib(server);

        {
            auto [producer, res] = PQLib.CreateProducer(SHORT_TOPIC_NAME, "123");
            for (ui32 i = 1; i <= 11; ++i) {
                auto f = producer->Write(i,  TString(10, 'a'));
                f.Wait();
            }
        }
        auto createConsumer = [&] () {
            auto [consumer, res] = PQLib.CreateConsumer({SHORT_TOPIC_NAME}, "user", 1, true);
            return std::move(consumer);
        };
        auto consumer = createConsumer();
        auto msg = consumer->GetNextMessage();
        msg.Wait();
        Cerr << msg.GetValue().Response << "\n";
        UNIT_ASSERT(msg.GetValue().Response.HasLock());
        UNIT_ASSERT(msg.GetValue().Response.GetLock().GetTopic() == DEFAULT_TOPIC_NAME);
        UNIT_ASSERT(msg.GetValue().Response.GetLock().GetPartition() == 0);
        UNIT_ASSERT(msg.GetValue().Response.GetLock().GetReadOffset() == 0);
        UNIT_ASSERT(msg.GetValue().Response.GetLock().GetEndOffset() == 11);

        auto pp = msg.GetValue().ReadyToRead;
        pp.SetValue(TLockInfo{0, 5, false});
        auto future = consumer->IsDead();
        future.Wait();
        Cerr << future.GetValue() << "\n";


        consumer = createConsumer();
        msg = consumer->GetNextMessage();
        msg.Wait();
        Cerr << msg.GetValue().Response << "\n";
        UNIT_ASSERT(msg.GetValue().Response.HasLock());
        UNIT_ASSERT(msg.GetValue().Response.GetLock().GetTopic() == DEFAULT_TOPIC_NAME);
        UNIT_ASSERT(msg.GetValue().Response.GetLock().GetPartition() == 0);
        UNIT_ASSERT(msg.GetValue().Response.GetLock().GetReadOffset() == 0);
        UNIT_ASSERT(msg.GetValue().Response.GetLock().GetEndOffset() == 11);

        pp = msg.GetValue().ReadyToRead;
        pp.SetValue(TLockInfo{12, 12, false});
        future = consumer->IsDead();
        future.Wait();
        Cerr << future.GetValue() << "\n";

        consumer = createConsumer();
        msg = consumer->GetNextMessage();
        msg.Wait();
        Cerr << msg.GetValue().Response << "\n";
        UNIT_ASSERT(msg.GetValue().Response.HasLock());
        UNIT_ASSERT(msg.GetValue().Response.GetLock().GetTopic() == DEFAULT_TOPIC_NAME);
        UNIT_ASSERT(msg.GetValue().Response.GetLock().GetPartition() == 0);
        UNIT_ASSERT(msg.GetValue().Response.GetLock().GetReadOffset() == 0);
        UNIT_ASSERT(msg.GetValue().Response.GetLock().GetEndOffset() == 11);

        pp = msg.GetValue().ReadyToRead;
        pp.SetValue(TLockInfo{11, 11, false});
        Sleep(TDuration::Seconds(5));


        consumer = createConsumer();
        msg = consumer->GetNextMessage();
        msg.Wait();
        Cerr << msg.GetValue().Response << "\n";
        UNIT_ASSERT(msg.GetValue().Response.HasLock());
        UNIT_ASSERT(msg.GetValue().Response.GetLock().GetTopic() == DEFAULT_TOPIC_NAME);
        UNIT_ASSERT(msg.GetValue().Response.GetLock().GetPartition() == 0);
        UNIT_ASSERT(msg.GetValue().Response.GetLock().GetReadOffset() == 11);
        UNIT_ASSERT(msg.GetValue().Response.GetLock().GetEndOffset() == 11);

        pp = msg.GetValue().ReadyToRead;
        pp.SetValue(TLockInfo{1, 0, true});
        future = consumer->IsDead();
        future.Wait();
        Cerr << future.GetValue() << "\n";

        consumer = createConsumer();
        msg = consumer->GetNextMessage();
        msg.Wait();
        Cerr << msg.GetValue().Response << "\n";
        UNIT_ASSERT(msg.GetValue().Response.HasLock());
        UNIT_ASSERT(msg.GetValue().Response.GetLock().GetTopic() == DEFAULT_TOPIC_NAME);
        UNIT_ASSERT(msg.GetValue().Response.GetLock().GetPartition() == 0);
        UNIT_ASSERT(msg.GetValue().Response.GetLock().GetReadOffset() == 11);
        UNIT_ASSERT(msg.GetValue().Response.GetLock().GetEndOffset() == 11);

        pp = msg.GetValue().ReadyToRead;
        pp.SetValue(TLockInfo{0, 0, false});
        future = consumer->IsDead();
        UNIT_ASSERT(!future.Wait(TDuration::Seconds(5)));
    }


    Y_UNIT_TEST(TestLocalChoose) {
        TTestServer server(false);
        server.ServerSettings.NodeCount = 3;
        server.StartServer();
        server.AnnoyingClient->CreateTopic(DEFAULT_TOPIC_NAME, 1);

        server.EnableLogs({ NKikimrServices::CHOOSE_PROXY });

        auto channel = grpc::CreateChannel("localhost:"+ToString(server.GrpcPort), grpc::InsecureChannelCredentials());
        auto stub(NKikimrClient::TGRpcServer::NewStub(channel));

        auto sender = server.CleverServer->GetRuntime()->AllocateEdgeActor();

        auto nodeId = server.CleverServer->GetRuntime()->GetNodeId(0);
        server.CleverServer->GetRuntime()->Send(new IEventHandle(
                MakeGRpcProxyStatusID(nodeId), sender, new TEvGRpcProxyStatus::TEvSetup(true, 0, 0)), 0, false
        );
        nodeId = server.CleverServer->GetRuntime()->GetNodeId(1);
        server.CleverServer->GetRuntime()->Send(
                new IEventHandle(
                        MakeGRpcProxyStatusID(nodeId), sender, new TEvGRpcProxyStatus::TEvSetup(true, 10000000, 1000000)
                ), 1, false
        );
        nodeId = server.CleverServer->GetRuntime()->GetNodeId(2);
        server.CleverServer->GetRuntime()->Send(
                new IEventHandle(
                        MakeGRpcProxyStatusID(nodeId), sender, new TEvGRpcProxyStatus::TEvSetup(true, 10000000, 1000000)
                ), 2, false
        );

        grpc::ClientContext context;
        NKikimrClient::TChooseProxyRequest request;
        request.SetPreferLocalProxy(true);
        NKikimrClient::TResponse response;
        auto status = stub->ChooseProxy(&context, request, &response);
        UNIT_ASSERT(status.ok());
        Cerr << response << "\n";
        UNIT_ASSERT(response.GetStatus() == NMsgBusProxy::MSTATUS_OK);
        UNIT_ASSERT(response.GetProxyCookie() == server.CleverServer->GetRuntime()->GetNodeId(0));

        grpc::ClientContext context2;
        request.SetPreferLocalProxy(false);
        NKikimrClient::TResponse response2;
        status = stub->ChooseProxy(&context2, request, &response2);
        UNIT_ASSERT(status.ok());
        Cerr << response2 << "\n";
        UNIT_ASSERT(response2.GetStatus() == NMsgBusProxy::MSTATUS_OK);
        UNIT_ASSERT(response2.GetProxyCookie() > server.CleverServer->GetRuntime()->GetNodeId(0));
    }


    Y_UNIT_TEST(TestRestartBalancer) {
        TTestServer server;
        server.AnnoyingClient->CreateTopic(DEFAULT_TOPIC_NAME, 1);
        server.EnableLogs({ NKikimrServices::PQ_READ_PROXY });

        auto writer = MakeDataWriter(server);
        TTestPQLib PQLib(server);

        {
            auto [producer, res] = PQLib.CreateProducer(SHORT_TOPIC_NAME, "123");
            for (ui32 i = 1; i < 2; ++i) {
                auto f = producer->Write(i,  TString(10, 'a'));
                f.Wait();
            }
        }
        auto [consumer, res] = PQLib.CreateConsumer({SHORT_TOPIC_NAME}, "user", 1, true, {}, {}, 100, {});
        Cerr << res.Response << "\n";

        auto msg = consumer->GetNextMessage();
        msg.Wait();
        Cerr << msg.GetValue().Response << "\n";
        UNIT_ASSERT(msg.GetValue().Response.HasLock());
        UNIT_ASSERT(msg.GetValue().Response.GetLock().GetTopic() == DEFAULT_TOPIC_NAME);
        UNIT_ASSERT(msg.GetValue().Response.GetLock().GetPartition() == 0);


        msg.GetValue().ReadyToRead.SetValue(TLockInfo{0,0,false});

        msg = consumer->GetNextMessage();
        msg.Wait();
        Cerr << msg.GetValue().Response << "\n";
        UNIT_ASSERT(msg.GetValue().Response.HasData());
//        Sleep(TDuration::MilliSeconds(10));
        server.AnnoyingClient->RestartBalancerTablet(server.CleverServer->GetRuntime(), DEFAULT_TOPIC_NAME);

        msg = consumer->GetNextMessage();
        msg.Wait();

        Cerr << msg.GetValue().Response << "\n";
        UNIT_ASSERT_C(msg.GetValue().Response.HasRelease(), "Response: " << msg.GetValue().Response);
    }

    Y_UNIT_TEST(TestBigMessage) {
        TTestServer server(false);
        server.GrpcServerOptions.SetMaxMessageSize(130_MB);
        server.StartServer();
        server.AnnoyingClient->CreateTopic(DEFAULT_TOPIC_NAME, 1);
        server.EnableLogs({ NKikimrServices::PQ_WRITE_PROXY });

        auto writer = MakeDataWriter(server);
        TTestPQLib PQLib(server);
        auto [producer, res] = PQLib.CreateProducer(SHORT_TOPIC_NAME, "123");
        auto f = producer->Write(1,  TString(63_MB, 'a'));
        f.Wait();
        Cerr << f.GetValue().Response << "\n";
        UNIT_ASSERT_C(f.GetValue().Response.HasAck(), f.GetValue().Response);
    }

    void TestRereadsWhenDataIsEmptyImpl(bool withWait) {
        TTestServer server;
        server.AnnoyingClient->CreateTopic(DEFAULT_TOPIC_NAME, 1);
        server.EnableLogs({ NKikimrServices::PQ_READ_PROXY });

        auto writer = MakeDataWriter(server);
        TTestPQLib PQLib(server);

        // Write nonempty data
        NKikimr::NPersQueueTests::TRequestWritePQ writeReq(DEFAULT_TOPIC_NAME, 0, "src", 4);

        auto write = [&](const TString& data, bool empty = false) {
            NKikimrPQClient::TDataChunk dataChunk;
            dataChunk.SetCreateTime(42);
            dataChunk.SetSeqNo(++writeReq.SeqNo);
            dataChunk.SetData(data);
            if (empty) {
                dataChunk.SetChunkType(NKikimrPQClient::TDataChunk::GROW); // this guarantees that data will be threated as empty
            }
            TString serialized;
            UNIT_ASSERT(dataChunk.SerializeToString(&serialized));
            server.AnnoyingClient->WriteToPQ(writeReq, serialized);
        };
        write("data1");
        write("data2", true);
        if (!withWait) {
            write("data3");
        }

        auto [consumer, res] = PQLib.CreateConsumer({SHORT_TOPIC_NAME}, "user", 1, false, {}, {}, 1, 1);
        UNIT_ASSERT_C(res.Response.HasInit(), res.Response);
        auto msg1 = consumer->GetNextMessage().GetValueSync().Response;

        auto assertHasData = [](const NPersQueue::TReadResponse& msg, const TString& data) {
            const auto& d = msg.GetData();
            UNIT_ASSERT_VALUES_EQUAL_C(d.MessageBatchSize(), 1, msg);
            UNIT_ASSERT_VALUES_EQUAL_C(d.GetMessageBatch(0).MessageSize(), 1, msg);
            UNIT_ASSERT_STRINGS_EQUAL_C(d.GetMessageBatch(0).GetMessage(0).GetData(), data, msg);
        };
        UNIT_ASSERT_VALUES_EQUAL_C(msg1.GetData().GetCookie(), 1, msg1);
        assertHasData(msg1, "data1");

        auto resp2Future = consumer->GetNextMessage();
        if (withWait) {
            // no data
            UNIT_ASSERT(!resp2Future.HasValue());
            UNIT_ASSERT(!resp2Future.HasException());

            // waits and data doesn't arrive
            Sleep(TDuration::MilliSeconds(100));
            UNIT_ASSERT(!resp2Future.HasValue());
            UNIT_ASSERT(!resp2Future.HasException());

            // write data
            write("data3");
        }
        const auto& msg2 = resp2Future.GetValueSync().Response;
        UNIT_ASSERT_VALUES_EQUAL_C(msg2.GetData().GetCookie(), 2, msg2);
        assertHasData(msg2, "data3");
    }

    Y_UNIT_TEST(TestRereadsWhenDataIsEmpty) {
        TestRereadsWhenDataIsEmptyImpl(false);
    }

    Y_UNIT_TEST(TestRereadsWhenDataIsEmptyWithWait) {
        TestRereadsWhenDataIsEmptyImpl(true);
    }


    Y_UNIT_TEST(TestLockAfterDrop) {
        TTestServer server;
        server.AnnoyingClient->CreateTopic(DEFAULT_TOPIC_NAME, 1);
        server.EnableLogs({ NKikimrServices::PQ_READ_PROXY });

        auto writer = MakeDataWriter(server);
        TTestPQLib PQLib(server);

        auto [producer, res] = PQLib.CreateProducer(SHORT_TOPIC_NAME, "123");
        auto f = producer->Write(1,  TString(1_KB, 'a'));
        f.Wait();

        auto [consumer, res2] = PQLib.CreateConsumer({SHORT_TOPIC_NAME}, "user", 1, true);
        Cerr << res2.Response << "\n";
        auto msg = consumer->GetNextMessage();
        msg.Wait();
        UNIT_ASSERT_C(msg.GetValue().Response.HasLock(), msg.GetValue().Response);
        UNIT_ASSERT_C(msg.GetValue().Response.GetLock().GetTopic() == DEFAULT_TOPIC_NAME, msg.GetValue().Response);
        UNIT_ASSERT_C(msg.GetValue().Response.GetLock().GetPartition() == 0, msg.GetValue().Response);

        server.CleverServer->GetRuntime()->ResetScheduledCount();
        server.AnnoyingClient->RestartPartitionTablets(server.CleverServer->GetRuntime(), DEFAULT_TOPIC_NAME);

        msg.GetValue().ReadyToRead.SetValue({0,0,false});

        msg = consumer->GetNextMessage();
        UNIT_ASSERT(msg.Wait(TDuration::Seconds(10)));

        Cerr << msg.GetValue().Response << "\n";
        UNIT_ASSERT(msg.GetValue().Response.HasData());
    }


    Y_UNIT_TEST(TestMaxNewTopicModel) {
        TTestServer server;
        server.AnnoyingClient->AlterUserAttributes("/", "Root", {{"__extra_path_symbols_allowed", "@"}});
        server.AnnoyingClient->CreateTopic("rt3.dc1--aaa@bbb@ccc--topic", 1);
        server.AnnoyingClient->CreateTopic(DEFAULT_TOPIC_NAME, 1);
        server.EnableLogs({ NKikimrServices::PQ_READ_PROXY });

        auto writer = MakeDataWriter(server);
        TTestPQLib PQLib(server);
        {
            auto [producer, res] = PQLib.CreateProducer("aaa/bbb/ccc/topic", "123");
            UNIT_ASSERT_C(res.Response.HasInit(), res.Response);
            for (ui32 i = 1; i <= 11; ++i) {
                auto f = producer->Write(i,  TString(10, 'a'));
                f.Wait();
                UNIT_ASSERT_C(f.GetValue().Response.HasAck(), f.GetValue().Response);
            }
        }

        auto [consumer, res2] = PQLib.CreateConsumer({"aaa/bbb/ccc/topic"}, "user", 1, true);
        UNIT_ASSERT_C(res2.Response.HasInit(), res2.Response);

        auto msg = consumer->GetNextMessage();
        msg.Wait();
        Cerr << msg.GetValue().Response << "\n";
        UNIT_ASSERT(msg.GetValue().Response.HasLock());
    }

    Y_UNIT_TEST(TestPartitionStatus) {
        TTestServer server;
        server.AnnoyingClient->AlterUserAttributes("/", "Root", {{"__extra_path_symbols_allowed", "@"}});
        server.AnnoyingClient->CreateTopic(DEFAULT_TOPIC_NAME, 1);
        server.EnableLogs({ NKikimrServices::PQ_READ_PROXY });

        auto writer = MakeDataWriter(server);
        TTestPQLib PQLib(server);

        auto [consumer, res] = PQLib.CreateConsumer({SHORT_TOPIC_NAME}, "user", 1, true);
        UNIT_ASSERT_C(res.Response.HasInit(), res.Response);

        auto msg = consumer->GetNextMessage();
        auto value = msg.ExtractValueSync();
        Cerr << value.Response << "\n";
        UNIT_ASSERT(value.Response.HasLock());
        value.ReadyToRead.SetValue(TLockInfo{});
        auto lock = value.Response.GetLock();
        consumer->RequestPartitionStatus(lock.GetTopic(), lock.GetPartition(), lock.GetGeneration());
        msg = consumer->GetNextMessage();
        value = msg.ExtractValueSync();
        Cerr << value.Response << "\n";
        UNIT_ASSERT(value.Response.HasPartitionStatus());
        UNIT_ASSERT(value.Response.GetPartitionStatus().GetCommittedOffset() == 0);
        UNIT_ASSERT(value.Response.GetPartitionStatus().GetEndOffset() == 0);
        auto wt = value.Response.GetPartitionStatus().GetWriteWatermarkMs();
        Sleep(TDuration::Seconds(15));

        consumer->RequestPartitionStatus(lock.GetTopic(), lock.GetPartition(), lock.GetGeneration());
        msg = consumer->GetNextMessage();
        value = msg.ExtractValueSync();
        Cerr << value.Response << "\n";
        UNIT_ASSERT(value.Response.HasPartitionStatus());
        UNIT_ASSERT(value.Response.GetPartitionStatus().GetCommittedOffset() == 0);
        UNIT_ASSERT(value.Response.GetPartitionStatus().GetEndOffset() == 0);
        UNIT_ASSERT(wt < value.Response.GetPartitionStatus().GetWriteWatermarkMs());
        wt = value.Response.GetPartitionStatus().GetWriteWatermarkMs();

        {
            auto [producer, res] = PQLib.CreateProducer(SHORT_TOPIC_NAME, "123");
            UNIT_ASSERT_C(res.Response.HasInit(), res.Response);
            auto f = producer->Write(1,  TString(10, 'a'));
            UNIT_ASSERT_C(f.GetValueSync().Response.HasAck(), f.GetValueSync().Response);
        }
        msg = consumer->GetNextMessage();
        value = msg.ExtractValueSync();
        Cerr << value.Response << "\n";
        UNIT_ASSERT(value.Response.HasData());
        auto cookie = value.Response.GetData().GetCookie();

        consumer->RequestPartitionStatus(lock.GetTopic(), lock.GetPartition(), lock.GetGeneration());
        msg = consumer->GetNextMessage();
        value = msg.ExtractValueSync();
        Cerr << value.Response << "\n";
        UNIT_ASSERT(value.Response.HasPartitionStatus());
        UNIT_ASSERT(value.Response.GetPartitionStatus().GetCommittedOffset() == 0);
        UNIT_ASSERT(value.Response.GetPartitionStatus().GetEndOffset() == 1);
        UNIT_ASSERT(wt < value.Response.GetPartitionStatus().GetWriteWatermarkMs());
        wt = value.Response.GetPartitionStatus().GetWriteWatermarkMs();
        consumer->Commit({cookie});
        msg = consumer->GetNextMessage();
        Cerr << msg.GetValueSync().Response << "\n";
        UNIT_ASSERT(msg.GetValueSync().Response.HasCommit());

        consumer->RequestPartitionStatus(lock.GetTopic(), lock.GetPartition(), lock.GetGeneration());
        msg = consumer->GetNextMessage();
        value = msg.ExtractValueSync();
        Cerr << value.Response << "\n";
        UNIT_ASSERT(value.Response.HasPartitionStatus());
        UNIT_ASSERT(value.Response.GetPartitionStatus().GetCommittedOffset() == 1);
        UNIT_ASSERT(value.Response.GetPartitionStatus().GetEndOffset() == 1);
    }

    Y_UNIT_TEST(TestDeletionOfTopic) {
        TTestServer server(false);
        server.GrpcServerOptions.SetMaxMessageSize(130_MB);
        server.StartServer();
        server.AnnoyingClient->CreateTopic(DEFAULT_TOPIC_NAME, 1);
        server.EnableLogs({ NKikimrServices::PQ_READ_PROXY });

        auto writer = MakeDataWriter(server);
        TTestPQLib PQLib(server);

        server.AnnoyingClient->DescribeTopic({DEFAULT_TOPIC_NAME});
        server.AnnoyingClient->DeleteTopic2(DEFAULT_TOPIC_NAME, NPersQueue::NErrorCode::OK, false);
        Sleep(TDuration::Seconds(1));
        auto [consumer, res] = PQLib.CreateConsumer({SHORT_TOPIC_NAME}, "user", 1, {}, {}, false);
        Cerr << res.Response << "\n";

        UNIT_ASSERT_EQUAL_C(res.Response.GetError().GetCode(), NPersQueue::NErrorCode::UNKNOWN_TOPIC, res.Response);
    }

    Y_UNIT_TEST(SetupYqlTimeout) {
        TTestServer server(PQSettings(0, 1, true, "1"));
        server.EnableLogs({ NKikimrServices::PQ_WRITE_PROXY });
        server.AnnoyingClient->CreateTopic(DEFAULT_TOPIC_NAME, 10);

        auto writer = MakeDataWriter(server);

        server.AnnoyingClient->MarkNodeInHive(server.CleverServer->GetRuntime(), 0, false);
        server.AnnoyingClient->KickNodeInHive(server.CleverServer->GetRuntime(), 0);

        writer.InitSession("sid1", 2, false);
    }

    Y_UNIT_TEST(TestFirstClassWriteAndRead) {
        auto settings = PQSettings(0, 1, true, "1");
        settings.PQConfig.SetTopicsAreFirstClassCitizen(true);
        TTestServer server(settings, false);
        server.StartServer(false);
        UNIT_ASSERT_VALUES_EQUAL(NMsgBusProxy::MSTATUS_OK,
                                        server.AnnoyingClient->AlterUserAttributes("/", "Root", {{"folder_id", "somefolder"}, {"cloud_id", "somecloud"}, {"database_id", "root"}}));


        Cerr << "HERE\n";

        PrepareForFstClass(*server.AnnoyingClient);
        server.EnableLogs({ NKikimrServices::PQ_METACACHE, NKikimrServices::PERSQUEUE }, NActors::NLog::PRI_INFO);
        server.EnableLogs({ NKikimrServices::PERSQUEUE_CLUSTER_TRACKER }, NActors::NLog::PRI_EMERG);
        server.EnableLogs({ NKikimrServices::PERSQUEUE_READ_BALANCER }, NActors::NLog::PRI_DEBUG);

        TTestPQLib pqLib(server);

        NACLib::TDiffACL acl;
//        acl.AddAccess(NACLib::EAccessType::Allow, NACLib::UpdateRow, "topic@" BUILTIN_ACL_DOMAIN);
//        server.AnnoyingClient->ModifyACL("/Root/account1", "root-acc-topic", acl.SerializeAsString());
        Sleep(TDuration::Seconds(5));
        {
            auto [producer, res] = pqLib.CreateProducer(FC_TOPIC_PATH, "123", {}, {}, {}, true);
            UNIT_ASSERT_C(res.Response.HasInit(), res.Response);
            Cerr << "Producer 1 start response: " << res.Response << "\n";
            auto f = producer->Write(1,  TString(10, 'a'));
            UNIT_ASSERT_C(f.GetValueSync().Response.HasAck(), f.GetValueSync().Response);
        }
        {
            auto [producer, res] = pqLib.CreateProducer(FC_TOPIC_PATH, "123", {}, {}, {}, true);
            UNIT_ASSERT_C(res.Response.HasInit(), res.Response);
            Cerr << "Producer 2 start response: " << res.Response << "\n";
            auto f = producer->Write(2,  TString(10, 'b'));
            UNIT_ASSERT_C(f.GetValueSync().Response.HasAck(), f.GetValueSync().Response);
        }
        {
            auto [consumer, res] = pqLib.CreateConsumer({FC_TOPIC_PATH}, "user");
            Cerr << "Consumer start response: " << res.Response << "\n";
            auto msg = consumer->GetNextMessage();
            msg.Wait();
            Cerr << "Read response: " << msg.GetValue().Response << "\n";
            UNIT_ASSERT(msg.GetValue().Type == EMT_DATA);
        }
        {
            auto [consumer, res] = pqLib.CreateConsumer({FC_TOPIC_PATH}, "user", {}, true);
            Cerr << "Consumer start response: " << res.Response << "\n";
            ui64 commitsDone = 0;
            while (true) {
                auto msg = consumer->GetNextMessage();
                msg.Wait();
                auto& value = msg.GetValue();
                if (value.Type == EMT_LOCK) {
                    TStringBuilder message;
                    Cerr << "Consumer lock response: " << value.Response << "\n";
                    UNIT_ASSERT_VALUES_EQUAL(value.Response.GetLock().GetTopic(), "account1/root-acc-topic");
                    msg.GetValue().ReadyToRead.SetValue(TLockInfo());
                } else if (value.Type == EMT_DATA) {
                    auto cookie = msg.GetValue().Response.GetData().GetCookie();
                    consumer->Commit({cookie});
                } else {
                    UNIT_ASSERT(value.Type == EMT_COMMIT);
                    commitsDone++;
                    break;
                }
            }
            UNIT_ASSERT(commitsDone > 0);
        }
    }

    Y_UNIT_TEST(SrcIdCompatibility) {
        TString srcId1 = "test-src-id-compat", srcId2 = "test-src-id-compat2";
        TTestServer server{};
        TString topicName = "rt3.dc1--topic100";
        TString fullPath = "Root/PQ/rt3.dc1--topic100";
        TString shortTopicName = "topic100";
        server.AnnoyingClient->CreateTopic(topicName, 100);
        server.EnableLogs({ NKikimrServices::PERSQUEUE }, NActors::NLog::PRI_INFO);

        auto runTest = [&] (
                const TString& topicToAdd, const TString& topicForHash, const TString& topicName,
                const TString& srcId, ui32 partId, ui64 accessTime = 0
        ) {
            TStringBuilder query;
            auto encoded = NPQ::NSourceIdEncoding::EncodeSrcId(topicForHash, srcId);
            Cerr << "===save partition with time: " << accessTime << Endl;

            if (accessTime == 0) {
                accessTime = TInstant::Now().MilliSeconds();
            }
            if (!topicToAdd.empty()) { // Empty means don't add anything
                query <<
                      "--!syntax_v1\n"
                      "UPSERT INTO `/Root/PQ/SourceIdMeta2` (Hash, Topic, SourceId, CreateTime, AccessTime, Partition) VALUES ("
                      << encoded.Hash << ", \"" << topicToAdd << "\", \"" << encoded.EscapedSourceId << "\", "
                      << TInstant::Now().MilliSeconds() << ", " << accessTime << ", " << partId << "); ";
                Cerr << "Run query:\n" << query << Endl;
                auto scResult = server.AnnoyingClient->RunYqlDataQuery(query);
                //UNIT_ASSERT(scResult.Defined());
            }
            TTestPQLib pqLib(server);
            auto[producer, response] = pqLib.CreateProducer(topicName, srcId, {}, {}, {}, true);
            UNIT_ASSERT_C(response.Response.HasInit(), response.Response);
            UNIT_ASSERT_VALUES_EQUAL(response.Response.GetInit().GetPartition(), partId);
        };

        runTest(fullPath, shortTopicName, shortTopicName, srcId1, 5, 100);
        runTest(topicName, shortTopicName, shortTopicName, srcId2, 6);
        runTest("", "", shortTopicName, srcId1, 5, 100);
        // Add another partition to the src mapping with different topic in key.
        // Expect newer partition to be discovered.
        ui64 time = (TInstant::Now() + TDuration::Hours(4)).MilliSeconds();
        runTest(topicName, shortTopicName, shortTopicName, srcId1, 7, time);

    }

} // Y_UNIT_TEST_SUITE(TPersQueueTest)

} // namespace NPersQueueTests
} // namespace NKikimr
