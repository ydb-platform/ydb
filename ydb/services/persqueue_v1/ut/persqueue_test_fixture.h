#pragma once

#include "test_utils.h"

#include <ydb/core/testlib/test_pq_client.h>

#include <ydb/library/aclib/aclib.h>


#include <ydb/public/sdk/cpp/client/ydb_driver/driver.h>
#include <ydb/public/sdk/cpp/client/ydb_table/table.h>
#include <ydb/public/sdk/cpp/client/ydb_persqueue_core/persqueue.h>
#include <ydb/public/sdk/cpp/client/ydb_persqueue_core/ut/ut_utils/test_server.h>

#include <util/system/env.h>

namespace NKikimr::NPersQueueTests {

static void ModifyTopicACL(NYdb::TDriver* driver, const TString& topic, const TVector<std::pair<TString, TVector<TString>>>& acl) {

    NYdb::NScheme::TSchemeClient schemeClient(*driver);
    auto modifyPermissionsSettings = NYdb::NScheme::TModifyPermissionsSettings();

    for (const auto& [token, permissions]: acl) {
        auto p = NYdb::NScheme::TPermissions(token, permissions);
        modifyPermissionsSettings.AddSetPermissions(p);
    }

    Cerr << "BEFORE MODIFY PERMISSIONS\n";

    auto result = schemeClient.ModifyPermissions(topic, modifyPermissionsSettings).ExtractValueSync();
    UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
}

#define SET_LOCALS                                              \
    auto& pqClient = server.Server->AnnoyingClient;             \
    Y_UNUSED(pqClient);                                         \
    auto* runtime = server.Server->CleverServer->GetRuntime();  \
    Y_UNUSED(runtime);                                          \


    using namespace Tests;
    using namespace NKikimrClient;
    using namespace Ydb::PersQueue;
    using namespace Ydb::PersQueue::V1;
    using namespace NThreading;
    using namespace NNetClassifier;

    class TPersQueueV1TestServerBase {
    public:
        TPersQueueV1TestServerBase(bool tenantModeEnabled = false)
            : TenantMode(tenantModeEnabled)
        {}

        virtual void AlterSettings(NKikimr::Tests::TServerSettings& settings) {
            Y_UNUSED(settings);
        }

        virtual NKikimr::Tests::TServerSettings GetServerSettings() {
            return NKikimr::NPersQueueTests::PQSettings();
        }

        void InitializePQ() {
            Y_ABORT_UNLESS(Server == nullptr);
            Server = MakeHolder<NPersQueue::TTestServer>(GetServerSettings(), false);
            Server->ServerSettings.PQConfig.SetTopicsAreFirstClassCitizen(TenantModeEnabled());
            Server->ServerSettings.PQConfig.MutablePQDiscoveryConfig()->SetLBFrontEnabled(true);
            Server->ServerSettings.PQConfig.SetACLRetryTimeoutSec(1);

            AlterSettings(Server->ServerSettings);

            Cerr << "=== Server->StartServer(false);" << Endl;
            Server->StartServer(false);

            Cerr << "=== TenantModeEnabled() = " << TenantModeEnabled() << Endl;
            if (TenantModeEnabled()) {
                Server->AnnoyingClient->SetNoConfigMode();
                Server->ServerSettings.PQConfig.SetSourceIdTablePath("some unused path");
            }
            Cerr << "=== Init PQ - start server on port " << Server->GrpcPort << Endl;
            Server->GrpcServerOptions.SetMaxMessageSize(130_MB);
            EnablePQLogs({NKikimrServices::PQ_READ_PROXY, NKikimrServices::PQ_WRITE_PROXY, NKikimrServices::FLAT_TX_SCHEMESHARD});
            EnablePQLogs({NKikimrServices::PERSQUEUE}, NLog::EPriority::PRI_INFO);
            EnablePQLogs({NKikimrServices::KQP_PROXY}, NLog::EPriority::PRI_EMERG);


            Server->AnnoyingClient->FullInit();
            if (!TenantModeEnabled())
                Server->AnnoyingClient->CheckClustersList(Server->CleverServer->GetRuntime());

            Server->AnnoyingClient->CreateConsumer("user");
            if (TenantModeEnabled()) {
                Cerr << "=== Will create fst-class topics\n";
                Server->AnnoyingClient->CreateTopicNoLegacy("/Root/acc/topic1", 1);
                Server->AnnoyingClient->CreateTopicNoLegacy("/Root/PQ/acc/topic1", 1);
            } else {
                Cerr << "=== Will create legacy-style topics\n";
                Server->AnnoyingClient->CreateTopicNoLegacy("rt3.dc1--acc--topic2dc", 1);
                Server->AnnoyingClient->CreateTopicNoLegacy("rt3.dc2--acc--topic2dc", 1, true, false);
                Server->AnnoyingClient->CreateTopicNoLegacy("rt3.dc1--topic1", 1);
                Server->AnnoyingClient->CreateTopicNoLegacy("rt3.dc1--acc--topic1", 1);
                Server->WaitInit("topic1");
            }

            Cerr << "=== EnablePQLogs" << Endl;
            EnablePQLogs({ NKikimrServices::KQP_PROXY }, NLog::EPriority::PRI_EMERG);

            Cerr << "=== CreateChannel" << Endl;
            InsecureChannel = grpc::CreateChannel("localhost:" + ToString(Server->GrpcPort), grpc::InsecureChannelCredentials());
            Cerr << "=== NewStub" << Endl;
            ServiceStub = Ydb::PersQueue::V1::PersQueueService::NewStub(InsecureChannel);
            Cerr << "=== InitializeWritePQService" << Endl;
            InitializeWritePQService(TenantModeEnabled() ? "Root/acc/topic1" : "topic1");

            Cerr << "=== PersQueueClient" << Endl;
            NYdb::TDriverConfig driverCfg;
            driverCfg.SetEndpoint(TStringBuilder() << "localhost:" << Server->GrpcPort).SetLog(CreateLogBackend("cerr", ELogPriority::TLOG_DEBUG)).SetDatabase("/Root");
            YdbDriver.reset(new NYdb::TDriver(driverCfg));
            PersQueueClient = MakeHolder<NYdb::NPersQueue::TPersQueueClient>(*YdbDriver);

            Cerr << "=== InitializePQ completed" << Endl;
        }

        void EnablePQLogs(const TVector<NKikimrServices::EServiceKikimr> services,
                        NActors::NLog::EPriority prio = NActors::NLog::PRI_DEBUG)
        {
            for (auto s : services) {
                Server->CleverServer->GetRuntime()->SetLogPriority(s, prio);
            }
        }

        void InitializeWritePQService(const TString &topicToWrite) {
            while (true) {
                Cerr << "=== InitializeWritePQService start iteration" << Endl;
                Sleep(TDuration::MilliSeconds(100));

                Ydb::PersQueue::V1::StreamingWriteClientMessage req;
                Ydb::PersQueue::V1::StreamingWriteServerMessage resp;
                grpc::ClientContext context;

                Cerr << "=== InitializeWritePQService create streamingWriter" << Endl;
                auto stream = ServiceStub->StreamingWrite(&context);
                UNIT_ASSERT(stream);

                req.mutable_init_request()->set_topic(topicToWrite);
                req.mutable_init_request()->set_message_group_id("12345678");

                Cerr << "=== InitializeWritePQService Write" << Endl;
                if (!stream->Write(req)) {
                    Cerr << "=== InitializeWritePQService Write fail" << Endl;
                    UNIT_ASSERT_C(stream->Read(&resp), "Context error: " << context.debug_error_string());
                    UNIT_ASSERT_C(resp.status() == Ydb::StatusIds::UNAVAILABLE,
                                  "Response: " << resp << ", Context error: " << context.debug_error_string());
                    continue;
                }

                AssertSuccessfullStreamingOperation(stream->Read(&resp), stream);
                if (resp.status() == Ydb::StatusIds::UNAVAILABLE) {
                    Cerr << "=== InitializeWritePQService Status = UNAVAILABLE" << Endl;
                    continue;
                }

                if (stream->WritesDone()) {
                    auto status = stream->Finish();
                    Cerr << "Finish: " << (int) status.error_code() << " " << status.error_message() << "\n";
                }

                break;
            }

            Cerr << "=== InitializeWritePQService done" << Endl;
        }

    public:
        bool TenantModeEnabled() const {
            return TenantMode;
        }

        void ModifyTopicACL(const TString& topic, const TVector<std::pair<TString, TVector<TString>>>& acl) {
            ::ModifyTopicACL(YdbDriver.get(), topic, acl);
        }

        void ModifyTopicACLAndWait(const TString& topic, const TVector<std::pair<TString, TVector<TString>>>& acl) {
            auto driver = YdbDriver.get();
            auto schemeClient = NYdb::NScheme::TSchemeClient(*driver);
            auto desc = schemeClient.DescribePath(topic).ExtractValueSync();
            auto size = desc.GetEntry().EffectivePermissions.size();

            ::ModifyTopicACL(driver, topic, acl);

            do {
                desc = schemeClient.DescribePath(topic).ExtractValueSync();
            } while (size == desc.GetEntry().EffectivePermissions.size());
        }


        TString GetRoot() const {
            return !TenantModeEnabled() ? "/Root/PQ" : "";
        }

        TString GetTopic() {
            return TenantModeEnabled() ? "/Root/acc/topic1" : "rt3.dc1--topic1";
        }

        TString GetTopicPath() {
            return TenantModeEnabled() ? "/Root/acc/topic1" : "topic1";
        }


        TString GetFullTopicPath() {
            return TenantModeEnabled() ? "/Root/acc/topic1" : "/Root/PQ/rt3.dc1--topic1";
        }
        TString GetTopicPathMultipleDC() const {
            return "acc/topic2dc";
        }

    public:
        const bool TenantMode;
        THolder<NPersQueue::TTestServer> Server;
        TSimpleSharedPtr<TPortManager> PortManager;
        std::shared_ptr<grpc::Channel> InsecureChannel;
        std::unique_ptr<Ydb::PersQueue::V1::PersQueueService::Stub> ServiceStub;

        std::shared_ptr<NYdb::TDriver> YdbDriver;
        THolder<NYdb::NPersQueue::TPersQueueClient> PersQueueClient;
    };

    struct TPersQueueV1TestServerSettings {
        bool CheckACL = false;
        bool TenantModeEnabled = false;
        ui32 NodeCount = PQ_DEFAULT_NODE_COUNT;
    };

    class TPersQueueV1TestServer : public TPersQueueV1TestServerBase {
    public:
        explicit TPersQueueV1TestServer(const TPersQueueV1TestServerSettings& settings)
            : TPersQueueV1TestServerBase(settings.TenantModeEnabled)
            , Settings(settings)
        {
            InitAll();
        }

        TPersQueueV1TestServer(bool checkAcl = false, bool tenantModeEnabled = false)
            : TPersQueueV1TestServer({ .CheckACL = checkAcl, .TenantModeEnabled = tenantModeEnabled })
        {}

        void InitAll() {
            InitializePQ();
        }

        void AlterSettings(NKikimr::Tests::TServerSettings& settings) override {
            if (Settings.CheckACL) {
                settings.PQConfig.SetCheckACL(true);
            }
        }

        NKikimr::Tests::TServerSettings GetServerSettings() override {
            return NKikimr::NPersQueueTests::PQSettings()
                .SetNodeCount(Settings.NodeCount);
        }

    private:
        TPersQueueV1TestServerSettings Settings;
    };

    class TPersQueueV1TestServerWithRateLimiter : public TPersQueueV1TestServerBase {
    private:
        NKikimrPQ::TPQConfig::TQuotingConfig::ELimitedEntity LimitedEntity;
    public:
        TPersQueueV1TestServerWithRateLimiter(bool tenantModeEnabled = false)
            : TPersQueueV1TestServerBase(tenantModeEnabled)
        {}

        void AlterSettings(NKikimr::Tests::TServerSettings& settings) override {
            settings.PQConfig.MutableQuotingConfig()->SetEnableQuoting(true);
            settings.PQConfig.MutableQuotingConfig()->SetTopicWriteQuotaEntityToLimit(LimitedEntity);
        }

        void InitAll(NKikimrPQ::TPQConfig::TQuotingConfig::ELimitedEntity limitedEntity) {
            LimitedEntity = limitedEntity;
            InitializePQ();
            InitQuotingPaths();
        }

        void InitQuotingPaths() {
            Server->AnnoyingClient->MkDir("/Root", "PersQueue");
            Server->AnnoyingClient->MkDir("/Root/PersQueue", "System");
            Server->AnnoyingClient->MkDir("/Root/PersQueue/System", "Quoters");
        }

        void CreateTopicWithQuota(const TString& path, bool createKesus = true, double writeQuota = 1000.0) {
            TVector<TString> pathComponents = SplitPath(path);
            const TString account = pathComponents[0];
            const TString name = NPersQueue::BuildFullTopicName(path, "dc1");

            if (TenantModeEnabled()) {
                Server->AnnoyingClient->CreateTopicNoLegacy("/Root/PQ/" + path, 1);
            } else {
                Cerr << "Creating topic \"" << name << "\"" << Endl;
                Server->AnnoyingClient->CreateTopicNoLegacy(name, 1);
            }

            const TString rootPath = "/Root/PersQueue/System/Quoters";
            const TString kesusPath = TStringBuilder() << rootPath << "/" << account;

            if (createKesus) {
                Cerr << "Creating kesus \"" << account << "\"" << Endl;
                const NMsgBusProxy::EResponseStatus createKesusResult = Server->AnnoyingClient->CreateKesus(rootPath, account);
                UNIT_ASSERT_C(createKesusResult == NMsgBusProxy::MSTATUS_OK, createKesusResult);

                const auto statusCode = Server->AnnoyingClient->AddQuoterResource(
                        Server->CleverServer->GetRuntime(), kesusPath, "write-quota", writeQuota
                );
                UNIT_ASSERT_EQUAL_C(statusCode, Ydb::StatusIds::SUCCESS, "Status: " << Ydb::StatusIds::StatusCode_Name(statusCode));
            }

            if (pathComponents.size() > 1) { // account (first component) + path of topic
                TStringBuilder prefixPath; // path without account
                prefixPath << "write-quota";
                for (auto currentComponent = pathComponents.begin() + 1; currentComponent != pathComponents.end(); ++currentComponent) {
                    prefixPath << "/" << *currentComponent;
                    Cerr << "Adding quoter resource: \"" << prefixPath << "\"" << Endl;
                    const auto statusCode = Server->AnnoyingClient->AddQuoterResource(
                            Server->CleverServer->GetRuntime(), kesusPath, prefixPath
                    );
                    UNIT_ASSERT_C(statusCode == Ydb::StatusIds::SUCCESS || statusCode == Ydb::StatusIds::ALREADY_EXISTS, "Status: " << Ydb::StatusIds::StatusCode_Name(statusCode));
                }
            }
        }
/*
        THolder<IProducer> StartProducer(const TString& topicPath, bool compress = false) {
            TString fullPath = TenantModeEnabled() ? "/Root/PQ/" + topicPath : topicPath;
            TProducerSettings producerSettings;
            producerSettings.Server = TServerSetting("localhost", Server->GrpcPort);
            producerSettings.Topic = fullPath;
            producerSettings.SourceId = "TRateLimiterTestSetupSourceId";
            producerSettings.Codec = compress ? "gzip" : "raw";
            THolder<IProducer> producer = PQLib->CreateProducer(producerSettings);
            auto startResult = producer->Start();
            UNIT_ASSERT_EQUAL_C(Ydb::StatusIds::SUCCESS, startResult.GetValueSync().Response.status(), "Response: " << startResult.GetValueSync().Response);
            return producer;
        }
*/
    };
}
