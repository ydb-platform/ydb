#include <library/cpp/testing/unittest/registar.h>
#include <util/string/builder.h>
#include <ydb/core/ymq/actor/cloud_events/cloud_events.h>
#include <ydb/core/ymq/actor/queue_schema.h>


#include <ydb/core/testlib/test_client.h>
#include <ydb/core/ymq/actor/index_events_processor.h>
#include <library/cpp/testing/unittest/registar.h>
#include <util/stream/file.h>

namespace NKikimr::NSQS {

using namespace Tests;
using namespace NYdb;

class TCloudEventsProcessorTests : public TTestBase {

public:
    TCloudEventsProcessorTests() {
        TPortManager portManager;
        auto mbusPort = portManager.GetPort(2134);
        auto grpcPort = portManager.GetPort(2135);
        auto settings = TServerSettings(mbusPort);
        settings.SetDomainName("Root");
        Server = MakeHolder<TServer>(settings);
        Server->EnableGRpc(NYdbGrpc::TServerOptions().SetHost("localhost").SetPort(grpcPort));
        auto driverConfig = TDriverConfig().SetEndpoint(TStringBuilder() << "localhost:" << grpcPort);

        Driver = MakeHolder<TDriver>(driverConfig);
        TableClient = MakeSimpleShared<NYdb::NTable::TTableClient>(*Driver);
    }

private:
    struct TTestRunner {
        TString TestName;
        TString SchemePath;
        NCloudEvents::TProcessor* Processor;
        TCloudEventsProcessorTests* Parent;
        TActorId ProcessorId;
        TString FullTablePath;

        TTestRunner(
            const TString& name,
            TCloudEventsProcessorTests* parent,
            const TDuration& retryTimeout
        )
            : TestName(name)
            , SchemePath(parent->Root + "/" + name)
            , Processor()
            , Parent(parent)
        {
            auto* runtime = parent->Server->GetRuntime();
            Processor = new NCloudEvents::TProcessor(
                SchemePath,
                TString(),
                retryTimeout
            );
            ProcessorId = runtime->Register(Processor);
            runtime->EnableScheduleForActor(ProcessorId, true);
            FullTablePath = SchemePath + "/" + TString(NCloudEvents::TProcessor::EventTableName);
            InitTable();
        }

        ~TTestRunner() {
            if (Processor != nullptr) {
                auto handle = new IEventHandle(ProcessorId, TActorId(), new TEvents::TEvPoisonPill());
                Parent->Server->GetRuntime()->Send(handle);
            }
        }

        void InitTable() {
            TClient client(Parent->Server->GetSettings());
            client.MkDir(Parent->Root, SchemePath);
            auto session = Parent->TableClient->CreateSession().GetValueSync().GetSession();

            auto desc = NYdb::NTable::TTableBuilder()
                .AddNullableColumn("Id", EPrimitiveType::Uint64)
                .AddNullableColumn("QueueName", EPrimitiveType::Utf8)
                .AddNullableColumn("CreatedAt", EPrimitiveType::Uint64)
                .AddNullableColumn("Type", EPrimitiveType::Utf8)
                .AddNullableColumn("CloudId", EPrimitiveType::Utf8)
                .AddNullableColumn("FolderId", EPrimitiveType::Utf8)
                .AddNullableColumn("UserSID", EPrimitiveType::Utf8)
                .AddNullableColumn("UserSanitizedToken", EPrimitiveType::Utf8)
                .AddNullableColumn("AuthType", EPrimitiveType::Utf8)
                .AddNullableColumn("PeerName", EPrimitiveType::Utf8)
                .AddNullableColumn("RequestId", EPrimitiveType::Utf8)
                .AddNullableColumn("IdempotencyId", EPrimitiveType::Utf8)
                .AddNullableColumn("Labels", EPrimitiveType::Utf8)
                .SetPrimaryKeyColumns({"Id", "QueueName"})
            .Build();

            auto status = session.CreateTable(FullTablePath, std::move(desc)).GetValueSync();
            UNIT_ASSERT(status.IsSuccess());
            session.Close();
        }

        TAsyncStatus RunDataQuery(const TString& query) {
            auto status = Parent->TableClient->RetryOperation<NYdb::NTable::TDataQueryResult>(
                    [query](NYdb::NTable::TSession session) {
                        return session.ExecuteDataQuery(
                                query, NYdb::NTable::TTxControl::BeginTx().CommitTx(),
                                NYdb::NTable::TExecDataQuerySettings().ClientTimeout(TDuration::Seconds(10))
                        ).Apply([](const auto &future) {
                            return future;
                        });
            });
            return status;
        }

        void ExecDataQuery(const TString& query) {
            std::cerr << "===Execute query: " << query << std::endl;
            auto statusVal = RunDataQuery(query).GetValueSync();
            if (!statusVal.IsSuccess()) {
                std::cerr << "Query execution failed with error: " << statusVal.GetIssues().ToString() << std::endl;
            }
            UNIT_ASSERT(statusVal.IsSuccess());
            std::cerr << "End execute query===" << std::endl;
        }

        void AddEvent(
            const TString& queueName,
            const TString& type,
            const TString& cloudId,
            const TString& folderId,
            const TString& userSID,
            const TString& token,
            const TString& authType,
            const TString& peerName,
            const TString& requestId,
            const TString& idempotencyId,
            const TString& labels
        )
        {
            TStringBuilder queryBuilder;
            uint_fast64_t createdAt = std::chrono::time_point_cast<std::chrono::microseconds>
                                      (std::chrono::high_resolution_clock::now())
                                      .time_since_epoch().count();

            queryBuilder
                << "UPSERT INTO" << "`" << FullTablePath
                << "` ("
                    << "Id,"
                    << "QueueName,"
                    << "CreatedAt,"
                    << "Type,"
                    << "CloudId,"
                    << "FolderId,"
                    << "UserSID,"
                    << "UserSanitizedToken,"
                    << "AuthType,"
                    << "PeerName,"
                    << "RequestId,"
                    << "IdempotencyId,"
                    << "Labels"
                << ")"
                << "VALUES"
                << "("
                    << NCloudEvents::TEventIdGenerator::Generate() << ","
                    << "'" << queueName << "'" << ","
                    << createdAt << ","
                    << "'" << type << "'" << ","                                                  // DeleteMessageQueue or CreateMessageQueue or UpdateMessageQueue
                    << "'" << cloudId << "'" << ","
                    << "'" << folderId << "'" << ","
                    << "'" << userSID << "'" << ","
                    << "'" << token << "'" << ","
                    << "'" << authType << "'" << ","
                    << "'" << peerName << "'" << ","
                    << "'" << requestId << "'" << ","
                    << "'" << idempotencyId << "'" << ","
                    << "'" << labels << "'" 
                << ");";

            ExecDataQuery(TString(queryBuilder));
        }

    };

    void SetUp() override {
        TClient client(Server->GetSettings());
        client.InitRootScheme();
        client.MkDir("/Root", "SQS");
    }

private:
    THolder<TDriver> Driver;
    TSimpleSharedPtr<NYdb::NTable::TTableClient> TableClient;
    THolder<TServer> Server;
    TString Root = "/Root/SQS";

    UNIT_TEST_SUITE(TCloudEventsProcessorTests)
    UNIT_TEST(TestCreateCloudEventProcessor)
    UNIT_TEST_SUITE_END();

    void TestCreateCloudEventProcessor() {
        TDuration retryTimeout = TDuration::Seconds(2);
        TTestRunner runner("CreateCloudEventProcessor", this, retryTimeout);
        TString queueName = "queue1";
        TString cloudId = "cloud1";
        TString folderId = "folder1";
        TString sid = "username";
        TString token = "sanitizedToken123";
        TString authType = "authtype";
        TString peerName = "localhost:8000";
        TString requestId = "req1";
        TString idempotencyKey = "idemp1";
        TString labels = "{\"k1\" : \"v1\"}";

        Sleep(TDuration::Seconds(1));

        runner.AddEvent(
            queueName,
            "CreateMessageQueue",
            cloudId,
            folderId,
            sid,
            token,
            authType,
            peerName,
            requestId,
            idempotencyKey,
            labels
        );

        runner.AddEvent(
            queueName,
            "UpdateMessageQueue",
            cloudId,
            folderId,
            sid,
            token,
            authType,
            peerName,
            requestId,
            idempotencyKey,
            labels
        );

        runner.AddEvent(
            queueName,
            "DeleteMessageQueue",
            cloudId,
            folderId,
            sid,
            token,
            authType,
            peerName,
            requestId,
            idempotencyKey,
            labels
        );

        Sleep(retryTimeout * 3);
    }
};
UNIT_TEST_SUITE_REGISTRATION(TCloudEventsProcessorTests);
} // NKikimr::NSQS
