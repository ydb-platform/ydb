#include <ydb/core/testlib/test_client.h>
#include <ydb/core/ymq/actor/index_events_processor.h>
#include <library/cpp/testing/unittest/registar.h>
#include <util/stream/file.h>
#include <library/cpp/json/json_reader.h>
#include "test_events_writer.h"

namespace NKikimr::NSQS {

using namespace Tests;
using namespace NYdb;


class TIndexProcesorTests : public TTestBase {
    using EEvType = TSearchEventsProcessor::EQueueEventType;

public:
    TIndexProcesorTests() {
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
        TSearchEventsProcessor* Processor;
        TDispatchOptions DispatchOpts;
        TInstant CurrTs;
        TInstant PrevTs;
        TMockSessionPtr EventsWriter;
        TIndexProcesorTests* Parent;
        TActorId ProcessorId;
        TTestRunner(const TString& name, TIndexProcesorTests* parent)
            : TestName(name)
            , SchemePath(parent->Root + "/" + name)
            , Processor()
            , CurrTs(TInstant::Now())
            , PrevTs(TInstant::Seconds(CurrTs.Seconds() - 1))
            , Parent(parent)
        {
            EventsWriter = MakeIntrusive<TTestEventsWriter>();

            auto* runtime = parent->Server->GetRuntime();
            Processor = new TSearchEventsProcessor(
                    SchemePath,
                    TDuration::Minutes(10), TDuration::MilliSeconds(100),
                    TString(),
                    EventsWriter,
                    true
            );
            ProcessorId = runtime->Register(Processor);
            runtime->EnableScheduleForActor(ProcessorId, true);
            parent->Server->GetRuntime()->SetObserverFunc(
                    [&](TAutoPtr<IEventHandle>&)
                    {
                        if (Processor->GetReindexCount() >= 1) return TTestActorRuntimeBase::EEventAction::DROP;
                        else return TTestActorRuntimeBase::EEventAction::PROCESS;
                    }
            );
            InitTables();
        }
        ~TTestRunner() {
            if (Processor != nullptr) {
                auto handle = new IEventHandle(ProcessorId, TActorId(), new TEvents::TEvPoisonPill());
                Parent->Server->GetRuntime()->Send(handle);
            }
        }
        void InitTables() {
            TClient client(Parent->Server->GetSettings());
            client.MkDir(Parent->Root, SchemePath);
            auto session = Parent->TableClient->CreateSession().GetValueSync().GetSession();
            auto desc = NYdb::NTable::TTableBuilder()
                    .AddNullableColumn("Account", EPrimitiveType::Utf8)
                    .AddNullableColumn("QueueName", EPrimitiveType::Utf8)
                    .AddNullableColumn("CreatedTimestamp", EPrimitiveType::Uint64)
                    .AddNullableColumn("FolderId", EPrimitiveType::Utf8)
                    .AddNullableColumn("CustomQueueName", EPrimitiveType::Utf8)
                    .AddNullableColumn("Tags", EPrimitiveType::Utf8)
                    .SetPrimaryKeyColumns({"Account", "QueueName"})
                    .Build();
            auto eventsDesc = NYdb::NTable::TTableBuilder()
                    .AddNullableColumn("Account", EPrimitiveType::Utf8)
                    .AddNullableColumn("QueueName", EPrimitiveType::Utf8)
                    .AddNullableColumn("EventType", EPrimitiveType::Uint64)
                    .AddNullableColumn("EventTimestamp", EPrimitiveType::Uint64)
                    .AddNullableColumn("FolderId", EPrimitiveType::Utf8)
                    .AddNullableColumn("CustomQueueName", EPrimitiveType::Utf8)
                    .AddNullableColumn("Labels", EPrimitiveType::Utf8)
                    .SetPrimaryKeyColumns({"Account", "QueueName", "EventType"})
                    .Build();
            auto f1 = session.CreateTable(SchemePath + "/.Queues", std::move(desc));
            auto f2 = session.CreateTable(SchemePath + "/.Events", std::move(eventsDesc));

            auto status = f1.GetValueSync();
            UNIT_ASSERT(status.IsSuccess());
            status = f2.GetValueSync();
            UNIT_ASSERT(status.IsSuccess());
            session.Close();
        }

        TAsyncStatus RunDataQuery(const TString& query) {
            auto status = Parent->TableClient->RetryOperation<NYdb::NTable::TDataQueryResult>(
                    [query](NYdb::NTable::TSession session) {
                        return session.ExecuteDataQuery(
                                query, NYdb::NTable::TTxControl::BeginTx().CommitTx(),
                                NYdb::NTable::TExecDataQuerySettings().ClientTimeout(TDuration::Seconds(10))
                        ).Apply([](const auto &future) mutable {
                            return future;
                        });
            });
            return status;
        }

        void ExecDataQuery(const TString& query) {
            Cerr << "===Execute query: " << query << Endl;
            auto statusVal = RunDataQuery(query).GetValueSync();
            if (!statusVal.IsSuccess()) {
                Cerr << "Query execution failed with error: " << statusVal.GetIssues().ToString() << Endl;
            }
            UNIT_ASSERT(statusVal.IsSuccess());
        }
        void AddEvent(
                const TString& account, const TString& queueName, const EEvType& type, TInstant ts = TInstant::Zero(), TMaybe<TString> labels = "{}")
        {
            if (ts == TInstant::Zero())
                ts = CurrTs;
            TStringBuilder queryBuilder;
            queryBuilder << "UPSERT INTO `" << SchemePath << "/.Events` (Account, QueueName, EventType, CustomQueueName, EventTimestamp, FolderId, Labels) "
                         << "VALUES ("
                            << "\"" << account << "\", "
                            << "\"" << queueName << "\", "
                            << static_cast<ui64>(type) << ", "
                            << "\"myQueueCustomName\", "
                            << ts.MilliSeconds() << ", "
                            << "\"myFolder\", "
                            << (labels.Defined() ? "\"" + labels.GetRef() + "\"" : "NULL")
                         << ");";
            ExecDataQuery(queryBuilder.c_str());
        }

        void AddQueue(const TString& account, const TString& queueName, TInstant ts = TInstant::Zero(), TMaybe<TString> tags = "{}") {
            if (ts == TInstant::Zero())
                ts = CurrTs;
            TStringBuilder queryBuilder;
            queryBuilder << "UPSERT INTO `" << SchemePath << "/.Queues` (Account, QueueName, CustomQueueName, CreatedTimestamp, FolderId, Tags) "
                         << "VALUES ("
                             << "\"" << account << "\", "
                             << "\"" << queueName << "\", "
                             << "\"myQueueCustomName\", "
                             << ts.MilliSeconds() << ", "
                             << "\"myFolder\", "
                             << (tags.Defined() ? "\"" + tags.GetRef() + "\"" : "NULL")
                         << ");";
            ExecDataQuery(queryBuilder.c_str());
        }

        void AddQueuesBatch(const TString& account, const TString& queueNameBase, ui64 count, ui64 startIndex = 0, TMaybe<TString> tags = "{}") {
            Cerr << "===Started add queue batch\n";
            TDeque<NYdb::NTable::TAsyncDataQueryResult> results;
            ui64 maxInflight = 1;
            auto getRessionResult = Parent->TableClient->GetSession().GetValueSync();
            UNIT_ASSERT(getRessionResult.IsSuccess());
            auto session = getRessionResult.GetSession();
            TStringBuilder queryBuilder;
            queryBuilder << "DECLARE $QueueName as Utf8; "
                         << "UPSERT INTO `" << SchemePath << "/.Queues` "
                         << "(Account, QueueName, CustomQueueName, CreatedTimestamp, Tags, FolderId) "
                         << "VALUES ("
                             << "\"" << account << "\", "
                             << "$QueueName, "
                             << "\"myQueueCustomName\", "
                             << CurrTs.MilliSeconds() << ", "
                             << "\"myFolder\", "
                             << (tags.Defined() ? "\"" + tags.GetRef() + "\"" : "NULL")
                         << ");";

            auto preparedResult = session.PrepareDataQuery(queryBuilder.c_str()).GetValueSync();
            UNIT_ASSERT(preparedResult.IsSuccess());
            auto prepQuery = preparedResult.GetQuery();

            auto txResult = session.BeginTransaction().GetValueSync();
            UNIT_ASSERT(txResult.IsSuccess());
            auto tx = txResult.GetTransaction();
            auto txControl = NYdb::NTable::TTxControl::Tx(tx);

            for (auto i = startIndex; i < startIndex + count; i++) {
                while (results.size() < maxInflight) {
                    auto builder = prepQuery.GetParamsBuilder();
                    builder.AddParam("$QueueName").Utf8(queueNameBase + ToString(i)).Build();

                    results.push_back(
                            prepQuery.Execute(txControl, builder.Build())
                    );
                }
                results.front().Wait();
                while(!results.empty() && results.front().HasValue()) {
                   UNIT_ASSERT_C(results.front().GetValueSync().IsSuccess(),results.front().GetValueSync().GetIssues().ToString().c_str());
                   results.pop_front();
                }

            }
            while (!results.empty()) {
                auto statusVal = results.front().GetValueSync();
                UNIT_ASSERT(statusVal.IsSuccess());
                results.pop_front();
            }
            auto commitStatus = tx.Commit().GetValueSync();
            UNIT_ASSERT(commitStatus.IsSuccess());
        }

        ui64 CountRecords(const TString& table) const {
            TStringBuilder queryBuilder;
            TMaybe<NYdb::NTable::TDataQueryResult> countEventsResult;

            queryBuilder << "select count(*) as Count from `" << SchemePath << "/." << table << "`;";
            auto status = Parent->TableClient->RetryOperation<NYdb::NTable::TDataQueryResult>([query = TString(queryBuilder), &countEventsResult](NYdb::NTable::TSession session) {
                return session.ExecuteDataQuery(
                        query, NYdb::NTable::TTxControl::BeginTx().CommitTx(),
                        NYdb::NTable::TExecDataQuerySettings().ClientTimeout(TDuration::Seconds(5))
                ).Apply([&](const auto& future) mutable {
                    countEventsResult = future.GetValue();
                    return future;
                });
            }).GetValueSync();
            UNIT_ASSERT(status.IsSuccess());
            auto result = countEventsResult.Get()->GetResultSet(0);
            TResultSetParser parser(result);
            auto haveNext = parser.TryNextRow();
            UNIT_ASSERT(haveNext);
            return parser.ColumnParser("Count").GetUint64();
        }
        ui64 CountEvents() const {
            return CountRecords("Events");
        }
        ui64 CountQueues() const {
            return CountRecords("Queues");
        }
        void DispatchEvents() {
            auto initialCount = Processor->GetReindexCount();
            DispatchOpts.CustomFinalCondition = [initialCount, processor = Processor]() { return processor->GetReindexCount() > initialCount; };
            if (!initialCount) {
                auto handle = new IEventHandle(ProcessorId, TActorId(), new TEvWakeup());
                Parent->Server->GetRuntime()->Send(handle);
            }
            Parent->Server->GetRuntime()->DispatchEvents(DispatchOpts, TDuration::Minutes(1));
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

    UNIT_TEST_SUITE(TIndexProcesorTests)
    UNIT_TEST(TestCreateIndexProcessor)
    UNIT_TEST(TestSingleCreateQueueEvent)
    UNIT_TEST(TestReindexSingleQueue)
    UNIT_TEST(TestDeletedQueueNotReindexed)
    UNIT_TEST(TestManyMessages)
    UNIT_TEST(TestOver1000Queues)
    UNIT_TEST_SUITE_END();

    void CheckEventsLine(
            const TString& line, EEvType type, const TString& queueName = TString(), const TString labels = "")
    {
        Cerr << "===CheckEventsLine: " << line << Endl;
        NJson::TJsonValue json;
        NJson::ReadJsonTree(line, &json, true);
        auto &map = json.GetMap();
        if (!queueName.empty()) {
            auto resId = map.find("resource_id")->second.GetString();
            UNIT_ASSERT_STRINGS_EQUAL(resId, queueName);
        }
        bool hasDeleted = map.find("deleted") != map.end();
        bool hasReindex = map.find("reindex_timestamp") != map.end();
        auto& res_path = map.find("resource_path")->second.GetArray();
        UNIT_ASSERT_VALUES_EQUAL(res_path.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(res_path[0].GetMap().size(), 2);

        if (labels.empty() || labels == "{}") {
            auto it1 = map.find("attributes");
            if (it1 != map.end()) {
                auto& attributes = it1->second.GetMap();
                auto it2 = attributes.find("labels");
                if (it2 != attributes.end()) {
                    UNIT_ASSERT(it2->second.GetMap().empty());
                }
            }
        } else {
            NJson::TJsonMap labelsMap;
            NJson::ReadJsonTree(labels, &labelsMap);
            auto it1 = map.find("attributes");
            UNIT_ASSERT(it1 != map.end());
            auto& attributes = it1->second.GetMap();
            auto it2 = attributes.find("labels");
            UNIT_ASSERT(it2 != attributes.end());
            UNIT_ASSERT_VALUES_EQUAL(it2->second.GetMap(), labelsMap.GetMap());
        }

        switch (type) {
            case EEvType::Existed:
                UNIT_ASSERT(!hasDeleted);
                UNIT_ASSERT(hasReindex);
                UNIT_ASSERT(!map.find("reindex_timestamp")->second.GetString().empty());
                break;
            case EEvType::Created:
                UNIT_ASSERT(!hasDeleted);
                UNIT_ASSERT(!hasReindex);
                break;
            case EEvType::Deleted:
                UNIT_ASSERT(hasDeleted);
                UNIT_ASSERT(!map.find("deleted")->second.GetString().empty());
                UNIT_ASSERT(!hasReindex);
                break;
        }
    }

    void TestCreateIndexProcessor() {
        TTestRunner("CreateIndexProcessor", this);
    }

    void CheckSingleCreateQueueEvent(bool nullLabels) {
        TTestRunner runner{"SingleCreateQueueEvent", this};
        const TString labels = "{\"k1\": \"v1\"}";
        const TString escapedLabels = EscapeC(labels);
        runner.AddEvent("cloud1", "queue1", EEvType::Created, {}, nullLabels ? Nothing() : TMaybe<TString>(escapedLabels));
        runner.DispatchEvents();
        auto messages = runner.EventsWriter->GetMessages();
        UNIT_ASSERT_VALUES_EQUAL(messages.size(), 2); // Events, reindex
        CheckEventsLine(messages[0], EEvType::Created, {}, nullLabels ? "{}" : labels);
        CheckEventsLine(messages[1], EEvType::Existed, {}, nullLabels ? "{}" : labels);
        UNIT_ASSERT_VALUES_EQUAL(runner.CountEvents(), 0);
    }

    void TestSingleCreateQueueEvent() {
        CheckSingleCreateQueueEvent(false);
        CheckSingleCreateQueueEvent(true);
    }

    void CheckReindexSingleQueue(bool nullLabels) {
        TTestRunner runner{"ReindexSingleQueue", this};
        const TString labels = "{\"k1\": \"v1\"}";
        const TString escapedLabels = EscapeC(labels);
        runner.AddQueue("cloud1", "queue1", {}, nullLabels ? Nothing() : TMaybe<TString>(escapedLabels));
        runner.DispatchEvents();
        auto messages = runner.EventsWriter->GetMessages();
        UNIT_ASSERT_VALUES_EQUAL(messages.size(), 1);
        CheckEventsLine(messages[0], EEvType::Existed, {}, nullLabels ? "{}" : labels);
    }

    void TestReindexSingleQueue() {
        CheckReindexSingleQueue(false);
        CheckReindexSingleQueue(true);
    }

    void CheckDeletedQueueNotReindexed(bool nullLabels) {
        TTestRunner runner{"DeletedQueueNotReindexed", this};
        const TString labels = "{\"k1\": \"v1\"}";
        const TString escapedLabels = EscapeC(labels);
        runner.AddQueue("cloud1", "queue2", runner.PrevTs, nullLabels ? Nothing() : TMaybe<TString>(escapedLabels));
        runner.AddEvent("cloud1", "queue2", EEvType::Deleted, {}, nullLabels ? Nothing() : TMaybe<TString>(escapedLabels));
        Sleep(TDuration::Seconds(1));
        runner.DispatchEvents();
        auto messages = runner.EventsWriter->GetMessages();
        UNIT_ASSERT_VALUES_EQUAL(messages.size(), 1);
        CheckEventsLine(messages[0], EEvType::Deleted, {}, nullLabels ? "{}" : labels);
    }

    void TestDeletedQueueNotReindexed() {
        CheckDeletedQueueNotReindexed(false);
        CheckDeletedQueueNotReindexed(true);
    }

    void TestManyMessages() {
        TTestRunner runner{"TestManyMessages", this};
        const TString labels = "{\"k1\": \"v1\"}";
        const TString escapedLabels = EscapeC(labels);
        runner.AddQueue("cloud1", "existing1", runner.PrevTs, escapedLabels);
        runner.AddQueue("cloud1", "existing2", runner.PrevTs, escapedLabels);
        runner.AddQueue("cloud1", "existing3", runner.PrevTs, escapedLabels);
        runner.AddQueue("cloud1", "deleting1", runner.PrevTs, escapedLabels);
        runner.AddQueue("cloud1", "deleting2", runner.PrevTs, escapedLabels);
        runner.AddEvent("cloud1", "deleting1", EEvType::Deleted, {}, escapedLabels);
        runner.AddEvent("cloud1", "deleting2", EEvType::Deleted, {}, escapedLabels);
        runner.AddEvent("cloud1", "creating1", EEvType::Created, {}, escapedLabels);
        runner.AddEvent("cloud1", "creating2", EEvType::Created, {}, escapedLabels);
        UNIT_ASSERT_VALUES_EQUAL(runner.CountEvents(), 4);

        runner.DispatchEvents();
        auto messages = runner.EventsWriter->GetMessages();
        UNIT_ASSERT_VALUES_EQUAL(messages.size(), 9); //4 events, 5 queues in reindex
        for (auto i = 0u; i < 4; i++) {
            if (messages[i].find("creating") != TString::npos)
                CheckEventsLine(messages[i], EEvType::Created, {}, labels);
            else {
                UNIT_ASSERT(messages[i].find("deleting") != TString::npos);
                UNIT_ASSERT(messages[i].find("existing") == TString::npos);
                CheckEventsLine(messages[i], EEvType::Deleted, {}, labels);
            }
        }
        for (auto i = 4u; i < messages.size(); i++) {
            UNIT_ASSERT_C(
                    messages[i].find("existing") != TString::npos
                    || messages[i].find("creating") != TString::npos,
                    messages[i].c_str()
            );
            CheckEventsLine(messages[i], EEvType::Existed, {}, labels);
        }
        UNIT_ASSERT_VALUES_EQUAL(runner.CountEvents(), 0);
    }

    void TestOver1000Queues() {
        TTestRunner runner{"TestOver1000Queues", this};
        ui64 queuesCount = 2010;
        const TString labels = "{\"k1\": \"v1\"}";
        const TString escapedLabels = EscapeC(labels);
        runner.AddQueuesBatch("cloud2", "queue-", queuesCount, 0, escapedLabels);
        UNIT_ASSERT_VALUES_EQUAL(runner.CountQueues(), queuesCount);
        Sleep(TDuration::Seconds(1));
        runner.EventsWriter->GetMessages(); //reset messages;
        runner.DispatchEvents();
        auto messages = runner.EventsWriter->GetMessages();
        UNIT_ASSERT_VALUES_EQUAL(messages.size(), queuesCount);

    }
};
UNIT_TEST_SUITE_REGISTRATION(TIndexProcesorTests);
} // NKikimr::NSQS
