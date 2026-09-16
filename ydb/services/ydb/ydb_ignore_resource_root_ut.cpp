#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/public/api/grpc/ydb_discovery_v1.grpc.pb.h>
#include <ydb/public/api/grpc/ydb_topic_v1.grpc.pb.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/coordination/coordination.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/rate_limiter/rate_limiter.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/scheme/scheme.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/table/table.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/client.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/scope.h>

#include <chrono>
#include <utility>

namespace NKikimr::NGRpcService {
namespace {

using namespace NYdb;

template <typename TResult>
TResult Await(NThreading::TFuture<TResult> future) {
    UNIT_ASSERT_C(future.Wait(TDuration::Seconds(30)), "Request timed out");
    return future.ExtractValueSync();
}

template <typename TResult>
TResult Success(NThreading::TFuture<TResult> future) {
    auto result = Await(std::move(future));
    UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
    return result;
}

class TResourceEnvironment {
public:
    const TString Root;
    const TString Name;
    const TString OldDatabase;
    const TString Database;
    NKqp::TKikimrRunner Runner;
    TString Endpoint;

    static NKqp::TKikimrSettings Settings(const TString& root, bool ignoreRoot) {
        NKqp::TKikimrSettings settings;
        settings.SetDomainRoot(root).SetWithSampleTables(false).SetAuthToken("root@builtin")
            .SetDynamicNodeCount(2).SetStoragePoolTypes({"ssd"});
        settings.AppConfig.MutableGRpcConfig()->SetIgnoreRoot(ignoreRoot);
        settings.FeatureFlags.SetCheckDatabaseAccessPermission(true);
        settings.PQConfig.SetTopicsAreFirstClassCitizen(true);
        return settings;
    }

    TResourceEnvironment(bool singleComponent, bool ignoreRoot)
        : Root("failover")
        , Name("mydb123")
        , OldDatabase(singleComponent ? "/ru" : "/ru/mydb123")
        , Database(singleComponent ? "/" + Root : "/" + Root + "/" + Name)
        , Runner(Settings(Root, ignoreRoot))
    {
        auto& runtime = *Runner.GetTestServer().GetRuntime();
        if (!singleComponent) {
            // CreateDatabase uses an anonymous local RPC. Restore authentication before test requests.
            auto& allowedSids = runtime.GetAppData().AdministrationAllowedSIDs;
            auto savedAllowedSids = std::exchange(allowedSids, {});
            Y_DEFER {
                allowedSids.swap(savedAllowedSids);
            };
            UNIT_ASSERT_VALUES_EQUAL(Runner.RunCall([&] {
                return Runner.CreateDatabase(Name, "ssd", {});
            }), Database);
        }
        for (ui32 node = 1; node < runtime.GetNodeCount(); ++node) {
            runtime.GetAppData(node).AdministrationAllowedSIDs.push_back("root@builtin");
        }

        auto discovery = Ydb::Discovery::V1::DiscoveryService::NewStub(
            grpc::CreateChannel(Runner.GetEndpoint(), grpc::InsecureChannelCredentials()));
        grpc::ClientContext context;
        Configure(context, Database);
        Ydb::Discovery::ListEndpointsRequest request;
        request.set_database(Database);
        Ydb::Discovery::ListEndpointsResponse response;
        const auto status = discovery->ListEndpoints(&context, request, &response);
        UNIT_ASSERT_C(status.ok(), status.error_message());
        UNIT_ASSERT_C(response.operation().status() == Ydb::StatusIds::SUCCESS, response.DebugString());
        Ydb::Discovery::ListEndpointsResult result;
        UNIT_ASSERT(response.operation().result().UnpackTo(&result));
        UNIT_ASSERT_C(result.endpoints_size(), response.DebugString());
        Endpoint = TStringBuilder() << result.endpoints(0).address() << ':' << result.endpoints(0).port();
    }

    void Configure(grpc::ClientContext& context, const TString& database) const {
        context.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(30));
        context.AddMetadata("x-ydb-database", database);
        context.AddMetadata("x-ydb-auth-ticket", "root@builtin");
    }

    TDriver Driver(const TString& database, const TString& token = "root@builtin") const {
        return TDriver(TDriverConfig().SetEndpoint(Endpoint).SetDatabase(database)
            .SetAuthToken(token).SetDiscoveryMode(EDiscoveryMode::Sync));
    }

    TVector<TString> Paths(const TString& path) const {
        return {OldDatabase + '/' + path, path, Database + '/' + path};
    }

    void UpdateOffsets(const NYdb::NTable::TTransaction& tx, const TString& path, ui64 begin) const {
        auto topic = Ydb::Topic::V1::TopicService::NewStub(
            grpc::CreateChannel(Endpoint, grpc::InsecureChannelCredentials()));
        grpc::ClientContext context;
        Configure(context, OldDatabase);
        Ydb::Topic::UpdateOffsetsInTransactionRequest request;
        request.mutable_operation_params()->set_operation_mode(Ydb::Operations::OperationParams::SYNC);
        request.mutable_tx()->set_session(TString(tx.GetSession().GetId()));
        request.mutable_tx()->set_id(TString(tx.GetId()));
        request.set_consumer("consumer");
        auto* resource = request.add_topics();
        resource->set_path(path);
        auto* partition = resource->add_partitions();
        partition->set_partition_id(0);
        auto* offsets = partition->add_partition_offsets();
        offsets->set_start(begin);
        offsets->set_end(begin + 1);
        Ydb::Topic::UpdateOffsetsInTransactionResponse response;
        const auto status = topic->UpdateOffsetsInTransaction(&context, request, &response);
        UNIT_ASSERT_C(status.ok(), status.error_message());
        UNIT_ASSERT_C(response.operation().status() == Ydb::StatusIds::SUCCESS, response.DebugString());
    }
};

void CheckReadTable(NYdb::NTable::TSession& session, const TString& path) {
    auto iterator = Success(session.ReadTable(path));
    size_t rows = 0;
    while (true) {
        auto part = Await(iterator.ReadNext());
        if (part.EOS()) {
            break;
        }
        UNIT_ASSERT_C(part.IsSuccess(), part.GetIssues().ToString());
        TResultSetParser parser(part.ExtractPart());
        while (parser.TryNextRow()) {
            UNIT_ASSERT_VALUES_EQUAL(parser.ColumnParser("Payload").GetOptionalUtf8().value(), "target");
            ++rows;
        }
    }
    UNIT_ASSERT_VALUES_EQUAL(rows, 1);
}

void CheckTopicRead(NTopic::TTopicClient& client, const TString& path, ui64 firstOffset = 0) {
    NTopic::TReadSessionSettings settings;
    settings.AppendTopics(NTopic::TTopicReadSettings(path).AppendPartitionIds(0));
    if (firstOffset) {
        settings.ConsumerName("consumer");
    } else {
        settings.WithoutConsumer();
    }
    auto session = client.CreateReadSession(settings);
    const auto deadline = TInstant::Now() + TDuration::Seconds(30);
    ui64 offset = firstOffset;
    while (offset < 4) {
        UNIT_ASSERT_C(session->WaitEvent().Wait(deadline), "No topic data for " << path);
        auto event = session->GetEvent();
        if (!event) {
            continue;
        }
        if (auto* start = std::get_if<NTopic::TReadSessionEvent::TStartPartitionSessionEvent>(&*event)) {
            UNIT_ASSERT_VALUES_EQUAL(start->GetPartitionSession()->GetPartitionId(), 0);
            UNIT_ASSERT_VALUES_EQUAL(start->GetCommittedOffset(), firstOffset);
            start->Confirm();
        } else if (auto* data = std::get_if<NTopic::TReadSessionEvent::TDataReceivedEvent>(&*event)) {
            for (const auto& message : data->GetMessages()) {
                UNIT_ASSERT_VALUES_EQUAL(message.GetOffset(), offset);
                UNIT_ASSERT_VALUES_EQUAL(message.GetData(), offset == 3 ? "transaction" : "message");
                ++offset;
            }
        } else if (auto* stop = std::get_if<NTopic::TReadSessionEvent::TStopPartitionSessionEvent>(&*event)) {
            stop->Confirm();
        } else if (auto* closed = std::get_if<NTopic::TSessionClosedEvent>(&*event)) {
            UNIT_FAIL(closed->DebugString());
        }
    }
    UNIT_ASSERT_VALUES_EQUAL(offset, 4);
    UNIT_ASSERT(session->Close(TDuration::Seconds(10)));
}

void CheckTopicPaths(const TResourceEnvironment& env, const TDriver& driver, NYdb::NTable::TSession& tableSession) {
    NTopic::TTopicClient topic(driver);
    const auto paths = env.Paths("dir/nested/topic");
    NTopic::TCreateTopicSettings settings;
    settings.BeginConfigurePartitioningSettings().MinActivePartitions(1).EndConfigurePartitioningSettings();
    settings.BeginAddConsumer("consumer").EndAddConsumer();
    Success(topic.CreateTopic(paths[0], settings));

    for (size_t index = 0; index < paths.size(); ++index) {
        Success(topic.DescribeTopic(paths[index]));
        auto writer = topic.CreateSimpleBlockingWriteSession(NTopic::TWriteSessionSettings()
            .Path(paths[index]).ProducerId("producer-" + ToString(index))
            .PartitionId(0).DirectWriteToPartition(true).Codec(NTopic::ECodec::RAW));
        UNIT_ASSERT(writer->Write(NTopic::TWriteMessage("message"), nullptr, TDuration::Seconds(30)));
        UNIT_ASSERT(writer->Close(TDuration::Seconds(30)));
    }

    auto transaction = Success(tableSession.BeginTransaction(NYdb::NTable::TTxSettings::SerializableRW())).GetTransaction();
    auto writer = topic.CreateSimpleBlockingWriteSession(NTopic::TWriteSessionSettings()
        .Path(paths[0]).ProducerId("transaction-producer").PartitionId(0).Codec(NTopic::ECodec::RAW));
    UNIT_ASSERT(writer->Write(NTopic::TWriteMessage("transaction"), &transaction, TDuration::Seconds(30)));
    UNIT_ASSERT(writer->Close(TDuration::Seconds(30)));
    Success(transaction.Commit());

    for (const auto& path : paths) {
        CheckTopicRead(topic, path);
    }
    for (size_t index = 0; index < paths.size(); ++index) {
        auto tx = Success(tableSession.BeginTransaction(NYdb::NTable::TTxSettings::SerializableRW())).GetTransaction();
        env.UpdateOffsets(tx, paths[index], index);
        Success(tx.Commit());
    }
    CheckTopicRead(topic, paths[0], 3);
    Success(topic.DropTopic(paths[0]));
}

} // namespace

Y_UNIT_TEST_SUITE(YdbIgnoreResourceRoot) {
    Y_UNIT_TEST(SlashlessFormerRootRemainsRelative) {
        TResourceEnvironment env(true, true);
        auto driver = env.Driver(env.OldDatabase);
        NYdb::NScheme::TSchemeClient scheme(driver);
        Success(scheme.MakeDirectory("ru"));
        Success(scheme.MakeDirectory("ru/nested"));
        Success(scheme.DescribePath(env.Database + "/ru/nested"));
        Success(scheme.DescribePath(env.OldDatabase + "/ru/nested"));
        const auto rootPath = Await(scheme.DescribePath(env.OldDatabase + "/nested"));
        UNIT_ASSERT_C(rootPath.GetStatus() == EStatus::SCHEME_ERROR, rootPath.GetIssues().ToString());

        NTopic::TTopicClient topic(driver);
        NTopic::TCreateTopicSettings settings;
        settings.BeginConfigurePartitioningSettings().MinActivePartitions(1).EndConfigurePartitioningSettings();
        Success(topic.CreateTopic("ru/topic", settings));
        Success(topic.DescribeTopic(env.Database + "/ru/topic"));
        Success(topic.DescribeTopic(env.OldDatabase + "/ru/topic"));
        const auto rootTopic = Await(topic.DescribeTopic(env.OldDatabase + "/topic"));
        UNIT_ASSERT_C(rootTopic.GetStatus() == EStatus::SCHEME_ERROR, rootTopic.GetIssues().ToString());
    }

    Y_UNIT_TEST_TWIN(SchemaAndStreamingPaths, SingleComponent) {
        TResourceEnvironment env(SingleComponent, true);
        // The SDK keeps the original database through discovery and subsequent RPCs.
        auto driver = env.Driver(env.OldDatabase);
        NYdb::NScheme::TSchemeClient scheme(driver);
        Success(scheme.MakeDirectory(env.OldDatabase + "/dir"));
        Success(scheme.MakeDirectory("dir/nested"));
        for (const auto& path : env.Paths("dir/nested")) {
            Success(scheme.DescribePath(path));
            Success(scheme.ListDirectory(path));
        }

        NYdb::NTable::TTableClient table(driver);
        auto session = Success(table.CreateSession()).GetSession();
        auto builder = table.GetTableBuilder();
        builder.AddNullableColumn("Id", EPrimitiveType::Uint64)
            .AddNullableColumn("Payload", EPrimitiveType::Utf8).SetPrimaryKeyColumn("Id");
        Success(session.CreateTable(env.OldDatabase + "/dir/nested/data", builder.Build()));
        TValueBuilder rows;
        rows.BeginList().AddListItem().BeginStruct()
            .AddMember("Id").Uint64(1).AddMember("Payload").Utf8("target")
            .EndStruct().EndList();
        Success(table.BulkUpsert("dir/nested/data", rows.Build()));
        for (const auto& path : env.Paths("dir/nested/data")) {
            Success(session.DescribeTable(path));
            CheckReadTable(session, path);
        }
        Success(session.CopyTable(env.OldDatabase + "/dir/nested/data", env.Database + "/dir/nested/copy"));
        Success(session.RenameTables({{"dir/nested/copy", env.OldDatabase + "/dir/nested/renamed"}}));
        CheckReadTable(session, env.OldDatabase + "/dir/nested/renamed");

        NCoordination::TClient coordination(driver);
        Success(coordination.CreateNode(env.OldDatabase + "/dir/nested/node"));
        NRateLimiter::TRateLimiterClient rateLimiter(driver);
        Success(rateLimiter.CreateResource(env.OldDatabase + "/dir/nested/node", "quota",
            NRateLimiter::TCreateResourceSettings().MaxUnitsPerSecond(100)));
        Success(rateLimiter.CreateResource("dir/nested/node", "quota/child"));
        for (const auto& path : env.Paths("dir/nested/node")) {
            Success(coordination.DescribeNode(path));
            auto semaphoreSession = Success(coordination.StartSession(path)).ExtractResult();
            Success(semaphoreSession.CreateSemaphore("semaphore", 1));
            UNIT_ASSERT(Success(semaphoreSession.AcquireSemaphore("semaphore",
                NCoordination::TAcquireSemaphoreSettings().Count(1).Timeout(TDuration::Seconds(5)))).GetResult());
            UNIT_ASSERT(Success(semaphoreSession.ReleaseSemaphore("semaphore")).GetResult());
            Success(semaphoreSession.DeleteSemaphore("semaphore"));
            Success(semaphoreSession.Close());
            const auto resource = Success(rateLimiter.DescribeResource(path, "quota/child"));
            UNIT_ASSERT_VALUES_EQUAL(resource.GetResourcePath(), "quota/child");
            Success(rateLimiter.AcquireResource(path, "quota/child",
                NRateLimiter::TAcquireResourceSettings().Amount(1)
                    .OperationTimeout(TDuration::Seconds(10)).CancelAfter(TDuration::Seconds(5))));
        }
        for (const auto& database : {env.Database, env.OldDatabase}) {
            auto result = Await(rateLimiter.CreateResource(database + "2/dir/nested/node", "quota",
                NRateLimiter::TCreateResourceSettings().MaxUnitsPerSecond(1)));
            UNIT_ASSERT_C(result.GetStatus() == EStatus::BAD_REQUEST, result.GetIssues().ToString());
        }

        CheckTopicPaths(env, driver, session);

        Success(scheme.ModifyPermissions(env.OldDatabase, NYdb::NScheme::TModifyPermissionsSettings()
            .AddGrantPermissions(NYdb::NScheme::TPermissions("connect-only@builtin", {"ydb.database.connect"}))));
        auto deniedDriver = env.Driver(env.OldDatabase, "connect-only@builtin");
        NYdb::NTable::TTableClient deniedTable(deniedDriver);
        auto deniedSession = Success(deniedTable.CreateSession()).GetSession();
        auto iterator = Await(deniedSession.ReadTable(env.OldDatabase + "/dir/nested/data"));
        if (iterator.IsSuccess()) {
            auto part = Await(iterator.ReadNext());
            UNIT_ASSERT_C(part.GetStatus() == EStatus::UNAUTHORIZED, part.GetIssues().ToString());
        } else {
            UNIT_ASSERT_C(iterator.GetStatus() == EStatus::UNAUTHORIZED, iterator.GetIssues().ToString());
        }
        NCoordination::TClient deniedCoordination(deniedDriver);
        const auto denied = Await(deniedCoordination.StartSession(env.OldDatabase + "/dir/nested/node"));
        UNIT_ASSERT_C(denied.GetStatus() == EStatus::UNAUTHORIZED, denied.GetIssues().ToString());
    }

    Y_UNIT_TEST_TWIN(DisabledKeepsAbsoluteResources, SingleComponent) {
        TResourceEnvironment env(SingleComponent, false);
        auto driver = env.Driver(env.Database);
        NYdb::NScheme::TSchemeClient scheme(driver);
        Success(scheme.MakeDirectory(env.Database + "/dir"));
        Success(scheme.MakeDirectory("dir/nested"));
        Success(scheme.DescribePath(env.Database + "/dir/nested"));
        Success(scheme.DescribePath("dir/nested"));
        const auto oldPath = Await(scheme.DescribePath(env.OldDatabase + "/dir/nested"));
        UNIT_ASSERT_C(oldPath.GetStatus() == EStatus::SCHEME_ERROR, oldPath.GetIssues().ToString());

        NCoordination::TClient coordination(driver);
        Success(coordination.CreateNode(env.Database + "/dir/nested/node"));
        NRateLimiter::TRateLimiterClient rateLimiter(driver);
        const auto result = Await(rateLimiter.CreateResource(env.OldDatabase + "/dir/nested/node", "quota",
            NRateLimiter::TCreateResourceSettings().MaxUnitsPerSecond(1)));
        UNIT_ASSERT_C(result.GetStatus() == EStatus::BAD_REQUEST, result.GetIssues().ToString());
    }
}

} // namespace NKikimr::NGRpcService
