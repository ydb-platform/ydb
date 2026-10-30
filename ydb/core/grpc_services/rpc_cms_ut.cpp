#include "local_rpc/local_rpc.h"
#include "rpc_calls.h"

#include <ydb/core/cms/console/console.h>
#include <ydb/core/change_exchange/change_exchange.h>
#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/core/persqueue/writer/writer.h>
#include <ydb/core/testlib/actors/block_events.h>
#include <ydb/core/tx/datashard/ut_common/datashard_ut_common.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>
#include <ydb/public/api/grpc/ydb_cms_v1.grpc.pb.h>
#include <ydb/public/api/grpc/ydb_operation_v1.grpc.pb.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/client.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/cms/cms.h>
#include <library/cpp/json/json_reader.h>

#include <grpcpp/create_channel.h>
#include <grpcpp/security/credentials.h>
#include <google/protobuf/descriptor.h>
#include <google/protobuf/unknown_field_set.h>

namespace NKikimr::NGRpcService {

using namespace NDataShard;
using namespace Tests;

namespace {

const TString Database = "/Root/FrozenDatabase";

NKqp::TKikimrSettings Settings() {
    NKikimrPQ::TPQConfig pq;
    pq.SetEnabled(true);
    pq.SetEnableProtoSourceIdInfo(true);
    pq.SetTopicsAreFirstClassCitizen(true);
    pq.SetMaxReadCookies(10);
    pq.AddClientServiceType()->SetName("data-streams");
    pq.SetCheckACL(false);
    pq.SetRequireCredentialsInNewProtocol(false);
    pq.MutableQuotingConfig()->SetEnableQuoting(false);
    return NKqp::TKikimrSettings().SetUseRealThreads(false).SetPQConfig(pq)
        .SetWithSampleTables(false).SetDynamicNodeCount(1).SetStoragePoolTypes({"hdd1"});
}

struct TEnv {
    NKqp::TKikimrRunner Kikimr;
    TServer::TPtr Server;
    TTestActorRuntime& Runtime;
    TTenants Tenants;
    TActorId Sender;
    ui64 TableSchemeShard = 0;
    std::shared_ptr<grpc::Channel> Channel;
    std::unique_ptr<Ydb::Cms::V1::CmsService::Stub> Cms;
    std::unique_ptr<Ydb::Operation::V1::OperationService::Stub> Operations;

    TEnv()
        : Kikimr(Settings())
        , Server(&Kikimr.GetTestServer())
        , Runtime(*Server->GetRuntime())
        , Tenants(Server)
        , Sender(Runtime.AllocateEdgeActor())
        , Channel(grpc::CreateChannel(Kikimr.GetEndpoint().c_str(), grpc::InsecureChannelCredentials()))
        , Cms(Ydb::Cms::V1::CmsService::NewStub(Channel))
        , Operations(Ydb::Operation::V1::OperationService::NewStub(Channel))
    {
        using TCreate = TGrpcRequestOperationCall<Ydb::Cms::CreateDatabaseRequest, Ydb::Cms::CreateDatabaseResponse>;
        Ydb::Cms::CreateDatabaseRequest request;
        request.set_path(Database);
        auto* storage = request.mutable_resources()->add_storage_units();
        storage->set_unit_kind("hdd1");
        storage->set_count(1);
        auto response = Runtime.WaitFuture(NRpcService::DoLocalRpc<TCreate>(
            std::move(request), "", "", Runtime.GetActorSystem(0)));
        UNIT_ASSERT_VALUES_EQUAL_C(response.operation().status(), Ydb::StatusIds::SUCCESS, response.DebugString());
        Tenants.Run(Database, 1);
        Ydb::Cms::GetDatabaseStatusResult state;
        for (ui32 attempt = 0; attempt < 300; ++attempt) {
            auto request = MakeHolder<NConsole::TEvConsole::TEvGetTenantStatusRequest>();
            request->Record.MutableRequest()->set_path(Database);
            Runtime.SendToPipe(MakeConsoleID(), Sender, request.Release(), 0, GetPipeConfigWithRetries());
            auto result = Runtime.GrabEdgeEvent<NConsole::TEvConsole::TEvGetTenantStatusResponse>(Sender, TDuration::Seconds(30));
            UNIT_ASSERT_C(result, "No status of the database " << Database);
            UNIT_ASSERT(result->Get()->Record.GetResponse().operation().result().UnpackTo(&state));
            if (state.state() == Ydb::Cms::GetDatabaseStatusResult::RUNNING) {
                return;
            }
            Runtime.SimulateSleep(TDuration::MilliSeconds(100));
        }
        UNIT_FAIL("The database " << Database << " is not running: " << state.DebugString());
    }

    Ydb::Operations::Operation Alter(Ydb::Cms::AlterDatabaseRequest request) {
        if (request.path().empty()) {
            request.set_path(Database);
        }
        return Kikimr.RunCall([&] {
            grpc::ClientContext context;
            context.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(30));
            Ydb::Cms::AlterDatabaseResponse response;
            const auto status = Cms->AlterDatabase(&context, request, &response);
            UNIT_ASSERT_C(status.ok(), status.error_message());
            return response.operation();
        });
    }

    Ydb::Operations::Operation Freeze(bool freeze,
            Ydb::Operations::OperationParams::OperationMode mode = Ydb::Operations::OperationParams::ASYNC,
            const TString& key = {})
    {
        Ydb::Cms::AlterDatabaseRequest request;
        request.set_freeze(freeze);
        request.set_idempotency_key(key);
        request.mutable_operation_params()->set_operation_mode(mode);
        return Alter(std::move(request));
    }

    Ydb::Operations::Operation Get(const TString& id) {
        return Kikimr.RunCall([&] {
            grpc::ClientContext context;
            context.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(30));
            Ydb::Operations::GetOperationRequest request;
            request.set_id(id);
            Ydb::Operations::GetOperationResponse response;
            const auto status = Operations->GetOperation(&context, request, &response);
            UNIT_ASSERT_C(status.ok(), status.error_message());
            return response.operation();
        });
    }

    Ydb::Operations::Operation Wait(const TString& id) {
        Ydb::Operations::Operation result;
        for (ui32 attempt = 0; attempt < 300; ++attempt) {
            result = Get(id);
            if (result.ready()) {
                if (result.status() != Ydb::StatusIds::SUCCESS) {
                    Cerr << "Database freeze operation: " << result.ShortDebugString() << Endl;
                }
                return result;
            }
            Runtime.SimulateSleep(TDuration::MilliSeconds(100));
        }
        UNIT_FAIL("CMS database freeze operation did not finish: " << result.DebugString());
        return result;
    }

    auto CreateTable(bool cdc = false, bool resolved = false, bool initialScan = false) {
        const auto created = Scheme("CREATE TABLE `Table` (key Uint32, value Uint32, PRIMARY KEY (key)) "
            "WITH (UNIFORM_PARTITIONS = 2);");
        UNIT_ASSERT_C(created.IsSuccess(), created.GetIssues().ToString());
        auto request = MakeHolder<NSchemeCache::TSchemeCacheNavigate>();
        request->DatabaseName = Database;
        auto& entry = request->ResultSet.emplace_back();
        entry.Path = NKikimr::SplitPath(Database + "/Table");
        entry.Operation = NSchemeCache::TSchemeCacheNavigate::OpTable;
        entry.SyncVersion = true;
        Runtime.Send(new IEventHandle(MakeSchemeCacheID(), Sender,
            new TEvTxProxySchemeCache::TEvNavigateKeySet(request.Release())));
        auto response = Runtime.GrabEdgeEventRethrow<TEvTxProxySchemeCache::TEvNavigateKeySetResult>(Sender);
        const auto& navigation = *response->Get()->Request;
        UNIT_ASSERT_VALUES_EQUAL(navigation.ResultSet.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL_C(navigation.ErrorCount, 0,
            "Table navigation status: " << ui32(navigation.ResultSet.front().Status));
        const auto tableId = navigation.ResultSet.front().TableId;
        TableSchemeShard = tableId.PathId.OwnerId;
        auto table = std::make_pair(GetTableShards(Server, Sender, Database + "/Table"), tableId);
        if (cdc) {
            AddStream(resolved, initialScan);
        }
        return table;
    }

    void AddStream(bool resolved = false, bool initialScan = false) {
        TShardedTableOptions::TCdcStream stream{
            .Name = "Stream",
            .Mode = NKikimrSchemeOp::ECdcStreamModeUpdate,
            .Format = NKikimrSchemeOp::ECdcStreamFormatJson,
            .TopicPartitions = 2,
        };
        if (resolved) {
            stream.VirtualTimestamps = true;
            stream.ResolvedTimestamps = TDuration::Seconds(1);
        }
        if (initialScan) {
            stream.InitialState = NKikimrSchemeOp::ECdcStreamStateScan;
        }
        auto notification = MakeHolder<NSchemeShard::TEvSchemeShard::TEvNotifyTxCompletion>();
        notification->Record.SetTxId(AsyncAlterAddStream(Server, Database, "Table", stream));
        Runtime.SendToPipe(TableSchemeShard, Sender, notification.Release(), 0, GetPipeConfigWithRetries());
        Runtime.GrabEdgeEventRethrow<NSchemeShard::TEvSchemeShard::TEvNotifyTxCompletionResult>(Sender);
    }

    auto Write(const TString& sql) {
        auto client = Kikimr.GetQueryClient(NYdb::NQuery::TClientSettings().Database(Database)
            .DiscoveryMode(NYdb::EDiscoveryMode::Off));
        return Kikimr.RunCall([&] {
            return client.ExecuteQuery(sql, NYdb::NQuery::TTxControl::BeginTx().CommitTx(),
                NYdb::NQuery::TExecuteQuerySettings().ClientTimeout(TDuration::Seconds(30))
                    .RetrySettings(NYdb::NRetry::TRetryOperationSettings().MaxRetries(0))).ExtractValueSync();
        });
    }

    NYdb::NQuery::TExecuteQueryResult Scheme(const TString& sql) {
        auto client = Kikimr.GetQueryClient(NYdb::NQuery::TClientSettings().Database(Database)
            .DiscoveryMode(NYdb::EDiscoveryMode::Off));
        return Kikimr.RunCall([&] {
            return client.ExecuteQuery(sql, NYdb::NQuery::TTxControl::NoTx(),
                NYdb::NQuery::TExecuteQuerySettings().ClientTimeout(TDuration::Seconds(30))
                    .RetrySettings(NYdb::NRetry::TRetryOperationSettings().MaxRetries(0))).ExtractValueSync();
        });
    }

    std::shared_ptr<NYdb::NTopic::IReadSession> Reader() {
        NYdb::NTopic::TTopicClient client(Kikimr.GetDriver(), NYdb::NTopic::TTopicClientSettings().Database(Database)
            .DiscoveryMode(NYdb::EDiscoveryMode::Off));
        return client.CreateReadSession(NYdb::NTopic::TReadSessionSettings().WithoutConsumer()
            .ReadFromTimestamp(TInstant::Zero()).AppendTopics(NYdb::NTopic::TTopicReadSettings((Database + "/Table/Stream").c_str())
                .AppendPartitionIds(0).AppendPartitionIds(1)));
    }

    void ExpectChange(const std::shared_ptr<NYdb::NTopic::IReadSession>& reader, ui32 key, ui32 value) {
        for (;;) {
            auto event = Kikimr.RunCall([&] { return reader->GetEvent(true); });
            UNIT_ASSERT(event);
            if (auto* start = std::get_if<NYdb::NTopic::TReadSessionEvent::TStartPartitionSessionEvent>(&*event)) {
                start->Confirm();
            } else if (auto* data = std::get_if<NYdb::NTopic::TReadSessionEvent::TDataReceivedEvent>(&*event)) {
                for (const auto& message : data->GetMessages()) {
                    NJson::TJsonValue body;
                    UNIT_ASSERT(NJson::ReadJsonTree(TString(message.GetData()), &body));
                    if (body.Has("resolved")) {
                        continue;
                    }
                    UNIT_ASSERT_VALUES_EQUAL(body["key"][0].GetInteger(), key);
                    UNIT_ASSERT_VALUES_EQUAL(body["update"]["value"].GetInteger(), value);
                    return;
                }
            } else if (auto* stop = std::get_if<NYdb::NTopic::TReadSessionEvent::TStopPartitionSessionEvent>(&*event)) {
                stop->Confirm();
            } else if (auto* closed = std::get_if<NYdb::NTopic::TSessionClosedEvent>(&*event)) {
                UNIT_FAIL(closed->DebugString());
            }
        }
    }
};

} // anonymous namespace

Y_UNIT_TEST_SUITE(CmsDatabaseFreeze) {
    Y_UNIT_TEST(SdkPreservesFreezePresence) {
        Ydb::Cms::AlterDatabaseRequest request;
        NYdb::NCms::TAlterDatabaseSettings().SerializeTo(request);
        UNIT_ASSERT(!request.has_freeze());
        NYdb::NCms::TAlterDatabaseSettings().Freeze(false).SerializeTo(request);
        UNIT_ASSERT(request.has_freeze());
        UNIT_ASSERT(!request.freeze());
        NYdb::NCms::TAlterDatabaseSettings().Freeze(true).SerializeTo(request);
        UNIT_ASSERT(request.has_freeze() && request.freeze());
    }
    Y_UNIT_TEST(OperationWaitsForTopicAckAndThawRestoresWrites) {
        TEnv env;
        const auto [shards, tableId] = env.CreateTable(true);
        TBlockEvents<NPQ::TEvPartitionWriter::TEvWriteResponse> ack(env.Runtime);
        auto write = env.Write("UPSERT INTO `Table` (key, value) VALUES (1, 10);");
        UNIT_ASSERT_C(write.IsSuccess(), write.GetIssues().ToString());
        env.Runtime.WaitFor("CDC write acknowledgement", [&] { return !ack.empty(); });

        THashSet<ui64> sealed;
        auto observer = env.Runtime.AddObserver<TEvDataShard::TEvDatabaseFreezeResult>([&](auto& ev) {
            const auto& result = ev->Get()->Record;
            if (result.GetStage() == NKikimrTxDataShard::TDatabaseFreezeState::DRAIN_CDC) {
                sealed.insert(result.GetTabletId());
            }
        });
        const auto freeze = env.Freeze(true);
        UNIT_ASSERT(!freeze.id().empty());
        UNIT_ASSERT(!freeze.ready());
        UNIT_ASSERT(!env.Get(freeze.id()).ready());

        // Observe installation on every shard rather than relying on elapsed time.
        env.Runtime.WaitFor("all shards sealed", [&] { return sealed.size() == shards.size(); });
        auto rejected = env.Write("UPSERT INTO `Table` (key, value) VALUES (2, 20);");
        UNIT_ASSERT(!rejected.IsSuccess());
        UNIT_ASSERT(!env.Get(freeze.id()).ready());
        auto read = env.Write("SELECT * FROM `Table`;");
        UNIT_ASSERT_C(read.IsSuccess(), read.GetIssues().ToString());

        ack.Stop().Unblock();
        const auto done = env.Wait(freeze.id());
        UNIT_ASSERT_VALUES_EQUAL_C(done.status(), Ydb::StatusIds::SUCCESS, done.DebugString());
        UNIT_ASSERT_VALUES_EQUAL(ReadTable(env.Server, shards, tableId), "key = 1, value = 10\n");
        auto reader = env.Reader();
        env.ExpectChange(reader, 1, 10);
        const auto thaw = env.Freeze(false);
        UNIT_ASSERT_VALUES_EQUAL(env.Wait(thaw.id()).status(), Ydb::StatusIds::SUCCESS);
        const auto after = env.Write("UPSERT INTO `Table` (key, value) VALUES (2, 20);");
        UNIT_ASSERT_C(after.IsSuccess(), after.GetIssues().ToString());
        env.ExpectChange(reader, 2, 20);
        env.Kikimr.RunCall([&] { return reader->Close(TDuration::Seconds(5)); });
        UNIT_ASSERT_VALUES_EQUAL(env.Get(freeze.id()).status(), Ydb::StatusIds::SUCCESS);
    }

    Y_UNIT_TEST(FreezeIsExclusiveIncludingFalse) {
        TEnv env;
        const auto* descriptor = Ydb::Cms::AlterDatabaseRequest::descriptor();
        for (const bool freeze : {false, true}) {
            for (int i = 0; i < descriptor->field_count(); ++i) {
                const auto* field = descriptor->field(i);
                if (field->number() == 1 || field->number() == 7 || field->number() == 8
                    || field->number() == 10 || field->number() == 15)
                {
                    continue;
                }
                Ydb::Cms::AlterDatabaseRequest request;
                request.set_freeze(freeze);
                const auto* reflection = request.GetReflection();
                UNIT_ASSERT_C(field->cpp_type() == google::protobuf::FieldDescriptor::CPPTYPE_MESSAGE, field->full_name());
                if (field->is_repeated()) {
                    reflection->AddMessage(&request, field);
                } else {
                    reflection->MutableMessage(&request, field);
                }
                const auto result = env.Alter(std::move(request));
                UNIT_ASSERT(result.ready());
                UNIT_ASSERT_VALUES_EQUAL_C(result.status(), Ydb::StatusIds::BAD_REQUEST, field->name());
            }
            Ydb::Cms::AlterDatabaseRequest request;
            request.set_freeze(freeze);
            request.GetReflection()->MutableUnknownFields(&request)->AddVarint(100, 1);
            UNIT_ASSERT_VALUES_EQUAL(env.Alter(std::move(request)).status(), Ydb::StatusIds::BAD_REQUEST);
        }
    }

    Y_UNIT_TEST(IdempotencyAndOperationHistorySurviveConsoleRestart) {
        TEnv env;
        env.CreateTable();
        const auto first = env.Freeze(true, Ydb::Operations::OperationParams::ASYNC, "freeze-key");
        UNIT_ASSERT_VALUES_EQUAL(env.Wait(first.id()).status(), Ydb::StatusIds::SUCCESS);
        const auto repeat = env.Freeze(true, Ydb::Operations::OperationParams::ASYNC, "freeze-key");
        UNIT_ASSERT_VALUES_EQUAL(repeat.id(), first.id());
        const auto conflict = env.Freeze(false, Ydb::Operations::OperationParams::ASYNC, "freeze-key");
        UNIT_ASSERT_VALUES_EQUAL(conflict.status(), Ydb::StatusIds::BAD_REQUEST);
        const auto thaw = env.Freeze(false);
        UNIT_ASSERT_VALUES_EQUAL(env.Wait(thaw.id()).status(), Ydb::StatusIds::SUCCESS);
        RebootTablet(env.Runtime, MakeConsoleID(), env.Sender);
        const auto old = env.Get(first.id());
        UNIT_ASSERT(old.ready());
        UNIT_ASSERT_VALUES_EQUAL(old.status(), Ydb::StatusIds::SUCCESS);
    }

    Y_UNIT_TEST(SyncWaitsForApplication) {
        TEnv env;
        const auto [shards, tableId] = env.CreateTable();
        const auto operation = env.Freeze(true, Ydb::Operations::OperationParams::SYNC);
        UNIT_ASSERT(operation.ready());
        UNIT_ASSERT(!operation.id().empty());
        UNIT_ASSERT_VALUES_EQUAL(operation.status(), Ydb::StatusIds::SUCCESS);
        UNIT_ASSERT(!env.Write("UPSERT INTO `Table` (key, value) VALUES (1, 10);").IsSuccess());
        UNIT_ASSERT_VALUES_EQUAL(ReadTable(env.Server, shards, tableId), "");
        UNIT_ASSERT_VALUES_EQUAL(env.Freeze(false, Ydb::Operations::OperationParams::SYNC).status(), Ydb::StatusIds::SUCCESS);
        UNIT_ASSERT(env.Write("UPSERT INTO `Table` (key, value) VALUES (1, 10);").IsSuccess());
    }

    Y_UNIT_TEST(LastResolvedAcknowledgementIsRequired) {
        TEnv env;
        env.CreateTable(true, true);
        TBlockEvents<NPQ::TEvPartitionWriter::TEvWriteResponse> ack(env.Runtime);
        const auto operation = env.Freeze(true);
        env.Runtime.WaitFor("resolved writes to multiple topic partitions", [&] { return ack.size() >= 2; });
        UNIT_ASSERT(!env.Get(operation.id()).ready());
        ack.Unblock(ack.size() - 1);
        UNIT_ASSERT(!env.Get(operation.id()).ready());
        ack.Stop().Unblock();
        UNIT_ASSERT_VALUES_EQUAL(env.Wait(operation.id()).status(), Ydb::StatusIds::SUCCESS);
        const auto thaw = env.Freeze(false);
        UNIT_ASSERT_VALUES_EQUAL(env.Wait(thaw.id()).status(), Ydb::StatusIds::SUCCESS);
        // Force a distributed write after the final resolved version.
        const auto resumed = env.Write("UPSERT INTO `Table` (key, value) VALUES (1, 10), (2147483648, 20);");
        UNIT_ASSERT_C(resumed.IsSuccess(), resumed.GetIssues().ToString());
    }

    Y_UNIT_TEST(ThawCancelsPendingDrainAndIgnoresLateAcknowledgements) {
        TEnv env;
        env.CreateTable(true);
        TBlockEvents<NPQ::TEvPartitionWriter::TEvWriteResponse> ack(env.Runtime);
        UNIT_ASSERT(env.Write("UPSERT INTO `Table` (key, value) VALUES (1, 10);").IsSuccess());
        env.Runtime.WaitFor("unconfirmed CDC write", [&] { return !ack.empty(); });
        const auto freeze = env.Freeze(true);
        UNIT_ASSERT(!env.Get(freeze.id()).ready());
        const auto thaw = env.Freeze(false);
        UNIT_ASSERT_VALUES_EQUAL(env.Wait(thaw.id()).status(), Ydb::StatusIds::SUCCESS);
        UNIT_ASSERT_VALUES_EQUAL(env.Get(freeze.id()).status(), Ydb::StatusIds::CANCELLED);
        ack.Stop().Unblock();
        UNIT_ASSERT(env.Write("UPSERT INTO `Table` (key, value) VALUES (2, 20);").IsSuccess());
        UNIT_ASSERT_VALUES_EQUAL(env.Get(freeze.id()).status(), Ydb::StatusIds::CANCELLED);
    }

    Y_UNIT_TEST(PendingOperationSurvivesConsoleSchemeShardAndDataShardRestarts) {
        TEnv env;
        const auto [shards, tableId] = env.CreateTable(true);
        TBlockEvents<NPQ::TEvPartitionWriter::TEvWriteResponse> ack(env.Runtime);
        UNIT_ASSERT(env.Write("UPSERT INTO `Table` (key, value) VALUES (1, 10);").IsSuccess());
        env.Runtime.WaitFor("unconfirmed CDC write", [&] { return !ack.empty(); });
        const auto freeze = env.Freeze(true, Ydb::Operations::OperationParams::ASYNC, "restart");
        UNIT_ASSERT(!env.Get(freeze.id()).ready());
        RebootTablet(env.Runtime, MakeConsoleID(), env.Sender);
        RebootTablet(env.Runtime, tableId.PathId.OwnerId, env.Sender);
        for (const auto shard : shards) {
            RebootTablet(env.Runtime, shard, env.Sender);
        }
        const auto retried = env.Freeze(true, Ydb::Operations::OperationParams::ASYNC, "restart");
        UNIT_ASSERT_VALUES_EQUAL(retried.id(), freeze.id());
        UNIT_ASSERT(!env.Get(freeze.id()).ready());
        ack.Stop().Unblock();
        UNIT_ASSERT_VALUES_EQUAL(env.Wait(freeze.id()).status(), Ydb::StatusIds::SUCCESS);
        UNIT_ASSERT(!env.Write("UPSERT INTO `Table` (key, value) VALUES (2, 20);").IsSuccess());
        auto reader = env.Reader();
        env.ExpectChange(reader, 1, 10);
        env.Kikimr.RunCall([&] { return reader->Close(TDuration::Seconds(5)); });
        const auto thaw = env.Freeze(false);
        UNIT_ASSERT_VALUES_EQUAL(env.Wait(thaw.id()).status(), Ydb::StatusIds::SUCCESS);
        UNIT_ASSERT(env.Write("UPSERT INTO `Table` (key, value) VALUES (2, 20);").IsSuccess());
    }

    Y_UNIT_TEST(DiscardIsNotATopicAcknowledgement) {
        TEnv env;
        const auto [shards, tableId] = env.CreateTable(true);
        TVector<ui64> orders;
        // key=1 belongs to the first of the two uniform Uint32 partitions.
        const auto source = shards.at(0);
        auto observer = env.Runtime.AddObserver<NChangeExchange::TEvChangeExchange::TEvEnqueueRecords>([&](auto& ev) {
            if (orders.empty() && !ev->Get()->Records.empty()) {
                for (const auto& record : ev->Get()->Records) {
                    orders.push_back(record.Order);
                }
            }
        });
        bool fenced = false;
        auto fenceObserver = env.Runtime.AddObserver<TEvDataShard::TEvDatabaseFreezeResult>([&](auto& ev) {
            const auto& result = ev->Get()->Record;
            fenced |= result.GetTabletId() == source && result.GetStage() == NKikimrTxDataShard::TDatabaseFreezeState::FENCE_WRITES
                && result.GetStatus() == Ydb::StatusIds::SUCCESS && result.GetReady();
        });
        TBlockEvents<NPQ::TEvPartitionWriter::TEvWriteResponse> ack(env.Runtime);
        UNIT_ASSERT(env.Write("UPSERT INTO `Table` (key, value) VALUES (1, 10);").IsSuccess());
        env.Runtime.WaitFor("unconfirmed CDC write", [&] { return !ack.empty() && !orders.empty(); });
        const auto freeze = env.Freeze(true);
        env.Runtime.WaitFor("write fence installed", [&] { return fenced; });
        auto discard = MakeHolder<NChangeExchange::TEvChangeExchange::TEvRemoveRecords>(std::move(orders));
        discard->DeliveryConfirmed = false;
        const auto sourceActor = ResolveTablet(env.Runtime, source);
        env.Runtime.Send(new IEventHandle(sourceActor, env.Sender, discard.Release()),
            sourceActor.NodeId() - env.Runtime.GetFirstNodeId());
        UNIT_ASSERT_VALUES_EQUAL(env.Wait(freeze.id()).status(), Ydb::StatusIds::PRECONDITION_FAILED);
        const auto retry = env.Freeze(true);
        UNIT_ASSERT_VALUES_EQUAL(env.Wait(retry.id()).status(), Ydb::StatusIds::PRECONDITION_FAILED);
        ack.Stop().Unblock();
        UNIT_ASSERT(!env.Write("UPSERT INTO `Table` (key, value) VALUES (2, 20);").IsSuccess());
        const auto thaw = env.Freeze(false);
        UNIT_ASSERT_VALUES_EQUAL(env.Wait(thaw.id()).status(), Ydb::StatusIds::SUCCESS);
        UNIT_ASSERT(env.Write("UPSERT INTO `Table` (key, value) VALUES (2, 20);").IsSuccess());
    }

    Y_UNIT_TEST(InitialScanFinalizationMustComplete) {
        TEnv env;
        env.CreateTable();
        UNIT_ASSERT(env.Write("UPSERT INTO `Table` (key, value) VALUES (1, 10);").IsSuccess());
        TBlockEvents<TEvDataShard::TEvCdcStreamScanResponse> scans(env.Runtime, [](const auto& ev) {
            return ev->Get()->Record.GetStatus() == NKikimrTxDataShard::TEvCdcStreamScanResponse::DONE;
        });
        env.AddStream(false, true);
        env.Runtime.WaitFor("initial scan completion", [&] { return !scans.empty(); });
        const auto freeze = env.Freeze(true);
        UNIT_ASSERT(!env.Get(freeze.id()).ready());
        scans.Stop().Unblock();
        UNIT_ASSERT_VALUES_EQUAL(env.Wait(freeze.id()).status(), Ydb::StatusIds::SUCCESS);
        auto reader = env.Reader();
        env.ExpectChange(reader, 1, 10);
        env.Kikimr.RunCall([&] { return reader->Close(TDuration::Seconds(5)); });
    }

    Y_UNIT_TEST(EmptyDatabaseAndLegacyAlter) {
        TEnv env;
        Ydb::Cms::AlterDatabaseRequest attributes;
        (*attributes.mutable_alter_attributes())["test"] = "value";
        const auto altered = env.Alter(std::move(attributes));
        UNIT_ASSERT(altered.ready());
        UNIT_ASSERT_VALUES_EQUAL(altered.status(), Ydb::StatusIds::SUCCESS);
        const auto freeze = env.Freeze(true);
        UNIT_ASSERT_VALUES_EQUAL(env.Wait(freeze.id()).status(), Ydb::StatusIds::SUCCESS);
        const auto thaw = env.Freeze(false);
        UNIT_ASSERT_VALUES_EQUAL(env.Wait(thaw.id()).status(), Ydb::StatusIds::SUCCESS);
    }

    Y_UNIT_TEST(SchemaChangesAreBlockedUntilThaw) {
        TEnv env;
        env.CreateTable();
        const auto freeze = env.Freeze(true);
        UNIT_ASSERT_VALUES_EQUAL(env.Wait(freeze.id()).status(), Ydb::StatusIds::SUCCESS);
        UNIT_ASSERT(!env.Scheme("CREATE TABLE Other (key Uint32, PRIMARY KEY (key));").IsSuccess());
        UNIT_ASSERT(!env.Scheme("DROP TABLE `Table`;").IsSuccess());
        const auto thaw = env.Freeze(false);
        UNIT_ASSERT_VALUES_EQUAL(env.Wait(thaw.id()).status(), Ydb::StatusIds::SUCCESS);
        UNIT_ASSERT(env.Scheme("CREATE TABLE Other (key Uint32, PRIMARY KEY (key));").IsSuccess());
    }

    Y_UNIT_TEST(FailedThawKeepsSchemaFencedUntilSuccessfulRetry) {
        TEnv env;
        env.CreateTable();
        const auto freeze = env.Freeze(true);
        UNIT_ASSERT_VALUES_EQUAL(env.Wait(freeze.id()).status(), Ydb::StatusIds::SUCCESS);
        bool failOnce = true;
        auto observer = env.Runtime.AddObserver<TEvDataShard::TEvDatabaseFreezeResult>([&](auto& ev) {
            auto& result = ev->Get()->Record;
            if (failOnce && result.GetStage() == NKikimrTxDataShard::TDatabaseFreezeState::OPEN
                && result.GetReady() && result.GetStatus() == Ydb::StatusIds::SUCCESS)
            {
                failOnce = false;
                result.SetStatus(Ydb::StatusIds::UNAVAILABLE);
                result.SetIssue("Injected thaw failure");
            }
        });
        const auto failed = env.Freeze(false);
        UNIT_ASSERT_VALUES_EQUAL(env.Wait(failed.id()).status(), Ydb::StatusIds::UNAVAILABLE);
        UNIT_ASSERT(!env.Scheme("CREATE TABLE Other (key Uint32, PRIMARY KEY (key));").IsSuccess());
        observer.Remove();
        const auto thaw = env.Freeze(false);
        UNIT_ASSERT_VALUES_EQUAL(env.Wait(thaw.id()).status(), Ydb::StatusIds::SUCCESS);
        UNIT_ASSERT(env.Scheme("CREATE TABLE Other (key Uint32, PRIMARY KEY (key));").IsSuccess());
        UNIT_ASSERT(env.Write("UPSERT INTO `Table` (key, value) VALUES (1, 10), (2147483648, 20);").IsSuccess());
    }

    Y_UNIT_TEST(UnsupportedColumnTableDoesNotFreezeRowTables) {
        TEnv env;
        env.CreateTable();
        const auto column = env.Scheme("CREATE TABLE ColumnTable (key Uint64 NOT NULL, PRIMARY KEY (key)) WITH (STORE = COLUMN);");
        UNIT_ASSERT_C(column.IsSuccess(), column.GetIssues().ToString());
        const auto freeze = env.Freeze(true);
        UNIT_ASSERT_VALUES_EQUAL(env.Wait(freeze.id()).status(), Ydb::StatusIds::UNSUPPORTED);
        UNIT_ASSERT(env.Write("UPSERT INTO `Table` (key, value) VALUES (1, 10);").IsSuccess());
        const auto thaw = env.Freeze(false);
        UNIT_ASSERT_VALUES_EQUAL(env.Wait(thaw.id()).status(), Ydb::StatusIds::SUCCESS);
    }
}

} // namespace NKikimr::NGRpcService
