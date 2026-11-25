#include "local_rpc/local_rpc.h"
#include "rpc_calls.h"

#include <ydb/core/base/blobstorage.h>
#include <ydb/core/cms/console/console.h>
#include <ydb/core/change_exchange/change_exchange.h>
#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/core/persqueue/writer/writer.h>
#include <ydb/core/protos/schemeshard/operations.pb.h>
#include <ydb/core/testlib/actors/block_events.h>
#include <ydb/core/tx/datashard/ut_common/datashard_ut_common.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>
#include <ydb/core/tx/tx_proxy/proxy.h>
#include <ydb/public/api/grpc/ydb_cms_v1.grpc.pb.h>
#include <ydb/public/api/grpc/ydb_operation_v1.grpc.pb.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/client.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/cms/cms.h>
#include <library/cpp/json/json_reader.h>
#include <util/string/builder.h>

#include <grpcpp/create_channel.h>
#include <grpcpp/security/credentials.h>
#include <google/protobuf/descriptor.h>
#include <google/protobuf/unknown_field_set.h>

#include <memory>
#include <optional>
#include <string>

namespace NKikimr::NGRpcService {

    using namespace NDataShard;
    using namespace Tests;

    namespace {

        const TString Database = "/Root/FrozenDatabase";

        TString ProgressText(const Ydb::Operations::Operation& operation) {
            if (!operation.has_metadata()) {
                return {};
            }
            Ydb::Cms::DatabaseFreezeMetadata metadata;
            UNIT_ASSERT_C(operation.metadata().UnpackTo(&metadata), "Unexpected freeze metadata: " << operation.DebugString());
            if (!metadata.progress().empty()) {
                UNIT_ASSERT(metadata.has_update_time());
            }
            return metadata.progress();
        }

        NKqp::TKikimrSettings Settings(ui32 dynamicNodeCount = 1) {
            NKikimrPQ::TPQConfig pq;
            pq.SetEnabled(true);
            pq.SetEnableProtoSourceIdInfo(true);
            pq.SetTopicsAreFirstClassCitizen(true);
            pq.SetMaxReadCookies(10);
            pq.AddClientServiceType()->SetName("data-streams");
            pq.SetCheckACL(false);
            pq.SetRequireCredentialsInNewProtocol(false);
            pq.MutableQuotingConfig()->SetEnableQuoting(false);
            return NKqp::TKikimrSettings().SetUseRealThreads(false).SetPQConfig(pq).SetWithSampleTables(false).SetDynamicNodeCount(dynamicNodeCount).SetStoragePoolTypes(
                dynamicNodeCount > 1 ? TVector<TString>{"hdd1", "hdd2"} : TVector<TString>{"hdd1"});
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

            explicit TEnv(ui32 dynamicNodeCount = 1, bool externalSubdomain = true)
                : Kikimr(Settings(dynamicNodeCount))
                , Server(&Kikimr.GetTestServer())
                , Runtime(*Server->GetRuntime())
                , Tenants(Server)
                , Sender(Runtime.AllocateEdgeActor())
                , Channel(grpc::CreateChannel(Kikimr.GetEndpoint().c_str(), grpc::InsecureChannelCredentials()))
                , Cms(Ydb::Cms::V1::CmsService::NewStub(Channel))
                , Operations(Ydb::Operation::V1::OperationService::NewStub(Channel))
            {
                CreateDatabase(Database, "hdd1", externalSubdomain);
            }

            void CreateDatabase(const TString& path, const TString& storageKind = "hdd1", bool externalSubdomain = true) {
                using TCreate = TGrpcRequestOperationCall<Ydb::Cms::CreateDatabaseRequest, Ydb::Cms::CreateDatabaseResponse>;
                Ydb::Cms::CreateDatabaseRequest request;
                request.set_path(path);
                if (!externalSubdomain) {
                    request.mutable_options()->set_disable_external_subdomain(true);
                }
                auto* storage = request.mutable_resources()->add_storage_units();
                storage->set_unit_kind(storageKind);
                storage->set_count(1);
                auto response = Runtime.WaitFuture(NRpcService::DoLocalRpc<TCreate>(
                    std::move(request), "", "", Runtime.GetActorSystem(0)));
                UNIT_ASSERT_VALUES_EQUAL_C(response.operation().status(), Ydb::StatusIds::SUCCESS, response.DebugString());
                Tenants.Run(path, 1);
                Ydb::Cms::GetDatabaseStatusResult state;
                for (ui32 attempt = 0; attempt < 300; ++attempt) {
                    auto request = MakeHolder<NConsole::TEvConsole::TEvGetTenantStatusRequest>();
                    request->Record.MutableRequest()->set_path(path);
                    Runtime.SendToPipe(MakeConsoleID(), Sender, request.Release(), 0, GetPipeConfigWithRetries());
                    auto result = Runtime.GrabEdgeEvent<NConsole::TEvConsole::TEvGetTenantStatusResponse>(Sender, TDuration::Seconds(30));
                    UNIT_ASSERT_C(result, "No status of the database " << path);
                    UNIT_ASSERT(result->Get()->Record.GetResponse().operation().result().UnpackTo(&state));
                    if (state.state() == Ydb::Cms::GetDatabaseStatusResult::RUNNING) {
                        return;
                    }
                    Runtime.SimulateSleep(TDuration::MilliSeconds(100));
                }
                UNIT_FAIL("The database " << path << " is not running: " << state.DebugString());
            }

            Ydb::Cms::GetDatabaseStatusResult DatabaseStatus() {
                auto request = MakeHolder<NConsole::TEvConsole::TEvGetTenantStatusRequest>();
                request->Record.MutableRequest()->set_path(Database);
                Runtime.SendToPipe(MakeConsoleID(), Sender, request.Release(), 0, GetPipeConfigWithRetries());
                auto response = Runtime.GrabEdgeEventRethrow<NConsole::TEvConsole::TEvGetTenantStatusResponse>(Sender);
                const auto& operation = response->Get()->Record.GetResponse().operation();
                UNIT_ASSERT_VALUES_EQUAL_C(operation.status(), Ydb::StatusIds::SUCCESS, operation.DebugString());
                Ydb::Cms::GetDatabaseStatusResult status;
                UNIT_ASSERT(operation.result().UnpackTo(&status));
                return status;
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
                request.set_freeze_schema(freeze);
                request.set_freeze_datashards(freeze);
                request.set_idempotency_key(key);
                request.mutable_operation_params()->set_operation_mode(mode);
                return Alter(std::move(request));
            }

            Ydb::Operations::Operation Patch(std::optional<bool> schema, std::optional<bool> datashards,
                                             const TString& key = {})
            {
                Ydb::Cms::AlterDatabaseRequest request;
                if (schema.has_value()) {
                    request.set_freeze_schema(*schema);
                }
                if (datashards.has_value()) {
                    request.set_freeze_datashards(*datashards);
                }
                request.set_idempotency_key(key);
                request.mutable_operation_params()->set_operation_mode(Ydb::Operations::OperationParams::ASYNC);
                return Alter(std::move(request));
            }

            Ydb::Operations::Operation Remove() {
                return Kikimr.RunCall([&] {
                    grpc::ClientContext context;
                    context.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(30));
                    Ydb::Cms::RemoveDatabaseRequest request;
                    request.set_path(Database);
                    request.mutable_operation_params()->set_operation_mode(Ydb::Operations::OperationParams::ASYNC);
                    Ydb::Cms::RemoveDatabaseResponse response;
                    const auto status = Cms->RemoveDatabase(&context, request, &response);
                    UNIT_ASSERT_C(status.ok(), status.error_message());
                    return response.operation();
                });
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

            TString WaitProgress(const TString& id, const TString& expected) {
                Ydb::Operations::Operation operation;
                for (ui32 attempt = 0; attempt < 100; ++attempt) {
                    operation = Get(id);
                    UNIT_ASSERT_C(!operation.ready(), "Operation completed while waiting for progress: " << operation.DebugString());
                    const auto text = ProgressText(operation);
                    if (text.Contains(expected)) {
                        return text;
                    }
                    Runtime.SimulateSleep(TDuration::MilliSeconds(100));
                }
                UNIT_FAIL("No public freeze progress containing '" << expected << "': " << operation.DebugString());
                return {};
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
                auto client = Kikimr.GetQueryClient(NYdb::NQuery::TClientSettings().Database(Database).DiscoveryMode(NYdb::EDiscoveryMode::Off));
                return Kikimr.RunCall([&] {
                    return client.ExecuteQuery(sql, NYdb::NQuery::TTxControl::BeginTx().CommitTx(),
                                               NYdb::NQuery::TExecuteQuerySettings().ClientTimeout(TDuration::Seconds(30)).RetrySettings(NYdb::NRetry::TRetryOperationSettings().MaxRetries(0)))
                        .ExtractValueSync();
                });
            }

            NYdb::NQuery::TExecuteQueryResult Scheme(const TString& sql) {
                auto client = Kikimr.GetQueryClient(NYdb::NQuery::TClientSettings().Database(Database).DiscoveryMode(NYdb::EDiscoveryMode::Off));
                return Kikimr.RunCall([&] {
                    return client.ExecuteQuery(sql, NYdb::NQuery::TTxControl::NoTx(),
                                               NYdb::NQuery::TExecuteQuerySettings().ClientTimeout(TDuration::Seconds(30)).RetrySettings(NYdb::NRetry::TRetryOperationSettings().MaxRetries(0)))
                        .ExtractValueSync();
                });
            }

            std::shared_ptr<NYdb::NTopic::IReadSession> Reader() {
                NYdb::NTopic::TTopicClient client(Kikimr.GetDriver(), NYdb::NTopic::TTopicClientSettings().Database(Database).DiscoveryMode(NYdb::EDiscoveryMode::Off));
                return client.CreateReadSession(NYdb::NTopic::TReadSessionSettings().WithoutConsumer().ReadFromTimestamp(TInstant::Zero()).AppendTopics(NYdb::NTopic::TTopicReadSettings((Database + "/Table/Stream").c_str()).AppendPartitionIds(0).AppendPartitionIds(1)));
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
            UNIT_ASSERT(!request.has_freeze_schema());
            NYdb::NCms::TAlterDatabaseSettings().FreezeSchema(false).SerializeTo(request);
            UNIT_ASSERT(request.has_freeze_schema());
            UNIT_ASSERT(!request.freeze_schema());
            NYdb::NCms::TAlterDatabaseSettings().FreezeSchema(true).SerializeTo(request);
            UNIT_ASSERT(request.has_freeze_schema() && request.freeze_schema());
            const auto settings = NYdb::NCms::TAlterDatabaseSettings().FreezeSchema(false);
            const auto copy = settings;
            request.Clear();
            copy.SerializeTo(request);
            UNIT_ASSERT(request.has_freeze_schema());
            UNIT_ASSERT(!request.freeze_schema());
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
                    if (field->number() == 1 || field->number() == 7 || field->number() == 8 || field->number() == 10 || field->number() == 15 || field->number() == 16)
                    {
                        continue;
                    }
                    Ydb::Cms::AlterDatabaseRequest request;
                    request.set_freeze_schema(freeze);
                    request.set_freeze_datashards(freeze);
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
                request.set_freeze_schema(freeze);
                request.set_freeze_datashards(freeze);
                request.GetReflection()->MutableUnknownFields(&request)->AddVarint(100, 1);
                UNIT_ASSERT_VALUES_EQUAL(env.Alter(std::move(request)).status(), Ydb::StatusIds::BAD_REQUEST);
            }
        }

        Y_UNIT_TEST(InvalidDatabaseGenerationAndOperationParametersDoNotFreeze) {
            TEnv env;
            env.CreateTable();
            for (const bool freeze : {false, true}) {
                Ydb::Cms::AlterDatabaseRequest missing;
                missing.set_path("/Root/DoesNotExist");
                missing.set_freeze_schema(freeze);
                missing.set_freeze_datashards(freeze);
                UNIT_ASSERT_VALUES_EQUAL(env.Alter(std::move(missing)).status(), Ydb::StatusIds::NOT_FOUND);

                Ydb::Cms::AlterDatabaseRequest stale;
                stale.set_freeze_schema(freeze);
                stale.set_freeze_datashards(freeze);
                stale.set_generation(Max<ui64>());
                UNIT_ASSERT_VALUES_EQUAL(env.Alter(std::move(stale)).status(), Ydb::StatusIds::BAD_REQUEST);

                for (const auto mode : {Ydb::Operations::OperationParams::SYNC, Ydb::Operations::OperationParams::ASYNC}) {
                    for (const bool cancel : {false, true}) {
                        Ydb::Cms::AlterDatabaseRequest request;
                        request.set_freeze_schema(freeze);
                        request.set_freeze_datashards(freeze);
                        auto* params = request.mutable_operation_params();
                        params->set_operation_mode(mode);
                        if (cancel) {
                            params->mutable_cancel_after()->set_seconds(60);
                        } else {
                            params->mutable_forget_after()->set_seconds(60);
                        }
                        const auto rejected = env.Alter(std::move(request));
                        UNIT_ASSERT(rejected.ready());
                        UNIT_ASSERT_VALUES_EQUAL(rejected.status(), Ydb::StatusIds::UNSUPPORTED);
                    }
                }
            }
            UNIT_ASSERT(env.Write("UPSERT INTO `Table` (key, value) VALUES (1, 10);").IsSuccess());
        }

        Y_UNIT_TEST(PendingRequestIsDeduplicatedAndBlocksConflictingChanges) {
            TEnv env;
            env.CreateTable(true);
            TBlockEvents<NPQ::TEvPartitionWriter::TEvWriteRequest> delivery(env.Runtime);
            UNIT_ASSERT(env.Write("UPSERT INTO `Table` (key, value) VALUES (1, 10);").IsSuccess());
            env.Runtime.WaitFor("undelivered CDC write", [&] { return !delivery.empty(); });
            const auto freeze = env.Freeze(true);
            UNIT_ASSERT(!freeze.ready());
            UNIT_ASSERT_VALUES_EQUAL(env.Freeze(true).id(), freeze.id());

            Ydb::Cms::AlterDatabaseRequest attributes;
            (*attributes.mutable_alter_attributes())["blocked"] = "value";
            UNIT_ASSERT_VALUES_EQUAL(env.Alter(std::move(attributes)).status(), Ydb::StatusIds::UNAVAILABLE);
            auto remove = [&] {
                return env.Kikimr.RunCall([&] {
                    grpc::ClientContext context;
                    context.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(30));
                    Ydb::Cms::RemoveDatabaseRequest request;
                    request.set_path(Database);
                    request.mutable_operation_params()->set_operation_mode(Ydb::Operations::OperationParams::ASYNC);
                    Ydb::Cms::RemoveDatabaseResponse response;
                    const auto status = env.Cms->RemoveDatabase(&context, request, &response);
                    UNIT_ASSERT_C(status.ok(), status.error_message());
                    return response.operation();
                });
            };
            UNIT_ASSERT_VALUES_EQUAL(remove().status(), Ydb::StatusIds::UNAVAILABLE);
            delivery.Stop().Unblock();
            UNIT_ASSERT_VALUES_EQUAL(env.Wait(freeze.id()).status(), Ydb::StatusIds::SUCCESS);
            UNIT_ASSERT_VALUES_EQUAL(remove().status(), Ydb::StatusIds::PRECONDITION_FAILED);
            const auto thaw = env.Freeze(false);
            UNIT_ASSERT_VALUES_EQUAL(env.Wait(thaw.id()).status(), Ydb::StatusIds::SUCCESS);
            UNIT_ASSERT(env.Write("UPSERT INTO `Table` (key, value) VALUES (2, 20);").IsSuccess());
        }

        Y_UNIT_TEST(LegacyConsoleSuccessDoesNotAcknowledgeFreeze) {
            TEnv env;
            for (const bool freeze : {false, true}) {
                auto legacy = env.Runtime.AddObserver<NConsole::TEvConsole::TEvAlterTenantResponse>([](auto& ev) {
                    auto* operation = ev->Get()->Record.MutableResponse()->mutable_operation();
                    operation->set_ready(true);
                    operation->set_status(Ydb::StatusIds::SUCCESS);
                    operation->clear_id();
                });
                const auto result = env.Freeze(freeze);
                UNIT_ASSERT(result.ready());
                UNIT_ASSERT_VALUES_EQUAL(result.status(), Ydb::StatusIds::UNSUPPORTED);
            }
            const auto thaw = env.Freeze(false);
            UNIT_ASSERT_VALUES_EQUAL(env.Wait(thaw.id()).status(), Ydb::StatusIds::SUCCESS);
        }

        Y_UNIT_TEST(SyncTimeoutDoesNotCancelDurableOperation) {
            TEnv env;
            env.CreateTable(true);
            TBlockEvents<NPQ::TEvPartitionWriter::TEvWriteRequest> delivery(env.Runtime);
            UNIT_ASSERT(env.Write("UPSERT INTO `Table` (key, value) VALUES (1, 10);").IsSuccess());
            env.Runtime.WaitFor("undelivered CDC write", [&] { return !delivery.empty(); });
            Ydb::Cms::AlterDatabaseRequest request;
            request.set_freeze_schema(true);
            request.set_freeze_datashards(true);
            request.set_idempotency_key("timeout");
            auto* params = request.mutable_operation_params();
            params->set_operation_mode(Ydb::Operations::OperationParams::SYNC);
            params->mutable_operation_timeout()->set_seconds(1);
            const auto timedOut = env.Alter(std::move(request));
            UNIT_ASSERT_VALUES_EQUAL(timedOut.status(), Ydb::StatusIds::TIMEOUT);
            const auto operation = env.Freeze(true, Ydb::Operations::OperationParams::ASYNC, "timeout");
            UNIT_ASSERT(!operation.id().empty());
            UNIT_ASSERT(!operation.ready());
            delivery.Stop().Unblock();
            UNIT_ASSERT_VALUES_EQUAL(env.Wait(operation.id()).status(), Ydb::StatusIds::SUCCESS);
            const auto thaw = env.Freeze(false);
            UNIT_ASSERT_VALUES_EQUAL(env.Wait(thaw.id()).status(), Ydb::StatusIds::SUCCESS);
        }

        Y_UNIT_TEST(SdkOperationTimeoutDoesNotCancelDurableFreeze) {
            TEnv env;
            env.CreateTable(true);
            TBlockEvents<NPQ::TEvPartitionWriter::TEvWriteRequest> delivery(env.Runtime);
            UNIT_ASSERT(env.Write("UPSERT INTO `Table` (key, value) VALUES (1, 10);").IsSuccess());
            env.Runtime.WaitFor("undelivered CDC write", [&] { return !delivery.empty(); });

            // The SDK has no idempotency-key setting. Start a keyed operation
            // through the public API; the SDK must join this same pending freeze.
            const auto operation = env.Freeze(true, Ydb::Operations::OperationParams::ASYNC, "sdk-timeout");
            UNIT_ASSERT(!operation.id().empty());
            UNIT_ASSERT(!env.Get(operation.id()).ready());
            TString sdkOperationId;
            auto observer = env.Runtime.AddObserver<NConsole::TEvConsole::TEvAlterTenantResponse>([&](auto& ev) {
                const auto& response = ev->Get()->Record.GetResponse().operation();
                if (!response.ready()) {
                    sdkOperationId = response.id();
                }
            });

            NYdb::TDriver driver(NYdb::TDriverConfig().SetEndpoint(env.Kikimr.GetEndpoint()).SetDatabase("/Root"));
            NYdb::NCms::TCmsClient client(driver, NYdb::TCommonClientSettings().DiscoveryMode(NYdb::EDiscoveryMode::Off));
            const auto result = client.AlterDatabase(Database,
                NYdb::NCms::TAlterDatabaseSettings().FreezeSchema(true).FreezeDataShards(true).OperationTimeout(TDuration::Seconds(1)));
            // Do not set ClientTimeout or Deadline: neither may mask a missing
            // OperationTimeout. This wait only bounds the test itself.
            const auto deadline = TInstant::Now() + TDuration::Seconds(10);
            // Let gRPC threads run between bounded simulation steps. An
            // unpaced simulation can hit the runtime's scheduled-event limit
            // before the wall-clock guard expires.
            while (!result.Wait(TDuration::MilliSeconds(1)) && TInstant::Now() < deadline) {
                env.Runtime.SimulateSleep(TDuration::MilliSeconds(10));
            }
            const bool completed = result.HasValue() || result.HasException();
            if (!completed) {
                delivery.Stop().Unblock();
            }
            // Cancel any pending SDK poll before asserting, so a regression
            // cannot leave an unbounded request running during test teardown.
            env.Kikimr.RunCall([&] {
                driver.Stop(true);
                return true;
            });
            observer.Remove();
            UNIT_ASSERT_VALUES_EQUAL(sdkOperationId, operation.id());
            UNIT_ASSERT_C(completed, "SDK freeze ignored OperationTimeout(1s): future is still pending after 10s without ClientTimeout");
            const auto timedOut = result.GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(timedOut.GetStatus(), NYdb::EStatus::TIMEOUT, timedOut.GetIssues().ToString());

            // Timeout finishes only the SDK wait. The original operation and
            // its idempotency key must survive while CDC delivery is blocked.
            UNIT_ASSERT(!env.Get(operation.id()).ready());
            const auto pending = env.Freeze(true, Ydb::Operations::OperationParams::ASYNC, "sdk-timeout");
            UNIT_ASSERT_VALUES_EQUAL(pending.id(), operation.id());
            UNIT_ASSERT(!pending.ready());
            delivery.Stop().Unblock();
            UNIT_ASSERT_VALUES_EQUAL(env.Wait(operation.id()).status(), Ydb::StatusIds::SUCCESS);
            const auto repeated = env.Freeze(true, Ydb::Operations::OperationParams::ASYNC, "sdk-timeout");
            UNIT_ASSERT_VALUES_EQUAL(repeated.id(), operation.id());
            UNIT_ASSERT(repeated.ready());
            UNIT_ASSERT_VALUES_EQUAL(repeated.status(), Ydb::StatusIds::SUCCESS);

            const auto thaw = env.Freeze(false);
            UNIT_ASSERT_VALUES_EQUAL(env.Wait(thaw.id()).status(), Ydb::StatusIds::SUCCESS);
        }

        Y_UNIT_TEST(PendingMetadataReportsAcceptedSchemaWork) {
            TEnv env;
            env.CreateTable();
            TBlockEvents<TEvDataShard::TEvProposeTransaction> schema(env.Runtime, [](const auto& ev) {
                return ev->Get()->Record.GetTxKind() == NKikimrTxDataShard::TX_KIND_SCHEME;
            });
            const auto txId = AsyncAlterAddExtraColumn(env.Server, Database, "Table");
            env.Runtime.WaitFor("accepted schema operation awaiting DataShard", [&] { return !schema.empty(); });
            const auto freeze = env.Freeze(true);
            const auto progress = env.WaitProgress(freeze.id(), "previously accepted schema operations");
            UNIT_ASSERT_STRING_CONTAINS(progress, "previously accepted schema operations");
            UNIT_ASSERT(!env.Get(freeze.id()).ready());

            schema.Stop().Unblock();
            auto notification = MakeHolder<NSchemeShard::TEvSchemeShard::TEvNotifyTxCompletion>();
            notification->Record.SetTxId(txId);
            env.Runtime.SendToPipe(env.TableSchemeShard, env.Sender, notification.Release(), 0, GetPipeConfigWithRetries());
            env.Runtime.GrabEdgeEventRethrow<NSchemeShard::TEvSchemeShard::TEvNotifyTxCompletionResult>(env.Sender);
            UNIT_ASSERT_VALUES_EQUAL(env.Wait(freeze.id()).status(), Ydb::StatusIds::SUCCESS);
            const auto thaw = env.Freeze(false);
            UNIT_ASSERT_VALUES_EQUAL(env.Wait(thaw.id()).status(), Ydb::StatusIds::SUCCESS);
        }

        Y_UNIT_TEST(PendingMetadataReportsCdcWaitAndRecoversAfterConsoleRestart) {
            TEnv env;
            const auto [shards, tableId] = env.CreateTable(true);
            Y_UNUSED(tableId);
            TBlockEvents<NPQ::TEvPartitionWriter::TEvWriteRequest> delivery(env.Runtime);
            UNIT_ASSERT(env.Write("UPSERT INTO `Table` (key, value) VALUES (1, 10);").IsSuccess());
            env.Runtime.WaitFor("undelivered CDC write", [&] { return !delivery.empty(); });
            ui64 pendingShard = 0;
            THashSet<ui64> participants;
            auto issues = env.Runtime.AddObserver<TEvDataShard::TEvDatabaseFreezeResult>([&](auto& ev) {
                auto& response = ev->Get()->Record;
                participants.insert(response.GetTabletId());
                if (!response.GetReady() && response.GetStage() == NKikimrTxDataShard::TDatabaseFreezeState::DRAIN_CDC) {
                    pendingShard = response.GetTabletId();
                    response.SetIssue("Test topic acknowledgement is held");
                }
            });
            const auto freeze = env.Freeze(true);
            auto checkProgress = [&] {
                const auto progress = env.WaitProgress(freeze.id(), "Test topic acknowledgement is held");
                UNIT_ASSERT_STRING_CONTAINS(progress, "DRAIN_CDC");
                UNIT_ASSERT(pendingShard);
                UNIT_ASSERT_STRING_CONTAINS(progress, ToString(pendingShard));
                // Service tables also belong to the database. Every participant
                // has replied at FENCE_WRITES before the worker reaches DRAIN_CDC.
                UNIT_ASSERT_C(participants.size() >= shards.size(), progress);
                UNIT_ASSERT_STRING_CONTAINS(progress, TStringBuilder() << "of " << participants.size() << " DataShards");
                const auto pending = env.Get(freeze.id());
                UNIT_ASSERT(!pending.ready());
                Ydb::Cms::DatabaseFreezeMetadata metadata;
                UNIT_ASSERT(pending.metadata().UnpackTo(&metadata));
                UNIT_ASSERT_VALUES_EQUAL(metadata.phase(), "DRAIN_CDC");
                UNIT_ASSERT(metadata.desired_schema() && metadata.desired_datashards());
            };
            checkProgress();
            RebootTablet(env.Runtime, MakeConsoleID(), env.Sender);
            checkProgress();

            issues.Remove();
            delivery.Stop().Unblock();
            const auto completed = env.Wait(freeze.id());
            UNIT_ASSERT_VALUES_EQUAL(completed.status(), Ydb::StatusIds::SUCCESS);
            UNIT_ASSERT_VALUES_EQUAL(completed.issues_size(), 0);
            Ydb::Cms::DatabaseFreezeMetadata completedMetadata;
            UNIT_ASSERT(completed.metadata().UnpackTo(&completedMetadata));
            UNIT_ASSERT(completedMetadata.progress().empty());
            UNIT_ASSERT(!completedMetadata.has_update_time());
            const auto thaw = env.Freeze(false);
            UNIT_ASSERT_VALUES_EQUAL(env.Wait(thaw.id()).status(), Ydb::StatusIds::SUCCESS);
        }

        Y_UNIT_TEST(SupersededWorkerCannotOverwritePendingMetadata) {
            TEnv env;
            env.CreateTable();
            TActorId oldWorker;
            TActorId newWorker;
            auto requests = env.Runtime.AddObserver<NSchemeShard::TEvSchemeShard::TEvDatabaseFreeze>([&](auto& ev) {
                if (!oldWorker && ev->Get()->Record.GetFreezeDataShards()) {
                    oldWorker = ev->Sender;
                } else if (!ev->Get()->Record.GetFreezeDataShards()) {
                    newWorker = ev->Sender;
                }
            });
            TBlockEvents<TEvDataShard::TEvDatabaseFreezeResult> cdc(env.Runtime, [](const auto& ev) {
                return ev->Get()->Record.GetStage() == NKikimrTxDataShard::TDatabaseFreezeState::DRAIN_CDC;
            });
            const auto freeze = env.Freeze(true);
            env.WaitProgress(freeze.id(), "DRAIN_CDC");
            TBlockEvents<NSchemeShard::TEvSchemeShard::TEvDatabaseFreezeResult> progress(env.Runtime, [&](const auto& ev) {
                return oldWorker && ev->Sender == oldWorker && !ev->Get()->Record.GetReady()
                    && ev->Get()->Record.GetIssue().Contains("DRAIN_CDC");
            });
            env.Runtime.WaitFor("old Console worker progress", [&] { return !progress.empty(); }, TDuration::Seconds(10));
            TBlockEvents<TEvDataShard::TEvDatabaseFreezeResult> open(env.Runtime, [](const auto& ev) {
                return ev->Get()->Record.GetStage() == NKikimrTxDataShard::TDatabaseFreezeState::OPEN;
            });
            const auto thaw = env.Freeze(false);
            env.Runtime.WaitFor("pending thaw acknowledgement", [&] { return !open.empty(); });
            env.WaitProgress(thaw.id(), "OPEN");
            UNIT_ASSERT(newWorker);
            // Fresh progress must not hide an incorrectly accepted stale update.
            TBlockEvents<NSchemeShard::TEvSchemeShard::TEvDatabaseFreezeResult> freshProgress(env.Runtime, [&](const auto& ev) {
                return ev->Sender == newWorker && !ev->Get()->Record.GetReady();
            });

            // A stale sender must be rejected even if it claims the new epoch.
            auto stale = MakeHolder<NSchemeShard::TEvSchemeShard::TEvDatabaseFreezeResult>();
            stale->Record.CopyFrom(progress.front()->Get()->Record);
            stale->Record.SetEpoch(open.front()->Get()->Record.GetEpoch());
            stale->Record.SetIssue("Obsolete Console worker progress");
            env.Runtime.Send(new IEventHandle(progress.front()->GetRecipientRewrite(), oldWorker, stale.Release()));
            progress.Stop().Unblock();
            env.Runtime.SimulateSleep(TDuration::Seconds(1));
            const auto pending = env.Get(thaw.id());
            UNIT_ASSERT(!pending.ready());
            const auto text = ProgressText(pending);
            UNIT_ASSERT_STRING_CONTAINS(text, "OPEN");
            UNIT_ASSERT(!text.Contains("Obsolete"));
            UNIT_ASSERT(!text.Contains("DRAIN_CDC"));
            UNIT_ASSERT_VALUES_EQUAL(env.Get(freeze.id()).status(), Ydb::StatusIds::CANCELLED);

            open.Stop().Unblock();
            UNIT_ASSERT_VALUES_EQUAL(env.Wait(thaw.id()).status(), Ydb::StatusIds::SUCCESS);
            freshProgress.Stop().Unblock();
            cdc.Stop().Unblock();
            env.Runtime.SimulateSleep(TDuration::MilliSeconds(100));
            const auto completed = env.Get(thaw.id());
            UNIT_ASSERT(completed.ready());
            UNIT_ASSERT_VALUES_EQUAL(completed.status(), Ydb::StatusIds::SUCCESS);
            Ydb::Cms::DatabaseFreezeMetadata completedMetadata;
            UNIT_ASSERT(completed.metadata().UnpackTo(&completedMetadata));
            UNIT_ASSERT(completedMetadata.progress().empty());
            UNIT_ASSERT(!completedMetadata.has_update_time());
            UNIT_ASSERT_VALUES_EQUAL(env.Get(freeze.id()).status(), Ydb::StatusIds::CANCELLED);
        }

        Y_UNIT_TEST(HistoricalIdempotencyKeysSurviveConsoleRestart) {
            TEnv env;
            env.CreateTable();
            struct TRecorded {
                bool Freeze;
                TString Key;
                TString Id;
            };
            TVector<TRecorded> history;
            for (ui32 cycle = 0; cycle < 3; ++cycle) {
                for (const bool freeze : {true, false}) {
                    const TString key = TStringBuilder() << "history-" << cycle << "-" << freeze;
                    const auto operation = env.Freeze(freeze, Ydb::Operations::OperationParams::ASYNC, key);
                    UNIT_ASSERT_VALUES_EQUAL(env.Wait(operation.id()).status(), Ydb::StatusIds::SUCCESS);
                    history.push_back({freeze, key, operation.id()});
                }
            }
            RebootTablet(env.Runtime, MakeConsoleID(), env.Sender);
            for (const auto& item : history) {
                const auto repeated = env.Freeze(item.Freeze, Ydb::Operations::OperationParams::ASYNC, item.Key);
                UNIT_ASSERT_VALUES_EQUAL(repeated.id(), item.Id);
                UNIT_ASSERT(repeated.ready());
                UNIT_ASSERT_VALUES_EQUAL(repeated.status(), Ydb::StatusIds::SUCCESS);
                const auto conflict = env.Freeze(!item.Freeze, Ydb::Operations::OperationParams::ASYNC, item.Key);
                UNIT_ASSERT(conflict.ready());
                UNIT_ASSERT_VALUES_EQUAL(conflict.status(), Ydb::StatusIds::BAD_REQUEST);
            }
            // Replaying a historical freeze must not install a new fence.
            UNIT_ASSERT(env.Write("UPSERT INTO `Table` (key, value) VALUES (1, 10);").IsSuccess());
            const auto active = env.Freeze(true, Ydb::Operations::OperationParams::ASYNC, "active-freeze");
            UNIT_ASSERT_VALUES_EQUAL(env.Wait(active.id()).status(), Ydb::StatusIds::SUCCESS);
            const auto& oldThaw = history[1];
            UNIT_ASSERT_VALUES_EQUAL(env.Freeze(false, Ydb::Operations::OperationParams::ASYNC, oldThaw.Key).id(), oldThaw.Id);
            UNIT_ASSERT(!env.Write("UPSERT INTO `Table` (key, value) VALUES (2, 20);").IsSuccess());
            const auto thaw = env.Freeze(false, Ydb::Operations::OperationParams::ASYNC, "active-thaw");
            UNIT_ASSERT_VALUES_EQUAL(env.Wait(thaw.id()).status(), Ydb::StatusIds::SUCCESS);
        }

        Y_UNIT_TEST(CancelledFreezeIdempotencyKeySurvivesConsoleRestart) {
            TEnv env;
            env.CreateTable();
            TBlockEvents<TEvDataShard::TEvDatabaseFreezeResult> cdc(env.Runtime, [](const auto& ev) {
                return ev->Get()->Record.GetStage() == NKikimrTxDataShard::TDatabaseFreezeState::DRAIN_CDC;
            });
            const auto freeze = env.Freeze(true, Ydb::Operations::OperationParams::ASYNC, "cancelled-key");
            env.Runtime.WaitFor("pending CDC acknowledgement", [&] { return !cdc.empty(); });
            const auto rejected = env.Freeze(true, Ydb::Operations::OperationParams::ASYNC, "cancelling-thaw");
            UNIT_ASSERT(rejected.ready());
            UNIT_ASSERT_VALUES_EQUAL_C(rejected.status(), Ydb::StatusIds::UNAVAILABLE, rejected.DebugString());
            UNIT_ASSERT(rejected.id().empty());
            UNIT_ASSERT(!env.Get(freeze.id()).ready());
            // A rejected request must not reserve its key: it can be used for thaw.
            const auto thaw = env.Freeze(false, Ydb::Operations::OperationParams::ASYNC, "cancelling-thaw");
            UNIT_ASSERT_VALUES_EQUAL(env.Wait(thaw.id()).status(), Ydb::StatusIds::SUCCESS);
            cdc.Stop().Unblock();
            RebootTablet(env.Runtime, MakeConsoleID(), env.Sender);
            const auto repeated = env.Freeze(true, Ydb::Operations::OperationParams::ASYNC, "cancelled-key");
            UNIT_ASSERT_VALUES_EQUAL(repeated.id(), freeze.id());
            UNIT_ASSERT(repeated.ready());
            UNIT_ASSERT_VALUES_EQUAL(repeated.status(), Ydb::StatusIds::CANCELLED);
            UNIT_ASSERT_VALUES_EQUAL(env.Freeze(false, Ydb::Operations::OperationParams::ASYNC, "cancelled-key").status(), Ydb::StatusIds::BAD_REQUEST);
            const auto repeatedThaw = env.Freeze(false, Ydb::Operations::OperationParams::ASYNC, "cancelling-thaw");
            UNIT_ASSERT_VALUES_EQUAL(repeatedThaw.id(), thaw.id());
            UNIT_ASSERT(repeatedThaw.ready());
            UNIT_ASSERT_VALUES_EQUAL(repeatedThaw.status(), Ydb::StatusIds::SUCCESS);
            UNIT_ASSERT(env.Write("UPSERT INTO `Table` (key, value) VALUES (1, 10);").IsSuccess());
        }

        Y_UNIT_TEST(DifferentDatabasesHaveIndependentIdempotencyKeys) {
            TEnv env(2);
            const TString otherDatabase = "/Root/OtherFrozenDatabase";
            env.CreateDatabase(otherDatabase, "hdd2");
            auto freezeOther = [&](bool freeze, const TString& key) {
                Ydb::Cms::AlterDatabaseRequest request;
                request.set_path(otherDatabase);
                request.set_freeze_schema(freeze);
                request.set_freeze_datashards(freeze);
                request.set_idempotency_key(key);
                request.mutable_operation_params()->set_operation_mode(Ydb::Operations::OperationParams::ASYNC);
                return env.Alter(std::move(request));
            };
            const auto first = env.Freeze(true, Ydb::Operations::OperationParams::ASYNC, "shared-key");
            const auto second = freezeOther(true, "shared-key");
            UNIT_ASSERT(first.id() != second.id());
            UNIT_ASSERT_VALUES_EQUAL(env.Wait(first.id()).status(), Ydb::StatusIds::SUCCESS);
            UNIT_ASSERT_VALUES_EQUAL(env.Wait(second.id()).status(), Ydb::StatusIds::SUCCESS);
            RebootTablet(env.Runtime, MakeConsoleID(), env.Sender);
            UNIT_ASSERT_VALUES_EQUAL(env.Freeze(true, Ydb::Operations::OperationParams::ASYNC, "shared-key").id(), first.id());
            UNIT_ASSERT_VALUES_EQUAL(freezeOther(true, "shared-key").id(), second.id());
            UNIT_ASSERT_VALUES_EQUAL(env.Freeze(false, Ydb::Operations::OperationParams::ASYNC, "shared-key").status(), Ydb::StatusIds::BAD_REQUEST);
            UNIT_ASSERT_VALUES_EQUAL(freezeOther(false, "shared-key").status(), Ydb::StatusIds::BAD_REQUEST);
            const auto thawFirst = env.Freeze(false, Ydb::Operations::OperationParams::ASYNC, "shared-thaw-key");
            const auto thawSecond = freezeOther(false, "shared-thaw-key");
            UNIT_ASSERT(thawFirst.id() != thawSecond.id());
            UNIT_ASSERT_VALUES_EQUAL(env.Wait(thawFirst.id()).status(), Ydb::StatusIds::SUCCESS);
            UNIT_ASSERT_VALUES_EQUAL(env.Wait(thawSecond.id()).status(), Ydb::StatusIds::SUCCESS);
        }

        Y_UNIT_TEST(RecreatedDatabaseHasIndependentIdempotencyKeys) {
            TEnv env;
            const auto oldFreeze = env.Freeze(true, Ydb::Operations::OperationParams::ASYNC, "incarnation-key");
            UNIT_ASSERT_VALUES_EQUAL(env.Wait(oldFreeze.id()).status(), Ydb::StatusIds::SUCCESS);
            const auto oldThaw = env.Freeze(false, Ydb::Operations::OperationParams::ASYNC, "incarnation-thaw-key");
            UNIT_ASSERT_VALUES_EQUAL(env.Wait(oldThaw.id()).status(), Ydb::StatusIds::SUCCESS);
            const auto removal = env.Remove();
            UNIT_ASSERT(!removal.id().empty());
            UNIT_ASSERT_VALUES_EQUAL(env.Wait(removal.id()).status(), Ydb::StatusIds::SUCCESS);
            env.Tenants.Stop(Database);
            env.CreateDatabase(Database);
            const auto newFreeze = env.Freeze(true, Ydb::Operations::OperationParams::ASYNC, "incarnation-key");
            UNIT_ASSERT(!newFreeze.id().empty());
            UNIT_ASSERT(newFreeze.id() != oldFreeze.id());
            UNIT_ASSERT_VALUES_EQUAL(env.Wait(newFreeze.id()).status(), Ydb::StatusIds::SUCCESS);
            RebootTablet(env.Runtime, MakeConsoleID(), env.Sender);
            UNIT_ASSERT_VALUES_EQUAL(env.Freeze(true, Ydb::Operations::OperationParams::ASYNC, "incarnation-key").id(), newFreeze.id());
            UNIT_ASSERT_VALUES_EQUAL(env.Get(oldFreeze.id()).status(), Ydb::StatusIds::SUCCESS);
            const auto newThaw = env.Freeze(false, Ydb::Operations::OperationParams::ASYNC, "incarnation-thaw-key");
            UNIT_ASSERT(newThaw.id() != oldThaw.id());
            UNIT_ASSERT_VALUES_EQUAL(env.Wait(newThaw.id()).status(), Ydb::StatusIds::SUCCESS);
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

        Y_UNIT_TEST(FreezeAfterFailedRemovalSurvivesConsoleRestart) {
            TEnv env;
            env.CreateTable(true);
            {
                ui32 failedRemovals = 0;
                auto failure = env.Runtime.AddObserver<TEvTxUserProxy::TEvProposeTransaction>([&](auto& ev) {
                    const auto& transaction = ev->Get()->Record.GetTransaction().GetModifyScheme();
                    const auto type = transaction.GetOperationType();
                    if ((type != NKikimrSchemeOp::ESchemeOpForceDropSubDomain
                         && type != NKikimrSchemeOp::ESchemeOpForceDropExtSubDomain)
                        || transaction.GetWorkingDir() + "/" + transaction.GetDrop().GetName() != Database)
                    {
                        return;
                    }
                    // Fail before the subdomain is dropped, but let Console run
                    // its normal removal-failure transaction and return to RUNNING.
                    ++failedRemovals;
                    auto response = MakeHolder<TEvTxUserProxy::TEvProposeTransactionStatus>(NTxProxy::TResultStatus::ExecError);
                    response->Record.SetSchemeShardStatus(NKikimrScheme::StatusPreconditionFailed);
                    response->Record.SetSchemeShardReason("Injected subdomain removal failure");
                    env.Runtime.Send(new IEventHandle(ev->Sender, ev->Recipient, response.Release(), 0, ev->Cookie));
                    ev.Reset();
                });
                const auto removal = env.Remove();
                UNIT_ASSERT(!removal.id().empty());
                UNIT_ASSERT_VALUES_EQUAL(env.Wait(removal.id()).status(), Ydb::StatusIds::GENERIC_ERROR);
                UNIT_ASSERT_VALUES_EQUAL(failedRemovals, 1);
            }

            // Keep freeze pending across the reboot. Blocking the write itself
            // ensures that a topic cannot acknowledge it during recovery.
            TBlockEvents<NPQ::TEvPartitionWriter::TEvWriteRequest> delivery(env.Runtime);
            UNIT_ASSERT(env.Write("UPSERT INTO `Table` (key, value) VALUES (1, 10);").IsSuccess());
            env.Runtime.WaitFor("undelivered CDC write", [&] { return !delivery.empty(); });
            const auto freeze = env.Freeze(true, Ydb::Operations::OperationParams::ASYNC, "after-failed-removal");
            UNIT_ASSERT(!freeze.id().empty());
            UNIT_ASSERT(!env.Get(freeze.id()).ready());
            UNIT_ASSERT_VALUES_EQUAL(env.Freeze(true, Ydb::Operations::OperationParams::ASYNC, "after-failed-removal").id(), freeze.id());

            auto checkPendingGuards = [&] {
                Ydb::Cms::AlterDatabaseRequest attributes;
                (*attributes.mutable_alter_attributes())["blocked"] = "value";
                UNIT_ASSERT_VALUES_EQUAL(env.Alter(std::move(attributes)).status(), Ydb::StatusIds::UNAVAILABLE);
                UNIT_ASSERT_VALUES_EQUAL(env.Remove().status(), Ydb::StatusIds::UNAVAILABLE);
            };
            checkPendingGuards();

            // Removal failure resets TxId only in memory. After reboot the
            // persisted removal TxId must not change the database's identity.
            RebootTablet(env.Runtime, MakeConsoleID(), env.Sender);
            UNIT_ASSERT(!env.Get(freeze.id()).ready());
            const auto repeated = env.Freeze(true, Ydb::Operations::OperationParams::ASYNC, "after-failed-removal");
            UNIT_ASSERT_VALUES_EQUAL_C(repeated.id(), freeze.id(), "Console must recover the same freeze operation after a failed removal");
            UNIT_ASSERT(!repeated.ready());
            checkPendingGuards();

            delivery.Stop().Unblock();
            UNIT_ASSERT_VALUES_EQUAL(env.Wait(freeze.id()).status(), Ydb::StatusIds::SUCCESS);

            // A completed freeze must also remain associated with the tenant:
            // Console must reject removal until an explicit thaw succeeds.
            RebootTablet(env.Runtime, MakeConsoleID(), env.Sender);
            const auto completed = env.Freeze(true, Ydb::Operations::OperationParams::ASYNC, "after-failed-removal");
            UNIT_ASSERT_VALUES_EQUAL(completed.id(), freeze.id());
            UNIT_ASSERT(completed.ready());
            UNIT_ASSERT_VALUES_EQUAL(completed.status(), Ydb::StatusIds::SUCCESS);
            UNIT_ASSERT_VALUES_EQUAL(env.Remove().status(), Ydb::StatusIds::PRECONDITION_FAILED);
            UNIT_ASSERT(!env.Write("UPSERT INTO `Table` (key, value) VALUES (2, 20);").IsSuccess());

            const auto thaw = env.Freeze(false);
            UNIT_ASSERT_VALUES_EQUAL(env.Wait(thaw.id()).status(), Ydb::StatusIds::SUCCESS);
            UNIT_ASSERT(env.Write("UPSERT INTO `Table` (key, value) VALUES (2, 20);").IsSuccess());
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

        Y_UNIT_TEST(CancellationIsPublishedOnlyAfterCommit) {
            TEnv env;
            env.CreateTable(true);
            TBlockEvents<NPQ::TEvPartitionWriter::TEvWriteRequest> delivery(env.Runtime);
            UNIT_ASSERT(env.Write("UPSERT INTO `Table` (key, value) VALUES (1, 10);").IsSuccess());
            env.Runtime.WaitFor("undelivered CDC write", [&] { return !delivery.empty(); });
            const auto freeze = env.Freeze(true);
            UNIT_ASSERT(!env.Get(freeze.id()).ready());

            TBlockEvents<TEvBlobStorage::TEvPut> commits(env.Runtime, [](const auto& ev) {
                return ev->Get()->Id.TabletID() == MakeConsoleID() && ev->Get()->Id.Channel() == 0;
            });
            using TAlter = TGrpcRequestOperationCall<Ydb::Cms::AlterDatabaseRequest, Ydb::Cms::AlterDatabaseResponse>;
            Ydb::Cms::AlterDatabaseRequest request;
            request.set_path(Database);
            request.set_freeze_schema(false);
            request.set_freeze_datashards(false);
            request.mutable_operation_params()->set_operation_mode(Ydb::Operations::OperationParams::ASYNC);
            auto thawFuture = NRpcService::DoLocalRpc<TAlter>(std::move(request), "", "", env.Runtime.GetActorSystem(0));
            env.Runtime.WaitFor("Console cancellation awaiting commit", [&] { return !commits.empty(); });
            UNIT_ASSERT(!thawFuture.IsReady());
            UNIT_ASSERT(!env.Get(freeze.id()).ready());

            commits.Stop().Unblock();
            const auto thaw = env.Runtime.WaitFuture(thawFuture).operation();
            UNIT_ASSERT_VALUES_EQUAL(env.Wait(thaw.id()).status(), Ydb::StatusIds::SUCCESS);
            UNIT_ASSERT_VALUES_EQUAL(env.Get(freeze.id()).status(), Ydb::StatusIds::CANCELLED);
            delivery.Stop().Unblock();
            RebootTablet(env.Runtime, MakeConsoleID(), env.Sender);
            UNIT_ASSERT_VALUES_EQUAL(env.Get(freeze.id()).status(), Ydb::StatusIds::CANCELLED);
            UNIT_ASSERT(env.Write("UPSERT INTO `Table` (key, value) VALUES (2, 20);").IsSuccess());
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
            // Hold the write itself: after reboot a topic may confirm an already
            // persisted record through MaxSeqNo without replaying its old ACK.
            TBlockEvents<NPQ::TEvPartitionWriter::TEvWriteRequest> delivery(env.Runtime);
            TBlockEvents<NPQ::TEvPartitionWriter::TEvWriteResponse> ack(env.Runtime);
            UNIT_ASSERT(env.Write("UPSERT INTO `Table` (key, value) VALUES (1, 10);").IsSuccess());
            env.Runtime.WaitFor("undelivered CDC write", [&] { return !delivery.empty(); });
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
            delivery.Stop().Unblock();
            env.Runtime.WaitFor("replayed CDC write acknowledgement", [&] { return !ack.empty(); });
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
                if (failOnce && result.GetStage() == NKikimrTxDataShard::TDatabaseFreezeState::OPEN && result.GetReady() && result.GetStatus() == Ydb::StatusIds::SUCCESS)
                {
                    failOnce = false;
                    result.SetStatus(Ydb::StatusIds::UNAVAILABLE);
                    result.SetIssue("Injected thaw failure");
                }
            });
            const auto failed = env.Freeze(false);
            UNIT_ASSERT_VALUES_EQUAL(env.Wait(failed.id()).status(), Ydb::StatusIds::UNAVAILABLE);
            UNIT_ASSERT(!env.Scheme("CREATE TABLE Other (key Uint32, PRIMARY KEY (key));").IsSuccess());
            Ydb::Cms::AlterDatabaseRequest attributes;
            (*attributes.mutable_alter_attributes())["after-thaw"] = "value";
            const auto generation = env.DatabaseStatus().generation();
            for (const bool reboot : {false, true}) {
                if (reboot) {
                    RebootTablet(env.Runtime, MakeConsoleID(), env.Sender);
                }
                UNIT_ASSERT_VALUES_EQUAL(env.Alter(attributes).status(), Ydb::StatusIds::PRECONDITION_FAILED);
                const auto state = env.DatabaseStatus();
                UNIT_ASSERT_VALUES_EQUAL(state.generation(), generation);
                UNIT_ASSERT(!state.attributes().contains("after-thaw"));
            }
            observer.Remove();
            const auto thaw = env.Freeze(false);
            UNIT_ASSERT_VALUES_EQUAL(env.Wait(thaw.id()).status(), Ydb::StatusIds::SUCCESS);
            UNIT_ASSERT(env.Scheme("CREATE TABLE Other (key Uint32, PRIMARY KEY (key));").IsSuccess());
            UNIT_ASSERT(env.Write("UPSERT INTO `Table` (key, value) VALUES (1, 10), (2147483648, 20);").IsSuccess());
            UNIT_ASSERT_VALUES_EQUAL(env.Alter(attributes).status(), Ydb::StatusIds::SUCCESS);
            UNIT_ASSERT_VALUES_EQUAL(env.DatabaseStatus().attributes().at("after-thaw"), "value");
        }

        Y_UNIT_TEST(UnsupportedColumnTableDoesNotFreezeRowTables) {
            TEnv env;
            env.CreateTable();
            const auto column = env.Scheme("CREATE TABLE ColumnTable (key Uint64 NOT NULL, PRIMARY KEY (key)) WITH (STORE = COLUMN);");
            UNIT_ASSERT_C(column.IsSuccess(), column.GetIssues().ToString());
            const auto freeze = env.Freeze(true);
            UNIT_ASSERT_VALUES_EQUAL(env.Wait(freeze.id()).status(), Ydb::StatusIds::UNSUPPORTED);
            UNIT_ASSERT(env.Write("UPSERT INTO `Table` (key, value) VALUES (1, 10);").IsSuccess());
            Ydb::Cms::AlterDatabaseRequest attributes;
            (*attributes.mutable_alter_attributes())["after-thaw"] = "value";
            UNIT_ASSERT_VALUES_EQUAL(env.Alter(attributes).status(), Ydb::StatusIds::PRECONDITION_FAILED);
            const auto thaw = env.Freeze(false);
            UNIT_ASSERT_VALUES_EQUAL(env.Wait(thaw.id()).status(), Ydb::StatusIds::SUCCESS);
            UNIT_ASSERT_VALUES_EQUAL(env.Alter(attributes).status(), Ydb::StatusIds::SUCCESS);
        }

        Y_UNIT_TEST(FrozenDatabaseRejectsQuotaChangesUntilThaw) {
            for (const bool externalSubdomain : {false, true}) {
                TEnv env(1, externalSubdomain);
                env.CreateTable();
                UNIT_ASSERT_VALUES_EQUAL(env.TableSchemeShard == Tests::SchemeRoot, !externalSubdomain);
                const auto freeze = env.Freeze(true);
                UNIT_ASSERT_VALUES_EQUAL(env.Wait(freeze.id()).status(), Ydb::StatusIds::SUCCESS);

                Ydb::Cms::AlterDatabaseRequest quotas;
                quotas.set_idempotency_key("quota-after-thaw");
                quotas.mutable_database_quotas()->set_data_size_hard_quota(1ULL << 40);
                quotas.mutable_database_quotas()->set_data_size_soft_quota(1ULL << 39);
                auto* bucket = quotas.mutable_schema_operation_quotas()->add_leaky_bucket_quotas();
                bucket->set_bucket_size(1000);
                bucket->set_bucket_seconds(1);

                const auto initial = env.DatabaseStatus();
                const auto initialScheme = env.Kikimr.GetTestClient().Describe(&env.Runtime, Database);
                UNIT_ASSERT_C(initialScheme.GetStatus() == NKikimrScheme::StatusSuccess, initialScheme.DebugString());
                const auto& initialDomain = initialScheme.GetPathDescription().GetDomainDescription();
                for (const bool reboot : {false, true}) {
                    if (reboot) {
                        RebootTablet(env.Runtime, MakeConsoleID(), env.Sender);
                    }
                    const auto rejected = env.Alter(quotas);
                    UNIT_ASSERT(rejected.ready());
                    UNIT_ASSERT_VALUES_EQUAL_C(rejected.status(), Ydb::StatusIds::PRECONDITION_FAILED, rejected.DebugString());
                    const auto unchanged = env.DatabaseStatus();
                    UNIT_ASSERT_VALUES_EQUAL(unchanged.generation(), initial.generation());
                    UNIT_ASSERT_VALUES_EQUAL(unchanged.database_quotas().SerializeAsString(), initial.database_quotas().SerializeAsString());
                    UNIT_ASSERT_VALUES_EQUAL(unchanged.schema_operation_quotas().SerializeAsString(), initial.schema_operation_quotas().SerializeAsString());
                    const auto scheme = env.Kikimr.GetTestClient().Describe(&env.Runtime, Database);
                    UNIT_ASSERT_C(scheme.GetStatus() == NKikimrScheme::StatusSuccess, scheme.DebugString());
                    const auto& domain = scheme.GetPathDescription().GetDomainDescription();
                    UNIT_ASSERT_VALUES_EQUAL(domain.GetDatabaseQuotas().SerializeAsString(),
                        initialDomain.GetDatabaseQuotas().SerializeAsString());
                    UNIT_ASSERT_VALUES_EQUAL(domain.GetDeclaredSchemeQuotas().SerializeAsString(),
                        initialDomain.GetDeclaredSchemeQuotas().SerializeAsString());
                }

                const auto thaw = env.Freeze(false);
                UNIT_ASSERT_VALUES_EQUAL(env.Wait(thaw.id()).status(), Ydb::StatusIds::SUCCESS);
                const auto generation = env.DatabaseStatus().generation();
                const auto accepted = env.Alter(quotas);
                UNIT_ASSERT_VALUES_EQUAL_C(accepted.status(), Ydb::StatusIds::SUCCESS, accepted.DebugString());
                // Ordinary Alter accepts settings before SchemeShard applies them.
                Ydb::Cms::GetDatabaseStatusResult applied;
                NKikimrScheme::TEvDescribeSchemeResult scheme;
                for (ui32 attempt = 0; attempt < 300; ++attempt) {
                    applied = env.DatabaseStatus();
                    scheme = env.Kikimr.GetTestClient().Describe(&env.Runtime, Database);
                    UNIT_ASSERT_C(scheme.GetStatus() == NKikimrScheme::StatusSuccess, scheme.DebugString());
                    const auto& domain = scheme.GetPathDescription().GetDomainDescription();
                    if (applied.state() == Ydb::Cms::GetDatabaseStatusResult::RUNNING
                        && domain.GetDatabaseQuotas().data_size_hard_quota() == (1ULL << 40)
                        && domain.GetDatabaseQuotas().data_size_soft_quota() == (1ULL << 39)
                        && domain.GetDeclaredSchemeQuotas().SchemeQuotasSize() == 1
                        && domain.GetDeclaredSchemeQuotas().GetSchemeQuotas(0).GetBucketSize() == 1000
                        && domain.GetDeclaredSchemeQuotas().GetSchemeQuotas(0).GetBucketSeconds() == 1)
                    {
                        break;
                    }
                    env.Runtime.SimulateSleep(TDuration::MilliSeconds(100));
                }
                UNIT_ASSERT_C(applied.state() == Ydb::Cms::GetDatabaseStatusResult::RUNNING, applied.DebugString());
                UNIT_ASSERT_VALUES_EQUAL(applied.generation(), generation + 1);
                UNIT_ASSERT_VALUES_EQUAL(applied.database_quotas().SerializeAsString(), quotas.database_quotas().SerializeAsString());
                UNIT_ASSERT_VALUES_EQUAL(applied.schema_operation_quotas().SerializeAsString(), quotas.schema_operation_quotas().SerializeAsString());
                const auto& domain = scheme.GetPathDescription().GetDomainDescription();
                UNIT_ASSERT_VALUES_EQUAL(domain.GetDatabaseQuotas().data_size_hard_quota(), 1ULL << 40);
                UNIT_ASSERT_VALUES_EQUAL(domain.GetDatabaseQuotas().data_size_soft_quota(), 1ULL << 39);
                UNIT_ASSERT_VALUES_EQUAL(domain.GetDeclaredSchemeQuotas().SchemeQuotasSize(), 1);
                UNIT_ASSERT_VALUES_EQUAL(domain.GetDeclaredSchemeQuotas().GetSchemeQuotas(0).GetBucketSize(), 1000);
                UNIT_ASSERT_VALUES_EQUAL(domain.GetDeclaredSchemeQuotas().GetSchemeQuotas(0).GetBucketSeconds(), 1);
            }
        }

        Y_UNIT_TEST(PendingFreezeRejectsNewIdempotencyKey) {
            TEnv env;
            env.CreateTable(true);
            TBlockEvents<NPQ::TEvPartitionWriter::TEvWriteRequest> delivery(env.Runtime);
            UNIT_ASSERT(env.Write("UPSERT INTO `Table` (key, value) VALUES (1, 10);").IsSuccess());
            env.Runtime.WaitFor("undelivered CDC write", [&] { return !delivery.empty(); });
            const auto freeze = env.Freeze(true, Ydb::Operations::OperationParams::ASYNC, "pending-freeze");
            UNIT_ASSERT(!freeze.id().empty());
            UNIT_ASSERT(!freeze.ready());
            env.WaitProgress(freeze.id(), "DRAIN_CDC");
            const auto generation = env.DatabaseStatus().generation();

            for (const bool reboot : {false, true}) {
                if (reboot) {
                    RebootTablet(env.Runtime, MakeConsoleID(), env.Sender);
                    env.WaitProgress(freeze.id(), "DRAIN_CDC");
                }
                const auto repeated = env.Freeze(true, Ydb::Operations::OperationParams::ASYNC, "pending-freeze");
                UNIT_ASSERT_VALUES_EQUAL(repeated.id(), freeze.id());
                UNIT_ASSERT(!repeated.ready());
                const auto unkeyed = env.Freeze(true);
                UNIT_ASSERT_VALUES_EQUAL(unkeyed.id(), freeze.id());
                UNIT_ASSERT(!unkeyed.ready());

                const auto rejected = env.Freeze(true, Ydb::Operations::OperationParams::ASYNC, "next-freeze");
                UNIT_ASSERT_C(rejected.ready(), rejected.DebugString());
                UNIT_ASSERT_VALUES_EQUAL_C(rejected.status(), Ydb::StatusIds::UNAVAILABLE, rejected.DebugString());
                UNIT_ASSERT(rejected.id().empty());
                UNIT_ASSERT_VALUES_EQUAL(rejected.issues_size(), 1);
                UNIT_ASSERT_STRING_CONTAINS(rejected.issues(0).message(), "already in progress");
                UNIT_ASSERT(!env.Get(freeze.id()).ready());
                UNIT_ASSERT_VALUES_EQUAL(env.DatabaseStatus().generation(), generation);
            }

            delivery.Stop().Unblock();
            UNIT_ASSERT_VALUES_EQUAL(env.Wait(freeze.id()).status(), Ydb::StatusIds::SUCCESS);
            // Reuse the rejected key after completion: a new epoch must be accepted.
            const auto next = env.Freeze(true, Ydb::Operations::OperationParams::ASYNC, "next-freeze");
            UNIT_ASSERT(!next.id().empty());
            UNIT_ASSERT(next.id() != freeze.id());
            UNIT_ASSERT_VALUES_EQUAL(env.Wait(next.id()).status(), Ydb::StatusIds::SUCCESS);
            UNIT_ASSERT_VALUES_EQUAL(env.DatabaseStatus().generation(), generation + 1);
            UNIT_ASSERT_VALUES_EQUAL(env.Freeze(true, Ydb::Operations::OperationParams::ASYNC, "next-freeze").id(), next.id());
            UNIT_ASSERT_VALUES_EQUAL(env.Freeze(true, Ydb::Operations::OperationParams::ASYNC, "pending-freeze").id(), freeze.id());
            UNIT_ASSERT_VALUES_EQUAL(env.Get(freeze.id()).status(), Ydb::StatusIds::SUCCESS);
            UNIT_ASSERT_VALUES_EQUAL(env.DatabaseStatus().generation(), generation + 1);
            const auto thaw = env.Freeze(false);
            UNIT_ASSERT_VALUES_EQUAL(env.Wait(thaw.id()).status(), Ydb::StatusIds::SUCCESS);
        }

        Y_UNIT_TEST(PendingThawRejectsNewIdempotencyKey) {
            TEnv env;
            env.CreateTable();
            const auto freeze = env.Freeze(true);
            UNIT_ASSERT_VALUES_EQUAL(env.Wait(freeze.id()).status(), Ydb::StatusIds::SUCCESS);
            TBlockEvents<TEvDataShard::TEvDatabaseFreeze> open(env.Runtime, [](const auto& ev) {
                return ev->Get()->Record.GetStage() == NKikimrTxDataShard::TDatabaseFreezeState::OPEN;
            });
            const auto thaw = env.Freeze(false, Ydb::Operations::OperationParams::ASYNC, "pending-thaw");
            UNIT_ASSERT(!thaw.id().empty());
            UNIT_ASSERT(!thaw.ready());
            env.Runtime.WaitFor("pending DataShard thaw", [&] { return !open.empty(); });
            const auto generation = env.DatabaseStatus().generation();

            for (const bool reboot : {false, true}) {
                if (reboot) {
                    RebootTablet(env.Runtime, MakeConsoleID(), env.Sender);
                }
                const auto repeated = env.Freeze(false, Ydb::Operations::OperationParams::ASYNC, "pending-thaw");
                UNIT_ASSERT_VALUES_EQUAL(repeated.id(), thaw.id());
                UNIT_ASSERT(!repeated.ready());
                const auto unkeyed = env.Freeze(false);
                UNIT_ASSERT_VALUES_EQUAL(unkeyed.id(), thaw.id());
                UNIT_ASSERT(!unkeyed.ready());

                const auto rejected = env.Freeze(false, Ydb::Operations::OperationParams::ASYNC, "next-thaw");
                UNIT_ASSERT_C(rejected.ready(), rejected.DebugString());
                UNIT_ASSERT_VALUES_EQUAL_C(rejected.status(), Ydb::StatusIds::UNAVAILABLE, rejected.DebugString());
                UNIT_ASSERT(rejected.id().empty());
                UNIT_ASSERT(!env.Get(thaw.id()).ready());
                UNIT_ASSERT_VALUES_EQUAL(env.DatabaseStatus().generation(), generation);
            }

            open.Stop().Unblock();
            UNIT_ASSERT_VALUES_EQUAL(env.Wait(thaw.id()).status(), Ydb::StatusIds::SUCCESS);
            UNIT_ASSERT(env.Write("UPSERT INTO `Table` (key, value) VALUES (1, 10);").IsSuccess());
            const auto next = env.Freeze(false, Ydb::Operations::OperationParams::ASYNC, "next-thaw");
            UNIT_ASSERT(!next.id().empty());
            UNIT_ASSERT(next.id() != thaw.id());
            UNIT_ASSERT_VALUES_EQUAL(env.Wait(next.id()).status(), Ydb::StatusIds::SUCCESS);
            UNIT_ASSERT_VALUES_EQUAL(env.DatabaseStatus().generation(), generation + 1);
            UNIT_ASSERT_VALUES_EQUAL(env.Freeze(false, Ydb::Operations::OperationParams::ASYNC, "next-thaw").id(), next.id());
            UNIT_ASSERT_VALUES_EQUAL(env.Freeze(false, Ydb::Operations::OperationParams::ASYNC, "pending-thaw").id(), thaw.id());
            UNIT_ASSERT_VALUES_EQUAL(env.Get(thaw.id()).status(), Ydb::StatusIds::SUCCESS);
            UNIT_ASSERT_VALUES_EQUAL(env.DatabaseStatus().generation(), generation + 1);
        }

        Y_UNIT_TEST(IndependentTargetsAndOriginalPatchSurviveLaterTransitions) {
            TEnv env;
            const auto initial = env.DatabaseStatus();
            UNIT_ASSERT(initial.has_freeze_status());
            UNIT_ASSERT(!initial.freeze_status().desired_schema());
            UNIT_ASSERT(!initial.freeze_status().desired_datashards());
            UNIT_ASSERT(initial.freeze_status().ready());
            UNIT_ASSERT_VALUES_EQUAL(initial.freeze_status().status(), Ydb::StatusIds::SUCCESS);
            UNIT_ASSERT(initial.freeze_status().operation_id().empty());
            env.CreateTable();
            const auto schema = env.Patch(true, std::nullopt, "schema-only");
            UNIT_ASSERT_VALUES_EQUAL(env.Wait(schema.id()).status(), Ydb::StatusIds::SUCCESS);
            UNIT_ASSERT(env.Write("UPSERT INTO `Table` (key, value) VALUES (1, 10);").IsSuccess());
            auto status = env.DatabaseStatus();
            UNIT_ASSERT(status.has_freeze_status());
            UNIT_ASSERT(status.freeze_status().desired_schema());
            UNIT_ASSERT(!status.freeze_status().desired_datashards());
            const auto data = env.Patch(std::nullopt, true, "data-only-patch");
            UNIT_ASSERT_VALUES_EQUAL(env.Wait(data.id()).status(), Ydb::StatusIds::SUCCESS);
            UNIT_ASSERT(!env.Write("UPSERT INTO `Table` (key, value) VALUES (2, 20);").IsSuccess());
            const auto releaseSchema = env.Patch(false, std::nullopt, "release-schema");
            UNIT_ASSERT_VALUES_EQUAL(env.Wait(releaseSchema.id()).status(), Ydb::StatusIds::SUCCESS);
            status = env.DatabaseStatus();
            UNIT_ASSERT(!status.freeze_status().desired_schema());
            UNIT_ASSERT(status.freeze_status().desired_datashards());
            UNIT_ASSERT_VALUES_EQUAL(status.freeze_status().operation_id(), releaseSchema.id());
            UNIT_ASSERT(!env.Write("UPSERT INTO `Table` (key, value) VALUES (3, 30);").IsSuccess());
            // Resolve the original key before applying today's absent-field defaults.
            UNIT_ASSERT_VALUES_EQUAL(env.Patch(true, std::nullopt, "schema-only").id(), schema.id());
            UNIT_ASSERT_VALUES_EQUAL(env.Patch(true, false, "schema-only").status(), Ydb::StatusIds::BAD_REQUEST);
            RebootTablet(env.Runtime, MakeConsoleID(), env.Sender);
            UNIT_ASSERT_VALUES_EQUAL(env.Patch(std::nullopt, true, "data-only-patch").id(), data.id());
            status = env.DatabaseStatus();
            UNIT_ASSERT(!status.freeze_status().desired_schema());
            UNIT_ASSERT(status.freeze_status().desired_datashards());
            const auto thaw = env.Patch(std::nullopt, false, "thaw-data");
            UNIT_ASSERT_VALUES_EQUAL(env.Wait(thaw.id()).status(), Ydb::StatusIds::SUCCESS);
            UNIT_ASSERT(env.Write("UPSERT INTO `Table` (key, value) VALUES (4, 40);").IsSuccess());
            status = env.DatabaseStatus();
            UNIT_ASSERT(!status.freeze_status().desired_schema());
            UNIT_ASSERT(!status.freeze_status().desired_datashards());
        }

        Y_UNIT_TEST(SchemaTargetChangesDuringCdcDrainKeepTheDataFence) {
            TEnv env;
            env.CreateTable(true);
            TBlockEvents<NPQ::TEvPartitionWriter::TEvWriteRequest> delivery(env.Runtime);
            UNIT_ASSERT(env.Write("UPSERT INTO `Table` (key, value) VALUES (1, 10);").IsSuccess());
            env.Runtime.WaitFor("undelivered CDC write", [&] { return !delivery.empty(); });
            ui32 opens = 0;
            auto commands = env.Runtime.AddObserver<TEvDataShard::TEvDatabaseFreeze>([&](auto& ev) {
                if (ev->Get()->Record.GetStage() == NKikimrTxDataShard::TDatabaseFreezeState::OPEN) {
                    ++opens;
                }
            });
            const auto data = env.Patch(std::nullopt, true, "data-drain");
            env.WaitProgress(data.id(), "DRAIN_CDC");
            // Absence resolves from the pending target, not the last successful mode.
            UNIT_ASSERT_VALUES_EQUAL(env.Patch(false, std::nullopt).id(), data.id());
            const auto full = env.Patch(true, std::nullopt, "schema-during-drain");
            UNIT_ASSERT_VALUES_EQUAL(env.Get(data.id()).status(), Ydb::StatusIds::CANCELLED);
            env.WaitProgress(full.id(), "DRAIN_CDC");
            const auto status = env.DatabaseStatus();
            UNIT_ASSERT(status.freeze_status().desired_schema());
            UNIT_ASSERT(status.freeze_status().desired_datashards());
            UNIT_ASSERT(!status.freeze_status().ready());
            const auto dataAgain = env.Patch(false, std::nullopt, "release-schema-during-drain");
            UNIT_ASSERT_VALUES_EQUAL(env.Get(full.id()).status(), Ydb::StatusIds::CANCELLED);
            env.WaitProgress(dataAgain.id(), "DRAIN_CDC");
            UNIT_ASSERT(!env.Write("UPSERT INTO `Table` (key, value) VALUES (2, 20);").IsSuccess());
            delivery.Stop().Unblock();
            UNIT_ASSERT_VALUES_EQUAL(env.Wait(dataAgain.id()).status(), Ydb::StatusIds::SUCCESS);
            UNIT_ASSERT_VALUES_EQUAL(opens, 0);
            UNIT_ASSERT(!env.Write("UPSERT INTO `Table` (key, value) VALUES (2, 20);").IsSuccess());
            const auto thaw = env.Freeze(false);
            UNIT_ASSERT_VALUES_EQUAL(env.Wait(thaw.id()).status(), Ydb::StatusIds::SUCCESS);
            UNIT_ASSERT(opens > 0);
            UNIT_ASSERT(env.Write("UPSERT INTO `Table` (key, value) VALUES (2, 20);").IsSuccess());
        }

        Y_UNIT_TEST(DataOnlyOperationSurvivesConsoleSchemeShardAndDataShardRestarts) {
            TEnv env;
            const auto [shards, tableId] = env.CreateTable(true);
            // Hold the write itself: after reboot a topic may confirm an already
            // persisted record through MaxSeqNo without replaying its old ACK.
            TBlockEvents<NPQ::TEvPartitionWriter::TEvWriteRequest> delivery(env.Runtime);
            TBlockEvents<NPQ::TEvPartitionWriter::TEvWriteResponse> ack(env.Runtime);
            UNIT_ASSERT(env.Write("UPSERT INTO `Table` (key, value) VALUES (1, 10);").IsSuccess());
            env.Runtime.WaitFor("undelivered CDC write", [&] { return !delivery.empty(); });
            const auto freeze = env.Patch(std::nullopt, true, "restart");
            UNIT_ASSERT(!env.Get(freeze.id()).ready());
            RebootTablet(env.Runtime, MakeConsoleID(), env.Sender);
            RebootTablet(env.Runtime, tableId.PathId.OwnerId, env.Sender);
            for (const auto shard : shards) {
                RebootTablet(env.Runtime, shard, env.Sender);
            }
            const auto retried = env.Patch(std::nullopt, true, "restart");
            UNIT_ASSERT_VALUES_EQUAL(retried.id(), freeze.id());
            UNIT_ASSERT(!env.Get(freeze.id()).ready());
            delivery.Stop().Unblock();
            env.Runtime.WaitFor("replayed CDC write acknowledgement", [&] { return !ack.empty(); });
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
    } // Y_UNIT_TEST_SUITE(CmsDatabaseFreeze)

} // namespace NKikimr::NGRpcService
