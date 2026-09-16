#include "ydb_common_ut.h"

#include <ydb/core/base/ticket_parser.h>
#include <ydb/core/grpc_services/local_rpc/local_rpc.h>
#include <ydb/core/grpc_services/service_logstore.h>
#include <ydb/core/protos/flat_scheme_op.pb.h>
#include <ydb/core/tx/tx_proxy/proxy.h>

#include <ydb/library/testlib/helpers.h>
#include <ydb/core/testlib/tablet_helpers.h>
#include <ydb/services/deprecated/persqueue_v0/grpc_pq_actor.h>
#include <ydb/services/keyvalue/grpc_service_v1.h>
#include <ydb/services/keyvalue/grpc_service_v2.h>
#include <ydb/public/api/grpc/ydb_discovery_v1.grpc.pb.h>
#include <ydb/public/api/grpc/ydb_keyvalue_v1.grpc.pb.h>
#include <ydb/public/api/grpc/ydb_keyvalue_v2.grpc.pb.h>
#include <ydb/public/api/grpc/ydb_scheme_v1.grpc.pb.h>
#include <ydb/public/api/grpc/ydb_table_v1.grpc.pb.h>
#include <ydb/public/api/grpc/ydb_topic_v1.grpc.pb.h>
#include <ydb/public/api/protos/ydb_topic.pb.h>
#include <ydb/public/api/grpc/draft/ydb_logstore_v1.grpc.pb.h>
#include <ydb/public/api/grpc/draft/ydb_clickhouse_internal_v1.grpc.pb.h>
#include <ydb/public/api/grpc/draft/ydb_object_storage_v1.grpc.pb.h>
#include <ydb/public/api/protos/ydb_cms.pb.h>

#include <grpcpp/client_context.h>
#include <grpcpp/create_channel.h>
#include <grpcpp/security/credentials.h>

#include <chrono>
#include <memory>
#include <set>
#include <string>
#include <tuple>
#include <type_traits>
#include <utility>

namespace NKikimr::NGRpcService {
    namespace {

        using TKv = Ydb::KeyValue::V1::KeyValueService::Stub;
        using TLog = Ydb::LogStore::V1::LogStoreService::Stub;
        using TScheme = Ydb::Scheme::V1::SchemeService::Stub;
        using TTable = Ydb::Table::V1::TableService::Stub;
        using TDiscovery = Ydb::Discovery::V1::DiscoveryService::Stub;

        NKikimrConfig::TAppConfig MakeConfig(bool aliases, bool identity = false) {
            NKikimrConfig::TAppConfig config;
            config.MutableFeatureFlags()->SetEnableColumnStore(true);
            if (aliases) {
                auto* rule = config.MutablePathRewriteConfig()->AddRules();
                rule->SetPattern(identity ? "^/Root/kfront(/|$)" : "^/kfront(/|$)");
                rule->SetReplacement("/Root/kfront\\1");
            }
            return config;
        }

        template <class TStub, class TRequest, class TResponse>
        TResponse Call(TStub& stub,
                       grpc::Status (TStub::*method)(grpc::ClientContext*, const TRequest&, TResponse*),
                       TRequest request, TStringBuf database = "/Root", TStringBuf token = "root@builtin")
        {
            grpc::ClientContext context;
            context.AddMetadata("x-ydb-database", std::string(database.data(), database.size()));
            context.AddMetadata("x-ydb-auth-ticket", std::string(token.data(), token.size()));
            context.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(30));
            if constexpr (requires { request.mutable_operation_params(); }) {
                request.mutable_operation_params()->set_operation_mode(Ydb::Operations::OperationParams::SYNC);
            }
            TResponse response;
            const auto status = (stub.*method)(&context, request, &response);
            UNIT_ASSERT_C(status.ok(), status.error_message());
            return response;
        }

        template <class TResponse>
        auto Status(const TResponse& response) {
            if constexpr (requires { response.operation(); }) {
                UNIT_ASSERT_C(response.operation().ready(), response.DebugString());
                return response.operation().status();
            } else {
                return response.status();
            }
        }

        template <class TResponse>
        void Success(const TResponse& response) {
            UNIT_ASSERT_VALUES_EQUAL_C(Status(response), Ydb::StatusIds::SUCCESS, response.DebugString());
        }

        template <class TResult, class TResponse>
        TResult Result(const TResponse& response) {
            Success(response);
            if constexpr (std::is_same_v<TResult, TResponse>) {
                return response;
            } else {
                TResult result;
                UNIT_ASSERT_C(response.operation().result().UnpackTo(&result), response.DebugString());
                return result;
            }
        }

        struct TFixture {
            NYdb::TKikimrWithGrpcAndRootSchema Server;
            std::shared_ptr<grpc::Channel> Channel;

            explicit TFixture(bool aliases = true, bool tenant = false, bool identity = false)
                : Server(MakeConfig(aliases, identity), {}, {}, false, nullptr, [tenant](Tests::TServerSettings& settings) {
                    if (tenant) {
                        // CMS creates the tenant's pool; do not precreate that pool in the root fixture.
                        settings.AddStoragePoolType("hdd");
                    }
                    settings.RegisterGrpcService<TKeyValueGRpcServiceV1>("keyvalue");
                    settings.RegisterGrpcService<TKeyValueGRpcServiceV2>("keyvalue");
                })
                , Channel(grpc::CreateChannel(TStringBuilder() << "localhost:" << Server.GetPort(),
                                              grpc::InsecureChannelCredentials()))
            {
                if (tenant) {
                    Ydb::Cms::CreateDatabaseRequest request;
                    request.set_path("/Root/kfront");
                    auto* storage = request.mutable_resources()->add_storage_units();
                    storage->set_unit_kind("hdd");
                    storage->set_count(1);
                    Server.Tenants_->CreateTenant(std::move(request));
                    for (const auto node : Server.Tenants_->List("/Root/kfront")) {
                        Server.GetServer().EnableGRpc(Server.GetPortManager().GetPort(), node, "/Root/kfront");
                    }
                } else {
                    auto stub = Ydb::Scheme::V1::SchemeService::NewStub(Channel);
                    Ydb::Scheme::MakeDirectoryRequest request;
                    request.set_path("/Root/kfront");
                    Success(Call(*stub, &TScheme::MakeDirectory, request));
                }
            }

            void CreateVolume(const TString& path) {
                auto stub = Ydb::KeyValue::V1::KeyValueService::NewStub(Channel);
                Ydb::KeyValue::CreateVolumeRequest request;
                request.set_path(path);
                request.set_partition_count(1);
                for (ui32 i = 0; i < 3; ++i) {
                    request.mutable_storage_config()->add_channel()->set_media("ssd");
                }
                Success(Call(*stub, &TKv::CreateVolume, request));
            }

            auto DescribeVolume(const TString& path, TStringBuf token = "root@builtin") {
                auto stub = Ydb::KeyValue::V1::KeyValueService::NewStub(Channel);
                Ydb::KeyValue::DescribeVolumeRequest request;
                request.set_path(path);
                return Call(*stub, &TKv::DescribeVolume, request, "/Root", token);
            }
        };

        template <class TStub>
        void CheckKeyValueOperations(TFixture& fixture, TStub& stub) {
            Ydb::KeyValue::ExecuteTransactionRequest write;
            write.set_path("/kfront/Volume");
            auto* command = write.add_commands()->mutable_write();
            command->set_key("/kfront/key");
            command->set_value("/kfront/value");
            command->set_storage_channel(1);
            Success(Call(stub, &TStub::ExecuteTransaction, write));

            for (const auto* path : {"/Root/kfront/Volume", "/kfront/Volume"}) {
                Ydb::KeyValue::ReadRequest read;
                read.set_path(path);
                read.set_key("/kfront/key");
                const auto result = Result<Ydb::KeyValue::ReadResult>(Call(stub, &TStub::Read, read));
                UNIT_ASSERT_VALUES_EQUAL(result.requested_key(), "/kfront/key");
                UNIT_ASSERT_VALUES_EQUAL(result.value(), "/kfront/value");
            }

            Ydb::KeyValue::ReadRangeRequest range;
            range.set_path("/kfront/Volume");
            range.mutable_range()->set_from_key_inclusive("/kfront/key");
            range.mutable_range()->set_to_key_inclusive("/kfront/key");
            const auto ranged = Result<Ydb::KeyValue::ReadRangeResult>(Call(stub, &TStub::ReadRange, range));
            UNIT_ASSERT_VALUES_EQUAL(ranged.pair_size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(ranged.pair(0).key(), "/kfront/key");
            UNIT_ASSERT_VALUES_EQUAL(ranged.pair(0).value(), "/kfront/value");

            Ydb::KeyValue::ListRangeRequest list;
            list.set_path("/kfront/Volume");
            const auto listed = Result<Ydb::KeyValue::ListRangeResult>(Call(stub, &TStub::ListRange, list));
            UNIT_ASSERT_VALUES_EQUAL(listed.key_size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(listed.key(0).key(), "/kfront/key");

            Ydb::KeyValue::AcquireLockRequest acquire;
            acquire.set_path("/kfront/Volume");
            const auto lock = Result<Ydb::KeyValue::AcquireLockResult>(Call(stub, &TStub::AcquireLock, acquire));
            UNIT_ASSERT(lock.lock_generation() > 0);
            Ydb::KeyValue::GetStorageChannelStatusRequest storage;
            storage.set_path("/kfront/Volume");
            storage.set_lock_generation(lock.lock_generation());
            storage.add_storage_channel(1);
            for (const auto* path : {"/Root/kfront/Volume", "/kfront/Volume"}) {
                storage.set_path(path);
                Success(Call(stub, &TStub::GetStorageChannelStatus, storage));
            }
            UNIT_ASSERT_VALUES_EQUAL(Result<Ydb::KeyValue::DescribeVolumeResult>(
                                         fixture.DescribeVolume("/kfront/Volume"))
                                         .path(), "/kfront/Volume");
        }

        auto EndpointIdentities(const Ydb::Discovery::ListEndpointsResult& result) {
            std::set<std::tuple<TString, ui32, ui32>> endpoints;
            for (const auto& endpoint : result.endpoints()) {
                endpoints.emplace(endpoint.address(), endpoint.port(), endpoint.node_id());
            }
            return endpoints;
        }

        void CheckPqV0RootHeaderAuthDatabase(const TString& header, ui32 mode) {
            struct THandler final: NGRpcProxy::ISessionHandler<NPersQueue::TReadResponse> {
                void Finish() override {
                }
                void Reply(NPersQueue::TReadResponse&&) override {
                }
                void ReadyForNextRead() override {
                }
                bool IsShuttingDown() const override {
                    return true;
                }
            };
            TTestBasicRuntime runtime;
            SetupTabletServices(runtime);
            auto& config = runtime.GetAppData().PQConfig;
            config.SetEnabled(true);
            config.SetTopicsAreFirstClassCitizen(true);
            config.SetDatabase("/Root");
            config.SetRoot("/Root/PQ");
            if (mode != 0) {
                // Unrelated, identity, and actual rewriting. A second rule must
                // neither override identity nor rematch an already-resolved DB.
                NKikimrConfig::TPathRewriteConfig rules;
                auto* rule = rules.AddRules();
                rule->SetPattern(mode == 1 ? "^/Never$" : "^/$");
                rule->SetReplacement(mode == 2 ? "/" : "/Root");
                auto* decoy = rules.AddRules();
                decoy->SetPattern(mode == 2 ? "^/$" : "^/Root$");
                decoy->SetReplacement("/Wrong");
                runtime.GetAppData().PathNormalizer = std::make_shared<NPathAliasing::TPathNormalizer>(rules);
            }
            const auto cache = runtime.AllocateEdgeActor();
            const auto client = runtime.AllocateEdgeActor();
            const auto ticketParser = runtime.AllocateEdgeActor();
            runtime.RegisterService(MakeTicketParserID(), ticketParser);
            NPersQueue::TTopicsListController topics(
                std::make_shared<NPersQueue::TTopicNamesConverterFactory>(config, "dc1"));
            auto handler = MakeIntrusive<THandler>();
            auto counters = MakeIntrusive<NMonitoring::TDynamicCounters>();
            const auto actor = runtime.Register(new NGRpcProxy::TReadSessionActor(
                handler, topics, 1, cache, cache, counters, Nothing()));
            NPersQueue::TReadRequest request;
            auto* init = request.MutableInit();
            init->AddTopics("/Root/Topic");
            init->SetClientId("consumer");
            init->SetProxyCookie(NGRpcProxy::MAGIC_COOKIE_VALUE);
            request.MutableCredentials()->SetOauthToken("ticket");
            // Queue behind Bootstrap, then observe the actor's real native edges.
            runtime.Send(new IEventHandle(actor, client,
                                          new NGRpcProxy::TEvPQProxy::TEvReadInit(request, "peer", header, "request")), 0, true);
            using TCache = NMsgBusProxy::NPqMetaCacheV2::TEvPqNewMetaCache;
            auto described = runtime.GrabEdgeEvent<TCache::TEvDescribeTopicsRequest>(cache, TDuration::Seconds(5));
            UNIT_ASSERT(described);
            UNIT_ASSERT_VALUES_EQUAL(described->Get()->Topics.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(described->Get()->Topics[0]->GetPrimaryPath(), "/Root/Topic");
            // Database/folder identifiers are optional on this authorization edge;
            // no tablet metadata or successful authentication is needed to observe it.
            runtime.Send(new IEventHandle(actor, cache, new TCache::TEvDescribeTopicsResponse(std::move(described->Get()->Topics), std::make_shared<NSchemeCache::TSchemeCacheNavigate>())), 0, true);
            auto authorize = runtime.GrabEdgeEvent<TEvTicketParser::TEvAuthorizeTicket>(ticketParser, TDuration::Seconds(5));
            UNIT_ASSERT(authorize);
            UNIT_ASSERT_VALUES_EQUAL(authorize->Get()->Ticket, "ticket");
            UNIT_ASSERT_VALUES_EQUAL(authorize->Get()->Database, mode == 3 ? TString("/Root") : TString());
        }

    } // namespace

    Y_UNIT_TEST_SUITE(YdbRawPathAliasing) {
        Y_UNIT_TEST_TWIN(PqV0RootHeaderWithoutRulesKeepsLegacyAuthDatabase, RepeatedSlashes) {
            CheckPqV0RootHeaderAuthDatabase(RepeatedSlashes ? "////" : "/", 0);
        }

        Y_UNIT_TEST_TWIN(PqV0RootHeaderWithUnrelatedRulesKeepsLegacyAuthDatabase, RepeatedSlashes) {
            CheckPqV0RootHeaderAuthDatabase(RepeatedSlashes ? "////" : "/", 1);
        }

        Y_UNIT_TEST_TWIN(PqV0RootHeaderIdentityKeepsLegacyAuthDatabase, RepeatedSlashes) {
            CheckPqV0RootHeaderAuthDatabase(RepeatedSlashes ? "////" : "/", 2);
        }

        Y_UNIT_TEST_TWIN(PqV0RootHeaderRewritesAuthDatabaseExactlyOnce, RepeatedSlashes) {
            CheckPqV0RootHeaderAuthDatabase(RepeatedSlashes ? "////" : "/", 3);
        }

        Y_UNIT_TEST(CreateLogTableResolvesEveryNativeTtlStorageOperandOnce) {
            using TRpc = TGrpcRequestOperationCall<Ydb::LogStore::CreateLogTableRequest,
                                                   Ydb::LogStore::CreateLogTableResponse>;
            using TRequestCtx = NRpcService::TLocalRpcCtx<TRpc,
                                                          std::function<void(const Ydb::LogStore::CreateLogTableResponse&)>>;
            struct TFacility final: IFacilityProvider {
                TTestBasicRuntime& Runtime;
                explicit TFacility(TTestBasicRuntime& runtime)
                    : Runtime(runtime)
                {
                }
                ui64 GetChannelBufferSize() const override {
                    return 0;
                }
                TActorId RegisterActor(IActor* actor) const override {
                    return Runtime.Register(actor);
                }
            };
            for (ui32 mode = 0; mode < 4; ++mode) {
                // Disabled, unrelated, identity, and actual rewriting.
                TTestBasicRuntime runtime;
                SetupTabletServices(runtime);
                NKikimrConfig::TPathRewriteConfig config;
                if (mode != 0) {
                    auto* rule = config.AddRules();
                    rule->SetPattern(mode == 1 ? "^/Never/" : "^/cold/");
                    rule->SetReplacement(mode == 2 ? "/cold/" : "/Root/storage/");
                    auto* decoy = config.AddRules();
                    decoy->SetPattern(mode == 2 ? "^/cold/" : "^/Root/storage/");
                    decoy->SetReplacement("/Root/Wrong/");
                }
                runtime.GetAppData().PathNormalizer = std::make_shared<NPathAliasing::TPathNormalizer>(config);
                const auto proxy = runtime.AllocateEdgeActor();
                runtime.RegisterService(MakeTxProxyID(), proxy);
                Ydb::LogStore::CreateLogTableRequest request;
                request.set_path("/Root/Store/Logs");
                request.set_schema_preset_name("default");
                request.set_shards_count(1);
                request.set_sharding_type(Ydb::LogStore::HASH_TYPE_MODULO_N);
                request.add_sharding_columns("timestamp");
                for (ui32 i = 0; i < 2; ++i) {
                    auto* tier = request.mutable_ttl_settings()->mutable_tiered_ttl()->add_tiers();
                    tier->mutable_date_type_column()->set_column_name("timestamp");
                    tier->mutable_date_type_column()->set_expire_after_seconds(360 * (i + 1));
                    tier->mutable_evict_to_external_storage()->set_storage(i == 0 ? "/cold/First" : "/cold/Second");
                }
                auto context = std::make_unique<TRequestCtx>(std::move(request),
                                                             [](const Ydb::LogStore::CreateLogTableResponse&) {}, "/Root", Nothing(), Nothing(), false);
                context->SetPathRewriteSettings(TPathRewriteSettings::UserInput());
                UNIT_ASSERT(context->InitializePathRewriteContext(runtime.GetAppData()).empty());
                TFacility facility(runtime);
                DoCreateLogTableRequest(std::move(context), facility);
                // Dispatches the handler's bootstrap before observing its native proposal.
                const auto proposed = runtime.GrabEdgeEvent<TEvTxUserProxy::TEvProposeTransaction>(proxy, TDuration::Seconds(5));
                UNIT_ASSERT(proposed);
                const auto& modify = proposed->Get()->Record.GetTransaction().GetModifyScheme();
                UNIT_ASSERT_VALUES_EQUAL(modify.GetWorkingDir(), "/Root/Store");
                UNIT_ASSERT(modify.HasCreateColumnTable());
                const auto& table = modify.GetCreateColumnTable();
                UNIT_ASSERT_VALUES_EQUAL(table.GetName(), "Logs");
                const auto& ttl = table.GetTtlSettings().GetEnabled();
                UNIT_ASSERT_VALUES_EQUAL(ttl.GetColumnName(), "timestamp");
                UNIT_ASSERT_VALUES_EQUAL(ttl.TiersSize(), 2);
                for (ui32 i = 0; i < 2; ++i) {
                    const TString expected = TString(mode == 3 ? "/Root/storage/" : "/cold/") + (i == 0 ? "First" : "Second");
                    UNIT_ASSERT_VALUES_EQUAL(ttl.GetTiers(i).GetEvictToExternalStorage().GetStorage(), expected);
                    UNIT_ASSERT_VALUES_EQUAL(ttl.GetTiers(i).GetApplyAfterSeconds(), 360 * (i + 1));
                }
            }
        }

        Y_UNIT_TEST(TopicUnchangedRulesPreservePartitionCorrelationName) {
            TString baseline;
            for (ui32 mode = 0; mode < 3; ++mode) {
                // Disabled, unrelated, then identity: none changes the resource.
                TFixture fixture(mode != 0, false, mode == 2);
                using TTopic = Ydb::Topic::V1::TopicService::Stub;
                auto topics = Ydb::Topic::V1::TopicService::NewStub(fixture.Channel);
                Ydb::Topic::CreateTopicRequest create;
                create.set_path("/Root/kfront/Topic");
                create.mutable_partitioning_settings()->set_min_active_partitions(1);
                create.add_consumers()->set_name("consumer");
                Success(Call(*topics, &TTopic::CreateTopic, create));

                for (const char* pathInput : {"/Root/kfront/Topic", "Rootkfront/Topic", "/Rootkfront/Topic"}) {
                    grpc::ClientContext context;
                    context.AddMetadata("x-ydb-database", "/Root");
                    context.AddMetadata("x-ydb-auth-ticket", "root@builtin");
                    context.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(30));
                    auto stream = topics->StreamRead(&context);
                    UNIT_ASSERT(stream);
                    Ydb::Topic::StreamReadMessage::FromClient request;
                    request.mutable_init_request()->set_consumer("consumer");
                    request.mutable_init_request()->add_topics_read_settings()->set_path(pathInput);
                    UNIT_ASSERT(stream->Write(request));
                    Ydb::Topic::StreamReadMessage::FromServer response;
                    TString path;
                    while (stream->Read(&response)) {
                        Success(response);
                        if (response.has_start_partition_session_request()) {
                            path = response.start_partition_session_request().partition_session().path();
                            break;
                        }
                    }
                    context.TryCancel();
                    while (stream->Read(&response)) {
                    }
                    stream->Finish();
                    UNIT_ASSERT(!path.empty());
                    if (mode == 0) {
                        baseline = path;
                        UNIT_ASSERT_VALUES_EQUAL(baseline, "kfront/Topic");
                    } else {
                        UNIT_ASSERT_VALUES_EQUAL(path, baseline);
                    }
                }
            }
        }

        Y_UNIT_TEST_TWIN(PqV0UnchangedRulesKeepConfiguredTopicResolution, IdentityRule) {
            struct THandler final: NGRpcProxy::ISessionHandler<NPersQueue::TWriteResponse> {
                void Finish() override {
                }
                void Reply(NPersQueue::TWriteResponse&&) override {
                }
                void ReadyForNextRead() override {
                }
                bool IsShuttingDown() const override {
                    return true;
                }
            };
            for (bool enabled : {false, true}) {
                for (const char* topic : {"Topic", "RootSibling/Topic", "/RootSibling/Topic"}) {
                    TTestBasicRuntime runtime;
                    SetupTabletServices(runtime);
                    auto& config = runtime.GetAppData().PQConfig;
                    config.SetEnabled(true);
                    config.SetTopicsAreFirstClassCitizen(true);
                    config.SetDatabase("/Root");
                    config.SetRoot("/Root/PQ");
                    if (enabled) {
                        NKikimrConfig::TPathRewriteConfig rules;
                        auto* rule = rules.AddRules();
                        rule->SetPattern(IdentityRule ? "^/Other(/|$)" : "^/Never(/|$)");
                        rule->SetReplacement(IdentityRule ? "/Other\\1" : "/Unused\\1");
                        if (IdentityRule) {
                            auto* resourceIdentity = rules.AddRules();
                            resourceIdentity->SetPattern("^/Root(/|$)");
                            resourceIdentity->SetReplacement("/Root\\1");
                            auto* decoy = rules.AddRules();
                            decoy->SetPattern("^/Root(/|$)");
                            decoy->SetReplacement("/Wrong\\1");
                        }
                        runtime.GetAppData().PathNormalizer = std::make_shared<NPathAliasing::TPathNormalizer>(rules);
                    }
                    const auto cache = runtime.AllocateEdgeActor();
                    const auto client = runtime.AllocateEdgeActor();
                    auto handler = MakeIntrusive<THandler>();
                    auto counters = MakeIntrusive<NMonitoring::TDynamicCounters>();
                    const auto actor = runtime.Register(new NGRpcProxy::TWriteSessionActor(
                        handler, 1, cache, counters, "dc1", Nothing()));
                    NPersQueue::TWriteRequest request;
                    auto* init = request.MutableInit();
                    init->SetTopic(topic);
                    init->SetSourceId("source");
                    init->SetProxyCookie(NGRpcProxy::MAGIC_COOKIE_VALUE);
                    // Queue behind the bootstrap event instead of delivering inline.
                    runtime.Send(new IEventHandle(actor, client, new NGRpcProxy::TEvPQProxy::TEvWriteInit(request, "peer", "/Other", "request")), 0, true);
                    using TDescribe = NMsgBusProxy::NPqMetaCacheV2::TEvPqNewMetaCache::TEvDescribeTopicsRequest;
                    auto described = runtime.GrabEdgeEvent<TDescribe>(cache, TDuration::Seconds(5));
                    UNIT_ASSERT(described);
                    UNIT_ASSERT_VALUES_EQUAL(described->Get()->Topics.size(), 1);
                    // Legacy PQv0 builds its converter from the configured database,
                    // before independently processing the fresh database header.
                    UNIT_ASSERT_VALUES_EQUAL(described->Get()->Topics[0]->GetPrimaryPath(),
                                             TString(topic) == "Topic" ? "/Root/Topic" : "/Root/Sibling/Topic");
                }
            }
        }

        Y_UNIT_TEST(ClickhouseDescribeScanAndEverySnapshotOperand) {
            TFixture fixture;
            using TClickhouse = Ydb::ClickhouseInternal::V1::ClickhouseInternalService::Stub;
            auto tables = Ydb::Table::V1::TableService::NewStub(fixture.Channel);
            auto clickhouse = Ydb::ClickhouseInternal::V1::ClickhouseInternalService::NewStub(fixture.Channel);
            const auto session = Result<Ydb::Table::CreateSessionResult>(Call(*tables,
                                                                              &TTable::CreateSession, Ydb::Table::CreateSessionRequest{}));
            for (const auto* name : {"First", "Second"}) {
                Ydb::Table::CreateTableRequest create;
                create.set_session_id(session.session_id());
                create.set_path(TStringBuilder() << "/Root/kfront/" << name);
                auto* key = create.add_columns();
                key->set_name("Key");
                key->mutable_type()->mutable_optional_type()->mutable_item()->set_type_id(Ydb::Type::UINT64);
                create.add_primary_key("Key");
                Success(Call(*tables, &TTable::CreateTable, create));
                Ydb::ClickhouseInternal::DescribeTableRequest describe;
                describe.set_path(TStringBuilder() << "/kfront/" << name);
                const auto description = Result<Ydb::ClickhouseInternal::DescribeTableResult>(
                    Call(*clickhouse, &TClickhouse::DescribeTable, describe));
                UNIT_ASSERT_VALUES_EQUAL(description.primary_key_size(), 1);
                UNIT_ASSERT_VALUES_EQUAL(description.primary_key(0), "Key");
            }
            Ydb::ClickhouseInternal::CreateSnapshotRequest create;
            create.add_path("/kfront/First");
            create.add_path("/kfront/Second");
            const auto snapshot = Result<Ydb::ClickhouseInternal::CreateSnapshotResult>(
                Call(*clickhouse, &TClickhouse::CreateSnapshot, create));
            UNIT_ASSERT(!snapshot.snapshot_id().empty());
            for (const auto* path : {"/kfront/First", "/kfront/Second"}) {
                Ydb::ClickhouseInternal::ScanRequest scan;
                scan.set_table(path);
                scan.add_columns("Key");
                scan.set_snapshot_id(snapshot.snapshot_id());
                scan.set_max_rows(10);
                scan.set_max_bytes(1024);
                const auto result = Result<Ydb::ClickhouseInternal::ScanResult>(Call(*clickhouse, &TClickhouse::Scan, scan));
                UNIT_ASSERT(result.eos());
            }
            Ydb::ClickhouseInternal::RefreshSnapshotRequest refresh;
            refresh.add_path("/kfront/First");
            refresh.add_path("/kfront/Second");
            refresh.set_snapshot_id(snapshot.snapshot_id());
            UNIT_ASSERT_VALUES_EQUAL(Result<Ydb::ClickhouseInternal::RefreshSnapshotResult>(
                                         Call(*clickhouse, &TClickhouse::RefreshSnapshot, refresh))
                                         .snapshot_id(), snapshot.snapshot_id());
            Ydb::ClickhouseInternal::DiscardSnapshotRequest discard;
            discard.add_path("/kfront/First");
            discard.add_path("/kfront/Second");
            discard.set_snapshot_id(snapshot.snapshot_id());
            Success(Call(*clickhouse, &TClickhouse::DiscardSnapshot, discard));
            Ydb::ClickhouseInternal::ScanRequest invalid;
            invalid.set_table("/kfront/First");
            invalid.add_columns("Key");
            invalid.set_snapshot_id("/kfront/not-a-snapshot-id");
            UNIT_ASSERT_VALUES_EQUAL(Status(Call(*clickhouse, &TClickhouse::Scan, invalid)), Ydb::StatusIds::BAD_REQUEST);
            Ydb::Table::DeleteSessionRequest close;
            close.set_session_id(session.session_id());
            Success(Call(*tables, &TTable::DeleteSession, close));
        }

        Y_UNIT_TEST(ObjectStorageRewritesTableButNotObjectKeysOrPrefixes) {
            TFixture fixture;
            using TObjectStorage = Ydb::ObjectStorage::V1::ObjectStorageService::Stub;
            auto tables = Ydb::Table::V1::TableService::NewStub(fixture.Channel);
            auto storage = Ydb::ObjectStorage::V1::ObjectStorageService::NewStub(fixture.Channel);
            const auto session = Result<Ydb::Table::CreateSessionResult>(Call(*tables,
                                                                              &TTable::CreateSession, Ydb::Table::CreateSessionRequest{}));
            Ydb::Table::CreateTableRequest create;
            create.set_session_id(session.session_id());
            create.set_path("/Root/kfront/Objects");
            for (const auto* name : {"Path", "Data"}) {
                auto* column = create.add_columns();
                column->set_name(name);
                column->mutable_type()->mutable_optional_type()->mutable_item()->set_type_id(Ydb::Type::UTF8);
            }
            create.add_primary_key("Path");
            Success(Call(*tables, &TTable::CreateTable, create));
            Ydb::Table::ExecuteDataQueryRequest upsert;
            upsert.set_session_id(session.session_id());
            upsert.mutable_query()->set_yql_text("--!syntax_v1\nUPSERT INTO `/Root/kfront/Objects` (Path, Data) VALUES "
                                                 "('/kfront/folder/one', '/kfront/payload'), ('/kfront/direct', '/kfront/payload');");
            upsert.mutable_tx_control()->mutable_begin_tx()->mutable_serializable_read_write();
            upsert.mutable_tx_control()->set_commit_tx(true);
            Success(Call(*tables, &TTable::ExecuteDataQuery, upsert));
            Ydb::ObjectStorage::ListingRequest list;
            list.set_table_name("/kfront/Objects");
            list.mutable_key_prefix()->mutable_type()->mutable_tuple_type();
            list.mutable_key_prefix()->mutable_value();
            list.mutable_start_after_key_suffix()->mutable_type()->mutable_tuple_type();
            list.mutable_start_after_key_suffix()->mutable_value();
            list.set_path_column_prefix("/kfront/");
            list.set_path_column_delimiter("/");
            list.set_max_keys(100);
            list.add_columns_to_return("Data");
            const auto aliased = Call(*storage, &TObjectStorage::List, list);
            Success(aliased);
            UNIT_ASSERT_VALUES_EQUAL(aliased.common_prefixes_size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(aliased.common_prefixes(0), "/kfront/folder/");
            UNIT_ASSERT_VALUES_EQUAL(aliased.contents().rows_size(), 1);
            list.set_table_name("/Root/kfront/Objects");
            const auto canonical = Call(*storage, &TObjectStorage::List, list);
            Success(canonical);
            UNIT_ASSERT_VALUES_EQUAL(aliased.SerializeAsString(), canonical.SerializeAsString());
            Ydb::Table::DeleteSessionRequest close;
            close.set_session_id(session.session_id());
            Success(Call(*tables, &TTable::DeleteSession, close));
        }

        Y_UNIT_TEST(VolumeCrudPreservesRequestedPathSpelling) {
            TFixture fixture;
            auto stub = Ydb::KeyValue::V1::KeyValueService::NewStub(fixture.Channel);
            fixture.CreateVolume("/Root/kfront/Volume");
            const auto described = Result<Ydb::KeyValue::DescribeVolumeResult>(fixture.DescribeVolume("/kfront/Volume"));
            UNIT_ASSERT_VALUES_EQUAL(described.path(), "/kfront/Volume");
            UNIT_ASSERT_VALUES_EQUAL(described.partition_count(), 1);

            Ydb::KeyValue::AlterVolumeRequest alter;
            alter.set_path("/kfront/Volume");
            alter.set_alter_partition_count(2);
            Success(Call(*stub, &TKv::AlterVolume, alter));
            UNIT_ASSERT_VALUES_EQUAL(Result<Ydb::KeyValue::DescribeVolumeResult>(
                                         fixture.DescribeVolume("/Root/kfront/Volume"))
                                         .partition_count(), 2);

            fixture.CreateVolume("/kfront/CreatedThroughAlias");
            Success(fixture.DescribeVolume("/Root/kfront/CreatedThroughAlias"));
            Ydb::KeyValue::DropVolumeRequest drop;
            drop.set_path("/kfront/Volume");
            Success(Call(*stub, &TKv::DropVolume, drop));
            UNIT_ASSERT(Status(fixture.DescribeVolume("/Root/kfront/Volume")) != Ydb::StatusIds::SUCCESS);
        }

        Y_UNIT_TEST_TWIN(VolumeDataPathsDoNotRewriteKeysOrValues, V2) {
            TFixture fixture;
            fixture.CreateVolume("/Root/kfront/Volume");
            if constexpr (V2) {
                auto stub = Ydb::KeyValue::V2::KeyValueService::NewStub(fixture.Channel);
                CheckKeyValueOperations(fixture, *stub);
            } else {
                auto stub = Ydb::KeyValue::V1::KeyValueService::NewStub(fixture.Channel);
                CheckKeyValueOperations(fixture, *stub);
            }
        }

        Y_UNIT_TEST(DisabledAliasesPreserveMissingPathBehavior) {
            TFixture fixture(false);
            fixture.CreateVolume("/Root/kfront/Volume");
            Success(fixture.DescribeVolume("/Root/kfront/Volume"));
            UNIT_ASSERT_VALUES_EQUAL(Status(fixture.DescribeVolume("/kfront/Volume")),
                                     Status(fixture.DescribeVolume("/unconfigured/Volume")));
            UNIT_ASSERT(Status(fixture.DescribeVolume("/kfront/Volume")) != Ydb::StatusIds::SUCCESS);
        }

        Y_UNIT_TEST(VolumeTargetAclPreservesCanonicalStatus) {
            TFixture fixture;
            fixture.CreateVolume("/Root/kfront/Volume");
            auto scheme = Ydb::Scheme::V1::SchemeService::NewStub(fixture.Channel);
            Ydb::Scheme::ModifyPermissionsRequest permissions;
            permissions.set_path("/Root");
            auto* connect = permissions.add_actions()->mutable_grant();
            connect->set_subject("reader@builtin");
            connect->add_permission_names("ydb.database.connect");
            Success(Call(*scheme, &TScheme::ModifyPermissions, permissions));
            fixture.Server.ResetSchemeCache("/Root");

            const auto canonical = fixture.DescribeVolume("/Root/kfront/Volume", "reader@builtin");
            const auto alias = fixture.DescribeVolume("/kfront/Volume", "reader@builtin");
            UNIT_ASSERT(Status(canonical) != Ydb::StatusIds::SUCCESS);
            UNIT_ASSERT_VALUES_EQUAL(Status(alias), Status(canonical));
            Success(fixture.DescribeVolume("/kfront/Volume"));
        }

        Y_UNIT_TEST(DiscoveryBodyAndHeaderAreIndependentPathInputs) {
            TFixture fixture(true, true);
            auto stub = Ydb::Discovery::V1::DiscoveryService::NewStub(fixture.Channel);
            Ydb::Discovery::ListEndpointsRequest request;
            request.set_database("/Root/kfront");
            const auto canonical = Result<Ydb::Discovery::ListEndpointsResult>(
                Call(*stub, &TDiscovery::ListEndpoints, request, "/Root/kfront"));
            const auto expected = EndpointIdentities(canonical);
            UNIT_ASSERT(!expected.empty());
            for (const auto* body : {"/Root/kfront", "/kfront"}) {
                for (const auto* header : {"/Root/kfront", "/kfront"}) {
                    request.set_database(body);
                    const auto result = Result<Ydb::Discovery::ListEndpointsResult>(
                        Call(*stub, &TDiscovery::ListEndpoints, request, header));
                    UNIT_ASSERT(EndpointIdentities(result) == expected);
                }
            }
        }

        Y_UNIT_TEST(LogStoreAndLogTableResourcePaths) {
            TFixture fixture;
            auto stub = Ydb::LogStore::V1::LogStoreService::NewStub(fixture.Channel);
            Ydb::LogStore::CreateLogStoreRequest store;
            store.set_path("/Root/kfront/Store");
            store.set_shards_count(1);
            auto* preset = store.add_schema_presets();
            preset->set_name("default");
            auto* column = preset->mutable_schema()->add_columns();
            column->set_name("timestamp");
            column->mutable_type()->set_type_id(Ydb::Type::TIMESTAMP);
            preset->mutable_schema()->add_primary_key("timestamp");
            Success(Call(*stub, &TLog::CreateLogStore, store));
            Ydb::LogStore::DescribeLogStoreRequest describeStore;
            describeStore.set_path("/kfront/Store");
            const auto storeInfo = Result<Ydb::LogStore::DescribeLogStoreResult>(Call(*stub, &TLog::DescribeLogStore, describeStore));
            UNIT_ASSERT_VALUES_EQUAL(storeInfo.self().name(), "Store");
            UNIT_ASSERT_VALUES_EQUAL(storeInfo.shards_count(), 1);

            Ydb::LogStore::CreateLogTableRequest table;
            table.set_path("/kfront/Store/Logs");
            table.set_schema_preset_name("default");
            table.set_shards_count(1);
            table.set_sharding_type(Ydb::LogStore::HASH_TYPE_MODULO_N);
            table.add_sharding_columns("timestamp");
            auto* ttl = table.mutable_ttl_settings()->mutable_date_type_column();
            ttl->set_column_name("timestamp");
            ttl->set_expire_after_seconds(3600);
            Success(Call(*stub, &TLog::CreateLogTable, table));
            Ydb::LogStore::DescribeLogTableRequest describeTable;
            describeTable.set_path("/Root/kfront/Store/Logs");
            const auto canonical = Result<Ydb::LogStore::DescribeLogTableResult>(Call(*stub, &TLog::DescribeLogTable, describeTable));
            describeTable.set_path("/kfront/Store/Logs");
            const auto aliased = Result<Ydb::LogStore::DescribeLogTableResult>(Call(*stub, &TLog::DescribeLogTable, describeTable));
            UNIT_ASSERT_VALUES_EQUAL(aliased.self().name(), canonical.self().name());
            UNIT_ASSERT_VALUES_EQUAL(aliased.schema_preset_name(), "default");

            Ydb::LogStore::AlterLogTableRequest alter;
            alter.set_path("/kfront/Store/Logs");
            auto* updatedTtl = alter.mutable_set_ttl_settings()->mutable_date_type_column();
            updatedTtl->set_column_name("timestamp");
            updatedTtl->set_expire_after_seconds(7200);
            Success(Call(*stub, &TLog::AlterLogTable, alter));
            describeTable.set_path("/Root/kfront/Store/Logs");
            UNIT_ASSERT_VALUES_EQUAL(Result<Ydb::LogStore::DescribeLogTableResult>(
                                         Call(*stub, &TLog::DescribeLogTable, describeTable))
                                         .ttl_settings()
                                         .date_type_column()
                                         .expire_after_seconds(), 7200);

            // AlterLogStore is unsupported today; aliases must preserve that contract.
            Ydb::LogStore::AlterLogStoreRequest alterStore;
            alterStore.set_path("/Root/kfront/Store");
            const auto canonicalAlter = Call(*stub, &TLog::AlterLogStore, alterStore);
            alterStore.set_path("/kfront/Store");
            UNIT_ASSERT_VALUES_EQUAL(Status(Call(*stub, &TLog::AlterLogStore, alterStore)), Status(canonicalAlter));
            UNIT_ASSERT_VALUES_EQUAL(Status(canonicalAlter), Ydb::StatusIds::UNSUPPORTED);

            Ydb::LogStore::DropLogTableRequest dropTable;
            dropTable.set_path("/kfront/Store/Logs");
            Success(Call(*stub, &TLog::DropLogTable, dropTable));
            UNIT_ASSERT(Status(Call(*stub, &TLog::DescribeLogTable, describeTable)) != Ydb::StatusIds::SUCCESS);
            Ydb::LogStore::DropLogStoreRequest dropStore;
            dropStore.set_path("/kfront/Store");
            Success(Call(*stub, &TLog::DropLogStore, dropStore));
            describeStore.set_path("/Root/kfront/Store");
            UNIT_ASSERT(Status(Call(*stub, &TLog::DescribeLogStore, describeStore)) != Ydb::StatusIds::SUCCESS);
        }

        Y_UNIT_TEST(CopyAndRenameNormalizeEveryOperand) {
            TFixture fixture;
            auto stub = Ydb::Table::V1::TableService::NewStub(fixture.Channel);
            const auto session = Result<Ydb::Table::CreateSessionResult>(Call(*stub,
                                                                              &TTable::CreateSession, Ydb::Table::CreateSessionRequest{}));
            for (const auto* name : {"First", "Second"}) {
                Ydb::Table::CreateTableRequest create;
                create.set_session_id(session.session_id());
                create.set_path(TStringBuilder() << "/Root/kfront/" << name);
                auto* key = create.add_columns();
                key->set_name("Key");
                key->mutable_type()->mutable_optional_type()->mutable_item()->set_type_id(Ydb::Type::UINT64);
                create.add_primary_key("Key");
                Success(Call(*stub, &TTable::CreateTable, create));
            }
            Ydb::Table::CopyTablesRequest copy;
            copy.set_session_id(session.session_id());
            for (const auto* name : {"First", "Second"}) {
                auto* item = copy.add_tables();
                item->set_source_path(TStringBuilder() << "/kfront/" << name);
                item->set_destination_path(TStringBuilder() << "/kfront/" << name << "Copy");
            }
            Success(Call(*stub, &TTable::CopyTables, copy));
            Ydb::Table::RenameTablesRequest rename;
            rename.set_session_id(session.session_id());
            for (const auto* name : {"First", "Second"}) {
                auto* item = rename.add_tables();
                item->set_source_path(TStringBuilder() << "/kfront/" << name << "Copy");
                item->set_destination_path(TStringBuilder() << "/kfront/" << name << "Moved");
            }
            Success(Call(*stub, &TTable::RenameTables, rename));
            for (const auto* name : {"First", "Second"}) {
                Ydb::Table::DescribeTableRequest describe;
                describe.set_session_id(session.session_id());
                describe.set_path(TStringBuilder() << "/Root/kfront/" << name << "Moved");
                Success(Call(*stub, &TTable::DescribeTable, describe));
                describe.set_path(TStringBuilder() << "/Root/kfront/" << name << "Copy");
                UNIT_ASSERT(Status(Call(*stub, &TTable::DescribeTable, describe)) != Ydb::StatusIds::SUCCESS);
                describe.set_path(TStringBuilder() << "/Root/kfront/" << name);
                Success(Call(*stub, &TTable::DescribeTable, describe));
            }
            Ydb::Table::DeleteSessionRequest close;
            close.set_session_id(session.session_id());
            Success(Call(*stub, &TTable::DeleteSession, close));
        }
    } // Y_UNIT_TEST_SUITE(YdbRawPathAliasing)

} // namespace NKikimr::NGRpcService
