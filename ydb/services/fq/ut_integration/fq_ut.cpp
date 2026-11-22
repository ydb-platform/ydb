#include "ut_utils.h"

#include <ydb/core/fq/libs/config/protos/issue_id.pb.h>
#include <ydb/core/fq/libs/control_plane_proxy/control_plane_proxy.h>
#include <ydb/core/fq/libs/control_plane_proxy/events/events.h>
#include <ydb/core/fq/libs/control_plane_storage/control_plane_storage.h>
#include <ydb/core/fq/libs/control_plane_storage/events/events.h>
#include <ydb/core/fq/libs/control_plane_storage/message_builders.h>
#include <ydb/core/fq/libs/db_id_async_resolver_impl/database_resolver.h>
#include <ydb/core/fq/libs/db_schema/db_schema.h>
#include <ydb/core/fq/libs/mock/yql_mock.h>
#include <ydb/core/fq/libs/private_client/private_client.h>

#include <ydb/public/lib/fq/fq.h>
#include <ydb/public/lib/fq/helpers.h>
#include <ydb/public/lib/fq/scope.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/driver/driver.h>
#include <ydb/services/fq/private_grpc.h>
#include <ydb/services/ydb/ydb_common_ut.h>

#include <google/protobuf/util/time_util.h>
#include <library/cpp/protobuf/util/pb_io.h>
#include <library/cpp/retry/retry.h>
#include <library/cpp/threading/future/future.h>
#include <util/generic/scope.h>
#include <util/string/split.h>
#include <util/system/hostname.h>
#include <util/system/mutex.h>
#include <yql/essentials/public/issue/yql_issue_message.h>

#include <algorithm>

using namespace NYdb;
using namespace FederatedQuery;
using namespace NYdb::NFq;

namespace {
const ui32 Retries = 20;

void PrintProtoIssues(const NProtoBuf::RepeatedPtrField<::Ydb::Issue::IssueMessage>& protoIssues) {
    if (protoIssues.empty()) {
        Cerr << "No Issues" << Endl;
        return;
    }
    NYql::TIssues issues;
    NYql::IssuesFromMessage(protoIssues, issues);
    Cerr << ">>> Issues: " << issues.ToString() << Endl;
}

TString CreateNewHistoryAndWaitFinish(const TString& folderId, NYdb::NFq::TClient& client, const TString& yqlText,
    const FederatedQuery::QueryMeta::ComputeStatus& expectedStatusResult)
{
    //CreateQuery
    TString queryId;
    {
        const auto result = DoWithRetryOnRetCode([&]() {
            auto request = ::NFq::TCreateQueryBuilder{}.SetText(yqlText).Build();
            auto result = client.CreateQuery(request, CreateFqSettings<TCreateQuerySettings>(folderId)).ExtractValueSync();

            if (result.GetStatus() != EStatus::SUCCESS) {
                return false;
            }

            queryId = result.GetResult().query_id();
            return true;
        }, TRetryOptions(Retries));
        UNIT_ASSERT_C(result, "failed to create the query");
    }
    // GetQueryStatus
    const auto request = ::NFq::TGetQueryStatusBuilder{}.SetQueryId(queryId).Build();
    const auto result = DoWithRetryOnRetCode([&]() {
        auto result = client.GetQueryStatus(request, CreateFqSettings<TGetQueryStatusSettings>(folderId)).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        return result.GetResult().status() == expectedStatusResult;
    }, TRetryOptions(Retries));
    UNIT_ASSERT_C(result, "the execution of the query did not end within the time limit");

    return queryId;
}

void Warmup(const TString& folderId, NYdb::NFq::TClient& client) {
    // wait for backend up
    CreateNewHistoryAndWaitFinish(folderId, client, "select 1", FederatedQuery::QueryMeta::COMPLETED);
}

void CheckGetResultData(
    NYdb::NFq::TClient& client, const TString& queryId, const TString& folderId, const ui64 rowsCount, const int colsSize, const int answer)
{
    const auto request = ::NFq::TGetResultDataBuilder{}.SetQueryId(queryId).Build();
    const auto result = client.GetResultData(request, CreateFqSettings<TGetResultDataSettings>(folderId)).ExtractValueSync();
    UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

    const auto& resultSet = result.GetResult().result_set();
    UNIT_ASSERT_VALUES_EQUAL(resultSet.rows().size(), rowsCount);
    UNIT_ASSERT_VALUES_EQUAL(resultSet.columns().size(), colsSize);
    if (!resultSet.rows().empty()) {
        const auto& item = resultSet.rows(0).items(0);
        TString str = item.DebugString();
        TVector<TString> arr;
        StringSplitter(str).Split(' ').SkipEmpty().AddTo(&arr);
        Y_ABORT_UNLESS(arr.size() == 2, "Incorrect numeric result");
        UNIT_ASSERT_VALUES_EQUAL(std::stoi(arr.back()), answer);
    }
}

class TLoseRenameCommitReply final: public NActors::TActor<TLoseRenameCommitReply> {
    using TStorage = ::NFq::TEvControlPlaneStorage;
    using TRename = FederatedQuery::Internal::BindingRename;

public:
    TLoseRenameCommitReply(const NActors::TActorId& storage, NThreading::TPromise<void> dropped)
        : TActor(&TLoseRenameCommitReply::StateWork)
        , Storage(storage)
        , Dropped(std::move(dropped))
    {
    }

private:
    STFUNC(StateWork) {
        if (ev->GetTypeRewrite() == TStorage::TEvModifyBindingRequest::EventType &&
            ev->Get<TStorage::TEvModifyBindingRequest>()->RenameAction == ::NFq::EBindingRenameAction::Commit && !Dropped.HasValue())
        {
            Y_ABORT_UNLESS(!Requester);
            Requester = ev->Sender;
            Send(Storage, ev->ReleaseBase().Release(), ev->Flags, ev->Cookie);
        } else if (ev->GetTypeRewrite() == TStorage::TEvModifyBindingResponse::EventType) {
            const auto& response = *ev->Get<TStorage::TEvModifyBindingResponse>();
            if (response.AuditDetails.After && response.AuditDetails.Rename && response.AuditDetails.Rename->state() == TRename::COMMITTED) {
                Dropped.SetValue();
                Send(Requester, new NActors::TEvents::TEvWakeup());
            } else {
                Send(ev->Forward(Requester));
            }
            Requester = {};
        } else {
            Send(ev->Forward(Storage));
        }
    }

    const NActors::TActorId Storage;
    NThreading::TPromise<void> Dropped;
    NActors::TActorId Requester;
};

}   // namespace

Y_UNIT_TEST_SUITE(FqPrivateSchema) {
    Y_UNIT_TEST(ExternalTableMutation) {
        NKikimrConfig::TAppConfig config;
        auto& security = *config.MutableDomainsConfig()->MutableSecurityConfig();
        security.SetEnforceUserTokenRequirement(true);
        security.AddAdministrationAllowedSIDs("root@builtin");
        security.AddAdministrationAllowedSIDs("other@builtin");
        security.AddDefaultAccess("+(ConnDB):regular@builtin");
        security.AddDefaultAccess("+(ConnDB):other@builtin");

        TKikimrWithGrpcAndRootSchema server(config, {}, {}, false, nullptr, [](auto& settings) {
            settings.template RegisterGrpcService<NKikimr::NGRpcService::TGRpcFqPrivateSchemaService>("yq_private_schema");
        });
        const TString endpoint = TStringBuilder() << "localhost:" << server.GetPort();
        const auto makeDriver = [&](const TString& token) {
            return TDriver(TDriverConfig().SetEndpoint(endpoint).SetDatabase("/Root").SetAuthToken(token));
        };
        const auto driver = makeDriver("root@builtin");
        const auto regularDriver = makeDriver("regular@builtin");
        const auto otherDriver = makeDriver("other@builtin");
        ::NFq::TPrivateClient client(driver);
        ::NFq::TPrivateClient regularClient(regularDriver);
        ::NFq::TPrivateClient otherClient(otherDriver);

        NYdb::NTable::TTableClient tableClient(driver);
        const auto sessionResult = tableClient.CreateSession().ExtractValueSync();
        UNIT_ASSERT_C(sessionResult.IsSuccess(), sessionResult.GetIssues().ToString());
        auto session = sessionResult.GetSession();
        const auto source = session
                                .ExecuteSchemeQuery(R"sql(
            CREATE EXTERNAL DATA SOURCE S3Source WITH (
                SOURCE_TYPE="ObjectStorage", LOCATION="http://localhost:12345/bucket/", AUTH_METHOD="NONE"
            );
        )sql")
                                .ExtractValueSync();
        UNIT_ASSERT_C(source.IsSuccess(), source.GetIssues().ToString());
        const auto grant = session.ExecuteSchemeQuery("GRANT DESCRIBE SCHEMA ON `/Root` TO `other@builtin`").ExtractValueSync();
        UNIT_ASSERT_C(grant.IsSuccess(), grant.GetIssues().ToString());

        const auto createTable = [](const TString& key) {
            Fq::Private::MutateExternalTableRequest request;
            request.set_relative_path("ExternalTable");
            auto& create = *request.mutable_create();
            create.set_key(key);
            create.set_relative_data_source_path("S3Source");
            create.set_location("/");
            (*create.mutable_attributes())["format"] = "json_each_row";
            auto& column = *create.add_columns();
            column.set_name("key");
            column.set_type("Uint64");
            return request;
        };
        UNIT_ASSERT_VALUES_EQUAL(regularClient.MutateExternalTable(createTable("first")).ExtractValueSync().GetStatus(), EStatus::UNAUTHORIZED);

        auto crossDatabase = createTable("first");
        crossDatabase.set_relative_path("OtherDatabase/ExternalTable");
        UNIT_ASSERT_VALUES_EQUAL(client.MutateExternalTable(std::move(crossDatabase)).ExtractValueSync().GetStatus(), EStatus::BAD_REQUEST);

        const auto created = client.MutateExternalTable(createTable("first")).ExtractValueSync();
        UNIT_ASSERT_C(created.IsSuccess(), created.GetIssues().ToString());
        UNIT_ASSERT(created.IsResultSet());
        const auto& identity = created.GetResult().created_table();
        UNIT_ASSERT(identity.owner_id());
        UNIT_ASSERT(identity.local_path_id());
        UNIT_ASSERT(identity.version());

        const auto describeTable = [] {
            Fq::Private::DescribeExternalTableRequest request;
            request.set_relative_path("ExternalTable");
            return request;
        };
        UNIT_ASSERT_VALUES_EQUAL(regularClient.DescribeExternalTable(describeTable()).ExtractValueSync().GetStatus(), EStatus::UNAUTHORIZED);
        UNIT_ASSERT_VALUES_EQUAL(
            otherClient.DescribeExternalTable(describeTable()).ExtractValueSync().GetStatus(), EStatus::PRECONDITION_FAILED);

        const auto described = client.DescribeExternalTable(describeTable()).ExtractValueSync();
        UNIT_ASSERT_C(described.IsSuccess(), described.GetIssues().ToString());
        UNIT_ASSERT(described.IsResultSet());
        UNIT_ASSERT_VALUES_EQUAL(described.GetResult().identity().SerializeAsString(), identity.SerializeAsString());
        UNIT_ASSERT_VALUES_EQUAL(described.GetResult().description().self().owner(), "root@builtin");
        UNIT_ASSERT_VALUES_EQUAL(described.GetResult().description().data_source_path(), "/Root/S3Source");

        const auto replay = client.MutateExternalTable(createTable("first")).ExtractValueSync();
        UNIT_ASSERT_C(replay.IsSuccess(), replay.GetIssues().ToString());
        UNIT_ASSERT(replay.IsResultSet());
        UNIT_ASSERT_VALUES_EQUAL(replay.GetResult().created_table().SerializeAsString(), identity.SerializeAsString());

        UNIT_ASSERT_VALUES_EQUAL(client.MutateExternalTable(createTable("conflict")).ExtractValueSync().GetStatus(), EStatus::SCHEME_ERROR);

        Fq::Private::MutateExternalTableRequest commit;
        commit.set_relative_path("ExternalTable");
        commit.mutable_commit()->set_key("first");
        const auto committed = client.MutateExternalTable(std::move(commit)).ExtractValueSync();
        UNIT_ASSERT_C(committed.IsSuccess(), committed.GetIssues().ToString());

        Fq::Private::MutateExternalTableRequest drop;
        drop.set_relative_path("ExternalTable");
        *drop.mutable_guarded_drop() = identity;
        drop.mutable_guarded_drop()->set_version(identity.version() + 1);
        UNIT_ASSERT_VALUES_EQUAL(client.MutateExternalTable(Fq::Private::MutateExternalTableRequest(drop)).ExtractValueSync().GetStatus(),
            EStatus::PRECONDITION_FAILED);

        *drop.mutable_guarded_drop() = identity;
        const auto dropped = client.MutateExternalTable(Fq::Private::MutateExternalTableRequest(drop)).ExtractValueSync();
        UNIT_ASSERT_C(dropped.IsSuccess(), dropped.GetIssues().ToString());
        UNIT_ASSERT_VALUES_EQUAL(client.MutateExternalTable(std::move(drop)).ExtractValueSync().GetStatus(), EStatus::NOT_FOUND);
        UNIT_ASSERT_VALUES_EQUAL(client.DescribeExternalTable(describeTable()).ExtractValueSync().GetStatus(), EStatus::NOT_FOUND);

        Fq::Private::MutateExternalTableRequest abort;
        abort.set_relative_path("ExternalTable");
        abort.mutable_abort()->set_key("aborted");
        const auto aborted = client.MutateExternalTable(std::move(abort)).ExtractValueSync();
        UNIT_ASSERT_C(aborted.IsSuccess(), aborted.GetIssues().ToString());
        UNIT_ASSERT_VALUES_EQUAL(
            client.MutateExternalTable(createTable("aborted")).ExtractValueSync().GetStatus(), EStatus::PRECONDITION_FAILED);
    }
    Y_UNIT_TEST(BindingRenameRetriesAfterLostStorageCommitReply) {
        NKikimrConfig::TAppConfig config;
        config.MutableFeatureFlags()->SetDisablePgCompatibility(false);
        auto& security = *config.MutableDomainsConfig()->MutableSecurityConfig();
        security.AddAdministrationAllowedSIDs("root@builtin");
        security.AddDefaultUserSIDs("root@builtin");
        TKikimrWithGrpcAndRootSchema server(config, {}, {}, true, nullptr, [](auto& settings) {
            settings.template RegisterGrpcService<NKikimr::NGRpcService::TGRpcFqPrivateSchemaService>("yq_private_schema");
        });
        const TString endpoint = TStringBuilder() << "localhost:" << server.GetPort();
        const TString user = "root@builtin";
        const TDriver driver(TDriverConfig().SetEndpoint(endpoint).SetDatabase("/Root").SetAuthToken(user));
        NYdb::NFq::TClient fq(driver);
        ::NFq::TPrivateClient compute(driver);
        const TScope scope("yandexcloud://rename_folder");
        Warmup(scope.ParseFolder(), fq);

        const auto createConnection = ::NFq::TCreateConnectionBuilder{}.SetName("s3_source").CreateObjectStorage("bucket", "").Build();
        const auto connection = fq.CreateConnection(createConnection, CreateFqSettings<TCreateConnectionSettings>(scope)).ExtractValueSync();
        UNIT_ASSERT_C(connection.IsSuccess(), connection.GetIssues().ToString());
        const auto& connectionId = connection.GetResult().connection_id();

        FederatedQuery::ObjectStorageBinding objectStorage;
        auto& subset = *objectStorage.add_subset();
        subset.set_path_pattern("/");
        subset.set_format("json_each_row");
        auto& column = *subset.mutable_schema()->add_column();
        column.set_name("key");
        column.mutable_type()->mutable_optional_type()->mutable_item()->set_type_id(Ydb::Type::UINT64);
        const auto createBinding =
            ::NFq::TCreateBindingBuilder{}.SetName("old_binding").SetConnectionId(connectionId).CreateObjectStorage(objectStorage).Build();
        const auto binding = fq.CreateBinding(createBinding, CreateFqSettings<TCreateBindingSettings>(scope)).ExtractValueSync();
        UNIT_ASSERT_C(binding.IsSuccess(), binding.GetIssues().ToString());
        const auto& bindingId = binding.GetResult().binding_id();

        NYdb::NTable::TTableClient tableClient(driver);
        const auto sessionResult = tableClient.CreateSession().ExtractValueSync();
        UNIT_ASSERT_C(sessionResult.IsSuccess(), sessionResult.GetIssues().ToString());
        auto session = sessionResult.GetSession();
        const auto source = session
                                .ExecuteSchemeQuery(R"sql(
            CREATE EXTERNAL DATA SOURCE s3_source WITH (
                SOURCE_TYPE="ObjectStorage", LOCATION="http://localhost:12345/bucket/", AUTH_METHOD="NONE"
            );
        )sql")
                                .ExtractValueSync();
        UNIT_ASSERT_C(source.IsSuccess(), source.GetIssues().ToString());
        const auto oldTable = session
                                  .ExecuteSchemeQuery(R"sql(
            CREATE EXTERNAL TABLE old_binding (key Uint64) WITH (
                DATA_SOURCE="s3_source", LOCATION="/", FORMAT="json_each_row"
            );
        )sql")
                                  .ExtractValueSync();
        UNIT_ASSERT_C(oldTable.IsSuccess(), oldTable.GetIssues().ToString());

        const auto describeBinding = [&] {
            auto result = fq.DescribeBinding(
                ::NFq::TDescribeBindingBuilder{}.SetBindingId(bindingId).Build(), CreateFqSettings<TDescribeBindingSettings>(scope))
                    .ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            return result.GetResult().binding();
        };
        const auto describeComputeTable = [&](const TString& name) {
            Fq::Private::DescribeExternalTableRequest request;
            request.set_relative_path(name);
            return compute.DescribeExternalTable(std::move(request)).ExtractValueSync();
        };
        const auto revision = describeBinding().meta().revision();
        const auto original = describeComputeTable("old_binding");
        UNIT_ASSERT_C(original.IsSuccess(), original.GetIssues().ToString());

        auto& runtime = *server.GetRuntime();
        const auto storageService = ::NFq::ControlPlaneStorageServiceActorId();
        const auto storage = runtime.GetLocalServiceId(storageService);
        UNIT_ASSERT(storage);
        auto dropped = NThreading::NewPromise<void>();
        runtime.RegisterService(storageService, runtime.Register(new TLoseRenameCommitReply(storage, dropped)));

        ::NFq::NConfig::TConfig fqConfig;
        auto& computeConfig = *fqConfig.MutableCompute();
        computeConfig.SetDefaultCompute(::NFq::NConfig::YDB);
        computeConfig.MutableSupportedComputeYdbFeatures()->SetRestartableExternalTableRename(true);
        auto& computeYdb = *computeConfig.MutableYdb();
        computeYdb.SetEnable(true);
        auto& controlPlane = *computeYdb.MutableControlPlane();
        controlPlane.SetEnable(true);
        controlPlane.MutableSingle()->MutableConnection()->SetToken(user);
        fqConfig.MutableControlPlaneStorage()->AddAvailableBinding("OBJECT_STORAGE");
        const auto credFactory = NKikimr::CreateYdbCredentialsProviderFactory;
        const auto counters = MakeIntrusive<::NMonitoring::TDynamicCounters>();
        const auto resources = ::NFq::CreateYqSharedResourcesImpl(fqConfig, credFactory, counters);
        resources->Init(runtime.GetActorSystem(0));
        Y_DEFER {
            resources->Stop();
        };
        const auto proxyService = ::NFq::ControlPlaneProxyActorId();
        runtime.RegisterService(proxyService,
            runtime.Register(::NFq::CreateControlPlaneProxyActor(fqConfig.GetControlPlaneProxy(), fqConfig.GetControlPlaneStorage(),
                computeConfig, fqConfig.GetCommon(), fqConfig.GetGateways().GetS3(), {}, resources, credFactory, counters, false)));

        column.mutable_type()->mutable_optional_type()->mutable_item()->mutable_pg_type()->set_type_name("int4");
        auto& textColumn = *subset.mutable_schema()->add_column();
        textColumn.set_name("value");
        auto& pg = *textColumn.mutable_type()->mutable_optional_type()->mutable_item()->mutable_pg_type();
        pg.set_type_name("pgvarchar");
        pg.set_type_modifier("17");
        const auto modify = ::NFq::TModifyBindingBuilder{}
                                .SetBindingId(bindingId)
                                .SetName("new_binding")
                                .SetConnectionId(connectionId)
                                .SetPreviousRevision(revision)
                                .SetIdempotencyKey("rename")
                                .CreateObjectStorage(objectStorage)
                                .Build();
        using TProxy = ::NFq::TEvControlPlaneProxy;
        const auto rename = [&](const FederatedQuery::ModifyBindingRequest& proto) {
            const auto sender = runtime.AllocateEdgeActor();
            auto request = std::make_unique<TProxy::TEvModifyBindingRequest>(scope.ToString(), proto, user, user, TVector<TString>{});
            request->CloudId = "cloud";
            request->SubjectType = "user";
            auto& computeDb = *request->ComputeDatabase.ConstructInPlace().mutable_connection();
            computeDb.set_endpoint(endpoint);
            computeDb.set_database("/Root");
            runtime.Send(new NActors::IEventHandle(proxyService, sender, request.release(), 0, 42));
            return runtime.GrabEdgeEventRethrow<TProxy::TEvModifyBindingResponse>(sender, TDuration::Seconds(30));
        };

        auto invalid = modify;
        *invalid.mutable_content()->mutable_setting()->mutable_object_storage()->add_subset() = objectStorage.subset(0);
        const auto rejected = rename(invalid);
        UNIT_ASSERT_STRING_CONTAINS(rejected->Get()->Issues.ToString(), "A binding rename requires exactly one Object Storage subset");
        UNIT_ASSERT(!dropped.HasValue());
        UNIT_ASSERT_VALUES_EQUAL(describeBinding().meta().revision(), revision);
        UNIT_ASSERT_VALUES_EQUAL(describeComputeTable("new_binding").GetStatus(), EStatus::NOT_FOUND);

        auto malformed = modify;
        auto& malformedSubset = *malformed.mutable_content()->mutable_setting()->mutable_object_storage()->mutable_subset(0);
        malformedSubset.mutable_schema()->add_column()->set_name("missing_type");
        const auto malformedResult = rename(malformed);
        UNIT_ASSERT_STRING_CONTAINS(malformedResult->Get()->Issues.ToString(), "Unsupported external table column type");
        UNIT_ASSERT(!dropped.HasValue());
        UNIT_ASSERT_VALUES_EQUAL(describeBinding().meta().revision(), revision);
        UNIT_ASSERT_VALUES_EQUAL(describeComputeTable("new_binding").GetStatus(), EStatus::NOT_FOUND);

        const auto interrupted = rename(modify);
        UNIT_ASSERT_C(dropped.HasValue(), interrupted->Get()->Issues.ToString());
        UNIT_ASSERT_VALUES_EQUAL(interrupted->Cookie, 42);
        UNIT_ASSERT_VALUES_EQUAL(interrupted->Get()->SubjectType, "user");
        UNIT_ASSERT_VALUES_EQUAL(interrupted->Get()->Issues.Size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(interrupted->Get()->Issues.begin()->GetCode(), static_cast<ui32>(::NFq::TIssuesIds::TIMEOUT));
        const auto afterCommit = describeBinding();
        UNIT_ASSERT_VALUES_EQUAL(afterCommit.content().name(), "new_binding");
        UNIT_ASSERT_VALUES_EQUAL(afterCommit.meta().revision(), revision + 1);
        const auto stillPresent = describeComputeTable("old_binding");
        UNIT_ASSERT_C(stillPresent.IsSuccess(), stillPresent.GetIssues().ToString());
        UNIT_ASSERT_VALUES_EQUAL(stillPresent.GetResult().identity().SerializeAsString(), original.GetResult().identity().SerializeAsString());
        const auto candidate = describeComputeTable("new_binding");
        UNIT_ASSERT_C(candidate.IsSuccess(), candidate.GetIssues().ToString());

        const auto retried = rename(modify);
        UNIT_ASSERT_C(!retried->Get()->Issues, retried->Get()->Issues.ToString());
        UNIT_ASSERT_VALUES_EQUAL(describeBinding().meta().revision(), revision + 1);
        UNIT_ASSERT_VALUES_EQUAL(describeComputeTable("old_binding").GetStatus(), EStatus::NOT_FOUND);
        const auto final = describeComputeTable("new_binding");
        UNIT_ASSERT_C(final.IsSuccess(), final.GetIssues().ToString());
        const auto& columns = final.GetResult().description().columns();
        const auto text = std::find_if(columns.begin(), columns.end(), [](const auto& item) {
            return item.name() == "value";
        });
        UNIT_ASSERT(text != columns.end());
        UNIT_ASSERT(text->type().has_pg_type());
        UNIT_ASSERT_VALUES_EQUAL(text->type().pg_type().type_name(), "pgvarchar");
        UNIT_ASSERT_VALUES_EQUAL(text->type().pg_type().type_modifier(), "17");
        const auto& finalIdentity = final.GetResult().identity();
        const auto& candidateIdentity = candidate.GetResult().identity();
        UNIT_ASSERT_VALUES_EQUAL(finalIdentity.owner_id(), candidateIdentity.owner_id());
        UNIT_ASSERT_VALUES_EQUAL(finalIdentity.local_path_id(), candidateIdentity.local_path_id());
    }
}

Y_UNIT_TEST_SUITE(Yq_1) {
    Y_UNIT_TEST(Basic) {
        TKikimrWithGrpcAndRootSchema server({}, {}, {}, true);
        ui16 grpc = server.GetPort();
        TString location = TStringBuilder() << "localhost:" << grpc;
        auto driverConfig = TDriverConfig().SetEndpoint(location).SetDatabase("/Root").SetAuthToken("root@builtin");
        auto driver = TDriver(driverConfig);
        NYdb::NFq::TClient client(driver);
        const auto folderId = "some_folder_id";
        const auto queryId = CreateNewHistoryAndWaitFinish(folderId, client, "select 1", FederatedQuery::QueryMeta::COMPLETED);
        CheckGetResultData(client, queryId, folderId, 1, 1, 1);

        {
            const auto request = ::NFq::TDescribeQueryBuilder().SetQueryId("foo").Build();
            auto result = client.DescribeQuery(request, CreateFqSettings<TDescribeQuerySettings>("WTF")).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::BAD_REQUEST);
        }

        {
            auto request = ::NFq::TListQueriesBuilder{}.Build();
            auto result = client.ListQueries(request, CreateFqSettings<TListQueriesSettings>("WTF")).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            UNIT_ASSERT_VALUES_EQUAL(result.GetResult().query().size(), 0);
        }

        {
            const auto request = ::NFq::TDescribeQueryBuilder().SetQueryId(queryId).Build();
            auto result = client.DescribeQuery(request, CreateFqSettings<TDescribeQuerySettings>(folderId)).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            const auto status = result.GetResult().query().meta().status();
            UNIT_ASSERT(status == FederatedQuery::QueryMeta::COMPLETED);
        }

        {
            const auto request = ::NFq::TModifyQueryBuilder().SetQueryId(queryId).SetName("MODIFIED_NAME").Build();
            const auto result = client.ModifyQuery(request, CreateFqSettings<TModifyQuerySettings>(folderId)).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        {
            const auto request = ::NFq::TDescribeQueryBuilder().SetQueryId(queryId).Build();
            const auto result = client.DescribeQuery(request, CreateFqSettings<TDescribeQuerySettings>(folderId)).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            const auto& res = result.GetResult();
            UNIT_ASSERT_VALUES_EQUAL(res.query().content().name(), "MODIFIED_NAME");
            UNIT_ASSERT(res.query().content().acl().visibility() == static_cast<int>(Acl_Visibility_SCOPE));
        }

        {
            const auto request = ::NFq::TDescribeQueryBuilder().SetQueryId("").Build();
            const auto result = client.DescribeQuery(request, CreateFqSettings<TDescribeQuerySettings>("")).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::BAD_REQUEST, result.GetIssues().ToString());
        }

        {
            const auto request = ::NFq::TGetResultDataBuilder().SetQueryId("").Build();
            const auto result = client.GetResultData(request, CreateFqSettings<TGetResultDataSettings>("")).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::BAD_REQUEST, result.GetIssues().ToString());
        }
    }

    Y_UNIT_TEST(Basic_EmptyList) {
        TKikimrWithGrpcAndRootSchema server({}, {}, {}, true);
        ui16 grpc = server.GetPort();
        TString location = TStringBuilder() << "localhost:" << grpc;
        auto driverConfig = TDriverConfig().SetEndpoint(location).SetDatabase("/Root").SetAuthToken("root@builtin");
        auto driver = TDriver(driverConfig);
        NYdb::NFq::TClient client(driver);
        const TString folderId = "some_folder_id";
        auto expectedStatus = FederatedQuery::QueryMeta::COMPLETED;
        CreateNewHistoryAndWaitFinish(folderId, client, "select []", expectedStatus);
    }

    Y_UNIT_TEST(Basic_EmptyDict) {
        TKikimrWithGrpcAndRootSchema server({}, {}, {}, true);
        ui16 grpc = server.GetPort();
        TString location = TStringBuilder() << "localhost:" << grpc;
        auto driverConfig = TDriverConfig().SetEndpoint(location).SetDatabase("/Root").SetAuthToken("root@builtin");
        auto driver = TDriver(driverConfig);
        NYdb::NFq::TClient client(driver);
        const TString folderId = "some_folder_id";
        auto expectedStatus = FederatedQuery::QueryMeta::COMPLETED;
        CreateNewHistoryAndWaitFinish(folderId, client, "select {}", expectedStatus);
    }

    Y_UNIT_TEST(Basic_Null) {
        TKikimrWithGrpcAndRootSchema server({}, {}, {}, true);
        ui16 grpc = server.GetPort();
        TString location = TStringBuilder() << "localhost:" << grpc;
        auto driverConfig = TDriverConfig().SetEndpoint(location).SetDatabase("/Root").SetAuthToken("root@builtin");
        auto driver = TDriver(driverConfig);
        NYdb::NFq::TClient client(driver);
        const TString folderId = "some_folder_id";
        auto expectedStatus = FederatedQuery::QueryMeta::COMPLETED;
        CreateNewHistoryAndWaitFinish(folderId, client, "select null", expectedStatus);
    }

    Y_UNIT_TEST(Basic_TaggedLiteral) {
        TKikimrWithGrpcAndRootSchema server({}, {}, {}, true);
        ui16 grpc = server.GetPort();
        TString location = TStringBuilder() << "localhost:" << grpc;
        auto driverConfig = TDriverConfig().SetEndpoint(location).SetDatabase("/Root").SetAuthToken("root@builtin");
        auto driver = TDriver(driverConfig);
        NYdb::NFq::TClient client(driver);
        const TString folderId = "some_folder_id";

        auto expectedStatus = FederatedQuery::QueryMeta::COMPLETED;
        CreateNewHistoryAndWaitFinish(folderId, client, "select AsTagged(1, \"tag\")", expectedStatus);
    }

    // use fork for data test due to ch initialization problem
    Y_UNIT_TEST(DescribeConnection) {
        TKikimrWithGrpcAndRootSchema server({}, {}, {}, true);
        ui16 grpc = server.GetPort();
        TString location = TStringBuilder() << "localhost:" << grpc;
        auto driverConfig = TDriverConfig().SetEndpoint(location).SetDatabase("/Root").SetAuthToken("root@builtin");
        auto driver = TDriver(driverConfig);
        NYdb::NFq::TClient client(driver);
        const TString folderId = "some_folder_id";
        Warmup(folderId, client);
        TString conId;
        {
            const auto request = ::NFq::TCreateConnectionBuilder().SetName("created_conn").CreateYdb("created_db", "").Build();
            const auto result = client.CreateConnection(request, CreateFqSettings<TCreateConnectionSettings>(folderId)).ExtractValueSync();
            UNIT_ASSERT_C(result.GetStatus() == EStatus::SUCCESS, result.GetIssues().ToString());
            conId = result.GetResult().connection_id();
        }
        {
            auto request = ::NFq::TDescribeConnectionBuilder().SetConnectionId(conId).Build();
            auto result = client.DescribeConnection(request, CreateFqSettings<TDescribeConnectionSettings>(folderId)).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);
            const auto& res = result.GetResult().connection();
            UNIT_ASSERT_VALUES_EQUAL(res.meta().id(), conId);
            UNIT_ASSERT_VALUES_EQUAL(res.meta().created_by(), "root@builtin");
            UNIT_ASSERT_VALUES_EQUAL(res.meta().modified_by(), "root@builtin");
            UNIT_ASSERT_VALUES_EQUAL(res.content().name(), "created_conn");
        }
    }

    Y_UNIT_TEST(ListConnections) {
        TKikimrWithGrpcAndRootSchema server({}, {}, {}, true);
        ui16 grpc = server.GetPort();
        TString location = TStringBuilder() << "localhost:" << grpc;
        auto driverConfig = TDriverConfig().SetEndpoint(location).SetDatabase("/Root").SetAuthToken("root@builtin");
        auto driver = TDriver(driverConfig);
        NYdb::NFq::TClient client(driver);
        const size_t conns = 3;
        const auto folderId = TString(__func__) + "folder_id";
        Warmup(folderId, client);
        {
            //CreateConnections
            for (size_t i = 0; i < conns - 1; ++i) {
                const auto request = ::NFq::TCreateConnectionBuilder().SetName("testdb" + ToString(i)).CreateYdb("FakeDatabaseId", "").Build();
                const auto result = client.CreateConnection(request, CreateFqSettings<TCreateConnectionSettings>(folderId)).ExtractValueSync();
                UNIT_ASSERT_C(result.GetStatus() == EStatus::SUCCESS, result.GetIssues().ToString());
            }

            // yds
            const auto request = ::NFq::TCreateConnectionBuilder()
                                     .SetName("testdb2")
                                     .CreateDataStreams("FakeDatabaseId", "")   // We can use the same db in yds and ydb
                                     .Build();
            const auto result = client.CreateConnection(request, CreateFqSettings<TCreateConnectionSettings>(folderId)).ExtractValueSync();
            UNIT_ASSERT_C(result.GetStatus() == EStatus::SUCCESS, result.GetIssues().ToString());
        }

        {
            const auto request = ::NFq::TListConnectionsBuilder().Build();
            auto result = client.ListConnections(request, CreateFqSettings<TListConnectionsSettings>(folderId)).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            UNIT_ASSERT_VALUES_EQUAL(result.GetResult().connection().size(), conns);
            size_t i = 0;
            auto res = result.GetResult();
            auto* conns = res.mutable_connection();
            std::sort(conns->begin(), conns->end(), [&](const auto& lhs, const auto& rhs) {
                return lhs.content().name() < rhs.content().name();
            });
            for (const auto& conn : *conns) {
                const auto& content = conn.content();
                const auto& meta = conn.meta();
                UNIT_ASSERT_VALUES_EQUAL(content.name(), "testdb" + ToString(i));
                UNIT_ASSERT_VALUES_EQUAL(meta.created_by(), "root@builtin");
                UNIT_ASSERT_VALUES_EQUAL(meta.modified_by(), "root@builtin");
                if (i < 2) {
                    UNIT_ASSERT_C(content.setting().has_ydb_database(), content);
                } else {
                    UNIT_ASSERT_C(content.setting().has_data_streams(), content);
                }
                i++;
            }
        }
    }

    Y_UNIT_TEST(ListConnectionsOnEmptyConnectionsTable) {
        TKikimrWithGrpcAndRootSchema server({}, {}, {}, true);
        ui16 grpc = server.GetPort();
        TString location = TStringBuilder() << "localhost:" << grpc;
        auto driverConfig = TDriverConfig().SetEndpoint(location).SetDatabase("/Root").SetAuthToken("root@builtin");
        auto driver = TDriver(driverConfig);
        NYdb::NFq::TClient client(driver);
        const TString folderId = "some_folder_id";
        Warmup(folderId, client);

        {
            const auto request = ::NFq::TListConnectionsBuilder().Build();
            auto result = client.ListConnections(request, CreateFqSettings<TListConnectionsSettings>("WTF")).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            UNIT_ASSERT(result.GetResult().connection().empty());
        }
    }

    Y_UNIT_TEST(ModifyConnections) {
        TKikimrWithGrpcAndRootSchema server({}, {}, {}, true);
        ui16 grpc = server.GetPort();
        TString location = TStringBuilder() << "localhost:" << grpc;
        TString userToken = "root@builtin";
        TString userId = "root";
        auto driverConfig = TDriverConfig().SetEndpoint(location).SetDatabase("/Root").SetAuthToken(userToken);
        auto driver = TDriver(driverConfig);
        NYdb::NFq::TClient client(driver);
        const auto folderId = TString(__func__) + "folder_id";
        Warmup(folderId, client);
        TString conId;
        {
            const auto request = ::NFq::TCreateConnectionBuilder().SetName("created_conn").CreateYdb("created_db", "").Build();
            const auto result = client.CreateConnection(request, CreateFqSettings<TCreateConnectionSettings>(folderId)).ExtractValueSync();
            UNIT_ASSERT_C(result.GetStatus() == EStatus::SUCCESS, result.GetIssues().ToString());
            conId = result.GetResult().connection_id();
        }

        {
            //Modify
            const auto request = ::NFq::TModifyConnectionBuilder()
                                     .SetName("modified_name")
                                     .SetConnectionId(conId)
                                     .CreateYdb("new ydb", "")
                                     .SetDescription("Modified")
                                     .Build();
            const auto result = client.ModifyConnection(request, CreateFqSettings<TModifyConnectionSettings>(folderId)).ExtractValueSync();
            UNIT_ASSERT_C(result.GetStatus() == EStatus::SUCCESS, result.GetIssues().ToString());
        }

        {
            auto request = ::NFq::TDescribeConnectionBuilder().SetConnectionId(conId).Build();
            auto result = client.DescribeConnection(request, CreateFqSettings<TDescribeConnectionSettings>(folderId)).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            const auto& res = result.GetResult().connection();
            UNIT_ASSERT_VALUES_EQUAL(res.meta().id(), conId);
            UNIT_ASSERT_VALUES_EQUAL(res.meta().created_by(), "root@builtin");
            UNIT_ASSERT_VALUES_EQUAL(res.meta().modified_by(), "root@builtin");
            UNIT_ASSERT_VALUES_EQUAL(res.content().name(), "modified_name");
            UNIT_ASSERT_VALUES_EQUAL(res.content().description(), "Modified");
        }
    }

    Y_UNIT_TEST(DeleteConnections) {
        TKikimrWithGrpcAndRootSchema server({}, {}, {}, true);
        ui16 grpc = server.GetPort();
        TString location = TStringBuilder() << "localhost:" << grpc;
        auto driverConfig = TDriverConfig().SetEndpoint(location).SetDatabase("/Root").SetAuthToken("root@builtin");
        auto driver = TDriver(driverConfig);
        NYdb::NFq::TClient client(driver);
        const auto folderId = TString(__func__) + "folder_id";
        Warmup(folderId, client);
        TString conId;
        {
            const auto request = ::NFq::TCreateConnectionBuilder().SetName("created_conn").CreateYdb("created_db", "").Build();
            const auto result = client.CreateConnection(request, CreateFqSettings<TCreateConnectionSettings>(folderId)).ExtractValueSync();
            UNIT_ASSERT_C(result.GetStatus() == EStatus::SUCCESS, result.GetIssues().ToString());
            conId = result.GetResult().connection_id();
        }

        {
            const auto request = ::NFq::TDeleteConnectionBuilder().SetConnectionId(conId).Build();

            const auto result = client.DeleteConnection(request, CreateFqSettings<TDeleteConnectionSettings>(folderId)).ExtractValueSync();
            UNIT_ASSERT_C(result.GetStatus() == EStatus::SUCCESS, result.GetIssues().ToString());
        }
    }

    Y_UNIT_TEST(Create_And_Modify_The_Same_Connection) {
        TKikimrWithGrpcAndRootSchema server({}, {}, {}, true);
        ui16 grpc = server.GetPort();
        TString location = TStringBuilder() << "localhost:" << grpc;
        TString userToken = "root@builtin";
        auto driverConfig = TDriverConfig().SetEndpoint(location).SetDatabase("/Root").SetAuthToken(userToken);
        auto driver = TDriver(driverConfig);
        NYdb::NFq::TClient client(driver);

        const auto folderId = TString(__func__) + "folder_id";
        Warmup(folderId, client);
        TString conId;
        {
            const auto request = ::NFq::TCreateConnectionBuilder().SetName("created_conn").CreateYdb("created_db", "").Build();
            const auto result = client.CreateConnection(request, CreateFqSettings<TCreateConnectionSettings>(folderId)).ExtractValueSync();
            UNIT_ASSERT_C(result.GetStatus() == EStatus::SUCCESS, result.GetIssues().ToString());
            conId = result.GetResult().connection_id();
        }

        {
            const auto request = ::NFq::TModifyConnectionBuilder()
                                     .SetConnectionId(conId)
                                     .CreateYdb("modified_db", "")   //TODO remove
                                     .Build();
            const auto result = client.ModifyConnection(request, CreateFqSettings<TModifyConnectionSettings>(folderId)).ExtractValueSync();
            UNIT_ASSERT_C(result.GetStatus() == EStatus::SUCCESS, result.GetIssues().ToString());
        }
    }

    Y_UNIT_TEST(CreateConnection_With_Existing_Name) {
        TKikimrWithGrpcAndRootSchema server({}, {}, {}, true);
        ui16 grpc = server.GetPort();
        TString location = TStringBuilder() << "localhost:" << grpc;
        TString userToken = "root@builtin";
        auto driverConfig = TDriverConfig().SetEndpoint(location).SetDatabase("/Root").SetAuthToken(userToken);
        auto driver = TDriver(driverConfig);
        NYdb::NFq::TClient client(driver);

        const auto folderId = TString(__func__) + "folder_id";
        Warmup(folderId, client);
        auto name = TString(__func__) + "_name";
        name.to_lower();
        {
            const auto request = ::NFq::TCreateConnectionBuilder().SetName(name).CreateYdb("created_db", "").Build();
            const auto result = client.CreateConnection(request, CreateFqSettings<TCreateConnectionSettings>(folderId)).ExtractValueSync();
            UNIT_ASSERT_C(result.GetStatus() == EStatus::SUCCESS, result.GetIssues().ToString());
        }

        {
            const auto request = ::NFq::TCreateConnectionBuilder().SetName(name).CreateYdb("created_db", "").Build();
            const auto result = client.CreateConnection(request, CreateFqSettings<TCreateConnectionSettings>(folderId)).ExtractValueSync();
            UNIT_ASSERT_C(result.GetStatus() == EStatus::BAD_REQUEST, result.GetIssues().ToString());   //TODO status should be ALREADY_EXISTS
        }
    }

    Y_UNIT_TEST(CreateConnections_With_Idempotency) {
        TKikimrWithGrpcAndRootSchema server({}, {}, {}, true);
        ui16 grpc = server.GetPort();
        TString location = TStringBuilder() << "localhost:" << grpc;
        TString userToken = "root@builtin";
        auto driverConfig = TDriverConfig().SetEndpoint(location).SetDatabase("/Root").SetAuthToken(userToken);
        auto driver = TDriver(driverConfig);
        NYdb::NFq::TClient client(driver);
        const auto folderId = TString(__func__) + "folder_id";
        Warmup(folderId, client);
        const auto name = "connection_name";
        const TString idempotencyKey = "idempotency_key";
        TString conId;
        {
            const auto request =
                ::NFq::TCreateConnectionBuilder().SetName(name).SetIdempotencyKey(idempotencyKey).CreateYdb("created_db", "").Build();
            const auto result = client.CreateConnection(request, CreateFqSettings<TCreateConnectionSettings>(folderId)).ExtractValueSync();
            UNIT_ASSERT_C(result.GetStatus() == EStatus::SUCCESS, result.GetIssues().ToString());
            conId = result.GetResult().connection_id();
        }

        {
            const auto request =
                ::NFq::TCreateConnectionBuilder().SetName(name).SetIdempotencyKey(idempotencyKey).CreateYdb("created_db", "").Build();
            const auto result = client.CreateConnection(request, CreateFqSettings<TCreateConnectionSettings>(folderId)).ExtractValueSync();
            UNIT_ASSERT_C(result.GetStatus() == EStatus::SUCCESS, result.GetIssues().ToString());
            UNIT_ASSERT_VALUES_EQUAL(conId, result.GetResult().connection_id());
        }
    }

    Y_UNIT_TEST(CreateQuery_With_Idempotency) {   //TODO Fix
        TKikimrWithGrpcAndRootSchema server({}, {}, {}, true);
        ui16 grpc = server.GetPort();
        TString location = TStringBuilder() << "localhost:" << grpc;
        TString userToken = "root@builtin";
        auto driverConfig = TDriverConfig().SetEndpoint(location).SetDatabase("/Root").SetAuthToken(userToken);
        auto driver = TDriver(driverConfig);
        NYdb::NFq::TClient client(driver);

        const auto folderId = TString(__func__) + "folder_id";
        Warmup(folderId, client);

        const TString idempotencyKey = "idempotency_key";
        const TString yqlText = "select 1";
        TString queryId;
        const auto request = ::NFq::TCreateQueryBuilder{}.SetText(yqlText).SetIdempotencyKey(idempotencyKey).Build();

        {
            auto result = client.CreateQuery(request, CreateFqSettings<TCreateQuerySettings>(folderId)).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            queryId = result.GetResult().query_id();
        }

        {
            const auto req = ::NFq::TDescribeQueryBuilder{}.SetQueryId(queryId).Build();
            const auto result = DoWithRetryOnRetCode([&]() {
                auto result = client.DescribeQuery(req, CreateFqSettings<TDescribeQuerySettings>(folderId)).ExtractValueSync();
                UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
                const auto status = result.GetResult().query().meta().status();
                PrintProtoIssues(result.GetResult().query().issue());
                return status == FederatedQuery::QueryMeta::COMPLETED;
            }, TRetryOptions(Retries));
            UNIT_ASSERT_C(result, "the execution of the query did not end within the time limit");
        }
        {
            auto result = client.CreateQuery(request, CreateFqSettings<TCreateQuerySettings>(folderId)).ExtractValueSync();

            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            UNIT_ASSERT_VALUES_EQUAL(queryId, result.GetResult().query_id());
        }
        CheckGetResultData(client, queryId, folderId, 1, 1, 1);
    }

    // use fork for data test due to ch initialization problem
    SIMPLE_UNIT_FORKED_TEST(CreateQuery_Without_Connection) {
        TKikimrWithGrpcAndRootSchema server({}, {}, {}, true);
        ui16 grpc = server.GetPort();
        TString location = TStringBuilder() << "localhost:" << grpc;
        TString userToken = "root@builtin";
        auto driverConfig = TDriverConfig().SetEndpoint(location).SetDatabase("/Root").SetAuthToken(userToken);
        auto driver = TDriver(driverConfig);
        NYdb::NFq::TClient client(driver);

        const TString yqlText = "select count(*) from testdbWTF.connections";
        CreateNewHistoryAndWaitFinish("folder_id_WTF", client, yqlText, FederatedQuery::QueryMeta::FAILED);
    }

    Y_UNIT_TEST(DeleteQuery) {
        TKikimrWithGrpcAndRootSchema server({}, {}, {}, true);
        ui16 grpc = server.GetPort();
        TString location = TStringBuilder() << "localhost:" << grpc;
        TString userToken = "root@builtin";
        auto driverConfig = TDriverConfig().SetEndpoint(location).SetDatabase("/Root").SetAuthToken(userToken);
        auto driver = TDriver(driverConfig);
        NYdb::NFq::TClient client(driver);

        const auto folderId = TString(__func__) + "folder_id";
        const TString yqlText = "select 1";
        const TString queryId = CreateNewHistoryAndWaitFinish(folderId, client, yqlText, FederatedQuery::QueryMeta::COMPLETED);
        CheckGetResultData(client, queryId, folderId, 1, 1, 1);

        {
            const auto request = ::NFq::TDeleteQueryBuilder().SetQueryId(queryId).Build();
            auto result = client.DeleteQuery(request, CreateFqSettings<TDeleteQuerySettings>(folderId)).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.IsTransportError(), false);
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        {
            auto request = ::NFq::TDescribeQueryBuilder().SetQueryId(queryId).Build();
            auto result = client.DescribeQuery(request, CreateFqSettings<TDescribeQuerySettings>(folderId)).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::BAD_REQUEST, result.GetIssues().ToString());
        }
    }

    Y_UNIT_TEST(ModifyQuery) {
        TKikimrWithGrpcAndRootSchema server({}, {}, {}, true);
        ui16 grpc = server.GetPort();
        TString location = TStringBuilder() << "localhost:" << grpc;
        TString userToken = "root@builtin";
        auto driverConfig = TDriverConfig().SetEndpoint(location).SetDatabase("/Root").SetAuthToken(userToken);
        auto driver = TDriver(driverConfig);
        NYdb::NFq::TClient client(driver);

        const auto folderId = TString(__func__) + "folder_id";
        const TString yqlText = "select 1";
        const TString queryId = CreateNewHistoryAndWaitFinish(folderId, client, yqlText, FederatedQuery::QueryMeta::COMPLETED);
        CheckGetResultData(client, queryId, folderId, 1, 1, 1);

        {
            const auto request = ::NFq::TModifyQueryBuilder().SetQueryId(queryId).SetName("ModifiedName").SetDescription("OK").Build();
            auto result = client.ModifyQuery(request, CreateFqSettings<TModifyQuerySettings>(folderId)).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.IsTransportError(), false);
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        {
            auto request = ::NFq::TDescribeQueryBuilder().SetQueryId(queryId).Build();
            auto result = client.DescribeQuery(request, CreateFqSettings<TDescribeQuerySettings>(folderId)).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            const auto& query = result.GetResult().query();
            UNIT_ASSERT_VALUES_EQUAL(query.content().name(), "ModifiedName");
            UNIT_ASSERT_VALUES_EQUAL(query.content().description(), "OK");
        }
    }

    Y_UNIT_TEST(DescribeJob) {
        TKikimrWithGrpcAndRootSchema server({}, {}, {}, true);
        ui16 grpc = server.GetPort();
        TString location = TStringBuilder() << "localhost:" << grpc;
        auto driverConfig = TDriverConfig().SetEndpoint(location).SetDatabase("/Root").SetAuthToken("root@builtin");
        auto driver = TDriver(driverConfig);
        NYdb::NFq::TClient client(driver);
        const auto folderId = "some_folder_id";
        const auto queryId = CreateNewHistoryAndWaitFinish(folderId, client, "select 1", FederatedQuery::QueryMeta::COMPLETED);
        CheckGetResultData(client, queryId, folderId, 1, 1, 1);
        TString jobId;

        {
            auto request = ::NFq::TListJobsBuilder{}.SetQueryId(queryId).Build();
            auto result = DoWithRetryOnRetCode([&]() {
                auto result = client.ListJobs(request, CreateFqSettings<TListJobsSettings>(folderId)).ExtractValueSync();
                UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
                UNIT_ASSERT_VALUES_EQUAL(result.GetResult().job_size(), 1);
                jobId = result.GetResult().job(0).meta().id();
                return result.GetStatus() == EStatus::SUCCESS;
            }, TRetryOptions(Retries));
            UNIT_ASSERT_C(result, "the execution of the query did not end within the time limit");
        }

        {
            const auto request = ::NFq::TDescribeJobBuilder().SetJobId(jobId).Build();
            auto result = client.DescribeJob(request, CreateFqSettings<TDescribeJobSettings>(folderId)).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            UNIT_ASSERT_VALUES_EQUAL(result.GetResult().job().query_meta().common().id(), queryId);
            UNIT_ASSERT_VALUES_EQUAL(result.GetResult().job().meta().id(), jobId);
            UNIT_ASSERT_VALUES_EQUAL(result.GetResult().job().query_name(), "test_query_name_1");
        }
    }

    Y_UNIT_TEST(DescribeQuery) {
        TKikimrWithGrpcAndRootSchema server({}, {}, {}, true);
        ui16 grpc = server.GetPort();
        TString location = TStringBuilder() << "localhost:" << grpc;
        auto driverConfig = TDriverConfig().SetEndpoint(location).SetDatabase("/Root").SetAuthToken("root@builtin");
        auto driver = TDriver(driverConfig);
        NYdb::NFq::TClient client(driver);
        const auto folderId = "some_folder_id";
        const auto queryId = CreateNewHistoryAndWaitFinish(folderId, client, "select 1", FederatedQuery::QueryMeta::COMPLETED);
        CheckGetResultData(client, queryId, folderId, 1, 1, 1);
        TString jobId;

        {
            const auto request = ::NFq::TDescribeQueryBuilder().SetQueryId(queryId).Build();
            auto result = client.DescribeQuery(request, CreateFqSettings<TDescribeQuerySettings>(folderId)).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            const auto query = result.GetResult().query();
            UNIT_ASSERT_VALUES_EQUAL(FederatedQuery::QueryMeta::ComputeStatus_Name(query.meta().status()),
                FederatedQuery::QueryMeta::ComputeStatus_Name(FederatedQuery::QueryMeta::COMPLETED));
            UNIT_ASSERT_VALUES_EQUAL(query.content().text(), "select 1");
            UNIT_ASSERT_VALUES_EQUAL(query.content().name(), "test_query_name_1");
        }
    }
}

Y_UNIT_TEST_SUITE(PrivateApi) {
    Y_UNIT_TEST(PingTask) {
        TKikimrWithGrpcAndRootSchema server({}, {}, {}, true);
        ui16 grpc = server.GetPort();
        TString location = TStringBuilder() << "localhost:" << grpc;
        auto driver = TDriver(TDriverConfig().SetEndpoint(location).SetDatabase("/Root").SetAuthToken("root@builtin"));
        ::NFq::TPrivateClient client(driver);
        const TString historyId = "id";
        const TString folderId = "folder_id";
        const TScope scope(folderId);
        {
            Fq::Private::PingTaskRequest req;
            req.mutable_query_id()->set_value("id");
            req.set_scope(scope.ToString());
            req.set_owner_id("some_owner");
            req.set_status(FederatedQuery::QueryMeta::COMPLETED);
            const auto result = DoWithRetryOnRetCode([&]() {
                auto r = req;
                auto result = client.PingTask(std::move(r)).ExtractValueSync();
                result.GetIssues().PrintTo(Cerr);
                if (result.GetStatus() == EStatus::TRANSPORT_UNAVAILABLE) {
                    return false;
                }
                UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::GENERIC_ERROR);
                return true;
            }, TRetryOptions(Retries));
            UNIT_ASSERT_C(result, "Incorrect ping did not fail within the time limit");
        }
    }

    Y_UNIT_TEST(GetTask) {   //PendingFetcher can take task first
        TKikimrWithGrpcAndRootSchema server({}, {}, {}, true);
        ui16 grpc = server.GetPort();
        TString location = TStringBuilder() << "localhost:" << grpc;
        auto driver = TDriver(TDriverConfig().SetEndpoint(location).SetDatabase("/Root").SetAuthToken("root@builtin"));
        ::NFq::TPrivateClient client(driver);
        {
            Fq::Private::GetTaskRequest req;
            req.set_owner_id("owner_id");
            req.set_host("host");
            const auto result = DoWithRetryOnRetCode([&]() {
                auto r = req;
                auto result = client.GetTask(std::move(r)).ExtractValueSync();
                if (result.GetStatus() == EStatus::TRANSPORT_UNAVAILABLE) {
                    return false;
                }
                result.GetIssues().PrintTo(Cerr);
                UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);
                return true;
            }, TRetryOptions(Retries));
            UNIT_ASSERT_C(result, "GetTask did not succeed within the time limit");
        }
    }

    Y_UNIT_TEST(Nodes) {
        TKikimrWithGrpcAndRootSchema server({}, {}, {}, true);
        ui16 grpc = server.GetPort();
        TString location = TStringBuilder() << "localhost:" << grpc;
        auto driver = TDriver(TDriverConfig().SetEndpoint(location).SetDatabase("/Root").SetAuthToken("root@builtin"));
        ::NFq::TPrivateClient client(driver);
        const auto instanceId = CreateGuidAsString();
        {
            Fq::Private::NodesHealthCheckRequest req;
            req.set_tenant("Tenant");
            auto& node = *req.mutable_node();
            node.set_hostname("hostname");
            node.set_node_id(100500);
            node.set_instance_id(instanceId);
            const auto result = DoWithRetryOnRetCode([&]() {
                auto r = req;
                auto result = client.NodesHealthCheck(std::move(r)).ExtractValueSync();
                if (result.GetStatus() == EStatus::SUCCESS) {
                    const auto& res = result.GetResult();
                    UNIT_ASSERT(!res.nodes().empty());
                    UNIT_ASSERT_VALUES_EQUAL(res.nodes(0).hostname(), "hostname");
                    UNIT_ASSERT_VALUES_EQUAL(res.nodes(0).node_id(), 100500);
                    UNIT_ASSERT_VALUES_EQUAL(res.nodes(0).instance_id(), instanceId);
                }
                // result.GetIssues().PrintTo(Cerr);
                return result.GetStatus() == EStatus::SUCCESS;
            }, TRetryOptions(Retries));
            UNIT_ASSERT_C(result, "the execution of the query did not end within the time limit");
        }
    }
}
