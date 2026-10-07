#include <ydb/core/fq/libs/compute/ydb/control_plane/compute_database_control_plane_service.h>
#include <ydb/core/fq/libs/compute/ydb/events/events.h>
#include <ydb/core/fq/libs/config/protos/issue_id.pb.h>
#include <ydb/core/fq/libs/control_plane_storage/control_plane_storage.h>
#include <ydb/core/fq/libs/control_plane_storage/events/events.h>
#include <ydb/core/testlib/basics/appdata.h>
#include <ydb/core/testlib/basics/runtime.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NFq {
namespace {

using namespace NActors;

void CheckDatabaseCreation(bool disableSlsCreating, bool hasRecord, bool databaseExists,
                           ui32 storageError = TIssuesIds::ACCESS_DENIED) {
    const TString scope = "yandexcloud://folder";
    const TString databasePath = "/shared/query_folder";
    ui32 createDatabaseRequests = 0;
    ui32 checkDatabaseRequests = 0;
    ui32 invalidateSynchronizationRequests = 0;
    ui32 createStorageRequests = 0;
    ui32 modifyStorageRequests = 0;
    TActorId requestActor;

    TTestBasicRuntime runtime;
    TAutoPtr<NKikimr::TAppPrepare> app = new NKikimr::TAppPrepare();
    runtime.Initialize(app->Unwrap());

    const auto storage = runtime.AllocateEdgeActor();
    runtime.RegisterService(ControlPlaneStorageServiceActorId(), storage);
    const auto sender = runtime.AllocateEdgeActor();

    NConfig::TComputeConfig config;
    config.MutableYdb()->SetEnable(true);
    auto& controlPlane = *config.MutableYdb()->MutableControlPlane();
    controlPlane.SetEnable(true);
    controlPlane.SetDisableSlsCreating(disableSlsCreating);
    controlPlane.SetDatabasePrefix("query_");
    auto& database = *controlPlane.MutableCms()->MutableDatabaseMapping()->AddCommon();
    database.SetTenant("/shared");
    database.MutableControlPlaneConnection()->SetEndpoint("localhost:2135");
    database.MutableControlPlaneConnection()->SetDatabase("/shared");
    database.MutableExecutionConnection()->SetEndpoint("localhost:2135");

    FederatedQuery::Internal::ComputeDatabaseInternal record;
    record.set_id(databasePath);
    record.mutable_connection()->set_endpoint("localhost:2135");
    record.mutable_connection()->set_database(databasePath);

    // Mock the external CMS; storage responses are supplied explicitly below.
    runtime.SetObserverFunc([&](TAutoPtr<IEventHandle>& event) {
        switch (event->GetTypeRewrite()) {
            case TEvYdbCompute::TEvListDatabasesRequest::EventType: {
                auto response = std::make_unique<TEvYdbCompute::TEvListDatabasesResponse>();
                if (databaseExists) {
                    response->Paths.insert(databasePath);
                }
                runtime.Send(new IEventHandle(event->Sender, event->Recipient, response.release(), 0, event->Cookie));
                return TTestActorRuntime::EEventAction::DROP;
            }
            case TEvYdbCompute::TEvCreateDatabaseRequest::EventType:
                if (event->Sender != sender) {
                    ++createDatabaseRequests;
                    const auto* request = event->Get<TEvYdbCompute::TEvCreateDatabaseRequest>();
                    UNIT_ASSERT_VALUES_EQUAL(request->Path, databasePath);
                    runtime.Send(new IEventHandle(event->Sender, event->Recipient,
                        new TEvYdbCompute::TEvCreateDatabaseResponse(record), 0, event->Cookie));
                    return TTestActorRuntime::EEventAction::DROP;
                }
                break;
            case TEvYdbCompute::TEvCheckDatabaseRequest::EventType:
                ++checkDatabaseRequests;
                break;
            case TEvYdbCompute::TEvInvalidateSynchronizationRequest::EventType:
                if (event->Sender == requestActor) {
                    ++invalidateSynchronizationRequests;
                }
                break;
            case TEvControlPlaneStorage::TEvCreateDatabaseRequest::EventType:
                ++createStorageRequests;
                break;
            case TEvControlPlaneStorage::TEvModifyDatabaseRequest::EventType:
                ++modifyStorageRequests;
                break;
        }
        return TTestActorRuntime::EEventAction::PROCESS;
    });

    const auto service = runtime.Register(CreateComputeDatabaseControlPlaneServiceActor(
        config, NKikimr::CreateYdbCredentialsProviderFactory, {}, {}, {},
        MakeIntrusive<NMonitoring::TDynamicCounters>()).release());
    runtime.Send(new IEventHandle(service, sender, new TEvYdbCompute::TEvCreateDatabaseRequest("cloud", scope)), 0, true);

    const auto describe = runtime.GrabEdgeEventRethrow<TEvControlPlaneStorage::TEvDescribeDatabaseRequest>(storage);
    requestActor = describe->Sender;
    UNIT_ASSERT_VALUES_EQUAL(describe->Get()->Scope, scope);
    if (hasRecord) {
        runtime.Send(new IEventHandle(describe->Sender, storage,
            new TEvControlPlaneStorage::TEvDescribeDatabaseResponse(record)));
    } else {
        NYql::TIssue issue("Database does not exist or permission denied");
        issue.SetCode(storageError, NYql::TSeverityIds::S_ERROR);
        runtime.Send(new IEventHandle(describe->Sender, storage,
            new TEvControlPlaneStorage::TEvDescribeDatabaseResponse(NYql::TIssues{issue})));
    }

    const bool rejected = !hasRecord && (disableSlsCreating || storageError != TIssuesIds::ACCESS_DENIED);
    if (!rejected) {
        if (hasRecord) {
            const auto modify = runtime.GrabEdgeEventRethrow<TEvControlPlaneStorage::TEvModifyDatabaseRequest>(storage);
            UNIT_ASSERT(modify->Get()->LastAccessAt);
            runtime.Send(new IEventHandle(modify->Sender, storage,
                new TEvControlPlaneStorage::TEvModifyDatabaseResponse(), 0, modify->Cookie));
            if (!databaseExists) {
                const auto invalidate = runtime.GrabEdgeEventRethrow<TEvControlPlaneStorage::TEvModifyDatabaseRequest>(storage);
                UNIT_ASSERT(invalidate->Get()->Synchronized.Defined());
                UNIT_ASSERT(!*invalidate->Get()->Synchronized);
                UNIT_ASSERT(invalidate->Get()->WorkloadManagerSynchronized.Defined());
                UNIT_ASSERT(!*invalidate->Get()->WorkloadManagerSynchronized);
                runtime.Send(new IEventHandle(invalidate->Sender, storage,
                    new TEvControlPlaneStorage::TEvModifyDatabaseResponse(), 0, invalidate->Cookie));
            }
        } else {
            const auto create = runtime.GrabEdgeEventRethrow<TEvControlPlaneStorage::TEvCreateDatabaseRequest>(storage);
            UNIT_ASSERT_VALUES_EQUAL(create->Get()->Scope, scope);
            UNIT_ASSERT_VALUES_EQUAL(create->Get()->Request.connection().database(), databasePath);
            UNIT_ASSERT_VALUES_EQUAL(createDatabaseRequests, 1);
            runtime.Send(new IEventHandle(create->Sender, storage,
                new TEvControlPlaneStorage::TEvCreateDatabaseResponse(), 0, create->Cookie));
        }
    }

    const auto response = runtime.GrabEdgeEventRethrow<TEvYdbCompute::TEvCreateDatabaseResponse>(sender);
    if (rejected) {
        UNIT_ASSERT(response->Get()->Issues);
        UNIT_ASSERT_VALUES_EQUAL(response->Get()->Issues.back().IssueCode, storageError);
        if (storageError == TIssuesIds::ACCESS_DENIED) {
            UNIT_ASSERT_STRING_CONTAINS(response->Get()->Issues.ToOneLineString(), "Adding new clients is disabled");
        } else {
            UNIT_ASSERT_STRING_CONTAINS(response->Get()->Issues.ToOneLineString(), "Database does not exist or permission denied");
        }
        UNIT_ASSERT_VALUES_EQUAL(createDatabaseRequests, 0);
        UNIT_ASSERT_VALUES_EQUAL(checkDatabaseRequests, 0);
        UNIT_ASSERT_VALUES_EQUAL(invalidateSynchronizationRequests, 0);
        UNIT_ASSERT_VALUES_EQUAL(createStorageRequests, 0);
        UNIT_ASSERT_VALUES_EQUAL(modifyStorageRequests, 0);
    } else {
        UNIT_ASSERT_C(!response->Get()->Issues, response->Get()->Issues.ToOneLineString());
        UNIT_ASSERT_VALUES_EQUAL(response->Get()->Result.connection().database(), databasePath);
        UNIT_ASSERT_VALUES_EQUAL(createDatabaseRequests, hasRecord && databaseExists ? 0 : 1);
        UNIT_ASSERT_VALUES_EQUAL(checkDatabaseRequests, hasRecord ? 1 : 0);
        UNIT_ASSERT_VALUES_EQUAL(invalidateSynchronizationRequests, hasRecord && databaseExists ? 0 : 1);
        UNIT_ASSERT_VALUES_EQUAL(createStorageRequests, hasRecord ? 0 : 1);
        UNIT_ASSERT_VALUES_EQUAL(modifyStorageRequests, hasRecord ? (databaseExists ? 1 : 2) : 0);
    }
}

} // namespace

Y_UNIT_TEST_SUITE(TComputeDatabaseControlPlane) {
    Y_UNIT_TEST(NewScopeRejectedWhenSlsCreatingDisabled) {
        CheckDatabaseCreation(true, false, false);
    }

    Y_UNIT_TEST(NewScopeCreatedByDefault) {
        NConfig::TYdbComputeControlPlane config;
        UNIT_ASSERT(!config.GetDisableSlsCreating());
        CheckDatabaseCreation(false, false, false);
    }

    Y_UNIT_TEST(ExistingScopeAllowedWhenSlsCreatingDisabled) {
        CheckDatabaseCreation(true, true, true);
    }

    Y_UNIT_TEST(ExistingScopeDatabaseRecreatedWhenSlsCreatingDisabled) {
        CheckDatabaseCreation(true, true, false);
    }

    Y_UNIT_TEST(StorageErrorPreservedWhenSlsCreatingDisabled) {
        CheckDatabaseCreation(true, false, false, TIssuesIds::INTERNAL_ERROR);
    }
}

} // namespace NFq
