#include <ydb/core/base/appdata.h>
#include <ydb/core/testlib/basics/appdata.h>
#include <ydb/core/testlib/basics/runtime.h>

#include <ydb/services/workload_manager/actors/workload_manager_state_actor.h>
#include <ydb/services/workload_manager/gateway.h>
#include <ydb/services/workload_manager/gateway_internal.h>
#include <ydb/services/workload_manager/ut/common/query_classifier_ut_common.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NWorkloadManager {

namespace {

struct TGatewayFixture: public NUnitTest::TBaseFixture {
    TTestBasicRuntime Runtime{ 1 };
    std::shared_ptr<NPrivate::TWorkloadManagerGateway> Gateway = std::make_shared<NPrivate::TWorkloadManagerGateway>();
    TActorId StateActorEdge;

    void SetUp(NUnitTest::TTestContext&) override {
        TAppPrepare app;
        app.SetEnableResourcePools(true);
        Runtime.Initialize(app.Unwrap());
        Runtime.GetAppData().WorkloadManagerGateway = Gateway;
        StateActorEdge = Runtime.AllocateEdgeActor();
    }

    void Publish(bool enableResourcePools = true, bool enableResourcePoolsOnServerless = false) {
        auto* snapshot = new NPrivate::TSnapshot();
        snapshot->StateActorId = StateActorEdge;
        snapshot->EnableResourcePools = enableResourcePools;
        snapshot->EnableResourcePoolsOnServerless = enableResourcePoolsOnServerless;
        Gateway->PublishSnapshot(NPrivate::TSnapshotPtr(snapshot));
    }

    static TClassifyContext MakeContext() {
        return TClassifyContext{
            .PoolId = "",
            .AppName = "",
            .UserToken = nullptr,
        };
    }

    std::shared_ptr<IQueryClassifier> TryCreateQueryClassifier(const TString& databaseId) {
        return Runtime.RunCall([databaseId] {
            return AppData()->WorkloadManagerGateway->TryCreateQueryClassifier(databaseId, MakeContext());
        });
    }
};

}   // namespace

Y_UNIT_TEST_SUITE(WorkloadManagerGateway) {
    // No snapshot published yet: TryCreateQueryClassifier returns nullptr.
    Y_UNIT_TEST_F(TestTryCreateQueryClassifierNullBeforeSnapshot, TGatewayFixture) {
        UNIT_ASSERT(!TryCreateQueryClassifier(TEST_DB));
    }

    // State actor registered, Bootstrap not processed yet. The published snapshot:
    // - carries the state actor id,
    // - has resource pools enabled from AppData.
    Y_UNIT_TEST_F(TestSnapshotPublishedOnRegister, TGatewayFixture) {
        const TActorId stateActor = Runtime.Register(CreateWorkloadManagerStateActor(Gateway));

        const auto snapshot = Gateway->GetSnapshot();
        UNIT_ASSERT(snapshot);
        UNIT_ASSERT_EQUAL(snapshot->StateActorId, stateActor);
        UNIT_ASSERT(snapshot->EnableResourcePools);
        UNIT_ASSERT(!snapshot->Classifiers);
    }

    // Resource pools disabled: TryCreateQueryClassifier returns nullptr.
    Y_UNIT_TEST_F(TestTryCreateQueryClassifierNullWhenPoolsDisabled, TGatewayFixture) {
        Publish(/*enableResourcePools=*/false);
        UNIT_ASSERT(!TryCreateQueryClassifier(TEST_DB));
    }

    // Empty database id: TryCreateQueryClassifier returns nullptr.
    Y_UNIT_TEST_F(TestTryCreateQueryClassifierNullForEmptyDatabaseId, TGatewayFixture) {
        Publish();
        UNIT_ASSERT(!TryCreateQueryClassifier(""));
    }

    // Serverless gate by database id. TryCreateQueryClassifier:
    // - returns nullptr for a serverless id while resource pools on serverless are off,
    // - returns a classifier for a dedicated id,
    // - returns a classifier for a serverless id once resource pools on serverless are on.
    Y_UNIT_TEST_F(TestTryCreateQueryClassifierServerlessGate, TGatewayFixture) {
        const TString serverlessId = "72075186224037888:5:/Root/serverless";

        Publish(/*enableResourcePools=*/true, /*enableResourcePoolsOnServerless=*/false);
        UNIT_ASSERT(!TryCreateQueryClassifier(serverlessId));
        UNIT_ASSERT(TryCreateQueryClassifier("/Root/dedicated"));

        Publish(/*enableResourcePools=*/true, /*enableResourcePoolsOnServerless=*/true);
        UNIT_ASSERT(TryCreateQueryClassifier(serverlessId));
    }
}

}   // namespace NKikimr::NWorkloadManager
