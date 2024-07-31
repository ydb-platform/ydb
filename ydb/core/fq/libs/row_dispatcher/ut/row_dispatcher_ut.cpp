#include <ydb/core/fq/libs/ydb/ydb.h>
#include <ydb/core/fq/libs/events/events.h>
#include <ydb/core/fq/libs/row_dispatcher/row_dispatcher.h>
#include <ydb/core/testlib/actors/test_runtime.h>
#include <ydb/core/testlib/basics/helpers.h>
#include <ydb/core/testlib/actor_helpers.h>
#include <library/cpp/testing/unittest/registar.h>

namespace {

using namespace NKikimr;
using namespace NFq;

class TFixture : public NUnitTest::TBaseFixture {

public:
    TFixture()
    : Runtime(true) {}

    void SetUp(NUnitTest::TTestContext&) override {
        TAutoPtr<TAppPrepare> app = new TAppPrepare();
        Runtime.Initialize(app->Unwrap());
        Runtime.SetLogPriority(NKikimrServices::YQ_ROW_DISPATCHER, NLog::PRI_DEBUG);
        NConfig::TRowDispatcherConfig config;
        NConfig::TCommonConfig commonConfig;
        NKikimr::TYdbCredentialsProviderFactory credentialsProviderFactory;
        TYqSharedResources::TPtr yqSharedResources;
        NYql::ISecuredServiceAccountCredentialsFactory::TPtr credentialsFactory;

        RowDispatcher = Runtime.Register(NewRowDispatcher(
            config,
            commonConfig,
            credentialsProviderFactory,
            yqSharedResources,
            credentialsFactory,
            "Tenant"
            ).release());

        Runtime.EnableScheduleForActor(RowDispatcher);
    }

    void TearDown(NUnitTest::TTestContext& /* context */) override {
    }

    TActorSystemStub actorSystemStub;
    NActors::TTestActorRuntime Runtime;
    NActors::TActorId RowDispatcher;
};

Y_UNIT_TEST_SUITE(RowDispatcherTests) {
    Y_UNIT_TEST_F(Simple1, TFixture) {

    }

}

}

