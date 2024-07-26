#include <ydb/core/fq/libs/ydb/ydb.h>
#include <ydb/core/fq/libs/events/events.h>
#include <ydb/core/fq/libs/row_dispatcher/topic_session.h>
#include <ydb/core/testlib/actors/test_runtime.h>
#include <ydb/core/testlib/basics/helpers.h>
#include <ydb/core/testlib/actor_helpers.h>
#include <library/cpp/testing/unittest/registar.h>
#include <ydb/tests/fq/pq_async_io/ut_helpers.h>
namespace {

using namespace NKikimr;
using namespace NFq;
using namespace NYql::NDq;

class TFixture : public NUnitTest::TBaseFixture {

public:
    TFixture()
    : Runtime(true) {}

    void SetUp(NUnitTest::TTestContext&) override {
        TAutoPtr<TAppPrepare> app = new TAppPrepare();
        Runtime.Initialize(app->Unwrap());
        Runtime.SetLogPriority(NKikimrServices::YQ_ROW_DISPATCHER, NLog::PRI_DEBUG);

        NYql::ISecuredServiceAccountCredentialsFactory::TPtr credentialsFactory;
    }

    void Init(const TString& topic) {
        Settings = BuildPqTopicSourceSettings(topic);

        TopicSession = Runtime.Register(NewTopicSession(
            RowDispatcherActorId,
            Settings,
            0,
            Driver,
            CredentialsProviderFactory
            ).release());
        Runtime.EnableScheduleForActor(TopicSession);
    }

    void TearDown(NUnitTest::TTestContext& /* context */) override {
    }

    TActorSystemStub actorSystemStub;
    NActors::TTestActorRuntime Runtime;
    NActors::TActorId TopicSession;
    NActors::TActorId RowDispatcherActorId;
    NYdb::TDriver Driver = NYdb::TDriver(NYdb::TDriverConfig().SetLog(CreateLogBackend("cerr")));
    std::shared_ptr<NYdb::ICredentialsProviderFactory> CredentialsProviderFactory;
    NYql::NPq::NProto::TDqPqTopicSource Settings;
};

Y_UNIT_TEST_SUITE(TopicSessionTests) {
    Y_UNIT_TEST_F(Simple1, TFixture) {
        const TString topicName = "ReadFromTopic";
        PQCreateStream(topicName);

        Init(topicName);
        const std::vector<TString> data = { "1" };
        PQWrite(data, topicName);


    }

}

}

