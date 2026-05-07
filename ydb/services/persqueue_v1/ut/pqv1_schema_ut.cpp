#include <ydb/services/lib/actors/pq_schema_actor.h>

#include <ydb/core/testlib/actors/test_runtime.h>
#include <ydb/core/testlib/basics/appdata.h>
#include <ydb/core/testlib/basics/runtime.h>
#include <ydb/public/api/protos/ydb_persqueue_v1.pb.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NPersQueueTests {

using namespace NActors;
using namespace NKikimr::NGRpcProxy::V1;

namespace {

struct TFillProposeRunner : TActorBootstrapped<TFillProposeRunner> {
    void Bootstrap(const TActorContext& ctx) {
        Ydb::PersQueue::V1::CreateTopicRequest pqRequest;
        pqRequest.set_path("/Root/PQ/my-topic");

        auto* settings = pqRequest.mutable_settings();
        settings->set_partitions_count(2);
        settings->set_retention_period_ms(3'600'000);
        settings->set_supported_format(Ydb::PersQueue::V1::TopicSettings::FORMAT_BASE);
        settings->add_supported_codecs(Ydb::PersQueue::V1::CODEC_RAW);

        auto* rr = settings->add_read_rules();
        rr->set_consumer_name("shared_consumer_ut");
        rr->set_supported_format(Ydb::PersQueue::V1::TopicSettings::FORMAT_BASE);
        rr->add_supported_codecs(Ydb::PersQueue::V1::CODEC_RAW);
        rr->set_version(1);
        rr->set_starting_message_timestamp_ms(0);

        auto* sct = rr->mutable_shared_consumer_type();
        sct->set_keep_messages_order(true);
        sct->mutable_default_processing_timeout()->set_seconds(42);
        sct->mutable_default_processing_timeout()->set_nanos(0);

        auto* dlp = sct->mutable_dead_letter_policy();
        dlp->set_enabled(true);
        dlp->mutable_condition()->set_max_processing_attempts(7);
        dlp->mutable_move_action()->set_dead_letter_queue("/Root/PQ/dlq");

        NKikimrSchemeOp::TModifyScheme modifyScheme;
        modifyScheme.SetWorkingDir("/Root/PQ");

        TString error;
        const auto status = FillProposeRequestImpl(
            "my-topic",
            pqRequest.settings(),
            modifyScheme,
            ctx,
            false,
            error,
            "/Root/PQ",
            "",
            "");

        UNIT_ASSERT_C(status == Ydb::StatusIds::SUCCESS, error);
        UNIT_ASSERT_C(error.empty(), error);

        const auto& pqDescr = modifyScheme.GetCreatePersQueueGroup();
        const auto& config = pqDescr.GetPQTabletConfig();
        UNIT_ASSERT_VALUES_EQUAL(config.ConsumersSize(), 1);

        const auto& c = config.GetConsumers(0);
        UNIT_ASSERT_VALUES_EQUAL(c.GetName(), "shared_consumer_ut");
        UNIT_ASSERT_VALUES_EQUAL(NKikimrPQ::TPQTabletConfig::EConsumerType_Name(c.GetType()),
            NKikimrPQ::TPQTabletConfig::EConsumerType_Name(NKikimrPQ::TPQTabletConfig::CONSUMER_TYPE_MLP));
        UNIT_ASSERT(c.GetKeepMessageOrder());
        UNIT_ASSERT_VALUES_EQUAL(c.GetDefaultProcessingTimeoutSeconds(), 42u);
        UNIT_ASSERT(c.GetDeadLetterPolicyEnabled());
        UNIT_ASSERT_VALUES_EQUAL(c.GetMaxProcessingAttempts(), 7u);
        UNIT_ASSERT_VALUES_EQUAL(NKikimrPQ::TPQTabletConfig::EDeadLetterPolicy_Name(c.GetDeadLetterPolicy()),
            NKikimrPQ::TPQTabletConfig::EDeadLetterPolicy_Name(NKikimrPQ::TPQTabletConfig::DEAD_LETTER_POLICY_MOVE));
        UNIT_ASSERT_VALUES_EQUAL(c.GetDeadLetterQueue(), "/Root/PQ/dlq");
        UNIT_ASSERT_VALUES_EQUAL(c.GetReadFromTimestampsMs(), 0u);
        UNIT_ASSERT_VALUES_EQUAL(c.GetVersion(), 1u);

        PassAway();
    }
};

} // namespace

Y_UNIT_TEST_SUITE(PQv1Schema) {

Y_UNIT_TEST(SharedConsumer) {
    TTestBasicRuntime runtime(1, false);

    NKikimr::TAppPrepare app;
    app.PQConfig.SetTopicsAreFirstClassCitizen(true);
    app.SetEnableTopicMessageLevelParallelism(std::optional<bool>(true));

    runtime.Initialize(app.Unwrap());

    const TActorId actorId = runtime.Register(new TFillProposeRunner(), 0);
    runtime.EnableScheduleForActor(actorId, true);

    TDispatchOptions options;
    options.CustomFinalCondition = [&]() {
        return runtime.FindActor(actorId) == nullptr;
    };
    UNIT_ASSERT(runtime.DispatchEvents(options));
}

} // Y_UNIT_TEST_SUITE(PQv1Schema)

} // namespace NKikimr::NPersQueueTests
