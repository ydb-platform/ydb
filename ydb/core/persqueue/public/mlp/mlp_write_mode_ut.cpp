#include <ydb/core/persqueue/public/mlp/ut/common/common.h>

namespace NKikimr::NPQ::NMLP {

namespace {

const TString Consumer = "mlp-consumer";

struct TModeCase {
    TString Mode;
    Ydb::StatusIds::StatusCode Status;
    TString Message;
};

const TModeCase Unavailable{"unavailable", Ydb::StatusIds::UNAVAILABLE, "Write to topic is temporarily disabled"};
const TModeCase Prohibited{"prohibited", Ydb::StatusIds::BAD_REQUEST, "Write to topic is prohibited"};

void SetWriteMode(std::shared_ptr<TTopicSdkTestSetup>& setup, const TString& topic, const TString& mode) {
    auto settings = NYdb::NTopic::TAlterTopicSettings()
        .BeginAlterAttributes()
            .Alter("_write_mode", mode)
        .EndAlterAttributes();
    auto result = AlterTopic(setup, topic, settings);
    UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
}

THolder<TEvWriteResponse> WriteOne(NActors::TTestActorRuntime& runtime, const TString& topic) {
    CreateWriterActor(runtime, {
        .DatabasePath = "/Root",
        .TopicName = topic,
        .Messages = {{
            .Index = 0,
            .MessageBody = "message_body",
        }},
    });
    return GetWriteResponse(runtime);
}

// The describer may still return the previous config right after the alter.
void AssertWriteRejected(NActors::TTestActorRuntime& runtime, const TString& topic, const TModeCase& mode) {
    const auto deadline = TInstant::Now() + TDuration::Seconds(30);
    while (true) {
        auto response = WriteOne(runtime, topic);
        UNIT_ASSERT_VALUES_EQUAL(response->DescribeStatus, NDescriber::EStatus::SUCCESS);
        if (response->Status != Ydb::StatusIds::SUCCESS) {
            UNIT_ASSERT_VALUES_EQUAL(response->Status, mode.Status);
            UNIT_ASSERT_VALUES_EQUAL(response->ErrorDescription, mode.Message);
            UNIT_ASSERT(response->Messages.empty());
            return;
        }
        UNIT_ASSERT_C(TInstant::Now() < deadline, "Writes were not rejected after the write mode change");
        Sleep(TDuration::MilliSeconds(100));
    }
}

void AssertChangeSucceeded(NActors::TTestActorRuntime& runtime) {
    auto response = GetChangeResponse(runtime);
    UNIT_ASSERT_VALUES_EQUAL_C(response->Status, Ydb::StatusIds::SUCCESS, response->ErrorDescription);
    UNIT_ASSERT_VALUES_EQUAL(response->Messages.size(), 1);
    UNIT_ASSERT(response->Messages[0].Status == EOperationResult::Success);
}

void CheckWriteMode(const TModeCase& mode) {
    const TString topic = "/Root/topic1";
    auto setup = CreateSetup();
    CreateTopic(setup, topic, Consumer);
    setup->Write(topic, "msg-1", 0);
    setup->Write(topic, "msg-2", 0);
    Sleep(TDuration::Seconds(2));

    auto& runtime = setup->GetRuntime();
    auto read = [&](ui32 count) {
        CreateReaderActor(runtime, {
            .DatabasePath = "/Root",
            .TopicName = topic,
            .Consumer = Consumer,
            .WaitTime = TDuration::Seconds(1),
            .ProcessingTimeout = TDuration::Seconds(30),
            .MaxNumberOfMessage = count,
        });
        return GetReadResponse(runtime);
    };
    auto locked = read(2);
    UNIT_ASSERT_VALUES_EQUAL_C(locked->Status, Ydb::StatusIds::SUCCESS, locked->ErrorDescription);
    UNIT_ASSERT_VALUES_EQUAL(locked->Messages.size(), 2);
    const auto first = locked->Messages[0].MessageId;
    const auto second = locked->Messages[1].MessageId;

    SetWriteMode(setup, topic, mode.Mode);
    AssertWriteRejected(runtime, topic, mode);

    // The tablet rejects reads in this mode, so MLP must fail them before calling it.
    auto rejected = read(1);
    UNIT_ASSERT_VALUES_EQUAL(rejected->Status, mode.Status);
    UNIT_ASSERT_VALUES_EQUAL(rejected->ErrorDescription, mode.Message);
    UNIT_ASSERT(rejected->Messages.empty());

    CreateMessageDeadlineChangerActor(runtime, {
        .DatabasePath = "/Root",
        .TopicName = topic,
        .Consumer = Consumer,
        .Messages = {second},
        .Deadlines = {TInstant::Now() + TDuration::Seconds(60)},
    });
    AssertChangeSucceeded(runtime);

    CreateUnlockerActor(runtime, {
        .DatabasePath = "/Root",
        .TopicName = topic,
        .Consumer = Consumer,
        .Messages = {second},
    });
    AssertChangeSucceeded(runtime);

    CreateCommitterActor(runtime, {
        .DatabasePath = "/Root",
        .TopicName = topic,
        .Consumer = Consumer,
        .Messages = {first},
    });
    AssertChangeSucceeded(runtime);

    CreatePurgerActor(runtime, {
        .DatabasePath = "/Root",
        .TopicName = topic,
        .Consumer = Consumer,
    });
    AssertPurgeOK(runtime);

    SetWriteMode(setup, topic, "available");
    const auto deadline = TInstant::Now() + TDuration::Seconds(30);
    while (true) {
        auto response = WriteOne(runtime, topic);
        if (response->Status == Ydb::StatusIds::SUCCESS) {
            UNIT_ASSERT_VALUES_EQUAL(response->Messages.size(), 1);
            UNIT_ASSERT(response->Messages[0].MessageId.has_value());
            break;
        }
        UNIT_ASSERT_C(TInstant::Now() < deadline, "Writes were not allowed after the write mode change");
        Sleep(TDuration::MilliSeconds(100));
    }
    auto reopened = read(1);
    UNIT_ASSERT_VALUES_EQUAL_C(reopened->Status, Ydb::StatusIds::SUCCESS, reopened->ErrorDescription);
}

} // namespace

Y_UNIT_TEST_SUITE(TMLPWriteModeTests) {

    Y_UNIT_TEST(Unavailable) {
        CheckWriteMode(Unavailable);
    }

    Y_UNIT_TEST(Prohibited) {
        CheckWriteMode(Prohibited);
    }

}

} // namespace NKikimr::NPQ::NMLP
