#include <ydb/core/tx/schemeshard/ut_helpers/helpers.h>

#include <ydb/core/audit/audit_log_service.h>
#include <ydb/core/audit/audit_log.h>

#include <library/cpp/json/json_reader.h>
#include <library/cpp/json/json_value.h>
#include <library/cpp/logger/backend.h>
#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/threading/blocking_queue/blocking_queue.h>

using namespace NKikimr;
using namespace NSchemeShard;
using namespace NSchemeShardUT_Private;

namespace {

using TLogQueue = NThreading::TBlockingQueue<TString>;
using TLogQueuePtr = std::shared_ptr<TLogQueue>;

class TTestLogBackend : public TLogBackend {
public:
    explicit TTestLogBackend(TLogQueuePtr queue)
        : Queue(std::move(queue))
    {}

    void WriteData(const TLogRecord& rec) override {
        Queue->Push(TString(rec.Data, rec.Len));
    }

    void ReopenLog() override {}

private:
    TLogQueuePtr Queue;
};

void AddTopicCloudEventsAuditService(TTestActorRuntime& runtime, TLogQueuePtr logQueue) {
    NAudit::TAuditLogBackends backends;
    backends[NKikimrConfig::TAuditConfig::JSON].emplace_back(MakeHolder<TTestLogBackend>(logQueue));

    auto auditActor = NAudit::CreateAuditWriter(std::move(backends));
    TActorId auditActorId = runtime.Register(auditActor.release(), 0);

    runtime.RegisterService(NAudit::MakeTopicCloudEventsAuditServiceID(), auditActorId, 0);
}

static NJson::TJsonValue ParseCloudEventFromAuditLog(const TString& log) {
    size_t jsonStart = log.find(": ");
    UNIT_ASSERT_C(jsonStart != TString::npos, "Log must contain timestamp prefix: " << log);
    TStringBuf jsonPart(log.data() + jsonStart + 2, log.size() - jsonStart - 2);

    NJson::TJsonValue root;
    UNIT_ASSERT_C(NJson::ReadJsonTree(jsonPart, &root), "Failed to parse audit log JSON: " << log);

    const auto* cloudEventJson = root.GetValueByPath("cloud_event_json");
    UNIT_ASSERT_C(cloudEventJson != nullptr, "Missing cloud_event_json in audit log");
    TString innerJson = cloudEventJson->GetString();

    NJson::TJsonValue cloudEvent;
    UNIT_ASSERT_C(NJson::ReadJsonTree(innerJson, &cloudEvent), "Failed to parse cloud_event_json: " << innerJson);
    return cloudEvent;
}

static void AssertCloudEventFields(const NJson::TJsonValue& cloudEvent,
                                   const TString& expectedEventType,
                                   const TString& expectedPath) {
    const auto* eventMetadata = cloudEvent.GetValueByPath("event_metadata");
    UNIT_ASSERT_C(eventMetadata != nullptr, "Missing event_metadata");
    UNIT_ASSERT_STRINGS_EQUAL((*eventMetadata)["event_type"].GetString(), expectedEventType);

    const auto* details = cloudEvent.GetValueByPath("details");
    UNIT_ASSERT_C(details != nullptr, "Missing details");
    UNIT_ASSERT_STRINGS_EQUAL((*details)["path"].GetString(), expectedPath);
}

} // anonymous namespace

Y_UNIT_TEST_SUITE(TSchemeShardTopicCloudEvents) {

    Y_UNIT_TEST(CreateTopicCloudEvent) {
        TTestBasicRuntime runtime;
        auto logQueue = std::make_shared<TLogQueue>(0);

        TTestEnv env(runtime);
        AddTopicCloudEventsAuditService(runtime, logQueue);
        ui64 txId = 1000;

        TestCreatePQGroup(runtime, ++txId, "/MyRoot",
            R"(
                Name: "MyTopic"
                TotalGroupCount: 4
                PartitionPerTablet: 2
                PQTabletConfig { PartitionConfig { LifetimeSeconds: 10 } }
            )");
        env.TestWaitNotification(runtime, txId);

        runtime.DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));

        auto log = logQueue->Pop(TDuration::Seconds(5));
        UNIT_ASSERT_C(log.Defined(), "Expected cloud event audit log");

        auto cloudEvent = ParseCloudEventFromAuditLog(*log);
        AssertCloudEventFields(cloudEvent,
            "yandex.cloud.events.ydb.topics.CreateTopic",
            "/MyRoot/MyTopic");
    }

    Y_UNIT_TEST(AlterTopicCloudEvent) {
        TTestBasicRuntime runtime;
        auto logQueue = std::make_shared<TLogQueue>(0);

        TTestEnv env(runtime);
        AddTopicCloudEventsAuditService(runtime, logQueue);
        ui64 txId = 1000;

        TestCreatePQGroup(runtime, ++txId, "/MyRoot",
            R"(
                Name: "MyTopic"
                TotalGroupCount: 4
                PartitionPerTablet: 2
                PQTabletConfig { PartitionConfig { LifetimeSeconds: 10 } }
            )");
        env.TestWaitNotification(runtime, txId);

        TestAlterPQGroup(runtime, ++txId, "/MyRoot",
            R"(
                Name: "MyTopic"
                TotalGroupCount: 6
                PartitionPerTablet: 2
                PQTabletConfig { PartitionConfig { LifetimeSeconds: 42 } }
            )");
        env.TestWaitNotification(runtime, txId);

        runtime.DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));

        auto createLog = logQueue->Pop(TDuration::Seconds(1));
        auto alterLog = logQueue->Pop(TDuration::Seconds(5));
        UNIT_ASSERT_C(createLog.Defined(), "Expected CreateTopic cloud event");
        UNIT_ASSERT_C(alterLog.Defined(), "Expected AlterTopic cloud event");

        auto alterCloudEvent = ParseCloudEventFromAuditLog(*alterLog);
        AssertCloudEventFields(alterCloudEvent,
            "yandex.cloud.events.ydb.topics.AlterTopic",
            "/MyRoot/MyTopic");
    }
}
