#include <ydb/core/persqueue/public/cloud_events/actor.h>

#include <ydb/core/audit/audit_log_service.h>
#include <ydb/core/audit/audit_log.h>

#include <ydb/core/base/events.h>
#include <ydb/core/protos/flat_scheme_op.pb.h>
#include <ydb/core/protos/schemeshard/operations.pb.h>
#include <ydb/core/protos/flat_tx_scheme.pb.h>

#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/testlib/test_runtime.h>

#include <library/cpp/json/json_reader.h>
#include <library/cpp/json/json_value.h>
#include <library/cpp/logger/backend.h>
#include <library/cpp/logger/stream.h>
#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/string.h>

#include <queue>
#include <mutex>
#include <condition_variable>

namespace NKikimr::NPQ::NCloudEvents {

template <typename T>
class TWaitableQueue {
public:
    void Push(const T& value) {
        {
            std::lock_guard<std::mutex> lock(Mutex);
            Queue.push(value);
        }
        CondVar.notify_one();
    }

    T Pop() {
        std::unique_lock<std::mutex> lock(Mutex);
        CondVar.wait(lock, [this] { return !Queue.empty(); });
        T value = Queue.front();
        Queue.pop();
        return value;
    }

    bool Empty() const {
        std::lock_guard<std::mutex> lock(Mutex);
        return Queue.empty();
    }

private:
    mutable std::mutex Mutex;
    std::condition_variable CondVar;
    std::queue<T> Queue;
};

using TLogQueue = TWaitableQueue<TString>;
using TLogQueuePtr = std::shared_ptr<TWaitableQueue<TString>>;

class TTestLogBackend : public TLogBackend {
public:
    explicit TTestLogBackend(TLogQueuePtr queue)
        : Queue(std::move(queue))
    {
    }

    void WriteData(const TLogRecord& rec) override {
        Queue->Push(TString(rec.Data, rec.Len));
    }

    void ReopenLog() override {
    }

private:
    TLogQueuePtr Queue;
};

struct TTestCloudEventsActorSystem : public NActors::TTestActorRuntimeBase {
    TTestCloudEventsActorSystem()
        : TTestActorRuntimeBase(1, true)
    {
    }

    void Init(NAudit::TAuditLogBackends&& backends) {
        AddLocalService(
            NAudit::MakeTopicCloudEventsAuditServiceID(),
            NActors::TActorSetupCmd(
                NAudit::CreateAuditWriter(std::move(backends)),
                NActors::TMailboxType::Simple,
                0
            )
        );
        InitNodes();
        SetLogBackend(new TStreamLogBackend(&Cerr));
        AppendToLogSettings(
            NKikimrServices::EServiceKikimr_MIN,
            NKikimrServices::EServiceKikimr_MAX,
            NKikimrServices::EServiceKikimr_Name<NActors::NLog::EComponent>
        );
    }
};

class TTestCloudEventsAuditService {
public:
    explicit TTestCloudEventsAuditService(NKikimrConfig::TAuditConfig::EFormat format) {
        NAudit::TAuditLogBackends backends;
        backends[format].emplace_back(MakeHolder<TTestLogBackend>(LogQueue));
        Runtime.Init(std::move(backends));
    }

    NActors::TActorId RegisterCloudEventsActor() {
        return Runtime.Register(new TCloudEventsActor());
    }

    void SendCloudEvent(NActors::TActorId cloudEventsActorId, TCloudEventInfo&& info) {
        Runtime.SingleSys()->Send(
            cloudEventsActorId,
            new TCloudEvent(std::move(info))
        );
    }

    TString WaitAuditLog() {
        return LogQueue->Pop();
    }

    NActors::TTestActorRuntimeBase& GetRuntime() {
        return Runtime;
    }

private:
    TLogQueuePtr LogQueue = std::make_shared<TLogQueue>();
    TTestCloudEventsActorSystem Runtime;
};

static TCloudEventInfo MakeCreateTopicEventInfo(const TString& topicPath = "/root/db/topic1") {
    NKikimrSchemeOp::TModifyScheme modifyScheme;
    modifyScheme.SetOperationType(NKikimrSchemeOp::ESchemeOpCreatePersQueueGroup);
    modifyScheme.MutableCreatePersQueueGroup();

    TCloudEventInfo info;
    info.CloudId = "cloud1";
    info.FolderId = "folder1";
    info.TopicPath = topicPath;
    info.Issue = "";
    info.MaskedToken = "***";
    info.UserSID = "user@iam";
    info.RemoteAddress = "127.0.0.1";
    info.CreatedAt = TInstant::Now();
    info.ModifyScheme = std::move(modifyScheme);
    info.OperationStatus = NKikimrScheme::StatusSuccess;
    return info;
}

static NJson::TJsonValue ParseAuditLogJson(const TString& log) {
    size_t jsonStart = log.find(": ");
    UNIT_ASSERT_C(jsonStart != TString::npos, "Log must contain timestamp prefix: " << log);
    TStringBuf jsonPart(log.data() + jsonStart + 2, log.size() - jsonStart - 2);
    NJson::TJsonValue root;
    UNIT_ASSERT_C(NJson::ReadJsonTree(jsonPart, &root), "Failed to parse audit log JSON: " << log);
    return root;
}

static NJson::TJsonValue ParseCloudEventFromAuditLog(const TString& log) {
    NJson::TJsonValue root = ParseAuditLogJson(log);
    const auto* cloudEventJson = root.GetValueByPath("cloud_event_json");
    UNIT_ASSERT_C(cloudEventJson != nullptr, "Missing cloud_event_json in audit log");
    TString innerJson = cloudEventJson->GetString();
    NJson::TJsonValue cloudEvent;
    UNIT_ASSERT_C(NJson::ReadJsonTree(innerJson, &cloudEvent), "Failed to parse cloud_event_json: " << innerJson);
    return cloudEvent;
}

static void AssertCloudEventJsonStructure(const NJson::TJsonValue& cloudEvent, const TString& expectedEventType, const TString& expectedPath) {
    const auto* eventMetadata = cloudEvent.GetValueByPath("event_metadata");
    UNIT_ASSERT_C(eventMetadata != nullptr, "Missing event_metadata");
    UNIT_ASSERT_STRINGS_EQUAL((*eventMetadata)["event_type"].GetString(), expectedEventType);

    const auto* details = cloudEvent.GetValueByPath("details");
    UNIT_ASSERT_C(details != nullptr, "Missing details");
    UNIT_ASSERT_STRINGS_EQUAL((*details)["path"].GetString(), expectedPath);

    const auto* auth = cloudEvent.GetValueByPath("authentication");
    UNIT_ASSERT_C(auth != nullptr, "Missing authentication");
    UNIT_ASSERT_STRINGS_EQUAL((*auth)["subject_id"].GetString(), "user@iam");

    const auto* evMetadata = cloudEvent.GetValueByPath("event_metadata");
    UNIT_ASSERT_C(evMetadata != nullptr, "Missing event_metadata");
    UNIT_ASSERT(evMetadata->GetMap().find("cloud_id") != evMetadata->GetMap().end());
    UNIT_ASSERT(evMetadata->GetMap().find("folder_id") != evMetadata->GetMap().end());
}

static TCloudEventInfo MakeDeleteTopicEventInfo(const TString& topicPath = "/root/db/topic1") {
    NKikimrSchemeOp::TModifyScheme modifyScheme;
    modifyScheme.SetOperationType(NKikimrSchemeOp::ESchemeOpDropPersQueueGroup);
    modifyScheme.MutableDeallocatePersQueueGroup()->SetName(topicPath);

    TCloudEventInfo info;
    info.CloudId = "cloud1";
    info.FolderId = "folder1";
    info.TopicPath = topicPath;
    info.Issue = "";
    info.MaskedToken = "***";
    info.UserSID = "user@iam";
    info.RemoteAddress = "127.0.0.1";
    info.CreatedAt = TInstant::Now();
    info.ModifyScheme = std::move(modifyScheme);
    info.OperationStatus = NKikimrScheme::StatusSuccess;
    return info;
}

Y_UNIT_TEST_SUITE(CloudEventsAuditTest) {
    Y_UNIT_TEST(CreateTopicEventAudit) {
        TTestCloudEventsAuditService test(NKikimrConfig::TAuditConfig::JSON);

        auto cloudEventsActorId = test.RegisterCloudEventsActor();
        test.GetRuntime().DispatchEvents();

        test.SendCloudEvent(cloudEventsActorId, MakeCreateTopicEventInfo("/root/my/topic"));
        test.GetRuntime().DispatchEvents();

        TString log = test.WaitAuditLog();
        NJson::TJsonValue cloudEvent = ParseCloudEventFromAuditLog(log);
        AssertCloudEventJsonStructure(cloudEvent, "yandex.cloud.events.ydb.topics.CreateTopic", "/root/my/topic");
    }

    Y_UNIT_TEST(DeleteTopicEventAudit) {
        TTestCloudEventsAuditService test(NKikimrConfig::TAuditConfig::TXT);

        auto cloudEventsActorId = test.RegisterCloudEventsActor();
        test.GetRuntime().DispatchEvents();

        test.SendCloudEvent(cloudEventsActorId, MakeDeleteTopicEventInfo("/root/my/deleted_topic"));
        test.GetRuntime().DispatchEvents();

        TString log = test.WaitAuditLog();
        UNIT_ASSERT_STRING_CONTAINS(log, "cloud_event_json");
        UNIT_ASSERT_STRING_CONTAINS(log, "DeleteTopic");
        UNIT_ASSERT_STRING_CONTAINS(log, "/root/my/deleted_topic");
    }

    Y_UNIT_TEST(CloudEventJsonFormat) {
        TTestCloudEventsAuditService test(NKikimrConfig::TAuditConfig::JSON);

        auto cloudEventsActorId = test.RegisterCloudEventsActor();
        test.GetRuntime().DispatchEvents();

        test.SendCloudEvent(cloudEventsActorId, MakeCreateTopicEventInfo());
        test.GetRuntime().DispatchEvents();

        TString log = test.WaitAuditLog();
        NJson::TJsonValue cloudEvent = ParseCloudEventFromAuditLog(log);
        AssertCloudEventJsonStructure(cloudEvent, "yandex.cloud.events.ydb.topics.CreateTopic", "/root/db/topic1");

        const auto* requestParams = cloudEvent.GetValueByPath("request_parameters");
        UNIT_ASSERT_C(requestParams != nullptr, "Missing request_parameters");
        UNIT_ASSERT_STRINGS_EQUAL((*requestParams)["path"].GetString(), "/root/db/topic1");

        UNIT_ASSERT(cloudEvent.GetMap().find("event_status") != cloudEvent.GetMap().end());
        UNIT_ASSERT_STRINGS_EQUAL(cloudEvent["event_status"].GetString(), "DONE");
    }
}

} // namespace NKikimr::NPQ::NCloudEvents
