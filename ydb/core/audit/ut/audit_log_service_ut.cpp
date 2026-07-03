#include <ydb/core/audit/audit_log_service.h>
#include <ydb/core/audit/audit_log.h>

#include <ydb/core/audit/audit_config/audit_config.h>
#include <ydb/core/audit/heartbeat_actor/heartbeat_actor.h>

#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/testlib/test_runtime.h>

#include <library/cpp/logger/stream.h>
#include <library/cpp/testing/unittest/registar.h>

#include <queue>
#include <mutex>
#include <condition_variable>

namespace NKikimr::NAudit {

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
    TTestLogBackend(TLogQueuePtr queue)
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

class TExceptionLogBackend : public TLogBackend {
public:
    void WriteData(const TLogRecord&) override {
        throw std::runtime_error("test");
    }

    void ReopenLog() override {
    }
};

struct TTestAuditLogActorSystem : public NActors::TTestActorRuntimeBase {
    TTestAuditLogActorSystem()
        : TTestActorRuntimeBase(1, true)
    {
    }

    void Init(TAuditLogBackends&& backends) {
        AddLocalService(MakeAuditServiceID(), NActors::TActorSetupCmd(CreateAuditWriter(std::move(backends)), NActors::TMailboxType::Simple, 0));
        InitNodes();
        SetLogBackend(new TStreamLogBackend(&Cerr));
        AppendToLogSettings(
            NKikimrServices::EServiceKikimr_MIN,
            NKikimrServices::EServiceKikimr_MAX,
            NKikimrServices::EServiceKikimr_Name<NActors::NLog::EComponent>
        );
        SetLogPriority(NKikimrServices::AUDIT_LOG_WRITER, NActors::NLog::PRI_DEBUG);
    }
};

class TTestAuditLogService {
public:
    explicit TTestAuditLogService(TAuditLogBackends&& backends) {
        Init(std::move(backends));
    }

    explicit TTestAuditLogService(NKikimrConfig::TAuditConfig::EFormat format) {
        Init(MakeBackends(format));
    }

    void Init(TAuditLogBackends&& backends) {
        Runtime.Init(std::move(backends));
    }

    TAuditLogBackends MakeBackends(NKikimrConfig::TAuditConfig::EFormat format) {
        TAuditLogBackends backends;
        backends[format].emplace_back(MakeHolder<TTestLogBackend>(LogQueue));
        backends[format].emplace_back(MakeHolder<TExceptionLogBackend>());
        return backends;
    }

    TString SendAuditLog(TAuditLogParts&& parts) { // Send log and wait for result
        NAudit::SendAuditLog(Runtime.SingleSys(), std::move(parts));
        return WaitAuditLog();
    }

    TString WaitAuditLog() {
        return LogQueue->Pop();
    }

    void MakeHeartbeatActor(const TAuditConfig& auditConfig) {
        auto actor = CreateHeartbeatActor(auditConfig);
        Runtime.Register(actor.release());
    }

private:
    TLogQueuePtr LogQueue = std::make_shared<TLogQueue>();
    TTestAuditLogActorSystem Runtime;
};

Y_UNIT_TEST_SUITE(AuditLogWriterServiceTest) {
    Y_UNIT_TEST(LoggingTxt) {
        TTestAuditLogService test(NKikimrConfig::TAuditConfig::TXT);

        TAuditLogParts parts = {
            {"name", "value"},
            {"fe", "\xfe\xfe"},
        };

        const TString result = test.SendAuditLog(std::move(parts));
        UNIT_ASSERT_STRING_CONTAINS(result, "fe=FEFE"); // non utf-8 is in hex
        UNIT_ASSERT_STRING_CONTAINS(result, "name=value");
    }

    Y_UNIT_TEST(LoggingJson) {
        TTestAuditLogService test(NKikimrConfig::TAuditConfig::JSON);

        TAuditLogParts parts = {
            {"name", "value"},
            {"fe", "\xfe\xfe"},
        };

        const TString result = test.SendAuditLog(std::move(parts));
        UNIT_ASSERT_STRING_CONTAINS(result, R"("name":"value")");
        UNIT_ASSERT_STRING_CONTAINS(result, R"("fe":"FEFE")"); // non utf-8 is in hex
    }

    Y_UNIT_TEST(LoggingJsonLog) {
        TTestAuditLogService test(NKikimrConfig::TAuditConfig::JSON_LOG_COMPATIBLE);

        TAuditLogParts parts = {
            {"name", "value"},
            {"fe", "\xfe\xfe"},
        };

        const TString result = test.SendAuditLog(std::move(parts));
        UNIT_ASSERT_STRING_CONTAINS(result, R"("@timestamp":)");
        UNIT_ASSERT_STRING_CONTAINS(result, R"("@log_type":"audit")");
        UNIT_ASSERT_STRING_CONTAINS(result, R"("fe":"FEFE")"); // non utf-8 is in hex
        UNIT_ASSERT_STRING_CONTAINS(result, R"("name":"value")");
    }

    Y_UNIT_TEST(SortParts) {
        TTestAuditLogService test(NKikimrConfig::TAuditConfig::TXT);

        // Parts are intentionally given in random priority order to verify sorting
        TAuditLogParts parts = {
            {"remote_address",  "192.168.1.1"},
            {"status",          "ERROR"},
            {"operation",       "DESCRIBE TABLE"},
            {"subject",         "user@domain"},
            {"component",       "schemeshard"},
            {"tx_id",           "281474976710656"},
            {"resource_id",     "resource-abc"},
            {"folder_id",       "folder-abc"},
            {"cloud_id",        "cloud-abc"},
            {"reason",          "Access denied"},
            {"detailed_status", "UNAUTHORIZED"},
            {"paths",           "/my/db/table1, /my/db/table2"},
            {"database",        "/my/db"},
            {"masked_token",    "user***"},
            {"sanitized_token", "user@domain"},
        };

        // Verify the full sorted output in one assertion:
        // known fields ordered by priority (0..12), then unknown fields lexicographically (paths < tx_id)
        UNIT_ASSERT_STRING_CONTAINS(
            test.SendAuditLog(std::move(parts)),
            "component=schemeshard, subject=user@domain, remote_address=192.168.1.1, "
            "sanitized_token=user@domain, masked_token=user***, operation=DESCRIBE TABLE, "
            "status=ERROR, detailed_status=UNAUTHORIZED, reason=Access denied, "
            "cloud_id=cloud-abc, folder_id=folder-abc, resource_id=resource-abc, "
            "database=/my/db, paths=/my/db/table1, /my/db/table2, tx_id=281474976710656"
        );
    }
}

Y_UNIT_TEST_SUITE(AuditLogHeartbeatTest) {
    Y_UNIT_TEST(LoggingHeartbeat) {
        NKikimrConfig::TAuditConfig protoCfg;
        protoCfg.MutableHeartbeat()->SetIntervalSeconds(1);
        auto* logClassSettings = protoCfg.AddLogClassConfig();
        logClassSettings->SetLogClass(NKikimrConfig::TAuditConfig::TLogClassConfig::AuditHeartbeat);
        logClassSettings->SetEnableLogging(true);
        TAuditConfig cfg(protoCfg);
        TTestAuditLogService test(NKikimrConfig::TAuditConfig::TXT);
        test.MakeHeartbeatActor(cfg);

        auto waitAndCheckLog = [&]() {
            const TString log = test.WaitAuditLog();
            UNIT_ASSERT_STRING_CONTAINS(log, "component=audit-service");
            UNIT_ASSERT_STRING_CONTAINS(log, "subject=metadata@system");
            UNIT_ASSERT_STRING_CONTAINS(log, "sanitized_token={none}");
            UNIT_ASSERT_STRING_CONTAINS(log, "operation=HEARTBEAT");
            UNIT_ASSERT_STRING_CONTAINS(log, "status=SUCCESS");
            UNIT_ASSERT_STRING_CONTAINS(log, "node_id=1");
        };

        waitAndCheckLog();
        waitAndCheckLog();
        waitAndCheckLog();
    }
}

} // namespace NKikimr::NAudit
