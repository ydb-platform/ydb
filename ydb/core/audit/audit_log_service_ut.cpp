#include "audit_log_service.h"
#include "audit_log.h"

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

    TString SendAuditLog(TVector<std::pair<TString, TString>>&& parts) { // Send log and wait for result
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

        TVector<std::pair<TString, TString>> parts = {
            {"name", "value"},
            {"fe", "\xfe\xfe"},
        };

        UNIT_ASSERT_STRING_CONTAINS(test.SendAuditLog(std::move(parts)), "name=value, fe=FEFE"); // non utf-8 is in base64
    }

    Y_UNIT_TEST(LoggingJson) {
        TTestAuditLogService test(NKikimrConfig::TAuditConfig::JSON);

        TVector<std::pair<TString, TString>> parts = {
            {"name", "value"},
            {"fe", "\xfe\xfe"},
        };

        UNIT_ASSERT_STRING_CONTAINS(test.SendAuditLog(std::move(parts)), R"({"name":"value","fe":"FEFE"})");
    }

    Y_UNIT_TEST(LoggingJsonLog) {
        TTestAuditLogService test(NKikimrConfig::TAuditConfig::JSON_LOG_COMPATIBLE);

        TVector<std::pair<TString, TString>> parts = {
            {"name", "value"},
            {"fe", "\xfe\xfe"},
        };

        UNIT_ASSERT_STRING_CONTAINS(test.SendAuditLog(std::move(parts)), R"("@log_type":"audit","name":"value","fe":"FEFE"})");
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
            UNIT_ASSERT_STRING_CONTAINS(log, "component=audit, subject=metadata@system, sanitized_token={none}, operation=HEARTBEAT, status=SUCCESS, node_id=1");
        };

        waitAndCheckLog();
        waitAndCheckLog();
        waitAndCheckLog();
    }
}

} // namespace NKikimr::NAudit
