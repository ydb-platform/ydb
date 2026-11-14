#include <ydb/core/audit/audit_log.h>
#include <ydb/core/base/test_failure_injection.h>
#include <ydb/core/testlib/audit_helpers/audit_helper.h>
#include <ydb/core/tx/schemeshard/schemeshard_audit_log.h>
#include <ydb/core/tx/schemeshard/schemeshard_impl.h>
#include <ydb/core/tx/schemeshard/ut_helpers/helpers.h>

#include <util/generic/algorithm.h>

#if defined(_unix_)
#include <csignal>
#include <sys/resource.h>
#include <sys/wait.h>
#include <unistd.h>
#endif

using namespace NKikimr;
using namespace NSchemeShard;
using namespace NSchemeShardUT_Private;

namespace {

void CheckAuditRecord(TTestBasicRuntime& runtime, const std::vector<std::string>& lines,
        ui64 txId, const TString& operation, NKikimrScheme::EStatus status, const TString& reason) {
    const std::string marker = "tx_id=" + std::to_string(txId);
    runtime.WaitFor("scheme audit record", [&] {
        return AnyOf(lines, [&](const auto& line) { return line.find(marker) != std::string::npos; });
    }, TDuration::Seconds(10));
    const auto line = Tests::FindAuditLine(lines, marker);
    UNIT_ASSERT_STRING_CONTAINS(line, "component=schemeshard");
    UNIT_ASSERT_STRING_CONTAINS(line, TStringBuilder() << "operation=" << operation);
    UNIT_ASSERT_STRING_CONTAINS(line, TStringBuilder() << "detailed_status=" << NKikimrScheme::EStatus_Name(status));
    if (reason.empty()) {
        UNIT_ASSERT_STRING_CONTAINS(line, "status=SUCCESS");
        UNIT_ASSERT(line.find("reason=") == std::string::npos);
    } else {
        UNIT_ASSERT_STRING_CONTAINS(line, "status=ERROR");
        UNIT_ASSERT_STRING_CONTAINS(line, TStringBuilder() << "reason=" << reason);
    }
}

NKikimrSchemeOp::TPersQueueGroupDescription TopicWithChannels(ui32 count) {
    NKikimrSchemeOp::TPersQueueGroupDescription topic;
    topic.SetName("Topic");
    topic.SetTotalGroupCount(1);
    topic.SetPartitionPerTablet(1);
    auto* config = topic.MutablePQTabletConfig()->MutablePartitionConfig();
    config->SetLifetimeSeconds(10);
    for (ui32 i = 0; i < count; ++i) {
        config->AddExplicitChannelProfiles()->SetPoolKind("pool-kind-1");
    }
    return topic;
}

} // anonymous namespace

Y_UNIT_TEST_SUITE(TSchemeShardAuditLog) {
    Y_UNIT_TEST_FLAG(TopicChannelErrorsHaveReason, AuditEnabled) {
        std::vector<std::string> auditLines;
        TTestBasicRuntime runtime;
        if (AuditEnabled) {
            runtime.AuditLogBackends = Tests::CreateTestAuditLogBackends(auditLines);
        }
        TTestEnv env(runtime);
        ui64 txId = 100;

        TestCreatePQGroup(runtime, ++txId, "/MyRoot", TopicWithChannels(4).DebugString());
        env.TestWaitNotification(runtime, txId);

        const TVector<std::pair<ui32, TString>> cases = {
            {2, "ExplicitChannelProfiles has 2 channels, should be [3 .. 256]"},
            {257, "ExplicitChannelProfiles has 257 channels, should be [3 .. 256]"},
            {3, "ExplicitChannelProfiles has 3 channels, should be at least 4 (current config)"},
        };
        for (const auto& [count, reason] : cases) {
            TestAlterPQGroup(runtime, ++txId, "/MyRoot", TopicWithChannels(count).DebugString(),
                {{NKikimrScheme::StatusInvalidParameter, reason}});
            if (AuditEnabled) {
                CheckAuditRecord(runtime, auditLines, txId, "ALTER PERSISTENT QUEUE",
                    NKikimrScheme::StatusInvalidParameter, reason);
            }
        }
    }

    Y_UNIT_TEST_FLAG(RestoreErrorsHaveReason, InjectFailure) {
        std::vector<std::string> auditLines;
        TTestBasicRuntime runtime;
        runtime.AuditLogBackends = Tests::CreateTestAuditLogBackends(auditLines);
        TTestEnv env(runtime, TTestEnvOptions().EnableBackupService(true));
        ui64 txId = 100;

        if (InjectFailure) {
            TestMkDir(runtime, ++txId, "/MyRoot", ".backups");
            env.TestWaitNotification(runtime, txId);
            TestMkDir(runtime, ++txId, "/MyRoot/.backups", "collections");
            env.TestWaitNotification(runtime, txId);
            TestCreateTable(runtime, ++txId, "/MyRoot", R"(
                Name: "Table"
                Columns { Name: "key" Type: "Uint32" }
                KeyColumnNames: ["key"]
            )");
            env.TestWaitNotification(runtime, txId);
            TestCreateBackupCollection(runtime, ++txId, "/MyRoot/.backups/collections", R"(
                Name: "Collection"
                ExplicitEntryList { Entries { Type: ETypeTable Path: "/MyRoot/Table" } }
                Cluster {}
            )");
            env.TestWaitNotification(runtime, txId);
            runtime.GetAppData().InjectFailure(static_cast<ui64>(EInjectedFailureType::LateBackupCollectionNotFound));
        }

        auto request = MakeHolder<TEvSchemeShard::TEvModifySchemeTransaction>(++txId, TTestTxConfig::SchemeShard);
        auto* operation = request->Record.AddTransaction();
        operation->SetWorkingDir("/MyRoot");
        operation->SetInternal(true);
        operation->SetOperationType(NKikimrSchemeOp::ESchemeOpCreateLongIncrementalRestoreOp);
        operation->MutableRestoreBackupCollection()->SetName(".backups/collections/Collection");
        AsyncSend(runtime, TTestTxConfig::SchemeShard, request.Release());

        const TString reason = "Backup collection not found: /MyRoot/.backups/collections/Collection";
        TestModificationResults(runtime, txId, {{NKikimrScheme::StatusPathDoesNotExist, reason}});
        CheckAuditRecord(runtime, auditLines, txId, "RESTORE INCREMENTAL", NKikimrScheme::StatusPathDoesNotExist, reason);
    }

    Y_UNIT_TEST(SuccessfulStatusesAllowEmptyReason) {
        std::vector<std::string> auditLines;
        TTestBasicRuntime runtime;
        runtime.AuditLogBackends = Tests::CreateTestAuditLogBackends(auditLines);
        TSchemeShard* ss = nullptr;
        auto factory = [&ss](const TActorId& tablet, TTabletStorageInfo* info) {
            ss = new TSchemeShard(tablet, info);
            return ss;
        };
        TTestEnv env(runtime, TTestEnvOptions(), factory);
        ui64 txId = 100;

        NKikimrSchemeOp::TModifyScheme operation;
        operation.SetOperationType(NKikimrSchemeOp::ESchemeOpMkDir);
        operation.SetWorkingDir("/MyRoot");
        operation.MutableMkDir()->SetName("Dir");
        for (auto status : {NKikimrScheme::StatusSuccess, NKikimrScheme::StatusAccepted, NKikimrScheme::StatusAlreadyExists}) {
            ++txId;
            runtime.RunCall([&] {
                AuditLogModifySchemeOperation(operation, status, {}, ss, {}, {}, {}, txId, {});
                return true;
            });
            CheckAuditRecord(runtime, auditLines, txId, "CREATE DIRECTORY", status, {});
        }
    }

#if defined(_unix_)
    Y_UNIT_TEST(ErrorWithoutReasonAbortsWithAuditDisabled) {
        const auto pid = fork();
        UNIT_ASSERT_C(pid >= 0, "fork failed");
        if (pid == 0) {
            const rlimit limits = {0, 0};
            setrlimit(RLIMIT_CORE, &limits);
            signal(SIGABRT, SIG_DFL);
            NAudit::AUDIT_LOG_ENABLED.store(false);
            NKikimrSchemeOp::TModifyScheme operation;
            operation.SetOperationType(NKikimrSchemeOp::ESchemeOpMkDir);
            AuditLogModifySchemeOperation(operation, NKikimrScheme::StatusInvalidParameter, {},
                nullptr, {}, {}, {}, 100, {});
            _exit(0);
        }
        int status = 0;
        UNIT_ASSERT_VALUES_EQUAL(waitpid(pid, &status, 0), pid);
        UNIT_ASSERT_C(WIFSIGNALED(status), status);
        UNIT_ASSERT_VALUES_EQUAL(WTERMSIG(status), SIGABRT);
    }
#endif
}
