#include <ydb/core/audit/audit_log.h>
#include <ydb/core/grpc_services/audit_log.h>
#include <ydb/core/grpc_services/base/base.h>
#include <ydb/core/testlib/audit_helpers/audit_helper.h>
#include <ydb/core/testlib/basics/appdata.h>
#include <ydb/core/testlib/basics/runtime.h>
#include <ydb/library/testlib/helpers.h>

#include <library/cpp/testing/unittest/registar.h>

#include <functional>

namespace NKikimr::NGRpcService {
namespace {

using TLogClassConfig = TAuditMode::TLogClassConfig;

const TLogClassConfig::ELogClass LogClasses[] = {
    TLogClassConfig::ClusterAdmin, TLogClassConfig::DatabaseAdmin,
    TLogClassConfig::Login, TLogClassConfig::NodeRegistration,
    TLogClassConfig::Ddl, TLogClassConfig::Dml, TLogClassConfig::Operations,
    TLogClassConfig::ExportImport, TLogClassConfig::Acl,
    TLogClassConfig::AuditHeartbeat, TLogClassConfig::Default,
};

const TString OptionalFields[] = {
    "database", "grpc_method", "start_time", "end_time", "cloud_id", "folder_id",
    "resource_id", "tablet_id", "program_text", "schema_changes", "storage_pool",
    "group", "hive_id", "request", "query_text", "prepared_query_id", "tx_id",
    "begin_tx", "commit_tx", "table", "row_count", "hive_generation", "reassign_id",
};

class TWriteAuditActor : public TActorBootstrapped<TWriteAuditActor> {
public:
    explicit TWriteAuditActor(std::function<void()> write)
        : Write(std::move(write))
    {}

    void Bootstrap() {
        Write();
        PassAway();
    }

private:
    std::function<void()> Write;
};

struct TAuditLogTest {
    std::vector<std::string> Lines;
    NActors::TTestBasicRuntime Runtime;

    TAuditLogTest() {
        Runtime.AuditLogBackends = Tests::CreateTestAuditLogBackends(Lines);
        Runtime.Initialize(TAppPrepare().Unwrap());
    }

    TString Log(std::function<void()> write) {
        const auto count = Lines.size();
        Runtime.Register(new TWriteAuditActor(std::move(write)));
        Runtime.WaitFor("gRPC audit record", [&] { return Lines.size() > count; }, TDuration::Seconds(10));
        UNIT_ASSERT_VALUES_EQUAL(Lines.size(), count + 1);
        return TString(Lines.back());
    }
};

} // namespace

Y_UNIT_TEST_SUITE(GrpcAuditLog) {
    Y_UNIT_TEST_TWIN(MissingRequiredAndOptionalFields, emptyValues) {
        TAuditLogTest test;
        TAuditLogParts parts;
        if (emptyValues) {
            for (const auto field : NAudit::RequiredFields) {
                parts[TString(field)] = {};
            }
            for (const auto& field : OptionalFields) {
                parts[field] = {};
            }
        }
        for (const auto logClass : LogClasses) {
            const auto line = test.Log([&] { AuditLog(logClass, std::nullopt, parts); });
            UNIT_ASSERT_STRING_CONTAINS(line, "component=grpc-proxy");
            UNIT_ASSERT_STRING_CONTAINS(line, "status=IN-PROCESS");
            for (const auto field : NAudit::RequiredFields) {
                if (field != "component" && field != "status") {
                    UNIT_ASSERT_STRING_CONTAINS(line, TString(field) + "={none}");
                }
            }
            for (const auto& field : OptionalFields) {
                UNIT_ASSERT_C(!line.Contains(field + "="), line);
            }
            UNIT_ASSERT_C(!line.Contains("reason="), line);
            UNIT_ASSERT_C(!line.Contains("detailed_status="), line);
        }
    }

    Y_UNIT_TEST(ReasonIsRequiredOnlyForErrors) {
        TAuditLogTest test;
        for (const auto logClass : LogClasses) {
            for (const auto status : {Ydb::StatusIds::SUCCESS, Ydb::StatusIds::BAD_REQUEST}) {
                for (const auto& parts : TVector<TAuditLogParts>{
                    {}, {{"reason", ""}}, {{"reason", "Invalid request"}},
                }) {
                    const auto line = test.Log([&] { AuditLog(logClass, status, parts); });
                    if (status == Ydb::StatusIds::BAD_REQUEST) {
                        for (const auto field : NAudit::RequiredFieldsOnError) {
                            UNIT_ASSERT_STRING_CONTAINS(line, " " + TString(field) + "=");
                        }
                    }
                    if (parts.contains("reason") && !parts.at("reason").empty()) {
                        UNIT_ASSERT_STRING_CONTAINS(line, "reason=Invalid request");
                    } else if (status == Ydb::StatusIds::BAD_REQUEST) {
                        UNIT_ASSERT_STRING_CONTAINS(line, "reason={none}");
                    } else {
                        UNIT_ASSERT_C(!line.Contains("reason="), line);
                    }
                }
            }
        }
    }

    Y_UNIT_TEST(ConnectionWithEmptyFields) {
        TAuditLogTest test;
        for (const auto logClass : LogClasses) {
            const auto line = test.Log([&] {
                TEvHttpRequestAuthAndCheck request(
                    {}, {}, {}, TAuditMode::Modifying(logClass), {}, {});
                AuditLogConn(&request, {}, {}, {});
            });
            UNIT_ASSERT_STRING_CONTAINS(line, "component=grpc-conn");
            UNIT_ASSERT_STRING_CONTAINS(line, "status=SUCCESS");
            UNIT_ASSERT_STRING_CONTAINS(line, "remote_address={none}");
            UNIT_ASSERT_STRING_CONTAINS(line, "subject={none}");
            UNIT_ASSERT_STRING_CONTAINS(line, "sanitized_token={none}");
            UNIT_ASSERT_C(!line.Contains("database="), line);
            UNIT_ASSERT_C(!line.Contains("reason="), line);
        }
    }

    Y_UNIT_TEST(PresentFieldsAndComponent) {
        TAuditLogTest test;
        const TAuditLogParts parts = {
            {"component", "supplied-component"}, {"remote_address", "192.0.2.1"},
            {"subject", "user@domain"}, {"sanitized_token", "sanitized-token"},
            {"database", "/Root"}, {"operation", "ExecuteDataQuery"},
            {"query_text", "SELECT 1"}, {"cloud_id", "cloud"},
            {"folder_id", "folder"}, {"resource_id", "resource"},
        };
        const auto line = test.Log([&] { AuditLog(TLogClassConfig::Dml, Ydb::StatusIds::SUCCESS, parts); });
        UNIT_ASSERT_STRING_CONTAINS(line, "component=grpc-proxy");
        UNIT_ASSERT_C(!line.Contains("supplied-component"), line);
        for (const auto& [name, value] : parts) {
            if (name != "component") {
                UNIT_ASSERT_STRING_CONTAINS(line, name + "=" + value);
            }
        }
    }
}

} // namespace NKikimr::NGRpcService
