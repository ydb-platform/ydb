#include <ydb/core/testlib/audit_helpers/audit_helper.h>
#include <ydb/core/tx/schemeshard/schemeshard_audit_log.h>
#include <ydb/core/tx/schemeshard/schemeshard_impl.h>
#include <ydb/core/tx/schemeshard/ut_helpers/helpers.h>

#include <util/generic/algorithm.h>

using namespace NKikimr;
using namespace NSchemeShard;
using namespace NSchemeShardUT_Private;

namespace {

class TFixture {
public:
    std::vector<std::string> AuditLines;
    TTestBasicRuntime Runtime;
    TSchemeShard* SchemeShard = nullptr;
    THolder<TTestEnv> Env;
    ui64 Id = 100;

    TFixture() {
        Runtime.AuditLogBackends = Tests::CreateTestAuditLogBackends(AuditLines);
        auto factory = [this](const TActorId& tablet, TTabletStorageInfo* info) {
            SchemeShard = new TSchemeShard(tablet, info);
            return SchemeShard;
        };
        Env = MakeHolder<TTestEnv>(Runtime, TTestEnvOptions(), factory);
    }

    void Check(const TString& operation, Ydb::StatusIds::StatusCode status, const TString& reason) {
        const std::string marker = ", id=" + std::to_string(Id);
        Runtime.WaitFor("import/export audit record", [&] {
            return AnyOf(AuditLines, [&](const auto& line) { return line.find(marker) != std::string::npos; });
        }, TDuration::Seconds(10));
        const auto line = Tests::FindAuditLine(AuditLines, marker);
        UNIT_ASSERT_STRING_CONTAINS(line, "component=schemeshard");
        UNIT_ASSERT_STRING_CONTAINS(line, TStringBuilder() << "operation=" << operation << ",");
        UNIT_ASSERT_STRING_CONTAINS(line, TStringBuilder() << "detailed_status=" << Ydb::StatusIds::StatusCode_Name(status));
        UNIT_ASSERT_STRING_CONTAINS(line, status == Ydb::StatusIds::SUCCESS ? "status=SUCCESS," : "status=ERROR,");
        if (!reason.empty()) {
            UNIT_ASSERT_STRING_CONTAINS(line, TStringBuilder() << "reason=" << reason);
        } else if (status != Ydb::StatusIds::SUCCESS) {
            UNIT_ASSERT_STRING_CONTAINS(line, TStringBuilder() << "reason=" << operation
                << " failed with status " << Ydb::StatusIds::StatusCode_Name(status));
        } else {
            UNIT_ASSERT(line.find("reason=") == std::string::npos);
        }
    }
};

template <typename TRequest, typename TResponse, typename TLog>
void CheckStart(const TString& operation, TLog log) {
    TFixture fixture;
    for (auto status : {Ydb::StatusIds::BAD_REQUEST, Ydb::StatusIds::UNAUTHORIZED, Ydb::StatusIds::SUCCESS}) {
        for (const TString& reason : {TString(), TString("Original diagnostic")}) {
            TRequest request;
            request.SetTxId(++fixture.Id);
            request.SetDatabaseName("/MyRoot");
            TResponse response;
            auto* entry = response.MutableResponse()->MutableEntry();
            entry->SetStatus(status);
            if (!reason.empty()) {
                entry->AddIssues()->set_message(reason);
            }
            fixture.Runtime.RunCall([&] {
                log(request, response, fixture.SchemeShard);
                return true;
            });
            fixture.Check(operation, status, reason);
        }
    }
}

template <typename TInfo, typename TLog>
void CheckEnd(const TString& operation, TLog log) {
    TFixture fixture;
    for (auto state : {TInfo::EState::Cancelled, TInfo::EState::Done}) {
        for (const TString& reason : {TString(), TString("Cancelled manually")}) {
            TInfo info(++fixture.Id, {}, TInfo::EKind::S3, TString(),
                TPath::Resolve("/MyRoot", fixture.SchemeShard).Base()->PathId, {});
            info.State = state;
            info.Issue = reason;
            fixture.Runtime.RunCall([&] {
                log(info, fixture.SchemeShard);
                return true;
            });
            fixture.Check(operation, state == TInfo::EState::Done
                ? Ydb::StatusIds::SUCCESS : Ydb::StatusIds::CANCELLED, reason);
        }
    }
}

} // anonymous namespace

Y_UNIT_TEST_SUITE(TSchemeShardXxportAuditLog) {
    Y_UNIT_TEST(ExportStart) {
        CheckStart<NKikimrExport::TEvCreateExportRequest, NKikimrExport::TEvCreateExportResponse>("EXPORT START", AuditLogExportStart);
    }

    Y_UNIT_TEST(ImportStart) {
        CheckStart<NKikimrImport::TEvCreateImportRequest, NKikimrImport::TEvCreateImportResponse>("IMPORT START", AuditLogImportStart);
    }

    Y_UNIT_TEST(ExportEnd) {
        CheckEnd<TExportInfo>("EXPORT END", AuditLogExportEnd);
    }

    Y_UNIT_TEST(ImportEnd) {
        CheckEnd<TImportInfo>("IMPORT END", AuditLogImportEnd);
    }
}
