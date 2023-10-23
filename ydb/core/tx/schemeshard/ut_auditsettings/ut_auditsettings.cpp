#include <ydb/core/tx/schemeshard/ut_helpers/helpers.h>

using namespace NKikimr;
using namespace NSchemeShard;
using namespace NSchemeShardUT_Private;

namespace {

template <class T>
T MessageFromText(const TString& text) {
    T msg;
    UNIT_ASSERT_C(google::protobuf::TextFormat::ParseFromString(text, &msg), "Invalid protobuf message text");
    return msg;
}

bool AuditSettingsCompare(TString* diff, const NKikimrSubDomains::TAuditSettings& a, const NKikimrSubDomains::TAuditSettings& b) {
    google::protobuf::util::MessageDifferencer d;
    d.ReportDifferencesToString(diff);
    d.TreatAsSet(NKikimrSubDomains::TAuditSettings::GetDescriptor()->FindFieldByName("ExpectedSubjects"));
    return d.Compare(a, b);
}

}   // anonymous namespace

Y_UNIT_TEST_SUITE(TSchemeShardAuditSettings) {

    const std::vector<TString> CreateTestParams = {
        R"()",
        R"(AuditSettings { EnableDmlAudit: false })",
        R"(AuditSettings { EnableDmlAudit: true })",
        R"(AuditSettings { EnableDmlAudit: false ExpectedSubjects: ["A", "B"] })",
        R"(AuditSettings { EnableDmlAudit: true ExpectedSubjects: ["A", "B"] })",
        R"(AuditSettings { EnableDmlAudit: true ExpectedSubjects: ["A", "A"] })",
    };

    Y_UNIT_TEST(CreateExtSubdomain) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        auto test = [&](const TStringBuf& auditSettingsFragment, const NKikimrSubDomains::TAuditSettings& expected) {
            TestCreateExtSubDomain(runtime, ++txId,  "/MyRoot", TString::Join(
                R"(
                    Name: "USER_0"
                )",
                auditSettingsFragment
            ));
            env.TestWaitNotification(runtime, txId);

            auto actual = DescribePath(runtime, "/MyRoot/USER_0")
                .GetPathDescription()
                .GetDomainDescription()
                .GetAuditSettings()
            ;

            TString diff;
            UNIT_ASSERT_C(AuditSettingsCompare(&diff, expected, actual), "FAILED for '" << auditSettingsFragment << "': " << diff);

            TestForceDropExtSubDomain(runtime, ++txId, "/MyRoot", "USER_0");
            env.TestWaitNotification(runtime, txId);
        };

        // for all variations expect returned TAuditSettings equal (or equivalent) to the original input
        for (const auto& i : CreateTestParams) {
            Cerr << "TEST CreateExtSubdomain, '" << i << "'" << Endl;
            test(i, MessageFromText<NKikimrSubDomains::TDomainDescription>(i).GetAuditSettings());
        }
    }

    Y_UNIT_TEST(CreateSubdomain) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        auto test = [&](const TStringBuf& auditSettingsFragment, const NKikimrSubDomains::TAuditSettings& expected) {
            TestCreateSubDomain(runtime, ++txId,  "/MyRoot", TString::Join(
                R"(
                    Name: "USER_0"
                )",
                auditSettingsFragment
            ));
            env.TestWaitNotification(runtime, txId);

            auto actual = DescribePath(runtime, "/MyRoot/USER_0")
                .GetPathDescription()
                .GetDomainDescription()
                .GetAuditSettings()
            ;

            TString diff;
            UNIT_ASSERT_C(AuditSettingsCompare(&diff, expected, actual), "FAILED for '" << auditSettingsFragment << "': " << diff);

            TestForceDropSubDomain(runtime, ++txId, "/MyRoot", "USER_0");
            env.TestWaitNotification(runtime, txId);
        };

        // for all variations expect returned TAuditSettings equal (or equivalent) to the original input
        for (const auto& i : CreateTestParams) {
            Cerr << "TEST CreateSubdomain, '" << i << "'" << Endl;
            test(i, MessageFromText<NKikimrSubDomains::TDomainDescription>(i).GetAuditSettings());
        }
    }

    struct TAlterTestParam {
        TString AtCreate;
        TString AtAlter;
        TString Expected;
    };
    std::vector<TAlterTestParam> AlterTestParams = {
        // Alter can set AuditSettings
        {
            .AtCreate = R"()",
            .AtAlter = R"()",
            .Expected = R"()",
        },
        {
            .AtCreate = R"()",
            .AtAlter = R"(AuditSettings { EnableDmlAudit: false })",
            .Expected = R"(AuditSettings { EnableDmlAudit: false })",
        },
        {
            .AtCreate = R"()",
            .AtAlter = R"(AuditSettings { EnableDmlAudit: true })",
            .Expected = R"(AuditSettings { EnableDmlAudit: true })",
        },
        {
            .AtCreate = R"()",
            .AtAlter = R"(AuditSettings { EnableDmlAudit: false ExpectedSubjects: ["A", "B"] })",
            .Expected = R"(AuditSettings { EnableDmlAudit: false ExpectedSubjects: ["A", "B"] })",
        },
        {
            .AtCreate = R"()",
            .AtAlter = R"(AuditSettings { EnableDmlAudit: true ExpectedSubjects: ["A", "B"] })",
            .Expected = R"(AuditSettings { EnableDmlAudit: true ExpectedSubjects: ["A", "B"] })",
        },
        {
            .AtCreate = R"()",
            .AtAlter = R"(AuditSettings { EnableDmlAudit: true ExpectedSubjects: ["A", "A"] })",
            .Expected = R"(AuditSettings { EnableDmlAudit: true ExpectedSubjects: ["A", "A"] })",
        },
        // Alter doesn't drop existing AuditSettings
        {
            .AtCreate = R"(AuditSettings { EnableDmlAudit: false })",
            .AtAlter = R"()",
            .Expected = R"(AuditSettings { EnableDmlAudit: false })",
        },
        {
            .AtCreate = R"(AuditSettings { EnableDmlAudit: true })",
            .AtAlter = R"()",
            .Expected = R"(AuditSettings { EnableDmlAudit: true })",
        },
        {
            .AtCreate = R"(AuditSettings { EnableDmlAudit: false ExpectedSubjects: ["A", "B"] })",
            .AtAlter = R"()",
            .Expected = R"(AuditSettings { EnableDmlAudit: false ExpectedSubjects: ["A", "B"] })",
        },
        {
            .AtCreate = R"(AuditSettings { EnableDmlAudit: true ExpectedSubjects: ["A", "B"] })",
            .AtAlter = R"()",
            .Expected = R"(AuditSettings { EnableDmlAudit: true ExpectedSubjects: ["A", "B"] })",
        },
        // EnableDmlAudit changes independently of ExpectedSubjects
        {
            .AtCreate = R"(AuditSettings { EnableDmlAudit: false })",
            .AtAlter = R"(AuditSettings { EnableDmlAudit: true })",
            .Expected = R"(AuditSettings { EnableDmlAudit: true })",
        },
        {
            .AtCreate = R"(AuditSettings { EnableDmlAudit: true })",
            .AtAlter = R"(AuditSettings { EnableDmlAudit: false })",
            .Expected = R"(AuditSettings { EnableDmlAudit: false })",
        },
        {
            .AtCreate = R"(AuditSettings { EnableDmlAudit: false ExpectedSubjects: ["A", "B"] })",
            .AtAlter = R"(AuditSettings { EnableDmlAudit: true })",
            .Expected = R"(AuditSettings { EnableDmlAudit: true ExpectedSubjects: ["A", "B"] })",
        },
        {
            .AtCreate = R"(AuditSettings { EnableDmlAudit: true ExpectedSubjects: ["A", "B"] })",
            .AtAlter = R"(AuditSettings { EnableDmlAudit: false })",
            .Expected = R"(AuditSettings { EnableDmlAudit: false ExpectedSubjects: ["A", "B"] })",
        },
        // ExpectedSubjects can be added independently of EnableDmlAudit
        {
            .AtCreate = R"(AuditSettings { EnableDmlAudit: false })",
            .AtAlter = R"(AuditSettings { ExpectedSubjects: ["A", "B"] })",
            .Expected = R"(AuditSettings { EnableDmlAudit: false ExpectedSubjects: ["A", "B"] })",
        },
        {
            .AtCreate = R"(AuditSettings { EnableDmlAudit: true })",
            .AtAlter = R"(AuditSettings { ExpectedSubjects: ["A", "B"] })",
            .Expected = R"(AuditSettings { EnableDmlAudit: true ExpectedSubjects: ["A", "B"] })",
        },
        // ExpectedSubjects can be removed independently of EnableDmlAudit
        {
            .AtCreate = R"(AuditSettings { EnableDmlAudit: false ExpectedSubjects: ["A", "B"] })",
            .AtAlter = R"(AuditSettings { ExpectedSubjects: [""] })",
            .Expected = R"(AuditSettings { EnableDmlAudit: false })",
        },
        {
            .AtCreate = R"(AuditSettings { EnableDmlAudit: true ExpectedSubjects: ["A", "B"] })",
            .AtAlter = R"(AuditSettings { ExpectedSubjects: [""] })",
            .Expected = R"(AuditSettings { EnableDmlAudit: true })",
        },
        // ExpectedSubjects can be changed independently of EnableDmlAudit
        {
            .AtCreate = R"(AuditSettings { EnableDmlAudit: false ExpectedSubjects: ["A", "B"] })",
            .AtAlter = R"(AuditSettings { ExpectedSubjects: ["A"] })",
            .Expected = R"(AuditSettings { EnableDmlAudit: false ExpectedSubjects: ["A"] })",
        },
        {
            .AtCreate = R"(AuditSettings { EnableDmlAudit: true ExpectedSubjects: ["A", "B"] })",
            .AtAlter = R"(AuditSettings { ExpectedSubjects: ["A"] })",
            .Expected = R"(AuditSettings { EnableDmlAudit: true ExpectedSubjects: ["A"] })",
        },
        {
            .AtCreate = R"(AuditSettings { EnableDmlAudit: false ExpectedSubjects: ["A"] })",
            .AtAlter = R"(AuditSettings { ExpectedSubjects: ["A", "B"] })",
            .Expected = R"(AuditSettings { EnableDmlAudit: false ExpectedSubjects: ["A", "B"] })",
        },
        {
            .AtCreate = R"(AuditSettings { EnableDmlAudit: true ExpectedSubjects: ["A"] })",
            .AtAlter = R"(AuditSettings { ExpectedSubjects: ["A", "B"] })",
            .Expected = R"(AuditSettings { EnableDmlAudit: true ExpectedSubjects: ["A", "B"] })",
        },
        // Empty subjects are removed
        {
            .AtCreate = R"(AuditSettings { ExpectedSubjects: ["A"] })",
            .AtAlter = R"(AuditSettings { ExpectedSubjects: ["", "B"] })",
            .Expected = R"(AuditSettings { ExpectedSubjects: ["B"] })",
        },
        {
            .AtCreate = R"(AuditSettings { ExpectedSubjects: ["A", "B"] })",
            .AtAlter = R"(AuditSettings { ExpectedSubjects: ["", "", "C", ""] })",
            .Expected = R"(AuditSettings { ExpectedSubjects: ["C"] })",
        },
        // EnableDmlAudit and ExpectedSubjects could be changed both
        {
            .AtCreate = R"(AuditSettings { EnableDmlAudit: false ExpectedSubjects: ["A", "B"] })",
            .AtAlter = R"(AuditSettings { EnableDmlAudit: true ExpectedSubjects: ["C", "D"] })",
            .Expected = R"(AuditSettings { EnableDmlAudit: true ExpectedSubjects: ["C", "D"] })",
        },
    };

    Y_UNIT_TEST_FLAG(AlterExtSubdomain, ExternalSchemeShard) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        TString externalSchemeshard;
        if (ExternalSchemeShard) {
            externalSchemeshard = R"(
                ExternalSchemeShard: true
                PlanResolution: 50
                Coordinators: 1
                Mediators: 1
                TimeCastBucketsPerMediator: 2
                StoragePools {
                    Name: "pool-1"
                    Kind: "hdd"
                }
            )";
        }

        auto test = [&](const TStringBuf& atCreate, const TStringBuf& atAlter, const NKikimrSubDomains::TAuditSettings& expected) {
            TestCreateExtSubDomain(runtime, ++txId,  "/MyRoot", TString::Join(
                R"(
                    Name: "USER_0"
                )",
                atCreate
            ));
            TestAlterExtSubDomain(runtime, ++txId,  "/MyRoot", TString::Join(
                R"(
                    Name: "USER_0"
                )",
                externalSchemeshard,
                atAlter
            ));
            env.TestWaitNotification(runtime, {txId, txId - 1});

            const auto& describe = DescribePath(runtime, "/MyRoot/USER_0");

            auto actual = describe.GetPathDescription().GetDomainDescription().GetAuditSettings();
            TString diff;
            UNIT_ASSERT_C(AuditSettingsCompare(&diff, expected, actual), "(at root) FAILED for '" << atCreate << "' + '" << atAlter << "': " << diff);

            if (ExternalSchemeShard) {
                // check auditSettings at tenant schemeshard also
                ui64 tenantSchemeShard = describe.GetPathDescription().GetDomainDescription().GetProcessingParams().GetSchemeShard();
                const auto& describe = DescribePath(runtime, tenantSchemeShard, "/MyRoot/USER_0");

                auto actual = describe.GetPathDescription().GetDomainDescription().GetAuditSettings();
                TString diff;
                UNIT_ASSERT_C(AuditSettingsCompare(&diff, expected, actual), "(at tenant) FAILED for '" << atCreate << "' + '" << atAlter << "': " << diff);
            }

            TestForceDropExtSubDomain(runtime, ++txId, "/MyRoot", "USER_0");
            env.TestWaitNotification(runtime, txId);
        };

        // for all variations match actual returned TAuditSettings to expected
        for (const auto& [atCreate, atAlter, expected] : AlterTestParams) {
            Cerr << "TEST AlterExtSubdomain, '" << atCreate << "' + '" << atAlter << "'" << Endl;
            test(atCreate, atAlter, MessageFromText<NKikimrSubDomains::TDomainDescription>(expected).GetAuditSettings());
        }
    }

    Y_UNIT_TEST(AlterSubdomain) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        auto test = [&](const TStringBuf& atCreate, const TStringBuf& atAlter, const NKikimrSubDomains::TAuditSettings& expected) {
            TestCreateSubDomain(runtime, ++txId,  "/MyRoot", TString::Join(
                R"(
                    Name: "USER_0"
                )",
                atCreate
            ));
            TestAlterSubDomain(runtime, ++txId,  "/MyRoot", TString::Join(
                R"(
                    Name: "USER_0"
                )",
                atAlter
            ));
            env.TestWaitNotification(runtime, {txId, txId - 1});

            const auto& describe = DescribePath(runtime, "/MyRoot/USER_0");

            auto actual = describe.GetPathDescription().GetDomainDescription().GetAuditSettings();
            TString diff;
            UNIT_ASSERT_C(AuditSettingsCompare(&diff, expected, actual), "(at root) FAILED for '" << atCreate << "' + '" << atAlter << "': " << diff);

            TestForceDropSubDomain(runtime, ++txId, "/MyRoot", "USER_0");
            env.TestWaitNotification(runtime, txId);
        };

        // for all variations match actual returned TAuditSettings to expected
        for (const auto& [atCreate, atAlter, expected] : AlterTestParams) {
            Cerr << "TEST AlterSubdomain, '" << atCreate << "' + '" << atAlter << "'" << Endl;
            test(atCreate, atAlter, MessageFromText<NKikimrSubDomains::TDomainDescription>(expected).GetAuditSettings());
        }
    }
}
