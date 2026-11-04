#include "database_config_patch.h"
#include "ut_helpers.h"

#include <ydb/core/base/database_feature_flags.h>
#include <ydb/core/testlib/audit_helpers/audit_helper.h>
#include <ydb/core/testlib/tablet_helpers.h>
#include <ydb/library/yaml_config/yaml_config.h>

namespace NKikimr::NConsole::NUT {
namespace {

NKikimrConsole::TPatchDatabaseConfigRequest MakePatch(std::optional<bool> value) {
    NKikimrConsole::TPatchDatabaseConfigRequest request;
    request.SetDatabase(TENANT1_1_NAME);
    request.SetRequestId("sql-feature-flags-test");
    auto* change = request.AddChanges();
    change->SetName("ENABLE_CLIENT_MODIFICATIONS");
    if (value) {
        change->SetValue(*value);
    }
    return request;
}

TTenantTestConfig PatchTestConfig() {
    TTenantTestConfig config;
    config.Domains = {{DOMAIN1_NAME, SCHEME_SHARD1_ID, {}}};
    config.HiveId = HIVE_ID;
    config.FakeTenantSlotBroker = true;
    config.FakeSchemeShard = false;
    config.CreateConsole = true;
    config.Nodes = {{{{}, "type1"}}};
    config.DataCenterCount = 1;
    return config;
}

NKikimrConfig::TAppConfig PatchAppConfig() {
    NKikimrConfig::TAppConfig config;
    config.MutableFeatureFlags()->SetDatabaseYamlConfigAllowed(true);
    config.MutableFeatureFlags()->SetEnableExternalSubdomains(true);
    return config;
}

class TPatchTestRuntime : public TTenantTestRuntime {
public:
    TPatchTestRuntime()
        : TTenantTestRuntime(PatchTestConfig(), PatchAppConfig())
    {
        // These tests exercise Console directly; end-to-end SQL tests cover the dispatcher.
        GetAppData().YamlConfigEnabled = true;
    }

    void RunTenant(const TString& path) {
        Send(new IEventHandle(MakeTenantPoolID(GetNodeId(0)), Sender, new TEvents::TEvPoisonPill));
        CreateTenantPool(0, {{{path, {1, 1, 1}}}, "type1"});
        WaitTenantRunning(*this, path);
    }
};

const TString MainConfig = R"(
metadata:
  kind: MainConfig
  cluster: ""
  version: 0
config:
  yaml_config_enabled: true
  feature_flags:
    database_yaml_config_allowed: true
    enable_client_modifications: true
    enable_unsafe_truncate_table: true
)";

const TString MergedFlagConfig = R"(
metadata:
  kind: DatabaseConfig
  database: /dc-1/users/tenant-1
  version: 0
config:
  private_database_config: &defaults
    enable_client_modifications: false
  feature_flags: !inherit
    <<: *defaults
)";

NKikimrConsole::TPatchDatabaseConfigResponse SendPatch(TTenantTestRuntime& runtime,
    const NKikimrConsole::TPatchDatabaseConfigRequest& request,
    Ydb::StatusIds::StatusCode expectedStatus = Ydb::StatusIds::SUCCESS)
{
    auto event = MakeHolder<TEvConsole::TEvPatchDatabaseConfigRequest>();
    event->Record.CopyFrom(request);
    runtime.SendToConsole(event.Release());
    auto response = runtime.GrabEdgeEventRethrow<TEvConsole::TEvPatchDatabaseConfigResponse>(runtime.Sender);
    UNIT_ASSERT_VALUES_EQUAL_C(response->Get()->Record.GetYdbStatus(), expectedStatus,
        response->Get()->Record.ShortDebugString());
    return response->Get()->Record;
}

TVector<NKikimrConsole::TDatabaseFeatureFlagsChange> ReadPatchHistory(TTenantTestRuntime& runtime) {
    runtime.SendToConsole(new TEvConsole::TEvGetLogTailRequest);
    auto response = runtime.GrabEdgeEventRethrow<TEvConsole::TEvGetLogTailResponse>(runtime.Sender);
    TVector<NKikimrConsole::TDatabaseFeatureFlagsChange> result;
    for (const auto& record : response->Get()->Record.GetLogRecords()) {
        if (record.GetData().HasDatabaseFeatureFlagsChange()) {
            result.push_back(record.GetData().GetDatabaseFeatureFlagsChange());
        }
    }
    return result;
}

} // namespace

Y_UNIT_TEST_SUITE(DatabaseFeatureFlagPatch) {
    Y_UNIT_TEST(InheritsUnrelatedClusterFlags) {
        const auto patch = PatchDatabaseFeatureFlags({}, MakePatch(false));
        UNIT_ASSERT(patch.Modified);
        UNIT_ASSERT_STRING_CONTAINS(patch.Config, "!inherit");
        NKikimrConfig::TAppConfig resolved;
        NYamlConfig::ResolveAndParseYamlConfig(MainConfig, {}, {}, resolved, patch.Config);
        UNIT_ASSERT(!resolved.GetFeatureFlags().GetEnableClientModifications());
        UNIT_ASSERT(resolved.GetFeatureFlags().GetEnableUnsafeTruncateTable());

        const auto reset = PatchDatabaseFeatureFlags(patch.Config, MakePatch(std::nullopt));
        resolved.Clear();
        NYamlConfig::ResolveAndParseYamlConfig(MainConfig, {}, {}, resolved, reset.Config);
        UNIT_ASSERT(resolved.GetFeatureFlags().GetEnableClientModifications());
        UNIT_ASSERT(resolved.GetFeatureFlags().GetEnableUnsafeTruncateTable());
    }

    Y_UNIT_TEST(PreservesUnknownFieldsTagsAndSelectorPrecedence) {
        const TString input = R"(
metadata:
  kind: DatabaseConfig
  database: /dc-1/users/tenant-1
  version: 4
config:
  feature_flags: !inherit
    enable_client_modifications: true
    future_feature_flag: true
  private_database_config:
    preserved: {value: 17}
allowed_labels:
  deployment: {type: string}
selector_config:
- description: existing override
  selector: {deployment: selected}
  config:
    feature_flags: !inherit
      enable_client_modifications: true
)";
        const auto patch = PatchDatabaseFeatureFlags(input, MakePatch(false));
        auto before = NFyaml::TDocument::Parse(input);
        auto after = NFyaml::TDocument::Parse(patch.Config);
        UNIT_ASSERT(before.Root().Map().at("selector_config").DeepEqual(after.Root().Map().at("selector_config")));
        UNIT_ASSERT(before.Root().Map().at("config").Map().at("private_database_config").DeepEqual(
            after.Root().Map().at("config").Map().at("private_database_config")));
        UNIT_ASSERT_STRING_CONTAINS(patch.Config, "future_feature_flag: true");
        NKikimrConfig::TAppConfig resolved;
        NYamlConfig::ResolveAndParseYamlConfig(MainConfig, {}, {{"deployment", "selected"}}, resolved, patch.Config);
        UNIT_ASSERT(resolved.GetFeatureFlags().GetEnableClientModifications());
    }

    Y_UNIT_TEST(NoOpRetainsDocumentAndResetDoesNotCreateConfig) {
        const auto patch = PatchDatabaseFeatureFlags({}, MakePatch(false));
        const auto noOp = PatchDatabaseFeatureFlags(patch.Config, MakePatch(false));
        UNIT_ASSERT(!noOp.Modified);
        UNIT_ASSERT_VALUES_EQUAL(noOp.Config, patch.Config);
        const auto resetAbsent = PatchDatabaseFeatureFlags({}, MakePatch(std::nullopt));
        UNIT_ASSERT(!resetAbsent.Modified);
        UNIT_ASSERT(resetAbsent.Config.empty());
    }

    Y_UNIT_TEST(RejectsUnsupportedFlagsAndDuplicateNames) {
        auto request = MakePatch(false);
        request.AddChanges()->SetName("ENABLE_UNSAFE_TRUNCATE_TABLE");
        UNIT_ASSERT_EXCEPTION(PatchDatabaseFeatureFlags({}, request), yexception);
        request.MutableChanges(1)->SetName("enable_client_modifications");
        UNIT_ASSERT_EXCEPTION(PatchDatabaseFeatureFlags({}, request), yexception);
        request.MutableChanges(1)->SetName("DOES_NOT_EXIST");
        UNIT_ASSERT_EXCEPTION(PatchDatabaseFeatureFlags({}, request), yexception);
        UNIT_ASSERT(!FindDatabaseSqlFeatureFlag("ENABLE_COMPUTE_SCHEDULER_MEMORY"));
    }

    Y_UNIT_TEST(RejectsReplacementMapping) {
        UNIT_ASSERT_EXCEPTION(PatchDatabaseFeatureFlags(
            "config: {feature_flags: {enable_client_modifications: true}}", MakePatch(false)), yexception);
    }

    Y_UNIT_TEST(RejectsMergeKeysBeforeNoOpDetection) {
        const TString mergedConfig = R"(
config:
  private_database_config: &defaults
    feature_flags: !inherit
      enable_client_modifications: false
  <<: *defaults
)";
        for (const auto& config : {MergedFlagConfig, mergedConfig}) {
            // Both are valid runtime YAML. RESET must not claim inheritance
            // merely because the flag is absent from the unresolved mapping.
            NKikimrConfig::TAppConfig resolved;
            NYamlConfig::ResolveAndParseYamlConfig(MainConfig, {}, {}, resolved, config);
            UNIT_ASSERT(!resolved.GetFeatureFlags().GetEnableClientModifications());
            for (const auto value : {std::optional<bool>{}, std::optional<bool>{false}, std::optional<bool>{true}}) {
                UNIT_ASSERT_EXCEPTION_CONTAINS(PatchDatabaseFeatureFlags(config, MakePatch(value)),
                    yexception, "must not use YAML merge keys");
            }
            const auto audit = DescribeDatabaseFeatureFlagPatch(config, MakePatch(std::nullopt));
            UNIT_ASSERT_VALUES_EQUAL(audit.OldOverridesSize(), 1);
            UNIT_ASSERT(audit.GetOldOverrides(0).HasValue());
            UNIT_ASSERT(!audit.GetOldOverrides(0).GetValue());
            UNIT_ASSERT(audit.GetNewOverrides(0).HasValue());
            UNIT_ASSERT(!audit.GetNewOverrides(0).GetValue());
        }
    }

    Y_UNIT_TEST(PreservesUnrelatedMergeKeys) {
        const TString config = R"(
config:
  feature_flags: !inherit
    enable_client_modifications: true
  private_database_config:
    defaults: &defaults {value: 17}
    merged:
      <<: *defaults
      extra: 42
)";
        const auto patch = PatchDatabaseFeatureFlags(config, MakePatch(false));
        auto before = NFyaml::TDocument::Parse(config);
        auto after = NFyaml::TDocument::Parse(patch.Config);
        UNIT_ASSERT(before.Root().Map().at("config").Map().at("private_database_config").DeepEqual(
            after.Root().Map().at("config").Map().at("private_database_config")));
        UNIT_ASSERT_STRING_CONTAINS(patch.Config, "<<: *defaults");
        NKikimrConfig::TAppConfig resolved;
        NYamlConfig::ResolveAndParseYamlConfig(MainConfig, {}, {}, resolved, patch.Config);
        UNIT_ASSERT(!resolved.GetFeatureFlags().GetEnableClientModifications());
        after.Resolve();
        UNIT_ASSERT_VALUES_EQUAL(after.Root().Map().at("config").Map().at("private_database_config")
            .Map().at("merged").Map().at("value").Scalar(), "17");
    }

    Y_UNIT_TEST(RejectsResetOfPersistedMergedOverride) {
        TPatchTestRuntime runtime;
        CheckCreateTenant(runtime, TENANT1_1_NAME, Ydb::StatusIds::SUCCESS, {{"hdd", 1}});
        CheckReplaceConfig(runtime, Ydb::StatusIds::SUCCESS, MainConfig);
        CheckForceReplaceDatabaseConfig(runtime, Ydb::StatusIds::SUCCESS, MergedFlagConfig);
        TString before;
        FetchDatabaseConfigFromConsole(runtime, TENANT1_1_NAME, before);
        const auto version = NYamlConfig::GetDatabaseMetadata(before).Version.value();
        const auto response = SendPatch(runtime, MakePatch(std::nullopt), Ydb::StatusIds::BAD_REQUEST);
        UNIT_ASSERT_STRING_CONTAINS(response.ShortDebugString(), "must not use YAML merge keys");
        UNIT_ASSERT_VALUES_EQUAL(response.GetVersion(), version);
        TString after;
        FetchDatabaseConfigFromConsole(runtime, TENANT1_1_NAME, after);
        UNIT_ASSERT_VALUES_EQUAL(after, before);
        UNIT_ASSERT(ReadPatchHistory(runtime).empty());
        NKikimrConfig::TAppConfig resolved;
        NYamlConfig::ResolveAndParseYamlConfig(MainConfig, {}, {}, resolved, after);
        UNIT_ASSERT(!resolved.GetFeatureFlags().GetEnableClientModifications());
    }

    Y_UNIT_TEST(PersistsVersionAndNoOpAuditAcrossRestart) {
        TPatchTestRuntime runtime;
        CheckCreateTenant(runtime, TENANT1_1_NAME, Ydb::StatusIds::SUCCESS, {{"hdd", 1}});
        CheckReplaceConfig(runtime, Ydb::StatusIds::SUCCESS, MainConfig);
        UNIT_ASSERT_VALUES_EQUAL(SendPatch(runtime, MakePatch(false)).GetVersion(), 1);
        UNIT_ASSERT_VALUES_EQUAL(SendPatch(runtime, MakePatch(false)).GetVersion(), 1);
        auto history = ReadPatchHistory(runtime);
        UNIT_ASSERT_VALUES_EQUAL(history.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(history.back().GetVersion(), 1);
        UNIT_ASSERT_VALUES_EQUAL(history.back().GetRequestId(), "sql-feature-flags-test");
        UNIT_ASSERT(history.back().GetOldOverrides(0).HasValue());
        UNIT_ASSERT(!history.back().GetOldOverrides(0).GetValue());

        RebootTablet(runtime, MakeConsoleID(), runtime.Sender);
        TString persisted;
        FetchDatabaseConfigFromConsole(runtime, TENANT1_1_NAME, persisted);
        UNIT_ASSERT_STRING_CONTAINS(persisted, "enable_client_modifications: false");
        UNIT_ASSERT_VALUES_EQUAL(NYamlConfig::GetDatabaseMetadata(persisted).Version.value(), 1);
        UNIT_ASSERT_VALUES_EQUAL(ReadPatchHistory(runtime).size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(SendPatch(runtime, MakePatch(std::nullopt)).GetVersion(), 2);
    }

    Y_UNIT_TEST(PreservesQueuedConfigReplacement) {
        TPatchTestRuntime runtime;
        CheckCreateTenant(runtime, TENANT1_1_NAME, Ydb::StatusIds::SUCCESS, {{"hdd", 1}});
        CheckReplaceConfig(runtime, Ydb::StatusIds::SUCCESS, MainConfig);
        SendPatch(runtime, MakePatch(false));
        const TString replacement = R"(
metadata:
  kind: DatabaseConfig
  database: /dc-1/users/tenant-1
  version: 1
config:
  feature_flags: !inherit
    enable_client_modifications: false
    future_flag: true
  private_database_config:
    preserved: 42
)";
        const auto pipe = runtime.ConnectToPipe(MakeConsoleID(), runtime.Sender, 0, GetPipeConfigWithRetries());
        auto replace = MakeHolder<TEvConsole::TEvReplaceYamlConfigRequest>();
        replace->Record.MutableRequest()->set_config(replacement);
        replace->Record.MutableRequest()->set_allow_unknown_fields(true);
        auto patch = MakeHolder<TEvConsole::TEvPatchDatabaseConfigRequest>();
        patch->Record.CopyFrom(MakePatch(true));
        // Queue both requests before dispatching either response. The patch must
        // read the replacement committed ahead of it, including unrelated fields.
        runtime.SendToPipe(pipe, runtime.Sender, replace.Release());
        runtime.SendToPipe(pipe, runtime.Sender, patch.Release());
        TAutoPtr<IEventHandle> handle;
        auto [success, error] = runtime.GrabEdgeEventsRethrow<TEvConsole::TEvReplaceYamlConfigResponse,
            TEvConsole::TEvGenericError>(handle, TDuration::Seconds(5));
        UNIT_ASSERT_C(success, (error ? error->Record.ShortDebugString() : "No replacement response"));
        auto response = runtime.GrabEdgeEventRethrow<TEvConsole::TEvPatchDatabaseConfigResponse>(runtime.Sender);
        UNIT_ASSERT_VALUES_EQUAL_C(response->Get()->Record.GetYdbStatus(), Ydb::StatusIds::SUCCESS,
            response->Get()->Record.ShortDebugString());
        UNIT_ASSERT_VALUES_EQUAL(response->Get()->Record.GetVersion(), 3);
        runtime.ClosePipe(pipe, runtime.Sender, 0);
        TString persisted;
        FetchDatabaseConfigFromConsole(runtime, TENANT1_1_NAME, persisted);
        UNIT_ASSERT_STRING_CONTAINS(persisted, "future_flag: true");
        UNIT_ASSERT_STRING_CONTAINS(persisted, "preserved: 42");
        UNIT_ASSERT_STRING_CONTAINS(persisted, "enable_client_modifications: true");
    }

    Y_UNIT_TEST(RejectsInvalidRequestsWithoutChangingConfig) {
        TPatchTestRuntime runtime;
        CheckCreateTenant(runtime, TENANT1_1_NAME, Ydb::StatusIds::SUCCESS, {{"hdd", 1}});
        CheckReplaceConfig(runtime, Ydb::StatusIds::SUCCESS, MainConfig);
        SendPatch(runtime, MakePatch(false));
        auto request = MakePatch(true);
        request.AddChanges()->SetName("NOT_A_FLAG");
        SendPatch(runtime, request, Ydb::StatusIds::BAD_REQUEST);
        request = MakePatch(true);
        request.SetDeadlineUnixMilliSeconds(1);
        SendPatch(runtime, request, Ydb::StatusIds::TIMEOUT);
        request = MakePatch(true);
        request.SetUserToken("not a serialized token");
        SendPatch(runtime, request, Ydb::StatusIds::UNAUTHORIZED);
        request = MakePatch(true);
        request.SetDatabase("/" + DOMAIN1_NAME);
        SendPatch(runtime, request, Ydb::StatusIds::SCHEME_ERROR);
        runtime.GetAppData().AdministrationAllowedSIDs = {"cluster-admin"};
        request = MakePatch(true);
        request.SetUserToken(NACLib::TUserToken("ordinary-user", TVector<TString>{}).SerializeAsString());
        SendPatch(runtime, request, Ydb::StatusIds::UNAUTHORIZED);
        UNIT_ASSERT_VALUES_EQUAL(ReadPatchHistory(runtime).size(), 1);
    }

    Y_UNIT_TEST(RequiresDatabaseYamlGateAndMainConfig) {
        TPatchTestRuntime runtime;
        CheckCreateTenant(runtime, TENANT1_1_NAME, Ydb::StatusIds::SUCCESS, {{"hdd", 1}});
        SendPatch(runtime, MakePatch(false), Ydb::StatusIds::PRECONDITION_FAILED);
        CheckReplaceConfig(runtime, Ydb::StatusIds::SUCCESS, MainConfig);
        runtime.GetAppData().YamlConfigEnabled = false;
        SendPatch(runtime, MakePatch(false), Ydb::StatusIds::PRECONDITION_FAILED);
        runtime.GetAppData().YamlConfigEnabled = true;
        runtime.GetAppData().FeatureFlags.SetDatabaseYamlConfigAllowed(false);
        SendPatch(runtime, MakePatch(false), Ydb::StatusIds::PRECONDITION_FAILED);
    }

    Y_UNIT_TEST(AuthorizesOwnerAndOwnerGroupAndEmitsSecurityAudit) {
        std::vector<std::string> auditLines;
        TPatchTestRuntime runtime;
        auto writer = NAudit::CreateAuditWriter(Tests::CreateTestAuditLogBackends(auditLines));
        runtime.RegisterService(NAudit::MakeAuditServiceID(), runtime.Register(writer.release()));
        const auto ownerToken = NACLib::TUserToken("root@builtin", {}).SerializeAsString();
        CheckCreateTenant(runtime, TENANT1_1_NAME, ownerToken, Ydb::StatusIds::SUCCESS, {{"hdd", 1}});
        runtime.RunTenant(TENANT1_1_NAME);
        CheckReplaceConfig(runtime, Ydb::StatusIds::SUCCESS, MainConfig);
        // The database owner is deliberately absent from the cluster administrator list.
        runtime.GetAppData().AdministrationAllowedSIDs = {"cluster-admin"};
        runtime.GetAppData().FeatureFlags.SetEnableDatabaseAdmin(true);

        auto auditLine = [&](const TString& requestId) {
            const std::string needle = std::string("request_id=") + requestId.c_str();
            runtime.WaitFor(Cerr, "database feature flag audit", [&] {
                for (const auto& line : auditLines) {
                    if (line.find(needle) != std::string::npos) {
                        return true;
                    }
                }
                return false;
            }, TDuration::Seconds(5));
            return Tests::FindAuditLine(auditLines, needle);
        };

        auto request = MakePatch(false);
        request.SetUserToken(ownerToken);
        request.SetPeerName("ipv4:127.0.0.1:10000");
        request.SetRequestId("owner-set");
        UNIT_ASSERT_VALUES_EQUAL(SendPatch(runtime, request).GetVersion(), 1);
        const auto success = auditLine("owner-set");
        UNIT_ASSERT_STRING_CONTAINS(success, "operation=ALTER DATABASE FEATURE FLAGS");
        UNIT_ASSERT_STRING_CONTAINS(success, "subject=root@builtin");
        UNIT_ASSERT_STRING_CONTAINS(success, "status=SUCCESS");
        UNIT_ASSERT_STRING_CONTAINS(success, "old_overrides=ENABLE_CLIENT_MODIFICATIONS=INHERIT");
        UNIT_ASSERT_STRING_CONTAINS(success, "new_overrides=ENABLE_CLIENT_MODIFICATIONS=false");
        UNIT_ASSERT_STRING_CONTAINS(success, "config_version=1");
        UNIT_ASSERT_STRING_CONTAINS(success, "no_op=false");

        request.SetRequestId("owner-noop");
        UNIT_ASSERT_VALUES_EQUAL(SendPatch(runtime, request).GetVersion(), 1);
        const auto noOp = auditLine("owner-noop");
        UNIT_ASSERT_STRING_CONTAINS(noOp, "status=SUCCESS");
        UNIT_ASSERT_STRING_CONTAINS(noOp, "old_overrides=ENABLE_CLIENT_MODIFICATIONS=false");
        UNIT_ASSERT_STRING_CONTAINS(noOp, "config_version=1");
        UNIT_ASSERT_STRING_CONTAINS(noOp, "no_op=true");

        request = MakePatch(true);
        request.SetRequestId("owner-group");
        request.SetUserToken(NACLib::TUserToken("member@builtin", {"root@builtin"}).SerializeAsString());
        UNIT_ASSERT_VALUES_EQUAL(SendPatch(runtime, request).GetVersion(), 2);
        UNIT_ASSERT_STRING_CONTAINS(auditLine("owner-group"), "subject=member@builtin");

        request = MakePatch(false);
        request.SetRequestId("invalid-batch");
        request.SetUserToken(ownerToken);
        request.AddChanges()->SetName("NOT_A_FLAG");
        SendPatch(runtime, request, Ydb::StatusIds::BAD_REQUEST);
        const auto failure = auditLine("invalid-batch");
        UNIT_ASSERT_STRING_CONTAINS(failure, "detailed_status=BAD_REQUEST");
        UNIT_ASSERT_STRING_CONTAINS(failure, "old_overrides=ENABLE_CLIENT_MODIFICATIONS=true");
        UNIT_ASSERT_STRING_CONTAINS(failure, "new_overrides=ENABLE_CLIENT_MODIFICATIONS=true");
        UNIT_ASSERT_STRING_CONTAINS(failure, "config_version=2");

        request = MakePatch(false);
        request.SetRequestId("denied-user");
        request.SetUserToken(NACLib::TUserToken("ordinary@builtin", {}).SerializeAsString());
        SendPatch(runtime, request, Ydb::StatusIds::UNAUTHORIZED);
        const auto denial = auditLine("denied-user");
        UNIT_ASSERT_STRING_CONTAINS(denial, "status=ERROR");
        UNIT_ASSERT_STRING_CONTAINS(denial, "detailed_status=UNAUTHORIZED");
        UNIT_ASSERT_STRING_CONTAINS(denial, "subject=ordinary@builtin");
        UNIT_ASSERT_STRING_CONTAINS(denial, "old_overrides=ENABLE_CLIENT_MODIFICATIONS=true");
        UNIT_ASSERT_STRING_CONTAINS(denial, "new_overrides=ENABLE_CLIENT_MODIFICATIONS=true");
        UNIT_ASSERT_STRING_CONTAINS(denial, "config_version=2");
        UNIT_ASSERT_VALUES_EQUAL(ReadPatchHistory(runtime).size(), 3);
    }

    Y_UNIT_TEST(RejectsSharedProviderAndServerlessTenant) {
        TPatchTestRuntime runtime;
        CheckCreateTenant(runtime, Ydb::StatusIds::SUCCESS,
            TCreateTenantRequest(TENANT1_1_NAME, TCreateTenantRequest::EType::Shared).WithPools({{"hdd", 1}}));
        runtime.RunTenant(TENANT1_1_NAME);
        CheckCreateTenant(runtime, Ydb::StatusIds::SUCCESS,
            TCreateTenantRequest(TENANT1_2_NAME).WithSharedDbPath(TENANT1_1_NAME));
        CheckReplaceConfig(runtime, Ydb::StatusIds::SUCCESS, MainConfig);
        SendPatch(runtime, MakePatch(false), Ydb::StatusIds::PRECONDITION_FAILED);
        auto request = MakePatch(false);
        request.SetDatabase(TENANT1_2_NAME);
        SendPatch(runtime, request, Ydb::StatusIds::PRECONDITION_FAILED);
    }
}

} // namespace NKikimr::NConsole::NUT
