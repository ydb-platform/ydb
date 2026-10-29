#include <ydb/core/backup/common/checksum.h>
#include <ydb/core/backup/common/metadata.h>
#include <ydb/core/kesus/tablet/events.h>
#include <ydb/core/testlib/actors/block_events.h>
#include <ydb/core/tx/schemeshard/schemeshard_private.h>
#include <ydb/core/tx/schemeshard/ut_helpers/helpers.h>
#include <ydb/core/tx/schemeshard/ut_helpers/test_with_reboots.h>
#include <ydb/core/wrappers/ut_helpers/s3_mock.h>
#include <ydb/core/ydb_convert/kesus_description.h>
#include <ydb/public/api/protos/ydb_coordination.pb.h>
#include <ydb/public/api/protos/ydb_rate_limiter.pb.h>

#include <google/protobuf/text_format.h>
#include <google/protobuf/util/message_differencer.h>

#include <util/folder/tempdir.h>
#include <util/string/builder.h>

#include <algorithm>
#include <utility>

using namespace NKikimr;
using namespace NKikimr::NSchemeShard;
using namespace NKikimr::NWrappers::NTestHelpers;
using namespace NSchemeShardUT_Private;
using namespace NActors;

namespace {

namespace TEvKesus = NKikimr::NKesus::TEvKesus;

const TString ResourcesKey = "/backup/coordination/rate_limiter_resources.pb";
const TString NodeKey = "/backup/coordination/create_coordination_node.pb";
const TString PermissionsKey = "/backup/coordination/permissions.pb";

Ydb::RateLimiter::Resources MakeResources(bool metering = false) {
    Ydb::RateLimiter::Resources result;
    // Several roots and three levels, deliberately not in lexical order.
    for (const TString& path : {"z", "a", "a/z", "a/a", "a/a/leaf"}) {
        auto& resource = *result.add_resources();
        resource.set_resource_path(path);
        auto& drr = *resource.mutable_hierarchical_drr();
        if (path.find('/') == TString::npos) {
            drr.set_max_units_per_second(100);
            drr.set_max_burst_size_coefficient(1.5);
            drr.set_prefetch_coefficient(0.4);
            drr.set_prefetch_watermark(0.6);
            if (metering) {
                auto& config = *resource.mutable_metering_config();
                config.set_enabled(false);
                config.set_report_period_ms(5000);
                config.set_meter_period_ms(1000);
                config.set_collect_period_sec(30);
                config.set_provisioned_units_per_second(25);
                auto& metric = *config.mutable_provisioned();
                metric.set_enabled(false);
                (*metric.mutable_labels())["kind"] = "test";
                (*metric.mutable_metric_fields()->mutable_fields())["cloud_id"].set_string_value("cloud");
            }
        } else if (path.EndsWith("leaf")) {
            drr.set_immediately_fill_up_to(-123);
            drr.mutable_replicated_bucket()->set_report_interval_ms(321);
        }
    }
    return result;
}

TString ResourcesText(const Ydb::RateLimiter::Resources& resources) {
    if (resources.resources().empty()) {
        return "resources: []\n";
    }
    return resources.DebugString();
}

Ydb::RateLimiter::Resources NormalizeResources(const Ydb::RateLimiter::Resources& resources) {
    Ydb::RateLimiter::Resources result;
    for (const auto& resource : resources.resources()) {
        NKikimrKesus::TStreamingQuoterResource internal;
        FillRateLimiterDescription(internal, resource);
        FillRateLimiterDescription(*result.add_resources(), internal);
    }
    auto& items = *result.mutable_resources();
    std::sort(items.begin(), items.end(), [](const auto& lhs, const auto& rhs) {
        return lhs.resource_path() < rhs.resource_path();
    });
    return result;
}

class TKesusImportEnv {
public:
    TKesusImportEnv()
        : Port(Ports.GetPort())
        , S3({}, TS3Mock::TSettings(Port))
        , Env(Runtime, TTestEnvOptions().EnablePermissionsExport(true).EnableChecksumsExport(true))
    {
        UNIT_ASSERT(S3.Start());
        Runtime.GetAppData().FeatureFlags.SetEnableFsBackups(true);
        Runtime.GetAppData().FeatureFlags.SetEnableEncryptedExport(true);
        Runtime.GetAppData().FeatureFlags.SetEnableExportFiltering(true);
    }

    ui64 CreateNode(const TString& name = "Original") {
        TestCreateKesus(Runtime, ++TxId, "/MyRoot", TStringBuilder() << R"(
            Name: ")" << name << R"("
            Config {
                self_check_period_millis: 1234
                session_grace_period_millis: 5678
                read_consistency_mode: CONSISTENCY_MODE_STRICT
                attach_consistency_mode: CONSISTENCY_MODE_RELAXED
                rate_limiter_counters_mode: RATE_LIMITER_COUNTERS_MODE_DETAILED
            }
        )");
        Env.TestWaitNotification(Runtime, TxId);
        return TabletId("/MyRoot/" + name);
    }

    ui64 TabletId(const TString& path = "/MyRoot/Restored") {
        const auto description = DescribePath(Runtime, path);
        UNIT_ASSERT_VALUES_EQUAL(description.GetStatus(), NKikimrScheme::StatusSuccess);
        UNIT_ASSERT(description.GetPathDescription().HasKesus());
        const ui64 id = description.GetPathDescription().GetKesus().GetKesusTabletId();
        UNIT_ASSERT(id);
        return id;
    }

    void AddResource(ui64 tabletId, const NKikimrKesus::TStreamingQuoterResource& resource) {
        auto request = MakeHolder<TEvKesus::TEvAddQuoterResource>();
        *request->Record.MutableResource() = resource;
        const auto sender = Runtime.AllocateEdgeActor();
        Runtime.SendToPipe(tabletId, sender, request.Release(), 0, GetPipeConfigWithRetries());
        auto response = Runtime.GrabEdgeEventRethrow<TEvKesus::TEvAddQuoterResourceResult>(sender);
        UNIT_ASSERT_VALUES_EQUAL_C(response->Get()->Record.GetError().GetStatus(), Ydb::StatusIds::SUCCESS,
            response->Get()->Record.DebugString());
    }

    void Populate(ui64 tabletId, const Ydb::RateLimiter::Resources& resources) {
        for (const auto& resource : resources.resources()) {
            NKikimrKesus::TStreamingQuoterResource internal;
            FillRateLimiterDescription(internal, resource);
            AddResource(tabletId, internal);
        }
    }

    Ydb::RateLimiter::Resources DescribeResources(const TString& path = "/MyRoot/Restored") {
        const auto sender = Runtime.AllocateEdgeActor();
        auto request = MakeHolder<TEvKesus::TEvDescribeQuoterResources>();
        request->Record.SetRecursive(true);
        Runtime.SendToPipe(TabletId(path), sender, request.Release(), 0, GetPipeConfigWithRetries());
        auto response = Runtime.GrabEdgeEventRethrow<TEvKesus::TEvDescribeQuoterResourcesResult>(sender);
        UNIT_ASSERT_VALUES_EQUAL_C(response->Get()->Record.GetError().GetStatus(), Ydb::StatusIds::SUCCESS,
            response->Get()->Record.DebugString());
        Ydb::RateLimiter::Resources result;
        for (const auto& resource : response->Get()->Record.GetResources()) {
            FillRateLimiterDescription(*result.add_resources(), resource);
        }
        return NormalizeResources(result);
    }

    void AssertResources(const Ydb::RateLimiter::Resources& expected) {
        const auto normalized = NormalizeResources(expected);
        const auto actual = DescribeResources();
        UNIT_ASSERT_C(google::protobuf::util::MessageDifferencer::Equals(normalized, actual),
            "Expected: " << normalized.DebugString() << "; actual: " << actual.DebugString());
    }

    void WriteFile(const TString& key, const TString& content, bool checksum = true) {
        S3.GetData()[key] = content;
        if (checksum) {
            S3.GetData()[key + ".sha256"] = NBackup::ComputeChecksum(content);
        }
    }

    void WriteBackup(const TString& resources, bool permissions = false) {
        NBackup::TMetadata metadata;
        metadata.SetVersion(1);
        metadata.SetEnablePermissions(permissions);
        WriteFile("/backup/coordination/metadata.json", metadata.Serialize());
        WriteFile(NodeKey, R"(config { self_check_period_millis: 1234 session_grace_period_millis: 5678 })");
        WriteFile(ResourcesKey, resources);
        if (permissions) {
            WriteFile(PermissionsKey, R"(
                actions { change_owner: "backup-owner" }
                actions { grant { subject: "reader" permission_names: "ydb.generic.read" } }
            )");
        }
    }

    void Export(bool fs, bool encrypted, bool table = false) {
        NKikimrExport::TCreateExportRequest request;
        auto configure = [encrypted](auto* settings) {
            settings->add_items()->set_source_path("/MyRoot/Original");
            if (encrypted) {
                settings->mutable_encryption_settings()->set_encryption_algorithm("AES-128-GCM");
                settings->mutable_encryption_settings()->mutable_symmetric_key()->set_key("0123456789012345");
            }
        };
        if (fs) {
            auto* settings = request.MutableExportToFsSettings();
            configure(settings);
            settings->set_base_path(TempDir.Path());
            settings->mutable_items(0)->set_destination_path("coordination");
        } else {
            auto* settings = request.MutableExportToS3Settings();
            configure(settings);
            settings->set_endpoint(TStringBuilder() << "localhost:" << Port);
            settings->set_scheme(Ydb::Export::ExportToS3Settings::HTTP);
            settings->set_destination_prefix("backup");
            settings->mutable_items(0)->set_destination_prefix("coordination");
            if (table) {
                auto* item = settings->add_items();
                item->set_source_path("/MyRoot/Table");
                item->set_destination_prefix("table");
            }
        }
        TestExport(Runtime, ++TxId, "/MyRoot", request.DebugString());
        Env.TestWaitNotification(Runtime, TxId);
        TestGetExport(Runtime, TxId, "/MyRoot", Ydb::StatusIds::SUCCESS);
    }

    NKikimrImport::TCreateImportRequest MakeImport(bool fs = false, bool encrypted = false) {
        NKikimrImport::TCreateImportRequest request;
        auto configure = [encrypted](auto* settings) {
            settings->set_number_of_retries(2);
            settings->add_items()->set_destination_path("/MyRoot/Restored");
            if (encrypted) {
                settings->mutable_encryption_settings()->set_encryption_algorithm("AES-128-GCM");
                settings->mutable_encryption_settings()->mutable_symmetric_key()->set_key("0123456789012345");
            }
        };
        if (fs) {
            auto* settings = request.MutableImportFromFsSettings();
            configure(settings);
            settings->set_base_path(TempDir.Path());
            settings->mutable_items(0)->set_source_path("coordination");
        } else {
            auto* settings = request.MutableImportFromS3Settings();
            configure(settings);
            settings->set_endpoint(TStringBuilder() << "localhost:" << Port);
            settings->set_scheme(Ydb::Import::ImportFromS3Settings::HTTP);
            if (encrypted) {
                settings->set_source_prefix("backup");
                settings->mutable_items(0)->set_source_prefix("coordination");
            } else {
                settings->mutable_items(0)->set_source_prefix("backup/coordination");
            }
        }
        return request;
    }

    ui64 StartImport(const NKikimrImport::TCreateImportRequest& request, bool allowMetering = false,
        Ydb::StatusIds::StatusCode expected = Ydb::StatusIds::SUCCESS)
    {
        const ui64 id = ++TxId;
        auto event = MakeHolder<TEvImport::TEvCreateImportRequest>(id, "/MyRoot", request);
        event->Record.SetUserSID("importer@builtin");
        event->Record.SetCanRestoreRateLimiterMetering(allowMetering);
        AsyncSend(Runtime, TTestTxConfig::SchemeShard, event.Release());
        TAutoPtr<IEventHandle> handle;
        const auto* response = Runtime.GrabEdgeEvent<TEvImport::TEvCreateImportResponse>(handle);
        const auto& entry = response->Record.GetResponse().GetEntry();
        UNIT_ASSERT_VALUES_EQUAL_C(entry.GetStatus(), expected, entry.DebugString());
        return id;
    }

    void Finish(ui64 id, Ydb::StatusIds::StatusCode status = Ydb::StatusIds::SUCCESS, TStringBuf issue = {}) {
        Env.TestWaitNotification(Runtime, id);
        const auto response = TestGetImport(Runtime, id, "/MyRoot", status);
        if (issue) {
            UNIT_ASSERT_STRING_CONTAINS(response.DebugString(), issue);
        }
        if (status == Ydb::StatusIds::SUCCESS) {
            UNIT_ASSERT_VALUES_EQUAL(response.GetResponse().GetEntry().GetProgress(), Ydb::Import::ImportProgress::PROGRESS_DONE);
        }
    }

    void AssertNotCreated() {
        TestDescribeResult(DescribePath(Runtime, "/MyRoot/Restored"), {NLs::PathNotExist});
    }

    void RebootSchemeShard() {
        RebootTablet(Runtime, TTestTxConfig::SchemeShard, Runtime.AllocateEdgeActor());
    }

    TPortManager Ports;
    const ui16 Port;
    TS3Mock S3;
    TTempDir TempDir;
    TTestBasicRuntime Runtime;
    TTestEnv Env;
    ui64 TxId = 100;
};

void TestRoundTrip(bool fs, bool encrypted, bool empty) {
    TKesusImportEnv t;
    const auto resources = empty ? Ydb::RateLimiter::Resources() : MakeResources(true);
    const ui64 originalTablet = t.CreateNode();
    t.Populate(originalTablet, resources);
    t.Export(fs, encrypted);
    t.Finish(t.StartImport(t.MakeImport(fs, encrypted), true));
    t.AssertResources(resources);
    UNIT_ASSERT(t.TabletId() != originalTablet);
    auto original = DescribePath(t.Runtime, "/MyRoot/Original").GetPathDescription().GetKesus().GetConfig();
    auto restored = DescribePath(t.Runtime, "/MyRoot/Restored").GetPathDescription().GetKesus().GetConfig();
    original.clear_path();
    restored.clear_path();
    UNIT_ASSERT(google::protobuf::util::MessageDifferencer::Equals(original, restored));
}

void RejectResources(const TString& text, TStringBuf issue) {
    TKesusImportEnv t;
    t.WriteBackup(text);
    t.Finish(t.StartImport(t.MakeImport()), Ydb::StatusIds::CANCELLED, issue);
    t.AssertNotCreated();
}

bool IsKesusCreation(const TEvSchemeShard::TEvModifySchemeTransaction::TPtr& ev) {
    const auto& record = ev->Get()->Record;
    return record.TransactionSize() && record.GetTransaction(0).GetOperationType() == NKikimrSchemeOp::ESchemeOpCreateKesus
        && record.GetTransaction(0).GetKesus().GetName() == "Restored";
}

} // namespace

Y_UNIT_TEST_SUITE(TKesusImportTests) {
    Y_UNIT_TEST_FLAGS(RoundTrip, Fs, Encrypted) {
        TestRoundTrip(Fs, Encrypted, false);
    }

    Y_UNIT_TEST_FLAGS(EmptyRoundTrip, Fs, Encrypted) {
        TestRoundTrip(Fs, Encrypted, true);
    }

    Y_UNIT_TEST(UnsortedResources) {
        TKesusImportEnv t;
        const auto resources = MakeResources();
        t.WriteBackup(ResourcesText(resources));
        t.Finish(t.StartImport(t.MakeImport()));
        t.AssertResources(resources);
    }

    Y_UNIT_TEST(MissingResources) {
        TKesusImportEnv t;
        t.WriteBackup("resources: []\n");
        t.S3.GetData().erase(ResourcesKey);
        t.Finish(t.StartImport(t.MakeImport()), Ydb::StatusIds::CANCELLED, "no rate limiter resources file found");
        t.AssertNotCreated();
    }

    Y_UNIT_TEST(EmptyResourcesFile) {
        RejectResources("", "empty rate limiter resources file");
    }

    Y_UNIT_TEST(CorruptResources) {
        RejectResources("resources {", "cannot parse rate limiter resources");
    }

    Y_UNIT_TEST(UnknownContainerField) {
        RejectResources("resoruces: []", "cannot parse rate limiter resources");
    }

    Y_UNIT_TEST(UnknownResourceField) {
        RejectResources(R"(resources { resource_path: "a" hierarchical_drr { max_units_per_second: 10 } unknown: 1 })",
            "cannot parse rate limiter resources");
    }

    Y_UNIT_TEST(NoncanonicalPath) {
        RejectResources(R"(resources { resource_path: "/a/" hierarchical_drr { max_units_per_second: 10 } })",
            "Invalid or noncanonical resource path");
    }

    Y_UNIT_TEST(InvalidPathCharacters) {
        RejectResources(R"(resources { resource_path: "a b" hierarchical_drr { max_units_per_second: 10 } })",
            "Invalid or noncanonical resource path");
    }

    Y_UNIT_TEST(EmptyResourcePath) {
        RejectResources(R"(resources { hierarchical_drr { max_units_per_second: 10 } })", "Invalid or noncanonical resource path");
    }

    Y_UNIT_TEST(DuplicatePaths) {
        RejectResources(R"(
            resources { resource_path: "a" hierarchical_drr { max_units_per_second: 10 } }
            resources { resource_path: "a" hierarchical_drr { max_units_per_second: 20 } }
        )", "Duplicate resource path");
    }

    Y_UNIT_TEST(MissingParent) {
        RejectResources(R"(resources { resource_path: "a/b" hierarchical_drr {} })", "parent resource 'a' is missing");
    }

    Y_UNIT_TEST(MissingResourceType) {
        RejectResources(R"(resources { resource_path: "a" })", "Unsupported resource type or no resource properties");
    }

    Y_UNIT_TEST(InvalidNumericPropertiesLeaveCreatedNode) {
        TKesusImportEnv t;
        t.WriteBackup(R"(resources { resource_path: "a" hierarchical_drr { max_units_per_second: -1 } })");
        t.Finish(t.StartImport(t.MakeImport()), Ydb::StatusIds::CANCELLED, "MaxUnitsPerSecond");
        UNIT_ASSERT_VALUES_EQUAL(t.DescribeResources().resources_size(), 0);
    }

    Y_UNIT_TEST(InvalidChildPropertiesPreservePreviouslyRestoredParent) {
        TKesusImportEnv t;
        t.WriteBackup(R"(
            resources { resource_path: "a" hierarchical_drr { max_units_per_second: 100 } }
            resources { resource_path: "a/b" hierarchical_drr { max_units_per_second: -1 } }
        )");
        t.Finish(t.StartImport(t.MakeImport()), Ydb::StatusIds::CANCELLED, "MaxUnitsPerSecond");
        const auto resources = t.DescribeResources();
        UNIT_ASSERT_VALUES_EQUAL(resources.resources_size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(resources.resources(0).resource_path(), "a");
        UNIT_ASSERT_VALUES_EQUAL(resources.resources(0).hierarchical_drr().max_units_per_second(), 100);
    }

    Y_UNIT_TEST(MeteringRequiresPermissionEvenWhenDisabled) {
        RejectResources(ResourcesText(MakeResources(true)), "Setting metering is allowed only for administrators");
    }

    Y_UNIT_TEST(MeteringWithPermission) {
        TKesusImportEnv t;
        const auto resources = MakeResources(true);
        t.WriteBackup(ResourcesText(resources));
        t.Finish(t.StartImport(t.MakeImport(), true));
        t.AssertResources(resources);
    }

    Y_UNIT_TEST_FLAG(InvalidMetricFields, WrongType) {
        TKesusImportEnv t;
        auto resources = MakeResources(true);
        auto* fields = resources.mutable_resources(0)->mutable_metering_config()->mutable_provisioned()->mutable_metric_fields()->mutable_fields();
        if (WrongType) {
            (*fields)["cloud_id"].set_number_value(1);
        } else {
            (*fields)["unknown"].set_string_value("value");
        }
        t.WriteBackup(ResourcesText(resources));
        t.Finish(t.StartImport(t.MakeImport(), true), Ydb::StatusIds::CANCELLED,
            WrongType ? "Unsupported type for metric" : "Unsupported key for metric");
        t.AssertNotCreated();
    }

    Y_UNIT_TEST(UnknownNodeField) {
        TKesusImportEnv t;
        t.WriteBackup("resources: []\n");
        t.WriteFile(NodeKey, "unknown_node_field: 1");
        t.Finish(t.StartImport(t.MakeImport()), Ydb::StatusIds::CANCELLED);
        t.AssertNotCreated();
    }

    Y_UNIT_TEST(SourceNodePathDoesNotOverrideDestination) {
        TKesusImportEnv t;
        t.WriteBackup("resources: []\n");
        t.WriteFile(NodeKey, R"(
            path: "/OldDatabase/OldNode"
            config { path: "/OldDatabase/OldNode" self_check_period_millis: 1234 }
        )");
        t.Finish(t.StartImport(t.MakeImport()));
        t.AssertResources({});
        UNIT_ASSERT_VALUES_EQUAL(DescribePath(t.Runtime, "/MyRoot/Restored").GetPathDescription().GetKesus()
            .GetConfig().self_check_period_millis(), 1234);
    }

    Y_UNIT_TEST_FLAG(ResourceChecksum, SkipValidation) {
        TKesusImportEnv t;
        const auto resources = MakeResources();
        t.WriteBackup(ResourcesText(resources));
        t.S3.GetData()[ResourcesKey + ".sha256"] = "bad checksum";
        auto request = t.MakeImport();
        request.MutableImportFromS3Settings()->set_skip_checksum_validation(SkipValidation);
        const ui64 id = t.StartImport(request);
        if (SkipValidation) {
            t.Finish(id);
            t.AssertResources(resources);
        } else {
            t.Finish(id, Ydb::StatusIds::CANCELLED, "Checksum mismatch");
            t.AssertNotCreated();
        }
    }

    Y_UNIT_TEST(WrongEncryptionKey) {
        TKesusImportEnv t;
        t.CreateNode();
        t.Export(false, true);
        auto request = t.MakeImport(false, true);
        request.MutableImportFromS3Settings()->mutable_encryption_settings()->mutable_symmetric_key()->set_key("9876543210987654");
        t.Finish(t.StartImport(request), Ydb::StatusIds::CANCELLED);
        t.AssertNotCreated();
    }

    Y_UNIT_TEST(EncryptedFilesCannotBeSubstituted) {
        TKesusImportEnv t;
        t.CreateNode();
        t.Export(false, true);
        // The key is correct, but the file-type IV belongs to the node configuration.
        t.S3.GetData()[ResourcesKey + ".enc"] = t.S3.GetData().at(NodeKey + ".enc");
        t.Finish(t.StartImport(t.MakeImport(false, true)), Ydb::StatusIds::CANCELLED);
        t.AssertNotCreated();
    }

    Y_UNIT_TEST_FLAG(Permissions, NoAcl) {
        TKesusImportEnv t;
        t.WriteBackup("resources: []\n", true);
        auto request = t.MakeImport();
        request.MutableImportFromS3Settings()->set_no_acl(NoAcl);
        t.Finish(t.StartImport(request));
        const auto description = DescribePath(t.Runtime, "/MyRoot/Restored");
        if (NoAcl) {
            TestDescribeResult(description, {NLs::HasOwner("importer@builtin")});
        } else {
            TestDescribeResult(description, {NLs::HasOwner("backup-owner"), NLs::HasRight("+R:reader")});
        }
    }

    Y_UNIT_TEST_FLAG(MissingPermissions, NoAcl) {
        TKesusImportEnv t;
        t.WriteBackup("resources: []\n", true);
        t.S3.GetData().erase(PermissionsKey);
        auto request = t.MakeImport();
        request.MutableImportFromS3Settings()->set_no_acl(NoAcl);
        t.Finish(t.StartImport(request), NoAcl ? Ydb::StatusIds::SUCCESS : Ydb::StatusIds::CANCELLED);
        if (!NoAcl) {
            t.AssertNotCreated();
        }
    }

    Y_UNIT_TEST(LegacyOptionalPermissions) {
        TKesusImportEnv t;
        t.WriteBackup(ResourcesText(MakeResources()));
        t.WriteFile("/backup/coordination/metadata.json", R"({"version": 1})");
        t.Finish(t.StartImport(t.MakeImport()));
        t.AssertResources(MakeResources());
    }

    Y_UNIT_TEST(RebootBeforeSchemeReadyPreservesMeteringPermission) {
        TKesusImportEnv t;
        const auto resources = MakeResources(true);
        t.WriteBackup(ResourcesText(resources));
        TBlockEvents<TEvPrivate::TEvImportSchemeReady> replies(t.Runtime);
        const ui64 id = t.StartImport(t.MakeImport(), true);
        t.Runtime.WaitFor("metering validated before payload persisted", [&] { return !replies.empty(); });
        UNIT_ASSERT(replies.front()->Get()->Success);
        replies.Stop();
        t.RebootSchemeShard();
        // The new getter reads the backup again and must authorize it using the saved permission.
        t.Finish(id);
        t.AssertResources(resources);
        replies.Unblock();
        TestGetImport(t.Runtime, id, "/MyRoot", Ydb::StatusIds::SUCCESS);
    }

    Y_UNIT_TEST(RebootReplaysSavedResourcesAfterLostAddResponse) {
        TKesusImportEnv t;
        const auto resources = MakeResources(true);
        t.WriteBackup(ResourcesText(resources));
        TBlockEvents<TEvKesus::TEvAddQuoterResourceResult> replies(t.Runtime);
        const ui64 id = t.StartImport(t.MakeImport(), true);
        t.Runtime.WaitFor("first resource committed before reply", [&] { return !replies.empty(); });
        UNIT_ASSERT_VALUES_EQUAL(replies.front()->Get()->Record.GetError().GetStatus(), Ydb::StatusIds::SUCCESS);
        // The persisted payload must survive the restart even if the source is replaced.
        t.WriteFile(ResourcesKey, "resources: []\n");
        replies.Stop();
        t.RebootSchemeShard();
        t.Finish(id);
        t.AssertResources(resources);
        replies.Unblock();
        TestGetImport(t.Runtime, id, "/MyRoot", Ydb::StatusIds::SUCCESS);
    }

    Y_UNIT_TEST(LostAddResponseIsReconciledWithoutReboot) {
        TKesusImportEnv t;
        const auto resources = MakeResources(true);
        t.WriteBackup(ResourcesText(resources));
        auto timers = t.Runtime.AddObserver<TEvSchemeShard::TEvDescribeSchemeResult>([&](auto& ev) {
            if (ev->Get()->GetRecord().GetPathDescription().HasKesus()) {
                t.Runtime.EnableScheduleForActor(ev->Recipient);
            }
        });
        bool blockFirst = true;
        TBlockEvents<TEvKesus::TEvAddQuoterResourceResult> replies(t.Runtime, [&](const auto&) {
            return std::exchange(blockFirst, false);
        });
        ui32 rootAdds = 0;
        auto adds = t.Runtime.AddObserver<TEvKesus::TEvAddQuoterResource>([&](auto& ev) {
            rootAdds += ev->Get()->Record.GetResource().GetResourcePath() == "a";
        });
        const ui64 id = t.StartImport(t.MakeImport(), true);
        t.Finish(id);
        UNIT_ASSERT_VALUES_EQUAL(replies.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(rootAdds, 1); // Retry describes the committed Add instead of adding it again.
        t.AssertResources(resources);
        replies.Stop().Unblock();
        TestGetImport(t.Runtime, id, "/MyRoot", Ydb::StatusIds::SUCCESS);
    }

    Y_UNIT_TEST(ResourceRequestRetriesAreBounded) {
        TKesusImportEnv t;
        t.WriteBackup(ResourcesText(MakeResources()));
        auto timers = t.Runtime.AddObserver<TEvSchemeShard::TEvDescribeSchemeResult>([&](auto& ev) {
            if (ev->Get()->GetRecord().GetPathDescription().HasKesus()) {
                t.Runtime.EnableScheduleForActor(ev->Recipient);
            }
        });
        TBlockEvents<TEvKesus::TEvDescribeQuoterResourcesResult> replies(t.Runtime);
        auto request = t.MakeImport();
        request.MutableImportFromS3Settings()->set_number_of_retries(1);
        const ui64 id = t.StartImport(request);
        t.Finish(id, Ydb::StatusIds::CANCELLED, "Timed out restoring coordination node resources");
        UNIT_ASSERT_VALUES_EQUAL(replies.size(), 2); // Initial attempt plus the configured single retry.
        replies.Stop().Unblock();
        UNIT_ASSERT_VALUES_EQUAL(t.DescribeResources().resources_size(), 0);
        TestGetImport(t.Runtime, id, "/MyRoot", Ydb::StatusIds::CANCELLED);
    }

    Y_UNIT_TEST_FLAG(RebootBeforePersistingResourceCompletion, Empty) {
        TKesusImportEnv t;
        const auto resources = Empty ? Ydb::RateLimiter::Resources() : MakeResources();
        t.WriteBackup(ResourcesText(resources));
        TBlockEvents<TEvPrivate::TEvImportKesusResourcesResult> replies(t.Runtime);
        const ui64 id = t.StartImport(t.MakeImport());
        t.Runtime.WaitFor("resources restored before completion persisted", [&] { return !replies.empty(); });
        UNIT_ASSERT(replies.front()->Get()->Success);
        replies.Stop();
        t.RebootSchemeShard();
        t.Finish(id);
        t.AssertResources(resources);
        replies.Unblock();
        TestGetImport(t.Runtime, id, "/MyRoot", Ydb::StatusIds::SUCCESS);
    }

    Y_UNIT_TEST(RebootBeforeCreateProposalIsDelivered) {
        TKesusImportEnv t;
        t.WriteBackup(ResourcesText(MakeResources()));
        TBlockEvents<TEvSchemeShard::TEvModifySchemeTransaction> proposals(t.Runtime, IsKesusCreation);
        const ui64 id = t.StartImport(t.MakeImport());
        t.Runtime.WaitFor("create txid persisted before proposal delivery", [&] { return !proposals.empty(); });
        const ui64 createTxId = proposals.front()->Get()->Record.GetTxId();
        proposals.Stop();
        ui32 repeatedProposals = 0;
        auto observer = t.Runtime.AddObserver<TEvSchemeShard::TEvModifySchemeTransaction>([&](auto& ev) {
            if (IsKesusCreation(ev)) {
                UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Record.GetTxId(), createTxId);
                ++repeatedProposals;
            }
        });
        t.RebootSchemeShard();
        t.Finish(id);
        UNIT_ASSERT(repeatedProposals);
        t.AssertResources(MakeResources());
        proposals.Unblock();
        TestGetImport(t.Runtime, id, "/MyRoot", Ydb::StatusIds::SUCCESS);
    }

    Y_UNIT_TEST(RebootAfterNodeCreationBeforeAcceptedIsPersisted) {
        TKesusImportEnv t;
        t.WriteBackup(ResourcesText(MakeResources()));
        ui64 createTxId = 0;
        auto observer = t.Runtime.AddObserver<TEvSchemeShard::TEvModifySchemeTransaction>([&](auto& ev) {
            if (IsKesusCreation(ev)) {
                createTxId = ev->Get()->Record.GetTxId();
            }
        });
        TBlockEvents<TEvSchemeShard::TEvModifySchemeTransactionResult> replies(t.Runtime, [&](const auto& ev) {
            return createTxId && ev->Get()->Record.GetTxId() == createTxId;
        });
        const ui64 id = t.StartImport(t.MakeImport());
        t.Runtime.WaitFor("Accepted reply blocked", [&] { return !replies.empty(); });
        UNIT_ASSERT_VALUES_EQUAL(replies.front()->Get()->Record.GetStatus(), NKikimrScheme::StatusAccepted);
        // Complete the actual schema operation without letting the import persist WaitTxId.
        // Replaying an in-flight operation alone would not cover the already-existing node case.
        t.Env.TestWaitNotification(t.Runtime, createTxId);
        const ui64 originalTablet = t.TabletId();
        replies.Stop();
        t.RebootSchemeShard();
        t.Finish(id);
        UNIT_ASSERT_VALUES_EQUAL(t.TabletId(), originalTablet);
        t.AssertResources(MakeResources());
        replies.Unblock();
        TestGetImport(t.Runtime, id, "/MyRoot", Ydb::StatusIds::SUCCESS);
    }

    Y_UNIT_TEST(CancelWithPendingAddResponse) {
        TKesusImportEnv t;
        t.WriteBackup(ResourcesText(MakeResources()));
        TBlockEvents<TEvKesus::TEvAddQuoterResourceResult> replies(t.Runtime);
        const ui64 id = t.StartImport(t.MakeImport());
        t.Runtime.WaitFor("first Add committed", [&] { return !replies.empty(); });
        TestCancelImport(t.Runtime, ++t.TxId, "/MyRoot", id);
        t.Finish(id, Ydb::StatusIds::CANCELLED);
        replies.Stop().Unblock();
        UNIT_ASSERT_VALUES_EQUAL(t.DescribeResources().resources_size(), 1);
        TestGetImport(t.Runtime, id, "/MyRoot", Ydb::StatusIds::CANCELLED);
    }

    Y_UNIT_TEST(CancelWithPendingCreateProposal) {
        TKesusImportEnv t;
        t.WriteBackup(ResourcesText(MakeResources()));
        TBlockEvents<TEvSchemeShard::TEvModifySchemeTransaction> proposals(t.Runtime, IsKesusCreation);
        ui32 addRequests = 0;
        auto observer = t.Runtime.AddObserver<TEvKesus::TEvAddQuoterResource>([&](const auto&) {
            ++addRequests;
        });
        const ui64 id = t.StartImport(t.MakeImport());
        t.Runtime.WaitFor("create proposal pending", [&] { return !proposals.empty(); });
        const ui64 createTxId = proposals.front()->Get()->Record.GetTxId();
        TestCancelImport(t.Runtime, ++t.TxId, "/MyRoot", id);
        t.Finish(id, Ydb::StatusIds::CANCELLED);
        // Cancellation cannot recall a proposal already sent to SchemeShard.
        proposals.Stop().Unblock();
        t.Env.TestWaitNotification(t.Runtime, createTxId);
        UNIT_ASSERT_VALUES_EQUAL(t.DescribeResources().resources_size(), 0);
        UNIT_ASSERT_VALUES_EQUAL(addRequests, 0);
        TestGetImport(t.Runtime, id, "/MyRoot", Ydb::StatusIds::CANCELLED);
    }

    Y_UNIT_TEST(CancelWithPendingCompletion) {
        TKesusImportEnv t;
        t.WriteBackup(ResourcesText(MakeResources()));
        TBlockEvents<TEvPrivate::TEvImportKesusResourcesResult> replies(t.Runtime);
        const ui64 id = t.StartImport(t.MakeImport());
        t.Runtime.WaitFor("restorer completion blocked", [&] { return !replies.empty(); });
        TestCancelImport(t.Runtime, ++t.TxId, "/MyRoot", id);
        t.Finish(id, Ydb::StatusIds::CANCELLED);
        replies.Stop().Unblock();
        t.AssertResources(MakeResources());
        TestGetImport(t.Runtime, id, "/MyRoot", Ydb::StatusIds::CANCELLED);
    }

    Y_UNIT_TEST(CancelWithPendingScheme) {
        TKesusImportEnv t;
        t.WriteBackup(ResourcesText(MakeResources()));
        TBlockEvents<TEvPrivate::TEvImportSchemeReady> replies(t.Runtime);
        const ui64 id = t.StartImport(t.MakeImport());
        t.Runtime.WaitFor("scheme download completed", [&] { return !replies.empty(); });
        TestCancelImport(t.Runtime, ++t.TxId, "/MyRoot", id);
        t.Finish(id, Ydb::StatusIds::CANCELLED);
        replies.Stop().Unblock();
        t.AssertNotCreated();
        TestGetImport(t.Runtime, id, "/MyRoot", Ydb::StatusIds::CANCELLED);
    }

    Y_UNIT_TEST(OtherItemFailureStopsResourcesRestorer) {
        TKesusImportEnv t;
        t.WriteBackup(ResourcesText(MakeResources()));
        t.WriteFile("/backup/broken/metadata.json", t.S3.GetData().at("/backup/coordination/metadata.json"));
        t.WriteFile("/backup/broken/create_coordination_node.pb", t.S3.GetData().at(NodeKey));
        t.WriteFile("/backup/broken/rate_limiter_resources.pb", "resources {");
        auto request = t.MakeImport();
        auto* item = request.MutableImportFromS3Settings()->add_items();
        item->set_source_prefix("backup/broken");
        item->set_destination_path("/MyRoot/Broken");
        TBlockEvents<TEvPrivate::TEvImportSchemeReady> schemes(t.Runtime, [](const auto& ev) {
            return ev->Get()->ItemIdx == 1;
        });
        TBlockEvents<TEvKesus::TEvAddQuoterResourceResult> resources(t.Runtime);
        const ui64 id = t.StartImport(request);
        t.Runtime.WaitFor("one item restoring resources and another failing", [&] {
            return !schemes.empty() && !resources.empty();
        });
        UNIT_ASSERT(!schemes.front()->Get()->Success);
        schemes.Stop().Unblock();
        t.Finish(id, Ydb::StatusIds::CANCELLED);
        resources.Stop().Unblock();
        UNIT_ASSERT_VALUES_EQUAL(t.DescribeResources().resources_size(), 1);
        TestDescribeResult(DescribePath(t.Runtime, "/MyRoot/Broken"), {NLs::PathNotExist});
        TestGetImport(t.Runtime, id, "/MyRoot", Ydb::StatusIds::CANCELLED);
    }

    Y_UNIT_TEST(DescribeResourcesFailureIsNotSuccess) {
        TKesusImportEnv t;
        t.WriteBackup("resources: []\n");
        auto observer = t.Runtime.AddObserver<TEvKesus::TEvDescribeQuoterResourcesResult>([](auto& ev) {
            ev->Get()->Record.MutableError()->SetStatus(Ydb::StatusIds::INTERNAL_ERROR);
            ev->Get()->Record.MutableError()->AddIssues()->set_message("Injected describe failure");
        });
        t.Finish(t.StartImport(t.MakeImport()), Ydb::StatusIds::CANCELLED, "Injected describe failure");
    }

    Y_UNIT_TEST_FLAG(AlreadyExistsChecksMetering, Conflicting) {
        TKesusImportEnv t;
        const auto resources = MakeResources(true);
        t.WriteBackup(ResourcesText(resources));
        TBlockEvents<TEvKesus::TEvAddQuoterResource> requests(t.Runtime);
        const ui64 id = t.StartImport(t.MakeImport(), true);
        t.Runtime.WaitFor("first Add blocked", [&] { return !requests.empty(); });
        auto resource = requests.front()->Get()->Record.GetResource();
        if (Conflicting) {
            resource.MutableAccountingConfig()->SetProvisionedUnitsPerSecond(26);
        }
        requests.Stop();
        t.AddResource(t.TabletId(), resource);
        bool alreadyExists = false;
        auto observer = t.Runtime.AddObserver<TEvKesus::TEvAddQuoterResourceResult>([&](auto& ev) {
            alreadyExists |= ev->Get()->Record.GetError().GetStatus() == Ydb::StatusIds::ALREADY_EXISTS;
        });
        requests.Unblock();
        t.Finish(id, Conflicting ? Ydb::StatusIds::CANCELLED : Ydb::StatusIds::SUCCESS);
        UNIT_ASSERT(alreadyExists);
        if (Conflicting) {
            const auto actual = t.DescribeResources();
            UNIT_ASSERT_VALUES_EQUAL(actual.resources_size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(actual.resources(0).metering_config().provisioned_units_per_second(), 26);
        } else {
            t.AssertResources(resources);
        }
    }

    Y_UNIT_TEST(ExistingNodeIsRejected) {
        TKesusImportEnv t;
        t.WriteBackup(ResourcesText(MakeResources()));
        const ui64 tabletId = t.CreateNode("Restored");
        t.StartImport(t.MakeImport(), false, Ydb::StatusIds::BAD_REQUEST);
        UNIT_ASSERT_VALUES_EQUAL(t.TabletId(), tabletId);
        UNIT_ASSERT_VALUES_EQUAL(t.DescribeResources().resources_size(), 0);
    }

    Y_UNIT_TEST(ExistingParentDirectory) {
        TKesusImportEnv t;
        t.WriteBackup(ResourcesText(MakeResources()));
        TestMkDir(t.Runtime, ++t.TxId, "/MyRoot", "Nested");
        t.Env.TestWaitNotification(t.Runtime, t.TxId);
        auto request = t.MakeImport();
        request.MutableImportFromS3Settings()->mutable_items(0)->set_destination_path("/MyRoot/Nested/Restored");
        t.Finish(t.StartImport(request));
        const auto resources = t.DescribeResources("/MyRoot/Nested/Restored");
        UNIT_ASSERT(google::protobuf::util::MessageDifferencer::Equals(resources, NormalizeResources(MakeResources())));
    }

    Y_UNIT_TEST(ParentMustBeADirectory) {
        TKesusImportEnv t;
        t.WriteBackup(ResourcesText(MakeResources()));
        const ui64 originalTablet = t.CreateNode("Blocker");
        auto request = t.MakeImport();
        request.MutableImportFromS3Settings()->mutable_items(0)->set_destination_path("/MyRoot/Blocker/Restored");
        t.Finish(t.StartImport(request), Ydb::StatusIds::CANCELLED, "Cannot create coordination node parent");
        TestDescribeResult(DescribePath(t.Runtime, "/MyRoot/Blocker/Restored"), {NLs::PathNotExist});
        UNIT_ASSERT_VALUES_EQUAL(t.TabletId("/MyRoot/Blocker"), originalTablet);
        UNIT_ASSERT_VALUES_EQUAL(t.DescribeResources("/MyRoot/Blocker").resources_size(), 0);
    }

    Y_UNIT_TEST(MissingParentDirectoriesAreCreated) {
        TKesusImportEnv t;
        t.WriteBackup(ResourcesText(MakeResources()));
        auto request = t.MakeImport();
        request.MutableImportFromS3Settings()->mutable_items(0)->set_destination_path("/MyRoot/Nested/Deeper/Restored");
        t.Finish(t.StartImport(request));
        const auto resources = t.DescribeResources("/MyRoot/Nested/Deeper/Restored");
        UNIT_ASSERT(google::protobuf::util::MessageDifferencer::Equals(resources, NormalizeResources(MakeResources())));
        TestDescribeResult(DescribePath(t.Runtime, "/MyRoot/Nested"), {NLs::PathExist});
        TestDescribeResult(DescribePath(t.Runtime, "/MyRoot/Nested/Deeper"), {NLs::PathExist});
    }

    Y_UNIT_TEST(RebootBeforeCreatingParentDirectories) {
        TKesusImportEnv t;
        t.WriteBackup(ResourcesText(MakeResources()));
        auto request = t.MakeImport();
        request.MutableImportFromS3Settings()->mutable_items(0)->set_destination_path("/MyRoot/Nested/Deeper/Restored");
        TBlockEvents<TEvSchemeShard::TEvModifySchemeTransaction> proposals(t.Runtime, [](const auto& ev) {
            const auto& record = ev->Get()->Record;
            return record.TransactionSize() && record.GetTransaction(0).GetOperationType() == NKikimrSchemeOp::ESchemeOpMkDir;
        });
        const ui64 id = t.StartImport(request);
        t.Runtime.WaitFor("parent creation proposal blocked", [&] { return !proposals.empty(); });
        proposals.Stop();
        t.RebootSchemeShard();
        t.Finish(id);
        const auto resources = t.DescribeResources("/MyRoot/Nested/Deeper/Restored");
        UNIT_ASSERT(google::protobuf::util::MessageDifferencer::Equals(resources, NormalizeResources(MakeResources())));
        proposals.Unblock();
        TestGetImport(t.Runtime, id, "/MyRoot", Ydb::StatusIds::SUCCESS);
    }

    Y_UNIT_TEST_FLAG(TwoItemsCreatingTheSameParent, InFlight) {
        TKesusImportEnv t;
        t.WriteBackup(ResourcesText(MakeResources()));
        auto request = t.MakeImport();
        auto* settings = request.MutableImportFromS3Settings();
        settings->mutable_items(0)->set_destination_path("/MyRoot/New/N1");
        auto* second = settings->add_items();
        second->set_source_prefix("backup/coordination");
        second->set_destination_path("/MyRoot/New/N2");
        TBlockEvents<TEvSchemeShard::TEvModifySchemeTransaction> proposals(t.Runtime, [](const auto& ev) {
            const auto& record = ev->Get()->Record;
            return record.TransactionSize() && record.GetTransaction(0).GetOperationType() == NKikimrSchemeOp::ESchemeOpMkDir;
        });
        const ui64 id = t.StartImport(request);
        t.Runtime.WaitFor("both items decided to create the same parent", [&] { return proposals.size() >= 2; });
        const ui64 firstParentTxId = proposals[0]->Get()->Record.GetTxId();
        const ui64 secondParentTxId = proposals[1]->Get()->Record.GetTxId();
        const TActorId schemeShard = proposals[0]->Sender;
        t.Runtime.EnableScheduleForActor(schemeShard);
        TBlockEvents<TEvPrivate::TEvOperationPlan> plans(t.Runtime, [&](const auto& ev) {
            return InFlight && ev->Get()->TxId == firstParentTxId;
        });
        // In the completed case keep the first item's subscription alive while the second
        // sees an existing parent. In the in-flight case hold the actual parent creation.
        TBlockEvents<TEvSchemeShard::TEvNotifyTxCompletionResult> notifications(t.Runtime, [&](const auto& ev) {
            return !InFlight && ev->Get()->Record.GetTxId() == firstParentTxId && ev->Recipient == schemeShard;
        });
        proposals.Stop().Unblock(1);
        if (InFlight) {
            t.Runtime.WaitFor("first parent creation still in flight", [&] { return !plans.empty(); });
        } else {
            t.Env.TestWaitNotification(t.Runtime, firstParentTxId);
        }
        bool secondProposalAnswered = false;
        auto observer = t.Runtime.AddObserver<TEvSchemeShard::TEvModifySchemeTransactionResult>([&](auto& ev) {
            if (ev->Get()->Record.GetTxId() == secondParentTxId) {
                UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Record.GetStatus(), InFlight
                    ? NKikimrScheme::StatusMultipleModifications : NKikimrScheme::StatusAlreadyExists);
                secondProposalAnswered = true;
            }
        });
        proposals.Unblock();
        t.Runtime.WaitFor("second parent proposal answered", [&] { return secondProposalAnswered; });
        plans.Stop().Unblock();
        notifications.Stop().Unblock();
        t.Finish(id);
        const auto expected = NormalizeResources(MakeResources());
        UNIT_ASSERT(google::protobuf::util::MessageDifferencer::Equals(expected, t.DescribeResources("/MyRoot/New/N1")));
        UNIT_ASSERT(google::protobuf::util::MessageDifferencer::Equals(expected, t.DescribeResources("/MyRoot/New/N2")));
    }

    Y_UNIT_TEST(ConcurrentNodeCreationDoesNotBecomeImportDestination) {
        TKesusImportEnv t;
        t.WriteBackup(ResourcesText(MakeResources()));
        bool blockFirst = true;
        TBlockEvents<TEvSchemeShard::TEvModifySchemeTransaction> proposals(t.Runtime, [&](const auto& ev) {
            if (blockFirst && IsKesusCreation(ev)) {
                blockFirst = false;
                return true;
            }
            return false;
        });
        const ui64 id = t.StartImport(t.MakeImport());
        t.Runtime.WaitFor("import checked destination before concurrent create", [&] { return !proposals.empty(); });
        const ui64 foreignTablet = t.CreateNode("Restored");
        proposals.Stop().Unblock();
        t.Finish(id, Ydb::StatusIds::CANCELLED);
        UNIT_ASSERT_VALUES_EQUAL(t.TabletId(), foreignTablet);
        UNIT_ASSERT_VALUES_EQUAL(t.DescribeResources().resources_size(), 0);
    }

    Y_UNIT_TEST(RebootDoesNotRestoreIntoReplacementNode) {
        TKesusImportEnv t;
        t.WriteBackup(ResourcesText(MakeResources()));
        TBlockEvents<TEvPrivate::TEvImportKesusResourcesResult> replies(t.Runtime);
        const ui64 id = t.StartImport(t.MakeImport());
        t.Runtime.WaitFor("restore completion blocked", [&] { return !replies.empty(); });
        const ui64 oldTablet = t.TabletId();
        TestDropKesus(t.Runtime, ++t.TxId, "/MyRoot", "Restored");
        t.Env.TestWaitNotification(t.Runtime, t.TxId);
        const ui64 replacementTablet = t.CreateNode("Restored");
        UNIT_ASSERT(replacementTablet != oldTablet);
        replies.Stop();
        t.RebootSchemeShard();
        t.Finish(id, Ydb::StatusIds::CANCELLED);
        UNIT_ASSERT_VALUES_EQUAL(t.TabletId(), replacementTablet);
        UNIT_ASSERT_VALUES_EQUAL(t.DescribeResources().resources_size(), 0);
        replies.Unblock();
        TestGetImport(t.Runtime, id, "/MyRoot", Ydb::StatusIds::CANCELLED);
    }

    Y_UNIT_TEST(MixedTableAndKesus) {
        TKesusImportEnv t;
        t.Populate(t.CreateNode(), MakeResources());
        TestCreateTable(t.Runtime, ++t.TxId, "/MyRoot", R"(
            Name: "Table"
            Columns { Name: "key" Type: "Uint32" }
            KeyColumnNames: ["key"]
        )");
        t.Env.TestWaitNotification(t.Runtime, t.TxId);
        t.Export(false, false, true);
        auto request = t.MakeImport();
        auto* item = request.MutableImportFromS3Settings()->add_items();
        item->set_source_prefix("backup/table");
        item->set_destination_path("/MyRoot/RestoredTable");
        t.Finish(t.StartImport(request));
        t.AssertResources(MakeResources());
        TestDescribeResult(DescribePath(t.Runtime, "/MyRoot/RestoredTable"), {NLs::PathExist, NLs::IsTable});
    }
}
