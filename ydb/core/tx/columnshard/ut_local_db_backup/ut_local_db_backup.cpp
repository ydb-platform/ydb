#include <ydb/core/tablet_flat/flat_executor_backup_common.h>
#include <ydb/core/testlib/tablet_helpers.h>
#include <ydb/core/tx/columnshard/columnshard.h>
#include <ydb/core/tx/columnshard/test_helper/columnshard_ut_common.h>
#include <ydb/core/tx/columnshard/test_helper/controllers.h>
#include <ydb/core/wrappers/ut_helpers/s3_mock.h>

#include <ydb/library/aclib/aclib.h>
#include <ydb/library/actors/core/mon.h>
#include <ydb/library/actors/protos/actors.pb.h>
#include <ydb/library/aws_init/aws.h>

#include <library/cpp/json/json_reader.h>
#include <library/cpp/testing/hook/hook.h>
#include <library/cpp/testing/unittest/registar.h>
#include <util/string/split.h>

Y_TEST_HOOK_BEFORE_RUN(InitAwsAPI) {
    NKikimr::InitAwsAPI();
}

namespace NKikimr::NColumnShard {

using namespace NTxUT;
using NWrappers::NTestHelpers::TS3Mock;

namespace {

constexpr ui64 TabletId = TTestTxConfig::TxTablet0;
constexpr ui64 TableId = 1;
const TString Bucket = "test-bucket";
const TString Prefix = "backups";

class TFixture {
public:
    NYDBTest::TControllers::TGuard<NOlap::TWaitCompactionController> Controller =
        NYDBTest::TControllers::RegisterCSControllerGuard<NOlap::TWaitCompactionController>();
    TTestBasicRuntime Runtime;
    TActorId Sender;
    TPortManager PortManager;
    ui16 S3Port = 0;
    THolder<TS3Mock> S3Mock;
    TestTableDescription Table;

    TFixture() {
        TTester::Setup(Runtime);
        Runtime.SetLogPriority(NKikimrServices::S3_WRAPPER, NActors::NLog::PRI_DEBUG);
        Controller->DisableBackground(NYDBTest::ICSController::EBackground::TTL);
        Controller->DisableBackground(NYDBTest::ICSController::EBackground::Compaction);

        S3Port = PortManager.GetPort();
        S3Mock.Reset(new TS3Mock(TS3Mock::TSettings(S3Port)));
        UNIT_ASSERT(S3Mock->Start());

        Sender = Runtime.AllocateEdgeActor();
        CreateTestBootstrapper(Runtime, CreateTestTabletInfo(TabletId, TTabletTypes::ColumnShard), &CreateColumnShard);
        TDispatchOptions options;
        options.FinalEvents.push_back(TDispatchOptions::TFinalEventCondition(TEvTablet::EvBoot));
        Runtime.DispatchEvents(options);
    }

    void PrepareData() {
        auto planStep = SetupSchema(Runtime, Sender, TableId, Table);
        Y_UNUSED(planStep);

        ui64 writeId = 1;
        ui64 txId = 100;
        std::vector<ui64> writeIds;
        const TString data = MakeTestBlob({ 0, 100 }, Table.Schema);
        UNIT_ASSERT(WriteData(Runtime, Sender, writeId, TableId, data, Table.Schema, true, &writeIds));
        const auto commitStep = ProposeCommit(Runtime, Sender, txId, writeIds);
        PlanCommit(Runtime, Sender, commitStep, txId);
    }

    TString Get(const TString& query) {
        Runtime.SendToPipe(TabletId, Sender, new NMon::TEvRemoteHttpInfo(query), 0, GetPipeConfigWithRetries());
        const auto page = Runtime.GrabEdgeEvent<NMon::TEvRemoteHttpInfoRes>(Sender);
        UNIT_ASSERT(page);
        return page->Get()->Html;
    }

    NJson::TJsonValue GetJson() {
        Runtime.SendToPipe(TabletId, Sender, new NMon::TEvRemoteHttpInfo("/app?page=localdb_backup&format=json&TabletID=" + ToString(TabletId)),
            0, GetPipeConfigWithRetries());
        const auto reply = Runtime.GrabEdgeEvent<NMon::TEvRemoteJsonInfoRes>(Sender);
        UNIT_ASSERT(reply);
        NJson::TJsonValue json;
        UNIT_ASSERT_C(NJson::ReadJsonTree(reply->Get()->Json, &json), reply->Get()->Json);
        return json;
    }

    TString Post(const THashMap<TString, TString>& params) {
        NActorsProto::TRemoteHttpInfo info;
        info.SetMethod(HTTP_METHOD_POST);
        info.SetPath("/app");
        {
            auto* param = info.AddQueryParams();
            param->SetKey("page");
            param->SetValue("localdb_backup");
            param = info.AddQueryParams();
            param->SetKey("TabletID");
            param->SetValue(ToString(TabletId));
        }
        for (const auto& [key, value] : params) {
            auto* param = info.AddPostParams();
            param->SetKey(key);
            param->SetValue(value);
        }
        Runtime.SendToPipe(TabletId, Sender, new NMon::TEvRemoteHttpInfo(info), 0, GetPipeConfigWithRetries());
        const auto page = Runtime.GrabEdgeEvent<NMon::TEvRemoteHttpInfoRes>(Sender);
        UNIT_ASSERT(page);
        return page->Get()->Html;
    }

    THashMap<TString, TString> S3Form() const {
        return {
            { "endpoint", TStringBuilder() << "localhost:" << S3Port },
            { "scheme", "HTTP" },
            { "bucket", Bucket },
            { "prefix", Prefix },
            { "access_key", "access" },
            { "secret_key", "secret" },
        };
    }

    TString BackupsPrefix() const {
        return TStringBuilder() << "/" << Bucket << "/" << Prefix << "/column_shard/" << TabletId << "/backup_";
    }

    // Backup directory keys ("/bucket/.../backup_<...>") that have a manifest checksum, i.e. are complete.
    TSet<TString> CompleteBackups() const {
        TSet<TString> result;
        const TString prefix = BackupsPrefix();
        const TString marker = "/snapshot/manifest.json.sha256";
        for (const auto& [key, _] : S3Mock->GetData()) {
            if (key.StartsWith(prefix) && key.EndsWith(marker)) {
                result.insert(key.substr(0, key.size() - marker.size()));
            }
        }
        return result;
    }

    void WaitBackups(size_t count) {
        Runtime.WaitFor(TStringBuilder() << count << " complete backups", [&] {
            return CompleteBackups().size() >= count;
        }, TDuration::Minutes(5));
        // let the writer report the result to the tablet
        Runtime.SimulateSleep(TDuration::Seconds(2));
    }

    void VerifyBackup(const TString& backupDir) {
        const auto& data = S3Mock->GetData();
        const TString snapshot = backupDir + "/snapshot/";

        auto manifestIt = data.find(snapshot + "manifest.json");
        UNIT_ASSERT_C(manifestIt != data.end(), "manifest.json is missing in " << backupDir);
        const TString& manifestStr = manifestIt->second;

        auto checksumIt = data.find(snapshot + "manifest.json.sha256");
        UNIT_ASSERT(checksumIt != data.end());
        UNIT_ASSERT_VALUES_EQUAL(checksumIt->second, NTabletFlatExecutor::NBackup::TSha256Hasher::Hash(manifestStr));

        NJson::TJsonValue manifest;
        UNIT_ASSERT(NJson::ReadJsonTree(manifestStr, &manifest));
        UNIT_ASSERT_VALUES_EQUAL(manifest["tablet_type"].GetString(), "column_shard");
        UNIT_ASSERT_VALUES_EQUAL(manifest["tablet_id"].GetUInteger(), TabletId);
        UNIT_ASSERT(manifest["generation"].GetUInteger() > 0);

        const auto& files = manifest["files"].GetArray();
        UNIT_ASSERT(files.size() > 1);
        bool hasSchema = false;
        bool hasValueTable = false;
        for (const auto& file : files) {
            const TString name = file["name"].GetString();
            auto fileIt = data.find(snapshot + name);
            UNIT_ASSERT_C(fileIt != data.end(), "file " << name << " is missing in " << backupDir);
            UNIT_ASSERT_VALUES_EQUAL_C(file["sha256"].GetString(), NTabletFlatExecutor::NBackup::TSha256Hasher::Hash(fileIt->second), name);

            if (name == "schema.json") {
                hasSchema = true;
                NJson::TJsonValue schema;
                UNIT_ASSERT(NJson::ReadJsonTree(fileIt->second, &schema));
                UNIT_ASSERT(schema.Has("delta"));
                continue;
            }

            size_t rows = 0;
            for (const auto& line : StringSplitter(fileIt->second).Split('\n').SkipEmpty()) {
                NJson::TJsonValue row;
                UNIT_ASSERT_C(NJson::ReadJsonTree(line.Token(), &row), "bad JSON line in " << name << ": " << line.Token());
                UNIT_ASSERT(row.IsMap());
                ++rows;
            }
            if (name == "Value.json") {
                hasValueTable = true;
                UNIT_ASSERT_C(rows > 0, "Value table snapshot is empty");
            }
        }
        UNIT_ASSERT(hasSchema);
        UNIT_ASSERT(hasValueTable);

        // every object under snapshot/ is listed in the manifest
        for (const auto& [key, _] : data) {
            if (!key.StartsWith(snapshot)) {
                continue;
            }
            const TString name = key.substr(snapshot.size());
            if (name == "manifest.json" || name == "manifest.json.sha256") {
                continue;
            }
            bool listed = false;
            for (const auto& file : files) {
                listed |= file["name"].GetString() == name;
            }
            UNIT_ASSERT_C(listed, "object " << name << " is not listed in manifest");
        }
    }
};

}   // namespace

Y_UNIT_TEST_SUITE(ColumnShardLocalDbBackup) {
    Y_UNIT_TEST(PageIsAvailable) {
        TFixture f;
        const TString main = f.Get("/app?TabletID=" + ToString(TabletId));
        UNIT_ASSERT_C(main.Contains("page=localdb_backup"), main);

        const TString page = f.Get("/app?page=localdb_backup&TabletID=" + ToString(TabletId));
        UNIT_ASSERT_C(page.Contains("Local DB backup to S3"), page);
        UNIT_ASSERT_C(page.Contains("ldb-badge-idle\">Idle"), page);
        UNIT_ASSERT_C(page.Contains(">Start backup</button>"), page);

        const auto json = f.GetJson();
        UNIT_ASSERT_VALUES_EQUAL(json["active"].GetBoolean(), false);
        UNIT_ASSERT(!json.Has("last"));
    }

    Y_UNIT_TEST(RejectsInvalidForm) {
        TFixture f;
        auto form = f.S3Form();
        form["endpoint"] = "";
        const TString page = f.Post(form);
        UNIT_ASSERT_C(page.Contains("ldb-alert-error\">S3 endpoint is required"), page);
        UNIT_ASSERT_C(page.Contains("ldb-badge-idle\">Idle"), page);
        // the form is rendered back with the submitted values, except for the secret
        UNIT_ASSERT_C(page.Contains("name=\"bucket\" value=\"" + Bucket + "\""), page);
        UNIT_ASSERT_C(!page.Contains("secret\""), page);
    }

    Y_UNIT_TEST(BackupToS3) {
        TFixture f;
        f.PrepareData();

        const TString page = f.Post(f.S3Form());
        UNIT_ASSERT_C(page.Contains("Backup started"), page);

        f.WaitBackups(1);
        const auto backups = f.CompleteBackups();
        UNIT_ASSERT_VALUES_EQUAL(backups.size(), 1);
        f.VerifyBackup(*backups.begin());

        const TString status = f.Get("/app?page=localdb_backup&TabletID=" + ToString(TabletId));
        UNIT_ASSERT_C(status.Contains("ldb-badge-success\">Completed"), status);
        UNIT_ASSERT_C(status.Contains("ldb-progress-success\" style=\"width:100%\""), status);
        UNIT_ASSERT_C(status.Contains(">Start backup</button>"), status);
        UNIT_ASSERT_C(!status.Contains("<button class=\"ldb-btn\" type=\"submit\" disabled"), status);
        // secrets are never rendered back
        UNIT_ASSERT_C(!status.Contains("secret\""), status);

        const auto json = f.GetJson();
        UNIT_ASSERT_VALUES_EQUAL(json["active"].GetBoolean(), false);
        UNIT_ASSERT_VALUES_EQUAL(json["stage"].GetString(), "Completed");
        UNIT_ASSERT_VALUES_EQUAL(json["tables_total"].GetUInteger(), json["tables_done"].GetUInteger());
        UNIT_ASSERT(json["tables_done"].GetUInteger() > 0);
        UNIT_ASSERT(json["bytes"].GetUInteger() > 0);
        UNIT_ASSERT_VALUES_EQUAL(json["last"]["success"].GetBoolean(), true);
        UNIT_ASSERT_VALUES_EQUAL("/" + Bucket + "/" + json["last"]["path"].GetString(), *backups.begin());
        bool hasValueTable = false;
        for (const auto& table : json["tables"].GetArray()) {
            UNIT_ASSERT_VALUES_EQUAL_C(table["state"].GetString(), "done", table["name"].GetString());
            hasValueTable |= table["name"].GetString() == "Value";
        }
        UNIT_ASSERT(hasValueTable);
    }

    Y_UNIT_TEST(RequiresAdministrator) {
        TFixture f;
        auto& appData = f.Runtime.GetAppData(0);
        appData.FeatureFlags.SetEnableTabletDevUiSecurePath(true);
        // A non-empty list denies the anonymous token; empty would make everyone an administrator.
        appData.AdministrationAllowedSIDs.push_back("cluster-admin");

        auto request = [&](const TString& userToken) {
            NActorsProto::TRemoteHttpInfo info;
            info.SetMethod(HTTP_METHOD_GET);
            info.SetPath("/app");
            info.SetUserToken(userToken);
            auto* param = info.AddQueryParams();
            param->SetKey("page");
            param->SetValue("localdb_backup");
            param = info.AddQueryParams();
            param->SetKey("TabletID");
            param->SetValue(ToString(TabletId));
            f.Runtime.SendToPipe(TabletId, f.Sender, new NMon::TEvRemoteHttpInfo(std::move(info)), 0, GetPipeConfigWithRetries());
        };

        request({});
        const auto forbidden = f.Runtime.GrabEdgeEvent<NMon::TEvRemoteBinaryInfoRes>(f.Sender);
        UNIT_ASSERT(forbidden);
        UNIT_ASSERT_C(forbidden->Get()->Blob.Contains("403"), forbidden->Get()->Blob);

        request(NACLib::TUserToken("cluster-admin", {}).SerializeAsString());
        const auto page = f.Runtime.GrabEdgeEvent<NMon::TEvRemoteHttpInfoRes>(f.Sender);
        UNIT_ASSERT(page);
        UNIT_ASSERT_C(page->Get()->Html.Contains("Local DB backup to S3"), page->Get()->Html);
        UNIT_ASSERT_C(page->Get()->Html.Contains("action=\"app?page=localdb_backup"), page->Get()->Html);
    }

    Y_UNIT_TEST(TwoBackupsInARow) {
        TFixture f;
        f.PrepareData();

        UNIT_ASSERT_C(f.Post(f.S3Form()).Contains("Backup started"), "first run");
        f.WaitBackups(1);
        UNIT_ASSERT_C(f.Post(f.S3Form()).Contains("Backup started"), "second run");
        f.WaitBackups(2);

        const auto backups = f.CompleteBackups();
        UNIT_ASSERT_VALUES_EQUAL(backups.size(), 2);
        for (const auto& backup : backups) {
            f.VerifyBackup(backup);
        }
    }
}

}   // namespace NKikimr::NColumnShard
