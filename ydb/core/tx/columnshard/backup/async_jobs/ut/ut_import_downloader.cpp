#include <library/cpp/testing/hook/hook.h>
#include <library/cpp/testing/unittest/registar.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/array/builder_binary.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/result.h>

#include <ydb/library/testlib/s3_recipe_helper/s3_recipe_helper.h>

#include <ydb/apps/ydbd/export/export.h>
#include <ydb/core/protos/flat_scheme_op.pb.h>
#include <ydb/core/protos/s3_settings.pb.h>
#include <ydb/core/testlib/basics/runtime.h>
#include <ydb/core/testlib/tablet_helpers.h>
#include <ydb/core/tx/columnshard/backup/async_jobs/import_downloader.h>
#include <ydb/core/tx/columnshard/backup/iscan/iscan.h>
#include <ydb/core/tx/columnshard/columnshard_private_events.h>
#include <ydb/core/scheme_types/scheme_type_info.h>
#include <ydb/core/tx/datashard/import_common.h>


namespace NKikimr {
    
namespace {
    
using TRuntimePtr = std::shared_ptr<TTestActorRuntime>;
    
std::shared_ptr<arrow::RecordBatch> TestRecordBatch() {
    std::vector<std::string> keys = {"foo", "bar", "baz"};
    std::vector<std::string> values = {"one", "two", "three"};

    arrow::StringBuilder key_builder;
    for (const auto& k : keys) {
        Y_UNUSED(key_builder.Append(k));
    }
    std::shared_ptr<arrow::Array> key_array;
    Y_UNUSED(key_builder.Finish(&key_array));

    arrow::StringBuilder value_builder;
    for (const auto& v : values) {
        Y_UNUSED(value_builder.Append(v));
    }
    std::shared_ptr<arrow::Array> value_array;
    Y_UNUSED(value_builder.Finish(&value_array));

    auto schema = arrow::schema({
        arrow::field("key", arrow::binary()),
        arrow::field("value", arrow::binary())
    });

    return arrow::RecordBatch::Make(schema, keys.size(), {key_array, value_array});
}

NDataShard::IExport::TTableColumns MakeYdbColumns() {
    NDataShard::IExport::TTableColumns columns;
    columns[0] = NDataShard::TUserTable::TUserColumn(NScheme::TTypeInfo(NScheme::NTypeIds::Utf8), "", "key", true);
    columns[1] = NDataShard::TUserTable::TUserColumn(NScheme::TTypeInfo(NScheme::NTypeIds::Utf8), "", "value", false);
    return columns;
}

TVector<std::pair<TString, NScheme::TTypeInfo>> MakeYdbSchema() {
    return {{"key", NScheme::TTypeInfo(NScheme::NTypeIds::Utf8)}, {"value", NScheme::TTypeInfo(NScheme::NTypeIds::Utf8)}};
}

NKikimrSchemeOp::TBackupTask MakeBackupTask(const TString& bucketName) {
    NKikimrSchemeOp::TBackupTask backupTask;
    backupTask.SetEnablePermissions(true);
    auto& s3Settings = *backupTask.MutableS3Settings();
    s3Settings.SetBucket(bucketName);
    s3Settings.SetEndpoint(GetEnv("S3_ENDPOINT"));
    auto& table = *backupTask.MutableTable();
    auto& tableDescription = *table.MutableColumnTableDescription();
    tableDescription.SetColumnShardCount(4);
    auto& col1 = *tableDescription.MutableSchema()->MutableColumns()->Add();
    col1.SetName("key");
    col1.SetType("Utf8");

    auto& col2 = *tableDescription.MutableSchema()->MutableColumns()->Add();
    col2.SetName("value");
    col2.SetType("Utf8");
    table.MutableSelf();
    return backupTask;
}

NKikimrSchemeOp::TRestoreTask MakeRestoreTask(const TString& bucketName) {
    NKikimrSchemeOp::TRestoreTask restoreTask;
    auto& s3Settings = *restoreTask.MutableS3Settings();
    s3Settings.SetBucket(bucketName);
    s3Settings.SetEndpoint(GetEnv("S3_ENDPOINT"));
    auto& description = *restoreTask.MutableTableDescription();
    auto& col1 = *description.AddColumns();
    col1.SetName("key");
    col1.SetType("Utf8");
    col1.SetId(1);
    col1.SetTypeId(NScheme::NTypeIds::Utf8);
    auto& col2 = *description.AddColumns();
    col2.SetName("value");
    col2.SetType("Utf8");
    col2.SetId(2);
    col2.SetTypeId(NScheme::NTypeIds::Utf8);
    description.AddKeyColumnNames("key");
    description.AddKeyColumnIds(1);
    return restoreTask;
}

}

using namespace NColumnShard;

Y_UNIT_TEST_SUITE(AsyncJobs) {

    Y_UNIT_TEST(Import) {
        Aws::S3::S3Client s3Client = NTestUtils::MakeS3Client();
        NTestUtils::CreateBucket("test2", s3Client);

        TRuntimePtr runtime(new TTestBasicRuntime());
        runtime->SetLogPriority(NKikimrServices::DATASHARD_BACKUP, NActors::NLog::PRI_DEBUG);
        runtime->SetLogPriority(NKikimrServices::DATASHARD_RESTORE, NActors::NLog::PRI_DEBUG);
        SetupTabletServices(*runtime);
        
        const auto edge = runtime->AllocateEdgeActor(0);
        auto exportFactory = std::make_shared<TDataShardExportFactory>();
        auto actor = NKikimr::NColumnShard::NBackup::CreateExportUploaderActor(edge, MakeBackupTask("test2"), exportFactory.get(), MakeYdbColumns(), 0);
        auto exporter = runtime->Register(actor.release());

        TAutoPtr<IEventHandle> handle;
        runtime->DispatchEvents({}, TDuration::Seconds(1));
        runtime->Send(new IEventHandle(exporter, edge, new NColumnShard::TEvPrivate::TEvBackupExportRecordBatch(TestRecordBatch(), false)));
        runtime->Send(new IEventHandle(exporter, edge, new NColumnShard::TEvPrivate::TEvBackupExportRecordBatch(TestRecordBatch(), true)));
        auto event1 = runtime->GrabEdgeEvent<NColumnShard::TEvPrivate::TEvBackupExportRecordBatchResult>(handle);
        UNIT_ASSERT(!event1->IsFinish);
        auto event2 = runtime->GrabEdgeEvent<NColumnShard::TEvPrivate::TEvBackupExportRecordBatchResult>(handle);
        UNIT_ASSERT(event2->IsFinish);
        
        runtime->DispatchEvents({}, TDuration::Seconds(5));
        std::vector<TString> result = NTestUtils::GetObjectKeys("test2", s3Client);
        UNIT_ASSERT_VALUES_EQUAL(NTestUtils::GetUncommittedUploadsCount("test2", s3Client), 0);
        UNIT_ASSERT_VALUES_EQUAL(JoinSeq(",", result), "data_00.csv,metadata.json,permissions.pb,scheme.pb");
        auto scheme = NTestUtils::GetObject("test2", "scheme.pb", s3Client);
        UNIT_ASSERT_VALUES_EQUAL(scheme, "columns {\n  name: \"key\"\n  type {\n    optional_type {\n      item {\n        type_id: UTF8\n      }\n    }\n  }\n}\ncolumns {\n  name: \"value\"\n  type {\n    optional_type {\n      item {\n        type_id: UTF8\n      }\n    }\n  }\n}\npartitioning_settings {\n  min_partitions_count: 4\n}\nstore_type: STORE_TYPE_COLUMN\n");
        auto metadata = NTestUtils::GetObject("test2", "metadata.json", s3Client);
        UNIT_ASSERT_VALUES_EQUAL(metadata, "{\"version\":0,\"full_backups\":[{\"snapshot_vts\":[0,0]}],\"permissions\":1,\"changefeeds\":[]}");
        auto data = NTestUtils::GetObject("test2", "data_00.csv", s3Client);
        UNIT_ASSERT_VALUES_EQUAL(data, "\"foo\",\"one\"\n\"bar\",\"two\"\n\"baz\",\"three\"\n\"foo\",\"one\"\n\"bar\",\"two\"\n\"baz\",\"three\"\n");
        

        auto restoreTask = MakeRestoreTask("test2");
        auto userTable = MakeIntrusiveConst<NDataShard::TUserTable>(ui32(0), restoreTask.GetTableDescription(), ui32(0));

        auto importActor = NKikimr::NColumnShard::NBackup::CreateImportDownloader(edge, 0, restoreTask, NKikimr::NDataShard::TTableInfo{0, userTable}, MakeYdbSchema());
        auto importActorId = runtime->Register(importActor.release());
        runtime->DispatchEvents({}, TDuration::Seconds(1));
        
        auto event3 = runtime->GrabEdgeEvent<TEvPrivate::TEvBackupImportRecordBatch>(handle);
        UNIT_ASSERT(!event3->IsLast);
        UNIT_ASSERT_VALUES_EQUAL(event3->Data->ToString(), "key:   [\n    \"foo\",\n    \"bar\",\n    \"baz\",\n    \"foo\",\n    \"bar\",\n    \"baz\"\n  ]\nvalue:   [\n    \"one\",\n    \"two\",\n    \"three\",\n    \"one\",\n    \"two\",\n    \"three\"\n  ]\n");
        runtime->Send(new IEventHandle(importActorId, edge, new TEvPrivate::TEvBackupImportRecordBatchResult()));

        auto event4 = runtime->GrabEdgeEvent<TEvPrivate::TEvBackupImportRecordBatch>(handle);
        UNIT_ASSERT(event4->IsLast);
        UNIT_ASSERT(!event4->Data);
    }
}

} // namespace NKikimr
