#include <ydb/core/protos/flat_scheme_op.pb.h>
#include <ydb/core/protos/s3_settings.pb.h>
#include <ydb/core/scheme_types/scheme_type_info.h>
#include <ydb/core/testlib/basics/runtime.h>
#include <ydb/core/testlib/tablet_helpers.h>
#include <ydb/core/tx/columnshard/backup/async_jobs/import_downloader.h>
#include <ydb/core/tx/columnshard/backup/iscan/iscan.h>
#include <ydb/core/tx/columnshard/columnshard_private_events.h>
#include <ydb/core/tx/datashard/import_common.h>

#include <ydb/apps/ydbd/export/export.h>
#include <ydb/library/testlib/s3_recipe_helper/s3_recipe_helper.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/array/builder_binary.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/result.h>
#include <library/cpp/testing/hook/hook.h>
#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr {

namespace {

using TRuntimePtr = std::shared_ptr<TTestActorRuntime>;

std::shared_ptr<arrow::RecordBatch> TestRecordBatch() {
    std::vector<std::string> keys = { "foo", "bar", "baz" };
    std::vector<std::string> values = { "one", "two", "three" };

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

    auto schema = arrow::schema({ arrow::field("key", arrow::binary()), arrow::field("value", arrow::binary()) });

    return arrow::RecordBatch::Make(schema, keys.size(), { key_array, value_array });
}

// 3-column record batch: key, col_a, col_b
std::shared_ptr<arrow::RecordBatch> TestRecordBatch3Cols() {
    std::vector<std::string> keys = { "k1", "k2" };
    std::vector<std::string> colA = { "a1", "a2" };
    std::vector<std::string> colB = { "b1", "b2" };

    arrow::StringBuilder key_builder;
    for (const auto& k : keys) {
        Y_UNUSED(key_builder.Append(k));
    }
    std::shared_ptr<arrow::Array> key_array;
    Y_UNUSED(key_builder.Finish(&key_array));

    arrow::StringBuilder a_builder;
    for (const auto& v : colA) {
        Y_UNUSED(a_builder.Append(v));
    }
    std::shared_ptr<arrow::Array> a_array;
    Y_UNUSED(a_builder.Finish(&a_array));

    arrow::StringBuilder b_builder;
    for (const auto& v : colB) {
        Y_UNUSED(b_builder.Append(v));
    }
    std::shared_ptr<arrow::Array> b_array;
    Y_UNUSED(b_builder.Finish(&b_array));

    auto schema = arrow::schema({
        arrow::field("key", arrow::binary()), arrow::field("col_a", arrow::binary()), arrow::field("col_b", arrow::binary()),
    });

    return arrow::RecordBatch::Make(schema, keys.size(), { key_array, a_array, b_array });
}

NDataShard::IExport::TTableColumns MakeYdbColumns() {
    NDataShard::IExport::TTableColumns columns;
    columns[0] = NDataShard::TUserTable::TUserColumn(NScheme::TTypeInfo(NScheme::NTypeIds::Utf8), "", "key", true);
    columns[1] = NDataShard::TUserTable::TUserColumn(NScheme::TTypeInfo(NScheme::NTypeIds::Utf8), "", "value", false);
    return columns;
}

NDataShard::IExport::TTableColumns MakeYdbColumns3Cols() {
    NDataShard::IExport::TTableColumns columns;
    columns[0] = NDataShard::TUserTable::TUserColumn(NScheme::TTypeInfo(NScheme::NTypeIds::Utf8), "", "key", true);
    columns[1] = NDataShard::TUserTable::TUserColumn(NScheme::TTypeInfo(NScheme::NTypeIds::Utf8), "", "col_a", false);
    columns[2] = NDataShard::TUserTable::TUserColumn(NScheme::TTypeInfo(NScheme::NTypeIds::Utf8), "", "col_b", false);
    return columns;
}

THashMap<ui32, std::pair<TString, NScheme::TTypeInfo>> MakeYdbSchema() {
    return {
        { 1, { "key", NScheme::TTypeInfo(NScheme::NTypeIds::Utf8) } },
        { 2, { "value", NScheme::TTypeInfo(NScheme::NTypeIds::Utf8) } },
    };
}

// YdbSchema with deliberately reversed value column order: key, col_b, col_a.
// This simulates the scenario where YdbSchema order differs from RowScheme order.
// The fix ensures RowScheme order (key, col_a, col_b) is used, not YdbSchema order.
THashMap<ui32, std::pair<TString, NScheme::TTypeInfo>> MakeYdbSchema3ColsReversed() {
    return {
        { 1, { "key", NScheme::TTypeInfo(NScheme::NTypeIds::Utf8) } },
        { 3, { "col_b", NScheme::TTypeInfo(NScheme::NTypeIds::Utf8) } },
        { 2, { "col_a", NScheme::TTypeInfo(NScheme::NTypeIds::Utf8) } },
    };
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

NKikimrSchemeOp::TBackupTask MakeBackupTask3Cols(const TString& bucketName) {
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
    col2.SetName("col_a");
    col2.SetType("Utf8");

    auto& col3 = *tableDescription.MutableSchema()->MutableColumns()->Add();
    col3.SetName("col_b");
    col3.SetType("Utf8");
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

// 3-column restore task: key (id=1), col_a (id=2), col_b (id=3)
NKikimrSchemeOp::TRestoreTask MakeRestoreTask3Cols(const TString& bucketName) {
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
    col2.SetName("col_a");
    col2.SetType("Utf8");
    col2.SetId(2);
    col2.SetTypeId(NScheme::NTypeIds::Utf8);
    auto& col3 = *description.AddColumns();
    col3.SetName("col_b");
    col3.SetType("Utf8");
    col3.SetId(3);
    col3.SetTypeId(NScheme::NTypeIds::Utf8);
    description.AddKeyColumnNames("key");
    description.AddKeyColumnIds(1);
    return restoreTask;
}

}   // namespace

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
        auto actor =
            NKikimr::NColumnShard::NBackup::CreateExportUploaderActor(edge, MakeBackupTask("test2"), exportFactory.get(), MakeYdbColumns(), 0);
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
        UNIT_ASSERT_VALUES_EQUAL(scheme,
            "columns {\n  name: \"key\"\n  type {\n    optional_type {\n      item {\n        type_id: UTF8\n      }\n    }\n  }\n}\ncolumns "
            "{\n  name: \"value\"\n  type {\n    optional_type {\n      item {\n        type_id: UTF8\n      }\n    }\n  "
            "}\n}\npartitioning_settings {\n  min_partitions_count: 4\n}\nstore_type: STORE_TYPE_COLUMN\n");
        auto metadata = NTestUtils::GetObject("test2", "metadata.json", s3Client);
        UNIT_ASSERT_VALUES_EQUAL(
            metadata, "{\"version\":0,\"full_backups\":[{\"snapshot_vts\":[0,0]}],\"permissions\":1,\"changefeeds\":[],\"indexes\":[]}");
        auto data = NTestUtils::GetObject("test2", "data_00.csv", s3Client);
        UNIT_ASSERT_VALUES_EQUAL(
            data, "\"foo\",\"one\"\n\"bar\",\"two\"\n\"baz\",\"three\"\n\"foo\",\"one\"\n\"bar\",\"two\"\n\"baz\",\"three\"\n");

        auto restoreTask = MakeRestoreTask("test2");
        auto userTable = MakeIntrusiveConst<NDataShard::TUserTable>(ui32(0), restoreTask.GetTableDescription(), ui32(0));

        auto importActor = NKikimr::NColumnShard::NBackup::CreateImportDownloader(
            edge, 0, restoreTask, NKikimr::NDataShard::TTableInfo{ 0, userTable }, MakeYdbSchema());
        auto importActorId = runtime->Register(importActor.release());
        runtime->DispatchEvents({}, TDuration::Seconds(1));

        auto event3 = runtime->GrabEdgeEvent<TEvPrivate::TEvBackupImportRecordBatch>(handle);
        UNIT_ASSERT(!event3->IsLast);
        UNIT_ASSERT_VALUES_EQUAL(event3->Data->ToString(),
            "key:   [\n    \"foo\",\n    \"bar\",\n    \"baz\",\n    \"foo\",\n    \"bar\",\n    \"baz\"\n  ]\nvalue:   [\n    \"one\",\n    "
            "\"two\",\n    \"three\",\n    \"one\",\n    \"two\",\n    \"three\"\n  ]\n");
        runtime->Send(new IEventHandle(importActorId, edge, new TEvPrivate::TEvBackupImportRecordBatchResult()));

        auto event4 = runtime->GrabEdgeEvent<TEvPrivate::TEvBackupImportRecordBatch>(handle);
        UNIT_ASSERT(event4->IsLast);
        UNIT_ASSERT(!event4->Data);
    }

    Y_UNIT_TEST(ImportSchemaOrder) {
        Aws::S3::S3Client s3Client = NTestUtils::MakeS3Client();
        NTestUtils::CreateBucket("test_schema_order", s3Client);

        TRuntimePtr runtime(new TTestBasicRuntime());
        runtime->SetLogPriority(NKikimrServices::DATASHARD_BACKUP, NActors::NLog::PRI_DEBUG);
        runtime->SetLogPriority(NKikimrServices::DATASHARD_RESTORE, NActors::NLog::PRI_DEBUG);
        SetupTabletServices(*runtime);

        const auto edge = runtime->AllocateEdgeActor(0);

        // Export 3-column data (key, col_a, col_b) to S3
        auto exportFactory = std::make_shared<TDataShardExportFactory>();
        auto actor = NKikimr::NColumnShard::NBackup::CreateExportUploaderActor(
            edge, MakeBackupTask3Cols("test_schema_order"), exportFactory.get(), MakeYdbColumns3Cols(), 0);
        auto exporter = runtime->Register(actor.release());

        TAutoPtr<IEventHandle> handle;
        runtime->DispatchEvents({}, TDuration::Seconds(1));
        runtime->Send(new IEventHandle(exporter, edge, new NColumnShard::TEvPrivate::TEvBackupExportRecordBatch(TestRecordBatch3Cols(), true)));
        auto exportEvent = runtime->GrabEdgeEvent<NColumnShard::TEvPrivate::TEvBackupExportRecordBatchResult>(handle);
        UNIT_ASSERT(exportEvent->IsFinish);

        runtime->DispatchEvents({}, TDuration::Seconds(5));

        auto restoreTask = MakeRestoreTask3Cols("test_schema_order");
        auto userTable = MakeIntrusiveConst<NDataShard::TUserTable>(ui32(0), restoreTask.GetTableDescription(), ui32(0));

        auto importActor = NKikimr::NColumnShard::NBackup::CreateImportDownloader(
            edge, 0, restoreTask, NKikimr::NDataShard::TTableInfo{ 0, userTable }, MakeYdbSchema3ColsReversed());
        auto importActorId = runtime->Register(importActor.release());
        runtime->DispatchEvents({}, TDuration::Seconds(1));

        auto event3 = runtime->GrabEdgeEvent<TEvPrivate::TEvBackupImportRecordBatch>(handle);
        UNIT_ASSERT(!event3->IsLast);
        UNIT_ASSERT(event3->Data);

        auto batch = event3->Data;
        UNIT_ASSERT_VALUES_EQUAL(batch->num_columns(), 3);
        UNIT_ASSERT_VALUES_EQUAL(batch->schema()->field(0)->name(), "key");
        UNIT_ASSERT_VALUES_EQUAL(batch->schema()->field(1)->name(), "col_a");
        UNIT_ASSERT_VALUES_EQUAL(batch->schema()->field(2)->name(), "col_b");

        UNIT_ASSERT_VALUES_EQUAL(batch->num_rows(), 2);
        UNIT_ASSERT_VALUES_EQUAL(batch->ToString(), "key:   [\n    \"k1\",\n    \"k2\"\n  ]\n"
                                                    "col_a:   [\n    \"a1\",\n    \"a2\"\n  ]\n"
                                                    "col_b:   [\n    \"b1\",\n    \"b2\"\n  ]\n");

        runtime->Send(new IEventHandle(importActorId, edge, new TEvPrivate::TEvBackupImportRecordBatchResult()));

        auto event4 = runtime->GrabEdgeEvent<TEvPrivate::TEvBackupImportRecordBatch>(handle);
        UNIT_ASSERT(event4->IsLast);
        UNIT_ASSERT(!event4->Data);
    }
}

}   // namespace NKikimr
