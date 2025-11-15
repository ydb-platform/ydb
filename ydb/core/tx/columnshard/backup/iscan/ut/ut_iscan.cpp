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
#include <ydb/core/tx/columnshard/backup/iscan/iscan.h>
#include <ydb/core/tx/columnshard/columnshard_private_events.h>


namespace NKikimr {

namespace {

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

TVector<std::pair<TString, NScheme::TTypeInfo>> MakeYdbSchema() {
    return {{"key", NScheme::TTypeInfo(NScheme::NTypeIds::String)}, {"value", NScheme::TTypeInfo(NScheme::NTypeIds::String)}};
}

TIntrusiveConstPtr<NTable::TRowScheme> MakeSchema() {
    NTable::TScheme::TTableSchema tableSchema;
    tableSchema.Columns[0] = NTable::TColumn("key", 0, NScheme::TTypeInfo(NScheme::NTypeIds::String), "");
    tableSchema.Columns[0].KeyOrder = 0;

    tableSchema.Columns[1] = NTable::TColumn("value", 1, NScheme::TTypeInfo(NScheme::NTypeIds::String), "");
    tableSchema.Columns[1].KeyOrder = 1;

    return NTable::TRowScheme::Make(tableSchema.Columns, NUtil::TSecond());
}

NDataShard::IExport::TTableColumns MakeYdbColumns() {
    NDataShard::IExport::TTableColumns columns;
    columns[0] = NDataShard::TUserTable::TUserColumn(NScheme::TTypeInfo(NScheme::NTypeIds::String), "", "key", true);
    columns[1] = NDataShard::TUserTable::TUserColumn(NScheme::TTypeInfo(NScheme::NTypeIds::String), "", "value", false);
    return columns;
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
    col1.SetType("String");

    auto& col2 = *tableDescription.MutableSchema()->MutableColumns()->Add();
    col2.SetName("value");
    col2.SetType("String");
    table.MutableSelf();
    return backupTask;
}

using TRuntimePtr = std::shared_ptr<TTestActorRuntime>;

class TGrabActor: public TActorBootstrapped<TGrabActor> {
    std::deque<NThreading::TPromise<TAutoPtr<IEventHandle>>> Futures;
    std::deque<TAutoPtr<IEventHandle>> Inputs;
    TMutex Mutex;
    std::unique_ptr<NColumnShard::NBackup::TExportDriver> Driver;
    std::unique_ptr<NTable::IScan> Exporter;

public:
    TRuntimePtr Runtime;

    TGrabActor(TRuntimePtr runtime)
        : Runtime(runtime)
    { }

    void Bootstrap() {
        NDataShard::IExport::TTableColumns columns;
        columns[0] = NDataShard::TUserTable::TUserColumn(NScheme::TTypeInfo(NScheme::NTypeIds::String), "", "key", true);
        columns[1] = NDataShard::TUserTable::TUserColumn(NScheme::TTypeInfo(NScheme::NTypeIds::String), "", "value", false);

        auto exportFactory = std::make_shared<TDataShardExportFactory>();


        Exporter = NColumnShard::NBackup::CreateIScanExportUploader(SelfId(), MakeBackupTask("test"), exportFactory.get(), columns, 0).DetachResult();
        UNIT_ASSERT(Exporter);
        Driver = std::make_unique<NColumnShard::NBackup::TExportDriver>(TActorContext::ActorSystem(), SelfId());
        auto initialState = Exporter->Prepare(Driver.get(), MakeSchema());
        UNIT_ASSERT_VALUES_EQUAL(initialState.Scan, NTable::EScan::Feed);

        NTable::TLead lead;
        auto seekState = Exporter->Seek(lead, 0);
        UNIT_ASSERT_VALUES_EQUAL(seekState, NTable::EScan::Feed);

        Become(&TGrabActor::StateFunc);
    }

    void SendData() {
        auto recordBatch = TestRecordBatch();
        TVector<TSerializedCellVec> cellVec = NColumnShard::NBackup::BatchToRows(recordBatch, MakeYdbSchema()).DetachResult();
        for (const auto& row: cellVec) {
            NTable::TRowState rowState(row.GetCells().size());
            int i = 0;
            for (const auto& cell: row.GetCells()) {
                rowState.Set(i++, { NTable::ECellOp::Set, NTable::ELargeObj::Inline }, cell);
            }
            Exporter->Feed({}, rowState);
        }
        auto exhaustedState = Exporter->Exhausted();
        UNIT_ASSERT_VALUES_EQUAL(exhaustedState, NTable::EScan::Sleep);
    }

    void Handle(NColumnShard::TEvPrivate::TEvBackupExportState::TPtr& ev) {
        if (ev->Get()->State == NTable::EScan::Final) {
            return;
        }
        SendData();
    }

    STFUNC(StateFunc)
    {
        if (ev->GetTypeRewrite() == NColumnShard::TEvPrivate::TEvBackupExportState::EventType) {
            NColumnShard::TEvPrivate::TEvBackupExportState::TPtr* x = reinterpret_cast<NColumnShard::TEvPrivate::TEvBackupExportState::TPtr*>(&ev);
            Handle(*x);
        }

        TGuard<TMutex> lock(Mutex);
        if (!Futures.empty()) {
            auto front = Futures.front();
            Futures.pop_front();
            front.SetValue(ev);
            return;
        }
        Inputs.push_back(ev);
    }

    NThreading::TFuture<TAutoPtr<IEventHandle>> WaitRequest()
    {
        TGuard<TMutex> lock(Mutex);
        if (!Inputs.empty()) {
            auto front = Inputs.front();
            Inputs.pop_front();
            return NThreading::MakeFuture(front);
        }
        Futures.push_back(NThreading::NewPromise<TAutoPtr<IEventHandle>>());
        return Futures.back();
    }

    TAutoPtr<IEventHandle> GetRequest()
    {
        auto future = WaitRequest();
        while (!future.HasValue()) {
            Runtime->DispatchEvents({}, TDuration::MilliSeconds(1));
        }
        return future.GetValue();
    }
};

}

using namespace NColumnShard;

Y_UNIT_TEST_SUITE(IScan) {
    Y_UNIT_TEST(SimpleExport) {
        Aws::S3::S3Client s3Client = NTestUtils::MakeS3Client();
        NTestUtils::CreateBucket("test", s3Client);

        TRuntimePtr runtime(new TTestBasicRuntime(1, true));
        runtime->SetLogPriority(NKikimrServices::DATASHARD_BACKUP, NActors::NLog::PRI_DEBUG);
        SetupTabletServices(*runtime);

        auto grabActor = new TGrabActor(runtime);
        runtime->Register(grabActor);

        while (true) {
            auto request = grabActor->GetRequest();
            auto event = request->Get<NColumnShard::TEvPrivate::TEvBackupExportState>();
            UNIT_ASSERT_C(event, request->GetTypeName());
            if (event->State == NTable::EScan::Final) {
                break;
            }
        }

        std::vector<TString> result = NTestUtils::GetObjectKeys("test", s3Client);
        UNIT_ASSERT_VALUES_EQUAL(NTestUtils::GetUncommittedUploadsCount("test", s3Client), 0);
        UNIT_ASSERT_VALUES_EQUAL(JoinSeq(",", result), "data_00.csv,metadata.json,permissions.pb,scheme.pb");
        auto scheme = NTestUtils::GetObject("test", "scheme.pb", s3Client);
        UNIT_ASSERT_VALUES_EQUAL(scheme, "columns {\n  name: \"key\"\n  type {\n    optional_type {\n      item {\n        type_id: STRING\n      }\n    }\n  }\n}\ncolumns {\n  name: \"value\"\n  type {\n    optional_type {\n      item {\n        type_id: STRING\n      }\n    }\n  }\n}\npartitioning_settings {\n  min_partitions_count: 4\n}\nstore_type: STORE_TYPE_COLUMN\n");
        auto metadata = NTestUtils::GetObject("test", "metadata.json", s3Client);
        UNIT_ASSERT_VALUES_EQUAL(metadata, "{\"version\":0,\"full_backups\":[{\"snapshot_vts\":[0,0]}],\"permissions\":1,\"changefeeds\":[]}");
        auto data = NTestUtils::GetObject("test", "data_00.csv", s3Client);
        UNIT_ASSERT_VALUES_EQUAL(data, "\"foo\",\"one\"\n\"bar\",\"two\"\n\"baz\",\"three\"\n");
    }

    Y_UNIT_TEST(UploaderExport) {
        Aws::S3::S3Client s3Client = NTestUtils::MakeS3Client();
        NTestUtils::CreateBucket("test1", s3Client);

        TRuntimePtr runtime(new TTestBasicRuntime());
        runtime->SetLogPriority(NKikimrServices::DATASHARD_BACKUP, NActors::NLog::PRI_DEBUG);
        SetupTabletServices(*runtime);

        const auto edge = runtime->AllocateEdgeActor(0);
        auto exportFactory = std::make_shared<TDataShardExportFactory>();
        auto actor = NKikimr::NColumnShard::NBackup::CreateExportUploaderActor(edge, MakeBackupTask("test1"), exportFactory.get(), MakeYdbColumns(), 0);
        auto exporter = runtime->Register(actor.release());

        TAutoPtr<IEventHandle> handle;
        runtime->DispatchEvents({}, TDuration::Seconds(1));
        runtime->Send(new IEventHandle(exporter, edge, new NColumnShard::TEvPrivate::TEvBackupExportRecordBatch(TestRecordBatch(), true)));
        auto event = runtime->GrabEdgeEvent<NColumnShard::TEvPrivate::TEvBackupExportRecordBatchResult>(handle);
        UNIT_ASSERT(event->IsFinish);

        runtime->DispatchEvents({}, TDuration::Seconds(5));
        std::vector<TString> result = NTestUtils::GetObjectKeys("test1", s3Client);
        UNIT_ASSERT_VALUES_EQUAL(NTestUtils::GetUncommittedUploadsCount("test1", s3Client), 0);
        UNIT_ASSERT_VALUES_EQUAL(JoinSeq(",", result), "data_00.csv,metadata.json,permissions.pb,scheme.pb");
        auto scheme = NTestUtils::GetObject("test1", "scheme.pb", s3Client);
        UNIT_ASSERT_VALUES_EQUAL(scheme, "columns {\n  name: \"key\"\n  type {\n    optional_type {\n      item {\n        type_id: STRING\n      }\n    }\n  }\n}\ncolumns {\n  name: \"value\"\n  type {\n    optional_type {\n      item {\n        type_id: STRING\n      }\n    }\n  }\n}\npartitioning_settings {\n  min_partitions_count: 4\n}\nstore_type: STORE_TYPE_COLUMN\n");
        auto metadata = NTestUtils::GetObject("test1", "metadata.json", s3Client);
        UNIT_ASSERT_VALUES_EQUAL(metadata, "{\"version\":0,\"full_backups\":[{\"snapshot_vts\":[0,0]}],\"permissions\":1,\"changefeeds\":[]}");
        auto data = NTestUtils::GetObject("test1", "data_00.csv", s3Client);
        UNIT_ASSERT_VALUES_EQUAL(data, "\"foo\",\"one\"\n\"bar\",\"two\"\n\"baz\",\"three\"\n");
    }

    Y_UNIT_TEST(MultiExport) {
        Aws::S3::S3Client s3Client = NTestUtils::MakeS3Client();
        NTestUtils::CreateBucket("test2", s3Client);

        TRuntimePtr runtime(new TTestBasicRuntime());
        runtime->SetLogPriority(NKikimrServices::DATASHARD_BACKUP, NActors::NLog::PRI_DEBUG);
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
        UNIT_ASSERT_VALUES_EQUAL(scheme, "columns {\n  name: \"key\"\n  type {\n    optional_type {\n      item {\n        type_id: STRING\n      }\n    }\n  }\n}\ncolumns {\n  name: \"value\"\n  type {\n    optional_type {\n      item {\n        type_id: STRING\n      }\n    }\n  }\n}\npartitioning_settings {\n  min_partitions_count: 4\n}\nstore_type: STORE_TYPE_COLUMN\n");
        auto metadata = NTestUtils::GetObject("test2", "metadata.json", s3Client);
        UNIT_ASSERT_VALUES_EQUAL(metadata, "{\"version\":0,\"full_backups\":[{\"snapshot_vts\":[0,0]}],\"permissions\":1,\"changefeeds\":[]}");
        auto data = NTestUtils::GetObject("test2", "data_00.csv", s3Client);
        UNIT_ASSERT_VALUES_EQUAL(data, "\"foo\",\"one\"\n\"bar\",\"two\"\n\"baz\",\"three\"\n\"foo\",\"one\"\n\"bar\",\"two\"\n\"baz\",\"three\"\n");
    }
}

} // namespace NKikimr
