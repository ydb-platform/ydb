#include <ydb/core/tx/datashard/export_data_format.h>
#include <ydb/core/tx/datashard/export_s3.h>
#include <ydb/core/tx/datashard/export_scan.h>

#include <ydb/core/protos/data_format_settings.pb.h>
#include <ydb/core/protos/flat_scheme_op.pb.h>
#include <ydb/core/protos/fs_settings.pb.h>
#include <ydb/core/protos/s3_settings.pb.h>

#include <ydb/core/testlib/basics/appdata.h>
#include <ydb/core/testlib/basics/runtime.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/events.h>

#include <library/cpp/testing/unittest/registar.h>

#include <ydb/library/testlib/parquet_helpers/parquet_helpers.h>

#include <arrow/api.h>

#ifndef KIKIMR_DISABLE_S3_OPS

#include "export_parquet_ut_enums.h"

namespace NKikimr::NDataShard {

namespace {

    void ConfigureParquetBackupTask(NKikimrSchemeOp::TBackupTask& task, EParquetExportSettings settings, ui32 rowGroupSize) {
        switch (settings) {
        case EParquetExportSettings::FS: {
            auto& fs = *task.MutableFSSettings();
            fs.SetBasePath("/tmp/exports");
            fs.SetPath("backup");
            fs.MutableExportDataSettings()->MutableParquet()->SetRowGroupSize(rowGroupSize);
            break;
        }
        case EParquetExportSettings::S3: {
            auto& s3 = *task.MutableS3Settings();
            s3.SetEndpoint("localhost");
            s3.SetBucket("test-bucket");
            s3.SetObjectKeyPattern("backup");
            s3.MutableExportDataSettings()->MutableParquet()->SetRowGroupSize(rowGroupSize);
            break;
        }
        }
    }

    // Runs a callback inside an actor (so AppData() is available) and replies when done.
    class TCbActor: public NActors::TActorBootstrapped<TCbActor> {
    public:
        TCbActor(std::function<void()> fn, const NActors::TActorId& replyTo)
            : Fn(std::move(fn))
            , ReplyTo(replyTo)
        {
        }

        void Bootstrap() {
            Fn();
            Send(ReplyTo, new NActors::TEvents::TEvWakeup());
            PassAway();
        }

    private:
        std::function<void()> Fn;
        const NActors::TActorId ReplyTo;
    };

    // Builds a Parquet backup task (FS or S3), runs TS3Export::CreateBuffer(), feeds the rows
    // and returns the produced (single) data file bytes.
    TString ExportRowsToParquet(EParquetExportSettings settings, const TVector<std::pair<ui32, TString>>& rows, ui32 rowGroupSize) {
        TTestActorRuntime runtime;
        runtime.Initialize(TAppPrepare().Unwrap());

        TString fileData;

        auto produce = [&]() {
            IExport::TTableColumns columns;
            columns[0] = TUserTable::TUserColumn(NScheme::TTypeInfo(NScheme::NTypeIds::Uint32), "", "key", true);
            columns[1] = TUserTable::TUserColumn(NScheme::TTypeInfo(NScheme::NTypeIds::Utf8), "", "value", false);

            NKikimrSchemeOp::TBackupTask task;
            ConfigureParquetBackupTask(task, settings, rowGroupSize);

            TS3Export exportTask(task, columns);
            THolder<NExportScan::IBuffer> buffer(exportTask.CreateBuffer());
            Y_ENSURE(buffer, "CreateBuffer returned null");

            buffer->ColumnsOrder({0, 1});

            for (const auto& [key, value] : rows) {
                NTable::IScan::TRow row;
                row.Init(2);
                row.Set(0, NKikimr::NTable::ECellOp::Set, NKikimr::TCell::Make(key));
                row.Set(1, NKikimr::NTable::ECellOp::Set, NKikimr::TCell(value.data(), value.size()));
                Y_ENSURE(buffer->Collect(row), "Collect failed: " << buffer->GetError());
            }

            NExportScan::IBuffer::TStats stats;
            THolder<NActors::IEventBase> event(buffer->PrepareEvent(true, stats));
            Y_ENSURE(event, "PrepareEvent returned null: " << buffer->GetError());

            auto* evBuffer = dynamic_cast<TEvExportScan::TEvBuffer<TBuffer>*>(event.Get());
            Y_ENSURE(evBuffer, "Unexpected event type");
            fileData.assign(evBuffer->Buffer.Data(), evBuffer->Buffer.Size());
        };

        const auto edge = runtime.AllocateEdgeActor();
        runtime.Register(new TCbActor(produce, edge));
        runtime.GrabEdgeEventRethrow<NActors::TEvents::TEvWakeup>(edge);

        return fileData;
    }

    TString ExportIntervalUuidDyNumberToParquet(EParquetExportSettings settings) {
        TTestActorRuntime runtime;
        runtime.Initialize(TAppPrepare().Unwrap());

        TString fileData;
        const i64 intervalUs = 1000000;
        const ui8 uuidBytes[16] = {
            0x55, 0x0e, 0x84, 0x00, 0xe2, 0x9b, 0x41, 0xd4,
            0xa7, 0x16, 0x44, 0x66, 0x55, 0x44, 0x00, 0x00,
        };

        const TString dyNumber = ".314e1";

        auto produce = [&]() {
            IExport::TTableColumns columns;
            columns[0] = TUserTable::TUserColumn(NScheme::TTypeInfo(NScheme::NTypeIds::Uint32), "", "key", true);
            columns[1] = TUserTable::TUserColumn(NScheme::TTypeInfo(NScheme::NTypeIds::Interval), "", "ival", false);
            columns[2] = TUserTable::TUserColumn(NScheme::TTypeInfo(NScheme::NTypeIds::Uuid), "", "uid", false);
            columns[3] = TUserTable::TUserColumn(NScheme::TTypeInfo(NScheme::NTypeIds::DyNumber), "", "dyn", false);

            NKikimrSchemeOp::TBackupTask task;
            ConfigureParquetBackupTask(task, settings, /*rowGroupSize=*/1);

            TS3Export exportTask(task, columns);
            THolder<NExportScan::IBuffer> buffer(exportTask.CreateBuffer());
            Y_ENSURE(buffer, "CreateBuffer returned null");

            buffer->ColumnsOrder({0, 1, 2, 3});

            NTable::IScan::TRow row;
            row.Init(4);
            row.Set(0, NKikimr::NTable::ECellOp::Set, NKikimr::TCell::Make<ui32>(1));
            row.Set(1, NKikimr::NTable::ECellOp::Set, NKikimr::TCell::Make(intervalUs));
            row.Set(2, NKikimr::NTable::ECellOp::Set, NKikimr::TCell(reinterpret_cast<const char*>(uuidBytes), sizeof(uuidBytes)));
            row.Set(3, NKikimr::NTable::ECellOp::Set, NKikimr::TCell(dyNumber.data(), dyNumber.size()));
            Y_ENSURE(buffer->Collect(row), "Collect failed: " << buffer->GetError());

            NExportScan::IBuffer::TStats stats;
            THolder<NActors::IEventBase> event(buffer->PrepareEvent(true, stats));
            Y_ENSURE(event, "PrepareEvent returned null: " << buffer->GetError());

            auto* evBuffer = dynamic_cast<TEvExportScan::TEvBuffer<TBuffer>*>(event.Get());
            Y_ENSURE(evBuffer, "Unexpected event type");
            fileData.assign(evBuffer->Buffer.Data(), evBuffer->Buffer.Size());
        };

        const auto edge = runtime.AllocateEdgeActor();
        runtime.Register(new TCbActor(produce, edge));
        runtime.GrabEdgeEventRethrow<NActors::TEvents::TEvWakeup>(edge);

        return fileData;
    }

} // namespace

Y_UNIT_TEST_SUITE(ExportParquetTest) {
    // The Parquet code path in TS3Export::CreateBuffer() (DataFormatFromTask /
    // ParquetExportSettingsFromTask reading S3/FS settings) must produce a valid Parquet file.
    Y_UNIT_TEST(ShouldProduceValidParquet, EParquetExportSettings) {
        const auto settings = Arg<0>();

        const TVector<std::pair<ui32, TString>> rows = {
            {1, "valueA"},
            {2, "valueB"},
            {3, "valueC"},
        };

        const TString data = ExportRowsToParquet(settings, rows, /* rowGroupSize */ 2);
        UNIT_ASSERT(!data.empty());

        const auto table = NTestUtils::ReadParquet(data);
        UNIT_ASSERT_VALUES_EQUAL(table->num_rows(), 3);
        UNIT_ASSERT_VALUES_EQUAL(table->num_columns(), 2);

        const auto keyColumn = table->GetColumnByName("key");
        const auto valueColumn = table->GetColumnByName("value");
        UNIT_ASSERT(keyColumn);
        UNIT_ASSERT(valueColumn);

        const auto keys = std::static_pointer_cast<arrow::Int64Array>(keyColumn->chunk(0));
        const auto values = std::static_pointer_cast<arrow::StringArray>(valueColumn->chunk(0));

        UNIT_ASSERT_VALUES_EQUAL(keys->Value(0), 1);
        UNIT_ASSERT_VALUES_EQUAL(keys->Value(1), 2);
        UNIT_ASSERT_VALUES_EQUAL(keys->Value(2), 3);
        UNIT_ASSERT_VALUES_EQUAL(values->GetString(0), "valueA");
        UNIT_ASSERT_VALUES_EQUAL(values->GetString(1), "valueB");
        UNIT_ASSERT_VALUES_EQUAL(values->GetString(2), "valueC");
    }

    // A small row group size forces multiple Parquet row groups; the file must still
    // round-trip every row correctly.
    Y_UNIT_TEST(ShouldProduceValidParquetWithSmallRowGroup, EParquetExportSettings) {
        const auto settings = Arg<0>();

        TVector<std::pair<ui32, TString>> rows;
        for (ui32 i = 0; i < 50; ++i) {
            rows.emplace_back(i, TStringBuilder() << "value_" << i);
        }

        const TString data = ExportRowsToParquet(settings, rows, /* rowGroupSize */ 1);
        UNIT_ASSERT(!data.empty());

        const auto table = NTestUtils::ReadParquet(data);
        UNIT_ASSERT_VALUES_EQUAL(table->num_rows(), 50);
        UNIT_ASSERT_VALUES_EQUAL(table->num_columns(), 2);

        const auto keys = std::static_pointer_cast<arrow::Int64Array>(table->GetColumnByName("key")->chunk(0));
        const auto values = std::static_pointer_cast<arrow::StringArray>(table->GetColumnByName("value")->chunk(0));
        for (ui32 i = 0; i < 50; ++i) {
            UNIT_ASSERT_VALUES_EQUAL(keys->Value(i), i);
            UNIT_ASSERT_VALUES_EQUAL(values->GetString(i), TStringBuilder() << "value_" << i);
        }
    }

    Y_UNIT_TEST(ShouldProduceValidParquetWithIntervalUuidDyNumber, EParquetExportSettings) {
        const auto settings = Arg<0>();

        const TString data = ExportIntervalUuidDyNumberToParquet(settings);
        UNIT_ASSERT_C(!data.empty(), "Parquet export for Interval/Uuid/DyNumber produced empty data");

        const auto table = NTestUtils::ReadParquet(data);
        UNIT_ASSERT_VALUES_EQUAL(table->num_rows(), 1);
        UNIT_ASSERT_VALUES_EQUAL(table->num_columns(), 4);

        const auto ival = std::static_pointer_cast<arrow::Int64Array>(table->GetColumnByName("ival")->chunk(0));
        UNIT_ASSERT_VALUES_EQUAL(ival->Value(0), 1000000);

        const auto uid = std::static_pointer_cast<arrow::FixedSizeBinaryArray>(table->GetColumnByName("uid")->chunk(0));
        UNIT_ASSERT_VALUES_EQUAL(uid->byte_width(), 16);
        UNIT_ASSERT_VALUES_EQUAL(TString(reinterpret_cast<const char*>(uid->GetValue(0)), 16).size(), 16);

        const auto dyn = std::static_pointer_cast<arrow::BinaryArray>(table->GetColumnByName("dyn")->chunk(0));
        UNIT_ASSERT_VALUES_EQUAL(dyn->GetString(0), ".314e1");
    }
}

} // namespace NKikimr::NDataShard

#endif // KIKIMR_DISABLE_S3_OPS
