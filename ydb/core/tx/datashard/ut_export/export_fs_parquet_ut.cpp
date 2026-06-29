#include <ydb/core/tx/datashard/export_s3.h>
#include <ydb/core/tx/datashard/export_scan.h>

#include <ydb/core/protos/data_format_settings.pb.h>
#include <ydb/core/protos/flat_scheme_op.pb.h>
#include <ydb/core/protos/fs_settings.pb.h>

#include <ydb/core/testlib/basics/appdata.h>
#include <ydb/core/testlib/basics/runtime.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/events.h>

#include <library/cpp/testing/unittest/registar.h>

#include <arrow/api.h>
#include <arrow/io/memory.h>
#include <parquet/arrow/reader.h>

#ifndef KIKIMR_DISABLE_S3_OPS

namespace NKikimr::NDataShard {

namespace {

    // Parses Parquet file content into a single-chunk arrow::Table.
    std::shared_ptr<arrow::Table> ReadParquet(const TString& data) {
        auto input = std::make_shared<arrow::io::BufferReader>(
            reinterpret_cast<const uint8_t*>(data.data()), static_cast<int64_t>(data.size()));

        parquet::arrow::FileReaderBuilder builder;
        UNIT_ASSERT_C(builder.Open(input).ok(), "Failed to open Parquet file");

        std::unique_ptr<parquet::arrow::FileReader> reader;
        UNIT_ASSERT_C(builder.Build(&reader).ok(), "Failed to build Parquet reader");

        std::shared_ptr<arrow::Table> table;
        const auto status = reader->ReadTable(&table);
        UNIT_ASSERT_C(status.ok(), status.message());

        auto combined = table->CombineChunks();
        UNIT_ASSERT_C(combined.ok(), combined.status().message());
        return combined.ValueOrDie();
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

    // Builds an FS-Parquet backup task, runs TS3Export::CreateBuffer(), feeds the rows
    // and returns the produced (single) data file bytes.
    TString ExportRowsToFsParquet(const TVector<std::pair<ui32, TString>>& rows, ui32 rowGroupSize) {
        TTestActorRuntime runtime;
        runtime.Initialize(TAppPrepare().Unwrap());

        TString fileData;

        auto produce = [&]() {
            IExport::TTableColumns columns;
            columns[0] = TUserTable::TUserColumn(NScheme::TTypeInfo(NScheme::NTypeIds::Uint32), "", "key", true);
            columns[1] = TUserTable::TUserColumn(NScheme::TTypeInfo(NScheme::NTypeIds::Utf8), "", "value", false);

            NKikimrSchemeOp::TBackupTask task;
            auto& fs = *task.MutableFSSettings();
            fs.SetBasePath("/tmp/exports");
            fs.SetPath("backup");
            fs.MutableExportDataSettings()->MutableParquet()->SetRowGroupSize(rowGroupSize);

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

} // namespace

Y_UNIT_TEST_SUITE(ExportFsParquetTest) {
    // The FS-Parquet code path in TS3Export::CreateBuffer() (DataFormatFromTask /
    // ParquetExportSettingsFromTask reading FSSettings) must produce a valid Parquet file.
    Y_UNIT_TEST(ShouldProduceValidParquetForFsTask) {
        const TVector<std::pair<ui32, TString>> rows = {
            {1, "valueA"},
            {2, "valueB"},
            {3, "valueC"},
        };

        const TString data = ExportRowsToFsParquet(rows, /* rowGroupSize */ 2);
        UNIT_ASSERT(!data.empty());

        const auto table = ReadParquet(data);
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
    Y_UNIT_TEST(ShouldProduceValidParquetWithSmallRowGroup) {
        TVector<std::pair<ui32, TString>> rows;
        for (ui32 i = 0; i < 50; ++i) {
            rows.emplace_back(i, TStringBuilder() << "value_" << i);
        }

        const TString data = ExportRowsToFsParquet(rows, /* rowGroupSize */ 1);
        UNIT_ASSERT(!data.empty());

        const auto table = ReadParquet(data);
        UNIT_ASSERT_VALUES_EQUAL(table->num_rows(), 50);
        UNIT_ASSERT_VALUES_EQUAL(table->num_columns(), 2);

        const auto keys = std::static_pointer_cast<arrow::Int64Array>(table->GetColumnByName("key")->chunk(0));
        const auto values = std::static_pointer_cast<arrow::StringArray>(table->GetColumnByName("value")->chunk(0));
        for (ui32 i = 0; i < 50; ++i) {
            UNIT_ASSERT_VALUES_EQUAL(keys->Value(i), i);
            UNIT_ASSERT_VALUES_EQUAL(values->GetString(i), TStringBuilder() << "value_" << i);
        }
    }
}

} // namespace NKikimr::NDataShard

#endif // KIKIMR_DISABLE_S3_OPS
