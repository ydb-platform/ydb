#include <ydb/core/tx/datashard/export_data_format.h>
#include <ydb/core/tx/datashard/export_s3.h>
#include <ydb/core/tx/datashard/export_s3_buffer.h>
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

#include <util/generic/size_literals.h>
#include <util/system/sanitizers.h>

#include <arrow/api.h>
#include <arrow/io/memory.h>
#include <parquet/file_reader.h>

#include <util/string/cast.h>
#include <util/string/join.h>

#include <cstring>
#include <functional>

#ifndef KIKIMR_DISABLE_S3_OPS

#include "export_parquet_ut_enums.h"

namespace NKikimr::NDataShard {

namespace {

    // A bytesBatchSize of 0 leaves the export batch to the config (BackupBytesBatchSize).
    void ConfigureParquetBackupTask(NKikimrSchemeOp::TBackupTask& task, EParquetExportSettings settings, ui32 rowGroupSize, ui64 bytesBatchSize = 0) {
        if (bytesBatchSize) {
            task.MutableScanSettings()->SetBytesBatchSize(bytesBatchSize);
        }
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

    IExport::TTableColumns KeyValueColumns() {
        IExport::TTableColumns columns;
        columns[0] = TUserTable::TUserColumn(NScheme::TTypeInfo(NScheme::NTypeIds::Uint32), "", "key", true);
        columns[1] = TUserTable::TUserColumn(NScheme::TTypeInfo(NScheme::NTypeIds::Utf8), "", "value", false);
        return columns;
    }

    // Feeds the rows as the export scan does, sending a part whenever the buffer is filled,
    // and returns the parts. The callback sees the buffer after every row, told whether it is filled.
    TVector<TString> CollectParts(NExportScan::IBuffer& buffer, const TVector<std::pair<ui32, TString>>& rows,
            const std::function<void(bool filled)>& onCollected = {}) {
        TVector<TString> parts;

        auto sendBuffer = [&](bool last) {
            NExportScan::IBuffer::TStats stats;
            THolder<NActors::IEventBase> event(buffer.PrepareEvent(last, stats));
            Y_ENSURE(event, "PrepareEvent returned null: " << buffer.GetError());

            auto* evBuffer = dynamic_cast<TEvExportScan::TEvBuffer<TBuffer>*>(event.Get());
            Y_ENSURE(evBuffer, "Unexpected event type");
            parts.emplace_back(evBuffer->Buffer.Data(), evBuffer->Buffer.Size());
        };

        buffer.ColumnsOrder({0, 1});

        for (const auto& [key, value] : rows) {
            NTable::IScan::TRow row;
            row.Init(2);
            row.Set(0, NKikimr::NTable::ECellOp::Set, NKikimr::TCell::Make(key));
            row.Set(1, NKikimr::NTable::ECellOp::Set, NKikimr::TCell(value.data(), value.size()));
            Y_ENSURE(buffer.Collect(row), "Collect failed: " << buffer.GetError());

            const bool filled = buffer.IsFilled();
            if (onCollected) {
                onCollected(filled);
            }
            if (filled) {
                sendBuffer(false);
            }
        }

        sendBuffer(true);

        return parts;
    }

    TString CollectRows(NExportScan::IBuffer& buffer, const TVector<std::pair<ui32, TString>>& rows) {
        return JoinSeq("", CollectParts(buffer, rows));
    }

    // Builds a Parquet backup task (FS or S3), runs TS3Export::CreateBuffer(), feeds the rows
    // and returns the produced data file bytes.
    TString ExportRowsToParquet(EParquetExportSettings settings, const TVector<std::pair<ui32, TString>>& rows, ui32 rowGroupSize, ui64 bytesBatchSize = 0) {
        TTestActorRuntime runtime;
        runtime.Initialize(TAppPrepare().Unwrap());

        TString fileData;

        auto produce = [&]() {
            NKikimrSchemeOp::TBackupTask task;
            ConfigureParquetBackupTask(task, settings, rowGroupSize, bytesBatchSize);

            TS3Export exportTask(task, KeyValueColumns());
            THolder<NExportScan::IBuffer> buffer(exportTask.CreateBuffer());
            Y_ENSURE(buffer, "CreateBuffer returned null");

            fileData = CollectRows(*buffer, rows);
        };

        const auto edge = runtime.AllocateEdgeActor();
        runtime.Register(new TCbActor(produce, edge));
        runtime.GrabEdgeEventRethrow<NActors::TEvents::TEvWakeup>(edge);

        return fileData;
    }

    THolder<NExportScan::IBuffer> CreateParquetBuffer(TS3ExportBufferSettings bufferSettings, ui64 rowGroupSize, ui64 rowGroupBytes) {
        TParquetExportSettings parquetSettings;
        parquetSettings
            .WithColumns(KeyValueColumns())
            .WithRowGroupSize(rowGroupSize)
            .WithRowGroupBytes(rowGroupBytes);
        bufferSettings.WithColumns(KeyValueColumns());

        return THolder<NExportScan::IBuffer>(CreateS3ExportBuffer(
            std::move(bufferSettings), CreateExportDataFormat(std::move(parquetSettings))));
    }

    // Feeds the rows to a Parquet buffer with the given limits of a row group and returns
    // the file. MinBytes and MaxBytes of 1 send a part as soon as a row group is encoded.
    TString ExportRowsToParquet(const TVector<std::pair<ui32, TString>>& rows, ui64 rowGroupSize, ui64 rowGroupBytes) {
        TS3ExportBufferSettings bufferSettings;
        bufferSettings
            .WithMaxRows(Max<ui64>())
            .WithMinBytes(1)
            .WithMaxBytes(1);

        const auto buffer = CreateParquetBuffer(std::move(bufferSettings), rowGroupSize, rowGroupBytes);
        return CollectRows(*buffer, rows);
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

    TString RowGroupRows(const TString& data) {
        const auto metadata = parquet::ReadMetaData(std::make_shared<arrow::io::BufferReader>(
            reinterpret_cast<const uint8_t*>(data.data()), static_cast<int64_t>(data.size())));

        TVector<i64> rows;
        for (int i = 0; i < metadata->num_row_groups(); ++i) {
            rows.push_back(metadata->RowGroup(i)->num_rows());
        }
        return JoinSeq(",", rows);
    }

    void CheckRows(const TString& data, const TVector<std::pair<ui32, TString>>& rows) {
        const auto table = NTestUtils::ReadParquet(data);
        UNIT_ASSERT_VALUES_EQUAL(table->num_rows(), rows.size());

        const auto keys = std::static_pointer_cast<arrow::Int64Array>(table->GetColumnByName("key")->chunk(0));
        const auto values = std::static_pointer_cast<arrow::StringArray>(table->GetColumnByName("value")->chunk(0));
        for (size_t i = 0; i < rows.size(); ++i) {
            UNIT_ASSERT_VALUES_EQUAL_C(keys->Value(i), rows[i].first, "row " << i);
            const auto value = values->GetView(i);
            UNIT_ASSERT_C(TStringBuf(value.data(), value.size()) == rows[i].second, "row " << i);
        }
    }

    struct TWideRowsExport {
        ui64 BatchBytes = 0; // the batch of the buffer
        ui32 Collected = 0;
        ui64 Parts = 0; // sent before the last one
        ui64 MaxMemoryBytes = 0;
        i64 MaxArrowBytesAfterPart = 0; // allocated by Arrow after a part is sent, over the start
        TString Error; // of the first failed call
        bool LastPartPrepared = false;
    };

    // Runs TS3Export::CreateBuffer() with S3 settings and the batch (0 for the default) and
    // feeds rows of a key and a value of valueSize bytes, sending a part whenever the buffer
    // is filled as the scan does, up to the first error.
    TWideRowsExport ExportWideRows(ui64 bytesBatchSize, ui32 rowCount, size_t valueSize) {
        TTestActorRuntime runtime;
        runtime.Initialize(TAppPrepare().Unwrap());

        TWideRowsExport result;
        TVector<char> value(valueSize, 'x');
        auto* const arrowPool = arrow::default_memory_pool();
        const i64 arrowBytesAtStart = arrowPool->bytes_allocated();

        auto produce = [&]() {
            NKikimrSchemeOp::TBackupTask task;
            ConfigureParquetBackupTask(task, EParquetExportSettings::S3, /* rowGroupSize */ 10000, bytesBatchSize);
            result.BatchBytes = bytesBatchSize ? bytesBatchSize : AppData()->DataShardConfig.GetBackupBytesBatchSize();

            TS3Export exportTask(task, KeyValueColumns());
            THolder<NExportScan::IBuffer> buffer(exportTask.CreateBuffer());
            Y_ENSURE(buffer, "CreateBuffer returned null");

            buffer->ColumnsOrder({0, 1});

            NExportScan::IBuffer::TStats stats;
            for (ui32 key = 0; key < rowCount; ++key) {
                // Distinct values encode about as they are (a dictionary folds equal ones).
                const TString keyText = ToString(key);
                memcpy(value.data(), keyText.data(), keyText.size());

                NTable::IScan::TRow row;
                row.Init(2);
                row.Set(0, NKikimr::NTable::ECellOp::Set, NKikimr::TCell::Make(key));
                row.Set(1, NKikimr::NTable::ECellOp::Set, NKikimr::TCell(value.data(), value.size()));
                if (!buffer->Collect(row)) {
                    result.Error = buffer->GetError();
                    break;
                }
                ++result.Collected;
                result.MaxMemoryBytes = Max(result.MaxMemoryBytes, buffer->GetMemoryBytes());
                if (buffer->IsFilled()) {
                    THolder<NActors::IEventBase> event(buffer->PrepareEvent(false, stats));
                    if (!event) {
                        result.Error = buffer->GetError();
                        break;
                    }
                    ++result.Parts;
                    result.MaxArrowBytesAfterPart = Max<i64>(result.MaxArrowBytesAfterPart, arrowPool->bytes_allocated() - arrowBytesAtStart);
                }
            }

            // After an error the buffer prepares no part.
            THolder<NActors::IEventBase> event(buffer->PrepareEvent(true, stats));
            result.LastPartPrepared = bool(event);
        };

        const auto edge = runtime.AllocateEdgeActor();
        runtime.Register(new TCbActor(produce, edge));
        runtime.GrabEdgeEventRethrow<NActors::TEvents::TEvWakeup>(edge);

        return result;
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

    // A small row group size forces multiple Parquet row groups (the limit on their bytes,
    // the export batch, does not replace it); the file must still round-trip every row.
    Y_UNIT_TEST(ShouldProduceValidParquetWithSmallRowGroup, EParquetExportSettings) {
        const auto settings = Arg<0>();

        TVector<std::pair<ui32, TString>> rows;
        for (ui32 i = 0; i < 50; ++i) {
            rows.emplace_back(i, TStringBuilder() << "value_" << i);
        }

        const TString data = ExportRowsToParquet(settings, rows, /* rowGroupSize */ 1);

        UNIT_ASSERT_VALUES_EQUAL(RowGroupRows(data), JoinSeq(",", TVector<ui32>(50, 1)));
        CheckRows(data, rows);
    }

    // A row group is cut by the cell bytes of its rows (0 is no limit) as well as by their
    // number. The row that does not fit begins the next row group and counts towards the
    // memory of the buffer, so a part is sent at about MaxBytes with it, not a row group
    // later, and it is kept across the part rather than written as a row group of its own.
    Y_UNIT_TEST(ShouldCutRowGroupsByBytes) {
        // Rows of a 4-byte key and 24 KB: a row group is two rows, 48 KB. The pending row after
        // it brings the memory over the 64 KB part, before a second row group would make the part 96 KB.
        TVector<std::pair<ui32, TString>> rows;
        for (ui32 i = 0; i < 9; ++i) {
            rows.emplace_back(i, TString(24_KB, static_cast<char>('a' + i)));
        }

        const ui64 maxBytes = 64_KB;
        TS3ExportBufferSettings bufferSettings;
        bufferSettings
            .WithMaxRows(Max<ui64>())
            .WithMinBytes(1)
            .WithMaxBytes(maxBytes);
        const auto buffer = CreateParquetBuffer(std::move(bufferSettings), /* rowGroupSize */ 1000, /* rowGroupBytes */ maxBytes);

        ui64 maxMemoryBytes = 0;
        const TVector<TString> parts = CollectParts(*buffer, rows, [&](bool filled) {
            maxMemoryBytes = Max(maxMemoryBytes, buffer->GetMemoryBytes());
            if (!filled) {
                UNIT_ASSERT_LT_C(buffer->GetMemoryBytes(), maxBytes,
                    "Buffer is not filled but holds " << buffer->GetMemoryBytes() << " bytes in memory");
            }
        });

        // A part per row group, and the last row with the footer.
        UNIT_ASSERT_VALUES_EQUAL(parts.size(), 5);
        for (const auto& part : parts) {
            UNIT_ASSERT_LE_C(part.size(), maxBytes, "part of " << part.size() << " bytes");
        }
        // The memory peaks at the part size and the row that fills it.
        UNIT_ASSERT_LT(maxMemoryBytes, maxBytes + 32_KB);

        const TString data = JoinSeq("", parts);
        UNIT_ASSERT_VALUES_EQUAL(RowGroupRows(data), "2,2,2,2,1");
        CheckRows(data, rows);

        const TString unlimited = ExportRowsToParquet(rows, /* rowGroupSize */ 1000, /* rowGroupBytes */ 0);
        UNIT_ASSERT_VALUES_EQUAL(RowGroupRows(unlimited), "9");
    }

    // A row group takes rows up to the limit inclusive. A row at or over the limit is a
    // row group of its own.
    Y_UNIT_TEST(ShouldFillARowGroupUpToTheLimit) {
        // A key takes 4 bytes of the limit.
        const TString half(32_KB - 4, 'h');
        const TVector<std::pair<ui32, TString>> rows = {
            {1, half},
            {2, half},
            {3, TString(100_KB, 'w')},
            {4, TString(64_KB - 4, 'l')},
            {5, half},
            {6, half},
        };

        const TString data = ExportRowsToParquet(rows, /* rowGroupSize */ 1000, /* rowGroupBytes */ 64_KB);

        UNIT_ASSERT_VALUES_EQUAL(RowGroupRows(data), "2,1,1,2");
        CheckRows(data, rows);
    }

    // A part cut by its rows before any row group is written takes the pending rows as a
    // row group, so that it is not empty.
    Y_UNIT_TEST(ShouldWritePendingRowsIntoAPartWithoutARowGroup) {
        TVector<std::pair<ui32, TString>> rows;
        for (ui32 i = 0; i < 7; ++i) {
            rows.emplace_back(i, TStringBuilder() << "value_" << i);
        }

        TS3ExportBufferSettings bufferSettings;
        bufferSettings
            .WithMaxRows(3)
            .WithMinBytes(0) // a part is sent without encoded output
            .WithMaxBytes(Max<ui64>());
        const auto buffer = CreateParquetBuffer(std::move(bufferSettings), /* rowGroupSize */ 1000, /* rowGroupBytes */ 64_KB);

        const TString data = CollectRows(*buffer, rows);

        UNIT_ASSERT_VALUES_EQUAL(RowGroupRows(data), "3,3,1");
        CheckRows(data, rows);
    }

    // The export cuts row groups by its batch size (BytesBatchSize of the scan settings,
    // BackupBytesBatchSize of the config by default).
    Y_UNIT_TEST(ShouldCutRowGroupsByTheExportBatchSize) {
        TVector<std::pair<ui32, TString>> rows;
        for (ui32 i = 0; i < 9; ++i) {
            rows.emplace_back(i, TString(24_KB, static_cast<char>('a' + i)));
        }

        const TString data = ExportRowsToParquet(EParquetExportSettings::S3, rows, /* rowGroupSize */ 1000, /* bytesBatchSize */ 64_KB);

        UNIT_ASSERT_VALUES_EQUAL(RowGroupRows(data), "2,2,2,2,1");
        CheckRows(data, rows);
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

    // Fewer rows than the row group size, but more than 2 GiB of data in one column: arrow binary arrays
    // use 32-bit offsets, so the row group cannot be built. Collect() has to report the error (the export
    // fails) instead of aborting the process. The export batch cuts row groups too, so the rows reach
    // the capacity only with a batch above it.
    Y_UNIT_TEST(ShouldReportArrowCapacityErrorForLargeRowGroup) {
        if (NSan::PlainOrUnderSanitizer(false, true)) {
            return; // the arrow buffer alone takes 2 GiB
        }

        const ui32 rowCount = 10000;
        const auto result = ExportWideRows(/* bytesBatchSize */ 4_GB, rowCount, /* valueSize */ 1_MB);

        UNIT_ASSERT_C(!result.Error.empty(), "all " << result.Collected << " rows were collected");
        UNIT_ASSERT_GE(result.Collected, 2000u);
        UNIT_ASSERT_LT(result.Collected, rowCount);
        UNIT_ASSERT_STRING_CONTAINS(result.Error, "Failed to append row to arrow batch");
        UNIT_ASSERT_STRING_CONTAINS(result.Error, "Capacity error");
        UNIT_ASSERT_STRING_CONTAINS(result.Error, "array cannot contain more than");
        UNIT_ASSERT(!result.LastPartPrepared);
    }

    // With the default batch, rows past the capacity of an Arrow array together export in
    // parts of about a batch, and the buffer holds about a batch.
    Y_UNIT_TEST(ShouldCutRowGroupsBelowTheArrowCapacityByDefault) {
        if (NSan::PlainOrUnderSanitizer(false, true)) {
            return; // the rows alone are 2 GiB to move
        }

        // A row group is 21 rows of 1.5 MB, half a megabyte short of the batch, so that the
        // row pending after it brings the buffer over the batch and the part is sent.
        const size_t valueSize = 1536_KB;
        const ui32 rowCount = static_cast<ui32>(2_GB / valueSize) + 64;
        const auto result = ExportWideRows(/* bytesBatchSize */ 0, rowCount, valueSize);

        UNIT_ASSERT_C(result.Error.empty(), "collected " << result.Collected << " rows, then: " << result.Error);
        UNIT_ASSERT_VALUES_EQUAL(result.Collected, rowCount);
        UNIT_ASSERT(result.LastPartPrepared);
        // A part per row group of about a batch, and the last part with the footer.
        UNIT_ASSERT_GE(result.Parts, ui64(rowCount) * valueSize / result.BatchBytes - 1);
        // The buffer holds a part and the row that fills it, not a row group near the capacity.
        UNIT_ASSERT_LT(result.MaxMemoryBytes, result.BatchBytes + 2 * valueSize);
        // The written row group is released: after a part is sent, Arrow holds the pending
        // rows, not the row group of the part as well.
        UNIT_ASSERT_LT(result.MaxArrowBytesAfterPart, static_cast<i64>(result.BatchBytes / 2));
    }
}

} // namespace NKikimr::NDataShard

#endif // KIKIMR_DISABLE_S3_OPS
