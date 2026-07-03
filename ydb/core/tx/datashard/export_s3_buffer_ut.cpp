#include "export_s3_buffer.h"

#include <ydb/core/tx/datashard/backup_restore_traits.h>
#include <ydb/core/tx/datashard/export_scan.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/array_ref.h>

#ifndef KIKIMR_DISABLE_S3_OPS

namespace NKikimr::NDataShard {

using NBackupRestoreTraits::EDataFormat;

class TExportS3BufferFixture : public NUnitTest::TBaseFixture {
public:
    void SetUp(NUnitTest::TTestContext&) override {
        TVector<ui32> tags;
        {
            ui32 tag = 0;
            Columns[tag] = TUserTable::TUserColumn(NScheme::TTypeInfo(NScheme::NTypeIds::Uint32), "", "key", true);
            tags.push_back(tag);
        }

        for (ui32 tag = 1; tag < 20; ++tag) {
            auto name = "value" + ToString(tag);
            Columns[tag] = TUserTable::TUserColumn(NScheme::TTypeInfo(NScheme::NTypeIds::String), "", name, false);
            tags.push_back(tag);
        }
        Tags.swap(tags);
    }

    TS3ExportBufferSettings& Settings() {
        return S3ExportBufferSettings;
    }

    IExport::TTableColumns& TableColumns() {
        return Columns;
    }

    NExportScan::IBuffer* CreateBuffer(EDataFormat dataFormat) {
        TS3ExportBufferSettings settings = S3ExportBufferSettings;
        settings.WithColumns(Columns);
        
        NExportScan::IBuffer* buffer = nullptr;
        switch (dataFormat) {
        case EDataFormat::YdbDump:
            {
                TYdbDumpExportSettings dataFormatSettings;
                dataFormatSettings.WithColumns(Columns);
                auto dataFormat = CreateExportDataFormat(std::move(dataFormatSettings));
                buffer = CreateS3ExportBuffer(std::move(settings), std::move(dataFormat));
                break;
            }
        case EDataFormat::Parquet:
            {
                TParquetExportSettings dataFormatSettings;
                dataFormatSettings.WithColumns(Columns);
                dataFormatSettings.WithRowGroupSize(100);
                // Mirror production: Parquet handles compression internally, so
                // buffer-level compression is disabled and the codec is forwarded
                // to the Parquet writer instead.
                if (settings.CompressionSettings) {
                    dataFormatSettings.WithCompression(TParquetExportSettings::TCompressionSettings()
                        .WithAlgorithm(TParquetExportSettings::TCompressionSettings::EAlgorithm::Zstd)
                        .WithLevel(settings.CompressionSettings->CompressionLevel));
                    settings.WithoutCompression();
                }
                auto dataFormat = CreateExportDataFormat(std::move(dataFormatSettings));
                buffer = CreateS3ExportBuffer(std::move(settings), std::move(dataFormat));
                break;
            }
        case EDataFormat::Invalid:
            break;
        }

        if (!buffer) {
            return nullptr;
        }

        buffer->ColumnsOrder(Tags);

        return buffer;
    }

    NExportScan::IBuffer* Buffer(EDataFormat dataFormat) {
        THolder<NExportScan::IBuffer>* exportBuffer = nullptr;
        switch (dataFormat) {
            case EDataFormat::YdbDump:
                exportBuffer = &S3ExportBuffer;
                break;
            case EDataFormat::Parquet:
                exportBuffer = &ParquetBuffer;
                break;
            case EDataFormat::Invalid:
                break;
        }

        if (!exportBuffer) {
            return nullptr;
        }

        if (!*exportBuffer) {
            exportBuffer->Reset(CreateBuffer(dataFormat));
        }
        return exportBuffer->Get();
    }

    bool CollectKeyValue(EDataFormat dataFormat, ui32 k, TStringBuf v) {
        NTable::IScan::TRow row;
        row.Init(Columns.size());
        row.Set(0, NKikimr::NTable::ECellOp::Set, NKikimr::TCell::Make(k));
        // The cells below reference the backing strings, so they must outlive the Collect() call.
        TVector<TString> values;
        values.reserve(Columns.size());
        for (ui32 tag = 1; tag < Columns.size(); ++tag) {
            const auto& value = values.emplace_back(TStringBuilder() << v << "_" << tag << "_" << k);
            row.Set(tag, NKikimr::NTable::ECellOp::Set, NKikimr::TCell(value.data(), value.size()));
        }
        auto buffer = Buffer(dataFormat);
        auto res = buffer->Collect(row);
        return res;
    }

    // Tests impl
    void TestMinBufferSize(EDataFormat dataFormat, ui64 minBufferSize) {
        if (dataFormat == EDataFormat::Invalid) {
            return;
        }

        for (ui32 i = 0; i < 10000; ++i) {
            UNIT_ASSERT(CollectKeyValue(dataFormat, i, TString("v") * 16));
            NExportScan::IBuffer::TStats stats;
            auto buffer = Buffer(dataFormat);
            if (buffer->IsFilled()) {
                THolder<NActors::IEventBase> event(buffer->PrepareEvent(false, stats));
                UNIT_ASSERT(event);
                auto* evBuffer = dynamic_cast<NKikimr::NDataShard::TEvExportScan::TEvBuffer<TBuffer>*>(event.get());
                UNIT_ASSERT(evBuffer);
                UNIT_ASSERT_GE_C(evBuffer->Buffer.Size(), minBufferSize, "Got buffer size " << evBuffer->Buffer.Size() << ". Iteration: " << i);
            }
        }
    }

public:
    TVector<ui32> Tags;
    IExport::TTableColumns Columns;
    TS3ExportBufferSettings S3ExportBufferSettings;
    THolder<NExportScan::IBuffer> S3ExportBuffer;
    THolder<NExportScan::IBuffer> ParquetBuffer;
};

Y_UNIT_TEST_SUITE_F(ExportS3BufferTest, TExportS3BufferFixture) {
    Y_UNIT_TEST(MinBufferSize, EDataFormat) {
        ui64 minBufferSize = 100000;
        auto dataFormat = Arg<0>();
        Settings()
            .WithMaxRows(2)
            .WithMinBytes(minBufferSize)
            .WithMaxBytes(1'000'000);

        TestMinBufferSize(dataFormat, minBufferSize);
    }

    Y_UNIT_TEST(MinBufferSizeSmall, EDataFormat) {
        ui64 minBufferSize = 1000;
        auto dataFormat = Arg<0>();
        Settings()
            .WithMaxRows(2)
            .WithMinBytes(minBufferSize)
            .WithMaxBytes(1'000'000);

        TestMinBufferSize(dataFormat, minBufferSize);
    }

    Y_UNIT_TEST(MinBufferSizeWithCompression, EDataFormat) {
        ui64 minBufferSize = 100000;
        auto dataFormat = Arg<0>();
        Settings()
            .WithCompression(TS3ExportBufferSettings::ZstdCompression(20))
            .WithMaxRows(2)
            .WithMinBytes(minBufferSize)
            .WithMaxBytes(1'000'000);

        TestMinBufferSize(dataFormat, minBufferSize);
    }

    Y_UNIT_TEST(MinBufferSizeWithCompressionAndEncryption, EDataFormat) {
        ui64 minBufferSize = 100000;
        auto dataFormat = Arg<0>();
        Settings()
            .WithCompression(TS3ExportBufferSettings::ZstdCompression(20))
            .WithEncryption(TS3ExportBufferSettings::TEncryptionSettings()
                .WithAlgorithm("AES-256-GCM")
                .WithIV(NBackup::TEncryptionIV::Generate())
                .WithKey(NBackup::TEncryptionKey("256 bit test symmetric key bytes")))
            .WithMaxRows(2)
            .WithMinBytes(minBufferSize)
            .WithMaxBytes(1'000'000);

        TestMinBufferSize(dataFormat, minBufferSize);
    }
}

} // namespace NKikimr::NDataShard

#endif // KIKIMR_DISABLE_S3_OPS
