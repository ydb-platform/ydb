#include "export_s3_buffer.h"
#include "ut_export/export_enums.h"

#include <ydb/core/tx/datashard/export_scan.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/array_ref.h>

#include <chrono>

#ifndef KIKIMR_DISABLE_S3_OPS

namespace NKikimr::NDataShard {

class TExportS3BufferFixture : public NUnitTest::TBaseFixture {
public:
    void SetUp(NUnitTest::TTestContext&) override {
        Columns[0] = TUserTable::TUserColumn(NScheme::TTypeInfo(NScheme::NTypeIds::Uint32), "", "key", true);
        Columns[1] = TUserTable::TUserColumn(NScheme::TTypeInfo(NScheme::NTypeIds::String), "", "value", false);
        TVector<ui32> tags{0, 1};

        for (ui32 tag = 2; tag < 20; ++tag) {
            auto name = "key" + ToString(tag);
            Columns[tag] = TUserTable::TUserColumn(NScheme::TTypeInfo(NScheme::NTypeIds::Uint32), "", name, false);
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
        settings.WithRowGroupSize(1000);
        
        NExportScan::IBuffer* buffer = nullptr;
        switch (dataFormat) {
        case EDataFormat::CSV:
            buffer = CreateS3ExportBuffer(std::move(settings));
            break;
        case EDataFormat::PARQUET:
            buffer = CreateS3ParquetExportBuffer(std::move(settings));
            break;
        default:
            return nullptr;
        }
        
        buffer->ColumnsOrder(Tags);

        return buffer;
    }

    NExportScan::IBuffer* Buffer(EDataFormat dataFormat) {
        THolder<NExportScan::IBuffer>* exportBuffer;
        switch (dataFormat) {
            case EDataFormat::CSV:
                exportBuffer = &S3ExportBuffer;
                break;
            case EDataFormat::PARQUET:
                exportBuffer = &ParquetBuffer;
                break;
            default:
                UNIT_FAIL((std::ostringstream() << "Unknown data format " << int(dataFormat)).str());
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
        row.Set(1, NKikimr::NTable::ECellOp::Set, NKikimr::TCell(v.data(), v.size()));
        for (ui32 tag = 2; tag < Columns.size(); ++tag) {
            row.Set(tag, NKikimr::NTable::ECellOp::Set, NKikimr::TCell::Make(100000*tag+k));
        }
        auto buffer = Buffer(dataFormat);
        auto res = buffer->Collect(row);
        return res;
    }

    // Tests impl
    void TestMinBufferSize(ui64 minBufferSize, EDataFormat dataFormat) {
        for (ui32 i = 0; i < 10000; ++i) {
            UNIT_ASSERT(CollectKeyValue(dataFormat, i, "1111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111"));
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
        ui64 minBufferSize = 5000;
        auto dataFormat = Arg<0>();
        Settings()
            .WithMaxRows(2)
            .WithMinBytes(minBufferSize)
            .WithMaxBytes(1'000'000);

        TestMinBufferSize(minBufferSize, dataFormat);
    }

    Y_UNIT_TEST(MinBufferSizeSmall, EDataFormat) {
        ui64 minBufferSize = 1000;
        auto dataFormat = Arg<0>();
        Settings()
            .WithMaxRows(2)
            .WithMinBytes(minBufferSize)
            .WithMaxBytes(1'000'000);

        TestMinBufferSize(minBufferSize, dataFormat);
    }

    Y_UNIT_TEST(MinBufferSizeWithCompression, EDataFormat) {
        ui64 minBufferSize = 5000;
        auto dataFormat = Arg<0>();
        Settings()
            .WithCompression(TS3ExportBufferSettings::ZstdCompression(20))
            .WithMaxRows(2)
            .WithMinBytes(minBufferSize)
            .WithMaxBytes(1'000'000);

        TestMinBufferSize(minBufferSize, dataFormat);
    }

    Y_UNIT_TEST(MinBufferSizeWithCompressionAndEncryption, EDataFormat) {
        ui64 minBufferSize = 5000;
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

        TestMinBufferSize(minBufferSize, dataFormat);
    }
}

} // namespace NKikimr::NDataShard

#endif // KIKIMR_DISABLE_S3_OPS
