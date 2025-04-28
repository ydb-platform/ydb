#include "export_s3_buffer.h"
#include <ydb/core/tx/datashard/export_scan.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/array_ref.h>

#ifndef KIKIMR_DISABLE_S3_OPS

namespace NKikimr::NDataShard {

class TExportS3BufferFixture : public NUnitTest::TBaseFixture {
public:
    void SetUp(NUnitTest::TTestContext&) override {
        Columns[0] = TUserTable::TUserColumn(NScheme::TTypeInfo(NScheme::NTypeIds::Uint32), "", "key", true);
        Columns[1] = TUserTable::TUserColumn(NScheme::TTypeInfo(NScheme::NTypeIds::String), "", "value", false);
    }

    TS3ExportBufferSettings& Settings() {
        return S3ExportBufferSettings;
    }

    IExport::TTableColumns& TableColumns() {
        return Columns;
    }

    NExportScan::IBuffer& Buffer() {
        if (!S3ExportBuffer) {
            TS3ExportBufferSettings settings = S3ExportBufferSettings;
            settings.WithColumns(Columns);
            S3ExportBuffer.Reset(CreateS3ExportBuffer(std::move(settings)));

            TVector<ui32> tags;
            tags.reserve(Columns.size());
            for (auto&& [tag, _] : Columns) {
                tags.push_back(tag);
            }
            S3ExportBuffer->ColumnsOrder(tags);
        }
        return *S3ExportBuffer;
    }

    bool CollectKeyValue(ui32 k, TStringBuf v) {
        NTable::IScan::TRow row;
        row.Init(2);
        row.Set(0, NKikimr::NTable::ECellOp::Set, NKikimr::TCell::Make(k));
        row.Set(1, NKikimr::NTable::ECellOp::Set, NKikimr::TCell(v.data(), v.size()));
        return Buffer().Collect(row);
    }

    // Tests impl
    void TestMinBufferSize(ui64 minBufferSize) {
        for (ui32 i = 0; i < 100; ++i) {
            UNIT_ASSERT(CollectKeyValue(i, "1111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111"));
            NExportScan::IBuffer::TStats stats;
            if (Buffer().IsFilled()) {
                THolder<NActors::IEventBase> event(Buffer().PrepareEvent(false, stats));
                UNIT_ASSERT(event);
                auto* evBuffer = dynamic_cast<NKikimr::NDataShard::TEvExportScan::TEvBuffer<TBuffer>*>(event.get());
                UNIT_ASSERT(evBuffer);
                UNIT_ASSERT_GE_C(evBuffer->Buffer.Size(), minBufferSize, "Got buffer size " << evBuffer->Buffer.Size() << ". Iteration: " << i);
            }
        }
    }

public:
    IExport::TTableColumns Columns;
    TS3ExportBufferSettings S3ExportBufferSettings;
    THolder<NExportScan::IBuffer> S3ExportBuffer;
};

Y_UNIT_TEST_SUITE_F(ExportS3BufferTest, TExportS3BufferFixture) {
    Y_UNIT_TEST(MinBufferSize) {
        ui64 minBufferSize = 5000;
        Settings()
            .WithMaxRows(2)
            .WithMinBytes(minBufferSize)
            .WithMaxBytes(1'000'000);

        TestMinBufferSize(minBufferSize);
    }

    Y_UNIT_TEST(MinBufferSizeWithCompression) {
        ui64 minBufferSize = 5000;
        Settings()
            .WithCompression(TS3ExportBufferSettings::ZstdCompression(20))
            .WithMaxRows(2)
            .WithMinBytes(minBufferSize)
            .WithMaxBytes(1'000'000);

        TestMinBufferSize(minBufferSize);
    }

    Y_UNIT_TEST(MinBufferSizeWithCompressionAndEncryption) {
        ui64 minBufferSize = 5000;
        Settings()
            .WithCompression(TS3ExportBufferSettings::ZstdCompression(20))
            .WithEncryption(TS3ExportBufferSettings::TEncryptionSettings()
                .WithAlgorithm("AES-256-GCM")
                .WithIV(NBackup::TEncryptionIV::Generate())
                .WithKey(NBackup::TEncryptionKey("256 bit test symmetric key bytes")))
            .WithMaxRows(2)
            .WithMinBytes(minBufferSize)
            .WithMaxBytes(1'000'000);

        TestMinBufferSize(minBufferSize);
    }
}

} // namespace NKikimr::NDataShard

#endif // KIKIMR_DISABLE_S3_OPS
