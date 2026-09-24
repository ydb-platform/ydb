#include "export_s3_buffer.h"
#include <ydb/core/tx/datashard/export_scan.h>

#include <library/cpp/streams/zstd/zstd.h>
#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/array_ref.h>
#include <util/stream/mem.h>

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

    // Highly compressible data with zstd: the compressed output stays below MinBytes for a long time,
    // so the buffer keeps collecting rows. The raw rows must not be accumulated in memory meanwhile,
    // the memory usage has to stay bounded by MaxBytes regardless of the compression ratio.
    Y_UNIT_TEST(MemoryIsBoundedWithZstdAndMinBytes) {
        const ui64 minBytes = 64'000;
        const ui64 maxBytes = 128'000;
        Settings()
            .WithCompression(TS3ExportBufferSettings::ZstdCompression(1))
            .WithMaxRows(Max<ui64>())
            .WithMinBytes(minBytes)
            .WithMaxBytes(maxBytes);

        const auto dataFormat = EDataFormat::YdbDump;
        const TString value(1024, 'a');

        TString expected;
        TString compressed;
        ui64 flushes = 0;
        ui64 rawBytesPerFlush = 0;
        ui64 maxMemoryBytes = 0;
        for (ui32 i = 0; i < 100'000 && flushes < 2; ++i) {
            UNIT_ASSERT(CollectKeyValue(dataFormat, i, value));
            expected += ToString(i);
            for (ui32 tag = 1; tag < Columns.size(); ++tag) {
                expected += TStringBuilder() << ",\"" << value << "_" << tag << "_" << i << "\"";
            }
            expected += "\n";

            auto* buffer = Buffer(dataFormat);
            maxMemoryBytes = Max(maxMemoryBytes, buffer->GetMemoryBytes());
            if (!buffer->IsFilled()) {
                UNIT_ASSERT_LT_C(buffer->GetMemoryBytes(), maxBytes,
                    "Buffer is not filled but holds " << buffer->GetMemoryBytes() << " bytes in memory"
                    << ", maxBytes=" << maxBytes << ", iteration=" << i);
                continue;
            }

            NExportScan::IBuffer::TStats stats;
            THolder<NActors::IEventBase> event(buffer->PrepareEvent(false, stats));
            UNIT_ASSERT(event);
            auto* evBuffer = dynamic_cast<TEvExportScan::TEvBuffer<TBuffer>*>(event.Get());
            UNIT_ASSERT(evBuffer);
            UNIT_ASSERT_GE(evBuffer->Buffer.Size(), minBytes);
            compressed.append(evBuffer->Buffer.Data(), evBuffer->Buffer.Size());
            rawBytesPerFlush = Max(rawBytesPerFlush, stats.BytesRead);
            ++flushes;
        }
        UNIT_ASSERT_VALUES_EQUAL(flushes, 2);
        // The scenario is meaningful only if the compression ratio is high enough for the raw data
        // of a single part to exceed MaxBytes
        UNIT_ASSERT_GT_C(rawBytesPerFlush, maxBytes * 4, "rawBytesPerFlush=" << rawBytesPerFlush);
        Cerr << "Raw bytes per flush: " << rawBytesPerFlush << ", max memory: " << maxMemoryBytes << Endl;

        TMemoryInput compressedInput(compressed);
        TZstdDecompress decompress(&compressedInput);
        UNIT_ASSERT_VALUES_EQUAL(decompress.ReadAll(), expected);
    }
}

} // namespace NKikimr::NDataShard

#endif // KIKIMR_DISABLE_S3_OPS
