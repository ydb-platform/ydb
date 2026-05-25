#ifndef KIKIMR_DISABLE_S3_OPS

#include <ydb/core/tx/datashard/import_s3_engine.h>
#include <ydb/core/tx/datashard/import_parquet_s3_file.h>

#include <library/cpp/testing/unittest/registar.h>

#include <arrow/api.h>
#include <arrow/io/memory.h>
#include <parquet/arrow/writer.h>
#include <parquet/file_reader.h>
#include <contrib/libs/zstd/include/zstd.h>

#include <util/generic/size_literals.h>
#include <util/generic/vector.h>
#include <util/memory/pool.h>
#include <util/string/builder.h>

#include <array>
#include <memory>
#include <utility>

namespace NKikimr::NDataShard {
namespace {

using namespace NBackupRestoreTraits;

NKikimrSchemeOp::TTableDescription MakeUtf8TableScheme() {
    NKikimrSchemeOp::TTableDescription scheme;
    scheme.SetName("Table");
    scheme.SetPath("/Root/Table");

    auto* key = scheme.AddColumns();
    key->SetId(1);
    key->SetName("key");
    key->SetTypeId(NScheme::NTypeIds::Utf8);

    auto* value = scheme.AddColumns();
    value->SetId(2);
    value->SetName("value");
    value->SetTypeId(NScheme::NTypeIds::Utf8);

    scheme.AddKeyColumnIds(1);
    scheme.AddKeyColumnNames("key");
    return scheme;
}

void AssertSuccess(std::expected<void, TString> result) {
    UNIT_ASSERT_C(result, result.error());
}

template <typename T>
T ExtractValue(std::expected<T, TString> result) {
    UNIT_ASSERT_C(result, result.error());
    return std::move(result.value());
}

class TEngineFixture {
public:
    TEngineFixture()
        : Scheme(MakeUtf8TableScheme())
        , UserTable(new TUserTable(1, Scheme, 0))
        , TableInfo(1, UserTable)
    {
    }

    IImportS3Engine::TPtr MakeEngine(
        EDataFormat dataFormat,
        TStringBuf source,
        ui32 readBatchSize,
        bool validateChecksum = false,
        ECompressionCodec compressionCodec = ECompressionCodec::None,
        ui64 bufferSizeLimit = 1_MB) const
    {
        TImportS3EngineSettings settings;
        settings.DataFormat = dataFormat;
        settings.CompressionCodec = compressionCodec;
        settings.ContentLength = source.size();
        settings.ReadBatchSize = readBatchSize;
        settings.BufferSizeLimit = bufferSizeLimit;
        settings.ValidateChecksum = validateChecksum;

        return ExtractValue(CreateImportS3Engine(settings, TableInfo, Scheme));
    }

    const NKikimrSchemeOp::TTableDescription Scheme;
    const TUserTable::TPtr UserTable;
    const TTableInfo TableInfo;
};

struct TDecodedRow {
    TString Key;
    TString Value;
};

IImportS3Engine::TAddRowFn CaptureRows(TVector<TDecodedRow>& rows) {
    return [&rows](const TVector<TCell>& keys, const TVector<TCell>& values) {
        UNIT_ASSERT_VALUES_EQUAL(keys.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(values.size(), 1);
        UNIT_ASSERT(!keys.front().IsNull());
        UNIT_ASSERT(!values.front().IsNull());

        rows.push_back({
            TString(keys.front().AsBuf()),
            TString(values.front().AsBuf()),
        });
    };
}

TString Slice(const TString& source, const TImportRange& range) {
    UNIT_ASSERT_C(range.Offset <= source.size(), "range starts past EOF");
    UNIT_ASSERT_C(range.Length <= source.size() - range.Offset, "range ends past EOF");
    return source.substr(range.Offset, range.Length);
}

void AssertReadExactlyOnce(const TVector<TImportRange>& ranges, ui64 sourceSize) {
    TVector<ui8> coverage(sourceSize, 0);
    ui64 totalBytes = 0;

    for (const auto& range : ranges) {
        UNIT_ASSERT_C(range.Offset <= sourceSize, "range starts past EOF");
        UNIT_ASSERT_C(range.Length <= sourceSize - range.Offset, "range ends past EOF");
        totalBytes += range.Length;

        for (ui64 offset = range.Offset; offset < range.End(); ++offset) {
            UNIT_ASSERT_C(!coverage[offset],
                "source byte " << offset << " was requested more than once");
            coverage[offset] = 1;
        }
    }

    UNIT_ASSERT_VALUES_EQUAL(totalBytes, sourceSize);
    for (ui64 offset = 0; offset < sourceSize; ++offset) {
        UNIT_ASSERT_C(coverage[offset], "source byte " << offset << " was not requested");
    }
}

TString BuildSmallParquet(i64 rowGroupSize = 4, size_t valueSize = 24_KB) {
    arrow::StringBuilder keyBuilder;
    arrow::StringBuilder valueBuilder;

    static const std::array<TStringBuf, 4> keys = {"k1", "k2", "k3", "k4"};
    for (size_t i = 0; i < keys.size(); ++i) {
        const TString value(valueSize, static_cast<char>('a' + i));
        UNIT_ASSERT_C(keyBuilder.Append(keys[i].data(), keys[i].size()).ok(),
            "failed to append key " << i);
        UNIT_ASSERT_C(valueBuilder.Append(value.data(), value.size()).ok(),
            "failed to append value " << i);
    }

    std::shared_ptr<arrow::Array> keyArray;
    std::shared_ptr<arrow::Array> valueArray;
    UNIT_ASSERT_C(keyBuilder.Finish(&keyArray).ok(), "failed to finish key array");
    UNIT_ASSERT_C(valueBuilder.Finish(&valueArray).ok(), "failed to finish value array");

    auto schema = std::make_shared<arrow::Schema>(arrow::FieldVector{
        arrow::field("key", arrow::utf8()),
        arrow::field("value", arrow::utf8()),
    });
    auto table = arrow::Table::Make(schema, {keyArray, valueArray});
    auto sink = arrow::io::BufferOutputStream::Create(0).ValueOrDie();

    parquet::WriterProperties::Builder propertiesBuilder;
    propertiesBuilder.compression(parquet::Compression::UNCOMPRESSED);
    propertiesBuilder.disable_dictionary();

    const auto writeStatus = parquet::arrow::WriteTable(
        *table,
        arrow::default_memory_pool(),
        sink,
        /*chunk_size=*/rowGroupSize,
        propertiesBuilder.build());
    UNIT_ASSERT_C(writeStatus.ok(), writeStatus.ToString());

    auto buffer = sink->Finish().ValueOrDie();
    return TString(reinterpret_cast<const char*>(buffer->data()), buffer->size());
}

TString BuildParquetWithManyRowGroups(ui32 rowGroupCount) {
    arrow::StringBuilder keyBuilder;
    arrow::StringBuilder valueBuilder;
    for (ui32 i = 0; i < rowGroupCount; ++i) {
        const TString key = TStringBuilder() << "k" << i;
        UNIT_ASSERT_C(keyBuilder.Append(key.data(), key.size()).ok(),
            "failed to append key " << i);
        UNIT_ASSERT_C(valueBuilder.Append("v", 1).ok(),
            "failed to append value " << i);
    }

    std::shared_ptr<arrow::Array> keyArray;
    std::shared_ptr<arrow::Array> valueArray;
    UNIT_ASSERT_C(keyBuilder.Finish(&keyArray).ok(), "failed to finish key array");
    UNIT_ASSERT_C(valueBuilder.Finish(&valueArray).ok(), "failed to finish value array");

    auto schema = std::make_shared<arrow::Schema>(arrow::FieldVector{
        arrow::field("key", arrow::utf8()),
        arrow::field("value", arrow::utf8()),
    });
    auto table = arrow::Table::Make(schema, {keyArray, valueArray});
    auto sink = arrow::io::BufferOutputStream::Create(0).ValueOrDie();

    parquet::WriterProperties::Builder propertiesBuilder;
    propertiesBuilder.compression(parquet::Compression::UNCOMPRESSED);
    propertiesBuilder.disable_dictionary();

    const auto writeStatus = parquet::arrow::WriteTable(
        *table,
        arrow::default_memory_pool(),
        sink,
        /*chunk_size=*/1,
        propertiesBuilder.build());
    UNIT_ASSERT_C(writeStatus.ok(), writeStatus.ToString());

    auto buffer = sink->Finish().ValueOrDie();
    return TString(reinterpret_cast<const char*>(buffer->data()), buffer->size());
}

TString BuildEmptyParquet(bool includeValueColumn) {
    arrow::FieldVector fields{arrow::field("key", arrow::utf8())};
    if (includeValueColumn) {
        fields.push_back(arrow::field("value", arrow::utf8()));
    }

    auto schema = std::make_shared<arrow::Schema>(std::move(fields));
    auto sink = arrow::io::BufferOutputStream::Create(0).ValueOrDie();
    std::unique_ptr<parquet::arrow::FileWriter> writer;
    const auto openStatus = parquet::arrow::FileWriter::Open(
        *schema,
        arrow::default_memory_pool(),
        sink,
        parquet::WriterProperties::Builder().build(),
        &writer);
    UNIT_ASSERT_C(openStatus.ok(), openStatus.ToString());
    UNIT_ASSERT_C(writer->Close().ok(), "failed to close empty Parquet writer");

    auto buffer = sink->Finish().ValueOrDie();
    auto reader = std::make_shared<arrow::io::BufferReader>(buffer);
    UNIT_ASSERT_VALUES_EQUAL(parquet::ReadMetaData(reader)->num_row_groups(), 0);
    return TString(reinterpret_cast<const char*>(buffer->data()), buffer->size());
}

TString ZstdCompress(TStringBuf source) {
    TString compressed;
    compressed.resize(ZSTD_compressBound(source.size()));
    const size_t size = ZSTD_compress(
        compressed.Detach(),
        compressed.size(),
        source.data(),
        source.size(),
        ZSTD_CLEVEL_DEFAULT);
    UNIT_ASSERT_C(!ZSTD_isError(size), ZSTD_getErrorName(size));
    compressed.resize(size);
    return compressed;
}

TString MakePseudoRandomAscii(size_t size) {
    static constexpr TStringBuf Alphabet =
        "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
    TString value(size, '\0');
    ui64 state = 0x9e3779b97f4a7c15ULL;
    for (char& ch : value) {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        ch = Alphabet[state % Alphabet.size()];
    }
    return value;
}

Y_UNIT_TEST_SUITE(TImportS3EngineTest) {
    Y_UNIT_TEST(CsvSplitsRangesAndWaitsForCommit) {
        const TString source = "\"k1\",\"v1\"\n\"k2\",\"v2\"\n";
        TEngineFixture fixture;
        auto engine = fixture.MakeEngine(
            EDataFormat::YdbDump,
            source,
            /*readBatchSize=*/5,
            /*validateChecksum=*/true);

        UNIT_ASSERT(engine->SupportsDirectPartImport());

        TMemoryPool pool(256);
        TVector<TDecodedRow> rows;
        TString checksumInput;
        const auto addRow = CaptureRows(rows);
        const auto addChecksum = [&checksumInput](TStringBuf data) {
            checksumInput.append(data.data(), data.size());
        };

        auto data = ExtractValue(engine->GetData(pool, addRow, addChecksum));
        UNIT_ASSERT(data.Status == IImportS3Engine::EDataStatus::NeedInput);

        auto range = ExtractValue(engine->NextRange());
        UNIT_ASSERT(range.Status == IImportS3Engine::ENextRangeStatus::Ready);
        UNIT_ASSERT_VALUES_EQUAL(range.Range.Offset, 0);
        UNIT_ASSERT_VALUES_EQUAL(range.Range.Length, 5);

        const auto blockedRange = ExtractValue(engine->NextRange());
        UNIT_ASSERT(blockedRange.Status == IImportS3Engine::ENextRangeStatus::Blocked);

        AssertSuccess(engine->PutRange(range.Range, Slice(source, range.Range)));

        data = ExtractValue(engine->GetData(pool, addRow, addChecksum));
        UNIT_ASSERT(data.Status == IImportS3Engine::EDataStatus::NeedInput);

        range = ExtractValue(engine->NextRange());
        UNIT_ASSERT(range.Status == IImportS3Engine::ENextRangeStatus::Ready);
        UNIT_ASSERT_VALUES_EQUAL(range.Range.Offset, 5);
        UNIT_ASSERT_VALUES_EQUAL(range.Range.Length, 5);
        AssertSuccess(engine->PutRange(range.Range, Slice(source, range.Range)));

        data = ExtractValue(engine->GetData(pool, addRow, addChecksum));
        UNIT_ASSERT(data.Status == IImportS3Engine::EDataStatus::Ready);
        UNIT_ASSERT_VALUES_EQUAL(data.Batch.ProcessedBytesAfter, 10);
        UNIT_ASSERT_VALUES_EQUAL(data.Batch.Rows, 1);
        UNIT_ASSERT_VALUES_EQUAL(data.Batch.DataBytes, 4);
        UNIT_ASSERT_VALUES_EQUAL(rows.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(rows[0].Key, "k1");
        UNIT_ASSERT_VALUES_EQUAL(rows[0].Value, "v1");

        const ui64 firstBatchId = data.Batch.Id;
        data = ExtractValue(engine->GetData(pool, addRow, addChecksum));
        UNIT_ASSERT(data.Status == IImportS3Engine::EDataStatus::WaitingForCommit);
        UNIT_ASSERT_VALUES_EQUAL(data.Batch.Id, firstBatchId);
        UNIT_ASSERT_VALUES_EQUAL(rows.size(), 1);
        UNIT_ASSERT(ExtractValue(engine->NextRange()).Status == IImportS3Engine::ENextRangeStatus::Blocked);

        const auto wrongCommit = engine->Commit(firstBatchId + 1);
        UNIT_ASSERT(!wrongCommit);
        UNIT_ASSERT_C(wrongCommit.error().Contains("unexpected import batch"), wrongCommit.error());

        AssertSuccess(engine->Commit(firstBatchId));

        data = ExtractValue(engine->GetData(pool, addRow, addChecksum));
        UNIT_ASSERT(data.Status == IImportS3Engine::EDataStatus::NeedInput);

        range = ExtractValue(engine->NextRange());
        UNIT_ASSERT(range.Status == IImportS3Engine::ENextRangeStatus::Ready);
        UNIT_ASSERT_VALUES_EQUAL(range.Range.Offset, 10);
        UNIT_ASSERT_VALUES_EQUAL(range.Range.Length, 5);
        AssertSuccess(engine->PutRange(range.Range, Slice(source, range.Range)));

        data = ExtractValue(engine->GetData(pool, addRow, addChecksum));
        UNIT_ASSERT(data.Status == IImportS3Engine::EDataStatus::NeedInput);

        range = ExtractValue(engine->NextRange());
        UNIT_ASSERT(range.Status == IImportS3Engine::ENextRangeStatus::Ready);
        UNIT_ASSERT_VALUES_EQUAL(range.Range.Offset, 15);
        UNIT_ASSERT_VALUES_EQUAL(range.Range.Length, source.size() - 15);
        AssertSuccess(engine->PutRange(range.Range, Slice(source, range.Range)));

        data = ExtractValue(engine->GetData(pool, addRow, addChecksum));
        UNIT_ASSERT(data.Status == IImportS3Engine::EDataStatus::Ready);
        UNIT_ASSERT_VALUES_EQUAL(data.Batch.ProcessedBytesAfter, source.size());
        UNIT_ASSERT_VALUES_EQUAL(data.Batch.Rows, 1);
        UNIT_ASSERT_VALUES_EQUAL(data.Batch.DataBytes, 4);
        UNIT_ASSERT_VALUES_EQUAL(rows.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(rows[1].Key, "k2");
        UNIT_ASSERT_VALUES_EQUAL(rows[1].Value, "v2");

        AssertSuccess(engine->Commit(data.Batch.Id));
        data = ExtractValue(engine->GetData(pool, addRow, addChecksum));
        UNIT_ASSERT(data.Status == IImportS3Engine::EDataStatus::Finished);
        UNIT_ASSERT_VALUES_EQUAL(checksumInput, source);
    }

    Y_UNIT_TEST(CsvRejectsWrongAndMalformedRanges) {
        const TString source = "\"k1\",\"v1\"\n";
        TEngineFixture fixture;
        auto engine = fixture.MakeEngine(
            EDataFormat::YdbDump,
            source,
            /*readBatchSize=*/5);

        const TImportRange unreserved{0, 5};
        auto result = engine->PutRange(unreserved, Slice(source, unreserved));
        UNIT_ASSERT(!result);
        UNIT_ASSERT_C(result.error().Contains("was not reserved"), result.error());

        const auto range = ExtractValue(engine->NextRange());
        UNIT_ASSERT(range.Status == IImportS3Engine::ENextRangeStatus::Ready);

        const TImportRange wrongRange{range.Range.Offset + 1, range.Range.Length};
        result = engine->PutRange(wrongRange, TString(wrongRange.Length, 'x'));
        UNIT_ASSERT(!result);
        UNIT_ASSERT_C(result.error().Contains("unexpected range"), result.error());

        result = engine->PutRange(
            range.Range,
            TString(range.Range.Length - 1, 'x'));
        UNIT_ASSERT(!result);
        UNIT_ASSERT_C(result.error().Contains("returned 4 bytes, expected 5"), result.error());

        result = engine->FailRange(wrongRange);
        UNIT_ASSERT(!result);
        UNIT_ASSERT_C(result.error().Contains("unexpected range"), result.error());

        AssertSuccess(engine->FailRange(range.Range));
        const auto retried = ExtractValue(engine->NextRange());
        UNIT_ASSERT(retried.Status == IImportS3Engine::ENextRangeStatus::Ready);
        UNIT_ASSERT_VALUES_EQUAL(retried.Range.Offset, range.Range.Offset);
        UNIT_ASSERT_VALUES_EQUAL(retried.Range.Length, range.Range.Length);

        AssertSuccess(engine->PutRange(retried.Range, Slice(source, retried.Range)));

        result = engine->PutRange(retried.Range, Slice(source, retried.Range));
        UNIT_ASSERT(!result);
        UNIT_ASSERT_C(result.error().Contains("was not reserved"), result.error());
    }

    Y_UNIT_TEST(ZstdDefersCountersUntilRestartableFrameBoundary) {
        const TString largeValue = MakePseudoRandomAscii(256_KB);
        const TString csv = TStringBuilder()
            << "\"k0\",\"v0\"\n"
            << "\"k1\",\"" << largeValue << "\"\n";
        const TString source = ZstdCompress(csv);
        UNIT_ASSERT_GT(source.size(), 128_KB);

        TEngineFixture fixture;
        auto engine = fixture.MakeEngine(
            EDataFormat::YdbDump,
            source,
            /*readBatchSize=*/4_KB,
            /*validateChecksum=*/true,
            ECompressionCodec::Zstd);

        TMemoryPool pool(256);
        TVector<TDecodedRow> rows;
        TString checksumInput;
        const auto addRow = CaptureRows(rows);
        const auto addChecksum = [&checksumInput](TStringBuf data) {
            checksumInput.append(data.data(), data.size());
        };

        bool sawDeferredBatch = false;
        bool sawCheckpointBatch = false;
        for (ui32 step = 0; step < 1024; ++step) {
            auto data = ExtractValue(engine->GetData(pool, addRow, addChecksum));
            if (data.Status == IImportS3Engine::EDataStatus::NeedInput) {
                const auto range = ExtractValue(engine->NextRange());
                UNIT_ASSERT(range.Status == IImportS3Engine::ENextRangeStatus::Ready);
                AssertSuccess(engine->PutRange(range.Range, Slice(source, range.Range)));
                continue;
            }

            if (data.Status == IImportS3Engine::EDataStatus::Ready) {
                if (!data.Batch.ProcessedBytesAfter) {
                    UNIT_ASSERT(!sawDeferredBatch);
                    sawDeferredBatch = true;
                    UNIT_ASSERT_VALUES_EQUAL(data.Batch.DataBytes, 0);
                    UNIT_ASSERT_VALUES_EQUAL(data.Batch.Rows, 0);
                    UNIT_ASSERT_VALUES_EQUAL(rows.size(), 1);
                } else {
                    UNIT_ASSERT(!sawCheckpointBatch);
                    sawCheckpointBatch = true;
                    UNIT_ASSERT_VALUES_EQUAL(data.Batch.ProcessedBytesAfter, source.size());
                    UNIT_ASSERT_VALUES_EQUAL(data.Batch.DataBytes, largeValue.size() + 6);
                    UNIT_ASSERT_VALUES_EQUAL(data.Batch.Rows, 2);
                    UNIT_ASSERT_VALUES_EQUAL(rows.size(), 2);
                }

                AssertSuccess(engine->Commit(data.Batch.Id));
                continue;
            }

            if (data.Status == IImportS3Engine::EDataStatus::Finished) {
                UNIT_ASSERT(sawDeferredBatch);
                UNIT_ASSERT(sawCheckpointBatch);
                UNIT_ASSERT_VALUES_EQUAL(checksumInput, csv);
                UNIT_ASSERT_VALUES_EQUAL(rows[0].Key, "k0");
                UNIT_ASSERT_VALUES_EQUAL(rows[1].Value, largeValue);
                return;
            }

            UNIT_FAIL("unexpected batch waiting for commit");
        }

        UNIT_FAIL("Zstd import engine did not finish within 1024 state transitions");
    }

    Y_UNIT_TEST(ParquetReadsARealFileThroughRanges) {
        const TString source = BuildSmallParquet();
        TEngineFixture fixture;
        auto engine = fixture.MakeEngine(
            EDataFormat::Parquet,
            source,
            /*readBatchSize=*/8_KB);

        UNIT_ASSERT(!engine->SupportsDirectPartImport());
        UNIT_ASSERT_GT(source.size(), 64_KB);
        UNIT_ASSERT_LT(source.size(), 1_MB);

        TMemoryPool pool(256);
        TVector<TDecodedRow> rows;
        const auto addRow = CaptureRows(rows);
        const auto unexpectedChecksum = [](TStringBuf) {
            UNIT_FAIL("checksum callback was called with validation disabled");
        };

        bool sawFirstRange = false;
        bool sawBatch = false;
        for (ui32 step = 0; step < 1024; ++step) {
            auto data = ExtractValue(engine->GetData(pool, addRow, unexpectedChecksum));
            switch (data.Status) {
            case IImportS3Engine::EDataStatus::NeedInput: {
                const auto range = ExtractValue(engine->NextRange());
                UNIT_ASSERT(range.Status == IImportS3Engine::ENextRangeStatus::Ready);
                UNIT_ASSERT_GT(range.Range.Length, 0);
                UNIT_ASSERT_LE(range.Range.End(), source.size());
                if (!sawFirstRange) {
                    sawFirstRange = true;
                    UNIT_ASSERT_GT(range.Range.Offset, 0);
                    UNIT_ASSERT_VALUES_EQUAL(range.Range.Offset, source.size() - 64_KB);
                }

                AssertSuccess(engine->PutRange(range.Range, Slice(source, range.Range)));
                break;
            }

            case IImportS3Engine::EDataStatus::Ready: {
                UNIT_ASSERT(!sawBatch);
                sawBatch = true;
                UNIT_ASSERT_VALUES_EQUAL(data.Batch.ProcessedBytesAfter, source.size());
                UNIT_ASSERT_VALUES_EQUAL(data.Batch.Rows, 4);
                UNIT_ASSERT_VALUES_EQUAL(rows.size(), 4);

                const auto waiting = ExtractValue(engine->GetData(pool, addRow, unexpectedChecksum));
                UNIT_ASSERT(waiting.Status == IImportS3Engine::EDataStatus::WaitingForCommit);
                UNIT_ASSERT_VALUES_EQUAL(waiting.Batch.Id, data.Batch.Id);

                AssertSuccess(engine->Commit(data.Batch.Id));
                break;
            }

            case IImportS3Engine::EDataStatus::Finished:
                UNIT_ASSERT(sawFirstRange);
                UNIT_ASSERT(sawBatch);
                UNIT_ASSERT_VALUES_EQUAL(rows.size(), 4);
                UNIT_ASSERT_VALUES_EQUAL(rows[0].Key, "k1");
                UNIT_ASSERT_VALUES_EQUAL(rows[0].Value, TString(24_KB, 'a'));
                UNIT_ASSERT_VALUES_EQUAL(rows[3].Key, "k4");
                UNIT_ASSERT_VALUES_EQUAL(rows[3].Value, TString(24_KB, 'd'));
                return;

            case IImportS3Engine::EDataStatus::WaitingForCommit:
                UNIT_FAIL("unexpected batch waiting for commit");
            }
        }

        UNIT_FAIL("Parquet import engine did not finish within 1024 state transitions");
    }

    Y_UNIT_TEST(ParquetChecksumReadsEachByteOnceInSourceOrder) {
        const TString source = BuildSmallParquet(/*rowGroupSize=*/1);
        UNIT_ASSERT_GT(source.size(), 64_KB);

        TEngineFixture fixture;
        auto engine = fixture.MakeEngine(
            EDataFormat::Parquet,
            source,
            /*readBatchSize=*/8_KB,
            /*validateChecksum=*/true);

        TMemoryPool pool(256);
        TVector<TDecodedRow> rows;
        TVector<TImportRange> requestedRanges;
        TString checksumInput;
        const auto addRow = CaptureRows(rows);
        const auto addChecksum = [&](TStringBuf data) {
            checksumInput.append(data.data(), data.size());
        };

        auto data = ExtractValue(engine->GetData(pool, addRow, addChecksum));
        UNIT_ASSERT(data.Status == IImportS3Engine::EDataStatus::NeedInput);

        const auto footerChunk = ExtractValue(engine->NextRange());
        UNIT_ASSERT(footerChunk.Status == IImportS3Engine::ENextRangeStatus::Ready);
        UNIT_ASSERT_VALUES_EQUAL(footerChunk.Range.Offset, source.size() - 64_KB);
        UNIT_ASSERT_VALUES_EQUAL(footerChunk.Range.Length, 8_KB);
        requestedRanges.push_back(footerChunk.Range);
        AssertSuccess(engine->PutRange(footerChunk.Range, Slice(source, footerChunk.Range)));

        UNIT_ASSERT_VALUES_EQUAL(engine->PendingBytes(), footerChunk.Range.Length);
        data = ExtractValue(engine->GetData(pool, addRow, addChecksum));
        UNIT_ASSERT(data.Status == IImportS3Engine::EDataStatus::NeedInput);
        UNIT_ASSERT(checksumInput.empty());

        bool finished = false;
        bool sawFinalBatch = false;
        bool sawBatchBeforeChecksumFinished = false;
        for (ui32 step = 0; step < 2048 && !finished; ++step) {
            data = ExtractValue(engine->GetData(pool, addRow, addChecksum));
            switch (data.Status) {
            case IImportS3Engine::EDataStatus::NeedInput: {
                const auto range = ExtractValue(engine->NextRange());
                UNIT_ASSERT(range.Status == IImportS3Engine::ENextRangeStatus::Ready);
                requestedRanges.push_back(range.Range);
                AssertSuccess(engine->PutRange(range.Range, Slice(source, range.Range)));
                break;
            }

            case IImportS3Engine::EDataStatus::Ready:
                if (data.Batch.ProcessedBytesAfter) {
                    UNIT_ASSERT(!sawFinalBatch);
                    sawFinalBatch = true;
                    UNIT_ASSERT_VALUES_EQUAL(data.Batch.ProcessedBytesAfter, source.size());
                } else {
                    UNIT_ASSERT_LT(checksumInput.size(), source.size());
                    sawBatchBeforeChecksumFinished = true;
                }
                AssertSuccess(engine->Commit(data.Batch.Id));
                break;

            case IImportS3Engine::EDataStatus::Finished:
                finished = true;
                break;

            case IImportS3Engine::EDataStatus::WaitingForCommit:
                UNIT_FAIL("unexpected batch waiting for commit");
            }
        }

        UNIT_ASSERT_C(finished, "Parquet import engine did not finish within 2048 state transitions");
        UNIT_ASSERT(sawFinalBatch);
        UNIT_ASSERT(sawBatchBeforeChecksumFinished);
        UNIT_ASSERT_VALUES_EQUAL(rows.size(), 4);
        UNIT_ASSERT_VALUES_EQUAL(checksumInput, source);
        AssertReadExactlyOnce(requestedRanges, source.size());
    }

    Y_UNIT_TEST(ParquetChecksumHashesCachedWholeFileWithoutRefetch) {
        const TString source = BuildEmptyParquet(/*includeValueColumn=*/true);
        UNIT_ASSERT_LT(source.size(), 64_KB);

        TEngineFixture fixture;
        auto engine = fixture.MakeEngine(
            EDataFormat::Parquet,
            source,
            /*readBatchSize=*/8_KB,
            /*validateChecksum=*/true);

        TMemoryPool pool(256);
        TString checksumInput;
        const auto unexpectedRow = [](const TVector<TCell>&, const TVector<TCell>&) {
            UNIT_FAIL("empty Parquet file emitted a row");
        };
        const auto addChecksum = [&](TStringBuf data) {
            checksumInput.append(data.data(), data.size());
        };

        auto data = ExtractValue(engine->GetData(pool, unexpectedRow, addChecksum));
        UNIT_ASSERT(data.Status == IImportS3Engine::EDataStatus::NeedInput);
        const auto range = ExtractValue(engine->NextRange());
        UNIT_ASSERT(range.Status == IImportS3Engine::ENextRangeStatus::Ready);
        UNIT_ASSERT_VALUES_EQUAL(range.Range.Offset, 0);
        UNIT_ASSERT_VALUES_EQUAL(range.Range.Length, source.size());
        AssertSuccess(engine->PutRange(range.Range, Slice(source, range.Range)));

        data = ExtractValue(engine->GetData(pool, unexpectedRow, addChecksum));
        UNIT_ASSERT(data.Status == IImportS3Engine::EDataStatus::Ready);
        UNIT_ASSERT_VALUES_EQUAL(data.Batch.ProcessedBytesAfter, source.size());
        UNIT_ASSERT_VALUES_EQUAL(data.Batch.Rows, 0);
        UNIT_ASSERT_VALUES_EQUAL(checksumInput, source);
        AssertReadExactlyOnce({range.Range}, source.size());

        AssertSuccess(engine->Commit(data.Batch.Id));
        data = ExtractValue(engine->GetData(pool, unexpectedRow, addChecksum));
        UNIT_ASSERT(data.Status == IImportS3Engine::EDataStatus::Finished);
    }

    Y_UNIT_TEST(ParquetChecksumRetainsMetadataLargerThanFooterProbe) {
        static constexpr ui32 RowGroupCount = 1024;
        const TString source = BuildParquetWithManyRowGroups(RowGroupCount);
        const auto footerTail = TParquetSparseFile::FooterTailRange(source.size());

        auto sparseFile = std::make_shared<TParquetSparseFile>(source.size());
        sparseFile->PutRange(footerTail.Offset, Slice(source, {
            .Offset = footerTail.Offset,
            .Length = footerTail.Length,
        }));
        const auto metadataRange = ExtractValue(sparseFile->TryParseFooterMetadataRange());
        UNIT_ASSERT(metadataRange);
        UNIT_ASSERT_LT(metadataRange->Offset, footerTail.Offset);
        UNIT_ASSERT_LT(source.size() - metadataRange->Offset, 4_MB - 32_KB);

        TEngineFixture fixture;
        auto engine = fixture.MakeEngine(
            EDataFormat::Parquet,
            source,
            /*readBatchSize=*/32_KB,
            /*validateChecksum=*/true,
            ECompressionCodec::None,
            /*bufferSizeLimit=*/4_MB);

        TMemoryPool pool(256);
        TVector<TDecodedRow> rows;
        TVector<TImportRange> requestedRanges;
        TString checksumInput;
        const auto addRow = CaptureRows(rows);
        const auto addChecksum = [&](TStringBuf data) {
            checksumInput.append(data.data(), data.size());
        };

        bool finished = false;
        for (ui32 step = 0; step < 8192 && !finished; ++step) {
            const auto data = ExtractValue(engine->GetData(pool, addRow, addChecksum));
            switch (data.Status) {
            case IImportS3Engine::EDataStatus::NeedInput: {
                const auto range = ExtractValue(engine->NextRange());
                UNIT_ASSERT(range.Status == IImportS3Engine::ENextRangeStatus::Ready);
                requestedRanges.push_back(range.Range);
                AssertSuccess(engine->PutRange(range.Range, Slice(source, range.Range)));
                break;
            }

            case IImportS3Engine::EDataStatus::Ready:
                AssertSuccess(engine->Commit(data.Batch.Id));
                break;

            case IImportS3Engine::EDataStatus::Finished:
                finished = true;
                break;

            case IImportS3Engine::EDataStatus::WaitingForCommit:
                UNIT_FAIL("unexpected batch waiting for commit");
            }
        }

        UNIT_ASSERT_C(finished, "Parquet import with large footer metadata did not finish");
        UNIT_ASSERT_VALUES_EQUAL(rows.size(), RowGroupCount);
        UNIT_ASSERT_VALUES_EQUAL(checksumInput, source);
        AssertReadExactlyOnce(requestedRanges, source.size());
    }

    Y_UNIT_TEST(ParquetValidatesSchemaWithoutRowGroups) {
        for (const bool includeValueColumn : {true, false}) {
            const TString source = BuildEmptyParquet(includeValueColumn);
            TEngineFixture fixture;
            auto engine = fixture.MakeEngine(
                EDataFormat::Parquet,
                source,
                /*readBatchSize=*/8_KB);

            TMemoryPool pool(256);
            const auto unexpectedRow = [](const TVector<TCell>&, const TVector<TCell>&) {
                UNIT_FAIL("empty Parquet file emitted a row");
            };
            const auto unexpectedChecksum = [](TStringBuf) {
                UNIT_FAIL("checksum callback was called with validation disabled");
            };

            TString importError;
            bool finalBatchCommitted = false;
            bool finished = false;
            for (ui32 step = 0; step < 256 && importError.empty() && !finished; ++step) {
                auto dataResult = engine->GetData(pool, unexpectedRow, unexpectedChecksum);
                if (!dataResult) {
                    importError = std::move(dataResult.error());
                    break;
                }
                auto data = std::move(*dataResult);
                switch (data.Status) {
                case IImportS3Engine::EDataStatus::NeedInput: {
                    auto rangeResult = engine->NextRange();
                    if (!rangeResult) {
                        importError = std::move(rangeResult.error());
                        break;
                    }
                    const auto range = std::move(*rangeResult);
                    UNIT_ASSERT(range.Status == IImportS3Engine::ENextRangeStatus::Ready);
                    if (auto result = engine->PutRange(range.Range, Slice(source, range.Range)); !result) {
                        importError = std::move(result.error());
                    }
                    break;
                }

                case IImportS3Engine::EDataStatus::Ready: {
                    UNIT_ASSERT(includeValueColumn);
                    UNIT_ASSERT_VALUES_EQUAL(data.Batch.ProcessedBytesAfter, source.size());
                    UNIT_ASSERT_VALUES_EQUAL(data.Batch.Rows, 0);
                    AssertSuccess(engine->Commit(data.Batch.Id));
                    finalBatchCommitted = true;
                    break;
                }

                case IImportS3Engine::EDataStatus::Finished:
                    finished = true;
                    break;

                case IImportS3Engine::EDataStatus::WaitingForCommit:
                    UNIT_FAIL("unexpected batch waiting for commit");
                }
            }

            if (includeValueColumn) {
                UNIT_ASSERT_C(importError.empty(), importError);
                UNIT_ASSERT(finalBatchCommitted);
                UNIT_ASSERT(finished);
            } else {
                UNIT_ASSERT_C(importError.Contains("column 'value' not found"), importError);
                UNIT_ASSERT(!finalBatchCommitted);
                UNIT_ASSERT(!finished);
            }
        }
    }

    Y_UNIT_TEST(ParquetKeepsOnlyOneRowGroupInFlight) {
        static constexpr ui64 ValueSize = 96_KB;
        static constexpr ui64 BufferSizeLimit = 192_KB;
        const TString source = BuildSmallParquet(/*rowGroupSize=*/1, ValueSize);
        UNIT_ASSERT_GT(source.size(), BufferSizeLimit);

        for (const bool validateChecksum : {false, true}) {
            TEngineFixture fixture;
            auto engine = fixture.MakeEngine(
                EDataFormat::Parquet,
                source,
                /*readBatchSize=*/8_KB,
                validateChecksum,
                ECompressionCodec::None,
                BufferSizeLimit);

            TMemoryPool pool(256);
            TVector<TDecodedRow> rows;
            TString checksumInput;
            const auto addRow = CaptureRows(rows);
            const auto addChecksum = [&](TStringBuf data) {
                UNIT_ASSERT_C(validateChecksum,
                    "checksum callback was called with validation disabled");
                checksumInput.append(data.data(), data.size());
            };

            ui64 maxPendingBytes = 0;
            ui32 batches = 0;
            ui32 evictedRowGroups = 0;
            const ui64 retainedFooterBytes = Min<ui64>(source.size(), 64_KB);
            bool footerProbeHandled = false;
            bool finished = false;
            for (ui32 step = 0; step < 2048; ++step) {
                maxPendingBytes = Max(maxPendingBytes, engine->PendingBytes());
                auto data = ExtractValue(engine->GetData(pool, addRow, addChecksum));
                maxPendingBytes = Max(maxPendingBytes, engine->PendingBytes());

                switch (data.Status) {
                case IImportS3Engine::EDataStatus::NeedInput: {
                    const auto range = ExtractValue(engine->NextRange());
                    UNIT_ASSERT(range.Status == IImportS3Engine::ENextRangeStatus::Ready);
                    UNIT_ASSERT_GT(range.Range.Length, 0);
                    UNIT_ASSERT_LE(range.Range.End(), source.size());
                    const bool footerSlice = !footerProbeHandled
                        && range.Range.Offset >= source.size() - 64_KB
                        && range.Range.End() == source.size();

                    AssertSuccess(engine->PutRange(range.Range, Slice(source, range.Range)));
                    if (footerSlice) {
                        footerProbeHandled = true;
                        UNIT_ASSERT_VALUES_EQUAL(
                            engine->PendingBytes(),
                            validateChecksum ? retainedFooterBytes : 0);
                    }
                    maxPendingBytes = Max(maxPendingBytes, engine->PendingBytes());
                    break;
                }

                case IImportS3Engine::EDataStatus::Ready: {
                    ++batches;
                    const ui64 pendingBeforeCommit = engine->PendingBytes();
                    UNIT_ASSERT(ExtractValue(engine->NextRange()).Status == IImportS3Engine::ENextRangeStatus::Blocked);

                    AssertSuccess(engine->Commit(data.Batch.Id));
                    if (!data.Batch.ProcessedBytesAfter) {
                        ++evictedRowGroups;
                        UNIT_ASSERT_GT(pendingBeforeCommit, 0);
                        UNIT_ASSERT_VALUES_EQUAL(
                            engine->PendingBytes(),
                            validateChecksum ? retainedFooterBytes : 0);
                    }
                    break;
                }

                case IImportS3Engine::EDataStatus::Finished:
                    finished = true;
                    UNIT_ASSERT_VALUES_EQUAL(rows.size(), 4);
                    UNIT_ASSERT_VALUES_EQUAL(batches, 4);
                    UNIT_ASSERT_VALUES_EQUAL(evictedRowGroups, 3);
                    UNIT_ASSERT(footerProbeHandled);
                    UNIT_ASSERT_LT(maxPendingBytes, BufferSizeLimit);
                    UNIT_ASSERT_VALUES_EQUAL(checksumInput, validateChecksum ? source : TString());
                    UNIT_ASSERT_VALUES_EQUAL(rows[0].Key, "k1");
                    UNIT_ASSERT_VALUES_EQUAL(rows[0].Value, TString(ValueSize, 'a'));
                    UNIT_ASSERT_VALUES_EQUAL(rows[3].Key, "k4");
                    UNIT_ASSERT_VALUES_EQUAL(rows[3].Value, TString(ValueSize, 'd'));
                    break;

                case IImportS3Engine::EDataStatus::WaitingForCommit:
                    UNIT_FAIL("unexpected batch waiting for commit");
                }

                if (finished) {
                    break;
                }
            }

            UNIT_ASSERT_C(finished, "Parquet import engine did not finish within 2048 state transitions");
            UNIT_ASSERT_VALUES_EQUAL(rows.size(), 4);
        }
    }

    Y_UNIT_TEST(ParquetChecksumFallsBackWhenRetainedSuffixWouldExceedBuffer) {
        static constexpr ui64 ValueSize = 96_KB;
        static constexpr ui64 BufferSizeLimit = 128_KB;
        const TString source = BuildSmallParquet(/*rowGroupSize=*/1, ValueSize);
        UNIT_ASSERT_GT(source.size(), BufferSizeLimit);

        TEngineFixture fixture;
        auto engine = fixture.MakeEngine(
            EDataFormat::Parquet,
            source,
            /*readBatchSize=*/8_KB,
            /*validateChecksum=*/true,
            ECompressionCodec::None,
            BufferSizeLimit);

        TMemoryPool pool(256);
        TVector<TDecodedRow> rows;
        TString checksumInput;
        const auto addRow = CaptureRows(rows);
        const auto addChecksum = [&](TStringBuf data) {
            checksumInput.append(data.data(), data.size());
        };

        auto data = ExtractValue(engine->GetData(pool, addRow, addChecksum));
        UNIT_ASSERT(data.Status == IImportS3Engine::EDataStatus::NeedInput);

        const TImportRange footerTail{
            .Offset = source.size() - 64_KB,
            .Length = 64_KB,
        };
        ui64 footerBytes = 0;
        ui64 requestedBytes = 0;
        while (footerBytes < footerTail.Length) {
            const auto range = ExtractValue(engine->NextRange());
            UNIT_ASSERT(range.Status == IImportS3Engine::ENextRangeStatus::Ready);
            UNIT_ASSERT_VALUES_EQUAL(range.Range.Offset, footerTail.Offset + footerBytes);
            footerBytes += range.Range.Length;
            requestedBytes += range.Range.Length;
            AssertSuccess(engine->PutRange(range.Range, Slice(source, range.Range)));

            data = ExtractValue(engine->GetData(pool, addRow, addChecksum));
            UNIT_ASSERT(data.Status == IImportS3Engine::EDataStatus::NeedInput);
        }

        UNIT_ASSERT_VALUES_EQUAL(engine->PendingBytes(), 0);
        UNIT_ASSERT(checksumInput.empty());

        bool finished = false;
        for (ui32 step = 0; step < 4096 && !finished; ++step) {
            data = ExtractValue(engine->GetData(pool, addRow, addChecksum));
            switch (data.Status) {
            case IImportS3Engine::EDataStatus::NeedInput: {
                const auto range = ExtractValue(engine->NextRange());
                UNIT_ASSERT(range.Status == IImportS3Engine::ENextRangeStatus::Ready);
                if (checksumInput.empty()) {
                    UNIT_ASSERT_VALUES_EQUAL(range.Range.Offset, 0);
                }
                requestedBytes += range.Range.Length;
                AssertSuccess(engine->PutRange(range.Range, Slice(source, range.Range)));
                break;
            }

            case IImportS3Engine::EDataStatus::Ready:
                AssertSuccess(engine->Commit(data.Batch.Id));
                break;

            case IImportS3Engine::EDataStatus::Finished:
                finished = true;
                break;

            case IImportS3Engine::EDataStatus::WaitingForCommit:
                UNIT_FAIL("unexpected batch waiting for commit");
            }
        }

        UNIT_ASSERT_C(finished, "Parquet checksum fallback did not finish");
        UNIT_ASSERT_VALUES_EQUAL(rows.size(), 4);
        UNIT_ASSERT_VALUES_EQUAL(checksumInput, source);
        UNIT_ASSERT_GT(requestedBytes, source.size());
    }

    Y_UNIT_TEST(ParquetReportsAnOversizedRowGroup) {
        static constexpr ui64 BufferSizeLimit = 192_KB;
        const TString source = BuildSmallParquet(/*rowGroupSize=*/4, /*valueSize=*/96_KB);
        UNIT_ASSERT_GT(source.size(), BufferSizeLimit);

        TEngineFixture fixture;
        auto engine = fixture.MakeEngine(
            EDataFormat::Parquet,
            source,
            /*readBatchSize=*/8_KB,
            /*validateChecksum=*/false,
            ECompressionCodec::None,
            BufferSizeLimit);

        TMemoryPool pool(256);
        TVector<TDecodedRow> rows;
        const auto addRow = CaptureRows(rows);
        const auto unexpectedChecksum = [](TStringBuf) {
            UNIT_FAIL("checksum callback was called with validation disabled");
        };

        for (ui32 step = 0; step < 256; ++step) {
            const auto data = ExtractValue(engine->GetData(pool, addRow, unexpectedChecksum));
            UNIT_ASSERT(data.Status == IImportS3Engine::EDataStatus::NeedInput);

            auto rangeResult = engine->NextRange();
            if (!rangeResult) {
                UNIT_ASSERT_C(rangeResult.error().Contains("reached buffer size limit"), rangeResult.error());
                UNIT_ASSERT_C(rangeResult.error().Contains("rowGroup=0"), rangeResult.error());
                UNIT_ASSERT(rows.empty());
                return;
            }

            const auto range = std::move(*rangeResult);
            UNIT_ASSERT(range.Status == IImportS3Engine::ENextRangeStatus::Ready);
            AssertSuccess(engine->PutRange(range.Range, Slice(source, range.Range)));
        }

        UNIT_FAIL("oversized Parquet row group did not reach the buffer limit");
    }

    Y_UNIT_TEST(ParquetPreservesCachedFooterAcrossRangeRetry) {
        const TString source = BuildSmallParquet();
        TEngineFixture fixture;
        auto engine = fixture.MakeEngine(
            EDataFormat::Parquet,
            source,
            /*readBatchSize=*/8_KB,
            /*validateChecksum=*/true);

        TMemoryPool pool(256);
        TVector<TDecodedRow> rows;
        TString checksumInput;
        const auto addRow = CaptureRows(rows);
        const auto addChecksum = [&](TStringBuf data) {
            checksumInput.append(data.data(), data.size());
        };

        auto data = ExtractValue(engine->GetData(pool, addRow, addChecksum));
        UNIT_ASSERT(data.Status == IImportS3Engine::EDataStatus::NeedInput);

        const TImportRange footerTail{
            .Offset = source.size() - 64_KB,
            .Length = 64_KB,
        };
        TVector<TImportRange> successfulRanges;
        ui64 footerBytes = 0;
        while (footerBytes < footerTail.Length) {
            const auto footerRange = ExtractValue(engine->NextRange());
            UNIT_ASSERT(footerRange.Status == IImportS3Engine::ENextRangeStatus::Ready);
            UNIT_ASSERT_VALUES_EQUAL(footerRange.Range.Offset, footerTail.Offset + footerBytes);
            UNIT_ASSERT_LE(footerRange.Range.End(), source.size());

            footerBytes += footerRange.Range.Length;
            successfulRanges.push_back(footerRange.Range);
            AssertSuccess(engine->PutRange(footerRange.Range, Slice(source, footerRange.Range)));
            data = ExtractValue(engine->GetData(pool, addRow, addChecksum));
            UNIT_ASSERT(data.Status == IImportS3Engine::EDataStatus::NeedInput);
        }

        UNIT_ASSERT(checksumInput.empty());
        UNIT_ASSERT_VALUES_EQUAL(engine->PendingBytes(), footerTail.Length);
        UNIT_ASSERT(engine->HasLiveState());

        const auto prefixRange = ExtractValue(engine->NextRange());
        UNIT_ASSERT(prefixRange.Status == IImportS3Engine::ENextRangeStatus::Ready);
        UNIT_ASSERT_VALUES_EQUAL(prefixRange.Range.Offset, 0);
        AssertSuccess(engine->FailRange(prefixRange.Range));

        NKikimrBackup::TS3DownloadState checkpoint;
        AssertSuccess(engine->RestoreFromState(/*processedBytes=*/0, checkpoint));
        UNIT_ASSERT_VALUES_EQUAL(engine->PendingBytes(), footerTail.Length);

        const auto retriedRange = ExtractValue(engine->NextRange());
        UNIT_ASSERT(retriedRange.Status == IImportS3Engine::ENextRangeStatus::Ready);
        UNIT_ASSERT_VALUES_EQUAL(retriedRange.Range.Offset, prefixRange.Range.Offset);
        UNIT_ASSERT_VALUES_EQUAL(retriedRange.Range.Length, prefixRange.Range.Length);
        UNIT_ASSERT_LE(retriedRange.Range.End(), footerTail.Offset);
        successfulRanges.push_back(retriedRange.Range);
        AssertSuccess(engine->PutRange(retriedRange.Range, Slice(source, retriedRange.Range)));

        bool finished = false;
        for (ui32 step = 0; step < 2048 && !finished; ++step) {
            data = ExtractValue(engine->GetData(pool, addRow, addChecksum));
            switch (data.Status) {
            case IImportS3Engine::EDataStatus::NeedInput: {
                const auto range = ExtractValue(engine->NextRange());
                UNIT_ASSERT(range.Status == IImportS3Engine::ENextRangeStatus::Ready);
                successfulRanges.push_back(range.Range);
                AssertSuccess(engine->PutRange(range.Range, Slice(source, range.Range)));
                break;
            }

            case IImportS3Engine::EDataStatus::Ready:
                AssertSuccess(engine->Commit(data.Batch.Id));
                break;

            case IImportS3Engine::EDataStatus::Finished:
                finished = true;
                break;

            case IImportS3Engine::EDataStatus::WaitingForCommit:
                UNIT_FAIL("unexpected batch waiting for commit");
            }
        }

        UNIT_ASSERT_C(finished, "Parquet import engine did not finish after range retry");
        UNIT_ASSERT_VALUES_EQUAL(rows.size(), 4);
        UNIT_ASSERT_VALUES_EQUAL(checksumInput, source);
        AssertReadExactlyOnce(successfulRanges, source.size());
    }
}

} // anonymous namespace
} // namespace NKikimr::NDataShard

#endif // KIKIMR_DISABLE_S3_OPS
