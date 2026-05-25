#include "ut_helpers/ut_backup_restore_common.h"

#include <ydb/public/api/protos/ydb_import.pb.h>

#include <ydb/core/backup/common/checksum.h>
#include <ydb/core/base/localdb.h>
#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/core/tablet/resource_broker.h>
#include <ydb/core/tx/datashard/datashard.h>
#include <ydb/core/tx/datashard/import_data_parser.h>
#include <ydb/core/tx/datashard/import_parquet_s3_file.h>
#include <ydb/core/tx/schemeshard/schemeshard_private.h>
#include <ydb/core/tx/schemeshard/ut_helpers/helpers.h>
#include <ydb/core/wrappers/ut_helpers/s3_mock.h>
#include <ydb/core/ydb_convert/table_description.h>

#include <library/cpp/testing/common/env.h>
#include <library/cpp/testing/unittest/registar.h>

#include <arrow/api.h>
#include <arrow/io/memory.h>
#include <parquet/arrow/reader.h>
#include <parquet/arrow/writer.h>

#include <util/generic/hash.h>
#include <util/generic/size_literals.h>
#include <util/string/builder.h>
#include <util/string/cast.h>
#include <util/string/printf.h>

#include <google/protobuf/text_format.h>

#include <algorithm>
#include <array>
#include <limits>

using namespace NKikimr::NSchemeShard;
using namespace NKikimr::NWrappers::NTestHelpers;
using namespace NKikimr;
using namespace NKikimrSchemeOp;
using namespace NSchemeShardUT_Private;
using namespace NKikimr::Tests;

namespace {

using ERestoreDataFormat = EDataFormat;

struct TParquetUtf8Row {
    TString Key;
    TString Value;
};

constexpr TStringBuf LargeParquetSizeParam = "parquet_table_size_mib";
constexpr ui64 LargeParquetRowBytes = 64_KB;
constexpr ui64 LargeParquetValueBytes = LargeParquetRowBytes - sizeof(i64);
constexpr i64 LargeParquetRowsPerGroup = 16;
constexpr ui64 SparseRangeChunkSize = 1_MB;

static_assert(1_MB % LargeParquetRowBytes == 0);

char LargeParquetValueChar(i64 key) {
    return static_cast<char>('a' + key % 26);
}

class TStringArrowOutputStream final : public arrow::io::OutputStream {
public:
    explicit TStringArrowOutputStream(TString* output)
        : Output(output)
    {
    }

    arrow::Status Close() override {
        Output = nullptr;
        return arrow::Status::OK();
    }

    bool closed() const override {
        return Output == nullptr;
    }

    arrow::Result<int64_t> Tell() const override {
        return Position;
    }

    arrow::Status Write(const void* data, int64_t size) override {
        if (!Output) {
            return arrow::Status::IOError("write to a closed stream");
        }
        if (size < 0) {
            return arrow::Status::Invalid("negative write size");
        }

        Output->append(static_cast<const char*>(data), static_cast<size_t>(size));
        Position += size;
        return arrow::Status::OK();
    }

    using arrow::io::Writable::Write;

private:
    TString* Output;
    int64_t Position = 0;
};

struct TLargeParquetData {
    TString Data;
    ui64 LogicalBytes = 0;
    ui64 Rows = 0;
    ui64 RowGroups = 0;
};

ui64 GetLargeParquetTableSize() {
    const TString value = GetTestParam(LargeParquetSizeParam, "1024");
    ui64 sizeMiB = 0;
    UNIT_ASSERT_C(TryFromString(value, sizeMiB),
        "invalid --test-param " << LargeParquetSizeParam << "=" << value);
    UNIT_ASSERT_C(sizeMiB > 0, LargeParquetSizeParam << " must be greater than zero");
    UNIT_ASSERT_C(sizeMiB <= std::numeric_limits<ui64>::max() / 1_MB,
        LargeParquetSizeParam << " is too large: " << sizeMiB);

    return sizeMiB * 1_MB;
}

TLargeParquetData BuildLargeParquetData(ui64 targetBytes) {
    UNIT_ASSERT_VALUES_EQUAL(targetBytes % LargeParquetRowBytes, 0);

    TLargeParquetData result;
    result.LogicalBytes = targetBytes;
    result.Rows = targetBytes / LargeParquetRowBytes;
    result.RowGroups = (result.Rows + LargeParquetRowsPerGroup - 1) / LargeParquetRowsPerGroup;
    UNIT_ASSERT_C(result.Rows <= static_cast<ui64>(std::numeric_limits<int64_t>::max()),
        "too many rows: " << result.Rows);

    const auto schema = std::make_shared<arrow::Schema>(arrow::FieldVector{
        arrow::field("key", arrow::int64()),
        arrow::field("value", arrow::utf8()),
    });

    parquet::WriterProperties::Builder propertiesBuilder;
    propertiesBuilder.compression(arrow::Compression::SNAPPY);
    propertiesBuilder.disable_dictionary();

    const auto sink = std::make_shared<TStringArrowOutputStream>(&result.Data);
    std::unique_ptr<parquet::arrow::FileWriter> writer;
    UNIT_ASSERT_C(parquet::arrow::FileWriter::Open(
        *schema,
        arrow::default_memory_pool(),
        sink,
        propertiesBuilder.build(),
        &writer).ok(), "failed to open parquet writer");

    std::array<TString, 26> values;
    for (size_t i = 0; i < values.size(); ++i) {
        values[i] = TString(LargeParquetValueBytes, static_cast<char>('a' + i));
    }

    ui64 firstRow = 0;
    while (firstRow < result.Rows) {
        const i64 rows = static_cast<i64>(Min<ui64>(LargeParquetRowsPerGroup, result.Rows - firstRow));
        arrow::Int64Builder keyBuilder;
        arrow::StringBuilder valueBuilder;
        UNIT_ASSERT(keyBuilder.Reserve(rows).ok());
        UNIT_ASSERT(valueBuilder.Reserve(rows).ok());
        UNIT_ASSERT(valueBuilder.ReserveData(rows * LargeParquetValueBytes).ok());

        for (i64 i = 0; i < rows; ++i) {
            const i64 key = static_cast<i64>(firstRow) + i;
            const auto& value = values[static_cast<size_t>(key % values.size())];
            UNIT_ASSERT(keyBuilder.Append(key).ok());
            UNIT_ASSERT(valueBuilder.Append(value.data(), value.size()).ok());
        }

        std::shared_ptr<arrow::Array> keyArray;
        std::shared_ptr<arrow::Array> valueArray;
        UNIT_ASSERT(keyBuilder.Finish(&keyArray).ok());
        UNIT_ASSERT(valueBuilder.Finish(&valueArray).ok());

        const auto table = arrow::Table::Make(schema, {keyArray, valueArray});
        UNIT_ASSERT_C(writer->WriteTable(*table, rows).ok(),
            "failed to write parquet row group " << firstRow / LargeParquetRowsPerGroup);
        firstRow += rows;
    }

    UNIT_ASSERT_C(writer->Close().ok(), "failed to close parquet writer");
    return result;
}

void PutSparseRange(
    const TString& source,
    const std::shared_ptr<NDataShard::TParquetSparseFile>& destination,
    ui64 offset,
    ui64 length)
{
    UNIT_ASSERT_C(offset <= source.size() && length <= source.size() - offset,
        "range " << offset << "+" << length << " is outside a " << source.size() << " byte file");

    while (length > 0) {
        const ui64 chunkSize = Min(length, SparseRangeChunkSize);
        destination->PutRange(offset, TString(source.data() + offset, static_cast<size_t>(chunkSize)));
        offset += chunkSize;
        length -= chunkSize;
    }
}

NKikimrSchemeOp::TTableDescription MakeLargeParquetTableScheme() {
    NKikimrSchemeOp::TTableDescription scheme;
    scheme.SetName("Table");
    scheme.SetPath("/MyRoot/Table");

    auto* key = scheme.AddColumns();
    key->SetId(1);
    key->SetName("key");
    key->SetTypeId(NScheme::NTypeIds::Int64);

    auto* value = scheme.AddColumns();
    value->SetId(2);
    value->SetName("value");
    value->SetTypeId(NScheme::NTypeIds::Utf8);

    scheme.AddKeyColumnIds(1);
    scheme.AddKeyColumnNames("key");
    return scheme;
}

void CheckLargeParquetRoundTrip(const TLargeParquetData& source) {
    auto sparseFile = std::make_shared<NDataShard::TParquetSparseFile>(source.Data.size());
    const auto footerRange = NDataShard::TParquetSparseFile::FooterTailRange(source.Data.size());
    PutSparseRange(source.Data, sparseFile, footerRange.Offset, footerRange.Length);
    const bool usesSparseReads = !sparseFile->IsFullyBuffered();

    auto metadataRangeResult = sparseFile->TryParseFooterMetadataRange();
    UNIT_ASSERT_C(metadataRangeResult.has_value(), metadataRangeResult.error());
    auto metadataRange = std::move(*metadataRangeResult);
    if (source.LogicalBytes >= 1_GB) {
        UNIT_ASSERT_C(metadataRange.Defined(),
            "the default fixture must have metadata larger than the 64 KiB footer tail");
    }
    if (metadataRange) {
        PutSparseRange(source.Data, sparseFile, metadataRange->Offset, metadataRange->Length);

        auto remainingMetadataRangeResult = sparseFile->TryParseFooterMetadataRange();
        UNIT_ASSERT_C(remainingMetadataRangeResult.has_value(), remainingMetadataRangeResult.error());
        auto remainingMetadataRange = std::move(*remainingMetadataRangeResult);
        UNIT_ASSERT_C(!remainingMetadataRange, "parquet metadata is still incomplete");
    }

    auto dataRangesResult = sparseFile->PlanColumnChunkRanges(sparseFile);
    UNIT_ASSERT_C(dataRangesResult.has_value(), dataRangesResult.error());
    auto dataRanges = std::move(*dataRangesResult);
    UNIT_ASSERT_VALUES_EQUAL(dataRanges.empty(), !usesSparseReads);
    for (const auto& range : dataRanges) {
        PutSparseRange(source.Data, sparseFile, range.Offset, range.Length);
    }

    UNIT_ASSERT_VALUES_EQUAL(sparseFile->IsFullyBuffered(), !usesSparseReads);

    const auto scheme = MakeLargeParquetTableScheme();
    NDataShard::TUserTable::TPtr userTable = new NDataShard::TUserTable(1, scheme, 0);
    const NDataShard::TTableInfo tableInfo(1, userTable);
    auto parser = NDataShard::CreateParquetDataParser();
    auto configureResult = parser->Configure(tableInfo, scheme);
    UNIT_ASSERT_C(configureResult.has_value(), configureResult.error());

    auto* streamParser = NDataShard::AsParquetStreamParser(parser.Get());
    UNIT_ASSERT(streamParser);
    auto openResult = streamParser->OpenFile(sparseFile->MakeRandomAccessFile(sparseFile));
    UNIT_ASSERT_C(openResult.has_value(), openResult.error());

    ui64 decodedBytes = 0;
    ui64 decodedRows = 0;
    const NDataShard::IDataParser::TAddRowFn addRow = [&](const TVector<TCell>& keys, const TVector<TCell>& values) {
        UNIT_ASSERT_VALUES_EQUAL(keys.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(values.size(), 1);
        UNIT_ASSERT(!keys[0].IsNull());
        UNIT_ASSERT(!values[0].IsNull());

        const i64 key = keys[0].AsValue<i64>();
        UNIT_ASSERT_VALUES_EQUAL(key, static_cast<i64>(decodedRows));
        UNIT_ASSERT_VALUES_EQUAL(values[0].Size(), LargeParquetValueBytes);

        const TStringBuf value(values[0].Data(), values[0].Size());
        const char expected = LargeParquetValueChar(key);
        UNIT_ASSERT_C(std::all_of(value.begin(), value.end(), [expected](char c) { return c == expected; }),
            "invalid value for key " << key);

        decodedBytes += keys[0].Size() + values[0].Size();
        ++decodedRows;
    };

    TMemoryPool pool(256);
    ui64 pendingBytes = 0;
    ui64 pendingRows = 0;
    while (true) {
        auto batchResult = streamParser->ProcessNextBatch(pool, addRow);
        UNIT_ASSERT_C(batchResult.has_value(), batchResult.error());
        pendingBytes += batchResult->DataBytes;
        pendingRows += batchResult->Rows;
        if (!batchResult->HasMore) {
            break;
        }
    }

    UNIT_ASSERT_VALUES_EQUAL(decodedRows, source.Rows);
    UNIT_ASSERT_VALUES_EQUAL(decodedBytes, source.LogicalBytes);
    UNIT_ASSERT_VALUES_EQUAL(pendingRows, source.Rows);
    UNIT_ASSERT_VALUES_EQUAL(pendingBytes, source.LogicalBytes);
}

TString BuildParquetUtf8Data(
    const TVector<TParquetUtf8Row>& rows,
    i64 rowGroupSize = 16)
{
    arrow::StringBuilder keyBuilder;
    arrow::StringBuilder valueBuilder;
    for (auto&& r : rows) {
        UNIT_ASSERT(keyBuilder.Append(r.Key.data(), r.Key.size()).ok());
        UNIT_ASSERT(valueBuilder.Append(r.Value.data(), r.Value.size()).ok());
    }

    std::shared_ptr<arrow::Array> keyArray;
    std::shared_ptr<arrow::Array> valueArray;
    UNIT_ASSERT(keyBuilder.Finish(&keyArray).ok());
    UNIT_ASSERT(valueBuilder.Finish(&valueArray).ok());

    auto schema = std::make_shared<arrow::Schema>(arrow::FieldVector{
        arrow::field("key", arrow::utf8()),
        arrow::field("value", arrow::utf8()),
    });

    auto table = arrow::Table::Make(schema, {keyArray, valueArray});

    auto sink = arrow::io::BufferOutputStream::Create(0).ValueOrDie();
    UNIT_ASSERT(parquet::arrow::WriteTable(
        *table,
        arrow::default_memory_pool(),
        sink,
        rowGroupSize).ok());
    auto buffer = sink->Finish().ValueOrDie();
    return TString(reinterpret_cast<const char*>(buffer->data()), buffer->size());
}

TString BuildParquetInt32KeyData(const TVector<std::pair<i32, TString>>& rows) {
    arrow::Int32Builder keyBuilder;
    arrow::StringBuilder valueBuilder;
    for (auto&& [k, v] : rows) {
        UNIT_ASSERT(keyBuilder.Append(k).ok());
        UNIT_ASSERT(valueBuilder.Append(v.data(), v.size()).ok());
    }

    std::shared_ptr<arrow::Array> keyArray;
    std::shared_ptr<arrow::Array> valueArray;
    UNIT_ASSERT(keyBuilder.Finish(&keyArray).ok());
    UNIT_ASSERT(valueBuilder.Finish(&valueArray).ok());

    auto schema = std::make_shared<arrow::Schema>(arrow::FieldVector{
        arrow::field("key", arrow::int32()),
        arrow::field("value", arrow::utf8()),
    });

    auto table = arrow::Table::Make(schema, {keyArray, valueArray});

    auto sink = arrow::io::BufferOutputStream::Create(0).ValueOrDie();
    UNIT_ASSERT(parquet::arrow::WriteTable(*table, arrow::default_memory_pool(), sink, /*chunk_size*/ 16).ok());
    auto buffer = sink->Finish().ValueOrDie();
    return TString(reinterpret_cast<const char*>(buffer->data()), buffer->size());
}

TString BuildParquetUtf8WithNullValue() {
    arrow::StringBuilder keyBuilder;
    arrow::StringBuilder valueBuilder;
    UNIT_ASSERT(keyBuilder.Append("k1", 2).ok());
    UNIT_ASSERT(valueBuilder.AppendNull().ok());
    UNIT_ASSERT(keyBuilder.Append("k2", 2).ok());
    UNIT_ASSERT(valueBuilder.Append("v2", 2).ok());

    std::shared_ptr<arrow::Array> keyArray;
    std::shared_ptr<arrow::Array> valueArray;
    UNIT_ASSERT(keyBuilder.Finish(&keyArray).ok());
    UNIT_ASSERT(valueBuilder.Finish(&valueArray).ok());

    auto schema = std::make_shared<arrow::Schema>(arrow::FieldVector{
        arrow::field("key", arrow::utf8()),
        arrow::field("value", arrow::utf8()),
    });

    auto table = arrow::Table::Make(schema, {keyArray, valueArray});
    auto sink = arrow::io::BufferOutputStream::Create(0).ValueOrDie();
    UNIT_ASSERT(parquet::arrow::WriteTable(*table, arrow::default_memory_pool(), sink, /*chunk_size*/ 16).ok());
    auto buffer = sink->Finish().ValueOrDie();
    return TString(reinterpret_cast<const char*>(buffer->data()), buffer->size());
}

struct TDataWithChecksum {
    TString Data;
    TString Checksum;

    TDataWithChecksum() = default;

    TDataWithChecksum(TString&& data)
        : Data(std::move(data))
        , Checksum(NBackup::ComputeChecksum(Data))
    {}

    TDataWithChecksum(const char* data)
        : TDataWithChecksum(TString(data))
    {}

    operator TString() const {
        return Data;
    }
};

struct TTestData {
    TDataWithChecksum RawData;
    TString Data;
    TString YsonStr;
    EDataFormat DataFormat = EDataFormat::YdbDump;

    TTestData(TString csvData, TString ysonStr)
        : RawData(std::move(csvData))
        , Data(RawData)
        , YsonStr(std::move(ysonStr))
    {}

    TTestData(TString payload, TString ysonStr, EDataFormat format)
        : RawData(std::move(payload))
        , Data(static_cast<TString>(RawData))
        , YsonStr(std::move(ysonStr))
        , DataFormat(format)
    {}

    TString Ext() const {
        switch (DataFormat) {
        case EDataFormat::YdbDump:
            return ".csv";
        case EDataFormat::Parquet:
            return ".parquet";
        case EDataFormat::Invalid:
            UNIT_ASSERT_C(false, "Invalid data format");
            break;
        }

        return {};
    }
};

TTestData GenerateCsvTestData(const TString& keyPrefix, ui32 count) {
    TStringBuilder csv;
    TStringBuilder yson;

    for (ui32 i = 1; i <= count; ++i) {
        if (keyPrefix) {
            csv << "\"" << keyPrefix << i << "\",";
        } else {
            csv << i << ",";
        }

        csv << "\"" << "value" << i << "\"" << Endl;

        if (i == 1) {
            yson << "[[[[";
        } else {
            yson << ";";
        }

        yson << "["
            << "[\"" << (keyPrefix ? keyPrefix + ToString(i) : ToString(i)) << "\"];"
            << "[\"" << "value" << i << "\"]"
            << "]";

        if (i == count) {
            yson << "];\%false]]]";
        }
    }

    return TTestData(std::move(csv), std::move(yson));
}

TTestData GenerateCsvUtf8Rows(const TVector<TParquetUtf8Row>& rows) {
    TStringBuilder csv;
    TStringBuilder yson;

    for (size_t i = 0; i < rows.size(); ++i) {
        csv << "\"" << rows[i].Key << "\",\"" << rows[i].Value << "\"" << Endl;

        if (i == 0) {
            yson << "[[[[";
        } else {
            yson << ";";
        }

        yson << "[[\"" << rows[i].Key << "\"];[\"" << rows[i].Value << "\"]]";

        if (i + 1 == rows.size()) {
            yson << "];\%false]]]";
        }
    }

    return TTestData(std::move(csv), std::move(yson));
}

struct TTestDataWithScheme {
    TDataWithChecksum Metadata = R"({"version": 0})";
    EPathType Type = EPathTypeTable;
    TDataWithChecksum Scheme;
    TVector<TTestData> Data;

    TTestDataWithScheme(TString&& scheme, TVector<TTestData>&& data)
        : Scheme(std::move(scheme))
        , Data(std::move(data))
    {}
};

THashMap<TString, TString> ConvertTableTestData(const TTestDataWithScheme& item) {
    THashMap<TString, TString> result;
    const TString prefix;

    result.emplace(prefix + "/metadata.json", item.Metadata);

    const auto schemeKey = prefix + "/scheme.pb";
    result.emplace(schemeKey, item.Scheme);

    for (ui32 i = 0; i < item.Data.size(); ++i) {
        const auto& data = item.Data.at(i);
        result.emplace(Sprintf("%s/data_%02d%s", prefix.data(), i, data.Ext().c_str()), data.Data);
    }

    return result;
}

THashMap<TString, TString> MakeParquetS3Data(
    const TString& scheme,
    const TVector<TString>& parts,
    bool withChecksums = false)
{
    THashMap<TString, TString> data;
    auto addObject = [&](TString key, const TString& value) {
        if (withChecksums) {
            data.emplace(NBackup::ChecksumKey(key), NBackup::ComputeChecksum(value));
        }
        data.emplace(std::move(key), value);
    };

    const TString metadata = withChecksums
        ? R"({"version": 1, "permissions": 0})"
        : R"({"version": 0})";
    addObject("/metadata.json", metadata);
    addObject("/scheme.pb", scheme);
    for (ui32 i = 0; i < parts.size(); ++i) {
        addObject(Sprintf("/data_%02d.parquet", i), parts[i]);
    }

    return data;
}

TString Utf8KeySchemePb() {
    return R"(
        columns {
          name: "key"
          type { optional_type { item { type_id: UTF8 } } }
        }
        columns {
          name: "value"
          type { optional_type { item { type_id: UTF8 } } }
        }
        primary_key: "key"
    )";
}

TString Int32KeySchemePb() {
    return R"(
        columns {
          name: "key"
          type { optional_type { item { type_id: INT32 } } }
        }
        columns {
          name: "value"
          type { optional_type { item { type_id: UTF8 } } }
        }
        primary_key: "key"
    )";
}

void ApplyParquetFeatureFlag(TTestBasicRuntime& runtime, ERestoreDataFormat format, bool enable = true) {
    if (format == ERestoreDataFormat::Parquet && enable) {
        runtime.GetAppData().FeatureFlags.SetEnableImportInParquet(true);
    }
}

void DoRestore(
    TTestBasicRuntime& runtime,
    TTestEnv& env,
    const TString& tableScheme,
    const THashMap<TString, TString>& s3Data,
    ERestoreDataFormat format,
    ui32 readBatchSize = 4194304)
{
    ApplyParquetFeatureFlag(runtime, format);

    ui64 txId = 100;
    TestCreateTable(runtime, ++txId, "/MyRoot", tableScheme);
    env.TestWaitNotification(runtime, txId);

    TPortManager portManager;
    const ui16 port = portManager.GetPort();

    TS3Mock s3Mock(s3Data, TS3Mock::TSettings(port));
    UNIT_ASSERT(s3Mock.Start());

    runtime.SetLogPriority(NKikimrServices::DATASHARD_RESTORE, NActors::NLog::PRI_TRACE);

    const auto desc = DescribePath(runtime, "/MyRoot/Table", true, true);
    UNIT_ASSERT_VALUES_EQUAL(desc.GetStatus(), NKikimrScheme::StatusSuccess);

    NKikimrSchemeOp::TTableDescription tableDescription;
    tableDescription.MutableColumns()->CopyFrom(desc.GetPathDescription().GetTable().GetColumns());
    tableDescription.MutableKeyColumnNames()->CopyFrom(desc.GetPathDescription().GetTable().GetKeyColumnNames());

    TString tableDescriptionStr;
    UNIT_ASSERT(google::protobuf::TextFormat::PrintToString(tableDescription, &tableDescriptionStr));

    TestRestore(runtime, ++txId, "/MyRoot", Sprintf(R"(
        TableName: "Table"
        TableDescription {
            %s
        }
        S3Settings {
            Endpoint: "localhost:%d"
            Scheme: HTTP
            Limits {
                ReadBatchSize: %u
            }
        }
    )", tableDescriptionStr.data(), port, readBatchSize), {NKikimrScheme::StatusAccepted});
    env.TestWaitNotification(runtime, txId);
}

void DoImport(
    TTestBasicRuntime& runtime,
    const THashMap<TString, TString>& s3Data,
    ERestoreDataFormat format,
    Ydb::StatusIds::StatusCode expectedStatus = Ydb::StatusIds::SUCCESS,
    bool enableParquetFeatureFlag = true,
    TStringBuf expectedIssue = {})
{
    TTestEnv env(runtime, TTestEnvOptions());
    ApplyParquetFeatureFlag(runtime, format, enableParquetFeatureFlag);

    ui64 id = 100;

    TPortManager portManager;
    const ui16 port = portManager.GetPort();

    TS3Mock s3Mock(s3Data, TS3Mock::TSettings(port));
    UNIT_ASSERT(s3Mock.Start());

    runtime.SetLogPriority(NKikimrServices::DATASHARD_RESTORE, NActors::NLog::PRI_TRACE);
    runtime.SetLogPriority(NKikimrServices::IMPORT, NActors::NLog::PRI_TRACE);

    const TString requestStr = Sprintf(R"(
        ImportFromS3Settings {
          endpoint: "localhost:%d"
          scheme: HTTP
          items {
            source_prefix: ""
            destination_path: "/MyRoot/Table"
          }
        }
    )", port);

    auto initialStatus = Ydb::StatusIds::SUCCESS;
    if (expectedStatus == Ydb::StatusIds::BAD_REQUEST ||
        expectedStatus == Ydb::StatusIds::PRECONDITION_FAILED) {
        initialStatus = expectedStatus;
    }

    TestImport(runtime, ++id, "/MyRoot", requestStr, "", "", initialStatus);
    env.TestWaitNotification(runtime, id);

    if (initialStatus == Ydb::StatusIds::SUCCESS) {
        const auto response = TestGetImport(runtime, id, "/MyRoot", expectedStatus);
        if (expectedIssue) {
            const auto& issues = response.GetResponse().GetEntry().GetIssues();
            UNIT_ASSERT(!issues.empty());
            UNIT_ASSERT_STRING_CONTAINS(issues.begin()->message(), expectedIssue);
        }
    }
}

THashMap<TString, TString> MakeSingleShardUtf8S3Data(ERestoreDataFormat format) {
    if (format == ERestoreDataFormat::Parquet) {
        const auto parquet = BuildParquetUtf8Data({
            {"a1", "value1"},
            {"a2", "value2"},
            {"a3", "value3"},
        });
        return MakeParquetS3Data(Utf8KeySchemePb(), {parquet});
    }

    return ConvertTableTestData(TTestDataWithScheme(
        Utf8KeySchemePb(),
        {GenerateCsvTestData("a", 3)}));
}

THashMap<TString, TString> MakeInt32KeyS3Data(ERestoreDataFormat format) {
    if (format == ERestoreDataFormat::Parquet) {
        const auto parquet = BuildParquetInt32KeyData({
            {1, "v1"},
            {2, "v2"},
            {3, "v3"},
        });

        return MakeParquetS3Data(Int32KeySchemePb(), {parquet});
    }

    TStringBuilder csv;
    TStringBuilder yson;
    for (i32 i = 1; i <= 3; ++i) {
        csv << i << ",\"v" << i << "\"" << Endl;
        if (i == 1) {
            yson << "[[[[";
        } else {
            yson << ";";
        }
        yson << "[[\"" << i << "\"];[\"v" << i << "\"]]";
        if (i == 3) {
            yson << "];\%false]]]";
        }
    }

    return ConvertTableTestData(TTestDataWithScheme(
        Int32KeySchemePb(),
        {TTestData(std::move(csv), std::move(yson))}));
}

THashMap<TString, TString> MakeMultiShardUtf8S3Data(ERestoreDataFormat format) {
    if (format == ERestoreDataFormat::Parquet) {
        return MakeParquetS3Data(Utf8KeySchemePb(), TVector<TString>{
            BuildParquetUtf8Data({{"a1", "v_a_1"}, {"a2", "v_a_2"}}),
            BuildParquetUtf8Data({{"b1", "v_b_1"}, {"b2", "v_b_2"}}),
        });
    }

    return ConvertTableTestData(TTestDataWithScheme(
        Utf8KeySchemePb(),
        {
            GenerateCsvUtf8Rows({{"a1", "v_a_1"}, {"a2", "v_a_2"}}),
            GenerateCsvUtf8Rows({{"b1", "v_b_1"}, {"b2", "v_b_2"}}),
        }));
}

THashMap<TString, TString> MakeImportSingleShardS3Data(ERestoreDataFormat format) {
    if (format == ERestoreDataFormat::Parquet) {
        const auto parquet = BuildParquetUtf8Data({
            {"a1", "value1"},
            {"a2", "value2"},
        });

        return MakeParquetS3Data(Utf8KeySchemePb(), {parquet});
    }

    return ConvertTableTestData(TTestDataWithScheme(
        Utf8KeySchemePb(),
        {GenerateCsvTestData("a", 2)}));
}

Y_UNIT_TEST_SUITE(TRestoreDataFormatTests) {
    Y_UNIT_TEST(ShouldSucceedOnSingleShardUtf8, ERestoreDataFormat) {
        const auto format = Arg<0>();
        if (format == ERestoreDataFormat::Invalid) {
            return;
        }

        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions());

        const auto s3Data = MakeSingleShardUtf8S3Data(format);

        DoRestore(runtime, env, R"(
            Name: "Table"
            Columns { Name: "key" Type: "Utf8" }
            Columns { Name: "value" Type: "Utf8" }
            KeyColumnNames: ["key"]
        )", s3Data, format);

        const TString expectedYson =
            R"([[[[[["a1"];["value1"]];[["a2"];["value2"]];[["a3"];["value3"]]];%false]]])";

        auto content = ReadTable(runtime, TTestTxConfig::FakeHiveTablets, "Table", {"key"}, {"key", "value"});
        NKqp::CompareYson(expectedYson, content);
    }

    Y_UNIT_TEST(ShouldSucceedOnInt32Key, ERestoreDataFormat) {
        const auto format = Arg<0>();
        if (format == ERestoreDataFormat::Invalid) {
            return;
        }

        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions());

        DoRestore(runtime, env, R"(
            Name: "Table"
            Columns { Name: "key" Type: "Int32" }
            Columns { Name: "value" Type: "Utf8" }
            KeyColumnNames: ["key"]
        )", MakeInt32KeyS3Data(format), format);

        const TString expectedYson =
            R"([[[[[["1"];["v1"]];[["2"];["v2"]];[["3"];["v3"]]];%false]]])";

        auto content = ReadTable(runtime, TTestTxConfig::FakeHiveTablets, "Table", {"key"}, {"key", "value"});
        NKqp::CompareYson(expectedYson, content);
    }

    Y_UNIT_TEST(ShouldSucceedOnMultiShardTable, ERestoreDataFormat) {
        const auto format = Arg<0>();
        if (format == ERestoreDataFormat::Invalid) {
            return;
        }

        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions());

        ApplyParquetFeatureFlag(runtime, format);

        ui64 txId = 100;
        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "Table"
            Columns { Name: "key" Type: "Utf8" }
            Columns { Name: "value" Type: "Utf8" }
            KeyColumnNames: ["key"]
            SplitBoundary {
              KeyPrefix {
                Tuple { Optional { Text: "b" } }
              }
            }
        )");

        env.TestWaitNotification(runtime, txId);

        TPortManager portManager;
        const ui16 port = portManager.GetPort();

        TS3Mock s3Mock(MakeMultiShardUtf8S3Data(format), TS3Mock::TSettings(port));
        UNIT_ASSERT(s3Mock.Start());

        runtime.SetLogPriority(NKikimrServices::DATASHARD_RESTORE, NActors::NLog::PRI_TRACE);

        const auto desc = DescribePath(runtime, "/MyRoot/Table", true, true);
        UNIT_ASSERT_VALUES_EQUAL(desc.GetStatus(), NKikimrScheme::StatusSuccess);

        NKikimrSchemeOp::TTableDescription tableDescription;
        tableDescription.MutableColumns()->CopyFrom(desc.GetPathDescription().GetTable().GetColumns());
        tableDescription.MutableKeyColumnNames()->CopyFrom(desc.GetPathDescription().GetTable().GetKeyColumnNames());

        TString tableDescriptionStr;
        UNIT_ASSERT(google::protobuf::TextFormat::PrintToString(tableDescription, &tableDescriptionStr));

        TestRestore(runtime, ++txId, "/MyRoot", Sprintf(R"(
            TableName: "Table"
            TableDescription {
                %s
            }
            S3Settings {
                Endpoint: "localhost:%d"
                Scheme: HTTP
            }
        )", tableDescriptionStr.data(), port));
        env.TestWaitNotification(runtime, txId);

        {
            auto content = ReadTable(runtime, TTestTxConfig::FakeHiveTablets + 0, "Table", {"key"}, {"key", "value"});
            NKqp::CompareYson(
                R"([[[[[["a1"];["v_a_1"]];[["a2"];["v_a_2"]]];%false]]])", content);
        }
        {
            auto content = ReadTable(runtime, TTestTxConfig::FakeHiveTablets + 1, "Table", {"key"}, {"key", "value"});
            NKqp::CompareYson(
                R"([[[[[["b1"];["v_b_1"]];[["b2"];["v_b_2"]]];%false]]])", content);
        }
    }

    // CSV lines cannot encode NULL (empty token is rejected by TYdbDump::ParseLine).
    Y_UNIT_TEST(ShouldHandleNullValues) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions());

        DoRestore(runtime, env, R"(
            Name: "Table"
            Columns { Name: "key" Type: "Utf8" }
            Columns { Name: "value" Type: "Utf8" }
            KeyColumnNames: ["key"]
        )", MakeParquetS3Data(Utf8KeySchemePb(), {BuildParquetUtf8WithNullValue()}), ERestoreDataFormat::Parquet,
            /*readBatchSize=*/32);

        const TString expectedYson =
            R"([[[[[["k1"];#];[["k2"];["v2"]]];%false]]])";

        auto content = ReadTable(runtime, TTestTxConfig::FakeHiveTablets, "Table", {"key"}, {"key", "value"});
        NKqp::CompareYson(expectedYson, content);
    }

    Y_UNIT_TEST(ShouldRoundTripConfiguredSize) {
        const ui64 targetBytes = GetLargeParquetTableSize();
        const auto parquet = BuildLargeParquetData(targetBytes);

        Cerr << "Large parquet round trip: logical bytes=" << parquet.LogicalBytes
            << ", serialized bytes=" << parquet.Data.size()
            << ", rows=" << parquet.Rows
            << ", row groups=" << parquet.RowGroups << Endl;

        CheckLargeParquetRoundTrip(parquet);
    }
}

Y_UNIT_TEST_SUITE(TImportFromS3DataFormatTests) {
    Y_UNIT_TEST(ShouldSucceedOnSingleShardTable, ERestoreDataFormat) {
        const auto format = Arg<0>();
        if (format == ERestoreDataFormat::Invalid) {
            return;
        }

        TTestBasicRuntime runtime;

        DoImport(runtime, MakeImportSingleShardS3Data(format), format);

        auto content = ReadTable(runtime, TTestTxConfig::FakeHiveTablets, "Table", {"key"}, {"key", "value"});
        NKqp::CompareYson(R"([[[[[["a1"];["value1"]];[["a2"];["value2"]]];%false]]])", content);
    }

    Y_UNIT_TEST(ShouldFailParquetChecksumAfterWritingRows) {
        TTestBasicRuntime runtime;

        const TVector<TParquetUtf8Row> rows = {
            {"a1", TString(24_KB, 'a')},
            {"a2", TString(24_KB, 'b')},
            {"a3", TString(24_KB, 'c')},
            {"a4", TString(24_KB, 'd')},
        };
        const auto parquet = BuildParquetUtf8Data(rows, /*rowGroupSize=*/1);
        UNIT_ASSERT_GT(parquet.size(), 64_KB);
        auto s3Data = MakeParquetS3Data(
            Utf8KeySchemePb(),
            {parquet},
            /*withChecksums=*/true);
        s3Data.at(NBackup::ChecksumKey("/data_00.parquet")) = TString(64, '0');

        DoImport(
            runtime,
            s3Data,
            ERestoreDataFormat::Parquet,
            Ydb::StatusIds::CANCELLED,
            /*enableParquetFeatureFlag=*/true,
            "checksum mismatch");

        auto content = ReadTable(
            runtime,
            TTestTxConfig::FakeHiveTablets,
            "Table",
            {"key"},
            {"key", "value"});
        NKqp::CompareYson(GenerateCsvUtf8Rows(rows).YsonStr, content);
    }

    Y_UNIT_TEST(ShouldFailWhenFeatureFlagDisabled) {
        TTestBasicRuntime runtime;

        const auto parquet = BuildParquetUtf8Data({{"k", "v"}});

        DoImport(
            runtime,
            MakeParquetS3Data(Utf8KeySchemePb(), {parquet}),
            ERestoreDataFormat::Parquet,
            Ydb::StatusIds::CANCELLED,
            /*enableParquetFeatureFlag=*/false,
            "Parquet import is disabled by feature flag EnableImportInParquet");
    }
}

} // anonymous namespace
