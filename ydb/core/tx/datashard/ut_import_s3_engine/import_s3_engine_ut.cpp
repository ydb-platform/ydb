#ifndef KIKIMR_DISABLE_S3_OPS

#include <ydb/core/scheme/scheme_type_info.h>
#include <ydb/core/scheme/scheme_types_proto.h>
#include <ydb/core/tx/datashard/export_data_format.h>
#include <ydb/core/tx/datashard/import_s3_engine.h>
#include <ydb/core/tx/datashard/import_parquet_s3_file.h>

#include <yql/essentials/public/decimal/yql_decimal.h>
#include <yql/essentials/public/udf/udf_data_type.h>
#include <yql/essentials/types/binary_json/read.h>
#include <yql/essentials/types/binary_json/write.h>
#include <yql/essentials/types/dynumber/dynumber.h>

#include <library/cpp/testing/unittest/registar.h>

#include <arrow/api.h>
#include <arrow/io/memory.h>
#include <parquet/arrow/reader.h>
#include <parquet/arrow/writer.h>
#include <parquet/file_reader.h>
#include <contrib/libs/zstd/include/zstd.h>

#include <util/generic/maybe.h>
#include <util/generic/size_literals.h>
#include <util/generic/vector.h>
#include <util/memory/pool.h>
#include <util/stream/null.h>
#include <util/string/builder.h>
#include <util/string/join.h>

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

NKikimrSchemeOp::TTableDescription MakeUint32KeyTableScheme() {
    auto scheme = MakeUtf8TableScheme();
    scheme.MutableColumns(0)->SetTypeId(NScheme::NTypeIds::Uint32);
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
    explicit TEngineFixture(NKikimrSchemeOp::TTableDescription scheme = MakeUtf8TableScheme())
        : Scheme(std::move(scheme))
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
    return [&rows](const TVector<TCell>& keys, const TVector<TCell>& values) -> std::expected<void, TString> {
        UNIT_ASSERT_VALUES_EQUAL(keys.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(values.size(), 1);
        UNIT_ASSERT(!keys.front().IsNull());
        UNIT_ASSERT(!values.front().IsNull());

        rows.push_back({
            TString(keys.front().AsBuf()),
            TString(values.front().AsBuf()),
        });
        return {};
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

// Written the way the exporter does it: default writer properties (Parquet
// format 1.0) and the Arrow schema stored in the file.
TString BuildUint32KeyParquet(const TVector<ui32>& keys) {
    arrow::UInt32Builder keyBuilder;
    arrow::StringBuilder valueBuilder;
    for (const ui32 key : keys) {
        UNIT_ASSERT_C(keyBuilder.Append(key).ok(), "failed to append key " << key);
        UNIT_ASSERT_C(valueBuilder.Append("v", 1).ok(), "failed to append value for key " << key);
    }

    std::shared_ptr<arrow::Array> keyArray;
    std::shared_ptr<arrow::Array> valueArray;
    UNIT_ASSERT_C(keyBuilder.Finish(&keyArray).ok(), "failed to finish key array");
    UNIT_ASSERT_C(valueBuilder.Finish(&valueArray).ok(), "failed to finish value array");

    auto schema = std::make_shared<arrow::Schema>(arrow::FieldVector{
        arrow::field("key", arrow::uint32()),
        arrow::field("value", arrow::utf8()),
    });
    auto table = arrow::Table::Make(schema, {keyArray, valueArray});
    auto sink = arrow::io::BufferOutputStream::Create(0).ValueOrDie();

    auto arrowPropertiesBuilder = parquet::ArrowWriterProperties::Builder();
    arrowPropertiesBuilder.store_schema();
    const auto writeStatus = parquet::arrow::WriteTable(
        *table,
        arrow::default_memory_pool(),
        sink,
        /*chunk_size=*/2,
        parquet::WriterProperties::Builder().build(),
        arrowPropertiesBuilder.build());
    UNIT_ASSERT_C(writeStatus.ok(), writeStatus.ToString());

    auto buffer = sink->Finish().ValueOrDie();
    const auto metadata = parquet::ReadMetaData(std::make_shared<arrow::io::BufferReader>(buffer));
    UNIT_ASSERT_C(metadata->schema()->Column(0)->physical_type() == parquet::Type::INT64,
        "the premise of the test changed: uint32 is no longer stored as INT64");
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

NKikimrSchemeOp::TTableDescription MakeDyNumberJsonDocumentTableScheme() {
    NKikimrSchemeOp::TTableDescription scheme;
    scheme.SetName("Table");
    scheme.SetPath("/Root/Table");

    const auto addColumn = [&scheme](ui32 id, const char* name, NScheme::TTypeId typeId) {
        auto* column = scheme.AddColumns();
        column->SetId(id);
        column->SetName(name);
        column->SetTypeId(typeId);
    };
    addColumn(1, "key", NScheme::NTypeIds::Utf8);
    addColumn(2, "dyn", NScheme::NTypeIds::DyNumber);
    addColumn(3, "doc", NScheme::NTypeIds::JsonDocument);

    scheme.AddKeyColumnIds(1);
    scheme.AddKeyColumnNames("key");
    return scheme;
}

// A row of the table above with its cells in the stored form, i.e. as a table
// scan hands them to the exporter: DyNumber and JsonDocument are binary.
struct TStoredRow {
    TString Key;
    TMaybe<TString> DyNumber;
    TMaybe<TString> JsonDocument;
};

TString StoredDyNumber(TStringBuf text) {
    const auto value = NDyNumber::ParseDyNumberString(text);
    UNIT_ASSERT_C(value, "invalid DyNumber literal " << text);
    return *value;
}

TString StoredJsonDocument(TStringBuf json) {
    const auto value = NBinaryJson::SerializeToBinaryJson(json);
    UNIT_ASSERT_C(std::holds_alternative<NBinaryJson::TBinaryJson>(value), "invalid JSON literal " << json);
    const auto& binaryJson = std::get<NBinaryJson::TBinaryJson>(value);
    return TString(binaryJson.Data(), binaryJson.Size());
}

// Runs the rows through the Parquet exporter itself, so the result is exactly
// the data file of a backup.
TString ExportToParquet(const TVector<TStoredRow>& rows, ui64 rowGroupSize) {
    IExport::TTableColumns columns;
    columns.emplace(1, TUserTable::TUserColumn(NScheme::TTypeInfo(NScheme::NTypeIds::Utf8), "", "key", true));
    columns.emplace(2, TUserTable::TUserColumn(NScheme::TTypeInfo(NScheme::NTypeIds::DyNumber), "", "dyn", false));
    columns.emplace(3, TUserTable::TUserColumn(NScheme::TTypeInfo(NScheme::NTypeIds::JsonDocument), "", "doc", false));

    TParquetExportSettings settings;
    settings.WithColumns(std::move(columns)).WithRowGroupSize(rowGroupSize);
    const auto format = CreateExportDataFormat(std::move(settings));
    UNIT_ASSERT_C(format->ColumnsOrder({1, 2, 3}), format->GetError());

    const auto set = [](NTable::IScan::TRow& row, ui32 pos, const TMaybe<TString>& value) {
        if (value) {
            row.Set(pos, NTable::ECellOp::Set, TCell(value->data(), value->size()));
        } else {
            row.Set(pos, NTable::ECellOp::Null, TCell());
        }
    };

    for (const auto& row : rows) {
        NTable::IScan::TRow scanRow;
        scanRow.Init(3);
        scanRow.Set(0, NTable::ECellOp::Set, TCell(row.Key.data(), row.Key.size()));
        set(scanRow, 1, row.DyNumber);
        set(scanRow, 2, row.JsonDocument);
        UNIT_ASSERT_C(format->Collect(scanRow, Cnull), format->GetError());
    }

    const auto data = format->Flush(/*last=*/true);
    UNIT_ASSERT_C(data, format->GetError());
    return TString(data->Data(), data->Size());
}

std::shared_ptr<arrow::Array> FinishArray(arrow::ArrayBuilder& builder) {
    std::shared_ptr<arrow::Array> array;
    const auto status = builder.Finish(&array);
    UNIT_ASSERT_C(status.ok(), status.ToString());
    return array;
}

template <typename TArrowType>
std::shared_ptr<arrow::Array> MakeNumericArray(
    const std::shared_ptr<arrow::DataType>& type,
    const TVector<typename TArrowType::c_type>& values)
{
    arrow::NumericBuilder<TArrowType> builder(type, arrow::default_memory_pool());
    for (const auto value : values) {
        UNIT_ASSERT(builder.Append(value).ok());
    }
    return FinishArray(builder);
}

std::shared_ptr<arrow::Array> MakeStringArray(const TVector<TString>& values) {
    arrow::StringBuilder builder;
    for (const auto& value : values) {
        UNIT_ASSERT(builder.Append(value.data(), value.size()).ok());
    }
    return FinishArray(builder);
}

std::shared_ptr<arrow::Array> MakeBinaryArray(const TVector<TString>& values) {
    arrow::BinaryBuilder builder;
    for (const auto& value : values) {
        UNIT_ASSERT(builder.Append(value.data(), value.size()).ok());
    }
    return FinishArray(builder);
}

std::shared_ptr<arrow::Array> MakeDecimalArray(const TVector<NYql::NDecimal::TInt128>& values) {
    arrow::FixedSizeBinaryBuilder builder(arrow::fixed_size_binary(sizeof(NYql::NDecimal::TInt128)));
    for (const auto& value : values) {
        UNIT_ASSERT(builder.Append(reinterpret_cast<const char*>(&value)).ok());
    }
    return FinishArray(builder);
}

// The bytes of a value of the array, which is what its cell holds.
TString ValueBytes(const arrow::Array& array, i64 index) {
    switch (array.type_id()) {
    case arrow::Type::STRING:
    case arrow::Type::BINARY: {
        const auto view = static_cast<const arrow::BinaryArray&>(array).GetView(index);
        return TString(view.data(), view.size());
    }
    case arrow::Type::FIXED_SIZE_BINARY: {
        const auto view = static_cast<const arrow::FixedSizeBinaryArray&>(array).GetView(index);
        return TString(view.data(), view.size());
    }
    default: {
        const auto& values = static_cast<const arrow::PrimitiveArray&>(array);
        const size_t width = static_cast<const arrow::FixedWidthType&>(*array.type()).bit_width() / 8;
        return TString(
            reinterpret_cast<const char*>(values.values()->data()) + (array.offset() + index) * width,
            width);
    }
    }
}

// Written the way the exporter does it: default writer properties (Parquet
// format 1.0) and the Arrow schema stored in the file.
TString WriteParquetLikeExporter(
    const std::shared_ptr<arrow::Table>& table,
    i64 rowGroupSize,
    parquet::Compression::type compression = parquet::Compression::UNCOMPRESSED)
{
    auto sink = arrow::io::BufferOutputStream::Create(0).ValueOrDie();

    parquet::WriterProperties::Builder propertiesBuilder;
    propertiesBuilder.compression(compression);
    auto arrowPropertiesBuilder = parquet::ArrowWriterProperties::Builder();
    arrowPropertiesBuilder.store_schema();
    const auto writeStatus = parquet::arrow::WriteTable(
        *table,
        arrow::default_memory_pool(),
        sink,
        rowGroupSize,
        propertiesBuilder.build(),
        arrowPropertiesBuilder.build());
    UNIT_ASSERT_C(writeStatus.ok(), writeStatus.ToString());

    auto buffer = sink->Finish().ValueOrDie();
    return TString(reinterpret_cast<const char*>(buffer->data()), buffer->size());
}

// A table with a Utf8 key and a value column of the given type.
NKikimrSchemeOp::TTableDescription MakeValueTableScheme(NScheme::TTypeId typeId, TMaybe<ui32> pgTypeId) {
    auto scheme = MakeUtf8TableScheme();
    auto& value = *scheme.MutableColumns(1);
    value.SetTypeId(typeId);
    if (pgTypeId) {
        value.MutableTypeInfo()->SetPgTypeId(*pgTypeId);
    }
    return scheme;
}

TString ValueTypeName(const NKikimrSchemeOp::TTableDescription& scheme) {
    const auto& value = scheme.GetColumns(1);
    return NScheme::TypeName(NScheme::TypeInfoModFromProtoColumnType(
        value.GetTypeId(), value.HasTypeInfo() ? &value.GetTypeInfo() : nullptr).TypeInfo);
}

// The data file of the table above: keys k0, k1, ... and the given values.
// With extra, the file has one more column, which the table does not have.
TString BuildKeyValueParquet(
    const std::shared_ptr<arrow::Array>& values,
    i64 rowGroupSize,
    parquet::Compression::type compression = parquet::Compression::UNCOMPRESSED,
    const std::shared_ptr<arrow::Array>& extra = nullptr)
{
    TVector<TString> keys;
    for (i64 i = 0; i < values->length(); ++i) {
        keys.push_back(TStringBuilder() << "k" << i);
    }

    arrow::FieldVector fields;
    arrow::ArrayVector columns;
    if (extra) {
        fields.push_back(arrow::field("extra", extra->type()));
        columns.push_back(extra);
    }
    fields.push_back(arrow::field("key", arrow::utf8()));
    columns.push_back(MakeStringArray(keys));
    fields.push_back(arrow::field("value", values->type()));
    columns.push_back(values);

    return WriteParquetLikeExporter(
        arrow::Table::Make(std::make_shared<arrow::Schema>(std::move(fields)), std::move(columns)),
        rowGroupSize,
        compression);
}

// A column type with restrictions on its values that the Arrow type of the
// column does not carry.
struct TValueCase {
    NScheme::TTypeId TypeId;
    TMaybe<ui32> PgTypeId;
    // Four values as they are in a backup. The arrays differ in the last value
    // only: Valid has one at the limit of what the type allows, Invalid has
    // one that a table of this type must not hold.
    std::shared_ptr<arrow::Array> Valid;
    std::shared_ptr<arrow::Array> Invalid;
};

template <typename TArrowType>
TValueCase MakeNumericCase(
    NScheme::TTypeId typeId,
    const std::shared_ptr<arrow::DataType>& type,
    typename TArrowType::c_type valid,
    typename TArrowType::c_type invalid)
{
    return {
        .TypeId = typeId,
        .PgTypeId = Nothing(),
        .Valid = MakeNumericArray<TArrowType>(type, {0, 1, 2, valid}),
        .Invalid = MakeNumericArray<TArrowType>(type, {0, 1, 2, invalid}),
    };
}

TVector<TValueCase> MakeValueCases() {
    using namespace NYql::NUdf;
    using namespace NYql::NDecimal;

    static constexpr ui32 PgTextTypeId = 25;

    const TString json = R"({"key":"value"})";
    const TString invalidUtf8 = "\xC3\x28"; // the second byte is not a continuation byte

    const auto makeStringCase = [](NScheme::TTypeId typeId, const TString& valid, const TString& invalid,
        TMaybe<ui32> pgTypeId = Nothing())
    {
        return TValueCase{
            .TypeId = typeId,
            .PgTypeId = pgTypeId,
            .Valid = MakeStringArray({valid, valid, valid, valid}),
            .Invalid = MakeStringArray({valid, valid, valid, invalid}),
        };
    };
    const auto makeBinaryCase = [](NScheme::TTypeId typeId, const TString& valid, const TString& invalid) {
        return TValueCase{
            .TypeId = typeId,
            .PgTypeId = Nothing(),
            .Valid = MakeBinaryArray({valid, valid, valid, valid}),
            .Invalid = MakeBinaryArray({valid, valid, valid, invalid}),
        };
    };

    return {
        MakeNumericCase<arrow::UInt16Type>(NScheme::NTypeIds::Date, arrow::uint16(), MAX_DATE - 1, MAX_DATE),
        // uint32 is stored as INT64 and cast back by the importer
        MakeNumericCase<arrow::UInt32Type>(NScheme::NTypeIds::Datetime, arrow::uint32(), MAX_DATETIME - 1, MAX_DATETIME),
        MakeNumericCase<arrow::TimestampType>(NScheme::NTypeIds::Timestamp,
            arrow::timestamp(arrow::TimeUnit::MICRO), MAX_TIMESTAMP - 1, MAX_TIMESTAMP),
        // the exporter writes Interval as int64
        MakeNumericCase<arrow::Int64Type>(NScheme::NTypeIds::Interval, arrow::int64(), MAX_TIMESTAMP - 1, MAX_TIMESTAMP),
        MakeNumericCase<arrow::Int32Type>(NScheme::NTypeIds::Date32, arrow::int32(), MAX_DATE32, MAX_DATE32 + 1),
        MakeNumericCase<arrow::Int64Type>(NScheme::NTypeIds::Datetime64, arrow::int64(), MAX_DATETIME64, MAX_DATETIME64 + 1),
        MakeNumericCase<arrow::Int64Type>(NScheme::NTypeIds::Timestamp64, arrow::int64(), MAX_TIMESTAMP64, MAX_TIMESTAMP64 + 1),
        MakeNumericCase<arrow::Int64Type>(NScheme::NTypeIds::Interval64, arrow::int64(), MAX_INTERVAL64, MAX_INTERVAL64 + 1),
        TValueCase{
            .TypeId = NScheme::NTypeIds::Decimal,
            .PgTypeId = Nothing(),
            .Valid = MakeDecimalArray({0, 1, 2, Nan()}),
            .Invalid = MakeDecimalArray({0, 1, 2, Err()}),
        },
        makeStringCase(NScheme::NTypeIds::Utf8, "valid", invalidUtf8),
        makeStringCase(NScheme::NTypeIds::Json, json, "not-json"),
        makeBinaryCase(NScheme::NTypeIds::Yson, "{key=value}", "{key="),
        // the stored form is binary, the text of a value is not a valid one
        makeBinaryCase(NScheme::NTypeIds::DyNumber, StoredDyNumber("3.14"), "3.14"),
        makeBinaryCase(NScheme::NTypeIds::JsonDocument, StoredJsonDocument(json), json),
        makeStringCase(NScheme::NTypeIds::Pg, "valid", invalidUtf8, PgTextTypeId),
    };
}

struct TImportOutcome {
    TVector<std::pair<TString, TMaybe<TString>>> Rows; // the key and the value cell of every row
    ui64 RequestedBytes = 0; // the bytes of the file the engine has asked for
    TMaybe<TString> Error;
};

// Imports a data file of a table with one key and one value column.
TImportOutcome ImportKeyValueParquet(
    const TEngineFixture& fixture,
    const TString& source,
    ui64 bufferSizeLimit = 1_MB)
{
    auto engine = fixture.MakeEngine(
        EDataFormat::Parquet,
        source,
        /*readBatchSize=*/8_KB,
        /*validateChecksum=*/false,
        ECompressionCodec::None,
        bufferSizeLimit);

    TMemoryPool pool(256);
    TImportOutcome outcome;
    const auto addRow = [&outcome](const TVector<TCell>& keys, const TVector<TCell>& values) -> std::expected<void, TString> {
        UNIT_ASSERT_VALUES_EQUAL(keys.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(values.size(), 1);
        outcome.Rows.emplace_back(
            TString(keys.front().AsBuf()),
            values.front().IsNull() ? Nothing() : MakeMaybe(TString(values.front().AsBuf())));
        return {};
    };
    const auto unexpectedChecksum = [](TStringBuf) {
        UNIT_FAIL("checksum callback was called with validation disabled");
    };

    for (ui32 step = 0; step < 1024; ++step) {
        auto data = engine->GetData(pool, addRow, unexpectedChecksum);
        if (!data) {
            outcome.Error = data.error();
            return outcome;
        }

        switch (data->Status) {
        case IImportS3Engine::EDataStatus::NeedInput: {
            const auto range = engine->NextRange();
            if (!range) {
                outcome.Error = range.error();
                return outcome;
            }
            UNIT_ASSERT(range->Status == IImportS3Engine::ENextRangeStatus::Ready);
            outcome.RequestedBytes += range->Range.Length;
            if (auto result = engine->PutRange(range->Range, Slice(source, range->Range)); !result) {
                outcome.Error = result.error();
                return outcome;
            }
            break;
        }
        case IImportS3Engine::EDataStatus::Ready:
            AssertSuccess(engine->Commit(data->Batch.Id));
            break;
        case IImportS3Engine::EDataStatus::Finished:
            return outcome;
        case IImportS3Engine::EDataStatus::WaitingForCommit:
            UNIT_FAIL("unexpected batch waiting for commit");
        }
    }

    UNIT_FAIL("Parquet import engine did not finish");
    return outcome;
}

struct TBatchedImport {
    TVector<TString> Keys;     // the keys of the imported rows, in the order they came
    TVector<ui64> BatchBytes;  // the cell bytes of the rows of every batch that has rows
    ui64 PeakArrowBytes = 0;   // the most memory Arrow held while rows were emitted
};

// Imports a data file the way the downloader does: every batch the engine
// reports as ready is one upload.
TBatchedImport ImportInBatches(const TEngineFixture& fixture, const TString& source, ui32 readBatchSize) {
    // The buffer limit is set to hold any row group of the tests, uncompressed:
    // they are about how a row group is read, not about its size.
    auto engine = fixture.MakeEngine(
        EDataFormat::Parquet,
        source,
        readBatchSize,
        /*validateChecksum=*/false,
        ECompressionCodec::None,
        /*bufferSizeLimit=*/128_MB);

    auto* arrowPool = arrow::default_memory_pool();
    const i64 arrowBytesBefore = arrowPool->bytes_allocated();

    TMemoryPool pool(256);
    TBatchedImport result;
    ui64 batchBytes = 0;
    const auto addRow = [&](const TVector<TCell>& keys, const TVector<TCell>& values) -> std::expected<void, TString> {
        UNIT_ASSERT_VALUES_EQUAL(keys.size(), 1);
        result.Keys.emplace_back(keys.front().AsBuf());
        for (const auto& cell : keys) {
            batchBytes += cell.Size();
        }
        for (const auto& cell : values) {
            batchBytes += cell.Size();
        }

        const i64 arrowBytes = arrowPool->bytes_allocated() - arrowBytesBefore;
        result.PeakArrowBytes = Max<ui64>(result.PeakArrowBytes, Max<i64>(arrowBytes, 0));
        return {};
    };
    const auto unexpectedChecksum = [](TStringBuf) {
        UNIT_FAIL("checksum callback was called with validation disabled");
    };

    for (ui32 step = 0; step < 1024 * 1024; ++step) {
        auto data = ExtractValue(engine->GetData(pool, addRow, unexpectedChecksum));
        switch (data.Status) {
        case IImportS3Engine::EDataStatus::NeedInput: {
            const auto range = ExtractValue(engine->NextRange());
            UNIT_ASSERT(range.Status == IImportS3Engine::ENextRangeStatus::Ready);
            AssertSuccess(engine->PutRange(range.Range, Slice(source, range.Range)));
            break;
        }
        case IImportS3Engine::EDataStatus::Ready:
            if (batchBytes) {
                result.BatchBytes.push_back(std::exchange(batchBytes, 0));
            }
            AssertSuccess(engine->Commit(data.Batch.Id));
            break;
        case IImportS3Engine::EDataStatus::Finished:
            return result;
        case IImportS3Engine::EDataStatus::WaitingForCommit:
            UNIT_FAIL("unexpected batch waiting for commit");
        }
    }

    UNIT_FAIL("Parquet import engine did not finish");
    return result;
}

// What the batches of a row group must be: every batch takes the rows that
// fit into the byte budget, and a row that does not fit into an empty batch is
// taken alone.
TVector<ui64> FillBatches(const TVector<ui64>& rowBytes, ui64 budget) {
    TVector<ui64> batches;
    ui64 batch = 0;
    bool empty = true;
    for (const ui64 bytes : rowBytes) {
        if (!empty && bytes > budget - Min(batch, budget)) {
            batches.push_back(std::exchange(batch, 0));
            empty = true;
        }
        batch += bytes;
        empty = false;
    }
    if (!empty) {
        batches.push_back(batch);
    }
    return batches;
}

// Imports the values as one row group of a table with a Utf8 key and a Utf8
// value, and checks that its batches are filled to the byte budget.
void CheckBatchesAreFilledToTheBudget(const TVector<TString>& values, ui32 budget) {
    const TString source = BuildKeyValueParquet(MakeStringArray(values), /*rowGroupSize=*/values.size());

    TVector<TString> keys;
    TVector<ui64> rowBytes;
    for (size_t i = 0; i < values.size(); ++i) {
        keys.push_back(TStringBuilder() << "k" << i);
        rowBytes.push_back(keys.back().size() + values[i].size());
    }

    const TEngineFixture fixture;
    const auto imported = ImportInBatches(fixture, source, budget);

    UNIT_ASSERT_VALUES_EQUAL(JoinSeq(",", imported.Keys), JoinSeq(",", keys));
    UNIT_ASSERT_VALUES_EQUAL(JoinSeq(",", imported.BatchBytes), JoinSeq(",", FillBatches(rowBytes, budget)));
}

// Runs an import to its end and returns the error it ends with, if any.
TMaybe<TString> RunImport(IImportS3Engine& engine, const TString& source, const IImportS3Engine::TAddRowFn& addRow) {
    TMemoryPool pool(256);
    const auto unexpectedChecksum = [](TStringBuf) {
        UNIT_FAIL("checksum callback was called with validation disabled");
    };

    for (ui32 step = 0; step < 1024; ++step) {
        const auto data = engine.GetData(pool, addRow, unexpectedChecksum);
        if (!data) {
            return data.error();
        }

        switch (data->Status) {
        case IImportS3Engine::EDataStatus::NeedInput: {
            const auto range = engine.NextRange();
            if (!range) {
                return range.error();
            }
            UNIT_ASSERT(range->Status == IImportS3Engine::ENextRangeStatus::Ready);
            if (auto result = engine.PutRange(range->Range, Slice(source, range->Range)); !result) {
                return result.error();
            }
            break;
        }
        case IImportS3Engine::EDataStatus::Ready:
            AssertSuccess(engine.Commit(data->Batch.Id));
            break;
        case IImportS3Engine::EDataStatus::Finished:
            return Nothing();
        case IImportS3Engine::EDataStatus::WaitingForCommit:
            UNIT_FAIL("unexpected batch waiting for commit");
        }
    }

    UNIT_FAIL("import engine did not finish");
    return Nothing();
}

// Takes the rows up to the one with the key, which it rejects.
IImportS3Engine::TAddRowFn RejectRow(TStringBuf key, TVector<TString>& taken) {
    return [key, &taken](const TVector<TCell>& keys, const TVector<TCell>&) -> std::expected<void, TString> {
        UNIT_ASSERT_VALUES_EQUAL(keys.size(), 1);
        if (keys.front().AsBuf() == key) {
            return std::unexpected("the row is rejected");
        }
        taken.emplace_back(keys.front().AsBuf());
        return {};
    };
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

        UNIT_ASSERT_GT(source.size(), 64_KB);
        UNIT_ASSERT_LT(source.size(), 1_MB);

        TMemoryPool pool(256);
        TVector<TDecodedRow> rows;
        const auto addRow = CaptureRows(rows);
        const auto unexpectedChecksum = [](TStringBuf) {
            UNIT_FAIL("checksum callback was called with validation disabled");
        };

        bool sawFirstRange = false;
        ui32 batches = 0;
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
                // The 8 KB batch budget yields one 24 KB row per batch; the
                // row group's counters arrive with the batch that completes it.
                ++batches;
                UNIT_ASSERT_VALUES_EQUAL(rows.size(), batches);
                if (data.Batch.Checkpoint) {
                    UNIT_ASSERT_VALUES_EQUAL(batches, 4);
                    UNIT_ASSERT_VALUES_EQUAL(data.Batch.ProcessedBytesAfter, source.size());
                    UNIT_ASSERT_VALUES_EQUAL(data.Batch.Rows, 4);
                } else {
                    UNIT_ASSERT_VALUES_EQUAL(data.Batch.ProcessedBytesAfter, 0);
                    UNIT_ASSERT_VALUES_EQUAL(data.Batch.Rows, 0);
                }

                const auto waiting = ExtractValue(engine->GetData(pool, addRow, unexpectedChecksum));
                UNIT_ASSERT(waiting.Status == IImportS3Engine::EDataStatus::WaitingForCommit);
                UNIT_ASSERT_VALUES_EQUAL(waiting.Batch.Id, data.Batch.Id);

                AssertSuccess(engine->Commit(data.Batch.Id));
                break;
            }

            case IImportS3Engine::EDataStatus::Finished:
                UNIT_ASSERT(sawFirstRange);
                UNIT_ASSERT_VALUES_EQUAL(batches, 4);
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

    Y_UNIT_TEST(ParquetReadsUint32StoredAsInt64) {
        // More than one row and values above 2^31: reading the INT64 storage
        // through a 32-bit array would return garbage from the second row on.
        const TVector<ui32> keys = {1, 2, 70000, Max<ui32>()};
        const TString source = BuildUint32KeyParquet(keys);

        TEngineFixture fixture(MakeUint32KeyTableScheme());
        auto engine = fixture.MakeEngine(EDataFormat::Parquet, source, /*readBatchSize=*/8_KB);

        TMemoryPool pool(256);
        TVector<ui32> importedKeys;
        const auto addRow = [&](const TVector<TCell>& rowKeys, const TVector<TCell>&) -> std::expected<void, TString> {
            UNIT_ASSERT_VALUES_EQUAL(rowKeys.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(rowKeys.front().Size(), sizeof(ui32));
            importedKeys.push_back(rowKeys.front().AsValue<ui32>());
            return {};
        };
        const auto unexpectedChecksum = [](TStringBuf) {
            UNIT_FAIL("checksum callback was called with validation disabled");
        };

        bool finished = false;
        for (ui32 step = 0; step < 1024 && !finished; ++step) {
            auto data = ExtractValue(engine->GetData(pool, addRow, unexpectedChecksum));
            switch (data.Status) {
            case IImportS3Engine::EDataStatus::NeedInput: {
                const auto range = ExtractValue(engine->NextRange());
                UNIT_ASSERT(range.Status == IImportS3Engine::ENextRangeStatus::Ready);
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

        UNIT_ASSERT_C(finished, "Parquet import engine did not finish");
        UNIT_ASSERT(importedKeys == keys);
    }

    Y_UNIT_TEST(ParquetRejectsIncompatibleColumnType) {
        const TString source = BuildSmallParquet(); // key is utf8 in the file

        TEngineFixture fixture(MakeUint32KeyTableScheme());
        auto engine = fixture.MakeEngine(EDataFormat::Parquet, source, /*readBatchSize=*/64_KB);

        TMemoryPool pool(256);
        const auto unexpectedRow = [](const TVector<TCell>&, const TVector<TCell>&) -> std::expected<void, TString> {
            UNIT_FAIL("a row was produced from a file with an incompatible schema");
            return {};
        };
        const auto unexpectedChecksum = [](TStringBuf) {
            UNIT_FAIL("checksum callback was called with validation disabled");
        };

        for (ui32 step = 0; step < 64; ++step) {
            auto data = engine->GetData(pool, unexpectedRow, unexpectedChecksum);
            if (!data) {
                UNIT_ASSERT_STRING_CONTAINS(data.error(), "column 'key' has parquet type string, expected uint32");
                return;
            }
            UNIT_ASSERT(data->Status == IImportS3Engine::EDataStatus::NeedInput);

            const auto range = ExtractValue(engine->NextRange());
            UNIT_ASSERT(range.Status == IImportS3Engine::ENextRangeStatus::Ready);
            if (auto result = engine->PutRange(range.Range, Slice(source, range.Range)); !result) {
                UNIT_ASSERT_STRING_CONTAINS(result.error(), "column 'key' has parquet type string, expected uint32");
                return;
            }
        }

        UNIT_FAIL("the incompatible Parquet schema was not rejected");
    }

    Y_UNIT_TEST(ParquetRoundTripsDyNumberAndJsonDocument) {
        // The exporter writes cells in their stored form, so a backup holds
        // DyNumber and JsonDocument in binary (a CSV backup holds their text).
        // They must be imported as they are, not parsed as text.
        const TVector<TStoredRow> exported = {
            {"k1", StoredDyNumber("3.14"), StoredJsonDocument(R"({"key":"value"})")},
            {"k2", Nothing(), Nothing()},
            {"k3", StoredDyNumber("-18"), StoredJsonDocument(R"([1,2,{"a":null}])")},
        };
        const TString source = ExportToParquet(exported, /*rowGroupSize=*/2);

        TEngineFixture fixture(MakeDyNumberJsonDocumentTableScheme());
        auto engine = fixture.MakeEngine(EDataFormat::Parquet, source, /*readBatchSize=*/8_KB);

        TMemoryPool pool(256);
        TVector<TStoredRow> imported;
        const auto addRow = [&](const TVector<TCell>& keys, const TVector<TCell>& values) -> std::expected<void, TString> {
            UNIT_ASSERT_VALUES_EQUAL(keys.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(values.size(), 2);
            const auto stored = [](const TCell& cell) -> TMaybe<TString> {
                return cell.IsNull() ? Nothing() : MakeMaybe(TString(cell.AsBuf()));
            };
            imported.push_back({TString(keys.front().AsBuf()), stored(values[0]), stored(values[1])});
            return {};
        };
        const auto unexpectedChecksum = [](TStringBuf) {
            UNIT_FAIL("checksum callback was called with validation disabled");
        };

        bool finished = false;
        for (ui32 step = 0; step < 1024 && !finished; ++step) {
            auto data = ExtractValue(engine->GetData(pool, addRow, unexpectedChecksum));
            switch (data.Status) {
            case IImportS3Engine::EDataStatus::NeedInput: {
                const auto range = ExtractValue(engine->NextRange());
                UNIT_ASSERT(range.Status == IImportS3Engine::ENextRangeStatus::Ready);
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

        UNIT_ASSERT_C(finished, "Parquet import engine did not finish");
        UNIT_ASSERT_VALUES_EQUAL(imported.size(), exported.size());
        for (size_t i = 0; i < exported.size(); ++i) {
            UNIT_ASSERT_VALUES_EQUAL_C(imported[i].Key, exported[i].Key, "row " << i);
            UNIT_ASSERT_C(imported[i].DyNumber == exported[i].DyNumber,
                "row " << i << ": DyNumber differs from the exported stored value");
            UNIT_ASSERT_C(imported[i].JsonDocument == exported[i].JsonDocument,
                "row " << i << ": JsonDocument differs from the exported stored value");
        }

        // The imported cells are valid stored values, readable by the table.
        UNIT_ASSERT_VALUES_EQUAL(NDyNumber::DyNumberToString(*imported[0].DyNumber), ".314e1");
        UNIT_ASSERT_VALUES_EQUAL(NDyNumber::DyNumberToString(*imported[2].DyNumber), "-.18e2");
        UNIT_ASSERT_VALUES_EQUAL(
            NBinaryJson::SerializeToJson(TStringBuf(*imported[0].JsonDocument)), R"({"key":"value"})");
        UNIT_ASSERT_VALUES_EQUAL(
            NBinaryJson::SerializeToJson(TStringBuf(*imported[2].JsonDocument)), R"([1,2,{"a":null}])");
    }

    // A row is rejected by the one that takes it, which knows the table: a key
    // out of the range of the shard, NULL in a column that is NOT NULL. The
    // parser knows where the row is in the file and adds the place to the error.
    Y_UNIT_TEST(CsvReportsTheLineOfARejectedRow) {
        const TString source = "\"k1\",\"v1\"\n\"k2\",\"v2\"\n\"k3\",\"v3\"\n";
        const TEngineFixture fixture;
        auto engine = fixture.MakeEngine(EDataFormat::YdbDump, source, /*readBatchSize=*/source.size());

        TVector<TString> taken;
        const auto error = RunImport(*engine, source, RejectRow("k2", taken));

        UNIT_ASSERT_C(error, "a rejected row did not stop the import");
        UNIT_ASSERT_VALUES_EQUAL(*error, "the row is rejected on line: \"k2\",\"v2\"");
        UNIT_ASSERT_VALUES_EQUAL(JoinSeq(",", taken), "k1");
    }

    Y_UNIT_TEST(ParquetReportsThePlaceOfARejectedRow) {
        // two row groups of two rows, with the keys from k1 to k4
        const TString source = BuildSmallParquet(/*rowGroupSize=*/2, /*valueSize=*/16);
        const TEngineFixture fixture;
        auto engine = fixture.MakeEngine(EDataFormat::Parquet, source, /*readBatchSize=*/8_KB);

        TVector<TString> taken;
        const auto error = RunImport(*engine, source, RejectRow("k4", taken));

        UNIT_ASSERT_C(error, "a rejected row did not stop the import");
        UNIT_ASSERT_VALUES_EQUAL(*error, "the row is rejected in row 1 of row group 1");
        UNIT_ASSERT_VALUES_EQUAL(JoinSeq(",", taken), "k1,k2,k3");
    }

    Y_UNIT_TEST(ParquetRejectsValuesInvalidForTheColumnType) {
        // The Arrow type of a column does not carry the value restrictions of
        // its YDB type, so a file with a matching schema can still hold values
        // that a table must not.
        TVector<TString> imported; // types whose invalid value was imported
        for (const auto& valueCase : MakeValueCases()) {
            const TEngineFixture fixture(MakeValueTableScheme(valueCase.TypeId, valueCase.PgTypeId));
            const TString typeName = ValueTypeName(fixture.Scheme);

            // Two row groups of two rows: the invalid value is row 1 of row group 1.
            const auto outcome = ImportKeyValueParquet(
                fixture, BuildKeyValueParquet(valueCase.Invalid, /*rowGroupSize=*/2));
            if (!outcome.Error) {
                imported.push_back(typeName);
                continue;
            }

            UNIT_ASSERT_STRING_CONTAINS_C(*outcome.Error,
                TStringBuilder() << "column 'value' has an invalid " << typeName
                    << " value in row 1 of row group 1",
                typeName);
            // The rows before the invalid one reach the sink, the invalid one does not.
            UNIT_ASSERT_VALUES_EQUAL_C(outcome.Rows.size(), 3, typeName);
        }

        UNIT_ASSERT_C(imported.empty(), "invalid values were imported for: " << JoinSeq(", ", imported));
    }

    Y_UNIT_TEST(ParquetAcceptsValuesAtTheLimitsOfTheColumnType) {
        for (const auto& valueCase : MakeValueCases()) {
            const TEngineFixture fixture(MakeValueTableScheme(valueCase.TypeId, valueCase.PgTypeId));
            const TString typeName = ValueTypeName(fixture.Scheme);

            const auto outcome = ImportKeyValueParquet(
                fixture, BuildKeyValueParquet(valueCase.Valid, /*rowGroupSize=*/2));
            UNIT_ASSERT_C(!outcome.Error, typeName << ": " << outcome.Error.GetOrElse(""));

            UNIT_ASSERT_VALUES_EQUAL_C(outcome.Rows.size(), valueCase.Valid->length(), typeName);
            for (size_t i = 0; i < outcome.Rows.size(); ++i) {
                UNIT_ASSERT_VALUES_EQUAL_C(outcome.Rows[i].first, TStringBuilder() << "k" << i, typeName);
                UNIT_ASSERT_C(outcome.Rows[i].second == MakeMaybe(ValueBytes(*valueCase.Valid, i)),
                    typeName << ": the value of row " << i << " differs from the one in the file");
            }
        }
    }

    Y_UNIT_TEST(ParquetFillsBatchesToTheByteBudget) {
        // A batch is one upload, which is one transaction of the shard, so it
        // must stay within the byte budget whatever the file says about the
        // size of its rows.
        static constexpr ui32 Budget = 256_KB;

        // Repeated values are written as a dictionary: the sizes in the
        // metadata of the file are far below the sizes of the decoded rows.
        CheckBatchesAreFilledToTheBudget(TVector<TString>(64, TString(64_KB, 'a')), Budget);

        // Narrow rows followed by wide ones: the rows read so far say nothing
        // about the rows that follow.
        {
            TVector<TString> values(200, TString(16, 'n'));
            for (ui32 i = 0; i < 32; ++i) {
                values.emplace_back(64_KB, static_cast<char>('a' + i % 26));
            }
            CheckBatchesAreFilledToTheBudget(values, Budget);
        }

        // A row wider than the budget is a batch of its own.
        CheckBatchesAreFilledToTheBudget({
            TString(16, 'n'),
            TString(300_KB, 'w'),
            TString(16, 'n'),
            TString(16, 'n'),
            TString(300_KB, 'w'),
        }, Budget);
    }

    Y_UNIT_TEST(ParquetBoundsTheMemoryOfDecodedRows) {
        // The buffer limit of the engine covers the bytes of the file only. The
        // rows decoded from them can take far more: here 64 MiB of rows are in
        // a file of less than 1 MiB.
        static constexpr ui32 Rows = 1024;
        static constexpr ui32 Budget = 256_KB;

        const TString source = BuildKeyValueParquet(
            MakeStringArray(TVector<TString>(Rows, TString(64_KB, 'a'))), /*rowGroupSize=*/Rows);
        UNIT_ASSERT_LT(source.size(), 1_MB);

        const TEngineFixture fixture;
        const auto imported = ImportInBatches(fixture, source, Budget);

        UNIT_ASSERT_VALUES_EQUAL(imported.Keys.size(), Rows);
        UNIT_ASSERT_LT_C(imported.PeakArrowBytes, 8_MB,
            "decoding held " << imported.PeakArrowBytes << " bytes with a budget of " << Budget);
    }

    Y_UNIT_TEST(ArrowAppliesANewBatchSizeToTheNextBatch) {
        // The parser sizes every decoded batch anew. That relies on the reader
        // taking the batch size for each batch, not once when it is opened.
        static constexpr i64 Rows = 100;

        const TString source = BuildKeyValueParquet(
            MakeStringArray(TVector<TString>(Rows, "value")), /*rowGroupSize=*/Rows);
        const auto buffer = std::make_shared<arrow::Buffer>(
            reinterpret_cast<const uint8_t*>(source.data()), source.size());

        std::unique_ptr<parquet::arrow::FileReader> fileReader;
        const auto openStatus = parquet::arrow::OpenFile(
            std::make_shared<arrow::io::BufferReader>(buffer), arrow::default_memory_pool(), &fileReader);
        UNIT_ASSERT_C(openStatus.ok(), openStatus.ToString());

        std::unique_ptr<arrow::RecordBatchReader> batchReader;
        const auto readerStatus = fileReader->GetRecordBatchReader({0}, &batchReader);
        UNIT_ASSERT_C(readerStatus.ok(), readerStatus.ToString());

        i64 left = Rows;
        for (const i64 batchSize : {1, 7, 3, 1000}) {
            fileReader->set_batch_size(batchSize);

            std::shared_ptr<arrow::RecordBatch> batch;
            const auto readStatus = batchReader->ReadNext(&batch);
            UNIT_ASSERT_C(readStatus.ok(), readStatus.ToString());
            UNIT_ASSERT(batch);
            UNIT_ASSERT_VALUES_EQUAL(batch->num_rows(), Min(batchSize, left));
            left -= batch->num_rows();
        }
        UNIT_ASSERT_VALUES_EQUAL(left, 0);
    }

    Y_UNIT_TEST(ParquetResumesFromCommittedRowGroup) {
        // Two row groups of two 24 KB rows each. The 8 KB batch budget splits a
        // row group into two batches, so the counter deferral is observable.
        const TString source = BuildSmallParquet(/*rowGroupSize=*/2);
        UNIT_ASSERT_GT(source.size(), 64_KB);

        for (const bool validateChecksum : {false, true}) {
            TEngineFixture fixture;
            TMemoryPool pool(256);
            TVector<TDecodedRow> rows;
            TString checksumInput;
            const auto addRow = CaptureRows(rows);
            const auto addChecksum = [&](TStringBuf data) {
                UNIT_ASSERT_C(validateChecksum,
                    "checksum callback was called with validation disabled");
                checksumInput.append(data.data(), data.size());
            };

            const auto feed = [&](IImportS3Engine& engine) {
                const auto range = ExtractValue(engine.NextRange());
                UNIT_ASSERT(range.Status == IImportS3Engine::ENextRangeStatus::Ready);
                AssertSuccess(engine.PutRange(range.Range, Slice(source, range.Range)));
            };

            // First attempt: import row group 0 and stop, as if the shard restarted.
            NKikimrBackup::TS3DownloadState checkpoint;
            TString checkpointChecksumInput;
            {
                auto engine = fixture.MakeEngine(
                    EDataFormat::Parquet, source, /*readBatchSize=*/8_KB, validateChecksum);

                ui32 batches = 0;
                bool committedRowGroup = false;
                for (ui32 step = 0; step < 1024 && !committedRowGroup; ++step) {
                    auto data = ExtractValue(engine->GetData(pool, addRow, addChecksum));
                    switch (data.Status) {
                    case IImportS3Engine::EDataStatus::NeedInput:
                        feed(*engine);
                        break;

                    case IImportS3Engine::EDataStatus::Ready: {
                        ++batches;
                        UNIT_ASSERT_VALUES_EQUAL(data.Batch.ProcessedBytesAfter, 0);
                        const auto& parquetState = data.Batch.DownloadStateAfter.GetParquet();
                        if (data.Batch.Checkpoint) {
                            UNIT_ASSERT_VALUES_EQUAL(batches, 2);
                            UNIT_ASSERT_VALUES_EQUAL(data.Batch.Rows, 2);
                            UNIT_ASSERT_GT(data.Batch.DataBytes, 48_KB);
                            UNIT_ASSERT_VALUES_EQUAL(parquetState.GetCommittedRowGroups(), 1);
                            UNIT_ASSERT(!parquetState.GetChecksumComplete());
                            UNIT_ASSERT_VALUES_EQUAL(parquetState.GetChecksumOffset(), checksumInput.size());
                            checkpoint = data.Batch.DownloadStateAfter;
                            checkpointChecksumInput = checksumInput;
                            committedRowGroup = true;
                        } else {
                            // rows of a partially imported row group are not counted yet
                            UNIT_ASSERT_VALUES_EQUAL(batches, 1);
                            UNIT_ASSERT_VALUES_EQUAL(data.Batch.Rows, 0);
                            UNIT_ASSERT_VALUES_EQUAL(data.Batch.DataBytes, 0);
                            UNIT_ASSERT_VALUES_EQUAL(parquetState.GetCommittedRowGroups(), 0);
                        }
                        AssertSuccess(engine->Commit(data.Batch.Id));
                        break;
                    }

                    default:
                        UNIT_FAIL("unexpected engine state before the first row group was committed");
                    }
                }

                UNIT_ASSERT(committedRowGroup);
                UNIT_ASSERT_VALUES_EQUAL(rows.size(), 2);
                UNIT_ASSERT_VALUES_EQUAL(rows[0].Key, "k1");
                UNIT_ASSERT_VALUES_EQUAL(rows[1].Key, "k2");
            }

            // Second attempt: a fresh engine restored from the durable checkpoint
            // (the coordinator restores the checksum state the same way).
            rows.clear();
            checksumInput = checkpointChecksumInput;
            auto engine = fixture.MakeEngine(
                EDataFormat::Parquet, source, /*readBatchSize=*/8_KB, validateChecksum);
            AssertSuccess(engine->RestoreFromState(/*processedBytes=*/0, checkpoint));

            ui64 countedRows = 0;
            bool finished = false;
            for (ui32 step = 0; step < 1024 && !finished; ++step) {
                auto data = ExtractValue(engine->GetData(pool, addRow, addChecksum));
                switch (data.Status) {
                case IImportS3Engine::EDataStatus::NeedInput:
                    feed(*engine);
                    break;

                case IImportS3Engine::EDataStatus::Ready:
                    countedRows += data.Batch.Rows;
                    if (data.Batch.Checkpoint) {
                        UNIT_ASSERT_VALUES_EQUAL(data.Batch.ProcessedBytesAfter, source.size());
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

            UNIT_ASSERT_C(finished, "resumed Parquet import did not finish");
            UNIT_ASSERT_VALUES_EQUAL(countedRows, 2);
            UNIT_ASSERT_VALUES_EQUAL(rows.size(), 2);
            UNIT_ASSERT_VALUES_EQUAL(rows[0].Key, "k3");
            UNIT_ASSERT_VALUES_EQUAL(rows[1].Key, "k4");
            UNIT_ASSERT_VALUES_EQUAL(checksumInput, validateChecksum ? source : TString());
        }
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
        const auto unexpectedRow = [](const TVector<TCell>&, const TVector<TCell>&) -> std::expected<void, TString> {
            UNIT_FAIL("empty Parquet file emitted a row");
            return {};
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
            const auto unexpectedRow = [](const TVector<TCell>&, const TVector<TCell>&) -> std::expected<void, TString> {
                UNIT_FAIL("empty Parquet file emitted a row");
                return {};
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

        const TEngineFixture fixture;
        const auto outcome = ImportKeyValueParquet(fixture, source, BufferSizeLimit);

        UNIT_ASSERT_C(outcome.Error, "a row group above the buffer limit was imported");
        UNIT_ASSERT_STRING_CONTAINS(*outcome.Error, "Parquet row group 0 takes ");
        UNIT_ASSERT_STRING_CONTAINS(*outcome.Error,
            " bytes in the file, the limit is 196608 bytes (RestoreReadBufferSizeLimit)");
        UNIT_ASSERT(outcome.Rows.empty());
        // The footer is enough to reject it: the row group itself is not downloaded.
        UNIT_ASSERT_LE(outcome.RequestedBytes, 64_KB);
    }

    Y_UNIT_TEST(ParquetRejectsARowGroupAboveTheLimit) {
        // A row group above the limit of the buffer, in the file or
        // uncompressed, is rejected by the footer with an error that names it.
        static constexpr ui64 Limit = 128_KB;
        const TString limitText = ", the limit is 131072 bytes (RestoreReadBufferSizeLimit)";
        const TString narrow = "narrow";
        const TEngineFixture fixture;

        // In the file. Two rows a row group: the first one is within the limit,
        // the second one holds 200 KB.
        {
            const auto outcome = ImportKeyValueParquet(
                fixture,
                BuildKeyValueParquet(
                    MakeStringArray({narrow, narrow, TString(100_KB, 'a'), TString(100_KB, 'b')}),
                    /*rowGroupSize=*/2),
                Limit);

            UNIT_ASSERT_C(outcome.Error, "a row group above the limit in the file was imported");
            UNIT_ASSERT_STRING_CONTAINS(*outcome.Error, "Parquet row group 1 takes ");
            UNIT_ASSERT_STRING_CONTAINS(*outcome.Error, " bytes in the file" + limitText);
            // by the footer: no row group is downloaded, the first one included
            UNIT_ASSERT(outcome.Rows.empty());
            UNIT_ASSERT_LE(outcome.RequestedBytes, 64_KB);
        }

        // Uncompressed. The same rows, compressed: they take little in the file.
        {
            const TString source = BuildKeyValueParquet(
                MakeStringArray({narrow, narrow, TString(100_KB, 'a'), TString(100_KB, 'b')}),
                /*rowGroupSize=*/2,
                parquet::Compression::ZSTD);
            UNIT_ASSERT_LT(source.size(), Limit);

            const auto outcome = ImportKeyValueParquet(fixture, source, Limit);

            UNIT_ASSERT_C(outcome.Error, "a row group above the limit when uncompressed was imported");
            UNIT_ASSERT_STRING_CONTAINS(*outcome.Error, "Parquet row group 1 takes ");
            UNIT_ASSERT_STRING_CONTAINS(*outcome.Error, " bytes when uncompressed" + limitText);
            UNIT_ASSERT(outcome.Rows.empty());
            UNIT_ASSERT_LE(outcome.RequestedBytes, 64_KB);
        }

        // A row group below the limit in both forms is imported.
        {
            const auto outcome = ImportKeyValueParquet(
                fixture,
                BuildKeyValueParquet(
                    MakeStringArray({narrow, narrow, TString(50_KB, 'a'), TString(50_KB, 'b')}),
                    /*rowGroupSize=*/2),
                Limit);

            UNIT_ASSERT_C(!outcome.Error, outcome.Error.GetOrElse(""));
            UNIT_ASSERT_VALUES_EQUAL(outcome.Rows.size(), 4);
        }
    }

    Y_UNIT_TEST(ParquetDoesNotLimitTheRowsDecodedFromARowGroup) {
        // A value that repeats is in the file once, as an entry of a
        // dictionary, so the footer says nothing about the rows it decodes to.
        // They are not limited: they are decoded and released in batches.
        static constexpr ui64 Limit = 128_KB;
        const TEngineFixture fixture;

        // 1 MiB of rows
        const TVector<TString> values(16, TString(64_KB, 'a'));
        const TString source = BuildKeyValueParquet(MakeStringArray(values), /*rowGroupSize=*/16);
        UNIT_ASSERT_LT(source.size(), Limit);

        const auto outcome = ImportKeyValueParquet(fixture, source, Limit);

        UNIT_ASSERT_C(!outcome.Error, outcome.Error.GetOrElse(""));
        UNIT_ASSERT_VALUES_EQUAL(outcome.Rows.size(), values.size());
        for (size_t i = 0; i < values.size(); ++i) {
            UNIT_ASSERT_VALUES_EQUAL_C(outcome.Rows[i].first, TStringBuilder() << "k" << i, "row " << i);
            UNIT_ASSERT_C(outcome.Rows[i].second == MakeMaybe(values[i]), "row " << i);
        }
    }

    Y_UNIT_TEST(ParquetDoesNotDownloadTheColumnsTheTableDoesNotHave) {
        // A file may have more columns than the table. They are not decoded,
        // and their bytes are not downloaded or held in the buffer either, so
        // they cannot make a row group too big for it.
        static constexpr ui64 Limit = 128_KB;
        const TEngineFixture fixture;

        TVector<TString> extra;
        for (ui32 i = 0; i < 4; ++i) {
            extra.emplace_back(100_KB, static_cast<char>('a' + i));
        }
        const TVector<TString> values = {"v0", "v1", "v2", "v3"};
        const TString source = BuildKeyValueParquet(
            MakeStringArray(values),
            /*rowGroupSize=*/4,
            parquet::Compression::UNCOMPRESSED,
            MakeStringArray(extra));
        UNIT_ASSERT_GT(source.size(), 400_KB);

        const auto outcome = ImportKeyValueParquet(fixture, source, Limit);

        UNIT_ASSERT_C(!outcome.Error, outcome.Error.GetOrElse(""));
        UNIT_ASSERT_VALUES_EQUAL(outcome.Rows.size(), values.size());
        for (size_t i = 0; i < values.size(); ++i) {
            UNIT_ASSERT_C(outcome.Rows[i].second == MakeMaybe(values[i]), "row " << i);
        }
        // the end of the file, where the footer is looked for, and the two
        // columns of the table
        UNIT_ASSERT_LT_C(outcome.RequestedBytes, 64_KB + 1_KB, outcome.RequestedBytes);
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

        // The checkpoint of a live engine must be the one it has committed
        // last, the place of the checksum included.
        {
            NKikimrBackup::TS3DownloadState other;
            other.MutableParquet()->SetChecksumOffset(1);
            const auto result = engine->RestoreFromState(/*processedBytes=*/0, other);
            UNIT_ASSERT_C(!result, "a checkpoint of another checksum offset was taken");
            UNIT_ASSERT_STRING_CONTAINS(result.error(), "cannot replace the checkpoint");
        }
        {
            NKikimrBackup::TS3DownloadState other;
            other.MutableParquet()->SetChecksumComplete(true);
            const auto result = engine->RestoreFromState(/*processedBytes=*/0, other);
            UNIT_ASSERT_C(!result, "a checkpoint of a complete checksum was taken");
            UNIT_ASSERT_STRING_CONTAINS(result.error(), "cannot replace the checkpoint");
        }

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

// HasBytes() and ReadBytes() are served by a single segment, so a positive
// answer for a range that was loaded in several puts proves those puts were
// merged.
Y_UNIT_TEST_SUITE(TParquetSparseFileTest) {
    Y_UNIT_TEST(MergesTouchingRangesPutInAnyOrder) {
        TParquetSparseFile file(300);
        AssertSuccess(file.PutRange(200, TString(100, 'c')));
        AssertSuccess(file.PutRange(0, TString(100, 'a')));
        UNIT_ASSERT_VALUES_EQUAL(file.BufferedBytes(), 200);
        UNIT_ASSERT(file.HasBytes(0, 100));
        UNIT_ASSERT(file.HasBytes(200, 100));
        UNIT_ASSERT(!file.HasBytes(100, 100));
        UNIT_ASSERT(!file.HasBytes(0, 300));
        UNIT_ASSERT(!file.ReadBytes(50, 100)); // crosses the gap
        UNIT_ASSERT(!file.IsFullyBuffered());

        // Filling the gap bridges both neighbours into one segment.
        AssertSuccess(file.PutRange(100, TString(100, 'b')));
        UNIT_ASSERT_VALUES_EQUAL(file.BufferedBytes(), 300);
        UNIT_ASSERT(file.HasBytes(0, 300));
        UNIT_ASSERT(file.IsFullyBuffered());
        UNIT_ASSERT_VALUES_EQUAL(*file.ReadBytes(0, 300),
            TString(100, 'a') + TString(100, 'b') + TString(100, 'c'));
        UNIT_ASSERT_VALUES_EQUAL(*file.ReadBytes(90, 20), TString(10, 'a') + TString(10, 'b'));
    }

    Y_UNIT_TEST(SkipsAlreadyLoadedPrefixAndKeepsLoadedBytes) {
        TParquetSparseFile file(200);
        AssertSuccess(file.PutRange(0, TString(100, 'a')));

        // A retried or re-routed chunk repeats loaded bytes: they are skipped,
        // not overwritten, and only the new tail is stored.
        AssertSuccess(file.PutRange(50, TString(100, 'b')));
        UNIT_ASSERT_VALUES_EQUAL(file.BufferedBytes(), 150);
        UNIT_ASSERT(file.HasBytes(0, 150));
        UNIT_ASSERT_VALUES_EQUAL(*file.ReadBytes(0, 150), TString(100, 'a') + TString(50, 'b'));

        // A chunk that is entirely loaded is a no-op.
        AssertSuccess(file.PutRange(20, TString(30, 'z')));
        UNIT_ASSERT_VALUES_EQUAL(file.BufferedBytes(), 150);
        UNIT_ASSERT_VALUES_EQUAL(*file.ReadBytes(20, 30), TString(30, 'a'));
    }

    Y_UNIT_TEST(RejectsOverlapWithFollowingSegmentAndPastEof) {
        TParquetSparseFile file(300);
        AssertSuccess(file.PutRange(200, TString(100, 'c')));

        // [150, 250) starts in a gap and runs into the loaded [200, 300).
        auto result = file.PutRange(150, TString(100, 'x'));
        UNIT_ASSERT(!result);
        UNIT_ASSERT_STRING_CONTAINS(result.error(), "overlaps loaded range");
        UNIT_ASSERT_VALUES_EQUAL(file.BufferedBytes(), 100);
        UNIT_ASSERT(!file.HasBytes(150, 50));

        result = file.PutRange(250, TString(100, 'x'));
        UNIT_ASSERT(!result);
        UNIT_ASSERT_STRING_CONTAINS(result.error(), "past the end");
        result = file.PutRange(300, TString(1, 'x'));
        UNIT_ASSERT(!result);
        UNIT_ASSERT_VALUES_EQUAL(file.BufferedBytes(), 100);
        UNIT_ASSERT(!file.HasBytes(299, 2));
        UNIT_ASSERT(!file.ReadBytes(299, 2));
    }

    Y_UNIT_TEST(ClearBeforeKeepsTailAndAccounting) {
        TParquetSparseFile file(400);
        AssertSuccess(file.PutRange(0, TString(100, 'a')));
        AssertSuccess(file.PutRange(100, TString(100, 'b')));
        AssertSuccess(file.PutRange(300, TString(100, 'd')));
        UNIT_ASSERT_VALUES_EQUAL(file.BufferedBytes(), 300);

        file.ClearBefore(150); // cuts the merged [0, 200) segment in the middle
        UNIT_ASSERT_VALUES_EQUAL(file.BufferedBytes(), 150);
        UNIT_ASSERT(!file.HasBytes(100, 50));
        UNIT_ASSERT(file.HasBytes(150, 50));
        UNIT_ASSERT_VALUES_EQUAL(*file.ReadBytes(150, 50), TString(50, 'b'));
        UNIT_ASSERT(file.HasBytes(300, 100));

        file.ClearBefore(250); // boundary inside a gap
        UNIT_ASSERT_VALUES_EQUAL(file.BufferedBytes(), 100);
        UNIT_ASSERT(!file.HasBytes(150, 50));
        UNIT_ASSERT(file.HasBytes(300, 100));

        // Evicted bytes can be loaded again and merge with the kept tail.
        AssertSuccess(file.PutRange(200, TString(100, 'c')));
        UNIT_ASSERT_VALUES_EQUAL(file.BufferedBytes(), 200);
        UNIT_ASSERT(file.HasBytes(200, 200));

        file.Clear();
        UNIT_ASSERT_VALUES_EQUAL(file.BufferedBytes(), 0);
        UNIT_ASSERT(!file.HasBytes(300, 1));
    }
}

} // anonymous namespace
} // namespace NKikimr::NDataShard

#endif // KIKIMR_DISABLE_S3_OPS
