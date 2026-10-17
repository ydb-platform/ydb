#ifndef KIKIMR_DISABLE_S3_OPS

#include <ydb/core/scheme/scheme_type_info.h>
#include <ydb/core/scheme/scheme_types_proto.h>
#include <ydb/core/backup/common/encryption.h>
#include <ydb/core/tx/datashard/export_data_format.h>
#include <ydb/core/tx/datashard/import_s3_engine.h>
#include <ydb/core/tx/datashard/import_data_parser.h>
#include <ydb/core/tx/datashard/import_parquet_s3_file.h>

#include <yql/essentials/public/decimal/yql_decimal.h>
#include <yql/essentials/public/udf/udf_data_type.h>
#include <yql/essentials/types/binary_json/read.h>
#include <yql/essentials/types/binary_json/write.h>
#include <yql/essentials/types/dynumber/dynumber.h>

#include <library/cpp/testing/common/env.h>
#include <library/cpp/testing/unittest/registar.h>

#include <arrow/api.h>
#include <arrow/io/memory.h>
#include <parquet/arrow/reader.h>
#include <parquet/arrow/writer.h>
#include <parquet/file_reader.h>

// For the footers of crafted files; the header does not compile under our warnings.
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
#include <parquet/thrift_internal.h>
#pragma GCC diagnostic pop
#include <contrib/libs/zstd/include/zstd.h>

#include <util/generic/maybe.h>
#include <util/generic/size_literals.h>
#include <util/generic/vector.h>
#include <util/memory/pool.h>
#include <util/stream/null.h>
#include <util/string/builder.h>
#include <util/string/cast.h>
#include <util/string/join.h>
#include <util/system/unaligned_mem.h>

#include <algorithm>
#include <array>
#include <functional>
#include <limits>
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

// Written the way the exporter does it: format 1.0, the Arrow schema stored in the file.
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

// A row of the table above in the stored form, as a scan hands it to the exporter.
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

// Runs the rows through the exporter itself: exactly the data file of a backup.
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

// Written the way the exporter does it: format 1.0, the Arrow schema stored in the file.
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

// The footer as its thrift structure, and where it starts, for crafting files.
parquet::format::FileMetaData ReadFooter(const TString& file, size_t* footerStart) {
    UNIT_ASSERT(file.size() >= 12 && file.EndsWith("PAR1"));
    const ui32 footerLength = ReadUnaligned<ui32>(file.data() + file.size() - 8);
    UNIT_ASSERT(footerLength + 12 <= file.size());
    *footerStart = file.size() - 8 - footerLength;

    parquet::format::FileMetaData metadata;
    ui32 length = footerLength;
    parquet::DeserializeThriftUnencryptedMsg(
        reinterpret_cast<const uint8_t*>(file.data() + *footerStart), &length, &metadata);
    return metadata;
}

// A Parquet file of the given body and footer.
TString WithFooter(TStringBuf body, const parquet::format::FileMetaData& metadata) {
    std::string serialized;
    parquet::ThriftSerializer serializer;
    serializer.SerializeToString(&metadata, &serialized);
    const ui32 length = serialized.size();
    return TStringBuilder() << body << serialized
        << TStringBuf(reinterpret_cast<const char*>(&length), sizeof(length)) << "PAR1";
}

// A Parquet file of the given body and footer bytes.
TString WithFooterBytes(TStringBuf body, TStringBuf footer) {
    const ui32 length = footer.size();
    return TStringBuilder() << body << footer
        << TStringBuf(reinterpret_cast<const char*>(&length), sizeof(length)) << "PAR1";
}

// A number the way thrift's compact protocol writes it.
TString Varint(ui32 value) {
    TString out;
    while (value >= 0x80) {
        out.push_back(static_cast<char>(value | 0x80));
        value >>= 7;
    }
    out.push_back(static_cast<char>(value));
    return out;
}

// The file with its footer changed by patch.
TString PatchFooter(const TString& file, const std::function<void(parquet::format::FileMetaData&)>& patch) {
    size_t footerStart = 0;
    auto metadata = ReadFooter(file, &footerStart);
    patch(metadata);
    return WithFooter(TStringBuf(file).SubStr(0, footerStart), metadata);
}

// The bytes of a column chunk in its file: the dictionary page, when there is
// one, and the data pages.
std::pair<size_t, size_t> ChunkRange(const parquet::format::ColumnChunk& chunk) {
    const auto& meta = chunk.meta_data;
    i64 start = meta.data_page_offset;
    if (meta.__isset.dictionary_page_offset && meta.dictionary_page_offset < start) {
        start = meta.dictionary_page_offset;
    }
    return {static_cast<size_t>(start), static_cast<size_t>(meta.total_compressed_size)};
}

// Values v0, v1, ... of the given prefix.
TVector<TString> SomeValues(TStringBuf prefix, ui32 count) {
    TVector<TString> values;
    for (ui32 i = 0; i < count; ++i) {
        values.push_back(TStringBuilder() << prefix << i);
    }
    return values;
}

// A column type with restrictions on its values that the Arrow type of the
// column does not carry.
struct TValueCase {
    NScheme::TTypeId TypeId;
    TMaybe<ui32> PgTypeId;
    // Four values of a backup; the last one is at the limit of the type (Valid) or beyond
    // it (Invalid).
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

    // Bool is a byte in the file: only 0 and 1 are values of the type.
    const auto makeBoolCase = [](ui8 invalid) {
        return TValueCase{
            .TypeId = NScheme::NTypeIds::Bool,
            .PgTypeId = Nothing(),
            .Valid = MakeNumericArray<arrow::UInt8Type>(arrow::uint8(), {0, 1, 0, 1}),
            .Invalid = MakeNumericArray<arrow::UInt8Type>(arrow::uint8(), {0, 1, 0, invalid}),
        };
    };

    // Decimal(22,9) is 128 bits in the file: the values are those of 22 digits, the
    // infinities and NaN.
    const TInt128 maxDecimal = GetBounds(NScheme::DECIMAL_PRECISION).second - 1;
    const i64 maxInterval = static_cast<i64>(MAX_TIMESTAMP) - 1;
    const auto makeDecimalCase = [&](TInt128 invalid) {
        return TValueCase{
            .TypeId = NScheme::NTypeIds::Decimal,
            .PgTypeId = Nothing(),
            .Valid = MakeDecimalArray({0, maxDecimal, -maxDecimal, Inf(), -Inf(), Nan()}),
            .Invalid = MakeDecimalArray({0, maxDecimal, -maxDecimal, invalid}),
        };
    };

    return {
        makeBoolCase(2),
        makeBoolCase(255),
        MakeNumericCase<arrow::UInt16Type>(NScheme::NTypeIds::Date, arrow::uint16(), MAX_DATE - 1, MAX_DATE),
        // uint32 is stored as INT64 and cast back by the importer
        MakeNumericCase<arrow::UInt32Type>(NScheme::NTypeIds::Datetime, arrow::uint32(), MAX_DATETIME - 1, MAX_DATETIME),
        MakeNumericCase<arrow::TimestampType>(NScheme::NTypeIds::Timestamp,
            arrow::timestamp(arrow::TimeUnit::MICRO), MAX_TIMESTAMP - 1, MAX_TIMESTAMP),
        // the exporter writes Interval as int64
        MakeNumericCase<arrow::Int64Type>(NScheme::NTypeIds::Interval, arrow::int64(), MAX_TIMESTAMP - 1, MAX_TIMESTAMP),
        MakeNumericCase<arrow::Int64Type>(NScheme::NTypeIds::Interval, arrow::int64(), -maxInterval, -maxInterval - 1),
        MakeNumericCase<arrow::Int32Type>(NScheme::NTypeIds::Date32, arrow::int32(), MAX_DATE32, MAX_DATE32 + 1),
        MakeNumericCase<arrow::Int64Type>(NScheme::NTypeIds::Datetime64, arrow::int64(), MAX_DATETIME64, MAX_DATETIME64 + 1),
        MakeNumericCase<arrow::Int64Type>(NScheme::NTypeIds::Timestamp64, arrow::int64(), MAX_TIMESTAMP64, MAX_TIMESTAMP64 + 1),
        MakeNumericCase<arrow::Int64Type>(NScheme::NTypeIds::Interval64, arrow::int64(), MAX_INTERVAL64, MAX_INTERVAL64 + 1),
        MakeNumericCase<arrow::Int64Type>(NScheme::NTypeIds::Interval64, arrow::int64(), -MAX_INTERVAL64, -MAX_INTERVAL64 - 1),
        makeDecimalCase(Err()),
        // one digit more than the column has
        makeDecimalCase(maxDecimal + 1),
        makeDecimalCase(-maxDecimal - 1),
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
    TVector<ui64> BatchBytes;  // what the rows of every batch that has rows take in an upload
    ui64 PeakArrowBytes = 0;   // the most memory Arrow held while rows were emitted
};

// Imports the way the downloader does: every ready batch is one upload. The default
// buffer limit holds any row group of the tests.
TBatchedImport ImportInBatches(
    const TEngineFixture& fixture,
    const TString& source,
    ui32 readBatchSize,
    ui64 bufferSizeLimit = 128_MB)
{
    auto engine = fixture.MakeEngine(
        EDataFormat::Parquet,
        source,
        readBatchSize,
        /*validateChecksum=*/false,
        ECompressionCodec::None,
        bufferSizeLimit);

    auto* arrowPool = arrow::default_memory_pool();
    const i64 arrowBytesBefore = arrowPool->bytes_allocated();

    TMemoryPool pool(256);
    TBatchedImport result;
    ui64 batchBytes = 0;
    const auto addRow = [&](const TVector<TCell>& keys, const TVector<TCell>& values) -> std::expected<void, TString> {
        UNIT_ASSERT_VALUES_EQUAL(keys.size(), 1);
        result.Keys.emplace_back(keys.front().AsBuf());
        // the way the downloader puts a row into an upload
        batchBytes += TSerializedCellVec::Serialize(keys).size() + TSerializedCellVec::Serialize(values).size();

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

// The batches a row group must make: each filled to the byte budget, an oversized row alone.
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

// Imports the values as one row group of a Utf8 key and value and checks the batches
// fill the byte budget. An unset value is NULL.
void CheckBatchesAreFilledToTheBudget(const TVector<TMaybe<TString>>& values, ui32 budget) {
    arrow::StringBuilder builder;
    for (const auto& value : values) {
        UNIT_ASSERT((value ? builder.Append(value->data(), value->size()) : builder.AppendNull()).ok());
    }
    const TString source = BuildKeyValueParquet(FinishArray(builder), /*rowGroupSize=*/values.size());

    // A row in an upload: two cell vectors with a header per cell, NULL included, on top
    // of the cell bytes.
    static constexpr ui64 RowOverhead = 2 * (sizeof(ui16) + sizeof(ui32));

    TVector<TString> keys;
    TVector<ui64> rowBytes;
    for (size_t i = 0; i < values.size(); ++i) {
        keys.push_back(TStringBuilder() << "k" << i);
        rowBytes.push_back(RowOverhead + keys.back().size() + (values[i] ? values[i]->size() : 0));
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

// A file of --test-param parquet_table_size_mib, read through the sparse file and the
// parser the way the engine does; the rows are checked one by one.
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
    const std::shared_ptr<TParquetSparseFile>& destination,
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
    auto sparseFile = std::make_shared<TParquetSparseFile>(source.Data.size());
    const auto footerRange = TParquetSparseFile::FooterTailRange(source.Data.size());
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

    const auto scheme = MakeLargeParquetTableScheme();
    TUserTable::TPtr userTable = new TUserTable(1, scheme, 0);
    const TTableInfo tableInfo(1, userTable);
    auto parser = CreateParquetDataParser(/*bufferSizeLimit=*/0);
    auto configureResult = parser->Configure(tableInfo, scheme);
    UNIT_ASSERT_C(configureResult.has_value(), configureResult.error());
    auto* streamParser = parser.Get();

    // The row groups' ranges are planned the way the engine does it, from the footer the
    // parser has checked and parsed; what the footer tail already holds is not loaded again.
    auto metadataResult = streamParser->OpenMetadata(
        sparseFile->MakeRandomAccessFile(sparseFile, streamParser->GetMemoryPool()));
    UNIT_ASSERT_C(metadataResult.has_value(), metadataResult.error());
    const auto metadata = streamParser->GetFileMetadata();
    UNIT_ASSERT(metadata);
    auto rowGroupRangesResult = sparseFile->PlanColumnChunkRangesByRowGroup(*metadata, streamParser->GetColumnIndices());
    UNIT_ASSERT_C(rowGroupRangesResult.has_value(), rowGroupRangesResult.error());
    ui64 loadedDataBytes = 0;
    for (const auto& rowGroupRanges : *rowGroupRangesResult) {
        for (const auto& range : rowGroupRanges) {
            const ui64 end = Min(range.Offset + range.Length, footerRange.Offset);
            if (range.Offset < end) {
                PutSparseRange(source.Data, sparseFile, range.Offset, end - range.Offset);
                loadedDataBytes += end - range.Offset;
            }
        }
    }
    UNIT_ASSERT_VALUES_EQUAL(loadedDataBytes > 0, usesSparseReads);
    UNIT_ASSERT_VALUES_EQUAL(sparseFile->IsFullyBuffered(), !usesSparseReads);

    auto openResult = streamParser->OpenFile(sparseFile->MakeRandomAccessFile(sparseFile));
    UNIT_ASSERT_C(openResult.has_value(), openResult.error());

    ui64 decodedBytes = 0;
    ui64 decodedRows = 0;
    const IDataParser::TAddRowFn addRow = [&](const TVector<TCell>& keys, const TVector<TCell>& values) -> std::expected<void, TString> {
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
        return {};
    };

    TMemoryPool pool(256);
    ui64 pendingBytes = 0;
    ui64 pendingRows = 0;
    while (true) {
        auto batchResult = streamParser->ProcessNextBatch(pool, addRow, /*maxDataBytes=*/0);
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

    Y_UNIT_TEST(EncryptedCsvTakesAFinishedCheckpointWithoutTheDecryptionState) {
        // The old binary stored a finished direct import as ProcessedBytes = ContentLength
        // with no state; nothing is left to decrypt there, so it is taken.
        const TString source(1024, 'x'); // never read
        const TEngineFixture fixture;
        const auto makeEngine = [&]() {
            TImportS3EngineSettings settings;
            settings.DataFormat = EDataFormat::YdbDump;
            settings.CompressionCodec = ECompressionCodec::None;
            settings.ContentLength = source.size();
            settings.ReadBatchSize = 128;
            settings.BufferSizeLimit = 1_MB;
            settings.EncryptionKey = NBackup::TEncryptionKey(TString(32, 'k'));
            settings.EncryptionIV = NBackup::TEncryptionIV::Generate();
            return ExtractValue(CreateImportS3Engine(settings, fixture.TableInfo, fixture.Scheme));
        };

        {
            auto engine = makeEngine();
            const auto result = engine->RestoreFromState(/*processedBytes=*/1, {});
            UNIT_ASSERT_C(!result, "a checkpoint inside an encrypted file was taken without the decryption state");
            UNIT_ASSERT_STRING_CONTAINS(result.error(), "encrypted CSV checkpoint has no deserializer state");
        }
        {
            auto engine = makeEngine();
            AssertSuccess(engine->RestoreFromState(source.size(), {}));

            TMemoryPool pool(256);
            const auto unexpectedRow = [](const TVector<TCell>&, const TVector<TCell>&) -> std::expected<void, TString> {
                UNIT_FAIL("a finished import emitted a row");
                return {};
            };
            const auto unexpectedChecksum = [](TStringBuf) {
                UNIT_FAIL("a finished import hashed data");
            };
            const auto data = ExtractValue(engine->GetData(pool, unexpectedRow, unexpectedChecksum));
            UNIT_ASSERT(data.Status == IImportS3Engine::EDataStatus::Finished);
        }
    }

    Y_UNIT_TEST(ZstdContinuesThroughEmptyLinesWithinAFrame) {
        // The zstd reader hands out lines before the end of a frame, where there is no
        // checkpoint, and empty lines give no rows: that is still progress. One frame of
        // 256 KiB of empty lines and a row, through a buffer of 128 KiB.
        const TString csv = TString(256_KB, '\n') + "\"k1\",\"v1\"\n";
        const TString source = ZstdCompress(csv);

        const TEngineFixture fixture;
        auto engine = fixture.MakeEngine(
            EDataFormat::YdbDump,
            source,
            /*readBatchSize=*/source.size(),
            /*validateChecksum=*/false,
            ECompressionCodec::Zstd);

        TVector<TString> taken;
        const auto error = RunImport(*engine, source, RejectRow("no such key", taken));

        UNIT_ASSERT_C(!error, error.GetOrElse(""));
        UNIT_ASSERT_VALUES_EQUAL(JoinSeq(",", taken), "k1");
    }

    Y_UNIT_TEST(ZstdDefersCountersUntilRestartableFrameBoundary) {
        // Rows of a frame are emitted as they come but counted when it ends, where a checkpoint
        // is possible. A read that fails in between is retried on the live reader: the second
        // pass checks that nothing is lost or counted twice.
        const TString largeValue = MakePseudoRandomAscii(256_KB);
        const TString csv = TStringBuilder()
            << "\"k0\",\"v0\"\n"
            << "\"k1\",\"" << largeValue << "\"\n";
        const TString source = ZstdCompress(csv);
        UNIT_ASSERT_GT(source.size(), 128_KB);

        for (const bool failReadAfterDeferredBatch : {false, true}) {
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
            bool failedRead = false;
            bool finished = false;
            for (ui32 step = 0; step < 1024 && !finished; ++step) {
                auto data = ExtractValue(engine->GetData(pool, addRow, addChecksum));
                switch (data.Status) {
                case IImportS3Engine::EDataStatus::NeedInput: {
                    const auto range = ExtractValue(engine->NextRange());
                    UNIT_ASSERT(range.Status == IImportS3Engine::ENextRangeStatus::Ready);
                    if (failReadAfterDeferredBatch && sawDeferredBatch && !failedRead) {
                        // the GetObject of this range fails, the reader stays
                        AssertSuccess(engine->FailRange(range.Range));
                        failedRead = true;
                        const auto again = ExtractValue(engine->NextRange());
                        UNIT_ASSERT(again.Status == IImportS3Engine::ENextRangeStatus::Ready);
                        UNIT_ASSERT_VALUES_EQUAL(again.Range.Offset, range.Range.Offset);
                        UNIT_ASSERT_VALUES_EQUAL(again.Range.Length, range.Range.Length);
                    }
                    AssertSuccess(engine->PutRange(range.Range, Slice(source, range.Range)));
                    break;
                }
                case IImportS3Engine::EDataStatus::Ready:
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
                    break;
                case IImportS3Engine::EDataStatus::Finished:
                    finished = true;
                    break;
                case IImportS3Engine::EDataStatus::WaitingForCommit:
                    UNIT_FAIL("unexpected batch waiting for commit");
                }
            }

            UNIT_ASSERT_C(finished, "Zstd import engine did not finish within 1024 state transitions");
            UNIT_ASSERT(sawDeferredBatch);
            UNIT_ASSERT(sawCheckpointBatch);
            UNIT_ASSERT_VALUES_EQUAL(failedRead, failReadAfterDeferredBatch);
            UNIT_ASSERT_VALUES_EQUAL(checksumInput, csv);
            UNIT_ASSERT_VALUES_EQUAL(rows[0].Key, "k0");
            UNIT_ASSERT_VALUES_EQUAL(rows[1].Value, largeValue);
        }
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
        // A backup holds DyNumber and JsonDocument in binary (CSV holds their text): they
        // are imported as they are.
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

    Y_UNIT_TEST(ReportsThePlaceOfARejectedRow) {
        { // CsvReportsTheLineOfARejectedRow
            // A row is rejected by the sink, which knows the table; the parser adds its place
            // in the file.
            const TString source = "\"k1\",\"v1\"\n\"k2\",\"v2\"\n\"k3\",\"v3\"\n";
            const TEngineFixture fixture;
            auto engine = fixture.MakeEngine(EDataFormat::YdbDump, source, /*readBatchSize=*/source.size());

            TVector<TString> taken;
            const auto error = RunImport(*engine, source, RejectRow("k2", taken));

            UNIT_ASSERT_C(error, "a rejected row did not stop the import");
            UNIT_ASSERT_VALUES_EQUAL(*error, "the row is rejected on line: \"k2\",\"v2\"");
            UNIT_ASSERT_VALUES_EQUAL(JoinSeq(",", taken), "k1");
        }

        { // ParquetReportsThePlaceOfARejectedRow
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
    }

    Y_UNIT_TEST(ParquetChecksValuesAgainstTheColumnType) {
        { // ParquetRejectsValuesInvalidForTheColumnType
            // The Arrow type does not carry the restrictions of the YDB type: a matching schema
            // can still hold invalid values.
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

        { // ParquetAcceptsValuesAtTheLimitsOfTheColumnType
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
    }

    Y_UNIT_TEST(ParquetFillsBatchesToTheUploadBudget) {
        { // ParquetFillsBatchesToTheByteBudget
            // A batch is one upload, one transaction: it stays within the budget whatever the
            // file says.
            static constexpr ui32 Budget = 256_KB;

            // Repeated values are written as a dictionary: the sizes in the
            // metadata of the file are far below the sizes of the decoded rows.
            CheckBatchesAreFilledToTheBudget(TVector<TMaybe<TString>>(64, TString(64_KB, 'a')), Budget);

            // Narrow rows followed by wide ones: the rows read so far say nothing
            // about the rows that follow.
            {
                TVector<TMaybe<TString>> values(200, TString(16, 'n'));
                for (ui32 i = 0; i < 32; ++i) {
                    values.emplace_back(TString(64_KB, static_cast<char>('a' + i % 26)));
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

        { // ParquetCountsNullsInTheByteBudget
            // A NULL has no cell bytes but a header in the upload, like every cell: narrow or
            // NULL rows take many times their bytes. The budget is what they take in the upload.
            static constexpr ui32 Budget = 64_KB;

            CheckBatchesAreFilledToTheBudget(TVector<TMaybe<TString>>(50000, Nothing()), Budget);

            // NULL and narrow values mixed
            {
                TVector<TMaybe<TString>> values;
                for (ui32 i = 0; i < 50000; ++i) {
                    values.push_back(i % 3 ? Nothing() : MakeMaybe(TString("v")));
                }
                CheckBatchesAreFilledToTheBudget(values, Budget);
            }
        }
    }

    Y_UNIT_TEST(ParquetLimitsTheMemoryOfDecoding) {
        { // ParquetBoundsTheMemoryOfDecodedRows
            // The buffer limit covers the file's bytes only: here 64 MiB of rows come from a
            // file under 1 MiB.
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

        { // ParquetDecodesWideRowsAfterNarrowOnesWithinTheMemoryLimit
            // A batch is sized by the rows before it: after many narrow rows it is thousands long,
            // and wide rows that follow take far more memory. A repeated value is in the file
            // once, so nothing tells that: 64 MiB of rows in a file under 1 MiB.
            static constexpr ui32 NarrowRows = 8191; // batches of 1, 2, ... 4096 rows
            static constexpr ui32 WideRows = 1024;
            static constexpr ui32 Budget = 256_KB;
            static constexpr ui64 BufferLimit = 8_MB; // decoding gets twice as much

            TVector<TString> values(NarrowRows, "narrow");
            values.insert(values.end(), WideRows, TString(64_KB, 'w'));
            TVector<TString> keys;
            for (size_t i = 0; i < values.size(); ++i) {
                keys.push_back(TStringBuilder() << "k" << i);
            }

            for (const auto compression : {parquet::Compression::UNCOMPRESSED, parquet::Compression::ZSTD}) {
                const TString source = BuildKeyValueParquet(
                    MakeStringArray(values), /*rowGroupSize=*/values.size(), compression);
                UNIT_ASSERT_LT(source.size(), 1_MB);

                const TEngineFixture fixture;

                // With a limit that is out of reach the wide rows are decoded as
                // one batch.
                const auto unlimited = ImportInBatches(fixture, source, Budget, /*bufferSizeLimit=*/1_GB);
                UNIT_ASSERT_VALUES_EQUAL(unlimited.Keys.size(), keys.size());
                UNIT_ASSERT_GT_C(unlimited.PeakArrowBytes, 2 * BufferLimit, unlimited.PeakArrowBytes);

                // With the limit that batch is refused, and the rows are decoded in
                // batches that fit: all of them, in the order of the file.
                const auto imported = ImportInBatches(fixture, source, Budget, BufferLimit);
                UNIT_ASSERT_VALUES_EQUAL(JoinSeq(",", imported.Keys), JoinSeq(",", keys));
                UNIT_ASSERT_VALUES_EQUAL(JoinSeq(",", imported.BatchBytes), JoinSeq(",", unlimited.BatchBytes));
                UNIT_ASSERT_LT_C(imported.PeakArrowBytes, 2 * BufferLimit, imported.PeakArrowBytes);
            }
        }

        { // ParquetFailsWhenARowCannotBeDecodedWithinTheMemoryLimit
            // A row group within the footer's limits can take more than decoding gets, even for
            // one row: a dictionary page with an 840 KB value, decoded twice. The error names
            // the row.
            static constexpr ui64 BufferLimit = 1_MB;

            const TString source = BuildKeyValueParquet(
                MakeStringArray({TString(840_KB, 'a')}), /*rowGroupSize=*/1, parquet::Compression::ZSTD);

            const TEngineFixture fixture;
            const auto outcome = ImportKeyValueParquet(fixture, source, BufferLimit);

            UNIT_ASSERT_C(outcome.Error, "a row that does not fit into the memory limit was imported");
            UNIT_ASSERT_VALUES_EQUAL(*outcome.Error,
                "Parquet row 0 of row group 0 cannot be decoded within 2097152 bytes of memory"
                " (twice RestoreReadBufferSizeLimit)");
            UNIT_ASSERT(outcome.Rows.empty());
        }
    }

    Y_UNIT_TEST(ArrowAppliesANewBatchSizeToTheNextBatch) {
        // Every batch is sized anew, so the reader must take the batch size per batch.
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

    Y_UNIT_TEST(ParquetRejectsARowGroupAboveTheLimit) {
        { // ParquetReportsAnOversizedRowGroup
            static constexpr ui64 BufferSizeLimit = 192_KB;
            const TString source = BuildSmallParquet(/*rowGroupSize=*/4, /*valueSize=*/96_KB);
            UNIT_ASSERT_GT(source.size(), BufferSizeLimit);

            const TEngineFixture fixture;
            const auto outcome = ImportKeyValueParquet(fixture, source, BufferSizeLimit);

            UNIT_ASSERT_C(outcome.Error, "a row group above the buffer limit was imported");
            UNIT_ASSERT_STRING_CONTAINS(*outcome.Error, "Parquet row group 0 takes ");
            UNIT_ASSERT_STRING_CONTAINS(*outcome.Error, " bytes in the file, the limit is ");
            UNIT_ASSERT_STRING_CONTAINS(*outcome.Error, " bytes (RestoreReadBufferSizeLimit less what the footer takes)");
            UNIT_ASSERT(outcome.Rows.empty());
            // The footer is enough to reject it: the row group itself is not downloaded.
            UNIT_ASSERT_LE(outcome.RequestedBytes, 64_KB);
        }

        { // ParquetRejectsARowGroupAboveTheLimit
            // A row group above the limit of the buffer, in the file or
            // uncompressed, is rejected by the footer with an error that names it.
            static constexpr ui64 Limit = 128_KB;
            const TString limitText = ", the limit is "; // of what is left of the buffer after the footer
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
    }

    Y_UNIT_TEST(ParquetDoesNotLimitTheRowsDecodedFromARowGroup) {
        // A repeated value is in the file once, so the footer says nothing about the decoded
        // rows: they are decoded and released in batches.
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
        // Extra columns of the file are neither decoded nor downloaded, so they cannot make
        // a row group too big.
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

    Y_UNIT_TEST(ParquetChecksTheRowCountsOfTheColumns) {
        { // a row group short of column chunks
            // Parquet throws when a column's chunk is missing from a row group; the parser
            // refuses such a footer first.
            const TString source = PatchFooter(
                BuildKeyValueParquet(MakeStringArray(SomeValues("v", 4)), /*rowGroupSize=*/4),
                [](parquet::format::FileMetaData& metadata) {
                    metadata.row_groups[0].columns.resize(1);
                });

            const TEngineFixture fixture;
            const auto outcome = ImportKeyValueParquet(fixture, source);

            UNIT_ASSERT_C(outcome.Error, "a row group short of column chunks was taken");
            UNIT_ASSERT_STRING_CONTAINS(*outcome.Error,
                "Parquet row group 0 has 1 column chunks, the schema has 2 columns");
            UNIT_ASSERT(outcome.Rows.empty());
            UNIT_ASSERT_LE(outcome.RequestedBytes, 64_KB);
        }

        { // ParquetRejectsAFooterWhoseColumnsDisagreeOnRows
            // Every chunk of a row group holds a value per row; a footer that says otherwise
            // is rejected before download.
            const TString source = PatchFooter(
                BuildKeyValueParquet(MakeStringArray(SomeValues("v", 10)), /*rowGroupSize=*/10),
                [](parquet::format::FileMetaData& metadata) {
                    metadata.row_groups[0].columns[1].meta_data.num_values = 6;
                });

            const TEngineFixture fixture;
            const auto outcome = ImportKeyValueParquet(fixture, source);

            UNIT_ASSERT_C(outcome.Error, "a file whose footer has a column short of values was imported");
            UNIT_ASSERT_STRING_CONTAINS(*outcome.Error,
                "Parquet column 'value' has 6 values in row group 0, which has 10 rows");
            UNIT_ASSERT(outcome.Rows.empty());
            UNIT_ASSERT_LE(outcome.RequestedBytes, 64_KB);
        }

        { // ParquetRejectsARowGroupShorterThanItsFooterSays
            // The pages hold six rows, the footer says ten: Arrow reads six and stops, and the
            // row group must not count as complete.
            const TString source = PatchFooter(
                BuildKeyValueParquet(MakeStringArray(SomeValues("v", 6)), /*rowGroupSize=*/6),
                [](parquet::format::FileMetaData& metadata) {
                    metadata.num_rows = 10;
                    metadata.row_groups[0].num_rows = 10;
                    for (auto& column : metadata.row_groups[0].columns) {
                        column.meta_data.num_values = 10;
                    }
                });

            const TEngineFixture fixture;
            const auto outcome = ImportKeyValueParquet(fixture, source);

            UNIT_ASSERT_C(outcome.Error, "a row group shorter than its footer says was imported as complete");
            UNIT_ASSERT_STRING_CONTAINS(*outcome.Error,
                "Parquet row group 0 has 10 rows by its footer, but 6 were read");
            // the six rows are emitted before the end of the row group is met
            UNIT_ASSERT_VALUES_EQUAL(outcome.Rows.size(), 6);
        }

        { // ParquetRejectsAColumnShorterThanTheFirstOne
            // A first column longer than another makes Arrow read out of bounds, so the parser
            // compares the lengths: the key column of ten rows, the value column of six from
            // another file, and the footer still claiming ten.
            const TString ten = BuildKeyValueParquet(MakeStringArray(SomeValues("v", 10)), /*rowGroupSize=*/10);
            const TString six = BuildKeyValueParquet(MakeStringArray(SomeValues("w", 6)), /*rowGroupSize=*/6);

            size_t sixFooter = 0;
            const auto sixMetadata = ReadFooter(six, &sixFooter);
            const auto& sixChunk = sixMetadata.row_groups[0].columns[1];
            const auto [sixStart, sixLength] = ChunkRange(sixChunk);

            size_t tenFooter = 0;
            auto metadata = ReadFooter(ten, &tenFooter);
            auto& chunk = metadata.row_groups[0].columns[1];
            const i64 shift = static_cast<i64>(tenFooter) - static_cast<i64>(sixStart);
            chunk.file_offset = sixChunk.file_offset + shift;
            chunk.meta_data.data_page_offset = sixChunk.meta_data.data_page_offset + shift;
            if (sixChunk.meta_data.__isset.dictionary_page_offset) {
                chunk.meta_data.__set_dictionary_page_offset(sixChunk.meta_data.dictionary_page_offset + shift);
            }
            chunk.meta_data.total_compressed_size = sixChunk.meta_data.total_compressed_size;
            chunk.meta_data.total_uncompressed_size = sixChunk.meta_data.total_uncompressed_size;
            // num_values stays ten: the footer is consistent, the data is not

            const TString source = WithFooter(
                TStringBuilder() << TStringBuf(ten).SubStr(0, tenFooter) << TStringBuf(six).SubStr(sixStart, sixLength),
                metadata);

            const TEngineFixture fixture;
            const auto outcome = ImportKeyValueParquet(fixture, source);

            UNIT_ASSERT_C(outcome.Error, "a row group whose columns differ in length was imported");
            // batches of 1, 2 and 4 rows: the third is where the value column ends
            UNIT_ASSERT_STRING_CONTAINS(*outcome.Error,
                "Parquet column 'value' has 3 rows where column 'key' has 4, from row 3 of row group 0");
            UNIT_ASSERT_VALUES_EQUAL(outcome.Rows.size(), 3);
        }
    }

    Y_UNIT_TEST(ParquetRejectsAMalformedFooter) {
        // The tail is checked before the footer is fetched, and the footer is walked before
        // Arrow parses it: nothing but the tail is downloaded.
        const TString source = BuildKeyValueParquet(MakeStringArray(SomeValues("v", 4)), /*rowGroupSize=*/4);
        size_t footerStart = 0;
        ReadFooter(source, &footerStart);
        const TStringBuf body = TStringBuf(source).SubStr(0, footerStart);
        const TString footer = source.substr(footerStart, source.size() - 8 - footerStart);

        const auto import = [](const TString& file, TStringBuf expected, TStringBuf what) {
            const TEngineFixture fixture;
            const auto outcome = ImportKeyValueParquet(fixture, file);
            UNIT_ASSERT_C(outcome.Error, what << " was taken");
            UNIT_ASSERT_STRING_CONTAINS_C(*outcome.Error, expected, what);
            UNIT_ASSERT_C(outcome.Rows.empty(), what);
            UNIT_ASSERT_LE_C(outcome.RequestedBytes, 64_KB, what);
        };

        { // the magic
            TString file = source;
            file.replace(file.size() - 4, 4, "XXXX");
            import(file, "parquet magic bytes not found in footer", "a file without the magic");
        }

        { // a footer length past the file
            TString file = source;
            const ui32 length = file.size();
            file.replace(file.size() - 8, 4, TStringBuf(reinterpret_cast<const char*>(&length), sizeof(length)));
            import(file, "exceeds file size", "a footer longer than the file");
        }

        { // a list the footer declares but does not hold: above the limit of
          // thrift, and below it
            // the row groups: 0x19 (field 4, a list) 0x1C (one struct); a longer list is 0xFC
            // and a varint
            const size_t header = footer.find("\x19\x1C");
            UNIT_ASSERT(header != TString::npos);
            for (const ui32 declared : {1000001u, 100000u}) {
                TString patched = footer;
                patched.replace(header + 1, 1, TStringBuilder() << '\xFC' << Varint(declared));
                import(WithFooterBytes(body, patched), "failed to parse the parquet footer",
                    TStringBuilder() << "a footer declaring " << declared << " row groups it does not hold");
            }
        }
    }

    Y_UNIT_TEST(ParquetLimitsTheFooter) {
        { // ParquetRejectsAFooterAboveTheLimit
            // A footer is parsed into many times its size, so it has its own limit, an eighth
            // of the buffer, checked before it is downloaded.
            static constexpr ui64 BufferLimit = 1_MB;
            const TString source = PatchFooter(
                BuildKeyValueParquet(MakeStringArray(SomeValues("v", 4)), /*rowGroupSize=*/4),
                [](parquet::format::FileMetaData& metadata) {
                    parquet::format::KeyValue padding;
                    padding.__set_key("padding");
                    padding.__set_value(std::string(200_KB, 'p'));
                    metadata.key_value_metadata.push_back(std::move(padding));
                    metadata.__isset.key_value_metadata = true;
                });
            UNIT_ASSERT_GT(source.size(), 200_KB);

            const TEngineFixture fixture;
            const auto outcome = ImportKeyValueParquet(fixture, source, BufferLimit);

            UNIT_ASSERT_C(outcome.Error, "a footer above the limit was taken");
            UNIT_ASSERT_STRING_CONTAINS(*outcome.Error, "Parquet footer is ");
            UNIT_ASSERT_STRING_CONTAINS(*outcome.Error,
                " bytes, the limit is 131072 bytes (an eighth of RestoreReadBufferSizeLimit)");
            UNIT_ASSERT(outcome.Rows.empty());
            UNIT_ASSERT_LE(outcome.RequestedBytes, 64_KB);
        }

        { // ParquetRejectsAFooterThatTakesTheBuffer
            // A footer within its limit can still take most of the buffer once parsed: 400 row
            // groups of two columns in a few tens of KB, against a buffer of 512 KB.
            static constexpr ui64 BufferLimit = 512_KB;
            const TString source = PatchFooter(
                BuildKeyValueParquet(MakeStringArray(SomeValues("v", 4)), /*rowGroupSize=*/4),
                [](parquet::format::FileMetaData& metadata) {
                    auto& rowGroup = metadata.row_groups[0];
                    rowGroup.num_rows = 0;
                    for (auto& column : rowGroup.columns) {
                        column.meta_data.num_values = 0;
                        column.meta_data.__isset.statistics = false;
                        column.meta_data.__isset.encoding_stats = false;
                    }
                    metadata.row_groups.resize(400, rowGroup);
                    metadata.num_rows = 0;
                });
            UNIT_ASSERT_LT(source.size(), 64_KB);

            const TEngineFixture fixture;
            const auto outcome = ImportKeyValueParquet(fixture, source, BufferLimit);

            UNIT_ASSERT_C(outcome.Error, "a footer that takes the buffer was taken");
            UNIT_ASSERT_STRING_CONTAINS(*outcome.Error, "Parquet footer takes about ");
            UNIT_ASSERT_STRING_CONTAINS(*outcome.Error,
                " bytes in memory, the limit is 524288 bytes (RestoreReadBufferSizeLimit)");
            UNIT_ASSERT(outcome.Rows.empty());
            UNIT_ASSERT_LE(outcome.RequestedBytes, 64_KB);
        }

        { // ParquetChecksTheFooterBeforeParsingIt
            // Thrift resizes a list before reading it, and a chunk is 3 bytes in the file: the
            // footer is walked before it is parsed. The two chunks of the row group are declared
            // to be many more.
            static constexpr ui64 BufferLimit = 1_MB;
            // the row groups: 0x19 0x1C (one struct); the row group's chunks: 0x19 0x2C (two
            // structs); a longer list is 0xFC and a varint
            const auto declaring = [](const TString& file, const std::function<ui32(size_t)>& chunks) {
                size_t footerStart = 0;
                ReadFooter(file, &footerStart);
                const TStringBuf body = TStringBuf(file).SubStr(0, footerStart);
                TString footer = file.substr(footerStart, file.size() - 8 - footerStart);
                const size_t header = footer.find("\x19\x1C\x19\x2C");
                UNIT_ASSERT(header != TString::npos);
                footer.replace(header + 3, 1, TStringBuilder() << '\xFC' << Varint(chunks(footer.size())));
                return WithFooterBytes(body, footer);
            };
            const auto import = [](const TString& file, TStringBuf expected, ui64 requestedAtMost, TStringBuf what) {
                const TEngineFixture fixture;
                const auto outcome = ImportKeyValueParquet(fixture, file, BufferLimit);
                UNIT_ASSERT_C(outcome.Error, what << " was taken");
                UNIT_ASSERT_STRING_CONTAINS_C(*outcome.Error, expected, what);
                UNIT_ASSERT_C(outcome.Rows.empty(), what);
                UNIT_ASSERT_LE_C(outcome.RequestedBytes, requestedAtMost, what);
            };
            const TString source = BuildKeyValueParquet(MakeStringArray(SomeValues("v", 4)), /*rowGroupSize=*/4);

            // more chunks than the footer has bytes: refused by thrift's limit
            // on a list, which is the footer's length here
            import(declaring(source, [](size_t) { return 1000000u; }),
                "failed to parse the parquet footer", 64_KB,
                "a footer declaring a million column chunks");

            // as many chunks as the footer has bytes: within thrift's limit,
            // but more than the bytes that follow the list hold
            import(declaring(source, [](size_t footerBytes) { return static_cast<ui32>(footerBytes); }),
                "Parquet footer declares a list of ", 64_KB,
                "a footer declaring more column chunks than its bytes hold");

            // chunks that would take the buffer once parsed, in a footer padded
            // after the row groups so that their count is within its bytes
            const TString padded = PatchFooter(source, [](parquet::format::FileMetaData& metadata) {
                parquet::format::KeyValue padding;
                padding.__set_key("padding");
                padding.__set_value(std::string(100_KB, 'p'));
                metadata.key_value_metadata.push_back(std::move(padding));
                metadata.__isset.key_value_metadata = true;
            });
            import(declaring(padded, [](size_t) { return 50000u; }),
                "Parquet footer takes about ", 256_KB,
                "a footer declaring column chunks that take the buffer");
        }

        { // ParquetCountsTheListsInsideAChunk
            // The lists inside a chunk count too: a path of twenty thousand parts per chunk,
            // 40 KB of footer against a buffer of 512 KB.
            static constexpr ui64 BufferLimit = 512_KB;
            const TString source = PatchFooter(
                BuildKeyValueParquet(MakeStringArray(SomeValues("v", 4)), /*rowGroupSize=*/4),
                [](parquet::format::FileMetaData& metadata) {
                    for (auto& column : metadata.row_groups[0].columns) {
                        column.meta_data.path_in_schema.assign(20000, std::string());
                    }
                });
            UNIT_ASSERT_LT(source.size(), 64_KB);

            const TEngineFixture fixture;
            const auto outcome = ImportKeyValueParquet(fixture, source, BufferLimit);

            UNIT_ASSERT_C(outcome.Error, "a footer with huge lists inside its chunks was taken");
            UNIT_ASSERT_STRING_CONTAINS(*outcome.Error, "Parquet footer takes about ");
            UNIT_ASSERT(outcome.Rows.empty());
            UNIT_ASSERT_LE(outcome.RequestedBytes, 64_KB);
        }

        { // ParquetFooterEstimateCoversTheThriftStructs
            // The estimate covers at least the thrift structures.
            UNIT_ASSERT_GE(ParquetFooterBytesPerColumnChunk, sizeof(parquet::format::ColumnChunk));
            UNIT_ASSERT_GE(ParquetFooterBytesPerRowGroup, sizeof(parquet::format::RowGroup));
            UNIT_ASSERT_GE(ParquetFooterBytesPerColumn, sizeof(parquet::format::SchemaElement));
        }
    }

    Y_UNIT_TEST(ParquetLimitsSchemaNesting) {
        { // ParquetRejectsASchemaNestedTooDeep
            // Arrow builds the schema tree by recursion; the depth is checked on the flat list
            // first. The root, 40 groups of one child each, and a leaf.
            const TString source = PatchFooter(
                BuildKeyValueParquet(MakeStringArray(SomeValues("v", 4)), /*rowGroupSize=*/4),
                [](parquet::format::FileMetaData& metadata) {
                    const auto leaf = metadata.schema.back();
                    metadata.schema.clear();
                    for (ui32 level = 0; level < 41; ++level) {
                        parquet::format::SchemaElement group;
                        group.__set_name(level ? TStringBuilder() << "g" << level : TString("schema"));
                        group.__set_num_children(1);
                        metadata.schema.push_back(std::move(group));
                    }
                    metadata.schema.push_back(leaf);
                });

            const TEngineFixture fixture;
            const auto outcome = ImportKeyValueParquet(fixture, source);

            UNIT_ASSERT_C(outcome.Error, "a schema nested too deep was taken");
            UNIT_ASSERT_STRING_CONTAINS(*outcome.Error, "Parquet schema is nested more than 32 levels deep");
            UNIT_ASSERT(outcome.Rows.empty());
        }

        { // ParquetIgnoresANestedColumnTheTableDoesNotHave
            // A nested column next to the table's is within the limit and ignored like any
            // extra column.
            const TVector<TString> values = SomeValues("v", 4);
            auto inner = arrow::StructArray::Make(
                {MakeNumericArray<arrow::Int32Type>(arrow::int32(), {1, 2, 3, 4})}, {"b"}).ValueOrDie();
            auto nested = arrow::StructArray::Make({inner}, {"a"}).ValueOrDie();
            TVector<TString> keys;
            for (size_t i = 0; i < values.size(); ++i) {
                keys.push_back(TStringBuilder() << "k" << i);
            }
            const auto schema = arrow::schema({
                arrow::field("key", arrow::utf8()),
                arrow::field("value", arrow::utf8()),
                arrow::field("nested", nested->type()),
            });
            const TString source = WriteParquetLikeExporter(
                arrow::Table::Make(schema, {MakeStringArray(keys), MakeStringArray(values), nested}),
                /*rowGroupSize=*/4);

            const TEngineFixture fixture;
            const auto outcome = ImportKeyValueParquet(fixture, source);

            UNIT_ASSERT_C(!outcome.Error, outcome.Error.GetOrElse(""));
            UNIT_ASSERT_VALUES_EQUAL(outcome.Rows.size(), values.size());
            for (size_t i = 0; i < values.size(); ++i) {
                UNIT_ASSERT_C(outcome.Rows[i].second == MakeMaybe(values[i]), "row " << i);
            }
        }
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

        // The checkpoint of a live engine must be the one it committed last, checksum place
        // included.
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

    Y_UNIT_TEST(ParquetRoundTripsAFileOfTheConfiguredSize) {
        const ui64 targetBytes = GetLargeParquetTableSize();
        const auto parquet = BuildLargeParquetData(targetBytes);

        Cerr << "Large parquet round trip: logical bytes=" << parquet.LogicalBytes
            << ", serialized bytes=" << parquet.Data.size()
            << ", rows=" << parquet.Rows
            << ", row groups=" << parquet.RowGroups << Endl;

        CheckLargeParquetRoundTrip(parquet);
    }
}

// A range served by one segment proves the puts that loaded it were merged.
Y_UNIT_TEST_SUITE(TParquetSparseFileTest) {
    Y_UNIT_TEST(ReadAtCopiesIntoThePoolOnce) {
        // Arrow's read of the sparse file is one copy in the given pool, where the decoding
        // limit counts it.
        const TString content = MakePseudoRandomAscii(300);
        auto file = std::make_shared<TParquetSparseFile>(content.size());
        AssertSuccess(file->PutRange(100, content.substr(100, 200)));

        arrow::ProxyMemoryPool pool(arrow::default_memory_pool());
        const auto reader = file->MakeRandomAccessFile(file, &pool);

        auto read = reader->ReadAt(150, 100);
        UNIT_ASSERT_C(read.ok(), read.status().ToString());
        UNIT_ASSERT_VALUES_EQUAL(TStringBuf(reinterpret_cast<const char*>((*read)->data()), (*read)->size()),
            TStringBuf(content).SubStr(150, 100));
        // Arrow rounds allocations up to 64 bytes: the pool holds the buffer's capacity.
        UNIT_ASSERT_VALUES_EQUAL(pool.bytes_allocated(), (*read)->capacity());
        UNIT_ASSERT_GE((*read)->capacity(), 100);
        UNIT_ASSERT_LT((*read)->capacity(), 100 + 64);

        read->reset();
        UNIT_ASSERT_VALUES_EQUAL(pool.bytes_allocated(), 0);

        // a range that is not loaded is an error, not a read of something else
        const auto missing = reader->ReadAt(50, 100);
        UNIT_ASSERT(!missing.ok());
        UNIT_ASSERT_VALUES_EQUAL(pool.bytes_allocated(), 0);
    }

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
