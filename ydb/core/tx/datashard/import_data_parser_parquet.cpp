#ifndef KIKIMR_DISABLE_S3_OPS

#include "import_data_parser.h"

#include <ydb/core/formats/arrow/arrow_helpers.h>
#include <ydb/core/formats/arrow/converter.h>
#include <ydb/core/io_formats/cell_maker/cell_maker.h>
#include <ydb/core/scheme/scheme_tablecell.h>
#include <ydb/core/scheme/scheme_types_proto.h>

#include <yql/essentials/parser/pg_wrapper/interface/type_desc.h>
#include <yql/essentials/public/decimal/yql_decimal.h>
#include <yql/essentials/types/binary_json/read.h>
#include <yql/essentials/types/dynumber/dynumber.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/compute/cast.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/compute/exec.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/io/memory.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/memory_pool.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/record_batch.h>
#include <contrib/libs/apache/arrow/cpp/src/parquet/arrow/reader.h>
#include <contrib/libs/apache/arrow/cpp/src/parquet/file_reader.h>

#include <util/generic/size_literals.h>
#include <util/string/builder.h>

#include <numeric>

namespace NKikimr::NDataShard {

namespace {

struct TColumnMeta {
    TString Name;
    NScheme::TTypeInfo TypeInfo;
    std::shared_ptr<arrow::DataType> ArrowType;
    ui32 KeyOrder = Max<ui32>();
};

// Checks the restrictions a YDB type puts on its values and its Arrow type
// does not carry: the range of a date, well-formed JSON and so on. CSV import
// enforces them by parsing the text of a value and by CheckCellValue(). A
// Parquet backup holds cells in their stored form, so the types that
// CheckCellValue() leaves to the text parser are checked here in that form.
bool IsValidCellValue(const TCell& cell, const NScheme::TTypeInfo& typeInfo) {
    if (cell.IsNull()) {
        return true;
    }

    switch (typeInfo.GetTypeId()) {
    case NScheme::NTypeIds::Bool:
        // a byte in the file, which can hold more than false and true
        return cell.AsValue<ui8>() <= 1;
    case NScheme::NTypeIds::Decimal: {
        // 128 bits in the file, which can hold more digits than the column has
        const auto value = cell.AsValue<NYql::NDecimal::TInt128>();
        return NYql::NDecimal::IsNan(value) || NYql::NDecimal::IsInf(value)
            || NYql::NDecimal::IsNormal(value, typeInfo.GetDecimalType().GetPrecision());
    }
    case NScheme::NTypeIds::DyNumber:
        return NDyNumber::IsValidDyNumber(cell.AsBuf());
    case NScheme::NTypeIds::JsonDocument:
        return NBinaryJson::IsValidBinaryJson(cell.AsBuf());
    case NScheme::NTypeIds::Pg:
        return !NPg::PgNativeBinaryValidate(cell.AsBuf(), typeInfo.GetPgTypeDesc());
    default:
        return NFormats::CheckCellValue(cell, typeInfo);
    }
}

// Splits the converter's flat cell row into keys (in key order) and values (in
// scheme order) and forwards them to the engine's addRow. A row with a value
// that is invalid for its column is not forwarded, and neither is any row
// after it or after a row that addRow has rejected: the import fails.
//
// The cells are borrowed from the converter (see IRowWriter::AddRow) and are
// not copied here: IDataParser::TAddRowFn carries the same borrowed-cells
// contract as for CSV, and both sinks (TUploadRowsRequestBuilder serializes,
// TDirectPartWriter encodes into pages) consume the row synchronously inside
// the call. addRow must never defer the cells.
class TImportParquetRowWriter final : public NArrow::IRowWriter {
public:
    TImportParquetRowWriter(
        const IDataParser::TAddRowFn& addRow,
        const TVector<TColumnMeta>& columnMeta,
        ui32 keyCount)
        : AddRowFn(addRow)
        , ColumnMeta(columnMeta)
        , KeyCount(keyCount)
    {
        Values.reserve(ColumnMeta.size() - KeyCount);
    }

    void AddRow(const TConstArrayRef<TCell>& cells) override {
        Y_ENSURE(cells.size() == ColumnMeta.size(),
            "parquet row has " << cells.size() << " cells, expected " << ColumnMeta.size());

        if (Error) {
            return; // the converter has no way to stop early
        }

        for (size_t i = 0; i < cells.size(); ++i) {
            if (!IsValidCellValue(cells[i], ColumnMeta[i].TypeInfo)) {
                Error = TStringBuilder() << "column '" << ColumnMeta[i].Name << "' has an invalid "
                    << NScheme::TypeName(ColumnMeta[i].TypeInfo) << " value";
                return;
            }
        }

        // kept across rows, so that a row costs no allocation
        Keys.assign(KeyCount, TCell());
        Values.clear();

        ui64 rowBytes = 0;
        for (size_t i = 0; i < cells.size(); ++i) {
            const auto& cell = cells[i];
            rowBytes += cell.Size();
            if (ColumnMeta[i].KeyOrder != Max<ui32>()) {
                Keys[ColumnMeta[i].KeyOrder] = cell;
            } else {
                Values.push_back(cell);
            }
        }

        if (auto added = AddRowFn(Keys, Values); !added) {
            Error = std::move(added.error());
            return;
        }
        PendingBytes += rowBytes;
        ++PendingRows;
    }

    IDataParser::TParsedData GetParsedData() const {
        return {
            .DataBytes = PendingBytes,
            .Rows = PendingRows,
        };
    }

    // Set once a row has an invalid value or is rejected. That row is the one
    // after the rows counted by GetParsedData().
    const TMaybe<TString>& GetError() const {
        return Error;
    }

private:
    const IDataParser::TAddRowFn& AddRowFn;
    const TVector<TColumnMeta>& ColumnMeta;
    const ui32 KeyCount;
    TVector<TCell> Keys;
    TVector<TCell> Values;
    ui64 PendingBytes = 0;
    ui64 PendingRows = 0;
    TMaybe<TString> Error;
};

// The Arrow writer stores some types differently from how it reads them back.
// With Parquet format 1.0 (the writer default, used by the exporter) there is
// no unsigned 32-bit annotation, so uint32 (YDB Uint32, Datetime) is written
// as INT64 and comes back as int64. Such columns are cast back before
// conversion; any other mismatch is an error.
bool IsWriterCoercion(const arrow::DataType& fileType, const arrow::DataType& expectedType) {
    return fileType.id() == arrow::Type::INT64 && expectedType.id() == arrow::Type::UINT32;
}

// The bytes the rows of a decoded batch take in an upload. A row is uploaded
// as two serialized cell vectors, the key and the value, where every cell has
// a header, a NULL as well: that is rowOverhead, the same for every row. The
// rest is the cell bytes, that is what the converter emits. Conversions are
// off, so a cell is the Arrow value as it is: the width of the type for a
// fixed-width column, the length of the value for a string or a binary one,
// and nothing for a null.
class TRowSizes {
public:
    TRowSizes(const arrow::RecordBatch& batch, ui64 rowOverhead)
        : RowOverhead(rowOverhead)
    {
        Columns.reserve(batch.num_columns());
        for (int i = 0; i < batch.num_columns(); ++i) {
            TColumn column{.Array = batch.column(i)};
            switch (column.Array->type_id()) {
            case arrow::Type::STRING:
            case arrow::Type::BINARY:
                column.Binary = static_cast<const arrow::BinaryArray*>(column.Array.get());
                break;
            default: {
                const auto* type = dynamic_cast<const arrow::FixedWidthType*>(column.Array->type().get());
                Y_ENSURE(type, "parquet column has an unexpected type " << column.Array->type()->ToString());
                column.Width = type->bit_width() / 8;
                break;
            }
            }
            Columns.push_back(std::move(column));
        }
    }

    ui64 RowBytes(i64 row) const {
        ui64 bytes = RowOverhead;
        for (const auto& column : Columns) {
            if (column.Array->IsNull(row)) {
                continue;
            }
            bytes += column.Binary ? column.Binary->value_length(row) : column.Width;
        }
        return bytes;
    }

private:
    struct TColumn {
        std::shared_ptr<arrow::Array> Array;
        const arrow::BinaryArray* Binary = nullptr; // set for a variable-width column
        ui64 Width = 0; // the cell bytes of a fixed-width column
    };

    const ui64 RowOverhead;
    TVector<TColumn> Columns;
};

// The memory the arrays of a decoded batch hold.
ui64 DecodedBytes(const arrow::RecordBatch& batch) {
    ui64 bytes = 0;
    for (int i = 0; i < batch.num_columns(); ++i) {
        for (const auto& buffer : batch.column_data(i)->buffers) {
            if (buffer) {
                bytes += buffer->capacity();
            }
        }
    }
    return bytes;
}

// The memory Arrow takes to read a file: the pages it uncompresses, the
// dictionaries and the rows it decodes. The file does not bound it: a page
// states its own size, and a value of a dictionary is decoded for every row
// that has it. So it is bounded here: an allocation above the limit is
// refused, which fails the read that needs it.
class TDecodeMemoryPool final : public arrow::MemoryPool {
public:
    explicit TDecodeMemoryPool(ui64 limit)
        : Limit(limit)
    {
    }

    arrow::Status Allocate(int64_t size, uint8_t** out) override {
        if (!Fits(size)) {
            return Refuse();
        }
        ARROW_RETURN_NOT_OK(Pool->Allocate(size, out));
        Allocated += size;
        return arrow::Status::OK();
    }

    arrow::Status Reallocate(int64_t oldSize, int64_t newSize, uint8_t** ptr) override {
        if (newSize > oldSize && !Fits(newSize - oldSize)) {
            return Refuse();
        }
        ARROW_RETURN_NOT_OK(Pool->Reallocate(oldSize, newSize, ptr));
        Allocated += newSize - oldSize;
        return arrow::Status::OK();
    }

    void Free(uint8_t* buffer, int64_t size) override {
        Pool->Free(buffer, size);
        Allocated -= size;
    }

    int64_t bytes_allocated() const override {
        return Allocated;
    }

    std::string backend_name() const override {
        return Pool->backend_name();
    }

    ui64 GetLimit() const {
        return Limit;
    }

    // Whether an allocation has been refused since the last reset.
    bool HasRefused() const {
        return Refused;
    }

    void ResetRefused() {
        Refused = false;
    }

private:
    bool Fits(int64_t size) const {
        return !Limit || static_cast<ui64>(Allocated) + static_cast<ui64>(size) <= Limit;
    }

    arrow::Status Refuse() {
        Refused = true;
        return arrow::Status::OutOfMemory("decoding takes more than ", Limit, " bytes of memory");
    }

private:
    arrow::MemoryPool* const Pool = arrow::default_memory_pool();
    const ui64 Limit; // 0 = no limit
    int64_t Allocated = 0;
    bool Refused = false;
};

struct TParquetFileSession {
    // The first one, so that it outlives everything that holds its memory.
    std::unique_ptr<TDecodeMemoryPool> Memory;
    std::shared_ptr<arrow::io::RandomAccessFile> Source;
    std::unique_ptr<parquet::arrow::FileReader> FileReader;
    std::vector<int> ColumnIndices; // parquet leaf columns to decode, in scheme order
    std::vector<std::pair<std::string, std::shared_ptr<arrow::DataType>>> CastColumns; // see IsWriterCoercion
    std::vector<int> RowGroups; // the row groups BatchReader reads, in that order
    std::unique_ptr<arrow::RecordBatchReader> BatchReader;
    std::shared_ptr<arrow::RecordBatch> HeldBatch; // rows [HeldOffset, num_rows) not yet emitted
    TMaybe<TRowSizes> HeldSizes; // the sizes of the rows of HeldBatch
    i64 HeldOffset = 0;
    ui64 RowsRead = 0; // rows of the open row groups that have been emitted
    i64 BatchRows = 1; // the rows the next batch is decoded with, see NextBatchRows()
    i64 BatchRowsLimit = Max<i64>(); // lowered by ReopenWithSmallerBatches()
    TVector<i64> DecodedBatches; // the rows of every batch decoded from the open row groups
};

class TParquetDataParser final : public IParquetStreamParser {
    // A row group is decoded in batches, so that the memory of the decoded
    // rows does not depend on how many rows it has or how well they are
    // packed. Nothing in the file tells the size of the decoded rows (the
    // sizes in its metadata are those of the encoded pages), so every batch is
    // sized to a byte target by the rows of the one before it:
    //  - the first batch of a row group is a single row;
    //  - a batch is at most twice as long as the one before it, so a run of
    //    wider rows is met by a batch of a limited length.
    // The byte target is what a batch is expected to take, not a bound: rows
    // far wider than those before them make a batch that takes far more. The
    // bound is the memory limit of the decoding, see TDecodeMemoryPool and
    // ReopenWithSmallerBatches().
    static constexpr i64 MaxBatchRows = 64 * 1024;
    // The byte target of a batch when the caller sets no byte budget.
    static constexpr ui64 DefaultDecodeBytes = 8_MB;

    static i64 NextBatchRows(ui64 targetBytes, ui64 decodedBytes, i64 decodedRows) {
        const ui64 rows = static_cast<ui64>(decodedRows);
        const ui64 rowBytes = Max<ui64>((decodedBytes + rows - 1) / rows, 1);
        const ui64 fitting = Max<ui64>(targetBytes / rowBytes, 1);
        return static_cast<i64>(Min<ui64>(fitting, Min<ui64>(2 * rows, MaxBatchRows)));
    }

    struct TFittingRows {
        i64 Rows = 0;
        ui64 Bytes = 0;
    };

    // The rows, out of count rows starting at offset, that fit into the byte
    // budget. A row that does not fit into an empty batch is taken alone: that
    // is the only way for a batch to exceed the budget.
    static TFittingRows RowsWithinBudget(const TRowSizes& sizes, i64 offset, i64 count, ui64 budget, bool emptyBatch) {
        TFittingRows fitting;
        while (fitting.Rows < count) {
            const ui64 rowBytes = sizes.RowBytes(offset + fitting.Rows);
            if (rowBytes > budget - fitting.Bytes) {
                break;
            }
            fitting.Bytes += rowBytes;
            ++fitting.Rows;
        }
        if (fitting.Rows == 0 && emptyBatch) {
            fitting = {.Rows = 1, .Bytes = sizes.RowBytes(offset)};
        }
        return fitting;
    }

public:
    // The pages of a row group and the dictionaries decoded from them take up
    // to its uncompressed size each, and the engine accepts a row group whose
    // uncompressed size is below the limit of the read buffer. So with twice
    // that limit a row group the engine accepts can be decoded.
    explicit TParquetDataParser(ui64 bufferSizeLimit)
        : DecodeMemoryLimit(bufferSizeLimit > Max<ui64>() / 2 ? Max<ui64>() : 2 * bufferSizeLimit)
    {
    }

    std::expected<void, TString> Configure(
        const TTableInfo& tableInfo,
        const NKikimrSchemeOp::TTableDescription& scheme) override
    {
        ResetFile();

        ColumnMeta.clear();
        ColumnMeta.reserve(scheme.GetColumns().size());
        YdbSchema.clear();
        YdbSchema.reserve(scheme.GetColumns().size());
        KeyCount = 0;

        for (auto&& column : scheme.GetColumns()) {
            auto typeInfoMod = NScheme::TypeInfoModFromProtoColumnType(
                column.GetTypeId(),
                column.HasTypeInfo() ? &column.GetTypeInfo() : nullptr);

            TColumnMeta meta;
            meta.Name = column.GetName();
            meta.TypeInfo = typeInfoMod.TypeInfo;
            meta.KeyOrder = tableInfo.KeyOrder(column.GetName());
            if (meta.KeyOrder != Max<ui32>()) {
                ++KeyCount;
            }

            // The Arrow type the exporter writes for this YDB type. The
            // converter static-casts every column to the array class implied
            // by the YDB type, so the file must match exactly.
            auto arrowType = NArrow::GetArrowType(meta.TypeInfo);
            if (!arrowType.ok()) {
                return std::unexpected(TStringBuilder() << "column '" << meta.Name
                    << "': " << arrowType.status().ToString());
            }
            meta.ArrowType = meta.TypeInfo.GetTypeId() == NScheme::NTypeIds::Interval
                ? arrow::int64() // mirrors the exporter's remap in export_parquet.cpp
                : arrowType.ValueUnsafe();

            // The converter casts a column to the Arrow array class of its YDB
            // type. For Interval that is a duration array, and the file has an
            // int64 one, so the converter is given Int64: the cell is the same.
            YdbSchema.emplace_back(
                meta.Name,
                meta.TypeInfo.GetTypeId() == NScheme::NTypeIds::Interval
                    ? NScheme::TTypeInfo(NScheme::NTypeIds::Int64)
                    : meta.TypeInfo);
            ColumnMeta.push_back(std::move(meta));
        }

        RowOverhead = TSerializedCellVec::SerializedSize(TVector<TCell>(KeyCount))
            + TSerializedCellVec::SerializedSize(TVector<TCell>(ColumnMeta.size() - KeyCount));

        return {};
    }

    bool HasOpenFile() const override {
        return static_cast<bool>(Session);
    }

    std::expected<void, TString> OpenFile(TStringBuf data) override {
        if (data.empty()) {
            ResetFile();
            return {};
        }

        auto buffer = std::make_shared<arrow::Buffer>(
            reinterpret_cast<const uint8_t*>(data.data()),
            static_cast<int64_t>(data.size()));
        return OpenFile(std::make_shared<arrow::io::BufferReader>(buffer));
    }

    std::expected<void, TString> OpenFile(std::shared_ptr<arrow::io::RandomAccessFile> source) override {
        if (auto result = OpenMetadata(std::move(source)); !result) {
            return result;
        }
        if (!Session) {
            return {};
        }

        std::vector<int> rowGroupIndices(Session->FileReader->num_row_groups());
        std::iota(rowGroupIndices.begin(), rowGroupIndices.end(), 0);
        if (auto result = OpenRowGroups(std::move(rowGroupIndices)); !result) {
            ResetFile();
            return result;
        }
        return {};
    }

    std::expected<void, TString> OpenMetadata(
        std::shared_ptr<arrow::io::RandomAccessFile> source) override
    {
        ResetFile();

        if (!source) {
            return {};
        }

        auto session = std::make_unique<TParquetFileSession>();
        session->Memory = std::make_unique<TDecodeMemoryPool>(DecodeMemoryLimit);
        session->Source = std::move(source);

        parquet::arrow::FileReaderBuilder builder;
        if (auto st = builder.Open(session->Source, parquet::ReaderProperties(session->Memory.get())); !st.ok()) {
            return std::unexpected(TStringBuilder() << "failed to open parquet file: " << st.ToString());
        }

        builder.memory_pool(session->Memory.get());
        builder.properties(parquet::ArrowReaderProperties(/*use_threads*/ false));

        if (auto st = builder.Build(&session->FileReader); !st.ok()) {
            return std::unexpected(TStringBuilder() << "failed to build parquet reader: " << st.ToString());
        }

        std::shared_ptr<arrow::Schema> schema;
        if (auto st = session->FileReader->GetSchema(&schema); !st.ok()) {
            return std::unexpected(TStringBuilder() << "failed to read parquet schema: " << st.ToString());
        }

        const auto* parquetSchema = session->FileReader->parquet_reader()->metadata()->schema();
        session->ColumnIndices.reserve(ColumnMeta.size());
        for (auto&& col : ColumnMeta) {
            const int fieldIndex = schema->GetFieldIndex(std::string(col.Name));
            if (fieldIndex < 0) {
                return std::unexpected(TStringBuilder()
                    << "column '" << col.Name << "' not found in parquet schema");
            }

            const auto& fileType = schema->field(fieldIndex)->type();
            if (!fileType->Equals(*col.ArrowType)) {
                if (!IsWriterCoercion(*fileType, *col.ArrowType)) {
                    return std::unexpected(TStringBuilder()
                        << "column '" << col.Name << "' has parquet type " << fileType->ToString()
                        << ", expected " << col.ArrowType->ToString()
                        << " for " << NScheme::TypeName(col.TypeInfo));
                }
                session->CastColumns.emplace_back(std::string(col.Name), col.ArrowType);
            }

            // Leaf column index for the reader; a primitive column's path is its name.
            const int columnIndex = parquetSchema->ColumnIndex(std::string(col.Name));
            if (columnIndex < 0) {
                return std::unexpected(TStringBuilder()
                    << "column '" << col.Name << "' is not a primitive parquet column");
            }
            session->ColumnIndices.push_back(columnIndex);
        }

        Session = std::move(session);
        return {};
    }

    const std::vector<int>& GetColumnIndices() const override {
        static const std::vector<int> none;
        return Session ? Session->ColumnIndices : none;
    }

    TVector<TRowGroupInfo> GetRowGroups() const override {
        TVector<TRowGroupInfo> rowGroups;
        if (!Session || !Session->FileReader) {
            return rowGroups;
        }

        const auto metadata = Session->FileReader->parquet_reader()->metadata();
        rowGroups.reserve(metadata->num_row_groups());
        for (int i = 0; i < metadata->num_row_groups(); ++i) {
            const auto rowGroup = metadata->RowGroup(i);

            TRowGroupInfo info;
            for (const int column : Session->ColumnIndices) {
                info.UncompressedBytes += static_cast<ui64>(
                    Max<int64_t>(rowGroup->ColumnChunk(column)->total_uncompressed_size(), 0));
            }
            rowGroups.push_back(info);
        }
        return rowGroups;
    }

    std::expected<void, TString> OpenRowGroup(ui32 rowGroupIndex) override {
        if (!Session || !Session->FileReader) {
            return std::unexpected(TString("Parquet metadata is not open"));
        }
        if (rowGroupIndex >= static_cast<ui32>(Session->FileReader->num_row_groups())) {
            return std::unexpected(TStringBuilder() << "Parquet row group " << rowGroupIndex
                << " is outside a file with " << Session->FileReader->num_row_groups()
                << " row groups");
        }

        return OpenRowGroups({static_cast<int>(rowGroupIndex)});
    }

    void ResetRowGroup() override {
        if (!Session) {
            return;
        }

        Session->RowGroups.clear();
        Session->BatchReader.reset();
        Session->HeldBatch.reset();
        Session->HeldSizes.Clear();
        Session->HeldOffset = 0;
        Session->RowsRead = 0;
        Session->BatchRows = 1;
        Session->BatchRowsLimit = Max<i64>();
        Session->DecodedBatches.clear();
    }

    std::expected<TParsedBatch, TString> ProcessNextBatch(
        TMemoryPool& pool,
        const IDataParser::TAddRowFn& addRow,
        ui64 maxDataBytes) override
    {
        Y_UNUSED(pool); // Arrow owns the decoded data; cells point into the record batch

        if (!Session || !Session->BatchReader) {
            return TParsedBatch{};
        }

        TImportParquetRowWriter rowWriter(addRow, ColumnMeta, KeyCount);
        // A backup holds cells in their stored form: the exporter appends them
        // to the Arrow builders as they are, so DyNumber and JsonDocument are
        // binary in the file. The converter's text conversions are meant for
        // user-supplied Arrow data (DyNumber as a numeric string, JsonDocument
        // as JSON text) and must stay off here.
        NArrow::TArrowToYdbConverter converter(
            YdbSchema, rowWriter, /*allowInfDouble=*/false, /*withConversion=*/false);
        const ui64 firstRow = Session->RowsRead;
        const ui64 decodeBytes = maxDataBytes ? maxDataBytes : DefaultDecodeBytes;

        // Makes sure HeldBatch holds unread rows; false once the row group is exhausted.
        const auto fetch = [this, decodeBytes]() -> std::expected<bool, TString> {
            while (!Session->HeldBatch) {
                std::shared_ptr<arrow::RecordBatch> batch;
                if (auto st = ReadBatch(Session->BatchRows, batch); !st.ok()) {
                    if (!Session->Memory->HasRefused()) {
                        return std::unexpected(TStringBuilder()
                            << "failed to read parquet record batch: " << st.ToString());
                    }
                    if (auto result = ReopenWithSmallerBatches(); !result) {
                        return std::unexpected(std::move(result.error()));
                    }
                    continue;
                }
                if (!batch) {
                    return false;
                }
                if (batch->num_rows() > 0) {
                    auto casted = CastCoercedColumns(std::move(batch));
                    if (!casted) {
                        if (!Session->Memory->HasRefused()) {
                            return std::unexpected(std::move(casted.error()));
                        }
                        if (auto result = ReopenWithSmallerBatches(); !result) {
                            return std::unexpected(std::move(result.error()));
                        }
                        continue;
                    }
                    batch = std::move(*casted);

                    Session->DecodedBatches.push_back(batch->num_rows());
                    Session->BatchRows = Min(
                        NextBatchRows(decodeBytes, DecodedBytes(*batch), batch->num_rows()),
                        Session->BatchRowsLimit);

                    Session->HeldBatch = std::move(batch);
                    Session->HeldSizes.ConstructInPlace(*Session->HeldBatch, RowOverhead);
                    Session->HeldOffset = 0;
                }
            }
            return true;
        };

        const auto makeResult = [&rowWriter](bool hasMore) {
            const auto parsedData = rowWriter.GetParsedData();
            return TParsedBatch{
                .DataBytes = parsedData.DataBytes,
                .Rows = parsedData.Rows,
                .HasMore = hasMore,
            };
        };

        ui64 usedBytes = 0; // of the byte budget, by the rows emitted so far
        while (true) {
            auto available = fetch();
            if (!available) {
                ResetRowGroup();
                return std::unexpected(std::move(available.error()));
            }
            if (!*available) {
                if (!rowWriter.GetParsedData().Rows) {
                    ResetRowGroup();
                }
                return makeResult(false);
            }

            auto& held = Session->HeldBatch;
            const i64 remaining = held->num_rows() - Session->HeldOffset;
            i64 take = remaining;
            if (maxDataBytes) {
                const auto fitting = RowsWithinBudget(
                    *Session->HeldSizes, Session->HeldOffset, remaining,
                    maxDataBytes > usedBytes ? maxDataBytes - usedBytes : 0,
                    /*emptyBatch=*/rowWriter.GetParsedData().Rows == 0);
                if (fitting.Rows == 0) {
                    return makeResult(true); // the rows that are left go to the next batch
                }
                take = fitting.Rows;
                usedBytes += fitting.Bytes;
            }

            const auto slice = take == held->num_rows()
                ? held
                : held->Slice(Session->HeldOffset, take);

            TString error;
            if (!converter.Process(*slice, error)) {
                ResetRowGroup();
                return std::unexpected(std::move(error));
            }
            if (const auto& invalidValue = rowWriter.GetError()) {
                error = TStringBuilder() << *invalidValue
                    << " in " << DescribeRow(firstRow + rowWriter.GetParsedData().Rows);
                ResetRowGroup();
                return std::unexpected(std::move(error));
            }

            Session->RowsRead += take;
            Session->HeldOffset += take;
            if (Session->HeldOffset < held->num_rows()) {
                return makeResult(true); // the byte budget is used up
            }

            // The batch is emitted and the budget is not used up: go on with
            // the next one. If there is none, the rows emitted so far are the
            // last ones of the row group.
            held.reset();
            Session->HeldSizes.Clear();
            Session->HeldOffset = 0;
        }
    }

    void ResetFile() override {
        Session.reset();
    }

    std::expected<TParsedData, TString> ParseBlock(
        TStringBuf data,
        TMemoryPool& pool,
        const TAddRowFn& addRow) override
    {
        if (auto result = OpenFile(data); !result) {
            return std::unexpected(std::move(result.error()));
        }

        TParsedData parsedData;
        while (true) {
            auto result = ProcessNextBatch(pool, addRow, /*maxDataBytes=*/0);
            if (!result) {
                ResetFile();
                return std::unexpected(std::move(result.error()));
            }

            parsedData.DataBytes += result->DataBytes;
            parsedData.Rows += result->Rows;
            if (!result->HasMore) {
                break;
            }
        }

        ResetFile();
        return parsedData;
    }

private:
    // Restores the expected Arrow type of columns the writer coerced. The cast
    // is checked: a value that does not fit the YDB type fails the import.
    std::expected<std::shared_ptr<arrow::RecordBatch>, TString> CastCoercedColumns(
        std::shared_ptr<arrow::RecordBatch> batch) const
    {
        // The memory of the columns the cast makes is within the limit of the
        // decoding as well: a cast that is refused is a batch that is refused.
        Session->Memory->ResetRefused();
        arrow::compute::ExecContext context(Session->Memory.get());

        for (const auto& [name, type] : Session->CastColumns) {
            const int index = batch->schema()->GetFieldIndex(name);
            if (index < 0) {
                return std::unexpected(TStringBuilder() << "column '" << name << "' is missing in a parquet record batch");
            }

            auto casted = arrow::compute::Cast(
                *batch->column(index), type, arrow::compute::CastOptions::Safe(), &context);
            if (!casted.ok()) {
                return std::unexpected(TStringBuilder() << "column '" << name << "': cannot convert parquet type "
                    << batch->column(index)->type()->ToString() << " to " << type->ToString()
                    << ": " << casted.status().ToString());
            }

            auto updated = batch->SetColumn(index, arrow::field(name, type), *casted);
            if (!updated.ok()) {
                return std::unexpected(TStringBuilder() << "column '" << name << "': " << updated.status().ToString());
            }
            batch = std::move(*updated);
        }
        return batch;
    }

    // Names a row of the open row groups by its position in the file. Both
    // numbers are zero-based, the way Parquet tools count them.
    TString DescribeRow(ui64 row) const {
        const auto metadata = Session->FileReader->parquet_reader()->metadata();
        for (const int rowGroup : Session->RowGroups) {
            const ui64 rows = static_cast<ui64>(Max<int64_t>(metadata->RowGroup(rowGroup)->num_rows(), 0));
            if (row < rows) {
                return TStringBuilder() << "row " << row << " of row group " << rowGroup;
            }
            row -= rows;
        }
        return TStringBuilder() << "row " << row << " past the open row groups";
    }

    // Decodes the next rows of the open row groups: the given number of them,
    // or fewer at the end. The batch is null when no rows are left.
    arrow::Status ReadBatch(i64 rows, std::shared_ptr<arrow::RecordBatch>& batch) {
        Session->Memory->ResetRefused();
        // The reader takes the batch size for every batch it decodes.
        Session->FileReader->set_batch_size(rows);
        return Session->BatchReader->ReadNext(&batch);
    }

    // Called when a batch does not fit into the memory limit of the decoding.
    // The reader cannot go on after a read that has failed, so the row groups
    // are opened again, the rows that have been emitted are decoded once more,
    // in the batches they were decoded in, and dropped. The reading goes on
    // with a batch of one row. The batches of these row groups stay shorter
    // than the one that has failed, so this happens a limited number of times.
    std::expected<void, TString> ReopenWithSmallerBatches() {
        const i64 failedRows = Session->BatchRows;
        if (failedRows == 1) {
            return std::unexpected(TStringBuilder() << "Parquet " << DescribeRow(Session->RowsRead)
                << " cannot be decoded within " << Session->Memory->GetLimit()
                << " bytes of memory (twice RestoreReadBufferSizeLimit)");
        }

        Session->BatchReader.reset();
        if (auto st = Session->FileReader->GetRecordBatchReader(
                Session->RowGroups, Session->ColumnIndices, &Session->BatchReader); !st.ok())
        {
            return std::unexpected(TStringBuilder()
                << "failed to get parquet record batch reader: " << st.ToString());
        }

        ui64 skipped = 0;
        for (size_t i = 0; skipped < Session->RowsRead; ++i) {
            const ui64 left = Session->RowsRead - skipped;
            const ui64 rows = i < Session->DecodedBatches.size()
                ? Min<ui64>(Session->DecodedBatches[i], left)
                : 1;

            std::shared_ptr<arrow::RecordBatch> batch;
            if (auto st = ReadBatch(static_cast<i64>(rows), batch); !st.ok()) {
                return std::unexpected(TStringBuilder()
                    << "failed to read parquet record batch again: " << st.ToString());
            }
            if (!batch || static_cast<ui64>(batch->num_rows()) > left) {
                return std::unexpected(TStringBuilder() << "parquet rows read again differ from those read before "
                    << DescribeRow(Session->RowsRead));
            }
            skipped += batch->num_rows();
        }

        Session->BatchRows = 1;
        Session->BatchRowsLimit = failedRows / 2;
        return {};
    }

    std::expected<void, TString> OpenRowGroups(std::vector<int> rowGroupIndices) {
        ResetRowGroup();

        if (auto st = Session->FileReader->GetRecordBatchReader(
                rowGroupIndices, Session->ColumnIndices, &Session->BatchReader); !st.ok())
        {
            ResetRowGroup();
            return std::unexpected(TStringBuilder()
                << "failed to get parquet record batch reader: " << st.ToString());
        }
        Session->RowGroups = std::move(rowGroupIndices);

        return {};
    }

private:
    const ui64 DecodeMemoryLimit; // 0 = no limit
    TVector<TColumnMeta> ColumnMeta;
    std::vector<std::pair<TString, NScheme::TTypeInfo>> YdbSchema; // what the converter takes the columns for
    ui32 KeyCount = 0;
    ui64 RowOverhead = 0; // see TRowSizes
    std::unique_ptr<TParquetFileSession> Session;
};

} // anonymous namespace

IParquetStreamParser::TPtr CreateParquetDataParser(ui64 bufferSizeLimit) {
    return MakeHolder<TParquetDataParser>(bufferSizeLimit);
}

} // namespace NKikimr::NDataShard

#endif // KIKIMR_DISABLE_S3_OPS
