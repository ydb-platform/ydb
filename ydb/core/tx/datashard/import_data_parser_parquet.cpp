#ifndef KIKIMR_DISABLE_S3_OPS

#include "import_data_parser.h"
#include "import_parquet_s3_file.h"

#include <ydb/core/formats/arrow/arrow_helpers.h>
#include <ydb/core/formats/arrow/converter.h>
#include <ydb/core/io_formats/cell_maker/cell_maker.h>
#include <ydb/core/scheme/scheme_tablecell.h>
#include <ydb/core/scheme/scheme_types_proto.h>

#include <yql/essentials/parser/pg_wrapper/interface/type_desc.h>
#include <yql/essentials/public/decimal/yql_decimal.h>
#include <yql/essentials/types/binary_json/read.h>
#include <yql/essentials/types/dynumber/dynumber.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/array/concatenate.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/compute/cast.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/compute/exec.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/io/memory.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/memory_pool.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/record_batch.h>
#include <contrib/libs/apache/arrow/cpp/src/parquet/arrow/reader.h>
#include <contrib/libs/apache/arrow/cpp/src/parquet/file_reader.h>
#include <contrib/libs/apache/arrow/cpp/src/generated/parquet_types.h>

#include <contrib/restricted/thrift/thrift/protocol/TCompactProtocol.h>
#include <contrib/restricted/thrift/thrift/transport/TBufferTransports.h>

#include <util/generic/size_literals.h>
#include <util/string/builder.h>

#include <cstring>
#include <numeric>

namespace NKikimr::NDataShard {

namespace {

struct TColumnMeta {
    TString Name;
    NScheme::TTypeInfo TypeInfo;
    std::shared_ptr<arrow::DataType> ArrowType;
    ui32 KeyOrder = Max<ui32>();
};

// Checks what a YDB type restricts beyond its Arrow type. CSV checks it while parsing
// text; a Parquet backup holds the stored form, so it is checked here.
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

// Splits the converter's row into keys and values and forwards it to addRow. An invalid
// or rejected row stops the import. The cells are borrowed: addRow must not defer them.
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

    // Set once a row is invalid or rejected: the row after those counted by GetParsedData().
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

// Parquet format 1.0 has no unsigned 32-bit annotation: the exporter writes uint32 as
// INT64, and such columns are cast back. Any other mismatch is an error.
bool IsWriterCoercion(const arrow::DataType& fileType, const arrow::DataType& expectedType) {
    return fileType.id() == arrow::Type::INT64 && expectedType.id() == arrow::Type::UINT32;
}

// The upload bytes of a batch's rows: rowOverhead (two cell vectors with a header per
// cell, NULL included) plus the cell bytes.
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

// Checks the footer before Arrow parses it. Thrift resizes a list to its declared length
// before reading it, and a 3-byte column chunk becomes 560 bytes in memory, so a crafted
// footer takes GBs; Arrow builds the schema tree by recursion, so a deep schema overflows
// the stack. One pass that builds nothing: list lengths against the bytes left, the
// memory of the parsed footer against the limit, and the schema depth.
constexpr size_t MaxSchemaNesting = 32;

namespace NThrift = apache::thrift::protocol;

class TFooterWalker {
    using TTransport = apache::thrift::transport::TMemoryBuffer;
    using TReader = NThrift::TCompactProtocolT<TTransport>;

    // Field ids, see parquet.thrift
    static constexpr int16_t FileMetaDataSchema = 2;
    static constexpr int16_t FileMetaDataRowGroups = 4;
    static constexpr int16_t RowGroupColumns = 1;
    static constexpr int16_t SchemaElementNumChildren = 5;

    // What an entry of any other list takes once parsed.
    static constexpr ui64 BytesPerOtherEntry = 64;
    static constexpr ui64 BytesPerString = 32;
    static constexpr ui64 BytesPerNumber = 8;

public:
    struct TRejected {
        TString Message;
    };

    TFooterWalker(TTransport& transport, TReader& reader, ui64 footerBytes, ui64 memoryLimit)
        : Transport(transport)
        , Reader(reader)
        , MemoryLimit(memoryLimit)
        , Estimate(footerBytes)
    {
    }

    // Throws TRejected for a footer the import refuses, thrift's exceptions for one it cannot read.
    void Walk() {
        NThrift::TInputRecursionTracker tracker(Reader);
        std::string name;
        Reader.readStructBegin(name);
        while (true) {
            NThrift::TType type;
            int16_t id;
            Reader.readFieldBegin(name, type, id);
            if (type == NThrift::T_STOP) {
                break;
            }
            if (id == FileMetaDataSchema && type == NThrift::T_LIST) {
                const ui32 count = ReadListOfStructs(ParquetFooterBytesPerColumn);
                for (ui32 i = 0; i < count; ++i) {
                    WalkSchemaElement();
                }
                Reader.readListEnd();
            } else if (id == FileMetaDataRowGroups && type == NThrift::T_LIST) {
                const ui32 count = ReadListOfStructs(ParquetFooterBytesPerRowGroup);
                for (ui32 i = 0; i < count; ++i) {
                    WalkRowGroup();
                }
                Reader.readListEnd();
            } else {
                Skip(type);
            }
            Reader.readFieldEnd();
        }
        Reader.readStructEnd();
    }

    ui64 GetEstimate() const {
        return Estimate;
    }

private:
    void WalkSchemaElement() {
        NThrift::TInputRecursionTracker tracker(Reader);
        std::string name;
        int32_t numChildren = 0;
        Reader.readStructBegin(name);
        while (true) {
            NThrift::TType type;
            int16_t id;
            Reader.readFieldBegin(name, type, id);
            if (type == NThrift::T_STOP) {
                break;
            }
            if (id == SchemaElementNumChildren && type == NThrift::T_I32) {
                Reader.readI32(numChildren);
            } else {
                Skip(type);
            }
            Reader.readFieldEnd();
        }
        Reader.readStructEnd();

        while (!Pending.empty() && Pending.back() == 0) {
            Pending.pop_back();
        }
        if (!Pending.empty()) {
            --Pending.back();
        }
        if (numChildren > 0) {
            Pending.push_back(numChildren);
            if (Pending.size() > MaxSchemaNesting) {
                throw TRejected{TStringBuilder() << "Parquet schema is nested more than "
                    << MaxSchemaNesting << " levels deep"};
            }
        }
    }

    void WalkRowGroup() {
        NThrift::TInputRecursionTracker tracker(Reader);
        std::string name;
        Reader.readStructBegin(name);
        while (true) {
            NThrift::TType type;
            int16_t id;
            Reader.readFieldBegin(name, type, id);
            if (type == NThrift::T_STOP) {
                break;
            }
            if (id == RowGroupColumns && type == NThrift::T_LIST) {
                const ui32 count = ReadListOfStructs(ParquetFooterBytesPerColumnChunk);
                for (ui32 i = 0; i < count; ++i) {
                    Skip(NThrift::T_STRUCT);
                }
                Reader.readListEnd();
            } else {
                Skip(type);
            }
            Reader.readFieldEnd();
        }
        Reader.readStructEnd();
    }

    // The header of a list of structures: its length, checked and charged.
    ui32 ReadListOfStructs(ui64 bytesPerEntry) {
        NThrift::TType type;
        ui32 count;
        Reader.readListBegin(type, count);
        if (type != NThrift::T_STRUCT) {
            throw TRejected{TStringBuilder() << "Parquet footer holds a list of values of type "
                << static_cast<int>(type) << " where a list of structures is expected"};
        }
        Declared(count);
        Charge(count * bytesPerEntry);
        return count;
    }

    // A list of count entries is declared: an entry takes a byte at least.
    void Declared(ui32 count) {
        const ui32 remaining = Transport.available_read();
        if (count > remaining) {
            throw TRejected{TStringBuilder() << "Parquet footer declares a list of " << count
                << " entries in its last " << remaining << " bytes"};
        }
    }

    void Charge(ui64 bytes) {
        Estimate += bytes;
        if (MemoryLimit && Estimate >= MemoryLimit) {
            throw TRejected{TStringBuilder() << "Parquet footer takes about " << Estimate
                << " bytes in memory, the limit is " << MemoryLimit
                << " bytes (RestoreReadBufferSizeLimit)"};
        }
    }

    static ui64 BytesPerEntry(NThrift::TType type) {
        switch (type) {
        case NThrift::T_STRUCT:
            return BytesPerOtherEntry;
        case NThrift::T_STRING:
        case NThrift::T_LIST:
        case NThrift::T_SET:
        case NThrift::T_MAP:
            return BytesPerString; // a string, a vector or a map before its entries
        default:
            return BytesPerNumber;
        }
    }

    // Thrift's skip, with lists checked and charged, and strings read into one buffer.
    void Skip(NThrift::TType type) {
        switch (type) {
        case NThrift::T_BOOL: {
            bool value;
            Reader.readBool(value);
            break;
        }
        case NThrift::T_BYTE: {
            int8_t value;
            Reader.readByte(value);
            break;
        }
        case NThrift::T_I16: {
            int16_t value;
            Reader.readI16(value);
            break;
        }
        case NThrift::T_I32: {
            int32_t value;
            Reader.readI32(value);
            break;
        }
        case NThrift::T_I64: {
            int64_t value;
            Reader.readI64(value);
            break;
        }
        case NThrift::T_DOUBLE: {
            double value;
            Reader.readDouble(value);
            break;
        }
        case NThrift::T_STRING:
            Reader.readBinary(Scratch);
            break;
        case NThrift::T_STRUCT: {
            NThrift::TInputRecursionTracker tracker(Reader);
            std::string name;
            Reader.readStructBegin(name);
            while (true) {
                NThrift::TType fieldType;
                int16_t id;
                Reader.readFieldBegin(name, fieldType, id);
                if (fieldType == NThrift::T_STOP) {
                    break;
                }
                Skip(fieldType);
                Reader.readFieldEnd();
            }
            Reader.readStructEnd();
            break;
        }
        case NThrift::T_LIST:
        case NThrift::T_SET: {
            NThrift::TInputRecursionTracker tracker(Reader);
            NThrift::TType entryType;
            ui32 count;
            if (type == NThrift::T_LIST) {
                Reader.readListBegin(entryType, count);
            } else {
                Reader.readSetBegin(entryType, count);
            }
            Declared(count);
            Charge(count * BytesPerEntry(entryType));
            for (ui32 i = 0; i < count; ++i) {
                Skip(entryType);
            }
            if (type == NThrift::T_LIST) {
                Reader.readListEnd();
            } else {
                Reader.readSetEnd();
            }
            break;
        }
        case NThrift::T_MAP: {
            NThrift::TInputRecursionTracker tracker(Reader);
            NThrift::TType keyType;
            NThrift::TType valueType;
            ui32 count;
            Reader.readMapBegin(keyType, valueType, count);
            Declared(count);
            Charge(count * (BytesPerEntry(keyType) + BytesPerEntry(valueType)));
            for (ui32 i = 0; i < count; ++i) {
                Skip(keyType);
                Skip(valueType);
            }
            Reader.readMapEnd();
            break;
        }
        default:
            throw TRejected{TStringBuilder() << "Parquet footer holds a value of unknown type "
                << static_cast<int>(type)};
        }
    }

    TTransport& Transport;
    TReader& Reader;
    const ui64 MemoryLimit; // 0 = no limit
    ui64 Estimate; // what the parsed footer takes, so far
    TVector<int32_t> Pending; // the children still to come, for every open group of the schema
    std::string Scratch; // the strings passed over land here
};

// Returns what the parsed footer will take in memory, by the walk.
std::expected<ui64, TString> CheckFooter(arrow::io::RandomAccessFile& source, ui64 bufferSizeLimit) {
    static constexpr int64_t FooterTail = 8; // the length of the footer and the magic

    auto size = source.GetSize();
    if (!size.ok()) {
        return std::unexpected(TStringBuilder() << "failed to get the size of the parquet file: " << size.status().ToString());
    }
    if (*size < FooterTail + 4) {
        return std::unexpected(TString("Parquet file is too small"));
    }

    auto tail = source.ReadAt(*size - FooterTail, FooterTail);
    if (!tail.ok() || (*tail)->size() != FooterTail) {
        return std::unexpected(TStringBuilder() << "failed to read the parquet footer: "
            << (tail.ok() ? "short read" : tail.status().ToString()));
    }
    if (memcmp((*tail)->data() + 4, "PAR1", 4) != 0) {
        return std::unexpected(TString("parquet magic bytes not found in footer"));
    }
    const uint32_t footerLength = arrow::util::SafeLoadAs<uint32_t>((*tail)->data());
    if (footerLength > *size - FooterTail) {
        return std::unexpected(TStringBuilder() << "parquet metadata length " << footerLength
            << " exceeds file size " << *size);
    }

    auto footer = source.ReadAt(*size - FooterTail - footerLength, footerLength);
    if (!footer.ok() || (*footer)->size() != footerLength) {
        return std::unexpected(TStringBuilder() << "failed to read the parquet footer: "
            << (footer.ok() ? "short read" : footer.status().ToString()));
    }

    ui64 estimate = 0;
    try {
        using TTransport = apache::thrift::transport::TMemoryBuffer;
        auto transport = std::make_shared<TTransport>(const_cast<uint8_t*>((*footer)->data()), footerLength);
        // A string or a list cannot exceed the footer's bytes (Arrow's limits: 100 MB and
        // a million).
        const auto limit = static_cast<int32_t>(Min<ui64>(footerLength, Max<int32_t>()));
        NThrift::TCompactProtocolT<TTransport> reader(transport, limit, limit);
        TFooterWalker walker(*transport, reader, footerLength, bufferSizeLimit);
        walker.Walk();
        estimate = walker.GetEstimate();
    } catch (const TFooterWalker::TRejected& rejected) {
        return std::unexpected(rejected.Message);
    } catch (const std::exception& ex) {
        return std::unexpected(TStringBuilder() << "failed to parse the parquet footer: " << ex.what());
    }

    return estimate;
}

// Parquet throws for some errors; the parser runs in an actor, so every entry point
// turns them into an error.
template <class TResult, class TFn>
TResult Guarded(TFn&& fn) {
    try {
        return fn();
    } catch (const parquet::ParquetException& ex) {
        return std::unexpected(TStringBuilder() << "parquet: " << ex.what());
    } catch (const std::exception& ex) {
        return std::unexpected(TStringBuilder() << "parquet: " << ex.what());
    }
}

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

// Bounds the memory Arrow takes to read a file (pages, dictionaries, decoded rows), which
// the file does not bound. An allocation above the limit is refused and fails the read.
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
    TDecodeMemoryPool* Memory = nullptr; // the parser's, which outlives the session
    std::shared_ptr<arrow::io::RandomAccessFile> Source;
    std::unique_ptr<parquet::arrow::FileReader> FileReader;
    ui64 FooterMemoryEstimate = 0; // by the walk over the footer, see CheckFooter()
    std::vector<int> ColumnIndices; // parquet leaf columns to decode, in scheme order
    std::vector<std::pair<std::string, std::shared_ptr<arrow::DataType>>> CastColumns; // see IsWriterCoercion
    std::vector<int> RowGroups; // the row groups to read, in that order
    size_t CurrentGroup = 0; // the one of RowGroups that ColumnReaders read
    // A reader per column over the current row group; ReadBatch() puts the columns together.
    std::vector<std::unique_ptr<arrow::RecordBatchReader>> ColumnReaders;
    std::shared_ptr<arrow::Schema> BatchSchema; // of the batches ReadBatch() makes
    ui64 DecodedInGroup = 0; // the rows decoded from the current row group
    std::shared_ptr<arrow::RecordBatch> HeldBatch; // rows [HeldOffset, num_rows) not yet emitted
    TMaybe<TRowSizes> HeldSizes; // the sizes of the rows of HeldBatch
    i64 HeldOffset = 0;
    ui64 RowsRead = 0; // rows of the open row groups that have been emitted
    i64 BatchRows = 1; // the rows the next batch is decoded with, see NextBatchRows()
    i64 BatchRowsLimit = Max<i64>(); // lowered by ReopenWithSmallerBatches()
    TVector<i64> DecodedBatches; // the rows of every batch decoded from the open row groups
};

class TParquetDataParser final : public IParquetStreamParser {
    // A row group is decoded in batches sized by the rows before them: one row first, then
    // at most twice the previous batch, to a byte target. The target is not a bound; the
    // bound is the decoding memory limit, see ReopenWithSmallerBatches().
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

    // The rows from offset that fit the byte budget; a row that does not fit an empty batch
    // is taken alone.
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
    // Pages and dictionaries of a row group take up to its uncompressed size each, so twice
    // the buffer limit decodes any row group the engine accepts.
    explicit TParquetDataParser(ui64 bufferSizeLimit)
        : BufferSizeLimit(bufferSizeLimit)
        , DecodeMemoryLimit(bufferSizeLimit > Max<ui64>() / 2 ? Max<ui64>() : 2 * bufferSizeLimit)
        , Memory(DecodeMemoryLimit)
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

            // The converter static-casts a column to the array class of its YDB type: the file
            // must match exactly.
            auto arrowType = NArrow::GetArrowType(meta.TypeInfo);
            if (!arrowType.ok()) {
                return std::unexpected(TStringBuilder() << "column '" << meta.Name
                    << "': " << arrowType.status().ToString());
            }
            meta.ArrowType = meta.TypeInfo.GetTypeId() == NScheme::NTypeIds::Interval
                ? arrow::int64() // mirrors the exporter's remap in export_parquet.cpp
                : arrowType.ValueUnsafe();

            // Interval is a duration array for the converter but int64 in the file: the converter
            // is given Int64.
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

        return Guarded<std::expected<void, TString>>([this] {
            std::vector<int> rowGroupIndices(Session->FileReader->num_row_groups());
            std::iota(rowGroupIndices.begin(), rowGroupIndices.end(), 0);
            if (auto result = OpenRowGroups(std::move(rowGroupIndices)); !result) {
                ResetFile();
                return result;
            }
            return std::expected<void, TString>{};
        });
    }

    std::expected<void, TString> OpenMetadata(
        std::shared_ptr<arrow::io::RandomAccessFile> source) override
    {
        return Guarded<std::expected<void, TString>>([this, &source] {
            auto result = OpenMetadataUnguarded(std::move(source));
            if (!result) {
                ResetFile();
            }
            return result;
        });
    }

    std::expected<void, TString> OpenMetadataUnguarded(std::shared_ptr<arrow::io::RandomAccessFile> source) {
        ResetFile();

        if (!source) {
            return {};
        }

        auto session = std::make_unique<TParquetFileSession>();
        session->Memory = &Memory;
        Memory.ResetRefused();
        session->Source = std::move(source);

        auto footerMemory = CheckFooter(*session->Source, BufferSizeLimit);
        if (!footerMemory) {
            return std::unexpected(std::move(footerMemory.error()));
        }
        session->FooterMemoryEstimate = *footerMemory;

        parquet::arrow::FileReaderBuilder builder;
        if (auto st = builder.Open(session->Source, parquet::ReaderProperties(session->Memory)); !st.ok()) {
            return std::unexpected(TStringBuilder() << "failed to open parquet file: " << st.ToString());
        }

        builder.memory_pool(session->Memory);
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

        // Arrow trusts the footer's row counts and reads out of bounds when a column is shorter,
        // so the columns are read one by one (ReadBatch) and the footer is checked first.
        const auto metadata = session->FileReader->parquet_reader()->metadata();
        for (int rowGroup = 0; rowGroup < metadata->num_row_groups(); ++rowGroup) {
            const auto rowGroupMeta = metadata->RowGroup(rowGroup);
            // a chunk for every column, or a lookup throws
            if (rowGroupMeta->num_columns() != metadata->num_columns()) {
                return std::unexpected(TStringBuilder() << "Parquet row group " << rowGroup << " has "
                    << rowGroupMeta->num_columns() << " column chunks, the schema has "
                    << metadata->num_columns() << " columns");
            }
            for (size_t i = 0; i < ColumnMeta.size(); ++i) {
                const int64_t values = rowGroupMeta->ColumnChunk(session->ColumnIndices[i])->num_values();
                if (values != rowGroupMeta->num_rows()) {
                    return std::unexpected(TStringBuilder() << "Parquet column '" << ColumnMeta[i].Name
                        << "' has " << values << " values in row group " << rowGroup
                        << ", which has " << rowGroupMeta->num_rows() << " rows");
                }
            }
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
        try {
            return CollectRowGroups();
        } catch (const std::exception&) {
            return rowGroups; // the engine sees fewer row groups than it has planned
        }
    }

    TVector<TRowGroupInfo> CollectRowGroups() const {
        TVector<TRowGroupInfo> rowGroups;

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

    arrow::MemoryPool* GetMemoryPool() override {
        return &Memory;
    }

    std::shared_ptr<parquet::FileMetaData> GetFileMetadata() const override {
        return Session && Session->FileReader ? Session->FileReader->parquet_reader()->metadata() : nullptr;
    }

    ui64 GetFooterMemoryEstimate() const override {
        return Session ? Session->FooterMemoryEstimate : 0;
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

        return Guarded<std::expected<void, TString>>([this, rowGroupIndex] {
            return OpenRowGroups({static_cast<int>(rowGroupIndex)});
        });
    }

    void ResetRowGroup() override {
        if (!Session) {
            return;
        }

        Session->RowGroups.clear();
        Session->CurrentGroup = 0;
        Session->ColumnReaders.clear();
        Session->BatchSchema.reset();
        Session->DecodedInGroup = 0;
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
        return Guarded<std::expected<TParsedBatch, TString>>([&] {
            return ProcessNextBatchUnguarded(pool, addRow, maxDataBytes);
        });
    }

    std::expected<TParsedBatch, TString> ProcessNextBatchUnguarded(
        TMemoryPool& pool,
        const IDataParser::TAddRowFn& addRow,
        ui64 maxDataBytes)
    {
        Y_UNUSED(pool); // Arrow owns the decoded data; cells point into the record batch

        if (!Session || Session->ColumnReaders.empty()) {
            return TParsedBatch{};
        }

        TImportParquetRowWriter rowWriter(addRow, ColumnMeta, KeyCount);
        // A backup holds cells in their stored form (DyNumber and JsonDocument are binary):
        // the converter's text conversions stay off.
        NArrow::TArrowToYdbConverter converter(
            YdbSchema, rowWriter, /*allowInfDouble=*/false, /*withConversion=*/false);
        const ui64 firstRow = Session->RowsRead;
        const ui64 decodeBytes = maxDataBytes ? maxDataBytes : DefaultDecodeBytes;

        // Makes sure HeldBatch holds unread rows; false once the row group is exhausted.
        const auto fetch = [this, decodeBytes]() -> std::expected<bool, TString> {
            while (!Session->HeldBatch) {
                auto decoded = DecodeNext(Session->BatchRows);
                if (!decoded) {
                    if (!Session->Memory->HasRefused()) {
                        return std::unexpected(std::move(decoded.error()));
                    }
                    if (auto result = ReopenWithSmallerBatches(); !result) {
                        return std::unexpected(std::move(result.error()));
                    }
                    continue;
                }
                auto batch = std::move(*decoded);
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

            // budget not used up: the next batch, or the end of the row group
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
    // Casts coerced columns back to the expected Arrow type; a value that does not fit
    // fails the import.
    std::expected<std::shared_ptr<arrow::RecordBatch>, TString> CastCoercedColumns(
        std::shared_ptr<arrow::RecordBatch> batch) const
    {
        // the cast's memory is within the decoding limit too
        Session->Memory->ResetRefused();
        arrow::compute::ExecContext context(Session->Memory);

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

    // Names a row by its zero-based position in the file, the way Parquet tools count.
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

    // The rows of a row group by the footer.
    ui64 RowsOfGroup(size_t index) const {
        const auto metadata = Session->FileReader->parquet_reader()->metadata();
        return static_cast<ui64>(Max<int64_t>(metadata->RowGroup(Session->RowGroups[index])->num_rows(), 0));
    }

    // The rows decoded from the open row groups so far.
    ui64 DecodedRows() const {
        ui64 rows = Session->DecodedInGroup;
        for (size_t i = 0; i < Session->CurrentGroup; ++i) {
            rows += RowsOfGroup(i);
        }
        return rows;
    }

    // Opens a reader for every column of the table over the current row group.
    std::expected<void, TString> OpenColumnReaders() {
        Session->ColumnReaders.clear();
        const std::vector<int> rowGroup = {Session->RowGroups[Session->CurrentGroup]};

        arrow::FieldVector fields;
        fields.reserve(Session->ColumnIndices.size());
        for (const int column : Session->ColumnIndices) {
            std::unique_ptr<arrow::RecordBatchReader> reader;
            if (auto st = Session->FileReader->GetRecordBatchReader(rowGroup, {column}, &reader); !st.ok()) {
                return std::unexpected(TStringBuilder()
                    << "failed to get parquet record batch reader: " << st.ToString());
            }
            fields.push_back(reader->schema()->field(0));
            Session->ColumnReaders.push_back(std::move(reader));
        }
        Session->BatchSchema = arrow::schema(std::move(fields));

        return {};
    }

    // One decode of a column: the given rows, or fewer at its end. Arrow splits only a
    // column above 2 GiB; the pieces are joined.
    std::expected<std::shared_ptr<arrow::Array>, TString> ReadColumn(size_t column, i64 rows) {
        arrow::ArrayVector pieces;
        i64 length = 0;
        while (length < rows) {
            std::shared_ptr<arrow::RecordBatch> piece;
            if (auto st = Session->ColumnReaders[column]->ReadNext(&piece); !st.ok()) {
                return std::unexpected(TStringBuilder() << "failed to read parquet column '"
                    << ColumnMeta[column].Name << "': " << st.ToString());
            }
            if (!piece) {
                break;
            }
            if (piece->num_rows() > 0) {
                length += piece->num_rows();
                pieces.push_back(piece->column(0));
            }
        }

        if (pieces.empty()) {
            return nullptr;
        }
        if (pieces.size() == 1) {
            return std::move(pieces.front());
        }
        auto joined = arrow::Concatenate(pieces, Session->Memory);
        if (!joined.ok()) {
            return std::unexpected(TStringBuilder() << "failed to join the pieces of parquet column '"
                << ColumnMeta[column].Name << "': " << joined.status().ToString());
        }
        return std::move(*joined);
    }

    // Decodes the next rows column by column and puts them together; columns of different
    // lengths are an error (Arrow would read out of bounds). Null at the end of the row group.
    std::expected<std::shared_ptr<arrow::RecordBatch>, TString> ReadBatch(i64 rows) {
        Session->Memory->ResetRefused();
        // The readers take the batch size for every batch they decode.
        Session->FileReader->set_batch_size(rows);

        arrow::ArrayVector columns;
        columns.reserve(Session->ColumnReaders.size());
        i64 length = 0;
        for (size_t i = 0; i < Session->ColumnReaders.size(); ++i) {
            auto column = ReadColumn(i, rows);
            if (!column) {
                return std::unexpected(std::move(column.error()));
            }

            const i64 columnLength = *column ? (*column)->length() : 0;
            if (i == 0) {
                length = columnLength;
            } else if (columnLength != length) {
                return std::unexpected(TStringBuilder() << "Parquet column '" << ColumnMeta[i].Name
                    << "' has " << columnLength << " rows where column '" << ColumnMeta[0].Name
                    << "' has " << length << ", from " << DescribeRow(DecodedRows()));
            }
            if (*column) {
                columns.push_back(std::move(*column));
            }
        }

        if (length == 0) {
            return nullptr;
        }
        return arrow::RecordBatch::Make(Session->BatchSchema, length, std::move(columns));
    }

    // The readers must have given all the rows the footer states; fewer is a crafted file.
    std::expected<void, TString> CheckRowGroupRead() const {
        const ui64 rows = RowsOfGroup(Session->CurrentGroup);
        if (Session->DecodedInGroup != rows) {
            return std::unexpected(TStringBuilder() << "Parquet row group "
                << Session->RowGroups[Session->CurrentGroup] << " has " << rows
                << " rows by its footer, but " << Session->DecodedInGroup << " were read");
        }
        return {};
    }

    // Decodes the next rows of the open row groups; null once the last one is read to its end.
    std::expected<std::shared_ptr<arrow::RecordBatch>, TString> DecodeNext(i64 rows) {
        while (true) {
            auto batch = ReadBatch(rows);
            if (!batch) {
                return batch;
            }
            if (*batch) {
                Session->DecodedInGroup += (*batch)->num_rows();
                return batch;
            }

            if (auto result = CheckRowGroupRead(); !result) {
                return std::unexpected(std::move(result.error()));
            }
            if (Session->CurrentGroup + 1 == Session->RowGroups.size()) {
                return nullptr;
            }
            ++Session->CurrentGroup;
            Session->DecodedInGroup = 0;
            if (auto result = OpenColumnReaders(); !result) {
                return std::unexpected(std::move(result.error()));
            }
        }
    }

    // A batch did not fit the decoding memory. The readers cannot go on after a failed read,
    // so the row groups are reopened, the rows already emitted are decoded again and dropped,
    // and reading goes on with batches that stay below the failed one.
    std::expected<void, TString> ReopenWithSmallerBatches() {
        const i64 failedRows = Session->BatchRows;
        if (failedRows == 1) {
            return std::unexpected(TStringBuilder() << "Parquet " << DescribeRow(Session->RowsRead)
                << " cannot be decoded within " << Session->Memory->GetLimit()
                << " bytes of memory (twice RestoreReadBufferSizeLimit)");
        }

        Session->CurrentGroup = 0;
        Session->DecodedInGroup = 0;
        if (auto result = OpenColumnReaders(); !result) {
            return result;
        }

        ui64 skipped = 0;
        for (size_t i = 0; skipped < Session->RowsRead; ++i) {
            const ui64 left = Session->RowsRead - skipped;
            const ui64 rows = i < Session->DecodedBatches.size()
                ? Min<ui64>(Session->DecodedBatches[i], left)
                : 1;

            auto batch = DecodeNext(static_cast<i64>(rows));
            if (!batch) {
                return std::unexpected(TStringBuilder()
                    << "failed to read parquet rows again: " << batch.error());
            }
            if (!*batch || static_cast<ui64>((*batch)->num_rows()) > left) {
                return std::unexpected(TStringBuilder() << "parquet rows read again differ from those read before "
                    << DescribeRow(Session->RowsRead));
            }
            skipped += (*batch)->num_rows();
        }

        Session->BatchRows = 1;
        Session->BatchRowsLimit = failedRows / 2;
        return {};
    }

    std::expected<void, TString> OpenRowGroups(std::vector<int> rowGroupIndices) {
        ResetRowGroup();

        Session->RowGroups = std::move(rowGroupIndices);
        if (Session->RowGroups.empty()) {
            return {}; // nothing to read
        }
        if (auto result = OpenColumnReaders(); !result) {
            ResetRowGroup();
            return result;
        }

        return {};
    }

private:
    const ui64 BufferSizeLimit; // what a parsed footer may take, 0 = no limit
    const ui64 DecodeMemoryLimit; // 0 = no limit
    TDecodeMemoryPool Memory; // declared before Session, which holds memory of it
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
