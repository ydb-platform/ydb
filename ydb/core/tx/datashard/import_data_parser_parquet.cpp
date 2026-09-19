#ifndef KIKIMR_DISABLE_S3_OPS

#include "import_data_parser.h"

#include <ydb/core/formats/arrow/arrow_helpers.h>
#include <ydb/core/formats/arrow/converter.h>
#include <ydb/core/scheme/scheme_types_proto.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/compute/cast.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/io/memory.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/record_batch.h>
#include <contrib/libs/apache/arrow/cpp/src/parquet/arrow/reader.h>
#include <contrib/libs/apache/arrow/cpp/src/parquet/file_reader.h>

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

// Splits the converter's flat cell row into keys (in key order) and values (in
// scheme order) and forwards them to the engine's addRow.
//
// IRowWriter::AddRow says an implementation must copy the cells: they point
// into the converter's scratch pool and the Arrow arrays and are gone once
// TArrowToYdbConverter::Process moves on. This writer deliberately does not
// copy. IDataParser::TAddRowFn carries the same borrowed-cells contract and
// both sinks (TUploadRowsRequestBuilder serializes, TDirectPartWriter encodes
// into pages) consume the row synchronously inside the call, so a copy here
// would only duplicate theirs. addRow must never defer the cells.
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
    }

    void AddRow(const TConstArrayRef<TCell>& cells) override {
        Y_ENSURE(cells.size() == ColumnMeta.size(),
            "parquet row has " << cells.size() << " cells, expected " << ColumnMeta.size());

        TVector<TCell> keys;
        keys.resize(KeyCount);
        TVector<TCell> values;
        values.reserve(cells.size() - KeyCount);

        for (size_t i = 0; i < cells.size(); ++i) {
            const auto& cell = cells[i];
            PendingBytes += cell.Size();
            if (ColumnMeta[i].KeyOrder != Max<ui32>()) {
                keys[ColumnMeta[i].KeyOrder] = cell;
            } else {
                values.push_back(cell);
            }
        }

        AddRowFn(keys, values);
        ++PendingRows;
    }

    IDataParser::TParsedData GetParsedData() const {
        return {
            .DataBytes = PendingBytes,
            .Rows = PendingRows,
        };
    }

private:
    const IDataParser::TAddRowFn& AddRowFn;
    const TVector<TColumnMeta>& ColumnMeta;
    const ui32 KeyCount;
    ui64 PendingBytes = 0;
    ui64 PendingRows = 0;
};

// The Arrow writer stores some types differently from how it reads them back.
// With Parquet format 1.0 (the writer default, used by the exporter) there is
// no unsigned 32-bit annotation, so uint32 (YDB Uint32, Datetime) is written
// as INT64 and comes back as int64. Such columns are cast back before
// conversion; any other mismatch is an error.
bool IsWriterCoercion(const arrow::DataType& fileType, const arrow::DataType& expectedType) {
    return fileType.id() == arrow::Type::INT64 && expectedType.id() == arrow::Type::UINT32;
}

struct TParquetFileSession {
    std::shared_ptr<arrow::io::RandomAccessFile> Source;
    std::unique_ptr<parquet::arrow::FileReader> FileReader;
    std::vector<int> ColumnIndices; // parquet leaf columns to decode, in scheme order
    std::vector<std::pair<std::string, std::shared_ptr<arrow::DataType>>> CastColumns; // see IsWriterCoercion
    std::unique_ptr<arrow::RecordBatchReader> BatchReader;
    std::shared_ptr<arrow::RecordBatch> HeldBatch; // rows [HeldOffset, num_rows) not yet emitted
    i64 HeldOffset = 0;
    ui64 RowBytesEstimate = 1; // average cell bytes per row of the open row groups
};

class TParquetDataParser final : public IParquetStreamParser {
    // Rows are converted in slices (zero-copy views of a record batch) sized
    // from the estimated row width so that a byte budget is checked before it
    // is exceeded by much, without giving up the converter's row batching.
    static constexpr i64 MaxSliceRows = 1024;

    static i64 SliceRowsFor(ui64 maxDataBytes, ui64 emittedBytes, ui64 rowBytesEstimate) {
        const ui64 budgetLeft = maxDataBytes > emittedBytes ? maxDataBytes - emittedBytes : 0;
        const ui64 rows = budgetLeft / Max<ui64>(rowBytesEstimate, 1);
        return static_cast<i64>(Min<ui64>(Max<ui64>(rows, 1), MaxSliceRows));
    }

public:
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

            YdbSchema.emplace_back(meta.Name, meta.TypeInfo);
            ColumnMeta.push_back(std::move(meta));
        }

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
        session->Source = std::move(source);

        parquet::arrow::FileReaderBuilder builder;
        if (auto st = builder.Open(session->Source); !st.ok()) {
            return std::unexpected(TStringBuilder() << "failed to open parquet file: " << st.ToString());
        }

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

        Session->BatchReader.reset();
        Session->HeldBatch.reset();
        Session->HeldOffset = 0;
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
        NArrow::TArrowToYdbConverter converter(YdbSchema, rowWriter);

        // Makes sure HeldBatch holds unread rows; false once the row group is exhausted.
        const auto fetch = [this]() -> std::expected<bool, TString> {
            while (!Session->HeldBatch) {
                std::shared_ptr<arrow::RecordBatch> batch;
                if (auto st = Session->BatchReader->ReadNext(&batch); !st.ok()) {
                    return std::unexpected(TStringBuilder()
                        << "failed to read parquet record batch: " << st.ToString());
                }
                if (!batch) {
                    return false;
                }
                if (batch->num_rows() > 0) {
                    auto casted = CastCoercedColumns(std::move(batch));
                    if (!casted) {
                        return std::unexpected(std::move(casted.error()));
                    }
                    Session->HeldBatch = std::move(*casted);
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
            const i64 take = maxDataBytes
                ? Min(remaining, SliceRowsFor(maxDataBytes, rowWriter.GetParsedData().DataBytes, Session->RowBytesEstimate))
                : remaining;
            const auto slice = take == held->num_rows()
                ? held
                : held->Slice(Session->HeldOffset, take);

            TString error;
            if (!converter.Process(*slice, error)) {
                ResetRowGroup();
                return std::unexpected(std::move(error));
            }

            Session->HeldOffset += take;
            if (Session->HeldOffset >= held->num_rows()) {
                held.reset();
                Session->HeldOffset = 0;
            }

            if (const auto parsed = rowWriter.GetParsedData(); parsed.Rows) {
                Session->RowBytesEstimate = Max<ui64>(parsed.DataBytes / parsed.Rows, 1);
            }

            if (maxDataBytes && rowWriter.GetParsedData().DataBytes >= maxDataBytes) {
                auto more = fetch();
                if (!more) {
                    ResetRowGroup();
                    return std::unexpected(std::move(more.error()));
                }
                return makeResult(*more);
            }
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
        for (const auto& [name, type] : Session->CastColumns) {
            const int index = batch->schema()->GetFieldIndex(name);
            if (index < 0) {
                return std::unexpected(TStringBuilder() << "column '" << name << "' is missing in a parquet record batch");
            }

            auto casted = arrow::compute::Cast(*batch->column(index), type, arrow::compute::CastOptions::Safe());
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

    std::expected<void, TString> OpenRowGroups(std::vector<int> rowGroupIndices) {
        ResetRowGroup();

        // Initial row-width estimate from the metadata (uncompressed bytes / rows).
        const auto metadata = Session->FileReader->parquet_reader()->metadata();
        ui64 totalBytes = 0;
        ui64 totalRows = 0;
        for (const int index : rowGroupIndices) {
            const auto rowGroup = metadata->RowGroup(index);
            totalBytes += static_cast<ui64>(Max<int64_t>(rowGroup->total_byte_size(), 0));
            totalRows += static_cast<ui64>(Max<int64_t>(rowGroup->num_rows(), 0));
        }
        Session->RowBytesEstimate = totalRows ? Max<ui64>(totalBytes / totalRows, 1) : 1;

        if (auto st = Session->FileReader->GetRecordBatchReader(
                rowGroupIndices, Session->ColumnIndices, &Session->BatchReader); !st.ok())
        {
            ResetRowGroup();
            return std::unexpected(TStringBuilder()
                << "failed to get parquet record batch reader: " << st.ToString());
        }

        return {};
    }

private:
    TVector<TColumnMeta> ColumnMeta;
    std::vector<std::pair<TString, NScheme::TTypeInfo>> YdbSchema;
    ui32 KeyCount = 0;
    std::unique_ptr<TParquetFileSession> Session;
};

} // anonymous namespace

IParquetStreamParser::TPtr CreateParquetDataParser() {
    return MakeHolder<TParquetDataParser>();
}

} // namespace NKikimr::NDataShard

#endif // KIKIMR_DISABLE_S3_OPS
