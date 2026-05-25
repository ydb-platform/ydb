#ifndef KIKIMR_DISABLE_S3_OPS

#include "import_data_parser.h"

#include <ydb/core/formats/arrow/converter.h>
#include <ydb/core/scheme/scheme_types_proto.h>

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
    ui32 KeyOrder = Max<ui32>();
};

class TImportParquetRowWriter final : public NArrow::IRowWriter {
public:
    TImportParquetRowWriter(
        const IDataParser::TAddRowFn& addRow,
        ui32 keyCount)
        : AddRowFn(addRow)
        , KeyCount(keyCount)
    {
    }

    void AddRow(const TConstArrayRef<TCell>& cells) override {
        TVector<TCell> keys;
        keys.resize(KeyCount);
        TVector<TCell> values;
        values.reserve(cells.size() > KeyCount ? cells.size() - KeyCount : 0);

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

    void SetColumnMeta(const TVector<TColumnMeta>& columnMeta) {
        ColumnMeta = columnMeta;
    }

    IDataParser::TParsedData GetParsedData() const {
        return {
            .DataBytes = PendingBytes,
            .Rows = PendingRows,
        };
    }

private:
    const IDataParser::TAddRowFn& AddRowFn;
    const ui32 KeyCount;
    ui64 PendingBytes = 0;
    ui64 PendingRows = 0;
    TVector<TColumnMeta> ColumnMeta;
};

class TParquetFileSession {
public:
    std::shared_ptr<arrow::io::RandomAccessFile> Source;
    std::unique_ptr<parquet::arrow::FileReader> FileReader;
    std::unique_ptr<arrow::RecordBatchReader> BatchReader;
    std::shared_ptr<arrow::RecordBatch> HeldBatch;
    THolder<NArrow::TArrowToYdbConverter> Converter;
    THolder<TImportParquetRowWriter> RowWriter;
};

class TParquetDataParser final
    : public IDataParser
    , public IParquetStreamParser {
public:
    std::expected<void, TString> Configure(
        const TTableInfo& tableInfo,
        const NKikimrSchemeOp::TTableDescription& scheme) override {
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

            ColumnMeta.push_back(std::move(meta));

            YdbSchema.emplace_back(column.GetName(), typeInfoMod.TypeInfo);
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

        Session = std::make_unique<TParquetFileSession>();
        Session->Source = std::move(source);

        parquet::arrow::FileReaderBuilder builder;
        if (auto st = builder.Open(Session->Source); !st.ok()) {
            ResetFile();
            return std::unexpected(TStringBuilder() << "failed to open parquet file: " << st.ToString());
        }

        builder.properties(parquet::ArrowReaderProperties(/*use_threads*/ false));

        if (auto st = builder.Build(&Session->FileReader); !st.ok()) {
            ResetFile();
            return std::unexpected(TStringBuilder() << "failed to build parquet reader: " << st.ToString());
        }

        std::shared_ptr<arrow::Schema> schema;
        if (auto st = Session->FileReader->GetSchema(&schema); !st.ok()) {
            ResetFile();
            return std::unexpected(TStringBuilder() << "failed to read parquet schema: " << st.ToString());
        }

        for (auto&& col : ColumnMeta) {
            if (schema->GetFieldIndex(std::string(col.Name)) < 0) {
                ResetFile();
                return std::unexpected(TStringBuilder()
                    << "column '" << col.Name << "' not found in parquet schema");
            }
        }

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
        Session->Converter.Reset();
        Session->RowWriter.Reset();
    }

private:
    std::expected<void, TString> OpenRowGroups(std::vector<int> rowGroupIndices) {
        ResetRowGroup();

        if (auto st = Session->FileReader->GetRecordBatchReader(rowGroupIndices, &Session->BatchReader); !st.ok()) {
            ResetRowGroup();
            return std::unexpected(TStringBuilder()
                << "failed to get parquet record batch reader: " << st.ToString());
        }

        return {};
    }

public:
    std::expected<TParsedBatch, TString> ProcessNextBatch(
        TMemoryPool& pool,
        const IDataParser::TAddRowFn& addRow) override {
        Y_UNUSED(pool);

        if (!Session || !Session->BatchReader) {
            return TParsedBatch{};
        }

        Session->RowWriter = MakeHolder<TImportParquetRowWriter>(addRow, KeyCount);
        Session->RowWriter->SetColumnMeta(ColumnMeta);
        Session->Converter = MakeHolder<NArrow::TArrowToYdbConverter>(YdbSchema, *Session->RowWriter);

        const auto makeResult = [this](bool hasMore) {
            const auto parsedData = Session->RowWriter->GetParsedData();
            return TParsedBatch{
                .DataBytes = parsedData.DataBytes,
                .Rows = parsedData.Rows,
                .HasMore = hasMore,
            };
        };

        std::shared_ptr<arrow::RecordBatch> batch;
        if (Session->HeldBatch) {
            batch = std::move(Session->HeldBatch);
        } else {
            while (true) {
                if (auto st = Session->BatchReader->ReadNext(&batch); !st.ok()) {
                    return std::unexpected(TStringBuilder()
                        << "failed to read parquet record batch: " << st.ToString());
                }

                if (!batch) {
                    ResetRowGroup();
                    return TParsedBatch{};
                }

                if (batch->num_rows() > 0) {
                    break;
                }
            }
        }

        TString error;
        if (!Session->Converter->Process(*batch, error)) {
            ResetRowGroup();
            return std::unexpected(std::move(error));
        }

        while (true) {
            std::shared_ptr<arrow::RecordBatch> nextBatch;
            if (auto st = Session->BatchReader->ReadNext(&nextBatch); !st.ok()) {
                ResetRowGroup();
                return std::unexpected(TStringBuilder()
                    << "failed to read parquet record batch: " << st.ToString());
            }

            if (!nextBatch) {
                return makeResult(false);
            }

            if (nextBatch->num_rows() > 0) {
                Session->HeldBatch = std::move(nextBatch);
                return makeResult(true);
            }
        }
    }

    void ResetFile() override {
        Session.reset();
    }

    std::expected<TParsedData, TString> ParseBlock(
        TStringBuf data,
        TMemoryPool& pool,
        const TAddRowFn& addRow) override {
        if (auto result = OpenFile(data); !result) {
            return std::unexpected(std::move(result.error()));
        }

        TParsedData parsedData;
        while (true) {
            auto result = ProcessNextBatch(pool, addRow);
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
    TVector<TColumnMeta> ColumnMeta;
    std::vector<std::pair<TString, NScheme::TTypeInfo>> YdbSchema;
    ui32 KeyCount = 0;
    std::unique_ptr<TParquetFileSession> Session;
};

} // anonymous namespace

IDataParser::TPtr CreateParquetDataParser() {
    return MakeHolder<TParquetDataParser>();
}

IParquetStreamParser* AsParquetStreamParser(IDataParser* parser) {
    return dynamic_cast<IParquetStreamParser*>(parser);
}

} // namespace NKikimr::NDataShard

#endif // KIKIMR_DISABLE_S3_OPS
