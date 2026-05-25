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
        ui32 keyCount,
        ui64& pendingBytes,
        ui64& pendingRows)
        : AddRowFn(addRow)
        , KeyCount(keyCount)
        , PendingBytes(pendingBytes)
        , PendingRows(pendingRows)
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

private:
    const IDataParser::TAddRowFn& AddRowFn;
    const ui32 KeyCount;
    ui64& PendingBytes;
    ui64& PendingRows;
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
    bool Configure(
        const TTableInfo& tableInfo,
        const NKikimrSchemeOp::TTableDescription& scheme,
        TString& error) override {
        Y_UNUSED(error);
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

        return true;
    }

    bool HasOpenFile() const override {
        return static_cast<bool>(Session);
    }

    bool OpenFile(TStringBuf data, TString& error) override {
        if (data.empty()) {
            ResetFile();
            return true;
        }

        auto buffer = std::make_shared<arrow::Buffer>(
            reinterpret_cast<const uint8_t*>(data.data()),
            static_cast<int64_t>(data.size()));
        return OpenFile(std::make_shared<arrow::io::BufferReader>(buffer), error);
    }

    bool OpenFile(std::shared_ptr<arrow::io::RandomAccessFile> source, TString& error) override {
        ResetFile();

        if (!source) {
            return true;
        }

        Session = std::make_unique<TParquetFileSession>();
        Session->Source = std::move(source);

        parquet::arrow::FileReaderBuilder builder;
        if (auto st = builder.Open(Session->Source); !st.ok()) {
            error = TStringBuilder() << "failed to open parquet file: " << st.ToString();
            ResetFile();
            return false;
        }

        builder.properties(parquet::ArrowReaderProperties(/*use_threads*/ false));

        if (auto st = builder.Build(&Session->FileReader); !st.ok()) {
            error = TStringBuilder() << "failed to build parquet reader: " << st.ToString();
            ResetFile();
            return false;
        }

        std::shared_ptr<arrow::Schema> schema;
        if (auto st = Session->FileReader->GetSchema(&schema); !st.ok()) {
            error = TStringBuilder() << "failed to read parquet schema: " << st.ToString();
            ResetFile();
            return false;
        }

        for (auto&& col : ColumnMeta) {
            if (schema->GetFieldIndex(std::string(col.Name)) < 0) {
                error = TStringBuilder() << "column '" << col.Name << "' not found in parquet schema";
                ResetFile();
                return false;
            }
        }

        std::vector<int> rowGroupIndices(Session->FileReader->num_row_groups());
        std::iota(rowGroupIndices.begin(), rowGroupIndices.end(), 0);

        if (auto st = Session->FileReader->GetRecordBatchReader(rowGroupIndices, &Session->BatchReader); !st.ok()) {
            error = TStringBuilder() << "failed to get parquet record batch reader: " << st.ToString();
            ResetFile();
            return false;
        }

        return true;
    }

    bool ProcessNextBatch(
        TMemoryPool& pool,
        ui64& pendingBytes,
        ui64& pendingRows,
        const IDataParser::TAddRowFn& addRow,
        TString& error) override {
        Y_UNUSED(pool);

        if (!Session || !Session->BatchReader) {
            return false;
        }

        Session->RowWriter = MakeHolder<TImportParquetRowWriter>(addRow, KeyCount, pendingBytes, pendingRows);
        Session->RowWriter->SetColumnMeta(ColumnMeta);
        Session->Converter = MakeHolder<NArrow::TArrowToYdbConverter>(YdbSchema, *Session->RowWriter);

        std::shared_ptr<arrow::RecordBatch> batch;
        if (Session->HeldBatch) {
            batch = std::move(Session->HeldBatch);
        } else {
            while (true) {
                if (auto st = Session->BatchReader->ReadNext(&batch); !st.ok()) {
                    error = TStringBuilder() << "failed to read parquet record batch: " << st.ToString();
                    return false;
                }

                if (!batch) {
                    ResetFile();
                    return false;
                }

                if (batch->num_rows() > 0) {
                    break;
                }
            }
        }

        if (!Session->Converter->Process(*batch, error)) {
            ResetFile();
            return false;
        }

        while (true) {
            std::shared_ptr<arrow::RecordBatch> nextBatch;
            if (auto st = Session->BatchReader->ReadNext(&nextBatch); !st.ok()) {
                error = TStringBuilder() << "failed to read parquet record batch: " << st.ToString();
                ResetFile();
                return false;
            }

            if (!nextBatch) {
                return false;
            }

            if (nextBatch->num_rows() > 0) {
                Session->HeldBatch = std::move(nextBatch);
                return true;
            }
        }
    }

    void ResetFile() override {
        Session.reset();
    }

    bool ParseBlock(
        TStringBuf data,
        TMemoryPool& pool,
        ui64& pendingBytes,
        ui64& pendingRows,
        const TAddRowFn& addRow,
        TString& error) override {
        if (!OpenFile(data, error)) {
            return false;
        }

        while (ProcessNextBatch(pool, pendingBytes, pendingRows, addRow, error)) {}

        const bool ok = error.empty();
        ResetFile();
        return ok;
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
