#include "vector_data_generator.h"

#include <ydb/library/formats/arrow/csv/converter/csv_arrow.h>
#include <ydb/library/yql/udfs/common/knn/knn-serializer-shared.h>

#include <ydb/public/api/protos/ydb_formats.pb.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/array/array_binary.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/array/array_nested.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/array/array_primitive.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/array/builder_binary.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/chunked_array.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/compute/cast.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/csv/api.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/csv/reader.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/csv/writer.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/io/memory.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/ipc/dictionary.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/ipc/reader.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/ipc/writer.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/record_batch.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/table.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/type.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/type_fwd.h>

#include <util/stream/mem.h>

namespace NYdbWorkload {

namespace {

class TTransformingDataGenerator final: public IBulkDataGenerator {
private:
    std::shared_ptr<IBulkDataGenerator> InnerDataGenerator;
    const TString EmbeddingSourceField;

private:
    static std::pair<std::shared_ptr<arrow::Schema>, std::shared_ptr<arrow::RecordBatch>> Deserialize(TDataPortion::TArrow* data) {
        arrow::ipc::DictionaryMemo dictionary;

        arrow::io::BufferReader schemaBuffer(arrow::util::string_view(data->Schema.data(), data->Schema.size()));
        const std::shared_ptr<arrow::Schema> schema = arrow::ipc::ReadSchema(&schemaBuffer, &dictionary).ValueOrDie();

        arrow::io::BufferReader recordBatchBuffer(arrow::util::string_view(data->Data.data(), data->Data.size()));
        const std::shared_ptr<arrow::RecordBatch> recordBatch = arrow::ipc::ReadRecordBatch(schema, &dictionary, {}, &recordBatchBuffer).ValueOrDie();

        return std::make_pair(schema, recordBatch);
    }

    std::shared_ptr<arrow::Table> Deserialize(TDataPortion::TCsv* data) {
        Ydb::Formats::CsvSettings csvSettings;
        if (Y_UNLIKELY(!csvSettings.ParseFromString(data->FormatString))) {
            ythrow yexception() << "Unable to parse CsvSettings";
        }

        arrow::csv::ReadOptions readOptions = arrow::csv::ReadOptions::Defaults();
        readOptions.skip_rows = csvSettings.skip_rows();
        if (data->Data.size() > NKikimr::NFormats::TArrowCSV::DEFAULT_BLOCK_SIZE) {
            ui32 blockSize = NKikimr::NFormats::TArrowCSV::DEFAULT_BLOCK_SIZE;
            blockSize *= data->Data.size() / blockSize + 1;
            readOptions.block_size = blockSize;
        }

        arrow::csv::ParseOptions parseOptions = arrow::csv::ParseOptions::Defaults();
        const auto& quoting = csvSettings.quoting();
        if (Y_UNLIKELY(quoting.quote_char().length() > 1)) {
            ythrow yexception() << "Cannot read CSV: Wrong quote char '" << quoting.quote_char() << "'";
        }
        const char qchar = quoting.quote_char().empty() ? '"' : quoting.quote_char().front();
        parseOptions.quoting = false;
        parseOptions.quote_char = qchar;
        parseOptions.double_quote = !quoting.double_quote_disabled();
        if (csvSettings.delimiter()) {
            if (Y_UNLIKELY(csvSettings.delimiter().size() != 1)) {
                ythrow yexception() << "Cannot read CSV: Invalid delimitr in csv: " << csvSettings.delimiter();
            }
            parseOptions.delimiter = csvSettings.delimiter().front();
        }

        arrow::csv::ConvertOptions convertOptions = arrow::csv::ConvertOptions::Defaults();
        if (csvSettings.null_value()) {
            convertOptions.null_values = { std::string(csvSettings.null_value().data(), csvSettings.null_value().size()) };
            convertOptions.strings_can_be_null = true;
            convertOptions.quoted_strings_can_be_null = false;
        }

        auto bufferReader = std::make_shared<arrow::io::BufferReader>(arrow::util::string_view(data->Data.data(), data->Data.size()));
        auto csvReader = arrow::csv::TableReader::Make(
            arrow::io::default_io_context(),
            bufferReader,
            readOptions,
            parseOptions,
            convertOptions
        ).ValueOrDie();

        return csvReader->Read().ValueOrDie();
    }

    void TransformArrow(TDataPortion::TArrow* data) {
        const auto [schema, batch] = Deserialize(data);

        // id
        const auto idColumn = batch->GetColumnByName("id");
        const auto newIdColumn = arrow::compute::Cast(idColumn, arrow::uint64()).ValueOrDie().make_array();

        // embedding
        const auto embeddingColumn = std::dynamic_pointer_cast<arrow::ListArray>(batch->GetColumnByName(EmbeddingSourceField));
        arrow::StringBuilder newEmbeddingsBuilder;
        for (int64_t row = 0; row < batch->num_rows(); ++row) {
            const auto embeddingFloatList = std::static_pointer_cast<arrow::FloatArray>(embeddingColumn->value_slice(row));

            TStringBuilder buffer;
            NKnnVectorSerialization::TSerializer<float> serializer(&buffer.Out);
            for (int64_t i = 0; i < embeddingFloatList->length(); ++i) {
                serializer.HandleElement(embeddingFloatList->Value(i));
            }
            serializer.Finish();

            if (const auto status = newEmbeddingsBuilder.Append(buffer.MutRef()); !status.ok()) {
                status.Abort();
            }
        }
        std::shared_ptr<arrow::StringArray> newEmbeddingColumn;
        if (const auto status = newEmbeddingsBuilder.Finish(&newEmbeddingColumn); !status.ok()) {
            status.Abort();
        }

        const auto newSchema = arrow::schema({
            arrow::field("id", arrow::uint64()),
            arrow::field("embedding", arrow::utf8()),
        });
        const auto newRecordBatch = arrow::RecordBatch::Make(
            newSchema,
            batch->num_rows(),
            {
                newIdColumn,
                newEmbeddingColumn,
            }
        );
        data->Schema = arrow::ipc::SerializeSchema(*newSchema).ValueOrDie()->ToString();
        data->Data = arrow::ipc::SerializeRecordBatch(*newRecordBatch, arrow::ipc::IpcWriteOptions{}).ValueOrDie()->ToString();
    }

    void TransformCsv(TDataPortion::TCsv* data) {
        const auto table = Deserialize(data);

        // id
        const auto idColumn = table->GetColumnByName("id");

        // embedding
        const auto embeddingColumn = table->GetColumnByName(EmbeddingSourceField);
        arrow::StringBuilder newEmbeddingsBuilder;
        for (int64_t row = 0; row < table->num_rows(); ++row) {
            const auto embeddingListString = std::static_pointer_cast<arrow::StringArray>(embeddingColumn->Slice(row, 1)->chunk(0))->Value(0);
            
            TStringBuf buffer(embeddingListString.data(), embeddingListString.size());
            buffer.SkipPrefix("[");
            buffer.ChopSuffix("]");
            TMemoryInput input(buffer);

            TStringBuilder newEmbeddingBuilder;
            NKnnVectorSerialization::TSerializer<float> serializer(&newEmbeddingBuilder.Out);
            while (!input.Exhausted()) {
                float val;
                input >> val;
                input.Skip(1);
                serializer.HandleElement(val);
            }
            serializer.Finish();

            if (const auto status = newEmbeddingsBuilder.Append(newEmbeddingBuilder.MutRef()); !status.ok()) {
                status.Abort();
            }
        }
        std::shared_ptr<arrow::StringArray> newEmbeddingColumn;
        if (const auto status = newEmbeddingsBuilder.Finish(&newEmbeddingColumn); !status.ok()) {
            status.Abort();
        }

        const auto newSchema = arrow::schema({
            arrow::field("id", arrow::uint64()),
            arrow::field("embedding", arrow::utf8()),
        });
        const auto newTable = arrow::Table::Make(
            newSchema,
            {
                idColumn,
                arrow::ChunkedArray::Make({newEmbeddingColumn}).ValueOrDie(),
            }
        );
        auto outputStream = arrow::io::BufferOutputStream::Create().ValueOrDie();
        if (const auto status = arrow::csv::WriteCSV(*newTable, arrow::csv::WriteOptions::Defaults(), outputStream.get()); !status.ok()) {
            status.Abort();
        }
        data->FormatString = "";
        data->Data = outputStream->Finish().ValueOrDie()->ToString();
    }

    void Transform(TDataPortion::TDataType& data) {
        if (auto* value = std::get_if<TDataPortion::TArrow>(&data)) {
            TransformArrow(value);
        }
        if (auto* value = std::get_if<TDataPortion::TCsv>(&data)) {
            TransformCsv(value);
        }
    }

public:
    TTransformingDataGenerator(std::shared_ptr<IBulkDataGenerator> innerDataGenerator, const TString embeddingSourceField)
        : IBulkDataGenerator(innerDataGenerator->GetName(), innerDataGenerator->GetSize())
        , InnerDataGenerator(innerDataGenerator)
        , EmbeddingSourceField(embeddingSourceField)
    {}

    virtual TDataPortions GenerateDataPortion() override {
        TDataPortions portions = InnerDataGenerator->GenerateDataPortion();
        for (auto portion : portions) {
            Transform(portion->MutableData());
        }
        return portions;
    }
};

}

TWorkloadVectorFilesDataInitializer::TWorkloadVectorFilesDataInitializer(const TVectorWorkloadParams& params)
    : TWorkloadDataInitializerBase("files", "Import vectors from files", params)
    , Params(params)
{ }

void TWorkloadVectorFilesDataInitializer::ConfigureOpts(NLastGetopt::TOpts& opts) {
    opts.AddLongOption('i', "input",
            "File or Directory with dataset. If directory is set, all its available files will be used. "
            "Supports zipped and unzipped csv, tsv files and parquet ones that may be downloaded here: "
            "https://huggingface.co/datasets/Cohere/wikipedia-22-12-simple-embeddings. "
            "For better performance you may split it into some parts for parallel upload."
        ).Required().StoreResult(&DataFiles);
    opts.AddLongOption('t', "transform",
            "Perform transformation of input data. "
            "Parquet: leave only required fields, cast to expected types, convert list of floats into serialized representation. "
            "CSV: leave only required fields, parse float list from string and serialize. "
            "Reference for embedding serialization: https://ydb.tech/docs/yql/reference/udf/list/knn#functions-convert"
        ).Optional().StoreTrue(&DoTransform);
    opts.AddLongOption(
            "transform-embedding-source-field",
            "Specify field that contains list of floats to be converted into YDB embedding format."
        ).DefaultValue(EmbeddingSourceField).StoreResult(&EmbeddingSourceField);
}

TBulkDataGeneratorList TWorkloadVectorFilesDataInitializer::DoGetBulkInitialData() {
    auto dataGenerator = std::make_shared<TDataGenerator>(
        *this,
        Params.TableName,
        0,
        Params.TableName,
        DataFiles,
        Params.GetColumns(),
        TDataGenerator::EPortionSizeUnit::Line
    );

    if (DoTransform) {
        return {std::make_shared<TTransformingDataGenerator>(dataGenerator, EmbeddingSourceField)};
    }
    return {dataGenerator};
}

} // namespace NYdbWorkload
