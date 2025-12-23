#include "vector_data_generator.h"

#include <ydb/library/formats/arrow/csv/converter/csv_arrow.h>
#include <ydb/library/yql/udfs/common/knn/knn-serializer-shared.h>

#include <ydb/public/api/protos/ydb_formats.pb.h>
#include <ydb/library/workload/abstract/colors.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/array/array_binary.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/array/array_nested.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/array/array_primitive.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/array/builder_binary.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/array/builder_primitive.h>
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
#include <library/cpp/colorizer/colors.h>

#include <util/stream/mem.h>


namespace NYdbWorkload {

namespace {

class TDataGeneratorWrapper final: public IBulkDataGenerator {
private:
    const std::shared_ptr<IBulkDataGenerator> InnerDataGenerator;
    const TString EmbeddingColumnName;

private:
    static std::pair<std::shared_ptr<arrow::Schema>, std::shared_ptr<arrow::RecordBatch>> DeserializeArrow(TDataPortion::TArrow* data) {
        arrow::ipc::DictionaryMemo dictionary;

        arrow::io::BufferReader schemaBuffer(arrow::util::string_view(data->Schema.data(), data->Schema.size()));
        const std::shared_ptr<arrow::Schema> schema = arrow::ipc::ReadSchema(&schemaBuffer, &dictionary).ValueOrDie();

        arrow::io::BufferReader recordBatchBuffer(arrow::util::string_view(data->Data.data(), data->Data.size()));
        const std::shared_ptr<arrow::RecordBatch> recordBatch = arrow::ipc::ReadRecordBatch(schema, &dictionary, {}, &recordBatchBuffer).ValueOrDie();

        return std::make_pair(schema, recordBatch);
    }

    static std::shared_ptr<arrow::Table> DeserializeCsv(TDataPortion::TCsv* data) {
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
        parseOptions.quoting = !quoting.disabled();
        parseOptions.quote_char = qchar;
        parseOptions.double_quote = !quoting.double_quote_disabled();
        if (csvSettings.delimiter()) {
            if (Y_UNLIKELY(csvSettings.delimiter().size() != 1)) {
                ythrow yexception() << "Cannot read CSV: Invalid delimiter in csv: " << csvSettings.delimiter();
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

    void CanonizeArrow(TDataPortion::TArrow* data) {
        const auto [schema, batch] = DeserializeArrow(data);

        std::vector<std::shared_ptr<arrow::Array>> resultColumns;

        // id
        const auto idColumn = batch->GetColumnByName("id");
        if (idColumn == nullptr) {
            ythrow yexception() << "Cannot find id column";
        }
        resultColumns.push_back(arrow::compute::Cast(idColumn, arrow::uint64()).ValueOrDie().make_array());

        // embedding
        const auto embeddingColumn = batch->GetColumnByName(EmbeddingColumnName);
        if (embeddingColumn == nullptr) {
            ythrow yexception() << "Cannot find embedding column '" << EmbeddingColumnName << "'";
        }
        if (embeddingColumn->type()->Equals(arrow::binary())) {
            resultColumns.push_back(embeddingColumn);
        } else if (embeddingColumn->type_id() == arrow::Type::LIST && std::static_pointer_cast<arrow::ListType>(embeddingColumn->type())->value_type()->Equals(arrow::float32())) {
            const std::shared_ptr<arrow::ListArray> embeddingListColumn = std::static_pointer_cast<arrow::ListArray>(embeddingColumn);
            arrow::StringBuilder newEmbeddingsBuilder;
            for (int64_t row = 0; row < batch->num_rows(); ++row) {
                const auto embeddingFloatList = std::static_pointer_cast<arrow::FloatArray>(embeddingListColumn->value_slice(row));

                TStringBuilder buffer;
                NKnnVectorSerialization::TSerializer<float> serializer(&buffer.Out);
                for (int64_t i = 0; i < embeddingFloatList->length(); ++i) {
                    serializer.HandleElement(embeddingFloatList->Value(i));
                }
                serializer.Finish();

                if (const auto status = newEmbeddingsBuilder.Append(buffer.MutRef()); !status.ok()) {
                    ythrow yexception() << status.ToString();
                }
            }
            std::shared_ptr<arrow::StringArray> newEmbeddingColumn;
            if (const auto status = newEmbeddingsBuilder.Finish(&newEmbeddingColumn); !status.ok()) {
                ythrow yexception() << status.ToString();
            }
            resultColumns.push_back(std::move(newEmbeddingColumn));
        } else {
            ythrow yexception() << "Only binary and list[float32] arrow types are supported for embedding column";
        }

        const auto newSchema = arrow::schema({
            arrow::field("id", arrow::uint64()),
            arrow::field("embedding", arrow::binary()),
        });
        const auto newRecordBatch = arrow::RecordBatch::Make(
            newSchema,
            batch->num_rows(),
            resultColumns
        );
        data->Schema = arrow::ipc::SerializeSchema(*newSchema).ValueOrDie()->ToString();
        data->Data = arrow::ipc::SerializeRecordBatch(*newRecordBatch, arrow::ipc::IpcWriteOptions{}).ValueOrDie()->ToString();
    }

    void CanonizeCsv(TDataPortion::TCsv* data) {
        const auto table = DeserializeCsv(data);

        std::vector<std::shared_ptr<arrow::ChunkedArray>> resultColumns;

        // id
        const auto idColumn = table->GetColumnByName("id");
        if (idColumn == nullptr) {
            ythrow yexception() << "Cannot find id column";
        }
        resultColumns.push_back(idColumn);

        // embedding
        const auto embeddingColumn = table->GetColumnByName(EmbeddingColumnName);
        if (embeddingColumn == nullptr) {
            ythrow yexception() << "Cannot find embedding column '" << EmbeddingColumnName << "'";
        }

        if (Y_UNLIKELY(embeddingColumn->type()->id() != arrow::Type::STRING)) {
            ythrow yexception() << "For CSV/TSV embedding column must be string";
        }

        arrow::StringBuilder newEmbeddingsBuilder;
        for (int64_t row = 0; row < table->num_rows(); ++row) {
            const auto embeddingListString = std::static_pointer_cast<arrow::StringArray>(embeddingColumn->Slice(row, 1)->chunk(0))->Value(0);
            
            TStringBuf buffer(embeddingListString.data(), embeddingListString.size());
            buffer.SkipPrefix("\"");
            buffer.ChopSuffix("\"");
            buffer.SkipPrefix("[");
            buffer.ChopSuffix("]");

            TStringBuilder newEmbeddingBuilder;
            const auto splitter = StringSplitter(buffer.begin(), buffer.end()).SplitByFunc([](char c) {
                return c == ',' || std::isspace(c);
            }).SkipEmpty();
            NKnnVectorSerialization::TSerializer<float> serializer(&newEmbeddingBuilder.Out);
            for (auto it = splitter.begin(); it != splitter.end(); ++it) {
                float value;
                if (!TryFromString(it->Token(), value)) {
                    ythrow yexception() << "Cannot parse float from embedding element '" << it->Token() << "'";
                }
                serializer.HandleElement(value);
            }
            serializer.Finish();

            if (const auto status = newEmbeddingsBuilder.Append(newEmbeddingBuilder.MutRef()); !status.ok()) {
                ythrow yexception() << status.ToString();
            }
        }
        std::shared_ptr<arrow::StringArray> newEmbeddingColumn;
        if (const auto status = newEmbeddingsBuilder.Finish(&newEmbeddingColumn); !status.ok()) {
            ythrow yexception() << status.ToString();
        }
        resultColumns.push_back(arrow::ChunkedArray::Make({newEmbeddingColumn}).ValueOrDie());

        const auto newTable = arrow::Table::Make(arrow::schema({
            arrow::field("id", arrow::uint64()),
            arrow::field("embedding", arrow::binary()),
        }), resultColumns);
        auto outputStream = arrow::io::BufferOutputStream::Create().ValueOrDie();
        if (const auto status = arrow::csv::WriteCSV(*newTable, arrow::csv::WriteOptions::Defaults(), outputStream.get()); !status.ok()) {
            ythrow yexception() << status.ToString();
        }
        data->FormatString = "";
        data->Data = outputStream->Finish().ValueOrDie()->ToString();
    }

    void CanonizePortion(TDataPortion::TDataType& data) {
        if (auto* value = std::get_if<TDataPortion::TArrow>(&data)) {
            CanonizeArrow(value);
        } else if (auto* value = std::get_if<TDataPortion::TCsv>(&data)) {
            CanonizeCsv(value);
        }
    }

public:
    TDataGeneratorWrapper(const std::shared_ptr<IBulkDataGenerator> innerDataGenerator, const TString embeddingColumnName)
        : IBulkDataGenerator(innerDataGenerator->GetName(), innerDataGenerator->GetSize())
        , InnerDataGenerator(innerDataGenerator)
        , EmbeddingColumnName(embeddingColumnName)
    {}

    virtual TDataPortions GenerateDataPortion() override {
        TDataPortions portions = InnerDataGenerator->GenerateDataPortion();
        for (auto& portion : portions) {
            CanonizePortion(portion->MutableData());
        }
        return portions;
    }
};

class TRandomDataGenerator final: public IBulkDataGenerator {
private:
    static constexpr size_t PORTION_SIZE = 8192;

    const TVectorWorkloadParams& Params;
    const NVector::TVectorOpts& VectorOpts;
    const size_t RowCount;

    std::mt19937 RandomGenerator;
    std::uniform_real_distribution<float> Distribution;
    TAdaptiveLock Lock;

    size_t DoneRows = 0;

private:
    template<typename T>
    TStringBuilder GenerateEmbedding() {
        TStringBuilder buffer;
        NKnnVectorSerialization::TSerializer<T> serializer(&buffer.Out);
        for (size_t j = 0; j < VectorOpts.VectorDimension; ++j) {
            if constexpr (std::is_same<T, float>::value) {
                serializer.HandleElement(Distribution(RandomGenerator) * 2 - 1);
            } else if constexpr (std::is_same<T, uint8_t>::value) {
                serializer.HandleElement(Distribution(RandomGenerator) * (UINT8_MAX + 1));
            } else if constexpr (std::is_same<T, int8_t>::value) {
                serializer.HandleElement(Distribution(RandomGenerator) * (INT8_MAX - INT8_MIN + 1) + INT8_MIN);
            } else if constexpr (std::is_same<T, bool>::value) {
                serializer.HandleElement(Distribution(RandomGenerator) >= 0.5);
            } else {
                static_assert(false, "Unsupported type");
            }
        }
        serializer.Finish();

        return buffer;
    }

public:
    TRandomDataGenerator(const TVectorWorkloadParams& params, const NVector::TVectorOpts& vectorOpts, const size_t rowCount, const uint32_t randomSeed)
        : IBulkDataGenerator(params.TableOpts.Name, rowCount)
        , Params(params)
        , VectorOpts(vectorOpts)
        , RowCount(rowCount)
        , RandomGenerator(randomSeed)
        , Distribution(0.0f, 1.0f)
    { }

    virtual TDataPortions GenerateDataPortion() override {
        // Sequential generation is required to ensure reproducibility for fixed seed value.
        with_lock(Lock) {
            std::vector<std::shared_ptr<arrow::Array>> resultColumns;

            arrow::UInt64Builder idsBuilder;
            arrow::StringBuilder embeddingsBuilder;

            std::function<TStringBuilder()> generateEmbedding;
            if (VectorOpts.VectorType == "float") {
                generateEmbedding = [this]() { return GenerateEmbedding<float>(); };
            } else if (VectorOpts.VectorType == "uint8") {
                generateEmbedding = [this]() { return GenerateEmbedding<uint8_t>(); };
            } else if (VectorOpts.VectorType == "int8") {
                generateEmbedding = [this]() { return GenerateEmbedding<int8_t>(); };
            } else if (VectorOpts.VectorType == "bit") {
                generateEmbedding = [this]() { return GenerateEmbedding<bool>(); };
            } else {
                ythrow yexception() << "Unknown vector type: " << VectorOpts.VectorType;
            }

            size_t currentBatchSize;
            for (currentBatchSize = 0; currentBatchSize < PORTION_SIZE && DoneRows < RowCount; ++currentBatchSize, ++DoneRows) {
                if (const auto status = idsBuilder.Append(static_cast<uint64_t>(DoneRows)); !status.ok()) {
                    ythrow yexception() << status.ToString();
                }

                TStringBuilder buffer = generateEmbedding();
                if (const auto status = embeddingsBuilder.Append(buffer.MutRef()); !status.ok()) {
                    ythrow yexception() << status.ToString();
                }
            }
            if (currentBatchSize == 0) {
                return {};
            }

            std::shared_ptr<arrow::UInt64Array> newIdColumn;
            if (const auto status = idsBuilder.Finish(&newIdColumn); !status.ok()) {
                ythrow yexception() << status.ToString();
            }
            resultColumns.push_back(std::move(newIdColumn));

            std::shared_ptr<arrow::StringArray> newEmbeddingColumn;
            if (const auto status = embeddingsBuilder.Finish(&newEmbeddingColumn); !status.ok()) {
                ythrow yexception() << status.ToString();
            }
            resultColumns.push_back(std::move(newEmbeddingColumn));

            const auto schema = arrow::schema({
                arrow::field("id", arrow::uint64()),
                arrow::field("embedding", arrow::binary()),
            });
            const auto recordBatch = arrow::RecordBatch::Make(
                schema,
                currentBatchSize,
                resultColumns
            );

            TDataPortion::TArrow arrowData(
                arrow::ipc::SerializeRecordBatch(*recordBatch, arrow::ipc::IpcWriteOptions{}).ValueOrDie()->ToString(),
                arrow::ipc::SerializeSchema(*schema).ValueOrDie()->ToString()
            );

            return {MakeIntrusive<TDataPortion>(Params.GetFullTableName(Params.TableOpts.Name.c_str()), std::move(arrowData), currentBatchSize)};
        }
    }
};

}

TWorkloadVectorFilesDataInitializer::TWorkloadVectorFilesDataInitializer(const TVectorWorkloadParams& params)
    : TWorkloadDataInitializerBase("files", "Import vectors from files", params)
    , Params(params)
{ }

void TWorkloadVectorFilesDataInitializer::ConfigureOpts(NLastGetopt::TOpts& opts) {
    NColorizer::TColors colors = GetColors(Cout);

    TStringBuilder inputDescription;
    inputDescription
        << "File or directory with the dataset to import. Only two columns are imported: "
        << colors.BoldColor() << "id" << colors.OldColor() << " and "
        << colors.BoldColor() << "embedding" << colors.OldColor() << ". "
        << "Any additional columns present in the input files (such as extra key columns or prefix columns) will be ignored during import. "
        << "If a directory is set, all supported files inside will be used."
        << "\nSupported formats: CSV/TSV (zipped or unzipped) and Parquet."
        << "\nIf embedding appears to be a list of floats, then it gets converted to YDB binary embedding format."
        << "\nOtherwise embedding must already be binary; "
        << "for CSV/TSV format, embeddings must always be represented as a list of floats e.g., \"[ 1.0 2.0 3.0 ]\" or \"[ 1.0, 2.0, 3.0 ]\"."
        << "\nExample dataset: https://huggingface.co/datasets/Cohere/wikipedia-22-12-simple-embeddings";

    opts.AddLongOption('i', "input", inputDescription)
        .RequiredArgument("PATH")
        .Required()
        .StoreResult(&DataFiles);

    opts.AddLongOption("embedding-column-name", "Alternative source column name for the embedding field in input files.")
        .RequiredArgument("NAME")
        .DefaultValue(EmbeddingColumnName)
        .StoreResult(&EmbeddingColumnName);
}

TBulkDataGeneratorList TWorkloadVectorFilesDataInitializer::DoGetBulkInitialData() {
    const auto basicDataGenerator = std::make_shared<TDataGenerator>(
        *this,
        Params.TableOpts.Name,
        0,
        Params.TableOpts.Name,
        DataFiles,
        Params.GetColumns(),
        TDataGenerator::EPortionSizeUnit::Line
    );

    return {
        std::make_shared<TDataGeneratorWrapper>(basicDataGenerator, EmbeddingColumnName)
    };
}

TWorkloadVectorGenerateDataInitializer::TWorkloadVectorGenerateDataInitializer(const TVectorWorkloadParams& params)
    : TWorkloadDataInitializerBase("generator", "Generate random vectors", params)
    , Params(params)
{ }

void TWorkloadVectorGenerateDataInitializer::ConfigureOpts(NLastGetopt::TOpts& opts) {
    NVector::ConfigureVectorOpts(opts, &VectorOpts);
    opts.AddLongOption( "rows", "Number of rows to generate")
        .RequiredArgument("NUMBER")
        .Required().StoreResult(&RowCount);
    opts.AddLongOption("seed", "Seed for random number generator")
        .RequiredArgument("NUMBER")
        .DefaultValue(RandomSeed)
        .StoreResult(&RandomSeed);
}

TBulkDataGeneratorList TWorkloadVectorGenerateDataInitializer::DoGetBulkInitialData() {
    Cout << "Using random seed: " << RandomSeed << Endl;
    return {std::make_shared<TRandomDataGenerator>(Params, VectorOpts, RowCount, RandomSeed)};
}

} // namespace NYdbWorkload
