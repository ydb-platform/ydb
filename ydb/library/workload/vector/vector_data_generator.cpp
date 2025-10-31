#include "vector_data_generator.h"

#include <contrib/libs/apache/arrow/cpp/src/arrow/array/array_binary.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/array/array_nested.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/array/array_primitive.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/array/builder_binary.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/compute/cast.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/io/memory.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/ipc/dictionary.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/ipc/reader.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/ipc/writer.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/record_batch.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/type.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/type_fwd.h>

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

    void ConvertArrow(TDataPortion::TArrow* data) {
        const auto [schema, batch] = Deserialize(data);

        // extract
        const auto idColumn = batch->GetColumnByName("id");
        const auto embeddingColumn = batch->GetColumnByName(EmbeddingSourceField);
        const auto embeddingListColumn = dynamic_cast<arrow::ListArray*>(embeddingColumn.get());

        // conversion
        const auto newIdColumn = arrow::compute::Cast(idColumn, arrow::uint64()).ValueOrDie().make_array();

        arrow::StringBuilder builder;
        for (int64_t row = 0; row < batch->num_rows(); ++row) {
            auto embeddingAsFloats = static_cast<const arrow::FloatArray*>(embeddingListColumn->value_slice(row).get());

            std::string serialized(embeddingAsFloats->length() * sizeof(float) + 1, '\0');
            float* float_bytes = reinterpret_cast<float*>(serialized.data());
            for (int64_t i = 0; i < embeddingAsFloats->length(); ++i) {
                float_bytes[i] = embeddingAsFloats->Value(i);
            }
            serialized.back() = '\x01';

            if (const auto status = builder.Append(serialized); !status.ok()) {
                status.Abort();
            }
        }

        std::shared_ptr<arrow::StringArray> newEmbeddingColumn;
        if (const auto status = builder.Finish(&newEmbeddingColumn); !status.ok()) {
            status.Abort();
        }

        // serialize
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

    void Convert(TDataPortion::TDataType& data) {
        if (auto* value = std::get_if<TDataPortion::TArrow>(&data)) {
            ConvertArrow(value);
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
            Convert(portion->MutableData());
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
            "Parquet: leave only required fields, cast to expected types, convert list of floats into serialized ydb representation."
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
