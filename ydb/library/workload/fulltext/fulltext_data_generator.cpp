#include "fulltext_data_generator.h"
#include "markov_model_evaluator.h"

#include <ydb/library/workload/abstract/colors.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/array/array_binary.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/array/builder_binary.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/array/builder_primitive.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/ipc/writer.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/record_batch.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/type.h>

#include <library/cpp/colorizer/colors.h>

#include <library/cpp/streams/factory/open_by_signature/factory.h>

#include <util/folder/path.h>
#include <util/stream/file.h>
#include <util/string/builder.h>

#include <random>

namespace NYdbWorkload {

    namespace {

        ui64 CountFileLines(const TFsPath& path) {
            ui64 count = 0;
            TString line;
            auto input = OpenOwnedMaybeCompressedInput(MakeHolder<TFileInput>(path));
            while (input->ReadLine(line)) {
                ++count;
            }
            return count > 0 ? count - 1 : 0;
        }

        ui64 CountDirLines(const TFsPath& path) {
            ui64 lineCount = 0;
            const TFsPath dataPath(path);
            if (dataPath.IsDirectory()) {
                TVector<TFsPath> children;
                dataPath.List(children);
                for (const auto& child : children) {
                    if (child.IsFile()) {
                        lineCount += CountFileLines(child);
                    }
                }
            } else {
                lineCount = CountFileLines(dataPath);
            }
            return lineCount;
        }

        TString BuildIndexDDL(const TFulltextWorkloadParams& params) {
            TStringBuilder ddl;
            ddl << "ALTER TABLE `" << params.GetFullTableName(params.TableName.c_str()) << "`\n";
            ddl << "ADD INDEX `" << params.IndexName << "`\n";
            ddl << "GLOBAL SYNC USING " << params.IndexType << "\n";
            ddl << "ON (`text`)\n";

            ddl << "WITH (";
            if (params.IndexParams.empty()) {
                ddl << "tokenizer=standard";
            } else {
                bool first = true;
                for (const auto& param : params.IndexParams) {
                    if (!first) {
                        ddl << ", ";
                    } else {
                        first = false;
                    }
                    ddl << param;
                }
            }
            ddl << ");";
            return ddl;
        }

        class TRandomTextGenerator final: public IBulkDataGenerator {
        private:
            static constexpr size_t PORTION_SIZE = 1000;

            const TFulltextWorkloadParams& Params;
            const ui64 RowCount;
            const size_t MinSentenceLen;
            const size_t MaxSentenceLen;

            TMarkovModelEvaluator Evaluator;
            TAdaptiveLock Lock;
            ui64 DoneRows = 0;

        public:
            TRandomTextGenerator(const TFulltextWorkloadParams& params, ui64 rowCount, const TString& modelPath,
                                 size_t minSentenceLen, size_t maxSentenceLen)
                : IBulkDataGenerator(params.TableName, rowCount)
                , Params(params)
                , RowCount(rowCount)
                , MinSentenceLen(minSentenceLen)
                , MaxSentenceLen(maxSentenceLen)
                , Evaluator(TMarkovModelEvaluator::LoadFromFile(modelPath))
            {}

            virtual TDataPortions GenerateDataPortion() override {
                ui64 batchStart = 0;
                size_t currentBatchSize = 0;
                with_lock(Lock) {
                    if (DoneRows >= RowCount) {
                        return {};
                    }
                    batchStart = DoneRows;
                    currentBatchSize = Min<ui64>(PORTION_SIZE, RowCount - DoneRows);
                    DoneRows += currentBatchSize;
                }

                thread_local std::mt19937 rng(std::random_device{}());
                std::uniform_int_distribution<size_t> sentenceLenDist(MinSentenceLen, MaxSentenceLen);

                arrow::UInt64Builder idsBuilder;
                arrow::BinaryBuilder textsBuilder;

                for (size_t i = 0; i < currentBatchSize; ++i) {
                    if (const auto status = idsBuilder.Append(batchStart + i); !status.ok()) {
                        ythrow yexception() << status.ToString();
                    }

                    const size_t targetLen = sentenceLenDist(rng);
                    TString text = Evaluator.GenerateSentence(targetLen, rng);

                    if (const auto status = textsBuilder.Append(std::string(text)); !status.ok()) {
                        ythrow yexception() << status.ToString();
                    }
                }

                std::shared_ptr<arrow::UInt64Array> idColumn;
                if (const auto status = idsBuilder.Finish(&idColumn); !status.ok()) {
                    ythrow yexception() << status.ToString();
                }

                std::shared_ptr<arrow::BinaryArray> textColumn;
                if (const auto status = textsBuilder.Finish(&textColumn); !status.ok()) {
                    ythrow yexception() << status.ToString();
                }

                const auto schema = arrow::schema({
                    arrow::field("id", arrow::uint64()),
                    arrow::field("text", arrow::binary()),
                });
                const auto recordBatch = arrow::RecordBatch::Make(
                    schema,
                    currentBatchSize,
                    {idColumn, textColumn}
                );

                auto serializedBatch = arrow::ipc::SerializeRecordBatch(*recordBatch, arrow::ipc::IpcWriteOptions{});
                if (!serializedBatch.ok()) {
                    ythrow yexception() << serializedBatch.status().ToString();
                }
                auto serializedSchema = arrow::ipc::SerializeSchema(*schema);
                if (!serializedSchema.ok()) {
                    ythrow yexception() << serializedSchema.status().ToString();
                }

                TDataPortion::TArrow arrowData(
                    (*serializedBatch)->ToString(),
                    (*serializedSchema)->ToString()
                );

                return {MakeIntrusive<TDataPortion>(
                    Params.GetFullTableName(Params.TableName.c_str()),
                    std::move(arrowData),
                    currentBatchSize
                )};
            }
        };

    } // namespace

    TFulltextDataInitializerBase::TFulltextDataInitializerBase(const TString& name, const TString& description, const TFulltextWorkloadParams& params)
        : TWorkloadDataInitializerBase(name, description, params)
        , FulltextParams(params)
    {}

    int TFulltextDataInitializerBase::PostImport() {
        const TString ddlQuery = BuildIndexDDL(FulltextParams);

        Cout << "Building fulltext index ..." << Endl;
        const auto result = FulltextParams.QueryClient->RetryQuerySync([&ddlQuery](NYdb::NQuery::TSession session) {
            return session.ExecuteQuery(ddlQuery, NYdb::NQuery::TTxControl::NoTx()).GetValueSync();
        });
        NYdb::NStatusHelpers::ThrowOnErrorOrPrintIssues(result);
        Cout << "Building fulltext index ...Ok" << Endl;

        return EXIT_SUCCESS;
    }

    TFulltextFilesDataInitializer::TFulltextFilesDataInitializer(const TFulltextWorkloadParams& params)
        : TFulltextDataInitializerBase("files", "Import fulltext data from files and build fulltext index", params)
    {}

    void TFulltextFilesDataInitializer::ConfigureOpts(NLastGetopt::TOpts& opts) {
        NColorizer::TColors colors = GetColors(Cout);

        TStringBuilder inputDescription;
        inputDescription
            << "File or directory with the dataset to import. Only two columns are imported: "
            << colors.BoldColor() << "id" << colors.OldColor() << " and "
            << colors.BoldColor() << "text" << colors.OldColor() << ". "
            << "If a directory is set, all supported files inside will be used."
            << "\nSupported formats: CSV/TSV (zipped or unzipped) and Parquet.";

        opts.AddLongOption('i', "input", inputDescription)
            .RequiredArgument("PATH")
            .Required()
            .StoreResult(&DataFiles);
    }

    TBulkDataGeneratorList TFulltextFilesDataInitializer::DoGetBulkInitialData() {
        const ui64 lineCount = CountDirLines(DataFiles);
        return {
            std::make_shared<TDataGenerator>(
                *this,
                FulltextParams.TableName,
                lineCount,
                FulltextParams.TableName,
                DataFiles,
                TVector<TString>(),
                TDataGenerator::EPortionSizeUnit::Line)};
    }

    TFulltextGeneratorDataInitializer::TFulltextGeneratorDataInitializer(const TFulltextWorkloadParams& params)
        : TFulltextDataInitializerBase("generator", "Generate random text and build fulltext index", params)
    {}

    void TFulltextGeneratorDataInitializer::ConfigureOpts(NLastGetopt::TOpts& opts) {
        opts.AddLongOption("rows", "Number of rows to generate")
            .RequiredArgument("NUMBER")
            .DefaultValue(RowCount)
            .StoreResult(&RowCount);
        opts.AddLongOption('m', "model", "Path to Markov chain model file (.tsv.gz)")
            .RequiredArgument("PATH")
            .Required()
            .StoreResult(&ModelPath);
        opts.AddLongOption("min-sentence-len", "Minimum number of words in a generated sentence")
            .RequiredArgument("NUMBER")
            .DefaultValue(MinSentenceLen)
            .StoreResult(&MinSentenceLen);
        opts.AddLongOption("max-sentence-len", "Maximum number of words in a generated sentence")
            .RequiredArgument("NUMBER")
            .DefaultValue(MaxSentenceLen)
            .StoreResult(&MaxSentenceLen);
    }

    TBulkDataGeneratorList TFulltextGeneratorDataInitializer::DoGetBulkInitialData() {
        Cout << "Generating " << RowCount << " random text rows..." << Endl;
        return {std::make_shared<TRandomTextGenerator>(FulltextParams, RowCount, ModelPath, MinSentenceLen, MaxSentenceLen)};
    }

} // namespace NYdbWorkload
