#include "vector_data_generator.h"

namespace NYdbWorkload {

TWorkloadVectorFilesDataInitializer::TWorkloadVectorFilesDataInitializer(const TVectorWorkloadParams& params)
    : TWorkloadDataInitializerBase("files", "Import vectors from files", params)
    , Params(params)
{ }

void TWorkloadVectorFilesDataInitializer::ConfigureOpts(NLastGetopt::TOpts& opts) {
    opts.AddLongOption('i', "input",
            "File or Directory with dataset. If directory is set, all its available files will be used. "
            "Supports zipped and unzipped csv, tsv files and parquet ones that may be downloaded here: "
            "https://huggingface.co/datasets/Cohere/wikipedia-22-12-simple-embeddings. "
            "For better perfomanse you may split it to some parts for parrallel upload."
        ).Required().StoreResult(&DataFiles);
}

TBulkDataGeneratorList TWorkloadVectorFilesDataInitializer::DoGetBulkInitialData() {
    return {
        std::make_shared<TDataGenerator>(*this, Params.TableName, 0, Params.TableName, DataFiles, Params.GetColumns(), TDataGenerator::EPortionSizeUnit::Line)
    };
}

} // namespace NYdbWorkload
