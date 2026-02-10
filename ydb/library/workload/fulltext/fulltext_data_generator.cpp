#include "fulltext_data_generator.h"
#include <ydb/library/workload/abstract/colors.h>
#include <library/cpp/colorizer/colors.h>
#include <util/string/builder.h>

namespace NYdbWorkload {

TFulltextWorkloadDataInitializer::TFulltextWorkloadDataInitializer(const TFulltextWorkloadParams& params)
    : TWorkloadDataInitializerBase("files", "Import fulltext data from files", params)
    , Params(params)
{ }

void TFulltextWorkloadDataInitializer::ConfigureOpts(NLastGetopt::TOpts& opts) {
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

TBulkDataGeneratorList TFulltextWorkloadDataInitializer::DoGetBulkInitialData() {
    return {
        std::make_shared<TDataGenerator>(
            *this,
            Params.TableName,
            0,
            Params.TableName,
            DataFiles,
            TVector<TString>{"id", "text"},
            TDataGenerator::EPortionSizeUnit::Line
        )
    };
}

} // namespace NYdbWorkload
