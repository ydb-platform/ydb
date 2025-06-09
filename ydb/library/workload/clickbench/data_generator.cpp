#include "data_generator.h"
#include <ydb/library/workload/benchmark_base/data_generator.h>
#include <ydb/library/yaml_json/yaml_to_json.h>
#include <library/cpp/resource/resource.h>
#include <contrib/libs/yaml-cpp/include/yaml-cpp/node/parse.h>

namespace NYdbWorkload {

TClickbenchWorkloadDataInitializerGenerator::TClickbenchWorkloadDataInitializerGenerator(const TClickbenchWorkloadParams& params)
    : TWorkloadDataInitializerBase("files", "Upload Clickbench dataset from files.", params)
{}

void TClickbenchWorkloadDataInitializerGenerator::ConfigureOpts(NLastGetopt::TOpts& opts) {
    TWorkloadDataInitializerBase::ConfigureOpts(opts);
    opts.AddLongOption('i', "input",
        "File or Directory with clickbench dataset. If directory is set, all its available files will be used."
        "Now supported zipped and unzipped csv and tsv files, that may be downloaded here:  https://datasets.clickhouse.com/hits_compatible/hits.csv.gz, "
        "https://datasets.clickhouse.com/hits_compatible/hits.tsv.gz and parquet one: https://datasets.clickhouse.com/hits_compatible/hits.csv.gz, "
        "https://datasets.clickhouse.com/hits_compatible/hits.parquet. "
        "For better perfomanse you may split it to some parts for parrallel upload."
        ).StoreResult(&DataFiles);
}

TBulkDataGeneratorList TClickbenchWorkloadDataInitializerGenerator::DoGetBulkInitialData() {
    if (!DataFiles.IsDefined()) {
        throw yexception() << "'input' parameter must be set.";
    }
    if (!DataFiles.Exists()) {
        throw yexception() << "Invalid 'input' parameter, path does not exist: " << DataFiles;
    }
    const auto yaml = YAML::Load(NResource::Find("click_bench_schema.yaml").c_str());
    const auto json = NKikimr::NYaml::Yaml2Json(yaml, true);
    const auto& columns = json["table"]["columns"].GetArray();
    TVector<TString> header;
    header.reserve(columns.size());
    for (const auto& c: columns) {
        header.emplace_back(c["name"].GetString());
    }
    return {std::make_shared<TDataGenerator>(*this, "hits", DataSetSize, "", DataFiles, header)};
}

}