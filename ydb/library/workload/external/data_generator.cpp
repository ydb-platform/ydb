#include "data_generator.h"
#include <ydb/library/workload/benchmark_base/data_generator.h>
#include <ydb/library/yaml_json/yaml_to_json.h>
#include <contrib/libs/yaml-cpp/include/yaml-cpp/node/parse.h>
#include <util/stream/file.h>


namespace NYdbWorkload {

namespace NExternal {

    TExternalWorkloadDataInitializer::TExternalWorkloadDataInitializer(const TExternalWorkloadParams& params)
        : TWorkloadDataInitializerBase("dir", "Upload external dataset from directory", params)
        , Params(params)
    {}

    void TExternalWorkloadDataInitializer::InitTableColumns() {
        const auto createPath = Params.GetDataPath() / "create";
        if (!createPath.Exists()) {
            return;
        }
        TVector<TFsPath> children;
        createPath.List(children);
        for (const auto& c: children) {
            if (c.IsFile() && (c.GetExtension() == "yaml" || c.GetExtension() == "yml")) {
                TFileInput fi(c.GetPath());
                const auto yaml = YAML::Load(fi.ReadAll().c_str());
                const auto json = NKikimr::NYaml::Yaml2Json(yaml, true);
                for (const auto& table: json["tables"].GetArray()) {
                    const auto& columns = table["columns"].GetArray();
                    const auto& tName = table["name"].GetString();
                    TablesForUpload.emplace(tName);
                    auto& header = TableColumns[tName];
                    header.reserve(columns.size());
                    for (const auto& c: columns) {
                        header.emplace_back(c["name"].GetString());
                    }
                }
            }
        }
    }

    void TExternalWorkloadDataInitializer::ConfigureOpts(NLastGetopt::TOpts& opts) {
        TWorkloadDataInitializerBase::ConfigureOpts(opts);
        InitTableColumns();
        opts.AddLongOption('i', "input",
            "Directory with dataset. Must contain subdir for every table. "
            "Now supported zipped and unzipped csv and tsv files. "
            "For better perfomanse you may split it to some parts for parrallel upload."
            ).StoreResult(&DataPath);
        opts.AddLongOption("tables", "Commaseparated list of tables for generate. Empty means all tables.\n"
                "Enabled tables: " + JoinSeq(", ", TablesForUpload))
            .Handler1T<TStringBuf>([this, allTables = TablesForUpload](TStringBuf arg) {
                TablesForUpload.clear();
                StringSplitter(arg).SplitBySet(", ").SkipEmpty().AddTo(&TablesForUpload);
                for (const auto& table: TablesForUpload) {
                    if (!allTables.contains(table)) {
                        throw yexception() << "Ivalid table for generate: " << table;
                    }
                }
            });
        opts.AddLongOption("without-header", "Data files does not contain header").StoreTrue(&WithoutHeaders).NoArgument();
    }

    TBulkDataGeneratorList TExternalWorkloadDataInitializer::DoGetBulkInitialData() {
        auto tables = TablesForUpload;
        if (tables.empty()) {
            for (const auto& [k, v]: TableColumns) {
                tables.emplace(k);
            }
        }
        TBulkDataGeneratorList gens;
        for (const auto& table: tables) {
            const auto path = DataPath / table;
            if (!path.Exists()) {
                throw yexception() << "Data for table " << table << " not found";
            }
            gens.emplace_back(std::make_shared<TDataGenerator>(*this, table, 0, table, path, WithoutHeaders ? TableColumns[table] : TVector<TString>()));
        }
        return gens;
    }

}

}