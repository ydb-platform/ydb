#include "data_generator.h"

#include <ydb/library/workload/benchmark_base/data_generator.h>
#include <ydb/library/yaml_json/yaml_to_json.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/status/status.h>
#include <contrib/libs/yaml-cpp/include/yaml-cpp/node/parse.h>
#include <library/cpp/resource/resource.h>
#include <library/cpp/streams/factory/open_by_signature/factory.h>
#include <util/generic/algorithm.h>
#include <util/stream/file.h>

namespace NYdbWorkload {

namespace {

// The original JOB CSV files escape quotes and backslashes with a backslash.
// BulkUpsert accepts RFC 4180 quoting, so normalize complete records first.
class TJobCsvGenerator final: public IBulkDataGenerator {
public:
    TJobCsvGenerator(const TWorkloadBaseParams& params, TGeneratorStateProcessor* state,
            const TString& table, const TFsPath& path, const TVector<TString>& columns)
        : IBulkDataGenerator(table, 0)
        , Params(params)
        , State(state)
        , Table(params.GetFullTableName(table.c_str()))
        , Source(path.RealPath())
        , Header(JoinSeq(",", columns) + "\n")
        , Input(OpenOwnedMaybeCompressedInput(MakeHolder<TFileInput>(path)))
    {
        if (State && State->GetState().contains(Source)) {
            const auto resume = State->GetState().at(Source).Position;
            while (Position < resume) {
                const auto skipped = Input->Skip(resume - Position);
                Y_ENSURE(skipped, "IMDb CSV file is shorter than its saved import position: " << Source);
                Position += skipped;
            }
        }
    }

    TDataPortions GenerateDataPortion() override {
        auto guard = Guard(Lock);
        const auto start = Position;
        TString data = Header;
        ui64 rows = 0;
        while (rows < Max<ui64>(1, Params.BulkSize) && ReadRecord(data)) {
            ++rows;
        }
        if (!rows) {
            return {};
        }
        return {MakeIntrusive<TDataPortionWithState>(State, Table, Source,
            TDataPortion::TCsv(std::move(data), TWorkloadGeneratorBase::CsvFormatString),
            start, Position - start)};
    }

private:
    bool ReadRecord(TString& output) {
        bool quoted = false;
        TString line;
        while (const auto bytes = Input->ReadLine(line)) {
            Position += bytes;
            for (size_t i = 0; i < line.size(); ++i) {
                const auto c = line[i];
                if (quoted && c == '\\' && i + 1 < line.size()
                        && (line[i + 1] == '"' || line[i + 1] == '\\')) {
                    const auto escaped = line[++i];
                    if (escaped == '"') {
                        output += '"';
                    }
                    output += escaped;
                } else if (c == '"') {
                    output += c;
                    if (quoted && i + 1 < line.size() && line[i + 1] == '"') {
                        output += line[++i];
                    } else {
                        quoted = !quoted;
                    }
                } else {
                    output += c;
                }
            }
            output += '\n';
            if (!quoted) {
                return true;
            }
        }
        Y_ENSURE(!quoted, "Unterminated quoted field in IMDb CSV file " << Source << " at byte " << Position);
        return false;
    }

    const TWorkloadBaseParams& Params;
    TGeneratorStateProcessor* State;
    const TString Table;
    const TString Source;
    const TString Header;
    THolder<IInputStream> Input;
    ui64 Position = 0;
    TAdaptiveLock Lock;
};

bool IsSupportedFile(const TFsPath& path) {
    if (!path.IsFile()) {
        return false;
    }
    const auto name = path.GetName();
    return name.EndsWith(".csv") || name.EndsWith(".csv.gz")
        || name.EndsWith(".tsv") || name.EndsWith(".tsv.gz")
#if !defined(_win32_)
        || name.EndsWith(".parquet")
#endif
        ;
}

} // namespace

TImdbWorkloadDataInitializer::TImdbWorkloadDataInitializer(const TImdbWorkloadParams& params)
    : TWorkloadDataInitializerBase("files", "Upload the IMDb dataset and collect column statistics", params)
    , ImdbParams(params)
{}

int TImdbWorkloadDataInitializer::PostImport() {
    Y_ENSURE(Params.QueryClient, "A query client is required to analyze IMDb tables.");
    const TImdbWorkloadGenerator generator(ImdbParams);
    for (const auto& query: generator.GetAnalyzeQueries()) {
        Cout << query << " ..." << Endl;
        const auto result = Params.QueryClient->RetryQuerySync([&query](NYdb::NQuery::TSession session) {
            return session.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).GetValueSync();
        });
        NYdb::NStatusHelpers::ThrowOnErrorOrPrintIssues(result);
        Cout << "Analyze ...Ok" << Endl;
    }
    return EXIT_SUCCESS;
}

void TImdbWorkloadDataInitializer::ConfigureOpts(NLastGetopt::TOpts& opts) {
    TWorkloadDataInitializerBase::ConfigureOpts(opts);
    opts.AddLongOption('i', "input",
        "Directory containing <table>.csv, .csv.gz, .tsv, .tsv.gz or .parquet files, "
        "or a subdirectory per table with files in these formats. CSV and TSV files must have no header.")
        .RequiredArgument("PATH").StoreResult(&DataFiles);
}

TBulkDataGeneratorList TImdbWorkloadDataInitializer::DoGetBulkInitialData() {
    if (!DataFiles.IsDefined() || !DataFiles.IsDirectory()) {
        throw yexception() << "'input' must be an existing directory with the IMDb dataset.";
    }
    const auto yaml = YAML::Load(NResource::Find("imdb/schema.yaml").c_str());
    const auto json = NKikimr::NYaml::Yaml2Json(yaml, true);
    TBulkDataGeneratorList result;
    for (const auto& table: json["tables"].GetArray()) {
        const auto& name = table["name"].GetString();
        TVector<TFsPath> paths;
        const auto directory = DataFiles / name;
        if (directory.IsDirectory()) {
            directory.List(paths);
        } else {
            for (const auto* extension: {".csv", ".csv.gz", ".tsv", ".tsv.gz", ".parquet"}) {
                const auto path = DataFiles / (name + extension);
                if (path.Exists()) {
                    paths.push_back(path);
                }
            }
        }
        if (paths.empty()) {
            throw yexception() << "Data for IMDb table " << name << " not found in " << DataFiles;
        }
        Sort(paths, [](const TFsPath& lhs, const TFsPath& rhs) {
            return lhs.GetPath() < rhs.GetPath();
        });
        TVector<TString> header;
        for (const auto& column: table["columns"].GetArray()) {
            header.push_back(column["name"].GetString());
        }
        for (const auto& path: paths) {
            if (!IsSupportedFile(path)) {
                throw yexception() << "Unsupported IMDb data file: " << path;
            }
            if (path.GetName().EndsWith(".csv") || path.GetName().EndsWith(".csv.gz")) {
                result.push_back(std::make_shared<TJobCsvGenerator>(Params, StateProcessor.Get(), name, path, header));
            } else {
                result.push_back(std::make_shared<TDataGenerator>(*this, name, 0, name, path, header));
            }
        }
    }
    return result;
}

} // namespace NYdbWorkload
