#include "clickbench.h"
#include "data_generator.h"

#include <library/cpp/resource/resource.h>
#include <library/cpp/string_utils/csv/csv.h>
#include <util/stream/file.h>
#include <util/random/random.h>

namespace NYdbWorkload {
TClickbenchWorkloadParams::TClickbenchWorkloadParams() {
    SetPath("clickbench/hits");
}

class TExternalVariable {
private:
    TString Id;
    TString Value;
public:
    TExternalVariable() = default;

    TExternalVariable(const TString& id, const TString& value)
        : Id(id)
        , Value(value) {

    }

    const TString& GetId() const {
        return Id;
    }

    const TString& GetValue() const {
        return Value;
    }

    bool DeserializeFromString(const TString& vStr) {
        TStringBuf sb(vStr.data(), vStr.size());
        TStringBuf l, r;
        if (!sb.TrySplit('=', l, r)) {
            Cerr << "Incorrect variables format: have to be a=b, but really have: " << sb << Endl;
            return false;
        }
        Id = l;
        Value = r;
        return true;
    }
};

TClickbenchWorkloadGenerator::TClickbenchWorkloadGenerator(const TClickbenchWorkloadParams& params)
    : TWorkloadGeneratorBase(params)
    , Params(params)
{}

TString TClickbenchWorkloadGenerator::DoGetDDLQueries() const {
    return NResource::Find("click_bench_schema.sql");
}

TQueryInfoList TClickbenchWorkloadGenerator::GetInitialData() {
    return {};
}

TQueryInfoList TClickbenchWorkloadGenerator::GetWorkload(int type) {
    TQueryInfoList result;
    if (type) {
        return result;
    }

    TVector<TString> queries;
    const TMap<ui32, TString> qResults = LoadExternalResults();
    if (Params.GetExternalQueries()) {
        queries = StringSplitter(Params.GetExternalQueries()).Split(';').ToList<TString>();
    } else if (Params.GetExternalQueriesFile()) {
        TFileInput fInput(Params.GetExternalQueriesFile());
        queries = StringSplitter(fInput.ReadAll()).Split(';').ToList<TString>();
    } else if (Params.GetExternalQueriesDir()) {
        TVector<TString> queriesList;
        Params.GetExternalQueriesDir().ListNames(queriesList);
        std::sort(queriesList.begin(), queriesList.end());
        for (auto&& i : queriesList) {
            const TString expectedFileName = "q" + ::ToString(queries.size()) + ".sql";
            Y_ABORT_UNLESS(i == expectedFileName, "incorrect files naming. have to be q<number>.sql where number in [0, N - 1], where N is requests count");
            TFileInput fInput(Params.GetExternalQueriesDir() / expectedFileName);
            queries.emplace_back(fInput.ReadAll());
        }
    } else {
        const auto resourceName = Params.IsCheckCannonical() ? "queries-deterministic.sql" : "click_bench_queries.sql";
        queries = StringSplitter(NResource::Find(resourceName)).Split(';').ToList<TString>();
    }
    auto strVariables = StringSplitter(Params.GetExternalVariablesString()).Split(';').SkipEmpty().ToList<TString>();
    TVector<TExternalVariable> vars;
    for (auto&& i : strVariables) {
        TExternalVariable v;
        Y_ABORT_UNLESS(v.DeserializeFromString(i));
        vars.emplace_back(v);
    }
    vars.emplace_back("table", "`" + Params.GetPath() + "`");
    ui32 resultsUsage = 0;
    for (ui32 i = 0; i < queries.size(); ++i) {
        auto& query = queries[i];
        for (auto&& v : vars) {
            SubstGlobal(query, "{" + v.GetId() + "}", v.GetValue());
        }
        SubstGlobal(query, "$data", "`" + Params.GetPath() + "`");
        result.emplace_back();
        result.back().Query = query;
        if (const auto* res = MapFindPtr(qResults, i)) {
            result.back().ExpectedResult = *res;
            ++resultsUsage;
        }
    }
    Y_ABORT_UNLESS(resultsUsage == qResults.size(), "there are unused files with results in directory");
    return result;
}

TVector<IWorkloadQueryGenerator::TWorkloadType> TClickbenchWorkloadGenerator::GetSupportedWorkloadTypes() const {
    return {TWorkloadType(0, "bench", "Perform benchmark", TWorkloadType::EKind::Benchmark)};
}

TMap<ui32, TString> TClickbenchWorkloadGenerator::LoadExternalResults() const {
    TMap<ui32, TString> result;
    if (Params.GetExternalResultsDir()) {
        TVector<TString> filesList;
        Params.GetExternalResultsDir().ListNames(filesList);
        std::sort(filesList.begin(), filesList.end());
        for (auto&& i : filesList) {
            Y_ABORT_UNLESS(i.StartsWith("q") && i.EndsWith(".result"));
            TStringBuf sb(i.data(), i.size());
            sb.Skip(1);
            sb.Chop(7);
            ui32 qId;
            Y_ABORT_UNLESS(TryFromString<ui32>(sb, qId));
            TFileInput fInput(Params.GetExternalResultsDir() / i);
            result.emplace(qId, fInput.ReadAll());
        }
    } else if (Params.IsCheckCannonical()) {
        for(ui32 qId = 0; qId < 43; ++qId) {
            const auto key = "click_bench_canonical/q" + ToString(qId) + ".result";
            if (!NResource::Has(key)) {
                continue;
            }
            result.emplace(qId, NResource::Find(key));
        }
    }
    return result;
}

void TClickbenchWorkloadParams::ConfigureOpts(NLastGetopt::TOpts& opts, const ECommandType commandType, int workloadType) {
    TWorkloadBaseParams::ConfigureOpts(opts, commandType, workloadType);
    switch (commandType) {
    case TWorkloadParams::ECommandType::Run:
        opts.AddLongOption("ext-queries-file", "File with external queries. Separated by ';'")
            .DefaultValue("")
            .StoreResult(&ExternalQueriesFile);
        opts.AddLongOption("ext-queries-dir", "Directory with external queries. Naming have to be q[0-N].sql")
            .DefaultValue("")
            .StoreResult(&ExternalQueriesDir);
        opts.AddLongOption("ext-results-dir", "Directory with external results. Naming have to be q[0-N].sql")
            .DefaultValue("")
            .StoreResult(&ExternalResultsDir);
        opts.AddLongOption("ext-query-variables", "v1_id=v1_value;v2_id=v2_value;...; applied for queries {v1_id} -> v1_value")
            .DefaultValue("")
            .StoreResult(&ExternalVariablesString);
        opts.AddLongOption('q', "ext-query", "String with external queries. Separated by ';'")
            .DefaultValue("")
            .StoreResult(&ExternalQueries);
        opts.AddLongOption('c', "check-cannonical", "Use deterministic queries and check results with cannonical ones.")
            .NoArgument().StoreTrue(&CheckCannonicalFlag);
        break;
    default:
        break;
    }
}


THolder<IWorkloadQueryGenerator> TClickbenchWorkloadParams::CreateGenerator() const {
    return MakeHolder<TClickbenchWorkloadGenerator>(*this);
}

TWorkloadDataInitializer::TList TClickbenchWorkloadParams::CreateDataInitializers() const {
    return {std::make_shared<TClickbenchWorkloadDataInitializerGenerator>(*this)};
}

TString TClickbenchWorkloadParams::GetWorkloadName() const {
    return "ClickBench";
}

}
