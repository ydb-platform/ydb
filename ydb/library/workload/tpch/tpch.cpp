#include "tpch.h"
#include "data_generator.h"

#include <library/cpp/resource/resource.h>
#include <util/stream/file.h>

namespace NYdbWorkload {

TTpchWorkloadGenerator::TTpchWorkloadGenerator(const TTpchWorkloadParams& params)
    : TWorkloadGeneratorBase(params)
    , Params(params)
{}

TString TTpchWorkloadGenerator::DoGetDDLQueries() const {
    auto schema = NResource::Find("tpch_schema.sql");
    return schema;
}

TQueryInfoList TTpchWorkloadGenerator::GetInitialData() {
    return {};
}

TQueryInfoList TTpchWorkloadGenerator::GetWorkload(int type) {
    TQueryInfoList result;
    if (type) {
        return result;
    }

    TVector<TString> queries;
    if (Params.GetExternalQueriesDir().IsDefined()) {
        TVector<TString> queriesList;
        TVector<ui32> queriesNums;
        Params.GetExternalQueriesDir().ListNames(queriesList);
        for (TStringBuf q: queriesList) {
            ui32 num;
            if (q.SkipPrefix("q") && q.ChopSuffix(".sql") && TryFromString(q, num)) {
                queriesNums.push_back(num);
            }
        }
        for (const auto& fname : queriesList) {
            ui32 num;
            TStringBuf q(fname);
            if (!q.SkipPrefix("q") || !q.ChopSuffix(".sql") || !TryFromString(q, num)) {
                continue;
            }
            if (queries.size() < num + 1) {
                queries.resize(num + 1);
            }
            TFileInput fInput(Params.GetExternalQueriesDir() / fname);
            queries[num] = fInput.ReadAll();
        }
    } else {
        NResource::TResources qresources;
        NResource::FindMatch("tpch/yql/q", &qresources);
        for (const auto& r: qresources) {
            ui32 num;
            TStringBuf q(r.Key);
            if (!q.SkipPrefix("tpch/yql/q") || !q.ChopSuffix(".sql") || !TryFromString(q, num)) {
                continue;
            }
            if (queries.size() < num + 1) {
                queries.resize(num + 1);
            }
            queries[num] = r.Data;
        }
    }
    for (auto& query : queries) {
        auto substTable= [this, &query](const char* name) {
            SubstGlobal(query, 
                TStringBuilder() << "{{" << name << "}}", 
                TStringBuilder() << "`" << Params.GetFullTableName(name) << "`"
            );
        };
        SubstGlobal(query, "{% include 'header.sql.jinja' %}", "");
        SubstGlobal(query, "{path}"      , Params.GetFullTableName(nullptr) + "/");
        substTable("customer");
        substTable("lineitem");
        substTable("nation");
        substTable("orders");
        substTable("part");
        substTable("partsupp");
        substTable("region");
        substTable("supplier");
        result.emplace_back();
        result.back().Query = query;
    }
    return result;
}

TVector<IWorkloadQueryGenerator::TWorkloadType> TTpchWorkloadGenerator::GetSupportedWorkloadTypes() const {
    return {TWorkloadType(0, "bench", "Perform benchmark", TWorkloadType::EKind::Benchmark)};
}

void TTpchWorkloadParams::ConfigureOpts(NLastGetopt::TOpts& opts, const ECommandType commandType, int workloadType) {
    TWorkloadBaseParams::ConfigureOpts(opts, commandType, workloadType);
    switch (commandType) {
    case TWorkloadParams::ECommandType::Run:
        opts.AddLongOption("ext-queries-dir", "Directory with external queries. Naming have to be q[0-N].sql")
            .StoreResult(&ExternalQueriesDir);
        break;
    default:
        break;
    }
}


THolder<IWorkloadQueryGenerator> TTpchWorkloadParams::CreateGenerator() const {
    return MakeHolder<TTpchWorkloadGenerator>(*this);
}

TString TTpchWorkloadParams::GetWorkloadName() const {
    return "TPC-H";
}

TWorkloadDataInitializer::TList TTpchWorkloadParams::CreateDataInitializers() const {
    return {std::make_shared<TTpchWorkloadDataInitializerGenerator>(*this)};
}

}
