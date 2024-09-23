#include "tpcds.h"

#include <library/cpp/resource/resource.h>
#include <util/stream/file.h>

namespace NYdbWorkload {

TTpcdsWorkloadGenerator::TTpcdsWorkloadGenerator(const TTpcdsWorkloadParams& params)
    : TWorkloadGeneratorBase(params)
    , Params(params)
{}

TString TTpcdsWorkloadGenerator::DoGetDDLQueries() const {
    auto schema = NResource::Find("tpcds_schema.sql");
    return schema;
}

TQueryInfoList TTpcdsWorkloadGenerator::GetInitialData() {
    return {};
}

TQueryInfoList TTpcdsWorkloadGenerator::GetWorkload(int type) {
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
        NResource::FindMatch("tpcds/yql/q", &qresources);
        for (const auto& r: qresources) {
            ui32 num;
            TStringBuf q(r.Key);
            if (!q.SkipPrefix("tpcds/yql/q") || !q.ChopSuffix(".sql") || !TryFromString(q, num)) {
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
        substTable("customer_address");
        substTable("customer_demographics");
        substTable("date_dim");
        substTable("warehouse");
        substTable("ship_mode");
        substTable("time_dim");
        substTable("reason");
        substTable("income_band");
        substTable("item");
        substTable("store");
        substTable("call_center");
        substTable("customer");
        substTable("web_site");
        substTable("store_returns");
        substTable("household_demographics");
        substTable("web_page");
        substTable("promotion");
        substTable("catalog_page");
        substTable("inventory");
        substTable("catalog_returns");
        substTable("web_returns");
        substTable("web_sales");
        substTable("catalog_sales");
        substTable("store_sales");
        result.emplace_back();
        result.back().Query = query;
    }
    return result;
}

TVector<IWorkloadQueryGenerator::TWorkloadType> TTpcdsWorkloadGenerator::GetSupportedWorkloadTypes() const {
    return {TWorkloadType(0, "bench", "Perform benchmark", TWorkloadType::EKind::Benchmark)};
}

void TTpcdsWorkloadParams::ConfigureOpts(NLastGetopt::TOpts& opts, const ECommandType commandType, int workloadType) {
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


THolder<IWorkloadQueryGenerator> TTpcdsWorkloadParams::CreateGenerator() const {
    return MakeHolder<TTpcdsWorkloadGenerator>(*this);
}

TString TTpcdsWorkloadParams::GetWorkloadName() const {
    return "TPC-DS";
}
/*
TWorkloadDataInitializer::TList TTpcdsWorkloadParams::CreateDataInitializers() const {
    return {std::make_shared<TTpcdsWorkloadDataInitializerGenerator>(*this)};
}
*/
}
