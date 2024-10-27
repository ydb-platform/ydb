#include "tpc_base.h"

#include <ydb/library/workload/benchmark_base/workload.h_serialized.h>
#include <ydb/public/lib/scheme_types/scheme_type_id.h>

#include <library/cpp/resource/resource.h>
#include <util/stream/file.h>
#include <util/string/split.h>
#include <util/string/strip.h>

namespace NYdbWorkload {

TTpcBaseWorkloadGenerator::TTpcBaseWorkloadGenerator(const TTpcBaseWorkloadParams& params)
    : TWorkloadGeneratorBase(params)
    , Params(params)
{}

TQueryInfoList TTpcBaseWorkloadGenerator::GetInitialData() {
    return {};
}

TVector<IWorkloadQueryGenerator::TWorkloadType> TTpcBaseWorkloadGenerator::GetSupportedWorkloadTypes() const {
    return {TWorkloadType(0, "bench", "Perform benchmark", TWorkloadType::EKind::Benchmark)};
}

TQueryInfoList TTpcBaseWorkloadGenerator::GetWorkload(int type) {
    TQueryInfoList result;
    if (type) {
        return result;
    }
    auto resourcePrefix = Params.GetWorkloadName() + "/";
    SubstGlobal(resourcePrefix, "-", "");
    resourcePrefix.to_lower();
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
        const auto prefix = resourcePrefix + ToString(Params.GetSyntax()) + "/q";
        NResource::FindMatch(prefix, &qresources);
        for (const auto& r: qresources) {
            ui32 num;
            TStringBuf q(r.Key);
            if (!q.SkipPrefix(prefix) || !q.ChopSuffix(".sql") || !TryFromString(q, num)) {
                continue;
            }
            if (queries.size() < num + 1) {
                queries.resize(num + 1);
            }
            queries[num] = r.Data;
        }
    }
    for (auto& query : queries) {
        PatchQuery(query);
        result.emplace_back();
        result.back().Query = query;
        if (Params.GetCheckCanonical()) {
            const auto key = resourcePrefix + "s" + ToString(Params.GetScale()) + "_canonical/q" + ToString(&query - queries.data()) + ".result";
            if (NResource::Has(key)) {
                result.back().ExpectedResult = NResource::Find(key);
            }
        }
    }
    return result;
}

void TTpcBaseWorkloadGenerator::PatchQuery(TString& query) const {
    SubstGlobal(query, "{% include 'header.sql.jinja' %}", GetHeader(query));
    SubstGlobal(query, "{path}", Params.GetFullTableName(nullptr) + "/");
    for (const auto& table: GetTablesList()) {
        SubstGlobal(query, 
            TStringBuilder() << "{{" << table << "}}", 
            TStringBuilder() << Params.GetTablePathQuote(Params.GetSyntax()) << Params.GetPath() << "/" << table << Params.GetTablePathQuote(Params.GetSyntax())
        );
    }
}

void TTpcBaseWorkloadGenerator::FilterHeader(IOutputStream& result, TStringBuf header, const TString& query) const {
    for(TStringBuf line; header.ReadLine(line);) {
        const auto pos = line.find('=');
        if (pos == line.npos) {
            continue;
        }
        const auto name = StripString(line.SubString(0, pos));
        if (name == "$scale_factor") {
            line = "$scale_factor = " + ToString(Params.GetScale()) + ";"; 
        } 
        for(auto posInQ = query.find(name); posInQ != query.npos; posInQ = query.find(name, posInQ)) {
            posInQ += name.length();
            if (posInQ >= query.length() || !IsAsciiAlnum(query[posInQ]) && query[posInQ] != '_') {
                result << line << Endl;
                break;
            }
        }
    }
}

TString TTpcBaseWorkloadGenerator::GetHeader(const TString& query) const {
    TStringBuilder header;
    if (Params.GetSyntax() == TWorkloadBaseParams::EQuerySyntax::PG) {
        header << "--!syntax_pg" << Endl;
    }
    switch (Params.GetFloatMode()) {
    case TTpcBaseWorkloadParams::EFloatMode::FLOAT:
        FilterHeader(header.Out, NResource::Find("consts.yql"), query);
        break;
    case TTpcBaseWorkloadParams::EFloatMode::DECIMAL:
    case TTpcBaseWorkloadParams::EFloatMode::DECIMAL_YDB:
        FilterHeader(header.Out, NResource::Find("consts_decimal.yql"), query);
        break;
    }

    if (Params.GetFloatMode() != TTpcBaseWorkloadParams::EFloatMode::DECIMAL_YDB) {
        return header;
    }
    header.to_lower();
    const TStringBuf dec("decimal(");
    auto p = header.find(dec);
    while (p != TString::npos) {
        p += dec.length();
        const auto q = header.find(')', p);
        TVector<ui32> decParams;
        StringSplitter(header.cbegin() + p, q - p).SplitBySet(", ").SkipEmpty().Limit(2).ParseInto(&decParams);
        TStringBuilder newDecParams;
        newDecParams
            << Max(decParams[0], NKikimr::NScheme::DECIMAL_PRECISION)
            << "," << Max(decParams[1], NKikimr::NScheme::DECIMAL_SCALE);
        header.replace(p, q - p, newDecParams);
        p = header.find(dec, q);
    }
    return header;
}

void TTpcBaseWorkloadParams::ConfigureOpts(NLastGetopt::TOpts& opts, const ECommandType commandType, int workloadType) {
    TWorkloadBaseParams::ConfigureOpts(opts, commandType, workloadType);
    switch (commandType) {
    case TWorkloadParams::ECommandType::Run:
        opts.AddLongOption("ext-queries-dir", "Directory with external queries. Naming have to be q[0-N].sql")
            .StoreResult(&ExternalQueriesDir);
        opts.AddLongOption( "syntax", "Query syntax [" + GetEnumAllNames<EQuerySyntax>() + "].")
            .StoreResult(&Syntax).DefaultValue(Syntax);
        opts.AddLongOption("scale", "scale in percents")
            .DefaultValue(Scale).StoreResult(&Scale);
        opts.AddLongOption("float-mode", "Float mode. Can be float, decimal or decimal_ydb. If set to 'float' - float will be used, 'decimal' means that decimal will be used with canonical size and 'decimal_ydb' means that all floats will be converted to decimal(22,9) because YDB supports only this type.")
            .StoreResult(&FloatMode).DefaultValue(FloatMode);
        break;
    case TWorkloadParams::ECommandType::Init:
        opts.AddLongOption("float-mode", "Float mode. Can be float, decimal or decimal_ydb. If set to 'float' - float will be used, 'decimal' means that decimal will be used with canonical size and 'decimal_ydb' means that all floats will be converted to decimal(22,9) because YDB supports only this type.")
            .StoreResult(&FloatMode).DefaultValue(FloatMode);
        break;
    default:
        break;
    }
}


}
