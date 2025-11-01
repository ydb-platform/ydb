#include "tpc_base.h"

#include <ydb/library/workload/benchmark_base/workload.h_serialized.h>
#include <ydb/public/lib/scheme_types/scheme_type_id.h>

#include <library/cpp/resource/resource.h>
#include <library/cpp/streams/factory/open_by_signature/factory.h>
#include <library/cpp/colorizer/colors.h>
#include <util/stream/file.h>
#include <util/string/split.h>
#include <util/string/strip.h>

namespace NYdbWorkload {

TTpcBaseWorkloadGenerator::TTpcBaseWorkloadGenerator(const TTpcBaseWorkloadParams& params)
    : TWorkloadGeneratorBase(params)
    , FloatMode(params.GetFloatMode())
    , Params(params)
{}

TTpcBaseWorkloadParams::EFloatMode TTpcBaseWorkloadGenerator::DetectFloatMode() const {
    if (!Params.TableClient) { //dry run case
        return TTpcBaseWorkloadParams::EFloatMode::DOUBLE;
    }
    const auto [table, column] = GetTableAndColumnForDetectFloatMode();
    auto session = Params.TableClient->GetSession(NYdb::NTable::TCreateSessionSettings()).ExtractValueSync();
    if (!session.IsSuccess()) {
        ythrow yexception() << "Cannot create session: " << session.GetIssues().ToString() << Endl;
    }
    const auto tableDescr = session.GetSession().DescribeTable(Params.GetFullTableName(table.c_str())).ExtractValueSync();
    if (!tableDescr.IsSuccess()) {
        auto issues = tableDescr.GetIssues();
        ythrow yexception() << "Cannot descibe table " << table << ": " << tableDescr.GetIssues().ToString() << Endl;
    }
    for (const auto& col:tableDescr.GetTableDescription().GetTableColumns()) {
        if (col.Name != column) {
            continue;
        }
        NYdb::TTypeParser type(col.Type);
        if (type.GetKind() == NYdb::TTypeParser::ETypeKind::Optional) {
            type.OpenOptional();
        }
        switch (type.GetKind()) {
        case NYdb::TTypeParser::ETypeKind::Decimal: {
            return TTpcBaseWorkloadParams::EFloatMode::DECIMAL;
        }
        case NYdb::TTypeParser::ETypeKind::Primitive:
            if (type.GetPrimitive() == NYdb::EPrimitiveType::Double || type.GetPrimitive() == NYdb::EPrimitiveType::Float) {
                return TTpcBaseWorkloadParams::EFloatMode::DOUBLE;
            }
        default:
            ythrow yexception() << "Invalid column " << column << " type: " << col.Type.ToString();
        }
    }
    ythrow yexception() << "There is no column " << column << " in table " << table;
}

TQueryInfoList TTpcBaseWorkloadGenerator::GetInitialData() {
    return {};
}

TVector<IWorkloadQueryGenerator::TWorkloadType> TTpcBaseWorkloadGenerator::GetSupportedWorkloadTypes() const {
    return {TWorkloadType(0, "bench", "Perform benchmark", TWorkloadType::EKind::Benchmark)};
}

TString TTpcBaseWorkloadGenerator::GetExpectedResult(const TString& name, const TString& resourcePrefix) const {
    const auto key = resourcePrefix + "s" + ToString(Params.GetScale()) + "_canonical/q" + name + ".result";
    if (NResource::Has(key)) {
        return NResource::Find(key);
    }
    if (NResource::Has(key + ".gz")) {
        const auto data = NResource::Find(key + ".gz");
        auto input = OpenOwnedMaybeCompressedInput(MakeHolder<TStringInput>(data));
        return input->ReadAll();
    }
    return {};
}

TQueryInfoList TTpcBaseWorkloadGenerator::GetWorkload(int type) {
    TQueryInfoList result;
    if (type) {
        return result;
    }
    FloatMode = DetectFloatMode();
    auto resourcePrefix = "resfs/file/" + Params.GetWorkloadName() + "/";
    SubstGlobal(resourcePrefix, "-", "");
    resourcePrefix.to_lower();
    TVector<TString> queries;
    NResource::TResources qresources;
    const auto prefix = resourcePrefix + "queries/" + ToString(Params.GetSyntax()) + "/q";
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
    for (auto& query : queries) {
        PatchQuery(query);
        result.emplace_back();
        result.back().Query = query;
        if (Params.GetCheckCanonical()) {
            for (const auto& name: {ToString(&query - queries.data()), ToString(&query - queries.data()) + "." + ToString(FloatMode)}) {
                result.back().ExpectedResult = GetExpectedResult(name, resourcePrefix);
                if (!result.back().ExpectedResult.empty()) {
                    break;
                }
            }
        }
    }
    return result;
}

void TTpcBaseWorkloadGenerator::PatchQuery(TString& query) const {
    SubstGlobal(query, "{% include 'header.sql.jinja' %}", GetHeader(query));
    SubstGlobal(query, "{path}", Params.GetFullTableName(nullptr) + "/");
    const auto tableJson = GetTablesJson();
    for (const auto& table: tableJson["tables"].GetArray()) {
        const auto& tableName = table["name"].GetString();
        const auto tableFullName = (Params.GetPath() ? Params.GetPath() + "/" : "") + tableName;
        SubstGlobal(query, 
            TStringBuilder() << "{{" << tableName << "}}", 
            TStringBuilder() << Params.GetTablePathQuote(Params.GetSyntax()) << tableFullName << Params.GetTablePathQuote(Params.GetSyntax())
        );
    }
}

void TTpcBaseWorkloadGenerator::FilterHeader(IOutputStream& result, TStringBuf header, const TString& query) const {
    TStringBuilder scaleFactor;
    scaleFactor << "$scale_factor = ";
    switch(FloatMode) {
    case TTpcBaseWorkloadParams::EFloatMode::DOUBLE:
        scaleFactor << Params.GetScale();
        break;
    case TTpcBaseWorkloadParams::EFloatMode::DECIMAL:
        scaleFactor << "cast('" << Params.GetScale() << "' as decimal(35,2))";
        break;
    }
    scaleFactor << ";";
    for(TStringBuf line; header.ReadLine(line);) {
        const auto pos = line.find('=');
        if (pos == line.npos) {
            continue;
        }
        const auto name = StripString(line.SubString(0, pos));
        if (name == "$scale_factor") {
            line = scaleFactor; 
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
    switch (FloatMode) {
    case TTpcBaseWorkloadParams::EFloatMode::DOUBLE:
        FilterHeader(header.Out, NResource::Find("consts.yql"), query);
        break;
    case TTpcBaseWorkloadParams::EFloatMode::DECIMAL:
        FilterHeader(header.Out, NResource::Find("consts_decimal.yql"), query);
        break;
    }
    return header;
}

void TTpcBaseWorkloadParams::ConfigureOpts(NLastGetopt::TOpts& opts, const ECommandType commandType, int workloadType) {
    TWorkloadBaseParams::ConfigureOpts(opts, commandType, workloadType);
    TStringBuilder floatDescr;
    auto colors = NColorizer::AutoColors(Cout);
    floatDescr << "Float mode. Defines the type to be used for floating-point values. Available options:" << Endl
        << "  " << colors.BoldColor() << EFloatMode::DOUBLE << colors.OldColor() << Endl
        << "    Use native Float type for floating-point values." << Endl
        << "  " << colors.BoldColor() << EFloatMode::DECIMAL << colors.OldColor() << Endl
        << "    Use Decimal type with canonical precision and scale." << Endl;
    switch (commandType) {
    case TWorkloadParams::ECommandType::Run:
        opts.AddLongOption( "syntax", "Query syntax [" + GetEnumAllNames<EQuerySyntax>() + "].")
            .StoreResult(&Syntax).DefaultValue(Syntax);
        opts.AddLongOption("scale", "Sets the percentage of the benchmark's data size and workload to use, relative to full scale.")
            .DefaultValue(Scale).StoreResult(&Scale);
        break;
    case TWorkloadParams::ECommandType::Init:
        opts.AddLongOption("float-mode", floatDescr)
            .StoreResult(&FloatMode).DefaultValue(FloatMode);
        opts.AddLongOption("scale", "Sets the percentage of the benchmark's data size and workload to use, relative to full scale.")
            .DefaultValue(Scale).StoreResult(&Scale);
        break;
    default:
        break;
    }
}


}
