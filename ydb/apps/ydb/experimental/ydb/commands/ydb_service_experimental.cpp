
#include "generate.h"
#include "ydb_service_experimental.h"
#include "ydb_sql.h"

#include <memory>
#include <ydb/public/lib/ydb_cli/common/plan2svg.h>
#include <ydb/public/lib/ydb_cli/topic/topic_read.h>

#include <ydb/public/lib/ydb_cli/commands/ydb_common.h>
#include <ydb/public/lib/ydb_cli/commands/ydb_service_topic.h>
#include <ydb/public/lib/yson_value/ydb_yson_value.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/proto/accessor.h>

#include <yql/essentials/public/issue/yql_issue_message.h>

#include <library/cpp/json/json_prettifier.h>
#include <library/cpp/protobuf/util/pb_io.h>
#include <library/cpp/protobuf/json/json2proto.h>

#include <util/generic/guid.h>
#include <util/string/printf.h>
#include <util/string/vector.h>

#include <iomanip>

namespace NYdb::NConsoleClient {

using namespace NFq;

TCommandExperimental::TCommandExperimental()
    : TClientCommandTree("experimental", {"exp"}, "Experimental operations")
{
    AddCommand(std::make_unique<TCommandStreamQuery>());
    AddCommand(std::make_unique<NExperimentalConsoleClient::TCommandExplain>());
    AddCommand(std::make_unique<TCommandFederatedQueryTree>());
    AddCommand(std::make_unique<TCommandGetConnection>());
    AddCommand(std::make_unique<TCommandGenerate>());
    AddCommand(std::make_unique<TCommandJson2Svg>());
    AddCommand(std::make_unique<TCommandSqlExperimental>());
    AddCommand(std::make_unique<TCommandSqlOperation>());
}

TCommandStreamQuery::TCommandStreamQuery()
    : TYdbCommand("streamquery", {"sq"}, "Execute stream query")
{}

void TCommandStreamQuery::OutputResult(NTable::TScanQueryPartIterator& it, IOutputStream& out) {
    bool columnsPrinted = false;
    for (;;) {
        auto streamPart = it.ReadNext().GetValueSync();
        if (!streamPart.IsSuccess()) {
            if (streamPart.EOS()) {
                break;
            }
            NStatusHelpers::ThrowOnErrorOrPrintIssues(streamPart);
        }

        if (streamPart.HasResultSet()) {
            auto result = streamPart.ExtractResultSet();
            auto columns = result.GetColumnsMeta();

            if (!columnsPrinted) {
                for (ui32 i = 0; i < columns.size(); ++i) {
                    out << columns[i].Name << "; ";
                }
                out << Endl;
                columnsPrinted = true;
            }

            NYdb::TResultSetParser parser(result);
            while (parser.TryNextRow()) {
                for (ui32 i = 0; i < columns.size(); ++i) {
                    out << FormatValueYson(parser.GetValue(i)) << "; ";
                }
                out << Endl;
            }
        }

        if (streamPart.HasQueryStats()) {
            auto profile = streamPart.ExtractQueryStats().ToString();
            if (ProfileFileName) {
                TFile pf{ProfileFileName, CreateAlways | WrOnly};
                pf.Write(profile.data(), profile.size());
                pf.Close();
            } else if (StatsFileName) {
                TFile pf{StatsFileName, CreateAlways | WrOnly};
                pf.Write(profile.data(), profile.size());
                pf.Close();
            } else {
                out << Endl << "_profile:" << Endl << profile << Endl << Endl;
            }
        }
    }
}

void TCommandStreamQuery::Config(TConfig& config) {
    config.Opts->AddLongOption('q', "query", "Query text")
        .Optional()
        .StoreResult(&Query);
    config.Opts->AddLongOption('f', "file", "File with query")
        .Optional()
        .StoreResult(&FileName);
    config.Opts->AddLongOption('s', "stats", "Stats file")
        .Optional()
        .StoreResult(&StatsFileName);
    config.Opts->AddLongOption('p', "profile", "Profile file")
        .Optional()
        .StoreResult(&ProfileFileName);
    config.SetFreeArgsNum(0);
}

int TCommandStreamQuery::Run(TConfig& config) {
    if (!Query) {
        if (!FileName) {
            throw TMisuseException() << "Neither query text (\"--query\", \"-q\") "
                << "nor path to a file with query text (\"--file\", \"-f\") were provided.";
        }
        Query = TFileInput(FileName).ReadAll();
    }

    NTable::TTableClient db(CreateDriver(config));
    NTable::TStreamExecScanQuerySettings settings;

    if (ProfileFileName) {
        settings.CollectQueryStats(NTable::ECollectQueryStatsMode::Full);
    } else if (StatsFileName) {
        settings.CollectQueryStats(NTable::ECollectQueryStatsMode::Basic);
    } else {
        settings.CollectQueryStats(NTable::ECollectQueryStatsMode::None);
    }

    auto it = db.StreamExecuteScanQuery(Query, settings).GetValueSync();
    NStatusHelpers::ThrowOnErrorOrPrintIssues(it);

    OutputResult(it, Cout);

    return EXIT_SUCCESS;
}

namespace {
    TString BlurSecret(const TString& in) {
        TString out(in);
        size_t clearSymbolsCount = Min(size_t(10), out.length() / 4);
        for (size_t i = clearSymbolsCount; i < out.length() - clearSymbolsCount; ++i) {
            out[i] = '*';
        }
        return out;
    }

    TString ReplaceWithAsterisks(const TString& in) {
        return TString(in.length(), '*');
    }
}

TCommandGetConnection::TCommandGetConnection()
    : TClientCommand("getconn", {}, "List current connection parameters")
{}

void TCommandGetConnection::Config(TConfig& config) {
    TClientCommand::Config(config);

    config.NeedToConnect = false;

    config.SetFreeArgsNum(0);
}

int TCommandGetConnection::Run(TConfig& config) {
    if (config.Address) {
        Cout << "  endpoint: " << config.Address << Endl;
    }
    if (config.Database) {
        Cout << "  database: " << config.Database << Endl;
    }
    if (config.SecurityToken) {
        Cout << "  token: " << BlurSecret(config.SecurityToken) << Endl;
    }
    if (config.UseIamAuth) {
        if (config.YCToken) {
            Cout << "  yc-token: " << BlurSecret(config.YCToken) << Endl;
        }
        if (config.SaKeyFile) {
            Cout << "  sa-key-file: "  << config.SaKeyFile << Endl;
        }
        if (config.UseMetadataCredentials) {
            Cout << "  use-metadata-credentials" << Endl;
        }
        if (config.IamEndpoint) {
            Cout << "  iam-endpoint: " << config.IamEndpoint << Endl;
        }
    }
    if (config.UseStaticCredentials) {
        if (!config.StaticCredentials.User.empty()) {
            Cout << "  user: " << config.StaticCredentials.User << Endl;
        }
        if (!config.StaticCredentials.Password.empty()) {
            Cout << "  password: " << ReplaceWithAsterisks(TString{config.StaticCredentials.Password}) << Endl;
        }
    }
    return EXIT_SUCCESS;
}

namespace NExperimentalConsoleClient {

TCommandExplain::TCommandExplain()
    : TTableCommand("explain", {}, "Explain query")
{}

void TCommandExplain::Config(TConfig& config) {
    TTableCommand::Config(config);

    config.Opts->AddLongOption('q', "query", "Text of query to explain").RequiredArgument("[String]").StoreResult(&Query);
    config.Opts->AddLongOption('f', "file", "Path to file with query text to explain")
        .RequiredArgument("PATH").StoreResult(&QueryFile);
    config.Opts->AddLongOption("ast", "Print query AST")
        .StoreTrue(&PrintAst);

    config.Opts->AddLongOption('t', "type", "Query type [data, scan]")
        .RequiredArgument("[String]").DefaultValue("data").StoreResult(&QueryType);
    config.Opts->AddLongOption("format", "Format type [pretty, json, dot, ast, svg]")
        .RequiredArgument("[String]").DefaultValue("").StoreResult(&Format);
    config.Opts->AddLongOption("analyze", "Run query and collect execution statistics")
        .NoArgument().StoreTrue(&Analyze);

    config.SetFreeArgsNum(0);
}

void TCommandExplain::Parse(TConfig& config) {
    TClientCommand::Parse(config);
    CheckQueryOptions(config);
}

bool TCommandExplain::ThrowVersionError() {
    throw TMisuseException() << "Invalid plan version: "
        "PrettyPrint and dot output are available only for plan version 0.2, "
        "which is supported by ScanQuery or DataQuery NewEngine (currently under kikimr.UseNewEngine pragma).";
}

TString TCommandExplain::JsonToString(const NJson::TJsonValue& jsonValue) {
    TStringBuilder str;

    if (jsonValue.IsString()) {
        str << jsonValue.GetString();
    } else if (jsonValue.IsArray()) {
        str << "[";
        const auto& array = jsonValue.GetArraySafe();
        for (auto it = array.begin(); it != array.end(); ++it) {
            str << (it != array.begin() ? ", " : "")
                << JsonToString(*it);
        }
        str << "]";
    } else if (jsonValue.IsMap()) {
        str << "{";
        const auto& map = jsonValue.GetMapSafe();
        for (auto it = map.begin(); it != map.end(); ++it) {
            str << (it != map.begin() ? ", " : "")
                << it->first << ": " << JsonToString(it->second);
        }
        str << "}";
    } else {
        str << jsonValue;
    }

    return str;
}

void TCommandExplain::PrettyPrintPlan(IOutputStream* out, const NJson::TJsonValue& planValue, TVector<TString>& offsets) {
    auto node = planValue.GetMapSafe();

    TStringBuilder short_prefix;
    TStringBuilder prefix;
    for (const auto& offset: offsets) {
        if (&offset != &offsets.back()) {
            short_prefix << offset;
        }
        prefix << offset;
    }

    if (!offsets.empty()) {
        bool last = offsets.back() == Edge;
        short_prefix << (last ? EdgeBranch : EdgeBranchLast);
    }

    if (node.contains("Operators")) {
        for (const auto& op : node.at("Operators").GetArraySafe()) {
            TVector<TString> info;
            for (const auto& [key, value] : op.GetMapSafe()) {
                if (!PPrintSkipKeys.contains(key)) {
                    info.emplace_back(TStringBuilder() << key << ": " << JsonToString(value));
                }
            }

            if (info.empty()) {
                *out << short_prefix << op.GetMapSafe().at("Name").GetString() << Endl;
            } else {
                *out << short_prefix << op.GetMapSafe().at("Name").GetString()
                     << " (" << JoinStrings(info, ", ") << ")" << Endl;
            }
        }
    } else if (node.contains("PlanNodeType") && node.at("PlanNodeType").GetString() == "Connection") {
        *out << short_prefix << "<" << node.at("Node Type").GetString() << ">" << Endl;
    } else {
        *out << short_prefix << node.at("Node Type").GetString() << Endl;
    }

    for (const auto& [key, val] : node) {
        if (!PPrintSkipKeys.contains(key)) {
            *out << prefix << key << ": " << JsonToString(val) << Endl;
        }
    }

    if (node.contains("Plans")) {
        const auto& plans = node.at("Plans").GetArraySafe();
        for (const auto& sublan: plans) {
            offsets.push_back(&sublan != &plans.back() ? Edge : NoEdge);
            PrettyPrintPlan(out, sublan, offsets);
            offsets.pop_back();
        }
    }
}

void TCommandExplain::DotPrintWalk(IOutputStream* out, const NJson::TJsonValue& planValue, int parent_id) {
    auto node = planValue.GetMapSafe();
    auto node_id = node.at("PlanNodeId").GetIntegerSafe();

    if (node.at("Node Type").GetString() != "StageLink") {
        *out << "  " << node_id
            << " [label=\"" << node.at("Node Type").GetString() << "\""
            << (node.FindPtr("IsConnection") ? "; style=\"rounded\"" : "; style=\"filled\"")
            << "];" << Endl;

        if (parent_id >= 0) {
            *out << "  " << node_id << " -> " << parent_id << ";" << Endl;
        }
    } else {
        *out << "  " << node.at("LinkedPlanNodeId").GetIntegerSafe() << " -> " << parent_id << ";" << Endl;
    }

    if (node.contains("Plans")) {
        const auto& plans = node.at("Plans").GetArraySafe();
        for (const auto& sublan: plans) {
            DotPrintWalk(out, sublan, node_id);
        }
    }
}

void TCommandExplain::DotPrintPlan(IOutputStream* out, const TString& planJson) {
    *out <<
        "digraph Query {\n"
        "  concentrate=True;\n"
        "  rankdir=BT;\n"
        "  node [shape=record];\n";
    NJson::TJsonValue plan;
    NJson::ReadJsonTree(planJson, &plan, true);
    if (plan.GetMapSafe().at("meta").GetMapSafe().at("version") != "0.2") {
        ThrowVersionError();
    }
    DotPrintWalk(out, plan.GetMapSafe().at("Plan").GetMapSafe().at("Plans").GetArraySafe()[0], -1);
    *out << "}" << Endl;
}

void TCommandExplain::SvgPrintPlan(IOutputStream* out, const TString& planJson) {
    TPlanVisualizer planviz;
    planviz.LoadPlans(planJson);
    *out << planviz.PrintSvgSafe();
}

int TCommandExplain::Run(TConfig& config) {
    CheckQueryFile();

    TString planJson;
    TString ast;
    std::optional<NTable::TQueryStats> stats;
    if (QueryType == "scan") {
        NTable::TTableClient client(CreateDriver(config));
        NTable::TStreamExecScanQuerySettings settings;

        if (Analyze) {
            settings.CollectQueryStats(NTable::ECollectQueryStatsMode::Full);
        } else {
            settings.Explain(true);
        }

        auto result = client.StreamExecuteScanQuery(Query, settings).GetValueSync();
        NStatusHelpers::ThrowOnErrorOrPrintIssues(result);

        SetInterruptHandlers();
        while (!IsInterrupted()) {
            auto tablePart = result.ReadNext().GetValueSync();
            if (!tablePart.IsSuccess()) {
                if (tablePart.EOS()) {
                    break;
                }
                NStatusHelpers::ThrowOnErrorOrPrintIssues(tablePart);
            }
            if (tablePart.HasQueryStats() ) {
                stats = tablePart.GetQueryStats();
                auto proto = NYdb::TProtoAccessor::GetProto(*stats);
                planJson = proto.query_plan();
                ast = proto.query_ast();
            }
        }

        if (IsInterrupted()) {
            Cerr << "<INTERRUPTED>" << Endl;
        }

    } else if (QueryType == "data" && Analyze) {
        NTable::TExecDataQuerySettings settings;
        settings.CollectQueryStats(NTable::ECollectQueryStatsMode::Full);

        auto result = GetSession(config).ExecuteDataQuery(
            Query,
            NTable::TTxControl::BeginTx(NTable::TTxSettings::SerializableRW()).CommitTx(),
            settings
        ).ExtractValueSync();
        NStatusHelpers::ThrowOnErrorOrPrintIssues(result);
        planJson = result.GetQueryPlan();
        stats = result.GetStats();
        if (stats) {
            auto proto = NYdb::TProtoAccessor::GetProto(*stats);
            ast = proto.query_ast();
        }
    } else if (QueryType == "data" && !Analyze) {
        NTable::TExplainQueryResult result = GetSession(config).ExplainDataQuery(
            Query,
            FillSettings(NTable::TExplainDataQuerySettings())
        ).GetValueSync();
        NStatusHelpers::ThrowOnErrorOrPrintIssues(result);
        planJson = result.GetPlan();
        ast = result.GetAst();

    } else {
        throw TMisuseException() << "Unknown query type for explain.";
    }

    if (PrintAst) {
        Cout << Endl << "Query AST:" << Endl << ast << Endl;
    }

    if (Format == "json") {
        Cout << /* "Query plan:" << Endl << */ NJson::PrettifyJson(planJson, false) << Endl;
    } else if (Format == "pretty" || Format == "") {
        NJson::TJsonValue plan;
        NJson::ReadJsonTree(planJson, &plan, true);

        if (plan.GetMapSafe().at("meta").GetMapSafe().at("version") == "0.2") {
            TVector<TString> edges;
            for (const auto& subplan : plan.GetMapSafe().at("Plan").GetMapSafe().at("Plans").GetArraySafe()) {
                PrettyPrintPlan(&Cout, subplan, edges);
            }
        } else {
            if (Format != "") {
                ThrowVersionError();
            } else {
                // emulate legacy behavior for old plan when format is not set explicitly
                Cout << "Query plan:" << Endl << NJson::PrettifyJson(planJson, true) << Endl;
            }
        }
    } else if (Format == "dot") {
        DotPrintPlan(&Cout, planJson);
    } else if (Format == "svg") {
        SvgPrintPlan(&Cout, planJson);
    } else {
        throw TMisuseException() << "Unknown plan output format.";
    }
/*
    if (Analyze && stats.Defined()) {
        Cout << Endl << "Statistics:" << Endl << stats->ToString();
    }
*/
    return EXIT_SUCCESS;
}

}

// Generate

TCommandGenerate::TCommandGenerate()
    : TClientCommandTree("generate", {}, "Generate data")
{
    AddCommand(std::make_unique<TCommandGenerateNumbers>());
}

// Generate Shared Config

void TCommandGenerateBase::Config(TConfig& config) {
    TYdbCommand::Config(config);

    config.Opts->AddLongOption("timeout", "Operation timeout. Operation should be executed on server within this timeout. "
            "There could also be a delay up to 200ms to receive timeout error from server")
        .RequiredArgument("VAL").StoreResult(&OperationTimeout).DefaultValue(TDuration::Seconds(5 * 60));

    config.Opts->AddLongOption('p', "path", "Database path to table")
        .Required().RequiredArgument("STRING").StoreResult(&Path);

    const TGenerateSettings defaults;
    config.Opts->AddLongOption("num-rows", "Rows to generate")
        .DefaultValue(defaults.RowsToGenerate_).StoreResult(&RowsToGenerate);
    config.Opts->AddLongOption("batch-rows", "Generate portions of this size in rows")
        .DefaultValue(defaults.RowsPerRequest_).StoreResult(&RowsPerRequest);
    config.Opts->AddLongOption("max-in-flight",
        "Maximum number of in-flight requests; increase to load big files faster (more memory needed)")
        .DefaultValue(defaults.MaxInFlightRequests_).StoreResult(&MaxInFlightRequests);
    config.Opts->AddLongOption("threads",
        "Maximum number of threads; number of available processors if not specified")
        .DefaultValue(defaults.Threads_).StoreResult(&Threads);
}

void TCommandGenerateBase::Parse(TConfig& config) {
    TYdbCommand::Parse(config);
    AdjustPath(config);

    if (MaxInFlightRequests == 0) {
        throw TMisuseException()
            << "--max-in-flight must be greater than zero";
    }
}

// Import Generate Numbers

void TCommandGenerateNumbers::Config(TConfig& config) {
    TCommandGenerateBase::Config(config);
}

int TCommandGenerateNumbers::Run(TConfig& config) {
    TGenerateSettings settings;
    settings.OperationTimeout(OperationTimeout);
    settings.MaxInFlightRequests(MaxInFlightRequests);
    settings.RowsToGenerate(RowsToGenerate);
    settings.RowsPerRequest(RowsPerRequest);
    settings.Threads(Threads);

    TGenerateClient client(CreateDriver(config), config);
    NStatusHelpers::ThrowOnErrorOrPrintIssues(client.Generate(Path, settings));

    return EXIT_SUCCESS;
}

void TCommandJson2Svg::Config(TConfig& config) {
    config.Opts->AddLongOption('i', "input", "Path to input file, default or \"-\" for stdin")
        .DefaultValue("-").StoreResult(&InputFileName);
    config.Opts->AddLongOption('o', "output", "Path to output file, default or \"-\" for stdout")
        .DefaultValue("-").StoreResult(&OutputFileName);
}

int TCommandJson2Svg::Run(TConfig&) {

    TString planJson;

    if (InputFileName && InputFileName != "-") {
        planJson = TFileInput(InputFileName).ReadAll();
    } else {
        planJson = Cin.ReadAll();
    }

    TPlanVisualizer planviz;

    NJson::TJsonReaderConfig jsonConfig;
    NJson::TJsonValue jsonNode;
    if (NJson::ReadJsonTree(planJson, &jsonConfig, &jsonNode)) {
        NJson::TJsonValue* topNode = nullptr;
        topNode = jsonNode.GetValueByPath("Plan");
        if (topNode == nullptr) {
            topNode = jsonNode.GetValueByPath("plan.Plan");
        }
        if (topNode) {
            planviz.LoadPlans(*topNode);
        }
    }

    TString planSvg = planviz.PrintSvgSafe();

    if (OutputFileName && OutputFileName != "-") {
        TFileOutput output(OutputFileName);
        output << planSvg;
    } else {
        Cout << planSvg;
    }

    return EXIT_SUCCESS;
}

} // NYdb::NConsoleClient
