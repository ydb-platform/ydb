#include "format.h"

#include <util/string/vector.h>
#include <library/cpp/json/json_prettifier.h>

#include <ydb/public/lib/json_value/ydb_json_value.h>

namespace NYdb {
namespace NConsoleClient {

namespace {
    THashMap<EOutputFormat, TString> InputFormatDescriptions = {
        { EOutputFormat::JsonUnicode, "Input in json format, binary strings are decoded with unicode characters" },
        { EOutputFormat::JsonBase64, "Input in json format, binary strings are decoded with base64" },
        { EOutputFormat::Csv, "Input in csv format" },
        { EOutputFormat::Tsv, "Input in tsv format" },
    };

    THashMap<EOutputFormat, TString> StdinFormatDescriptions = {
        { EOutputFormat::JsonUnicode, "Parameter names and values in json unicode format" },
        { EOutputFormat::JsonBase64, "Parameter names and values in json unicode format, binary string parameter values are base64-encoded" },
        { EOutputFormat::Csv, "Parameter names and values in csv format" },
        { EOutputFormat::Tsv, "Parameter names and values in tsv format" },
        { EOutputFormat::NewlineDelimited, "Newline character delimits parameter sets on stdin and triggers "
                                            "processing in accordance to \"batch\" option" },
        { EOutputFormat::Raw, "Binary value with no transformations or parsing, parameter name is set by an \"stdin-par\" option" },
        { EOutputFormat::NoFraming, "Data from stdin is taken as a single set of parameters" },
    };

    THashMap<EOutputFormat, TString> FormatDescriptions = {
        { EOutputFormat::Pretty, "Human readable output" },
        { EOutputFormat::PrettyTable, "Human readable table output" },
        { EOutputFormat::Json, "Output in json format" },
        { EOutputFormat::JsonUnicode, "Output in json format, binary strings are encoded with unicode characters. "
                                      "Every row is a separate json on a separate line." },
        { EOutputFormat::JsonUnicodeArray, "Output in json format, binary strings are encoded with unicode characters. "
                                           "Every resultset is a json array of rows. "
                                           "Every row is a separate json on a separate line." },
        { EOutputFormat::JsonBase64, "Output in json format, binary strings are encoded with base64. "
                                     "Every row is a separate json on a separate line." },
        { EOutputFormat::JsonBase64Array, "Output in json format, binary strings are encoded with base64. "
                                           "Every resultset is a json array of rows. "
                                           "Every row is a separate json on a separate line." },
        { EOutputFormat::JsonRawArray, "Output in json format, binary strings are not encoded."
                                        "Every resultset is a json array of rows. "
                                        "Every row is a separate binary data on a separate line"},
        { EOutputFormat::ProtoJsonBase64, "Output result protobuf in json format, binary strings are encoded with base64" },
        { EOutputFormat::Csv, "Output in csv format" },
        { EOutputFormat::Tsv, "Output in tsv format" },
    };

    THashMap<EMessagingFormat, TString> MessagingFormatDescriptions = {
        { EMessagingFormat::Pretty, "Human readable output with metadata." },
        { EMessagingFormat::SingleMessage, "Single message."}, // TODO(shmel1k@): improve
        { EMessagingFormat::NewlineDelimited, "Newline delimited stream of messages."}, // TODO(shmel1k@): improve
        { EMessagingFormat::Concatenated, "Concatenated output stream of messages."}, // TODO(shmel1k@): improve,
        { EMessagingFormat::JsonStreamConcat, "Concatenated Json stream of envelopes with metadata and messages in the ""body"" attribute." }, // TODO(shmel1k@): improve,
        { EMessagingFormat::JsonArray, "Json array of envelopes with metadata and messages in the ""body"" attribute." }, // TODO(shmel1k@): improve,
    };
}

void TCommandWithResponseHeaders::PrintResponseHeader(const TStatus& status) {
    if (!ShowHeaders)
        return;

    PrintResponseHeaderPretty(status);
}

void TCommandWithResponseHeaders::PrintResponseHeaderPretty(const TStatus& status) {
    const auto columnNames = TVector<TString>{"meta key", "meta value"};
    TPrettyTable table(columnNames);

    const auto headers = status.GetResponseMetadata();
    for (const auto& h : headers) {
        auto& row = table.AddRow();
        row.Column(0, h.first);
        row.Column(1, h.second);
    }

    Cout << table;
}

const TString TCommandWithResponseHeaders::ResponseHeadersHelp = "Show response metadata for ydb call";

// Deprecated
void TCommandWithFormat::AddDeprecatedJsonOption(TClientCommand::TConfig& config, const TString& description) {
    config.Opts->AddLongOption("json", description).NoArgument()
        .StoreValue(&OutputFormat, EOutputFormat::Json).StoreValue(&DeprecatedOptionUsed, true)
        .Hidden();
}

void TCommandWithFormat::AddInputFormats(TClientCommand::TConfig& config, 
                                         const TVector<EOutputFormat>& allowedFormats, EOutputFormat defaultFormat) {
    TStringStream description;
    description << "Input format. Available options: ";
    NColorizer::TColors colors = NColorizer::AutoColors(Cout);
    Y_ABORT_UNLESS(std::find(allowedFormats.begin(), allowedFormats.end(), defaultFormat) != allowedFormats.end(), 
        "Couldn't find default format %s in allowed formats", (TStringBuilder() << defaultFormat).c_str());
    for (const auto& format : allowedFormats) {
        auto findResult = InputFormatDescriptions.find(format);
        Y_ABORT_UNLESS(findResult != InputFormatDescriptions.end(),
            "Couldn't find description for %s input format", (TStringBuilder() << format).c_str());
        description << "\n  " << colors.BoldColor() << format << colors.OldColor()
            << "\n    " << findResult->second;
    }
    description << "\nDefault: " << colors.CyanColor() << "\"" << defaultFormat << "\"" << colors.OldColor() << ".";
    config.Opts->AddLongOption("input-format", description.Str())
        .RequiredArgument("STRING").StoreResult(&InputFormat);
    AllowedInputFormats = allowedFormats;
}

void TCommandWithFormat::AddStdinFormats(TClientCommand::TConfig &config, const TVector<EOutputFormat>& allowedStdinFormats,
                                         const TVector<EOutputFormat>& allowedFramingFormats) {
    TStringStream description;
    description << "Stdin parameters format and framing. Specify this option twice to select both.\n"
                << "1. Parameters format. Available options: ";
    NColorizer::TColors colors = NColorizer::AutoColors(Cout);
    for (const auto& format : allowedStdinFormats) {
        auto findResult = StdinFormatDescriptions.find(format);
        Y_ABORT_UNLESS(findResult != StdinFormatDescriptions.end(),
                 "Couldn't find description for %s stdin format", (TStringBuilder() << format).c_str());
        description << "\n  " << colors.BoldColor() << format << colors.OldColor()
                    << "\n    " << findResult->second;
    }
    description << "\nDefault: " << colors.CyanColor() << "\"json-unicode\"" << colors.OldColor() << ".";
    description << "\n2. Framing: defines how parameter sets are delimited on the stdin. Available options: ";
    for (const auto& format : allowedFramingFormats) {
        auto findResult = StdinFormatDescriptions.find(format);
        Y_ABORT_UNLESS(findResult != StdinFormatDescriptions.end(),
                 "Couldn't find description for %s framing format", (TStringBuilder() << format).c_str());
        description << "\n  " << colors.BoldColor() << format << colors.OldColor()
                    << "\n    " << findResult->second;
    }
    description << "\nDefault: " << colors.CyanColor() << "\"no-framing\"" << colors.OldColor() << ".";
    config.Opts->AddLongOption("stdin-format", description.Str())
            .RequiredArgument("STRING").AppendTo(&StdinFormats);
    AllowedStdinFormats = allowedStdinFormats;
    AllowedFramingFormats = allowedFramingFormats;
}

void TCommandWithFormat::AddFormats(TClientCommand::TConfig& config, 
                                    const TVector<EOutputFormat>& allowedFormats, EOutputFormat defaultFormat) {
    TStringStream description;
    description << "Output format. Available options: ";
    NColorizer::TColors colors = NColorizer::AutoColors(Cout);
    Y_ABORT_UNLESS(std::find(allowedFormats.begin(), allowedFormats.end(), defaultFormat) != allowedFormats.end(), 
        "Couldn't find default format %s in allowed formats", (TStringBuilder() << defaultFormat).c_str());
    for (const auto& format : allowedFormats) {
        auto findResult = FormatDescriptions.find(format);
        Y_ABORT_UNLESS(findResult != FormatDescriptions.end(),
            "Couldn't find description for %s output format", (TStringBuilder() << format).c_str());
        description << "\n  " << colors.BoldColor() << format << colors.OldColor()
            << "\n    " << findResult->second;
    }
    description << "\nDefault: " << colors.CyanColor() << "\"" << defaultFormat << "\"" << colors.OldColor() << ".";
    config.Opts->AddLongOption("format", description.Str())
        .RequiredArgument("STRING").StoreResult(&OutputFormat);
    AllowedFormats = allowedFormats;
}

void TCommandWithFormat::AddMessagingFormats(TClientCommand::TConfig& config, const TVector<EMessagingFormat>& allowedFormats) {
    TStringStream description;
    description << "Client-side format. Available options: ";
    NColorizer::TColors colors = NColorizer::AutoColors(Cout);
    for (const auto& format : allowedFormats) {
        auto findResult = MessagingFormatDescriptions.find(format);
        Y_ABORT_UNLESS(findResult != MessagingFormatDescriptions.end(),
            "Couldn't find description for %s output format", (TStringBuilder() << format).c_str());
        description << "\n  " << colors.BoldColor() << format << colors.OldColor()
            << "\n    " << findResult->second;
    }
    config.Opts->AddLongOption("format", description.Str())
        .DefaultValue( "single-message" )
        .RequiredArgument("STRING").StoreResult(&MessagingFormat);
    AllowedMessagingFormats = allowedFormats;
}

void TCommandWithFormat::ParseFormats() {
    if (InputFormat != EOutputFormat::Default
            && std::find(AllowedInputFormats.begin(), AllowedInputFormats.end(), InputFormat) == AllowedInputFormats.end()) {
        throw TMisuseException() << "Input format " << InputFormat << " is not available for this command";
    }

    if (!StdinFormats.empty()) {
        for (const auto& format : AllowedInputFormats) {
            Y_ABORT_UNLESS(std::find(AllowedStdinFormats.begin(), AllowedStdinFormats.end(), format) != AllowedStdinFormats.end(), 
                     "Allowed stdin formats should contain all allowed input formats");
        }
        for (const auto& format : StdinFormats) {
            if (format == EOutputFormat::Default) {
                IsStdinFormatSet = true;
                IsFramingFormatSet = true;
            } else if (std::find(AllowedStdinFormats.begin(), AllowedStdinFormats.end(), format) != AllowedStdinFormats.end()) {
                if (IsStdinFormatSet) {
                    throw TMisuseException() << "Formats " << StdinFormat << " and " << format
                                             << " are mutually exclusive, choose only one of them.";
                }
                StdinFormat = format;
                IsStdinFormatSet = true;
            } else if (std::find(AllowedFramingFormats.begin(), AllowedFramingFormats.end(), format) != AllowedFramingFormats.end()) {
                if (IsFramingFormatSet) {
                    throw TMisuseException() << "Formats " << FramingFormat << " and " << format
                                             << " are mutually exclusive, choose only one of them.";
                }
                FramingFormat = format;
                IsFramingFormatSet = true;
            } else {
                throw TMisuseException() << "Stdin format " << format << " is not available for this command";
            }
        }
        if (!IsStdinFormatSet) {
            StdinFormat = InputFormat;
        }
    }

    if (OutputFormat == EOutputFormat::Default || DeprecatedOptionUsed) {
        return;
    }
    if (std::find(AllowedFormats.begin(), AllowedFormats.end(), OutputFormat) == AllowedFormats.end()) {
        throw TMisuseException() << "Output format " << OutputFormat << " is not available for this command";
    }
}

void TCommandWithFormat::ParseMessagingFormats() {
    if (MessagingFormat != EMessagingFormat::SingleMessage
            && std::find(AllowedMessagingFormats.begin(), AllowedMessagingFormats.end(), MessagingFormat) == AllowedMessagingFormats.end()) {
        throw TMisuseException() << "Messaging format " << MessagingFormat << " is not available for this command";
    }
}


void TQueryPlanPrinter::Print(const TString& plan) {
    switch (Format) {
        case EOutputFormat::Default:
        case EOutputFormat::Pretty:
        case EOutputFormat::PrettyTable: {
            NJson::TJsonValue planJson;
            NJson::ReadJsonTree(plan, &planJson, true);

            Y_ENSURE(planJson.GetMapSafe().contains("meta"));
            const auto& meta = planJson.GetMapSafe().at("meta");

            Y_ENSURE(meta.GetMapSafe().contains("type"));
            if (meta.GetMapSafe().at("type").GetStringSafe() == "script") {
                Y_ENSURE(planJson.GetMapSafe().contains("queries"));
                const auto& queries = planJson.GetMapSafe().at("queries").GetArraySafe();
                for (size_t queryId = 0; queryId < queries.size(); ++queryId) {
                    const auto& query = queries[queryId];
                    Output << "Query " << queryId << ":" << Endl;

                    if (Format == EOutputFormat::PrettyTable) {
                        PrintPrettyTable(query);
                    } else {
                        PrintPretty(query);
                    }
                }
            } else {
                if (Format == EOutputFormat::PrettyTable) {
                    PrintPrettyTable(planJson);
                } else {
                    PrintPretty(planJson);
                }
            }

            break;
        }
        case EOutputFormat::JsonUnicode:
        case EOutputFormat::JsonBase64:
            PrintJson(plan);
            break;
        default:
            throw TMisuseException() << "This command doesn't support " << Format << " output format";
    }
}

void TQueryPlanPrinter::PrintJson(const TString& plan) {
    Output << NJson::PrettifyJson(plan, false) << Endl;
}

void TQueryPlanPrinter::PrintPretty(const NJson::TJsonValue& plan) {
    if (plan.GetMapSafe().contains("Plan")) {
        const auto& queryPlan = plan.GetMapSafe().at("Plan").GetMapSafe();
        Y_ENSURE(queryPlan.contains("Plans"));

        TVector<TString> offsets;
        for (const auto& subplan : queryPlan.at("Plans").GetArraySafe()) {
            PrintPrettyImpl(subplan, offsets);
        }
    } else { /* old format plan */
        PrintJson(plan.GetStringRobust());
    }
}

void TQueryPlanPrinter::PrintPrettyImpl(const NJson::TJsonValue& plan, TVector<TString>& offsets) {
    static const TString edge = "|  ";
    static const TString noEdge = "   ";
    static const TString edgeBranch = "├──";
    static const TString edgeBranchLast = "└──";

    TStringBuilder prefix;
    TStringBuilder headerPrefix;
    for (const auto& offset : offsets) {
        if (&offset != &offsets.back()) {
            prefix << offset;
        }
        headerPrefix << offset;
    }

    if (!offsets.empty()) {
        bool last = (offsets.back() == edge);
        prefix << (last ? edgeBranch : edgeBranchLast);
    }

    const auto& node = plan.GetMapSafe();

    if (node.contains("Operators")) {
        for (const auto& op : node.at("Operators").GetArraySafe()) {
            TVector<TString> info;
            for (const auto& [key, value] : op.GetMapSafe()) {
                if (key != "Name") {
                    info.emplace_back(TStringBuilder() << key << ": " << JsonToString(value));
                }
            }

            if (info.empty()) {
                Output << prefix << op.GetMapSafe().at("Name").GetString() << Endl;
            } else {
                Output << prefix << op.GetMapSafe().at("Name").GetString()
                     << " (" << JoinStrings(info, ", ") << ")" << Endl;
            }
        }
    } else if (node.contains("PlanNodeType") && node.at("PlanNodeType").GetString() == "Connection") {
        Output << prefix << "<" << node.at("Node Type").GetString() << ">" << Endl;
    } else {
        Output << prefix << node.at("Node Type").GetString() << Endl;
    }

    static const THashSet<TString> requiredFields = {"CTE Name", "Tables"};
    for (const auto& [key, value] : node) {
        if (requiredFields.contains(key)) {
            Output << headerPrefix << key << ": " << JsonToString(value) << Endl;
        }
    }

    if (AnalyzeMode && node.contains("Stats")) {
        NColorizer::TColors colors = NColorizer::AutoColors(Output);
        for (const auto& [key, value] : node.at("Stats").GetMapSafe()) {
            Output << headerPrefix << colors.Yellow() << key << ": " << colors.Cyan()
                 << JsonToString(value) << colors.Default() << Endl;
        }
    }

    if (node.contains("Plans")) {
        const auto& plans = node.at("Plans").GetArraySafe();
        for (const auto& subplan : plans) {
            offsets.push_back(&subplan != &plans.back() ? edge : noEdge);
            PrintPrettyImpl(subplan, offsets);
            offsets.pop_back();
        }
    }
}

void TQueryPlanPrinter::PrintPrettyTable(const NJson::TJsonValue& plan) {
    static const TVector<TString> explainColumnNames = {"Operation", "E-Cost", "E-Rows"};
    static const TVector<TString> explainAnalyzeColumnNames = {"Operation", "DurationMs", "Rows", "E-Cost", "E-Rows"};

    if (plan.GetMapSafe().contains("Plan")) {
        auto queryPlan = plan.GetMapSafe().at("Plan");
        SimplifyQueryPlan(queryPlan);

        TPrettyTable table(AnalyzeMode ? explainAnalyzeColumnNames : explainColumnNames,
            TPrettyTableConfig().WithoutRowDelimiters());

        Y_ENSURE(queryPlan.GetMapSafe().contains("Plans"));

        TString offset;
        for (auto subplan : queryPlan.GetMapSafe().at("Plans").GetArraySafe()) {
            PrintPrettyTableImpl(subplan, offset, table);
        }

        Output << table;
    } else { /* old format plan */
        PrintJson(plan.GetStringRobust());
    }
}

void TQueryPlanPrinter::PrintPrettyTableImpl(const NJson::TJsonValue& plan, TString& offset, TPrettyTable& table) {
    const auto& node = plan.GetMapSafe();

    auto& newRow = table.AddRow();
    if (AnalyzeMode) {
        TString duration;
        TString nRows;

        if (node.contains("Stats")) {
            const auto& stats = node.at("Stats").GetMapSafe();

            if (stats.contains("TotalOutputRows")) {
                nRows = JsonToString(stats.at("TotalOutputRows"));
            }

            if (stats.contains("TotalDurationMs")) {
                duration = JsonToString(stats.at("TotalDurationMs"));
            }
        }

        newRow.Column(1, std::move(duration));
        newRow.Column(2, std::move(nRows));
    }


    if (node.contains("Operators")) {
        for (const auto& op : node.at("Operators").GetArraySafe()) {
            TVector<TString> info;
            TString eCost;
            TString eRows;

            for (const auto& [key, value] : op.GetMapSafe()) {
                if (key == "E-Cost") {
                    eCost = JsonToString(value);
                } else if (key == "E-Rows") {
                    eRows = JsonToString(value);
                } else if (key != "Name") {
                    info.emplace_back(TStringBuilder() << key << ": " << JsonToString(value));
                }
            }

            TStringBuilder operation;
            if (info.empty()) {
                operation << offset << " -> " << op.GetMapSafe().at("Name").GetString();
            } else {
                operation << offset << " -> " << op.GetMapSafe().at("Name").GetString()
                     << " (" << JoinStrings(info, ", ") << ")";
            }

            auto& row = table.AddRow();
            row.Column(0, std::move(operation));
            if (AnalyzeMode) {
                row.Column(3, std::move(eCost));
                row.Column(4, std::move(eRows));
            }
            else {
                row.Column(1, std::move(eCost));
                row.Column(2, std::move(eRows));
            }
        }
    } else {
        TStringBuilder operation;
        operation << offset << " -> " << node.at("Node Type").GetString();
        newRow.Column(0, std::move(operation));
    }

    if (node.contains("Plans")) {
        auto& plans = node.at("Plans").GetArraySafe();
        for (auto subplan : plans) {
            offset += "  ";
            PrintPrettyTableImpl(subplan, offset, table);
            offset.resize(offset.size() - 2);
        }
    }
}

TVector<NJson::TJsonValue> TQueryPlanPrinter::RemoveRedundantNodes(NJson::TJsonValue& plan, const THashSet<TString>& redundantNodes) {
    auto& planMap = plan.GetMapSafe();

    TVector<NJson::TJsonValue> children;
    if (planMap.contains("Plans")) {
        for (auto& child : planMap.at("Plans").GetArraySafe()) {
            auto newChildren = RemoveRedundantNodes(child, redundantNodes);
            children.insert(children.end(), newChildren.begin(), newChildren.end());
        }
    }

    planMap.erase("Plans");
    if (!children.empty()) {
        auto& plans = planMap["Plans"];
        for (auto& child :  children) {
            plans.AppendValue(child);
        }
    }

    const auto typeName = planMap.at("Node Type").GetStringSafe();
    if (redundantNodes.contains(typeName)) {
        return children;
    }

    return {plan};
}

THashMap<TString, NJson::TJsonValue> TQueryPlanPrinter::ExtractPrecomputes(NJson::TJsonValue& plan) {
    auto& planMap = plan.GetMapSafe();

    if (!planMap.contains("Plans")) {
        return {};
    }

    THashMap<TString, NJson::TJsonValue> precomputes;
    TVector<NJson::TJsonValue> results;
    for (auto& child : planMap.at("Plans").GetArraySafe()) {
        if (child.GetMapSafe().contains("Subplan Name")) {
            const auto& precomputeName = child.GetMapSafe().at("Subplan Name").GetStringSafe();

            auto pos = precomputeName.find("precompute");
            if (pos != TString::npos) {
                precomputes[precomputeName.substr(pos)] = std::move(child);
            }
        } else {
            results.push_back(std::move(child));
        }
    }

    planMap.erase("Plans");
    if (!results.empty()) {
        auto& plans = planMap["Plans"];
        for (auto& result : results) {
            plans.AppendValue(std::move(result));
        }
    }

    return precomputes;
}

void TQueryPlanPrinter::ResolvePrecomputeLinks(NJson::TJsonValue& plan, const THashMap<TString, NJson::TJsonValue>& precomputes) {
    auto& planMap = plan.GetMapSafe();

    if (planMap.contains("CTE Name")) {
        const auto& precomputeName = planMap.at("CTE Name").GetStringSafe();

        auto precomputeIt = precomputes.find(precomputeName);
        if (precomputeIt != precomputes.end()) {
            auto& precompute = precomputeIt->second;

            if (precompute.GetMapSafe().contains("Plans")) {
                auto& plans = planMap["Plans"];
                for (auto& child : precompute.GetMapSafe().at("Plans").GetArraySafe()) {
                    plans.AppendValue(child);
                }
            }

            if (planMap.contains("Operators")) {
                // delete precompute link from operator block
                for (auto &op : planMap.at("Operators").GetArraySafe()) {
                    if (op.GetMapSafe().contains("Iterator")) {
                        op.GetMapSafe().erase("Iterator");
                    } else if (op.GetMapSafe().contains("Input")) {
                        op.GetMapSafe().erase("Input");
                    }
                }
            }

        }

        // delete precompute link
        planMap.erase("CTE Name");
    }

    if (planMap.contains("Plans")) {
        for (auto& child : planMap.at("Plans").GetArraySafe()) {
            ResolvePrecomputeLinks(child, precomputes);
        }
    }
}

/**
 * Several joins fall into a single stage, so you need to split such stages to do this. 
 * To do this, you'll need to look through all of the nodes on the initial plan. 
 * As soon as you find at least two joins in a single node, divide the node into as many sections as there are joins, 
 * and distribute the inputs from the original section to each of the new sections. 
 * Each subsequent joint should be fed into the previous section, so they are connected in sequence from the end. 
 * After that, we change the stage with several joints to the stage with first join.
**/
void TQueryPlanPrinter::SplitJoinsPlan(NJson::TJsonValue& plan) {
    auto& planMap = plan.GetMapSafe();

    if (planMap.contains("Plans")) {
        NJson::TJsonValue joins;
        
        TVector<NJson::TJsonValue> grandchilds;

        for (auto& child : planMap.at("Plans").GetArray()) {
            const auto& nodeName = child.GetMapSafe().at("Node Type").GetStringSafe();
            // trying find 2 Join in 1 Node
            auto pos = nodeName.find("MapJoin", nodeName.find("MapJoin") + 1);

            if (pos != TString::npos) {
                // save inputs
                grandchilds.insert(grandchilds.end(), child.GetMapSafe().at("Plans").GetArray().begin(), child.GetMapSafe().at("Plans").GetArray().end());
                std::sort(grandchilds.begin(), grandchilds.end(), [](NJson::TJsonValue& a, NJson::TJsonValue& b) {
                    return a.GetMapSafe().at("PlanNodeId").GetIntegerSafe() > b.GetMapSafe().at("PlanNodeId").GetIntegerSafe();
                });

                NJson::TJsonValue inputs;
                // convert vector to TJsonValue
                for (size_t i = 0; i < grandchilds.size(); i++) {
                    NJson::TJsonValue inp;
                    inp.AppendValue(grandchilds[i]);
                    inputs.AppendValue(inp);
                }

                size_t inputsSize = inputs.GetArraySafe().size();
    
                NJson::TJsonValue operators;

                TString name = "";
                // counter of meeting joins, value from 0 to 2
                int meetJoins = 0;
                // counter of creating nodes
                size_t newNodeCount = 0;
                // counter not typed operators
                size_t opUncount = 0;

                NJson::TJsonValue temp;

                for (auto &op : child.GetMapSafe().at("Operators").GetArray()) {
                    opUncount++;
                    const auto& opName = op.GetMapSafe().at("Name").GetStringSafe();
                    pos = opName.find("Join");
                    if (pos != TString::npos) {
                        meetJoins++;
                    }
                    if (meetJoins < 2) {
                        // if we met less then 2 joins then complement name of node (op1-op2-...-opN)
                        if (name == "") {
                            name += opName;
                        } else {
                            name += "-" + opName;
                        }
                        operators.AppendValue(op);
                    } else {
                        // if we met 2 joins then create new Node
                        temp["Node Type"] = name;
                        temp["Operators"] = operators;
                        temp["PlanNodeId"] = child.GetMapSafe().at("PlanNodeId").GetIntegerSafe();
                        temp["Plans"] = inputs[newNodeCount];

                        NJson::TJsonValue arrayTemp;
                        arrayTemp.AppendValue(temp);
                        joins.AppendValue(arrayTemp);

                        name = opName;

                        operators.SetType(NJson::JSON_MAP);
                        operators.AppendValue(op);

                        meetJoins = 1;
                        newNodeCount++;

                        opUncount = 0;

                        if (newNodeCount >= inputsSize) {
                            ythrow NJson::TJsonException() << "Too few inputs";
                        }
                    }
                }
    
                if (opUncount != 0) {
                    temp["Node Type"] = name;
                    temp["Operators"] = operators;
                    temp["PlanNodeId"] = child.GetMapSafe().at("PlanNodeId").GetIntegerSafe();
                    temp["Plans"] = inputs[newNodeCount];
                    NJson::TJsonValue arrayTemp;
                    arrayTemp.AppendValue(temp);
                    joins.AppendValue(arrayTemp);
                }

                if (joins.GetArraySafe().size() != inputsSize) {
                    ythrow NJson::TJsonException() << "Too many inputs";
                }
            }
        }

        // connecting Joins
        if (joins.GetArray().size() > 1) {
            for (size_t i = joins.GetArray().size() - 1; i > 0; --i) {
                // add join[i] to join[i - 1]["Plans"]
                auto& currentPlans = joins[i].GetArraySafe();
                auto& prevPlans = joins[i - 1][0].GetMapSafe().at("Plans").GetArraySafe();
                prevPlans.insert(prevPlans.end(), currentPlans.begin(), currentPlans.end());
            }
            // replacing the original join with the first element of the joins array (now it contains all the added joins with plans)
            // planMap["Plans"] must be a TArray, so there is a TArray in Joins[0]
            planMap["Plans"] = joins[0];
        }

        for (auto& child : planMap.at("Plans").GetArraySafe()) {
            SplitJoinsPlan(child);
        }
    }    
}

void TQueryPlanPrinter::SimplifyQueryPlan(NJson::TJsonValue& plan) {
    static const THashSet<TString> redundantNodes = {
        "UnionAll",
        "Broadcast",
        "Map",
        "HashShuffle",
        "Merge",
        "Collect",
        "Stage"
    };

    RemoveRedundantNodes(plan, redundantNodes);
    auto precomputes = ExtractPrecomputes(plan);
    ResolvePrecomputeLinks(plan, precomputes);
    SplitJoinsPlan(plan);
}

TString TQueryPlanPrinter::JsonToString(const NJson::TJsonValue& jsonValue) {
    return jsonValue.GetStringRobust();
}


TResultSetPrinter::TResultSetPrinter(EOutputFormat format, std::function<bool()> isInterrupted)
    : Format(format)
    , IsInterrupted(isInterrupted)
{}

TResultSetPrinter::~TResultSetPrinter() {
    if (PrintedSomething && !IsInterrupted()) {
        EndResultSet();
    }
}

void TResultSetPrinter::Print(const TResultSet& resultSet) {
    if (FirstPart) {
        BeginResultSet();
    }
    PrintedSomething = true;

    switch (Format) {
    case EOutputFormat::Default:
    case EOutputFormat::Pretty:
        PrintPretty(resultSet);
        break;
    case EOutputFormat::JsonUnicodeArray:
        PrintJsonArray(resultSet, EBinaryStringEncoding::Unicode);
        break;
    case EOutputFormat::JsonUnicode:
        FormatResultSetJson(resultSet, &Cout, EBinaryStringEncoding::Unicode);
        break;
    case EOutputFormat::JsonBase64Array:
        PrintJsonArray(resultSet, EBinaryStringEncoding::Base64);
        break;
    case EOutputFormat::JsonBase64:
        FormatResultSetJson(resultSet, &Cout, EBinaryStringEncoding::Base64);
        break;
    case EOutputFormat::Csv:
        PrintCsv(resultSet, ",");
        break;
    case EOutputFormat::Tsv:
        PrintCsv(resultSet, "\t");
        break;
    default:
        throw TMisuseException() << "This command doesn't support " << Format << " output format";
    }

    if (FirstPart) {
        FirstPart = false;
    }
}

void TResultSetPrinter::Reset() {
    if (PrintedSomething) {
        EndResultSet();
        FirstPart = true;
    }
}

void TResultSetPrinter::BeginResultSet() {
    switch (Format) {
    case EOutputFormat::JsonUnicodeArray:
    case EOutputFormat::JsonBase64Array:
        Cout << '[';
        break;
    default:
        break;
    }
}

void TResultSetPrinter::EndResultSet() {
    switch (Format) {
    case EOutputFormat::JsonUnicodeArray:
    case EOutputFormat::JsonBase64Array:
        Cout << ']' << Endl;
        break;
    default:
        break;
    }
}

void TResultSetPrinter::EndLineBeforeNextResult() {
    switch (Format) {
    case EOutputFormat::JsonUnicodeArray:
    case EOutputFormat::JsonBase64Array:
        Cout << ',' << Endl;
        break;
    default:
        break;
    }
}

void TResultSetPrinter::PrintPretty(const TResultSet& resultSet) {
    const TVector<TColumn>& columns = resultSet.GetColumnsMeta();
    TResultSetParser parser(resultSet);
    TVector<TString> columnNames;
    for (const auto& column : columns) {
        columnNames.push_back(column.Name);
    }

    TPrettyTableConfig tableConfig;
    if (!FirstPart) {
        tableConfig.WithoutHeader();
    }
    TPrettyTable table(columnNames, tableConfig);

    while (parser.TryNextRow()) {
        auto& row = table.AddRow();
        for (ui32 i = 0; i < columns.size(); ++i) {
            row.Column(i, FormatValueJson(parser.GetValue(i), EBinaryStringEncoding::Unicode));
        }
    }

    Cout << table;
}

void TResultSetPrinter::PrintJsonArray(const TResultSet& resultSet, EBinaryStringEncoding encoding) {
    auto columns = resultSet.GetColumnsMeta();

    TResultSetParser parser(resultSet);
    bool firstRow = true;
    while (parser.TryNextRow()) {
        if (!firstRow || !FirstPart) {
            EndLineBeforeNextResult();
        }
        if (firstRow) {
            firstRow = false;
        }
        NJsonWriter::TBuf writer(NJsonWriter::HEM_UNSAFE, &Cout);
        FormatResultRowJson(parser, columns, writer, encoding);
    }
}

void TResultSetPrinter::PrintCsv(const TResultSet& resultSet, const char* delim) {
    const TVector<TColumn>& columns = resultSet.GetColumnsMeta();
    TResultSetParser parser(resultSet);
    while (parser.TryNextRow()) {
        for (ui32 i = 0; i < columns.size(); ++i) {
            Cout << FormatValueJson(parser.GetValue(i), EBinaryStringEncoding::Unicode);
            if (i < columns.size() - 1) {
                Cout << delim;
            }
        }
        Cout << Endl;
    }
}

}
}
