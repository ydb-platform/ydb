#include "format.h"

#include <util/string/vector.h>
#include <library/cpp/json/json_prettifier.h>

#include <ydb/public/lib/json_value/ydb_json_value.h>
#include <ydb/library/arrow_parquet/result_set_parquet_printer.h>

#include <iomanip>
#include <strstream>

namespace NYdb {
namespace NConsoleClient {

namespace {
    THashMap<EDataFormat, TString> InputFormatDescriptions = {
        { EDataFormat::JsonUnicode, "Input in json format, binary strings are decoded with unicode characters" },
        { EDataFormat::JsonBase64, "Input in json format, binary strings are decoded with base64" },
        { EDataFormat::Csv, "Input in csv format" },
        { EDataFormat::Tsv, "Input in tsv format" },
    };

    THashMap<EDataFormat, TString> StdinFormatDescriptions = {
        { EDataFormat::JsonUnicode, "Parameter names and values in json unicode format" },
        { EDataFormat::JsonBase64, "Parameter names and values in json unicode format, binary string parameter values are base64-encoded" },
        { EDataFormat::Csv, "Parameter names and values in csv format" },
        { EDataFormat::Tsv, "Parameter names and values in tsv format" },
        { EDataFormat::NewlineDelimited, "Newline character delimits parameter sets on stdin and triggers "
                                            "processing in accordance to \"batch\" option" },
        { EDataFormat::Raw, "Binary value with no transformations or parsing, parameter name is set by an \"stdin-par\" option" },
        { EDataFormat::NoFraming, "Data from stdin is taken as a single set of parameters" },
    };

    THashMap<EDataFormat, TString> FormatDescriptions = {
        { EDataFormat::Pretty, "Human readable output" },
        { EDataFormat::PrettyTable, "Human readable table output" },
        { EDataFormat::Json, "Output in json format" },
        { EDataFormat::JsonUnicode, "Output in json format, binary strings are encoded with unicode characters. "
                                      "Every row is a separate json on a separate line." },
        { EDataFormat::JsonUnicodeArray, "Output in json format, binary strings are encoded with unicode characters. "
                                           "Every resultset is a json array of rows. "
                                           "Every row is a separate json on a separate line." },
        { EDataFormat::JsonBase64, "Output in json format, binary strings are encoded with base64. "
                                     "Every row is a separate json on a separate line." },
        { EDataFormat::JsonBase64Simplify, "Output in json format, binary strings are encoded with base64. "
                                     "Every row is a separate json on a separate line. " 
                                     "Output only basic information about plan." },
        { EDataFormat::JsonBase64Array, "Output in json format, binary strings are encoded with base64. "
                                           "Every resultset is a json array of rows. "
                                           "Every row is a separate json on a separate line." },
        { EDataFormat::JsonRawArray, "Output in json format, binary strings are not encoded."
                                        "Every resultset is a json array of rows. "
                                        "Every row is a separate binary data on a separate line"},
        { EDataFormat::ProtoJsonBase64, "Output result protobuf in json format, binary strings are encoded with base64" },
        { EDataFormat::Csv, "Output in csv format" },
        { EDataFormat::Tsv, "Output in tsv format" },
        { EDataFormat::Parquet, "Output in parquet format" },
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
        .StoreValue(&OutputFormat, EDataFormat::Json).StoreValue(&DeprecatedOptionUsed, true)
        .Hidden();
}

void TCommandWithFormat::AddInputFormats(TClientCommand::TConfig& config, 
                                         const TVector<EDataFormat>& allowedFormats, EDataFormat defaultFormat) {
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

void TCommandWithFormat::AddStdinFormats(TClientCommand::TConfig &config, const TVector<EDataFormat>& allowedStdinFormats,
                                         const TVector<EDataFormat>& allowedFramingFormats) {
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

void TCommandWithFormat::AddOutputFormats(TClientCommand::TConfig& config, 
                                    const TVector<EDataFormat>& allowedFormats, EDataFormat defaultFormat) {
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
    if (InputFormat != EDataFormat::Default
            && std::find(AllowedInputFormats.begin(), AllowedInputFormats.end(), InputFormat) == AllowedInputFormats.end()) {
        throw TMisuseException() << "Input format " << InputFormat << " is not available for this command";
    }

    if (!StdinFormats.empty()) {
        for (const auto& format : AllowedInputFormats) {
            Y_ABORT_UNLESS(std::find(AllowedStdinFormats.begin(), AllowedStdinFormats.end(), format) != AllowedStdinFormats.end(), 
                     "Allowed stdin formats should contain all allowed input formats");
        }
        for (const auto& format : StdinFormats) {
            if (format == EDataFormat::Default) {
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

    if (OutputFormat == EDataFormat::Default || DeprecatedOptionUsed) {
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
        case EDataFormat::Default:
        case EDataFormat::JsonBase64Simplify:
        case EDataFormat::Pretty:
        case EDataFormat::PrettyTable: {
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

                    if (Format == EDataFormat::PrettyTable) {
                        PrintPrettyTable(query);
                    } else if (Format == EDataFormat::JsonBase64Simplify) {
                        PrintSimplifyJson(query);
                    } else{
                        PrintPretty(query);
                    }
                }
            } else {
                if (Format == EDataFormat::PrettyTable) {
                    PrintPrettyTable(planJson);
                } else if (Format == EDataFormat::JsonBase64Simplify) {
                    PrintSimplifyJson(planJson);
                } else {
                    PrintPretty(planJson);
                }
            }

            break;
        }
        case EDataFormat::JsonUnicode:
        case EDataFormat::JsonBase64:
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

void TQueryPlanPrinter::PrintSimplifyJson(const NJson::TJsonValue& plan) {
    if (plan.GetMapSafe().contains("SimplifiedPlan")) {
        auto queryPlan = plan.GetMapSafe().at("SimplifiedPlan");
        Output << NJson::PrettifyJson(JsonToString(queryPlan), false) << Endl;
    } else { /* old format plan */
        PrintJson(plan.GetStringRobust());
    }
}

void TQueryPlanPrinter::PrintPrettyTable(const NJson::TJsonValue& plan) {
    static const TVector<TString> explainColumnNames = {"Operation", "E-Cost", "E-Rows", "E-Size"};
    static const TVector<TString> explainAnalyzeColumnNames = {"Operation", "A-Cpu", "A-Rows", "E-Cost", "E-Rows", "E-Size"};

    if (plan.GetMapSafe().contains("SimplifiedPlan")) {
        auto queryPlan = plan.GetMapSafe().at("SimplifiedPlan");

        TPrettyTable table(AnalyzeMode ? explainAnalyzeColumnNames : explainColumnNames,
            TPrettyTableConfig().WithoutRowDelimiters().MaxWidth(MaxWidth));

        Y_ENSURE(queryPlan.GetMapSafe().contains("Plans"));

        TString offset;
        for (auto subplan : queryPlan.GetMapSafe().at("Plans").GetArraySafe()) {
            PrintPrettyTableImpl(subplan, offset, table);
        }

        Output << table;
    } else { /* old format plan */
        throw TMisuseException() << "Got no logical plan from the server. "
            "Most likely, server version is old and does not support logical plans yet. "
            "Try also using \"--execution-plan\" option to see the execution plan";
    }
}

TString ReplaceAll(TString str, const TString& from, const TString& to) {
    if (!from) {
        return str;
    }
        
    size_t startPos = 0;
    while ((startPos = str.find(from, startPos)) != TString::npos) {
        str.replace(startPos, from.length(), to);
        startPos += to.length();
    }

    return str;
}

TString FormatPrettyTableDouble(TString stringValue) {
    std::strstream stream;

    double value = 0.0;

    try {
        value = std::stod(stringValue);
    } catch (const std::invalid_argument& ia) {
        return stringValue;
    }

    if (1e-3 < value && value < 1e8 || value == 0) {
        stream << static_cast<int64_t>(std::round(value)) << '\0';
        return ToString(stream.str());
    }


    stream << std::fixed << std::setprecision(3) << std::scientific << value << '\0';
    return ToString(stream.str());   
}

void TQueryPlanPrinter::PrintPrettyTableImpl(const NJson::TJsonValue& plan, TString& offset, TPrettyTable& table) {
    const auto& node = plan.GetMapSafe();

    auto& newRow = table.AddRow();

    NColorizer::TColors colors = NColorizer::AutoColors(Output);
    TStringBuf color;
    switch(offset.size() % 3) {
        case 0: 
            color = colors.LightRed();
            break;
        case 1:
            color = colors.LightGreen();
            break;
        case 2:
            color = colors.LightBlue();
            break;
        default:
            color = colors.Default();
            break;
    }

    if (node.contains("Operators")) {
        for (const auto& op : node.at("Operators").GetArraySafe()) {
            TVector<TString> info;
            TString aCpu;
            TString aRows;
            TString eCost;
            TString eRows;
            TString eSize;

            for (const auto& [key, value] : op.GetMapSafe()) {
                if (key == "A-Cpu") {
                    aCpu = FormatPrettyTableDouble(std::to_string(value.GetDouble()));
                } else if (key == "A-Rows") {
                    aRows = FormatPrettyTableDouble(std::to_string(value.GetDouble()));
                } else if (key == "E-Cost") {
                    eCost = FormatPrettyTableDouble(value.GetString());
                } else if (key == "E-Rows") {
                    eRows = FormatPrettyTableDouble(value.GetString());
                } else if (key == "E-Size") {
                    eSize = FormatPrettyTableDouble(value.GetString());
                } else if (key != "Name") {
                    if (key == "SsaProgram") {
                        // skip this attribute
                    }
                    else if (key == "Predicate" || key == "Condition" || key == "SortBy") {
                        info.emplace_back(TStringBuilder() << ReplaceAll(ReplaceAll(JsonToString(value), "item.", ""), "state.", ""));
                    } else if (key == "Table") {
                        info.insert(info.begin(), TStringBuilder() << colors.LightYellow() << key << colors.Default() << ":" << colors.LightGreen() << " " << ReplaceAll(ReplaceAll(JsonToString(value), "item.", ""), "state.", "") << colors.Default());
                    } else {
                        info.emplace_back(TStringBuilder() << colors.LightYellow() << key << colors.Default() << ": " << ReplaceAll(ReplaceAll(JsonToString(value), "item.", ""), "state.", ""));
                    }
                }
            }

            TStringBuilder operation;
            if (info.empty()) {
                operation << offset << color << " -> " << colors.LightCyan() << op.GetMapSafe().at("Name").GetString() << colors.Default();
            } else {
                operation << offset << color << " -> " << colors.LightCyan() << op.GetMapSafe().at("Name").GetString() << colors.Default()
                     << " (" << JoinStrings(info, ", ") << ")";
            }

            newRow.Column(0, std::move(operation));
            if (AnalyzeMode) {
                newRow.Column(1, std::move(aCpu));
                newRow.Column(2, std::move(aRows));
                newRow.Column(3, std::move(eCost));
                newRow.Column(4, std::move(eRows));
                newRow.Column(5, std::move(eSize));
            }
            else {
                newRow.Column(1, std::move(eCost));
                newRow.Column(2, std::move(eRows));
                newRow.Column(3, std::move(eSize));
            }
        }
    } else {
        TStringBuilder operation;
        operation << offset << color << " -> " << colors.LightCyan() << node.at("Node Type").GetString() << colors.Default();
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

TString TQueryPlanPrinter::JsonToString(const NJson::TJsonValue& jsonValue) {
    return jsonValue.GetStringRobust();
}

TResultSetPrinter::TResultSetPrinter(EDataFormat format, std::function<bool()> isInterrupted)
    : Format(format)
    , IsInterrupted(isInterrupted)
    , ParquetPrinter(std::make_unique<TResultSetParquetPrinter>(""))
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
    case EDataFormat::Default:
    case EDataFormat::Pretty:
        PrintPretty(resultSet);
        break;
    case EDataFormat::JsonUnicodeArray:
        PrintJsonArray(resultSet, EBinaryStringEncoding::Unicode);
        break;
    case EDataFormat::JsonUnicode:
        FormatResultSetJson(resultSet, &Cout, EBinaryStringEncoding::Unicode);
        break;
    case EDataFormat::JsonBase64Array:
        PrintJsonArray(resultSet, EBinaryStringEncoding::Base64);
        break;
    case EDataFormat::JsonBase64:
        FormatResultSetJson(resultSet, &Cout, EBinaryStringEncoding::Base64);
        break;
    case EDataFormat::Csv:
        PrintCsv(resultSet, ",");
        break;
    case EDataFormat::Tsv:
        PrintCsv(resultSet, "\t");
        break;
    case EDataFormat::Parquet:
        ParquetPrinter->Print(resultSet);
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
    case EDataFormat::JsonUnicodeArray:
    case EDataFormat::JsonBase64Array:
        Cout << '[';
        break;
    default:
        break;
    }
}

void TResultSetPrinter::EndResultSet() {
    switch (Format) {
    case EDataFormat::JsonUnicodeArray:
    case EDataFormat::JsonBase64Array:
        Cout << ']' << Endl;
        break;
    case EDataFormat::Parquet:
        ParquetPrinter->Reset();
        break;
    default:
        break;
    }
}

void TResultSetPrinter::EndLineBeforeNextResult() {
    switch (Format) {
    case EDataFormat::JsonUnicodeArray:
    case EDataFormat::JsonBase64Array:
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
