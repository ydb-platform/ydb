#include "format.h"

#include <util/string/vector.h>
#include <library/cpp/json/json_prettifier.h>

#include <ydb/public/lib/json_value/ydb_json_value.h>
#include <ydb/public/lib/ydb_cli/common/colors.h>
#include <ydb/library/arrow_parquet/result_set_parquet_printer.h>

#include <iomanip>
#include <regex>

namespace NYdb::NConsoleClient {

namespace {
    THashMap<EDataFormat, TString> DefaultInputFormatDescriptions = {
        { EDataFormat::Json, "Input in json format, to set binary string encoding use --input-binary-strings option" },
        { EDataFormat::JsonUnicode, "Input in json format, binary strings are decoded with unicode characters" },
        { EDataFormat::JsonBase64, "Input in json format, binary strings are decoded with base64" },
        { EDataFormat::Csv, "Input in csv format" },
        { EDataFormat::Tsv, "Input in tsv format" },
    };

    THashMap<EFramingFormat, TString> InputFramingDescriptions = {
        { EFramingFormat::NewlineDelimited, "Newline character delimits parameter sets on the input and triggers "
                                            "processing in accordance to \"--input-batch\" option" },
        { EFramingFormat::NoFraming, "Data from input is taken as a single set of parameters" },
    };

    THashMap<EBinaryStringEncodingFormat, TString> BinaryStringEncodingFormatDescriptions = {
        { EBinaryStringEncodingFormat::Unicode, "Every byte in binary strings that is not a printable ASCII symbol (codes 32-126) should be encoded as utf-8" },
        { EBinaryStringEncodingFormat::Base64, "Binary strings should be fully encoded with base64" },
    };

    THashMap<EDataFormat, TString> FormatDescriptions = {
        { EDataFormat::Pretty, "Human readable output" },
        { EDataFormat::PrettyTable, "Human readable table output" },
        { EDataFormat::Json, "Json format" },
        { EDataFormat::JsonUnicode, "Json format, binary strings are encoded with unicode characters." },
        { EDataFormat::JsonUnicodeArray, "Json format, binary strings are encoded with unicode characters. "
                                           "Every resultset is a json array of rows. "
                                           "Every row is a separate json on a separate line." },
        { EDataFormat::JsonBase64, "Json format, binary strings are encoded with base64." },
        { EDataFormat::JsonBase64Simplify, "Json format, binary strings are encoded with base64. "
                                     "Only basic information about plan." },
        { EDataFormat::JsonBase64Array, "Json format, binary strings are encoded with base64. "
                                           "Every resultset is a json array of rows. "
                                           "Every row is a separate json on a separate line." },
        { EDataFormat::JsonRawArray, "Json format, binary strings are not encoded."
                                        "Every resultset is a json array of rows. "
                                        "Every row is a separate binary data on a separate line"},
        { EDataFormat::ProtoJsonBase64, "Protobuf in json format, binary strings are encoded with base64" },
        { EDataFormat::Csv, "CSV format" },
        { EDataFormat::Tsv, "TSV format" },
        { EDataFormat::Parquet, "Parquet format" },
    };

    THashMap<EMessagingFormat, TString> MessagingFormatDescriptions = {
        { EMessagingFormat::Pretty, "Human readable output with metadata." },
        { EMessagingFormat::SingleMessage, "Single message."}, // TODO(shmel1k@): improve
        { EMessagingFormat::NewlineDelimited, "Newline delimited stream of messages."}, // TODO(shmel1k@): improve
        { EMessagingFormat::Concatenated, "Concatenated output stream of messages."}, // TODO(shmel1k@): improve,
        { EMessagingFormat::JsonStreamConcat, "Concatenated Json stream of envelopes with metadata and messages in the ""body"" attribute." }, // TODO(shmel1k@): improve,
        { EMessagingFormat::JsonArray, "Json array of envelopes with metadata and messages in the ""body"" attribute." }, // TODO(shmel1k@): improve,
        { EMessagingFormat::Csv, "CSV format with header row containing metadata field names." },
        { EMessagingFormat::Tsv, "TSV format with header row containing metadata field names." },
    };
} // anonymous namespace

void TCommandWithResponseHeaders::PrintResponseHeader(const TStatus& status) {
    if (!ShowHeaders) {
        return;
    }

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


bool TCommandWithFormat::IsIoCommand(){
    return HasInput() && HasOutput();
}

bool TCommandWithFormat::HasInput() {
    return false;
}

bool TCommandWithFormat::HasOutput() {
    return false;
}

void TCommandWithInput::AddInputFormats(TClientCommand::TConfig& config,
                                         const TVector<EDataFormat>& allowedFormats, EDataFormat defaultFormat) {
    TStringStream description;
    description << "Input format. Available options: ";
    NColorizer::TColors colors = NConsoleClient::AutoColors(Cout);
    Y_ABORT_UNLESS(std::find(allowedFormats.begin(), allowedFormats.end(), defaultFormat) != allowedFormats.end(),
        "Couldn't find default input format %s in allowed formats", (TStringBuilder() << defaultFormat).c_str());
    auto& inputFormatDescriptions = GetInputFormatDescriptions();
    for (const auto& format : allowedFormats) {
        auto findResult = inputFormatDescriptions.find(format);
        Y_ABORT_UNLESS(findResult != inputFormatDescriptions.end(),
            "Couldn't find description for %s input format", (TStringBuilder() << format).c_str());
        description << "\n  " << colors.BoldColor() << format << colors.OldColor()
            << "\n    " << findResult->second;
        Y_ABORT_UNLESS(!AllowedInputFormats.contains(format),
            "%s input format is added twice", (TStringBuilder() << format).c_str());
        AllowedInputFormats.insert(format);
    }
    description << "\nDefault: " << colors.CyanColor() << "\"" << defaultFormat << "\"" << colors.OldColor() << ".";
    if (config.HelpCommandVerbosiltyLevel <= 1) {
        description << Endl << "Use -hh option to see all options relevant to input format.";
    }
    config.Opts->AddLongOption("input-format", description.Str())
        .RequiredArgument("STRING").StoreResult(&InputFormat);
}

void TCommandWithInput::AddInputFramingFormats(TClientCommand::TConfig &config,
        const TVector<EFramingFormat>& allowedFormats, EFramingFormat defaultFormat) {
    TStringStream description;
    NColorizer::TColors colors = NConsoleClient::AutoColors(Cout);
    Y_ABORT_UNLESS(std::find(allowedFormats.begin(), allowedFormats.end(), defaultFormat) != allowedFormats.end(),
        "Couldn't find default framing format %s in allowed formats", (TStringBuilder() << defaultFormat).c_str());
    description << "Input framing format. Defines how parameter sets are delimited on the input. Available options: ";
    for (const auto& format : allowedFormats) {
        auto findResult = InputFramingDescriptions.find(format);
        Y_ABORT_UNLESS(findResult != InputFramingDescriptions.end(),
                 "Couldn't find description for %s framing format", (TStringBuilder() << format).c_str());
        description << "\n  " << colors.BoldColor() << format << colors.OldColor()
                    << "\n    " << findResult->second;
        Y_ABORT_UNLESS(!AllowedInputFramingFormats.contains(format),
            "%s input framing format is added twice", (TStringBuilder() << format).c_str());
        AllowedInputFramingFormats.insert(format);
    }
    description << "\nDefault: " << colors.CyanColor() << "\"" << defaultFormat << "\"" << colors.OldColor() << ".";
    auto& inputFraming = config.Opts->AddLongOption("input-framing", description.Str())
            .RequiredArgument("STRING").StoreResult(&InputFramingFormat);
    if (config.HelpCommandVerbosiltyLevel <= 1) {
        inputFraming.Hidden();
    }
}

void TCommandWithInput::AddInputBinaryStringEncodingFormats(TClientCommand::TConfig& config,
        const TVector<EBinaryStringEncodingFormat>& allowedFormats, EBinaryStringEncodingFormat defaultFormat) {
    TStringStream description;
    description << "Input binary strings encoding format. Sets how binary strings in the input should be interpreted. Available options: ";
    NColorizer::TColors colors = NConsoleClient::AutoColors(Cout);
    Y_ABORT_UNLESS(std::find(allowedFormats.begin(), allowedFormats.end(), defaultFormat) != allowedFormats.end(),
        "Couldn't find default binary string format %s in allowed formats", (TStringBuilder() << defaultFormat).c_str());
    for (const auto& format : allowedFormats) {
        auto findResult = BinaryStringEncodingFormatDescriptions.find(format);
        Y_ABORT_UNLESS(findResult != BinaryStringEncodingFormatDescriptions.end(),
            "Couldn't find description for %s binary string format", (TStringBuilder() << format).c_str());
        description << "\n  " << colors.BoldColor() << format << colors.OldColor()
            << "\n    " << findResult->second;
        Y_ABORT_UNLESS(!AllowedBinaryStringEncodingFormats.contains(format),
            "%s binary string format is added twice", (TStringBuilder() << format).c_str());
        AllowedBinaryStringEncodingFormats.insert(format);
    }
    description << "\nDefault: " << colors.CyanColor() << "\"" << defaultFormat << "\"" << colors.OldColor() << ".";
    config.Opts->AddLongOption("input-binary-strings", description.Str())

        .RequiredArgument("STRING").StoreResult(&InputBinaryStringEncodingFormat);
}

void TCommandWithInput::AddLegacyInputFormats(TClientCommand::TConfig& config, const TString& legacyName,
        const TVector<TString>& newNames, const TVector<EDataFormat>& allowedFormats) {
    for (const auto& format : allowedFormats) {
        Y_ABORT_UNLESS(!AllowedInputFormats.contains(format),
            "%s legacy input format is already added to allowed input formats", (TStringBuilder() << format).c_str());
        AllowedInputFormats.insert(format);
    }
    config.Opts->AddLongOption(legacyName)
            .RequiredArgument("STRING").AppendTo(&LegacyInputFormats)
            .Hidden();
    for (const auto& newName : newNames) {
        config.Opts->MutuallyExclusive(legacyName, newName);
    }
}

void TCommandWithInput::AddLegacyJsonInputFormats(TClientCommand::TConfig& config) {
    AddInputBinaryStringEncodingFormats(config, {
        EBinaryStringEncodingFormat::Unicode,
        EBinaryStringEncodingFormat::Base64,
    });
    for (const auto& format : { EDataFormat::JsonUnicode, EDataFormat::JsonBase64}) {
        Y_ABORT_UNLESS(!AllowedInputFormats.contains(format),
            "%s legacy input format is already added to allowed input formats", (TStringBuilder() << format).c_str());
        AllowedInputFormats.insert(format);
    }
    config.Opts->AddLongOption("input-format")
        .RequiredArgument("STRING").StoreResult(&InputFormat)
        .Hidden();
    config.Opts->MutuallyExclusive("input-format", "input-binary-strings");
}

THashMap<EDataFormat, TString>& TCommandWithInput::GetInputFormatDescriptions() {
    return DefaultInputFormatDescriptions;
}

bool TCommandWithInput::HasInput() {
    return true;
}

void TCommandWithInput::AddInputFileOption(TClientCommand::TConfig& config, bool allowMultiple,
        const TString& description) {
    AllowMultipleInputFiles = allowMultiple;
    config.Opts->AddLongOption('i', "input-file", description)
        .RequiredArgument("PATH").AppendTo(&InputFiles);
}

// Deprecated
void TCommandWithOutput::AddDeprecatedJsonOption(TClientCommand::TConfig& config, const TString& description) {
    config.Opts->GetOpts().AddLongOption("json", description).NoArgument()
        .StoreValue(&OutputFormat, EDataFormat::Json).StoreValue(&DeprecatedOptionUsed, true)
        .Hidden();
}

void TCommandWithOutput::AddOutputFormats(TClientCommand::TConfig& config,
                                    const TVector<EDataFormat>& allowedFormats, EDataFormat defaultFormat) {
    TStringStream description;
    description << "Output format. Available options: ";
    NColorizer::TColors colors = NConsoleClient::AutoColors(Cout);
    Y_ABORT_UNLESS(std::find(allowedFormats.begin(), allowedFormats.end(), defaultFormat) != allowedFormats.end(),
        "Couldn't find default output format %s in allowed formats", (TStringBuilder() << defaultFormat).c_str());
    bool printComma = false;
    for (const auto& format : allowedFormats) {
        auto findResult = FormatDescriptions.find(format);
        Y_ABORT_UNLESS(findResult != FormatDescriptions.end(),
            "Couldn't find description for %s output format", (TStringBuilder() << format).c_str());
        if (config.HelpCommandVerbosiltyLevel >= 2) {
            description << "\n  " << colors.BoldColor() << format << colors.OldColor()
                << "\n    " << findResult->second;
        } else {
            if (printComma) {
                description << ", ";
            } else {
                printComma = true;
            }
            description << colors.BoldColor() << format << colors.OldColor();
        }
    }
    if (config.HelpCommandVerbosiltyLevel >= 2) {
        description << "\nDefault: " << colors.CyanColor() << defaultFormat << colors.OldColor() << ".";
    } else {
        description << " (default: " << colors.CyanColor() << defaultFormat << colors.OldColor() << ")";
    }
    config.Opts->AddLongOption("format", description.Str())
        .RequiredArgument("STRING").StoreResult(&OutputFormat);
    AllowedFormats = allowedFormats;
}

void TCommandWithInput::ParseInputFormats() {
    if (InputFormat != EDataFormat::Default
            && std::find(AllowedInputFormats.begin(), AllowedInputFormats.end(), InputFormat) == AllowedInputFormats.end()) {
        throw TMisuseException() << "Input format " << InputFormat << " is not available for this command";
    }

    if (!LegacyInputFormats.empty()) {
        for (const auto& format : LegacyInputFormats) {
            switch (format) {
                case EDataFormat::NoFraming:
                case EDataFormat::NewlineDelimited:
                {
                    EFramingFormat framingFormat = format == EDataFormat::NoFraming ? EFramingFormat::NoFraming : EFramingFormat::NewlineDelimited;
                    if (AllowedInputFramingFormats.contains(framingFormat)) {
                        if (InputFramingFormat != EFramingFormat::Default) {
                            throw TMisuseException() << "Input framing format can be set only once";
                        }
                        InputFramingFormat = framingFormat;
                    } else {
                        throw TMisuseException() << "Stdin framing format " << format << " is not allowed.";
                    }
                    break;
                }
                default:
                    if (AllowedInputFormats.contains(format)) {
                        if (InputFormat != EDataFormat::Default) {
                            throw TMisuseException() << "Stdin format " << format << " is not allowed.";
                        }
                        InputFormat = format;
                    }
                    break;
            }
        }
    }

    switch (InputBinaryStringEncodingFormat) {
        case EBinaryStringEncodingFormat::Default:
            switch(InputFormat) {
                case EDataFormat::JsonBase64:
                    InputBinaryStringEncoding = EBinaryStringEncoding::Base64;
                    break;
                case EDataFormat::JsonUnicode:
                default:
                    InputBinaryStringEncoding = EBinaryStringEncoding::Unicode;
                    break;
            }
            break;
        case EBinaryStringEncodingFormat::Unicode:
            InputBinaryStringEncoding = EBinaryStringEncoding::Unicode;
            break;
        case EBinaryStringEncodingFormat::Base64:
            InputBinaryStringEncoding = EBinaryStringEncoding::Base64;
            break;
        default:
            throw TMisuseException() << "Unknown binary string encoding format: " << InputBinaryStringEncodingFormat;
    }

    if (InputFiles.size() > 1 && !AllowMultipleInputFiles) {
        throw TMisuseException() << "Multiple input files are not allowed for this command";
    }
}

void TCommandWithOutput::ParseOutputFormats() {
    if (OutputFormat == EDataFormat::Default || DeprecatedOptionUsed) {
        return;
    }
    if (std::find(AllowedFormats.begin(), AllowedFormats.end(), OutputFormat) == AllowedFormats.end()) {
        throw TMisuseException() << "Output format " << OutputFormat << " is not available for this command";
    }
}

void TCommandWithMessagingFormat::AddMessagingFormats(TClientCommand::TConfig& config, const TVector<EMessagingFormat>& allowedFormats) {
    TStringStream description;
    description << "Client-side format. Available options: ";
    NColorizer::TColors colors = NConsoleClient::AutoColors(Cout);
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

void TCommandWithMessagingFormat::ParseMessagingFormats() {
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
        NColorizer::TColors colors = NConsoleClient::AutoColors(Output);
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
    static const TVector<TString> explainColumnNames = {"E-Cost", "E-Rows", "E-Size", "Operation"};
    static const TVector<TString> explainAnalyzeColumnNames = {"A-Cpu", "A-Rows", "E-Cost", "E-Rows", "E-Size", "Operation"};

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
    std::stringstream stream;

    double value = 0.0;

    try {
        value = std::stod(stringValue);
    } catch (const std::invalid_argument& ia) {
        return stringValue;
    }

    if (1e-3 < value && value < 1e8 || value == 0) {
        stream << static_cast<int64_t>(std::round(value));
        return ToString(stream.str());
    }


    stream << std::fixed << std::setprecision(3) << std::scientific << value;
    return ToString(stream.str());
}

void TQueryPlanPrinter::PrintPrettyTableImpl(const NJson::TJsonValue& plan, TString& offset, TPrettyTable& table, bool isLast, TVector<bool> hasMore) {
    const auto& node = plan.GetMapSafe();

    auto& newRow = table.AddRow();

    NColorizer::TColors colors = NConsoleClient::AutoColors(Output);

    bool hasChildren = node.contains("Plans") && !node.at("Plans").GetArraySafe().empty();

    TStringBuilder arrowOffset;
    for (size_t i = 0; i < hasMore.size(); ++i) {
        if (hasMore[i]) {
            arrowOffset << "│ ";
        } else {
            arrowOffset << "  ";
        }
    }

    if (offset.empty()) {
        arrowOffset << "┌> ";
    } else {
        if (isLast) {
            if (hasChildren) {
                arrowOffset << "└─┬> ";
            } else {
                arrowOffset << "└──> ";
            }
        } else {
            if (hasChildren) {
                arrowOffset << "├─┬> ";
            } else {
                arrowOffset << "├──> ";
            }
        }
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
                    std::string valueWithoutInnerVars = JsonToString(value);
                    valueWithoutInnerVars = std::regex_replace(valueWithoutInnerVars, std::regex(R"((\b(item|state)\.\b))"), "");

                    if (key == "SsaProgram") {
                        // skip this attribute
                    }
                    else if (key == "Predicate" || key == "Condition" || key == "SortBy") {
                        info.emplace_back(TStringBuilder() << valueWithoutInnerVars);
                    } else if (key == "Table") {
                        info.insert(info.begin(), TStringBuilder() << colors.LightYellow() << key << colors.Default() << ":" << colors.LightGreen() << " " << valueWithoutInnerVars << colors.Default());
                    } else {
                        info.emplace_back(TStringBuilder() << colors.LightYellow() << key << colors.Default() << ": " << valueWithoutInnerVars);
                    }
                }
            }

            TStringBuilder operation;
            if (info.empty()) {
                operation << arrowOffset << colors.LightCyan() << op.GetMapSafe().at("Name").GetString() << colors.Default();
            } else {
                operation << arrowOffset << colors.LightCyan() << op.GetMapSafe().at("Name").GetString() << colors.Default()
                     << " (" << JoinStrings(info, ", ") << ")";
            }

            if (AnalyzeMode) {
                newRow.Column(0, std::move(aCpu));
                newRow.Column(1, std::move(aRows));
                newRow.Column(2, std::move(eCost));
                newRow.Column(3, std::move(eRows));
                newRow.Column(4, std::move(eSize));
            }
            else {
                newRow.Column(0, std::move(eCost));
                newRow.Column(1, std::move(eRows));
                newRow.Column(2, std::move(eSize));
            }
            newRow.WriteToLastColumn(std::move(operation));
        }
    } else {
        TStringBuilder operation;
        operation << arrowOffset << colors.LightCyan() << node.at("Node Type").GetString() << colors.Default();
        newRow.WriteToLastColumn(std::move(operation));
    }

    if (node.contains("Plans")) {
        auto& plans = node.at("Plans").GetArraySafe();
        for (size_t i = 0; i < plans.size(); ++i) {
            bool isLastChild = (i == plans.size() - 1);

            TVector<bool> newHasMore = hasMore;
            if (!offset.empty()) {
                newHasMore.push_back(!isLast);
            }

            offset += "  ";
            PrintPrettyTableImpl(plans[i], offset, table, isLastChild, newHasMore);
            offset.resize(offset.size() - 2);
        }
    }
}

TString TQueryPlanPrinter::JsonToString(const NJson::TJsonValue& jsonValue) {
    return jsonValue.GetStringRobust();
}

TResultSetPrinter::TResultSetPrinter(const TSettings& settings)
    : Settings(settings)
    , ParquetPrinter(std::make_unique<TResultSetParquetPrinter>(""))
{}

TResultSetPrinter::TResultSetPrinter(EDataFormat format, std::function<bool()> isInterrupted)
    : TResultSetPrinter(TSettings()
        .SetFormat(format)
        .SetIsInterrupted(isInterrupted)
    )
{}

TResultSetPrinter::~TResultSetPrinter() {
    if (PrintedSomething && !Settings.GetIsInterrupted()()) {
        EndResultSet();
    }
}

void TResultSetPrinter::Print(const TResultSet& resultSet) {
    if (FirstPart) {
        BeginResultSet();
    }
    PrintedSomething = true;

    switch (Settings.GetFormat()) {
    case EDataFormat::Default:
    case EDataFormat::Pretty:
        PrintPretty(resultSet);
        break;
    case EDataFormat::JsonUnicodeArray:
        PrintJsonArray(resultSet, EBinaryStringEncoding::Unicode);
        break;
    case EDataFormat::JsonUnicode:
        FormatResultSetJson(resultSet, Settings.GetOutput(), EBinaryStringEncoding::Unicode);
        break;
    case EDataFormat::JsonBase64Array:
        PrintJsonArray(resultSet, EBinaryStringEncoding::Base64);
        break;
    case EDataFormat::JsonBase64:
        FormatResultSetJson(resultSet, Settings.GetOutput(), EBinaryStringEncoding::Base64);
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
        throw TMisuseException() << "This command doesn't support " << Settings.GetFormat() << " output format";
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
    switch (Settings.GetFormat()) {
    case EDataFormat::JsonUnicodeArray:
    case EDataFormat::JsonBase64Array:
        *Settings.GetOutput() << '[';
        break;
    default:
        break;
    }
}

void TResultSetPrinter::EndResultSet() {
    switch (Settings.GetFormat()) {
    case EDataFormat::JsonUnicodeArray:
    case EDataFormat::JsonBase64Array:
        *Settings.GetOutput() << ']' << Endl;
        break;
    case EDataFormat::Parquet:
        ParquetPrinter->Reset();
        break;
    default:
        break;
    }
}

void TResultSetPrinter::EndLineBeforeNextResult() {
    switch (Settings.GetFormat()) {
    case EDataFormat::JsonUnicodeArray:
    case EDataFormat::JsonBase64Array:
        *Settings.GetOutput() << ',' << Endl;
        break;
    default:
        break;
    }
}

void TResultSetPrinter::PrintPretty(const TResultSet& resultSet) {
    const std::vector<TColumn>& columns = resultSet.GetColumnsMeta();
    TResultSetParser parser(resultSet);
    TVector<TString> columnNames;
    for (const auto& column : columns) {
        columnNames.push_back(TString{column.Name});
    }

    TPrettyTableConfig tableConfig;
    tableConfig.MaxWidth(Settings.GetMaxWidth());
    if (!FirstPart) {
        tableConfig.WithoutHeader();
    }
    TPrettyTable table(columnNames, tableConfig);
    for (size_t printed = 0; parser.TryNextRow(); ++printed) {
        auto& row = table.AddRow();
        if (Settings.GetMaxRowsCount() && printed >= Settings.GetMaxRowsCount()) {
            row.FreeText(TStringBuilder() << "And " << (resultSet.RowsCount() - printed) << " more lines, total " << resultSet.RowsCount());
            break;
        }
        for (ui32 i = 0; i < columns.size(); ++i) {
            row.Column(i, FormatValueJson(parser.GetValue(i), EBinaryStringEncoding::Unicode));
        }
    }

    *Settings.GetOutput() << table;
}

void TResultSetPrinter::PrintJsonArray(const TResultSet& resultSet, EBinaryStringEncoding encoding) {
    auto columns = resultSet.GetColumnsMeta();

    TResultSetParser parser(resultSet);
    bool firstRow = true;
    for (size_t printed = 0; parser.TryNextRow(); ++printed) {
        if (!firstRow || !FirstPart) {
            EndLineBeforeNextResult();
        }
        if (firstRow) {
            firstRow = false;
        }
        if (Settings.GetMaxRowsCount() && printed >= Settings.GetMaxRowsCount()) {
            *Settings.GetOutput() << "And " << (resultSet.RowsCount() - printed) << " more lines, total " << resultSet.RowsCount();
            break;
        }
        NJsonWriter::TBuf writer(NJsonWriter::HEM_UNSAFE, Settings.GetOutput());
        FormatResultRowJson(parser, columns, writer, encoding);
    }
}

void TResultSetPrinter::PrintCsv(const TResultSet& resultSet, const char* delim) {
    const std::vector<TColumn>& columns = resultSet.GetColumnsMeta();
    TResultSetParser parser(resultSet);
    if (Settings.IsCsvWithHeader()) {
        for (ui32 i = 0; i < columns.size(); ++i) {
            *Settings.GetOutput() << columns[i].Name;
            if (i < columns.size() - 1) {
                *Settings.GetOutput() << delim;
            }
        }
        *Settings.GetOutput() << Endl;
    }
    for (size_t printed = 0; parser.TryNextRow(); ++printed) {
        if (Settings.GetMaxRowsCount() && printed >= Settings.GetMaxRowsCount()) {
            *Settings.GetOutput() << "And " << (resultSet.RowsCount() - printed) << " more lines, total " << resultSet.RowsCount() << Endl;
            break;
        }
        for (ui32 i = 0; i < columns.size(); ++i) {
            *Settings.GetOutput() << FormatValueJson(parser.GetValue(i), EBinaryStringEncoding::Unicode);
            if (i < columns.size() - 1) {
                *Settings.GetOutput() << delim;
            }
        }
        *Settings.GetOutput() << Endl;
    }
}

} // namespace NYdb::NConsoleClient
