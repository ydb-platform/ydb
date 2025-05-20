#include "parameters.h"

#include <ydb/public/lib/json_value/ydb_json_value.h>
#include <ydb/public/lib/ydb_cli/commands/ydb_common.h>
#include <ydb/public/lib/ydb_cli/common/interactive.h>
#include <ydb/public/lib/ydb_cli/common/yql_parser/yql_parser.h>
#include <library/cpp/json/json_reader.h>
#include <library/cpp/threading/future/async.h>

namespace NYdb {
namespace NConsoleClient {

namespace {
    const size_t DEFAULT_BATCH_LIMIT = 1000;
    const TDuration DEFAULT_BATCH_MAX_DELAY = TDuration::Seconds(1);

    THashMap<EDataFormat, TString> InputFormatParamDescriptions = {
        { EDataFormat::Json, "Parameters from the input are parsed in json format, binary string encoding can be set with --input-binary-strings option" },
        { EDataFormat::Csv, "Parameters from the input are parsed in csv format" },
        { EDataFormat::Tsv, "Parameters from the input are parsed in tsv format" },
        { EDataFormat::Raw, "Input is read as parameter value[s] with no transformation or parsing. Parameter name should be set with \"--input-param-name\" option" },
    };
}

void TCommandWithParameters::AddParametersOption(TClientCommand::TConfig& config, const TString& clarification) {
    TStringStream descr;
    NColorizer::TColors colors = NColorizer::AutoColors(Cout);
    descr << "Query parameter[s].";
    if (clarification) {
        descr << ' ' << clarification;
    }
    descr << Endl << "Format should be 'name=value' or '$name=value' where value is parsed as Json." << Endl
        << "Several parameter options can be specified. "
        << "To change binary strings encoding use --input-binary-strings option. "
        << "Escaping depends on operating system.";
    if (config.HelpCommandVerbosiltyLevel <= 1) {
        descr << Endl << "Use -hh option to see usage examples and all other options to work with parameters.";
    }
    descr << Endl << "More information and examples in the documentation:" << Endl
        << "  https://ydb.tech/docs/en/reference/ydb-cli/parameterized-queries-cli";
    config.Opts->AddLongOption('p', "param", descr.Str())
        .RequiredArgument("STRING").AppendTo(&ParameterOptions);

    TStringStream inputFileDescr;
    inputFileDescr << "File name with input parameter names and values. Format is configured with --input-format option.";
    if (config.HelpCommandVerbosiltyLevel <= 1) {
        inputFileDescr << Endl << "Use -hh option to see all options to work with parameters.";
    }
    AddInputFileOption(config, false, inputFileDescr.Str());

    if (config.HelpCommandVerbosiltyLevel > 1) {
        AddOptionExamples(
            "param",
            TExampleSetBuilder()
                .Title("Examples (with default json-unicode format)")
                .BeginExample()
                    .Title("One parameter of type Uint64")
                    .Text(TStringBuilder() << "What cli expects:     " << colors.BoldColor() << "$id=3" << colors.OldColor() << Endl
                        << "How to pass in linux: " << colors.BoldColor() << "--param '$id=3'" << colors.OldColor())
                .EndExample()
                .BeginExample()
                    .Title("Two parameters of types Uint64 and Utf8")
                    .Text(TStringBuilder() << "What cli expects:     " << colors.BoldColor() << "$key=1 $value=\"One\"" << colors.OldColor() << Endl
                        << "How to pass in linux: " << colors.BoldColor() << "--param '$key=3' --param '$value=\"One\"'" << colors.OldColor())
                .EndExample()
                .BeginExample()
                    .Title("More complex parameter of type List<Struct<key:Uint64, value:Utf8>>")
                    .Text(TStringBuilder() << "What cli expects:     " << colors.BoldColor() << "$values=[{\"key\":1,\"value\":\"one\"},{\"key\":2,\"value\":\"two\"}]" << colors.OldColor() << Endl
                        << "How to pass in linux: " << colors.BoldColor() << "--param '$values=[{\"key\":1,\"value\":\"one\"},{\"key\":2,\"value\":\"two\"}]'" << colors.OldColor())
                .EndExample()
            .Build()
        );
    }
}

void TCommandWithParameters::AddLegacyParametersFileOption(TClientCommand::TConfig& config) {
    config.Opts->AddLongOption("param-file", "File name with parameter names and values "
        "in json format. You may specify this option repeatedly.")
        .RequiredArgument("PATH").AppendTo(&ParameterFiles)
        .Hidden();
}

void TCommandWithParameters::AddDefaultParamFormats(TClientCommand::TConfig& config) {
    AddInputFormats(config, {
        EDataFormat::Json,
        EDataFormat::Csv,
        EDataFormat::Tsv,
        EDataFormat::Raw,
    });
    AddInputFramingFormats(config, {
        EFramingFormat::NoFraming,
        EFramingFormat::NewlineDelimited,
    });
    AddInputBinaryStringEncodingFormats(config, {
        EBinaryStringEncodingFormat::Unicode,
        EBinaryStringEncodingFormat::Base64,
    });
}

void TCommandWithParameters::AddLegacyStdinFormats(TClientCommand::TConfig& config) {
    AddLegacyInputFormats(config, "stdin-format", {"input-format", "input-framing"},
        {
            EDataFormat::JsonUnicode,
            EDataFormat::JsonBase64,
        }
    );
}

void TCommandWithParameters::AddBatchParametersOptions(TClientCommand::TConfig& config, const TString& requestString) {
    TStringStream descr;
    NColorizer::TColors colors = NColorizer::AutoColors(Cout);
    descr << "Batching mode for input parameters processing. Available options:\n  "
        << colors.BoldColor() << "iterative" << colors.OldColor()
        << "\n    Executes " << requestString << " for each parameter set (exactly one execution "
        "when no framing specified in \"stdin-format\")\n  "
        << colors.BoldColor() << "full" << colors.OldColor()
        << "\n    Executes " << requestString << " once, with all parameter sets wrapped in json list, when EOF is reached on stdin\n  "
        << colors.BoldColor() << "adaptive" << colors.OldColor()
        << "\n    Executes " << requestString << " with a json list of parameter sets every time when its number reaches batch-limit, "
        "or the waiting time reaches batch-max-delay."
        "\nDefault: " << colors.CyanColor() << "\"iterative\"" << colors.OldColor() << ".";
    auto& inputParamName = config.Opts->AddLongOption("input-param-name",
            "Parameter name on the input stream, required/applicable when input format implies values only (i.e. raw).")
        .RequiredArgument("STRING").AppendTo(&InputParamNames);
    auto& inputColumns = config.Opts->AddLongOption("input-columns", "String with column names that replaces CSV/TSV header. "
            "Relevant when passing parameters in CSV/TSV format only. "
            "It is assumed that there is no header in the file")
            .RequiredArgument("STR").StoreResult(&Columns);
    auto& inputSkipRows = config.Opts->AddLongOption("input-skip-rows",
            "Number of CSV/TSV header rows to skip in the input data (not including the row of column names, if any). "
            "Relevant when passing parameters in CSV/TSV format only.")
            .RequiredArgument("NUM").StoreResult(&SkipRows).DefaultValue(0);
    auto& inputBatch = config.Opts->AddLongOption("input-batch", descr.Str()).RequiredArgument("STRING").StoreResult(&BatchMode);
    auto& inputBatchMaxRows = config.Opts->AddLongOption("input-batch-max-rows", "Maximum size of list for input adaptive batching mode")
        .RequiredArgument("INT").StoreResult(&BatchLimit).DefaultValue(DEFAULT_BATCH_LIMIT);
    auto& inputBatchMaxDelay = config.Opts->AddLongOption("input-batch-max-delay", "Maximum delay to process first item in the list for adaptive batching mode")
            .RequiredArgument("VAL").StoreResult(&BatchMaxDelay).DefaultValue(DEFAULT_BATCH_MAX_DELAY);
    if (config.HelpCommandVerbosiltyLevel <= 1) {
        inputParamName.Hidden();
        inputColumns.Hidden();
        inputSkipRows.Hidden();
        inputBatch.Hidden();
        inputBatchMaxRows.Hidden();
        inputBatchMaxDelay.Hidden();
    }
}

void TCommandWithParameters::AddLegacyBatchParametersOptions(TClientCommand::TConfig& config) {
    // For backward compatibility:
    config.Opts->AddLongOption("columns").RequiredArgument("STR").StoreResult(&Columns)
        .Hidden();
    config.Opts->MutuallyExclusive("input-columns", "columns");
    config.Opts->AddLongOption("skip-rows").RequiredArgument("NUM")
        .StoreResult(&DeprecatedSkipRows)
        .Hidden();
    config.Opts->MutuallyExclusive("input-skip-rows", "skip-rows");
    config.Opts->AddLongOption("stdin-par",
            "Parameter name on stdin, required/applicable when stdin-format implies values only.")
        .RequiredArgument("STRING").AppendTo(&InputParamNames)
        .Hidden();
    config.Opts->MutuallyExclusive("input-param-name", "stdin-par");
    config.Opts->AddLongOption("batch").RequiredArgument("STRING")
        .StoreResult(&BatchMode)
        .Hidden();
    config.Opts->MutuallyExclusive("input-batch", "batch");
    config.Opts->AddLongOption("batch-limit")
        .RequiredArgument("INT").StoreResult(&DeprecatedBatchLimit)
        .Hidden();
    config.Opts->MutuallyExclusive("input-batch-max-rows", "batch-limit");
    config.Opts->AddLongOption("batch-max-delay").RequiredArgument("VAL")
        .StoreResult(&DeprecatedBatchMaxDelay)
        .Hidden();
    config.Opts->MutuallyExclusive("input-batch-max-delay", "batch-max-delay");
}

THashMap<EDataFormat, TString>& TCommandWithParameters::GetInputFormatDescriptions() {
    return InputFormatParamDescriptions;
}

void TCommandWithParameters::AddParams(TParamsBuilder& paramBuilder) {
    for (const auto&[name, value] : Parameters) {
        auto paramIt = ParamTypes.find(name);
        if (paramIt == ParamTypes.end()) {
            if (ParameterSources[name] == "\'--param\' option") {
                throw TMisuseException() << "Query does not contain parameter \"" << name << "\".";
            } else {
                continue;
            }
        }
        const TType& type = (*paramIt).second;
        paramBuilder.AddParam(name, JsonToYdbValue(value, type, InputBinaryStringEncoding));
    }
}

namespace {
    bool StdinHasData(bool verbose) {
#if defined(_win32_)
        // Too complex case for Windows
        return false;
#else
        // fd_set to store a set of descriptor set.
        fd_set read_fds;
        FD_ZERO(&read_fds);
        FD_SET(STDIN_FILENO, &read_fds);

        struct timeval timeout;
        timeout.tv_sec = 0;
        timeout.tv_usec = 0; // No timeout, instant check

        // Check if stdin is available for reading
        int selectResult = select(STDIN_FILENO + 1, &read_fds, NULL, NULL, &timeout);
        if (selectResult == 0) {
            if (verbose) {
                Cerr << "stdin is not available" << Endl;
            }
            return false;
        }

        // Trying to read 1 symbol from stdin
        char buffer[1];
        ssize_t result = read(fileno(stdin), buffer, sizeof(buffer));
        if (result == -1) {
            if (verbose) {
                Cerr << "Error reading from stdin. Error: " << strerror(errno) << Endl;
            }
        } else if (result == 0) {
            if (verbose) {
                Cerr << "No data from stdin" << Endl;
            }
        } else {
            if (verbose) {
                Cerr << "stdin has data, returning first symbol '" << buffer[0] << "' back..." << Endl;
            }
            ungetc(buffer[0], stdin);
            return true;
        }
        return false;
#endif
    }
}

void TCommandWithParameters::ParseParameters(TClientCommand::TConfig& config) {
    // Deprecated options with defaults:
    if (!DeprecatedSkipRows.empty()) {
        SkipRows = FromString<size_t>(DeprecatedSkipRows);
    }
    if (!DeprecatedBatchLimit.empty()) {
        BatchLimit = FromString<size_t>(DeprecatedBatchLimit);
    }
    if (!DeprecatedBatchMaxDelay.empty()) {
        BatchMaxDelay = FromString<TDuration>(DeprecatedBatchMaxDelay);
    }

    switch (InputFormat) {
        case EDataFormat::Csv:
            Delimiter = ',';
            break;
        case EDataFormat::Tsv:
            Delimiter = '\t';
            break;
        default:
            break;
    }

    for (const auto& parameterOption : ParameterOptions) {
        auto equalPos = parameterOption.find("=");
        if (equalPos == TString::npos) {
            throw TMisuseException() << "Wrong parameter format for \"" << parameterOption
                << "\". Parameter option should have equal sign. Examples (for linux):\n"
                << "--param 'input=1'";
        }

        auto paramName = parameterOption.substr(0, equalPos);
        if (!paramName.StartsWith('$')) {
            paramName = "$" + paramName;
        }
        if (Parameters.find(paramName) != Parameters.end()) {
            throw TMisuseException() << "Parameter " << paramName << " value found in more than one \'--param\' option.";
        }
        Parameters[paramName] = parameterOption.substr(equalPos + 1);
        ParameterSources[paramName] = "\'--param\' option";
    }
    
    if (!ParameterFiles.empty() && !InputFiles.empty()) {
        throw TMisuseException() << "Can't use both \"--input-file\" and \"--param-file\" options";
    }

    for (auto& file : ParameterFiles) {
        TString data;
        data = ReadFromFile(file, "param-file");
        std::map<TString, TString> params;
        ParseJson(std::move(data), params);
        for (const auto& [name, value]: params) {
            if (Parameters.find(name) != Parameters.end()) {
                throw TMisuseException() << "Parameter " << name << " value found in more than one source: "
                    << "param file " << file << ", " << ParameterSources[name] << ".";
            }
            Parameters[name] = value;
            ParameterSources[name] = "param file " + file;
        }
    }

    bool verbose = config.IsVerbose();

    if (InputFiles.empty()) {
        if (!IsStdinInteractive() && !ReadingSomethingFromStdin && StdinHasData(verbose)) {
            // By default reading params from stdin
            SetParamsInputFromStdin(verbose);
        }
    } else {
        auto& file = InputFiles[0];
        if (file == "-") {
            if (IsStdinInteractive()) {
                throw TMisuseException() << "Path to input file is \"-\", meaning that parameter value[s] should be read "
                    "from stdin. This is only available in non-interactive mode";
            }
            SetParamsInputFromStdin(verbose);
        } else {
            if (IsStdinInteractive() || ReadingSomethingFromStdin) {
                SetParamsInputFromFile(file, verbose);
            } else {
                throw TMisuseException() << "Path to input file is \"" << file << "\", meaning that parameter value[s]"
                    " should be read from file. This is only available in interactive mode. Can't read parameters both"
                    " from stdin and file. Choose only one of theese options";
            }
        }
    }

    if (InputFormat != EDataFormat::Csv && InputFormat != EDataFormat::Tsv && (!Columns.empty()
            || SkipRows != 0 || !DeprecatedSkipRows.empty())) {
        throw TMisuseException() << "Options \"--input-columns\" and  \"--input-skip-rows\" requires \"csv\" or \"tsv\" formats";
    }
    if (InputParamNames.empty() && InputFormat == EDataFormat::Raw) {
        throw TMisuseException() << "For \"raw\" format \"--input-param-name\" option should be used.";
    }
    if (!InputParamNames.empty() && !InputParamStream) {
        throw TMisuseException() << "\"--input-param-name\" option is allowed only with input from stdin or input file.";
    }
    if (BatchMode == EBatchMode::Full || BatchMode == EBatchMode::Adaptive) {
        if (InputParamNames.size() > 1) {
            throw TMisuseException() << "Only one input parameter with --input-param-name is allowed in \""
                << BatchMode << "\" batch mode.";
        }
        if (InputParamNames.empty()) {
            throw TMisuseException() << "Input parameter name must be specified in \""
                << BatchMode << "\" batch mode with --input-param-name option.";
        }
    }

    for (auto it = InputParamNames.begin(); it != InputParamNames.end(); ++it) {
        if (std::find(InputParamNames.begin(), it, *it) != it) {
            throw TMisuseException() << "Parameter $" << *it << " value found in more than one --input-param-name option.";
        }
        if (Parameters.find("$" + *it) != Parameters.end()) {
            throw TMisuseException() << "Parameter $" << *it << " value found in more than one source: \'--input-param-name\' option, "
                << ParameterSources["$" + *it] << ".";
        }
    }
    if (BatchMode != EBatchMode::Adaptive && (config.ParseResult->Has("input-batch-max-rows") || config.ParseResult->Has("input-batch-max-delay"))) {
        throw TMisuseException() << "Options \"--input-batch-max-rows\" and \"--input-batch-max-delay\" are allowed only in \"adaptive\" batch mode.";
    }
}

void TCommandWithParameters::SetParamsInput(IInputStream* input) {
    if (InputFormat == EDataFormat::Csv || InputFormat == EDataFormat::Tsv) {
        InputParamStream = MakeHolder<TCsvParamStream>(input);
    } else {
        InputParamStream = MakeHolder<TSimpleParamStream>(input);
    }
}

void TCommandWithParameters::SetParamsInputFromStdin(bool verbose) {
    if (ReadingSomethingFromStdin) {
        throw TMisuseException() << "Can't read both parameters and query text from stdinput";
    }
    ReadingSomethingFromStdin = true;
    SetParamsInput(&Cin);
    if (verbose) {
        Cerr << "Reading parameters from stdin" << Endl;
    }
}

void TCommandWithParameters::SetParamsInputFromFile(TString& file, bool verbose) {
    TFsPath fsPath = GetExistingFsPath(file, "input file");
    InputFileHolder = MakeHolder<TFileInput>(fsPath);
    SetParamsInput(InputFileHolder.Get());
    if (verbose) {
        Cerr << "Reading parameters from file \"" << file << '\"' << Endl;
    }
}

void TCommandWithParameters::InitParamTypes(const TDriver& driver, const TString& queryText, bool verbose) {
    if (SyntaxType == NQuery::ESyntax::Pg) {
        ParamTypes.clear();
        return;
    }

    auto types = TYqlParamParser::GetParamTypes(queryText);
    if (types.has_value()) {
        ParamTypes = *types;
        if (verbose) {
            Cerr << "Successfully retrieved parameter types from query text locally" << Endl;
        }
        return;
    }

    if (verbose) {
        Cerr << "Failed to retrieve parameter types from query text locally. Executing ExplainYqlScript..." << Endl;
    }
    // Fallback to ExplainYql
    NScripting::TScriptingClient client(driver);
    auto explainSettings = NScripting::TExplainYqlRequestSettings()
        .Mode(NScripting::ExplainYqlRequestMode::Validate);

    auto result = client.ExplainYqlScript(
        queryText,
        explainSettings
    ).GetValueSync();

    NStatusHelpers::ThrowOnErrorOrPrintIssues(result);
    ParamTypes = result.GetParameterTypes();
}

bool TCommandWithParameters::GetNextParams(const TDriver& driver, const TString& queryText,
        THolder<TParamsBuilder>& paramBuilder, bool verbose) {
    paramBuilder = MakeHolder<TParamsBuilder>();
    if (IsFirstEncounter) {
        IsFirstEncounter = false;
        InitParamTypes(driver, queryText, verbose);

        if (!InputParamStream) {
            AddParams(*paramBuilder);
            return true;
        }
        if (InputFormat == EDataFormat::Csv || InputFormat == EDataFormat::Tsv) {
            TString headerRow, temp;
            if (Columns) {
                headerRow = Columns;
            } else {
                if (!InputParamStream->ReadLine(headerRow)) {
                    return false;
                }
            }

            while (SkipRows > 0) {
                if (!InputParamStream->ReadLine(temp)) {
                    return false;
                }
                --SkipRows;
            }
            CsvParser = TCsvParser(std::move(headerRow), Delimiter, "", &ParamTypes, &ParameterSources);
        }
    }
    if (!InputParamStream) {
        return false;
    }

    AddParams(*paramBuilder);

    switch(BatchMode) {
        case EBatchMode::Default:
        case EBatchMode::Iterative:
            if (InputParamNames.empty()) {
                auto data = ReadData();
                if (!data.Defined()) {
                    return false;
                }
                if (data->empty()) {
                    return true;
                }
                switch (InputFormat) {
                    case EDataFormat::Default:
                    case EDataFormat::Json:
                    case EDataFormat::JsonUnicode:
                    case EDataFormat::JsonBase64: {
                        std::map<TString, TString> result;
                        ParseJson(std::move(*data), result);
                        ApplyJsonParams(result, *paramBuilder);
                        break;
                    }
                    case EDataFormat::Csv:
                    case EDataFormat::Tsv: {
                        CsvParser.BuildParams(*data, *paramBuilder, TCsvParser::TParseMetadata{});
                        break;
                    }
                    default:
                        Y_ABORT_UNLESS(false, "Unexpected input format");
                }
            } else {
                for (const auto &name: InputParamNames) {
                    auto data = ReadData();
                    if (!data.Defined()) {
                        return false;
                    }
                    TString fullname = "$" + name;
                    auto paramIt = ParamTypes.find(fullname);
                    if (paramIt == ParamTypes.end()) {
                        throw TMisuseException() << "Query does not contain parameter \"" << fullname << "\".";
                    }

                    const TType &type = (*paramIt).second;
                    switch (InputFormat) {
                        case EDataFormat::Default:
                        case EDataFormat::Json:
                        case EDataFormat::JsonUnicode:
                        case EDataFormat::JsonBase64: {
                            paramBuilder->AddParam(fullname, JsonToYdbValue(*data, type, InputBinaryStringEncoding));
                            break;
                        }
                        case EDataFormat::Raw: {
                            TTypeParser parser(type);
                            if (parser.GetKind() != TTypeParser::ETypeKind::Primitive) {
                                throw TMisuseException() << "Wrong type of parameter \"" << fullname << "\".";
                            }
                            if (parser.GetPrimitive() == EPrimitiveType::String) {
                                paramBuilder->AddParam(fullname, TValueBuilder().String(*data).Build());
                            } else if (parser.GetPrimitive() == EPrimitiveType::Utf8) {
                                paramBuilder->AddParam(fullname, TValueBuilder().Utf8(*data).Build());
                            } else {
                                throw TMisuseException() << "Wrong type of parameter \"" << fullname << "\".";
                            }
                            break;
                        }
                        case EDataFormat::Csv:
                        case EDataFormat::Tsv: {
                            TValueBuilder valueBuilder;
                            CsvParser.BuildValue(*data, valueBuilder, type, TCsvParser::TParseMetadata{});
                            paramBuilder->AddParam(fullname, valueBuilder.Build());
                            break;
                        }
                        default:
                            Y_ABORT_UNLESS(false, "Unexpected input format");
                    }
                }
            }
            break;
        case EBatchMode::Full:
        case EBatchMode::Adaptive:
        {
            static bool isEndReached = false;
            static auto pool = CreateThreadPool(2);
            if (isEndReached) {
                return false;
            }
            Y_ABORT_UNLESS(InputParamNames.size() == 1, "Wrong number of input parameter names");
            TString name = InputParamNames.front();
            TString fullname = "$" + name;
            auto paramIt = ParamTypes.find(fullname);
            if (paramIt == ParamTypes.end()) {
                throw TMisuseException() << "Query does not contain parameter \"" << fullname << "\".";
            }
            const TType &type = (*paramIt).second;
            TTypeParser parser(type);

            if (parser.GetKind() != TTypeParser::ETypeKind::List) {
                throw TMisuseException() << "Wrong type of parameter \"" << fullname << "\".";
            }

            TMaybe<TString> data;
            size_t listSize = 0;
            TInstant endTime;
            auto readData = [this] {
                return ReadData();
            };

            TValueBuilder valueBuilder;
            valueBuilder.BeginList();
            parser.OpenList();
            while (true) {
                static NThreading::TFuture<TMaybe<TString>> futureData;
                if (!futureData.Initialized() || listSize) {
                    futureData = NThreading::Async(readData, *pool);
                }
                if (BatchMode == EBatchMode::Adaptive && listSize &&
                    ((BatchMaxDelay != TDuration::Zero() && !futureData.Wait(endTime)) || listSize == BatchLimit)) {
                    break;
                }
                data = futureData.GetValueSync();
                if (!data.Defined()) {
                    isEndReached = true;
                    break;
                }
                if (!listSize) {
                    endTime = Now() + BatchMaxDelay;
                }
                switch (InputFormat) {
                    case EDataFormat::Default:
                    case EDataFormat::Json:
                    case EDataFormat::JsonUnicode:
                    case EDataFormat::JsonBase64: {
                        valueBuilder.AddListItem(JsonToYdbValue(*data, type.GetProto().list_type().item(),
                            InputBinaryStringEncoding));
                        break;
                    }
                    case EDataFormat::Raw: {
                        if (parser.GetKind() != TTypeParser::ETypeKind::Primitive) {
                            throw TMisuseException() << "Wrong type of list \"" << fullname << "\" elements.";
                        }
                        if (parser.GetPrimitive() == EPrimitiveType::String) {
                            valueBuilder.AddListItem(TValueBuilder().String(*data).Build());
                        } else if (parser.GetPrimitive() == EPrimitiveType::Utf8) {
                            valueBuilder.AddListItem(TValueBuilder().Utf8(*data).Build());
                        } else {
                            throw TMisuseException() << "Wrong type of list \"" << fullname << "\" elements.";
                        }
                        break;
                    }
                    case EDataFormat::Csv:
                    case EDataFormat::Tsv: {
                        valueBuilder.AddListItem();
                        CsvParser.BuildValue(*data, valueBuilder, type.GetProto().list_type().item(), TCsvParser::TParseMetadata{});
                        break;
                    }
                    default:
                        Y_ABORT_UNLESS(false, "Unexpected input format");
                }
                ++listSize;
            }
            if (!listSize) {
                return false;
            }
            valueBuilder.EndList();
            paramBuilder->AddParam(fullname, valueBuilder.Build());
            break;
        }
        default:
            throw TMisuseException() << "Unknown batch format.";
    }
    return true;
}

void TCommandWithParameters::ParseJson(TString&& str, std::map<TString, TString>& result) {
    NJson::TJsonValue jsonValue;
    if (!NJson::ReadJsonTree(str, &jsonValue)) {
        throw TMisuseException() << "Can't parse \"" << str << "\" as json.";
    }
    if (!jsonValue.IsMap()) {
        throw TMisuseException() << "Json value \"" << str << "\" is not a map.";
    }
    for (const auto&[name, value] : jsonValue.GetMap()) {
        TString fullname = "$" + name;
        result[fullname] = ToString(value);
    }
}

void TCommandWithParameters::ApplyJsonParams(const std::map<TString, TString> &params, TParamsBuilder &paramBuilder) {
    for (const auto &[name, value]: params) {
        auto paramIt = ParamTypes.find(name);
        if (paramIt == ParamTypes.end()) {
            continue;
        }
        auto paramSource = ParameterSources.find(name);
        if (paramSource != ParameterSources.end()) {
            throw TMisuseException() << "Parameter " << name << " value found in more than one source: stdin, " << paramSource->second << ".";
        }
        paramBuilder.AddParam(name, JsonToYdbValue(value, paramIt->second, InputBinaryStringEncoding));
    }
}

TMaybe<TString> TCommandWithParameters::ReadData() {
    TString result;
    if (InputFramingFormat == EFramingFormat::Default || InputFramingFormat == EFramingFormat::NoFraming) {
        static bool isFirstLine = true;
        if (!isFirstLine) {
            return Nothing();
        }
        result = InputParamStream->ReadAll();
        isFirstLine = false;
    } else if (InputFramingFormat == EFramingFormat::NewlineDelimited) {
        if (!InputParamStream->ReadLine(result)) {
            return Nothing();
        }
    } else {
        throw TMisuseException() << "Unknown framing format: " << InputFramingFormat;
    }
    return result;

}

}
}
