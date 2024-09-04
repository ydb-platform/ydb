#include "parameters.h"

#include <ydb/public/lib/json_value/ydb_json_value.h>
#include <ydb/public/lib/ydb_cli/common/interactive.h>
#include <library/cpp/json/json_reader.h>
#include <library/cpp/threading/future/async.h>

namespace NYdb {
namespace NConsoleClient {

void TCommandWithParameters::ParseParameters(TClientCommand::TConfig& config) {
    switch (InputFormat) {
        case EOutputFormat::Default:
        case EOutputFormat::JsonUnicode:
            InputEncoding = EBinaryStringEncoding::Unicode;
            break;
        case EOutputFormat::JsonBase64:
            InputEncoding = EBinaryStringEncoding::Base64;
            break;
        default:
            throw TMisuseException() << "Unknown input format: " << InputFormat;
    }

    switch (StdinFormat) {
        case EOutputFormat::Csv:
            Delimiter = ',';
            break;
        case EOutputFormat::Tsv:
            Delimiter = '\t';
            break;
        case EOutputFormat::Raw:
            break;
        case EOutputFormat::Default:
        case EOutputFormat::JsonUnicode:
            StdinEncoding = EBinaryStringEncoding::Unicode;
            break;
        case EOutputFormat::JsonBase64:
            StdinEncoding = EBinaryStringEncoding::Base64;
            break;
        default:
            throw TMisuseException() << "Unknown stdin format: " << StdinFormat;
    }

    for (const auto& parameterOption : ParameterOptions) {
        auto equalPos = parameterOption.find("=");
        if (equalPos == TString::npos) {
            throw TMisuseException() << "Wrong parameter format for \"" << parameterOption
                << "\". Parameter option should have equal sign. Example (for linux):\n"
                << "--param '$input=1'";
        }

        auto paramName = parameterOption.substr(0, equalPos);

        if (!paramName.StartsWith('$') || equalPos <= 1) {
            throw TMisuseException() << "Wrong parameter name \"" << paramName << "\" at option \""
                << parameterOption << "\". " << "Parameter name should start with '$' sign. Example (for linux):\n"
                << "--param '$input=1'";
        }
        if (Parameters.find(paramName) != Parameters.end()) {
            throw TMisuseException() << "Parameter $" << paramName << " value found in more than one source: \'--param\' option.";
        }
        Parameters[paramName] = parameterOption.substr(equalPos + 1);
        ParameterSources[paramName] = "\'--param\' option";
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
    if (StdinFormat != EOutputFormat::Csv && StdinFormat != EOutputFormat::Tsv && (!Columns.Empty() || config.ParseResult->Has("skip-rows"))) {
        throw TMisuseException() << "Options \"--columns\" and  \"--skip-rows\" requires \"csv\" or \"tsv\" formats";
    }
    if (StdinParameters.empty() && StdinFormat == EOutputFormat::Raw) {
        throw TMisuseException() << "For \"raw\" format \"--stdin-par\" option should be used.";
    }
    if (!StdinParameters.empty() && IsStdinInteractive()) {
        throw TMisuseException() << "\"--stdin-par\" option is allowed only with non-interactive stdin.";
    }
    if (BatchMode == EBatchMode::Full || BatchMode == EBatchMode::Adaptive) {
        if (StdinParameters.size() > 1) {
            throw TMisuseException() << "Only one stdin parameter allowed in \""
                << BatchMode << "\" batch mode.";
        }
        if (StdinParameters.empty()) {
            throw TMisuseException() << "An stdin parameter name must be specified in \""
                << BatchMode << "\" batch mode.";
        }
    }

    for (auto it = StdinParameters.begin(); it != StdinParameters.end(); ++it) {
        if (std::find(StdinParameters.begin(), it, *it) != it) {
            throw TMisuseException() << "Parameter $" << *it << " value found in more than one source: \'--stdin-par\' option.";
        }
        if (Parameters.find("$" + *it) != Parameters.end()) {
            throw TMisuseException() << "Parameter $" << *it << " value found in more than one source: \'--stdin-par\' option, "
                << ParameterSources["$" + *it] << ".";
        }
    }
    if (BatchMode != EBatchMode::Adaptive && (config.ParseResult->Has("batch-limit") || config.ParseResult->Has("batch-max-delay"))) {
        throw TMisuseException() << "Options \"--batch-limit\" and \"--batch-max-delay\" are allowed only in \"adaptive\" batch mode.";
    }
}

void TCommandWithParameters::AddParametersOption(TClientCommand::TConfig& config, const TString& clarification) {
    TStringStream descr;
    NColorizer::TColors colors = NColorizer::AutoColors(Cout);
    descr << "Query parameter[s].";
    if (clarification) {
        descr << ' ' << clarification;
    }
    descr << Endl << "Several parameter options can be specified. " << Endl
        << "To change input format use --input-format option." << Endl
        << "Escaping depends on operating system.";
    config.Opts->AddLongOption('p', "param", descr.Str())
        .RequiredArgument("$name=value").AppendTo(&ParameterOptions);
    config.Opts->AddLongOption("param-file", "File name with parameter names and values "
        "in json format. You may specify this option repeatedly.")
        .RequiredArgument("PATH").AppendTo(&ParameterFiles);

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

void TCommandWithParameters::AddParametersStdinOption(TClientCommand::TConfig& config, const TString& requestString) {
    TStringStream descr;
    NColorizer::TColors colors = NColorizer::AutoColors(Cout);
    descr << "Batching mode for stdin parameters processing. Available options:\n  "
        << colors.BoldColor() << "iterative" << colors.OldColor()
        << "\n    Executes " << requestString << " for each parameter set (exactly one execution "
        "when no framing specified in \"stdin-format\")\n  "
        << colors.BoldColor() << "full" << colors.OldColor()
        << "\n    Executes " << requestString << " once, with all parameter sets wrapped in json list, when EOF is reached on stdin\n  "
        << colors.BoldColor() << "adaptive" << colors.OldColor()
        << "\n    Executes " << requestString << " with a json list of parameter sets every time when its number reaches batch-limit, "
        "or the waiting time reaches batch-max-delay."
        "\nDefault: " << colors.CyanColor() << "\"iterative\"" << colors.OldColor() << ".";
    config.Opts->AddLongOption("columns", "String with column names that replaces header. "
            "Relevant when passing parameters in CSV/TSV format only. "
            "It is assumed that there is no header in the file")
            .RequiredArgument("STR").StoreResult(&Columns);
    config.Opts->AddLongOption("skip-rows", "Number of header rows to skip (not including the row of column names, if any). "
            "Relevant when passing parameters in CSV/TSV format only.")
            .RequiredArgument("NUM").StoreResult(&SkipRows).DefaultValue(0);
    config.Opts->AddLongOption("stdin-par", "Parameter name on stdin, required/applicable when stdin-format implies values only.")
            .RequiredArgument("STRING").AppendTo(&StdinParameters);
    config.Opts->AddLongOption("batch", descr.Str()).RequiredArgument("STRING").StoreResult(&BatchMode);
    config.Opts->AddLongOption("batch-limit", "Maximum size of list for adaptive batching mode").RequiredArgument("INT")
            .StoreResult(&BatchLimit).DefaultValue(1000);
    config.Opts->AddLongOption("batch-max-delay", "Maximum delay to process first item in the list for adaptive batching mode")
            .RequiredArgument("VAL").StoreResult(&BatchMaxDelay).DefaultValue(TDuration::Seconds(1));
}

void TCommandWithParameters::AddParams(TParamsBuilder& paramBuilder) {
     switch (InputFormat) {
        case EOutputFormat::Default:
        case EOutputFormat::JsonUnicode:
        case EOutputFormat::JsonBase64: {
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
                paramBuilder.AddParam(name, JsonToYdbValue(value, type, InputEncoding));
            }
            break;
        }
        default:
            Y_ABORT_UNLESS(false, "Unexpected input format");
    }
}

bool TCommandWithParameters::GetNextParams(THolder<TParamsBuilder>& paramBuilder) {
    paramBuilder = MakeHolder<TParamsBuilder>();
    if (IsFirstEncounter) {
        IsFirstEncounter = false;
        ParamTypes = ValidateResult->GetParameterTypes();
        if (IsStdinInteractive()) {
            AddParams(*paramBuilder);
            return true;
        }
        if (StdinFormat == EOutputFormat::Csv || StdinFormat == EOutputFormat::Tsv) {
            Input = MakeHolder<TCsvParamStream>();
            TString headerRow, temp;
            if (Columns) {
                headerRow = Columns;
            } else {
                if (!Input->ReadLine(headerRow)) {
                    return false;
                }
            }

            while (SkipRows > 0) {
                if (!Input->ReadLine(temp)) {
                    return false;
                }
                --SkipRows;
            }
            CsvParser = TCsvParser(std::move(headerRow), Delimiter, "", &ParamTypes, &ParameterSources);
        } else {
            Input = MakeHolder<TSimpleParamStream>();
        }
    }
    if (IsStdinInteractive()) {
        return false;
    }

    AddParams(*paramBuilder);
    if (BatchMode == EBatchMode::Iterative) {
        if (StdinParameters.empty()) {
            auto data = ReadData();
            if (!data.Defined()) {
                return false;
            }
            if (data->empty()) {
                return true;
            }
            switch (StdinFormat) {
                case EOutputFormat::Default:
                case EOutputFormat::JsonUnicode:
                case EOutputFormat::JsonBase64: {
                    std::map<TString, TString> result;
                    ParseJson(std::move(*data), result);
                    ApplyJsonParams(result, *paramBuilder);
                    break;
                }
                case EOutputFormat::Csv:
                case EOutputFormat::Tsv: {
                    CsvParser.GetParams(std::move(*data), *paramBuilder, TCsvParser::TParseMetadata{});
                    break;
                }
                default:
                    Y_ABORT_UNLESS(false, "Unexpected stdin format");
            }
        } else {
            for (const auto &name: StdinParameters) {
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
                switch (StdinFormat) {
                    case EOutputFormat::Default:
                    case EOutputFormat::JsonUnicode:
                    case EOutputFormat::JsonBase64: {
                        paramBuilder->AddParam(fullname, JsonToYdbValue(*data, type, StdinEncoding));
                        break;
                    }
                    case EOutputFormat::Raw: {
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
                    case EOutputFormat::Csv:
                    case EOutputFormat::Tsv: {
                        TValueBuilder valueBuilder;
                        CsvParser.GetValue(std::move(*data), valueBuilder, type, TCsvParser::TParseMetadata{});
                        paramBuilder->AddParam(fullname, valueBuilder.Build());
                        break;
                    }
                    default:
                        Y_ABORT_UNLESS(false, "Unexpected stdin format");
                }
            }
        }
    } else if (BatchMode == EBatchMode::Full || BatchMode == EBatchMode::Adaptive) {
        static bool isEndReached = false;
        static auto pool = CreateThreadPool(2);
        if (isEndReached) {
            return false;
        }
        Y_ABORT_UNLESS(StdinParameters.size() == 1, "Wrong number of stdin parameters");
        TString name = StdinParameters.front();
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
            switch (StdinFormat) {
                case EOutputFormat::Default:
                case EOutputFormat::JsonUnicode:
                case EOutputFormat::JsonBase64: {
                    valueBuilder.AddListItem(JsonToYdbValue(*data, type.GetProto().list_type().item(), StdinEncoding));
                    break;
                }
                case EOutputFormat::Raw: {
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
                case EOutputFormat::Csv:
                case EOutputFormat::Tsv: {
                    valueBuilder.AddListItem();
                    CsvParser.GetValue(std::move(*data), valueBuilder, type.GetProto().list_type().item(), TCsvParser::TParseMetadata{});
                    break;
                }
                default:
                    Y_ABORT_UNLESS(false, "Unexpected stdin format");
            }
            ++listSize;
        }
        if (!listSize) {
            return false;
        }
        valueBuilder.EndList();
        paramBuilder->AddParam(fullname, valueBuilder.Build());
    } else {
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
        paramBuilder.AddParam(name, JsonToYdbValue(value, paramIt->second, StdinEncoding));
    }
}

TMaybe<TString> TCommandWithParameters::ReadData() {
    TString result;
    if (FramingFormat == EOutputFormat::Default || FramingFormat == EOutputFormat::NoFraming) {
        static bool isFirstLine = true;
        if (!isFirstLine) {
            return Nothing();
        }
        result = Input->ReadAll();
        isFirstLine = false;
    } else if (FramingFormat == EOutputFormat::NewlineDelimited) {
        if (!Input->ReadLine(result)) {
            return Nothing();
        }
    } else {
        throw TMisuseException() << "Unknown framing format: " << FramingFormat;
    }
    return result;

}

}
}
