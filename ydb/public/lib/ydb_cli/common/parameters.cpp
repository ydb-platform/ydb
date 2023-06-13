#include "parameters.h"

#include <ydb/public/lib/json_value/ydb_json_value.h>
#include <ydb/public/lib/ydb_cli/common/interactive.h>
#include <library/cpp/json/json_reader.h>
#include <library/cpp/threading/future/async.h>

namespace NYdb {
namespace NConsoleClient {

void TCommandWithParameters::ParseParameters(TClientCommand::TConfig& config) {
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
        TMap<TString, TString> params;
        ParseJson(data, params, "param file " + file);
        for (const auto& [name, value]: params) {
            if (Parameters.find(name) != Parameters.end()) {
                throw TMisuseException() << "Parameter " << name << " value found in more than one source: "
                    << "param file " << file << ", " << ParameterSources[name] << ".";
            }
            Parameters[name] = value;
            ParameterSources[name] = "param file " + file;
        }
    }

    if (!StdinParameters.empty() && IsStdinInteractive()) {
        throw TMisuseException() << "\"--stdin-par\" option is allowed only with non-interactive stdin.";
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
    config.Opts->AddLongOption("stdin-par", "Parameter name on stdin, required/applicable when stdin-format implies values only.")
            .RequiredArgument("STRING").AppendTo(&StdinParameters);
    config.Opts->AddLongOption("batch", descr.Str()).RequiredArgument("STRING").StoreResult(&BatchMode);
    config.Opts->AddLongOption("batch-limit", "Maximum size of list for adaptive batching mode").RequiredArgument("INT")
            .StoreResult(&BatchLimit).DefaultValue(1000);
    config.Opts->AddLongOption("batch-max-delay", "Maximum delay to process first item in the list for adaptive batching mode")
            .RequiredArgument("VAL").StoreResult(&BatchMaxDelay).DefaultValue(TDuration::Seconds(1));
}

void TCommandWithParameters::AddParams(const std::map<TString, TType>& paramTypes, EOutputFormat inputFormat, TParamsBuilder& paramBuilder) {
    EBinaryStringEncoding encoding;
    switch (inputFormat) {
    case EOutputFormat::Default:
    case EOutputFormat::JsonUnicode:
        encoding = EBinaryStringEncoding::Unicode;
        break;
    case EOutputFormat::JsonBase64:
        encoding = EBinaryStringEncoding::Base64;
        break;
    default:
        throw TMisuseException() << "Unknown input format: " << inputFormat;
    }

    for (const auto&[name, value] : Parameters) {
        auto paramIt = paramTypes.find(name);
        if (paramIt == paramTypes.end()) {
            if (ParameterSources[name] == "\'--param\' option") {
                throw TMisuseException() << "Query does not contain parameter \"" << name << "\".";
            } else {
                continue;
            }
        }
        const TType& type = (*paramIt).second;
        paramBuilder.AddParam(name, JsonToYdbValue(value, type, encoding));
    }
}

bool TCommandWithParameters::GetNextParams(const std::map<TString, TType>& paramTypes, EOutputFormat inputFormat, EOutputFormat encodingFormat,
                                            EOutputFormat framingFormat, THolder<TParamsBuilder>& paramBuilder) {
    paramBuilder = MakeHolder<TParamsBuilder>();
    AddParams(paramTypes, inputFormat, *paramBuilder);
    if (IsStdinInteractive()) {
        static bool firstEncounter = true;
        if (!firstEncounter) {
            return false;
        }
        firstEncounter = false;
        return true;
    }
    EBinaryStringEncoding encoding;
    switch (encodingFormat) {
        case EOutputFormat::Default:
        case EOutputFormat::Raw:
        case EOutputFormat::JsonUnicode:
            encoding = EBinaryStringEncoding::Unicode;
            break;
        case EOutputFormat::JsonBase64:
            encoding = EBinaryStringEncoding::Base64;
            break;
        default:
            throw TMisuseException() << "Unknown encoding format: " << encodingFormat;
    }
    if (BatchMode == EBatchMode::Iterative) {
        TString data;
        if (StdinParameters.empty()) {
            if (encodingFormat == EOutputFormat::Raw) {
                throw TMisuseException() << "For Raw format \"--stdin-par\" option should be used.";
            }
            auto data = ReadData(framingFormat);
            if (!data.Defined()) {
                return false;
            }
            if (!data->empty()) {
                TMap<TString, TString> result;
                ParseJson(*data, result, "stdin");
                ApplyParams(result, paramTypes, encoding, *paramBuilder, "stdin");
            }
        } else {
            for (const auto &name: StdinParameters) {
                auto data = ReadData(framingFormat);
                if (!data.Defined()) {
                    return false;
                }
                TString fullname = "$" + name;
                auto paramIt = paramTypes.find(fullname);
                if (paramIt == paramTypes.end()) {
                    throw TMisuseException() << "Query does not contain parameter \"" << fullname << "\".";
                }

                const TType &type = (*paramIt).second;
                if (encodingFormat == EOutputFormat::Raw) {
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
                } else {
                    paramBuilder->AddParam(fullname, JsonToYdbValue(*data, type, encoding));
                }
            }
        }
    } else if (BatchMode == EBatchMode::Full || BatchMode == EBatchMode::Adaptive) {
        static bool isEndReached = false;
        static auto pool = CreateThreadPool(2);
        if (isEndReached) {
            return false;
        }

        if (StdinParameters.size() > 1) {
            throw TMisuseException() << "Only one stdin parameter allowed in \""
                << BatchMode << "\" batch mode.";
        }
        if (StdinParameters.empty()) {
            throw TMisuseException() << "An stdin parameter name must be specified in \""
                << BatchMode << "\" batch mode.";
        }
        TString name = StdinParameters.front();
        TString fullname = "$" + name;
        auto paramIt = paramTypes.find(fullname);
        if (paramIt == paramTypes.end()) {
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
        auto ReadDataLambda = [framingFormat]() {
                    return ReadData(framingFormat);
                };
        if (encodingFormat == EOutputFormat::Raw) {
            TValueBuilder valueBuilder;
            valueBuilder.BeginList();
            parser.OpenList();
            while (true) {
                static NThreading::TFuture<TMaybe<TString>> futureData;
                if (!futureData.Initialized() || listSize) {
                    futureData = NThreading::Async(ReadDataLambda, *pool);
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
                ++listSize;
            }
            if (!listSize) {
                return false;
            }
            valueBuilder.EndList();
            paramBuilder->AddParam(fullname, valueBuilder.Build());
        } else {
            TString array = "[";
            while (true) {
                static NThreading::TFuture<TMaybe<TString>> futureData;
                if (!futureData.Initialized() || listSize) {
                    futureData = NThreading::Async(ReadDataLambda, *pool);
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
                } else {
                    array += ",";
                }
                array += *data;
                ++listSize;
            }
            if (!listSize) {
                return false;
            }
            array += "]";
            paramBuilder->AddParam(fullname, JsonToYdbValue(array, type, encoding));
        }

    } else {
        throw TMisuseException() << "Unknown batch format.";
    }

    return true;
}

void TCommandWithParameters::ParseJson(const TString& str, TMap<TString, TString>& result, const TString& source) {
    NJson::TJsonValue jsonValue;
    if (!NJson::ReadJsonTree(str, &jsonValue)) {
        throw TMisuseException() << "Can't parse \"" << str << "\" as json.";
    }
    if (!jsonValue.IsMap()) {
        throw TMisuseException() << "Json value \"" << str << "\" is not a map.";
    }
    for (const auto&[name, value] : jsonValue.GetMap()) {
        TString fullname = "$" + name;
        if (result.find(fullname) != result.end()) {
            throw TMisuseException() << "Parameter " << fullname << " value found in more than one source: " << source << ".";
        }
        result[fullname] = ToString(value);
    }
}

void TCommandWithParameters::ApplyParams(const TMap<TString, TString> &params, const std::map<TString, TType>& paramTypes,
                                         EBinaryStringEncoding encoding, TParamsBuilder &paramBuilder, const TString& source) {
    for (const auto &[name, value]: params) {
        auto paramIt = paramTypes.find(name);
        if (paramIt == paramTypes.end()) {
            continue;
        }
        if (Parameters.find(name) != Parameters.end()) {
            throw TMisuseException() << "Parameter " << name << " value found in more than one source: "
                << source << ", " << ParameterSources[name] << ".";
        }
        const TType &type = (*paramIt).second;
        paramBuilder.AddParam(name, JsonToYdbValue(value, type, encoding));
    }
}

TMaybe<TString> TCommandWithParameters::ReadData(EOutputFormat framingFormat) {
    TString result;
    if (framingFormat == EOutputFormat::Default || framingFormat == EOutputFormat::NoFraming) {
        static bool isFirstEncounter = true;
        if (!isFirstEncounter) {
            return Nothing();
        }
        result = Cin.ReadAll();
        isFirstEncounter = false;
    } else if (framingFormat == EOutputFormat::NewlineDelimited) {
        if (!Cin.ReadLine(result)) {
            return Nothing();
        }
    } else {
        throw TMisuseException() << "Unknown framing format: " << framingFormat;
    }
    return result;

}

}
}
