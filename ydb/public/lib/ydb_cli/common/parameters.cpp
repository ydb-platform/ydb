#include "parameters.h"

#include <ydb/public/lib/json_value/ydb_json_value.h>

namespace NYdb {
namespace NConsoleClient {

void TCommandWithParameters::ParseParameters() {
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

        Parameters[paramName] = parameterOption.substr(equalPos + 1);
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

TParams TCommandWithParameters::BuildParams(const std::map<TString, TType>& paramTypes, EOutputFormat inputFormat) {
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

    TParamsBuilder paramBuilder;
    for (const auto&[name, value] : Parameters) {
        auto paramIt = paramTypes.find(name);
        if (paramIt == paramTypes.end()) {
            throw TMisuseException() << "Query does not contain parameter \"" << name << "\".";
        }
        const TType& type = (*paramIt).second;
        paramBuilder.AddParam(name, JsonToYdbValue(value, type, encoding));
    }
    return paramBuilder.Build();
}

}
}
