#pragma once

#include "examples.h"
#include "command.h"
#include "formats.h"

#include <ydb/public/lib/ydb_cli/commands/ydb_command.h>
#include <ydb/public/lib/ydb_cli/common/csv_parser.h>
#include <ydb/public/lib/ydb_cli/common/format.h>
#include <ydb/public/lib/ydb_cli/common/parameter_stream.h>
#include <ydb/public/sdk/cpp/client/ydb_params/params.h>
#include <ydb/public/lib/json_value/ydb_json_value.h>

namespace NYdb {
namespace NConsoleClient {

enum class EBatchMode {
    Iterative /* "iterative" */,
    Full /* "full" */,
    Adaptive /* "adaptive" */
};

class TCommandWithParameters : public TCommandWithExamples, public TCommandWithFormat {
protected:
    void ParseParameters(TClientCommand::TConfig& config);
    void AddParametersOption(TClientCommand::TConfig& config, const TString& clarification = "");
    void AddParametersStdinOption(TClientCommand::TConfig& config, const TString& requestString);
    bool GetNextParams(THolder<TParamsBuilder>& paramBuilder);

private:
    void AddParams(TParamsBuilder& paramBuilder);
    static void ParseJson(TString&& str, std::map<TString, TString>& result);
    void ApplyJsonParams(const std::map<TString, TString>& params, TParamsBuilder& paramBuilder);

    TMaybe<TString> ReadData();

    EBinaryStringEncoding InputEncoding, StdinEncoding;
    std::map<TString, TType> ParamTypes;
    TVector<TString> Header;
    TString Columns;
    THolder<IParamStream> Input;
    bool IsFirstEncounter = true;
    size_t SkipRows = 0;
    ui64 Row = 0;
    char Delimiter;
    TCsvParser CsvParser;

protected:
    TVector<TString> ParameterOptions, ParameterFiles, StdinParameters;
    std::map<TString, TString> Parameters;
    std::map<TString, TString> ParameterSources;
    EBatchMode BatchMode = EBatchMode::Iterative;
    size_t BatchLimit;
    TDuration BatchMaxDelay;
    THolder<NScripting::TExplainYqlResult> ValidateResult;
};

}
}
