#pragma once

#include "examples.h"
#include "command.h"
#include "formats.h"

#include <ydb/public/lib/ydb_cli/commands/ydb_command.h>
#include <ydb/public/sdk/cpp/client/ydb_params/params.h>
#include <ydb/public/lib/json_value/ydb_json_value.h>

#if defined(_win32_)
#include <io.h>
#elif defined(_unix_)
#include <unistd.h>
#endif

namespace NYdb {
namespace NConsoleClient {

enum class EBatchMode {
    Iterative /* "iterative" */,
    Full /* "full" */,
    Adaptive /* "adaptive" */
};

bool IsStdinInteractive();

class TCommandWithParameters : public TCommandWithExamples {
protected:
    void ParseParameters(TClientCommand::TConfig& config);
    void AddParametersOption(TClientCommand::TConfig& config, const TString& clarification = "");
    void AddParametersStdinOption(TClientCommand::TConfig& config, const TString& requestString);
    bool GetNextParams(const std::map<TString, TType>& paramTypes, EOutputFormat inputFormat, EOutputFormat encodingFormat,
                                EOutputFormat framingFormat, THolder<TParamsBuilder>& paramBuilder);

private:
    void AddParams(const std::map<TString, TType>& paramTypes, EOutputFormat inputFormat, TParamsBuilder& paramBuilder);
    static void ParseJson(const TString& str, TMap<TString, TString>& result, const TString& source);
    void ApplyParams(const TMap<TString, TString>& params, const std::map<TString, TType>& paramTypes,
                     EBinaryStringEncoding encoding, TParamsBuilder& paramBuilder, const TString& source);

    static TMaybe<TString> ReadData(EOutputFormat framingFormat);

protected:
    TVector<TString> ParameterOptions, ParameterFiles, StdinParameters;
    TMap<TString, TString> Parameters;
    TMap<TString, TString> ParameterSources;
    EBatchMode BatchMode = EBatchMode::Iterative;
    size_t BatchLimit;
    TDuration BatchMaxDelay;
    THolder<NScripting::TExplainYqlResult> ValidateResult;
};

}
}
