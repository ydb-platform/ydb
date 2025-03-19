#pragma once

#include "examples.h"
#include "command.h"
#include "formats.h"

#include <ydb/public/lib/ydb_cli/commands/ydb_command.h>
#include <ydb/public/lib/ydb_cli/common/csv_parser.h>
#include <ydb/public/lib/ydb_cli/common/format.h>
#include <ydb/public/lib/ydb_cli/common/parameter_stream.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/params/params.h>
#include <ydb/public/lib/json_value/ydb_json_value.h>

namespace NYdb {
namespace NConsoleClient {

enum class EBatchMode {
    Default /* "default" */,
    Iterative /* "iterative" */,
    Full /* "full" */,
    Adaptive /* "adaptive" */
};

class TCommandWithParameters : public TCommandWithExamples, public TCommandWithInput {
protected:
    void ParseParameters(TClientCommand::TConfig& config);
    void AddParametersOption(TClientCommand::TConfig& config, const TString& clarification = "");
    // Deprecated. Use --input-file instead.
    void AddLegacyParametersFileOption(TClientCommand::TConfig& config);
    void AddBatchParametersOptions(TClientCommand::TConfig& config, const TString& requestString);
    void AddLegacyBatchParametersOptions(TClientCommand::TConfig& config);
    void AddDefaultParamFormats(TClientCommand::TConfig& config);
    void AddLegacyStdinFormats(TClientCommand::TConfig& config);
    bool GetNextParams(const TDriver& driver, const TString& queryText, THolder<TParamsBuilder>& paramBuilder);
    
    THashMap<EDataFormat, TString>& GetInputFormatDescriptions() override;

private:
    void AddParams(TParamsBuilder& paramBuilder);
    static void ParseJson(TString&& str, std::map<TString, TString>& result);
    void ApplyJsonParams(const std::map<TString, TString>& params, TParamsBuilder& paramBuilder);
    void SetParamsInput(IInputStream* input);
    void SetParamsInputFromFile(TString& file);
    void SetParamsInputFromStdin();
    void GetParamTypes(const TDriver& driver, const TString& queryText);

    TMaybe<TString> ReadData();

    std::map<std::string, TType> ParamTypes;
    TVector<TString> Header;
    TString Columns;
    THolder<TFileInput> InputFileHolder;
    bool IsFirstEncounter = true;
    size_t SkipRows;
    char Delimiter;
    TCsvParser CsvParser;
    TString DeprecatedSkipRows;
    TString DeprecatedBatchLimit;
    TString DeprecatedBatchMaxDelay;

protected:
    TVector<TString> ParameterOptions, ParameterFiles;
    TVector<TString> InputParamNames;
    std::map<TString, TString> Parameters;
    std::map<TString, TString> ParameterSources;
    THolder<IParamStream> InputParamStream;
    EBatchMode BatchMode = EBatchMode::Default;
    size_t BatchLimit;
    TDuration BatchMaxDelay;
    THolder<NScripting::TExplainYqlResult> ValidateResult;
    bool ReadingSomethingFromStdin = false;
};

}
}
