#pragma once

#include "examples.h"
#include "command.h"
#include "formats.h"

#include <ydb/public/sdk/cpp/client/ydb_params/params.h>

namespace NYdb {
namespace NConsoleClient {

class TCommandWithParameters : public TCommandWithExamples {
protected:
    void ParseParameters();
    void AddParametersOption(TClientCommand::TConfig& config, const TString& clarification = "");
    TParams BuildParams(const std::map<TString, TType>& paramTypes, EOutputFormat inputFormat);

protected:
    TVector<TString> ParameterOptions;
    TMap<TString, TString> Parameters;
};

}
}
