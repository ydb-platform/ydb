#include "stderr_writer.h"

#include <util/stream/str.h>

using namespace NLWTrace;

bool TStderrActionExecutor::DoExecute(TOrbit&, const TParams& params) {
    TString ParamValues[LWTRACE_MAX_PARAMS];
    Probe->Event.Signature.SerializeParams(params, ParamValues);

    TStringStream ss;
    ss << Probe->Event.GetProvider() << "." << Probe->Event.Name;
    for (ui32 i = 0; i < Probe->Event.Signature.ParamCount; ++i) {
        ss << " " << Probe->Event.Signature.ParamNames[i] << "=" << ParamValues[i];
    }
    ss << "\n";
    Cerr << ss.Str();
    return true;
}
