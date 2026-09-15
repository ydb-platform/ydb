#include "params.h"
#include <library/cpp/cgiparam/cgiparam.h>

namespace NKikimr::NHttpProxy::NSQS {

TParameters ParseParameters(const TStringBuf& input) {
    TCgiParameters cgiParameters(TStringBuf(input.Data(), input.Size()));
    return ParseParameters(cgiParameters);
}

TParameters ParseParameters(const TCgiParameters& cgiParameters) {
    TParameters params;
    TParametersParser parser(&params);
    for (auto pi = cgiParameters.begin(); pi != cgiParameters.end(); ++pi) {
        parser.Append(pi->first, pi->second);
    }
    return params;
}

} // namespace NKikimr::NHttpProxy::NSQS
