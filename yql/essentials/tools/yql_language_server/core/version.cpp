#include "version.h"

#include <util/system/defaults.h>

#ifndef YQL_LANGUAGE_SERVER_VERSION
    #define YQL_LANGUAGE_SERVER_VERSION 0.0.1
#endif

namespace NLsp::NYql {

TStringBuf Version() {
    return Y_STRINGIZE(YQL_LANGUAGE_SERVER_VERSION);
}

} // namespace NLsp::NYql
