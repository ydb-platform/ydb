#include "auth_helpers.h"

#include <ydb/library/http_proxy/error/error.h>

#include <util/string/ascii.h>
#include <util/string/strip.h>

#include <utility>

namespace NKikimr::NSQS {

static void SkipSpaces(TStringBuf& value) {
    while (value && isspace(value[0])) {
        value.Skip(1);
    }
}

static size_t FindSpace(const TStringBuf& value) {
    size_t pos = 0;
    while (pos < value.size()) {
        if (isspace(value[pos])) {
            return pos;
        } else {
            ++pos;
        }
    }
    return TStringBuf::npos;
}

TMap<TString, TString> ParseAuthorizationParams(TStringBuf value) {
    TMap<TString, TString> paramsMap;
    SkipSpaces(value);

    // parse type
    const size_t spaceDelim = FindSpace(value);
    if (spaceDelim == TStringBuf::npos) {
        throw TSQSException(NErrors::INVALID_PARAMETER_VALUE) << "Invalid authorization parameters structure.";
    }
    TStringBuf type, params;
    value.SplitOn(spaceDelim, type, params); // delimiter is excluded
    SkipSpaces(params);

    while (params) {
        TStringBuf param = StripString(params.NextTok(','));
        if (param) {
            TStringBuf k, v;
            if (!param.TrySplit('=', k, v) || !k) {
                throw TSQSException(NErrors::INVALID_PARAMETER_VALUE) << "Invalid authorization parameters structure.";
            }
            paramsMap.insert(std::make_pair(to_lower(TString(k)), TString(v)));
        }
    }
    return paramsMap;
}

} // namespace NKikimr::NSQS
