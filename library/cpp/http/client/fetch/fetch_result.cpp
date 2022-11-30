#include "codes.h"
#include "fetch_result.h"

#include <library/cpp/charset/recyr.hh>

namespace NHttpFetcher {
    TResult::TResult(const TString& url, int code)
        : RequestUrl(url)
        , ResolvedUrl(url)
        , Code(code)
        , ConnectionReused(false)
    {
    }

    TString TResult::DecodeData(bool* decoded) const {
        if (!!Encoding && *Encoding != CODES_UTF8) {
            if (decoded) {
                *decoded = true;
            }
            return Recode(*Encoding, CODES_UTF8, Data);
        }
        if (decoded) {
            *decoded = false;
        }
        return Data;
    }

    bool TResult::Success() const {
        return Code == FETCH_SUCCESS_CODE;
    }

}
