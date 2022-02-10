#include "http_code_extractor.h"

#include <library/cpp/http/io/stream.h>
#include <library/cpp/http/misc/httpcodes.h>

#include <util/generic/maybe.h>
#include <util/generic/strbuf.h>
#include <util/string/cast.h>

namespace NRainCheck {
    TMaybe<HttpCodes> TryGetHttpCodeFromErrorDescription(const TStringBuf& errorMessage) {
        // Try to get HttpCode from library/cpp/neh response.
        // If response has HttpCode and it is not 200 OK, library/cpp/neh will send a message
        // "library/cpp/neh/http.cpp:<LINE>: request failed(<FIRST-HTTP-RESPONSE-LINE>)"
        // (see library/cpp/neh/http.cpp:625). So, we will try to parse this message and
        // find out HttpCode in it. It is bad temporary solution, but we have no choice.
        const TStringBuf SUBSTR = "request failed(";
        const size_t SUBSTR_LEN = SUBSTR.size();
        const size_t FIRST_LINE_LEN = TStringBuf("HTTP/1.X NNN").size();

        TMaybe<HttpCodes> httpCode;

        const size_t substrPos = errorMessage.find(SUBSTR);
        if (substrPos != TStringBuf::npos) {
            const TStringBuf firstLineStart = errorMessage.SubStr(substrPos + SUBSTR_LEN, FIRST_LINE_LEN);
            try {
                httpCode = static_cast<HttpCodes>(ParseHttpRetCode(firstLineStart));
                if (*httpCode < HTTP_CONTINUE || *httpCode >= HTTP_CODE_MAX) {
                    httpCode = Nothing();
                }
            } catch (const TFromStringException& ex) {
                // Can't parse HttpCode: it is OK, because ErrorDescription can be random string.
            }
        }

        return httpCode;
    }

}
