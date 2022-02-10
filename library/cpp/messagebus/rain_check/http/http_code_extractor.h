#pragma once

#include <library/cpp/http/misc/httpcodes.h>

#include <util/generic/maybe.h>
#include <util/generic/strbuf.h>

namespace NRainCheck {
    // Try to get HttpCode from library/cpp/neh response.
    // If response has HttpCode and it is not 200 OK, library/cpp/neh will send a message
    // "library/cpp/neh/http.cpp:<LINE>: request failed(<FIRST-HTTP-RESPONSE-LINE>)"
    // (see library/cpp/neh/http.cpp:625). So, we will try to parse this message and
    // find out HttpCode in it. It is bad temporary solution, but we have no choice.
    TMaybe<HttpCodes> TryGetHttpCodeFromErrorDescription(const TStringBuf& errorMessage);

}
