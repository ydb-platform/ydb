#pragma once

#include <util/generic/strbuf.h>
#include <util/generic/string.h>

class TCgiParameters;

namespace NKikimr {

// Returns Content-Type value without parameters, e.g. "application/json" from "application/json; charset=utf-8".
TStringBuf TrimHttpContentTypeHeader(const TStringBuf contentTypeHeader);

// Extracts the `database` HTTP parameter. Priority:
// 1. Query parameter `database`
// 2. JSON body field `database` for POST requests
// Note: `database` param is never sent via application/x-www-form-urlencoded bodies.
TString ExtractHttpDatabaseParam(
    const TCgiParameters& queryParams,
    const TStringBuf method,
    const TStringBuf body,
    const TStringBuf contentType);

// Same as ExtractHttpDatabaseParam, but parses query parameters from URL.
TString ExtractHttpDatabaseParamFromUrl(
    const TStringBuf url,
    const TStringBuf method,
    const TStringBuf body,
    const TStringBuf contentTypeHeader);

} // namespace NKikimr
