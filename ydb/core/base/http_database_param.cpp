#include "http_database_param.h"

#include <library/cpp/cgiparam/cgiparam.h>
#include <library/cpp/json/json_reader.h>

#include <util/string/ascii.h>

namespace NKikimr {

static constexpr const char* DATABASE_PARAM = "database";

TStringBuf TrimHttpContentTypeHeader(const TStringBuf contentTypeHeader) {
    TStringBuf contentType = contentTypeHeader.Before(';');
    while (!contentType.empty() && contentType.front() == ' ') {
        contentType.Skip(1);
    }
    while (!contentType.empty() && contentType.back() == ' ') {
        contentType.Chop(1);
    }
    return contentType;
}

TString ExtractHttpDatabaseParamFromUrl(
    const TStringBuf url,
    const TStringBuf method,
    const TStringBuf body,
    const TStringBuf contentTypeHeader)
{
    TCgiParameters queryParams(url.After('?'));
    return ExtractHttpDatabaseParam(queryParams, method, body, TrimHttpContentTypeHeader(contentTypeHeader));
}

TString ExtractHttpDatabaseParam(
    const TCgiParameters& queryParams,
    const TStringBuf method,
    const TStringBuf body,
    const TStringBuf contentType)
{
    TString database = queryParams.Get(DATABASE_PARAM);
    if (database) {
        return database;
    }
    if (method != "POST" || body.empty()) {
        return {};
    }
    if (!contentType.empty() && !AsciiEqualsIgnoreCase(contentType, "application/json")) {
        return {};
    }
    NJson::TJsonValue requestData;
    if (NJson::ReadJsonTree(body, &requestData)) {
        return requestData[DATABASE_PARAM].GetString();
    }
    return {};
}

} // namespace NKikimr
