#include "viewer_utils.h"

#include <library/cpp/json/json_reader.h>

namespace NKikimr::NViewer {

TString GetDatabaseParam(const TCgiParameters& params, const TStringBuf& method, const TStringBuf& body) {
    TString database = params.Get("database");
    if (database) {
        return database;
    }
    if (method != "POST" || body.empty()) {
        return {};
    }
    NJson::TJsonValue requestData;
    if (NJson::ReadJsonTree(body, &requestData)) {
        return requestData["database"].GetString();
    }
    // "database" param is never sent via application/x-www-form-urlencoded bodies, so we don't need to check for it
    return {};
}

} // namespace NKikimr::NViewer
