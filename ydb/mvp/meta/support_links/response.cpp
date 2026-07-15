#include "response.h"

#include <library/cpp/json/json_value.h>
#include <library/cpp/json/json_writer.h>

namespace NMVP::NSupportLinks {

namespace {

void AppendErrorJson(NJson::TJsonValue& errorsJson, const TSupportError& error) {
    NJson::TJsonValue& item = errorsJson.AppendValue(NJson::TJsonValue());
    item["source"] = error.Source;
    if (error.Status) {
        item["status"] = *error.Status;
    }
    if (!error.Reason.empty()) {
        item["reason"] = error.Reason;
    }
    if (!error.Message.empty()) {
        item["message"] = error.Message;
    }
}

void AppendSourceOutputJson(NJson::TJsonValue& linksJson, NJson::TJsonValue& errorsJson, const TResolveOutput& sourceOutput) {
    for (const auto& link : sourceOutput.Links) {
        NJson::TJsonValue& linkItem = linksJson.AppendValue(NJson::TJsonValue());
        if (!link.Title.empty()) {
            linkItem["title"] = link.Title;
        }
        linkItem["url"] = link.Url;
    }
    for (const auto& error : sourceOutput.Errors) {
        AppendErrorJson(errorsJson, error);
    }
}

} // namespace

TString BuildResponseBody(const TVector<TResolveOutput>* sourceOutputs, const TVector<TSupportError>& pendingErrors) {
    NJson::TJsonValue root;
    NJson::TJsonValue& linksJson = root["links"];
    linksJson.SetType(NJson::JSON_ARRAY);

    NJson::TJsonValue errorsJson;
    errorsJson.SetType(NJson::JSON_ARRAY);

    if (sourceOutputs) {
        for (const auto& sourceOutput : *sourceOutputs) {
            AppendSourceOutputJson(linksJson, errorsJson, sourceOutput);
        }
    }

    for (const auto& error : pendingErrors) {
        AppendErrorJson(errorsJson, error);
    }

    if (!errorsJson.GetArray().empty()) {
        root["errors"] = std::move(errorsJson);
    }

    return NJson::WriteJson(root, false);
}

} // namespace NMVP::NSupportLinks
