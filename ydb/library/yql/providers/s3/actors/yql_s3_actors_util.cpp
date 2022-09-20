#include "yql_s3_actors_util.h"

#include <util/string/builder.h>

#ifdef THROW
#undef THROW
#endif
#include <library/cpp/xml/document/xml-document.h>

namespace NYql::NDq {

bool ParseS3ErrorResponse(const TString& response, TString& errorCode, TString& message) {
    try {
        const NXml::TDocument xml(response, NXml::TDocument::String);
        if (const auto& root = xml.Root(); root.Name() == "Error") {
            auto tmpMessage = root.Node("Message", true).Value<TString>();
            errorCode = root.Node("Code", true).Value<TString>();
            message = tmpMessage;
            return true;
        }
    }
    catch (std::exception&) {
        // just suppress any possible errors
    }
    return false;
}

TIssues BuildIssues(long httpCode, const TString& s3ErrorCode, const TString& message) {

    TIssues issues;

    if (httpCode) {
        issues.AddIssue(TStringBuilder() << "HTTP Code: " << httpCode);
    }
    if (s3ErrorCode) {
        issues.AddIssue(TStringBuilder() << "Object Storage Code: " << s3ErrorCode << ", " << message);
    } else {
        issues.AddIssue(message);
    }

    return issues;
}

}
