#include "yql_s3_actors_util.h"

#include <util/string/builder.h>

#ifdef THROW
#undef THROW
#endif
#include <library/cpp/xml/document/xml-document.h>

namespace NYql::NDq {

bool ParseS3ErrorResponse(const TString& response, TString& errorCode, TString& message) {
    TS3Result s3Result(response);
    if (s3Result.Parsed && s3Result.IsError) {
        errorCode = std::move(s3Result.S3ErrorCode);
        message = std::move(s3Result.ErrorMessage);
        return true;
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

static const THashMap<TStringBuf, NDqProto::StatusIds::StatusCode> S3ErrorToStatusCode = {
    { "BucketMaxSizeExceeded"sv, NDqProto::StatusIds::LIMIT_EXCEEDED },
    { "CloudTotalAliveSizeQuotaExceed"sv, NDqProto::StatusIds::LIMIT_EXCEEDED },
    { "EntityTooSmall"sv, NDqProto::StatusIds::LIMIT_EXCEEDED },
    { "EntityTooLarge"sv, NDqProto::StatusIds::LIMIT_EXCEEDED },
    { "KeyTooLongError"sv, NDqProto::StatusIds::LIMIT_EXCEEDED },
    { "InvalidStorageClass"sv, NDqProto::StatusIds::PRECONDITION_FAILED },
    { "AccessDenied"sv, NDqProto::StatusIds::BAD_REQUEST },
    { "NoSuchBucket"sv, NDqProto::StatusIds::BAD_REQUEST }
};

NDqProto::StatusIds::StatusCode StatusFromS3ErrorCode(const TString& s3ErrorCode) {
    if (s3ErrorCode.empty()) {
        return NDqProto::StatusIds::UNSPECIFIED;
    }
    const auto it = S3ErrorToStatusCode.find(TStringBuf(s3ErrorCode));
    if (it != S3ErrorToStatusCode.end()) {
        return it->second;
    }
    return NYql::NDqProto::StatusIds::EXTERNAL_ERROR;
}

TS3Result::TS3Result(const TString& body)
    : Body(body)
{
    try {
        Xml.emplace(Body, NXml::TDocument::String);
        Parsed = true;
    } catch (const std::exception& ex) {
        ErrorMessage = ex.what();
        IsError = true;
    }
    if (Parsed) {
        if (const auto& root = Xml->Root(); root.Name() == "Error") {
            IsError = true;
            S3ErrorCode = root.Node("Code", true).Value<TString>();
            ErrorMessage = root.Node("Message", true).Value<TString>();
        }
    }
}

}
