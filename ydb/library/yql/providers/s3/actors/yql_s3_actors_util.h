#pragma once

#include <ydb/library/yql/public/issue/yql_issue.h>
#include <ydb/library/yql/dq/actors/protos/dq_status_codes.pb.h>

#include <library/cpp/xml/document/xml-document-decl.h>

#include <util/generic/string.h>

#include <optional>

namespace NYql::NDq {

bool ParseS3ErrorResponse(const TString& response, TString& errorCode, TString& message);
TIssues BuildIssues(long httpCode, const TString& s3ErrorCode, const TString& message);
NDqProto::StatusIds::StatusCode StatusFromS3ErrorCode(const TString& s3ErrorCode);

struct TS3Result {
    const TString Body;
    bool Parsed = false;
    std::optional<NXml::TDocument> Xml;

    bool IsError = false;
    TString S3ErrorCode;
    TString ErrorMessage;

    TS3Result(const TString& body);

    operator bool() const {
        return Parsed;
    }

    NXml::TConstNode GetRootNode() const {
        return Parsed ? Xml->Root() : NXml::TConstNode{};
    }
};

}
