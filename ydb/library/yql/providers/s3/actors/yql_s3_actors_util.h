#pragma once

#include <ydb/library/actors/core/actorid.h>
#include <ydb/library/yql/dq/actors/compute/dq_compute_actor_async_io.h>
#include <ydb/library/yql/dq/actors/protos/dq_status_codes.pb.h>

#include <yql/essentials/public/issue/yql_issue.h>

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

    explicit TS3Result(const TString& body);

    operator bool() const {
        return Parsed;
    }

    NXml::TConstNode GetRootNode() const {
        return Parsed ? Xml->Root() : NXml::TConstNode{};
    }
};

class TSourceErrorHandler {
public:
    explicit TSourceErrorHandler(ui64 inputIndex);

    static void CanonizeFatalError(TIssues& issues, NYql::NDqProto::StatusIds::StatusCode& fatalCode, const std::source_location& location);

protected:
    void OnRetriableError(const TIssues& issues);

    void OnFatalError(TIssues issues, NYql::NDqProto::StatusIds::StatusCode fatalCode, std::source_location location = std::source_location::current());

    virtual void SendError(std::unique_ptr<IDqComputeActorAsyncInput::TEvAsyncInputError> ev) = 0;

    const ui64 InputIndex = 0;
};

} // namespace NYql::NDq
