#pragma once

#include <arrow/io/interfaces.h>
#include <contrib/libs/curl/include/curl/curl.h>

#include <util/generic/guid.h>
#include <util/system/types.h>
#include <ydb/library/actors/core/event_local.h>
#include <ydb/library/actors/core/events.h>
#include <ydb/library/yql/providers/common/http_gateway/yql_http_gateway.h>
#include <ydb/core/fq/libs/config/protos/issue_id.pb.h>
#include <ydb/public/api/protos/ydb_value.pb.h>
#include <ydb/public/sdk/cpp/client/ydb_types/status/status.h>

namespace NKikimr::NExternalSource::NObjectStorage {

enum EEventTypes : ui32 {
    EvBegin = NActors::TEvents::ES_PRIVATE,

    EvFileError,

    EvRequestS3Range,
    EvS3DownloadResponse,
    EvS3RangeResponse,
    EvS3RangeError,

    // Move somewhere separate?
    EvInferFileSchema,
    EvInferredFileSchema,

    EvArrowFile,

    EvEnd,
};

struct TEvFileError : public NActors::TEventLocal<TEvFileError, EvFileError> {
    TEvFileError(TString path, NYql::TIssues issues = {})
        : Path{std::move(path)}
        , Issues{std::move(issues)}
    {}

    TString Path;
    NYql::TIssues Issues;
};

inline TEvFileError* MakeError(TString path, NFq::TIssuesIds::EIssueCode code = NFq::TIssuesIds::DEFAULT_ERROR, TString message = "", NYql::TIssues&& issues = {}) {
    auto error = new TEvFileError{std::move(path), std::move(issues)};
    error->Issues.AddIssue(std::move(message));
    error->Issues.back().SetCode(code, NYql::TSeverityIds::S_ERROR);
    return error;
}

struct TEvRequestS3Range : public NActors::TEventLocal<TEvRequestS3Range, EvRequestS3Range> {
    TEvRequestS3Range(TString path, ui64 from, ui64 to, const TGUID& requestId, NActors::TActorId sender)
        : Path{std::move(path)}
        , Start{from}
        , End{to}
        , RequestId{requestId}
        , Sender{sender}
    {}

    TString Path;

    ui64 Start = 0;
    ui64 End = 0;

    TGUID RequestId;
    NActors::TActorId Sender;
};

struct TEvS3DownloadResponse : public NActors::TEventLocal<TEvS3DownloadResponse, EvS3DownloadResponse> {
    TEvS3DownloadResponse(std::shared_ptr<TEvRequestS3Range>&& request, NYql::IHTTPGateway::TResult&& result)
        : Request{std::move(request)}
        , Result{std::move(result)}
    {}

    std::shared_ptr<TEvRequestS3Range> Request;
    NYql::IHTTPGateway::TResult Result;
};

struct TEvS3RangeResponse : public NActors::TEventLocal<TEvS3RangeResponse, EvS3RangeResponse> {
    TEvS3RangeResponse(long httpCode, TString&& data, TString&& path, const TGUID& requestId)
        : Data{std::move(data)}
        , HttpCode{httpCode}
        , Path{std::move(path)}
        , RequestId{requestId}
    {}

    TString Data;
    long HttpCode;
    TString Path;
    TGUID RequestId;
};

struct TEvS3RangeError : public NActors::TEventLocal<TEvS3RangeError, EvS3RangeError> {
    TEvS3RangeError(CURLcode code, long httpCode, NYql::TIssues&& issues, TString&& path, const TGUID& requestId)
        : CurlResponseCode{code}
        , HttpCode{httpCode}
        , Issues{std::move(issues)}
        , Path{std::move(path)}
        , RequestId{requestId}
    {}

    CURLcode CurlResponseCode;
    long HttpCode;
    NYql::TIssues Issues;
    TString Path;
    TGUID RequestId;
};

struct TEvArrowFile : public NActors::TEventLocal<TEvArrowFile, EvArrowFile> {
    TEvArrowFile(std::shared_ptr<arrow::io::RandomAccessFile> file, TString path)
        : File{std::move(file)}
        , Path{std::move(path)}
    {}

    std::shared_ptr<arrow::io::RandomAccessFile> File;
    TString Path;
};

struct TEvInferFileSchema : public NActors::TEventLocal<TEvInferFileSchema, EvInferFileSchema> {
    explicit TEvInferFileSchema(TString&& path)
        : Path{std::move(path)}
    {}

    TString Path;
};

struct TEvInferredFileSchema : public NActors::TEventLocal<TEvInferredFileSchema, EvInferredFileSchema> {
    TEvInferredFileSchema(TString path, std::vector<Ydb::Column>&& fields)
        : Path{std::move(path)}
        , Status{NYdb::EStatus::SUCCESS, {}}
        , Fields{std::move(fields)}
    {}
    TEvInferredFileSchema(TString path, NYql::TIssues&& issues)
        : Path{std::move(path)}
        , Status{NYdb::EStatus::INTERNAL_ERROR, std::move(issues)}
    {}

    TString Path;
    NYdb::TStatus Status;
    std::vector<Ydb::Column> Fields;
};

inline TEvInferredFileSchema* MakeErrorSchema(TString path, NFq::TIssuesIds::EIssueCode code, TString message) {
    NYql::TIssues issues;
    issues.AddIssue(std::move(message));
    issues.back().SetCode(code, NYql::TSeverityIds::S_ERROR);
    return new TEvInferredFileSchema{std::move(path), std::move(issues)};
}

}  // namespace NKikimr::NExternalSource::NObjectStorage
