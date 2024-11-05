#include "errors.h"

#include <library/cpp/yson/node/node_io.h>
#include <library/cpp/yson/node/node_visitor.h>

#include <yt/cpp/mapreduce/interface/error_codes.h>

#include <library/cpp/json/json_reader.h>
#include <library/cpp/yson/writer.h>

#include <util/string/builder.h>
#include <util/stream/str.h>
#include <util/generic/set.h>

namespace NYT {

using namespace NJson;

////////////////////////////////////////////////////////////////////////////////

static void WriteErrorDescription(const TYtError& error, IOutputStream* out)
{
    (*out) << '\'' << error.GetMessage() << '\'';
    const auto& innerErrorList = error.InnerErrors();
    if (!innerErrorList.empty()) {
        (*out) << " { ";
        bool first = true;
        for (const auto& innerError : innerErrorList) {
            if (first) {
                first = false;
            } else {
                (*out) << " ; ";
            }
            WriteErrorDescription(innerError, out);
        }
        (*out) << " }";
    }
}

static void SerializeError(const TYtError& error, NYson::IYsonConsumer* consumer)
{
    consumer->OnBeginMap();
    {
        consumer->OnKeyedItem("code");
        consumer->OnInt64Scalar(error.GetCode());

        consumer->OnKeyedItem("message");
        consumer->OnStringScalar(error.GetMessage());

        if (!error.GetAttributes().empty()) {
            consumer->OnKeyedItem("attributes");
            consumer->OnBeginMap();
            {
                for (const auto& item : error.GetAttributes()) {
                    consumer->OnKeyedItem(item.first);
                    TNodeVisitor(consumer).Visit(item.second);
                }
            }
            consumer->OnEndMap();
        }

        if (!error.InnerErrors().empty()) {
            consumer->OnKeyedItem("inner_errors");
            {
                consumer->OnBeginList();
                for (const auto& innerError : error.InnerErrors()) {
                    SerializeError(innerError, consumer);
                }
                consumer->OnEndList();
            }
        }
    }
    consumer->OnEndMap();
}

static TString DumpJobInfoForException(const TOperationId& operationId, const TVector<TFailedJobInfo>& failedJobInfoList)
{
    ::TStringBuilder output;
    // Exceptions have limit to contain 65508 bytes of text, so we also limit stderr text
    constexpr size_t MAX_SIZE = 65508 / 2;

    size_t written = 0;
    for (const auto& failedJobInfo : failedJobInfoList) {
        if (written >= MAX_SIZE) {
            break;
        }
        TStringStream nextChunk;
        nextChunk << '\n';
        nextChunk << "OperationId: " << GetGuidAsString(operationId) << " JobId: " << GetGuidAsString(failedJobInfo.JobId) << '\n';
        nextChunk << "Error: " << failedJobInfo.Error.FullDescription() << '\n';
        if (!failedJobInfo.Stderr.empty()) {
            nextChunk << "Stderr: " << Endl;
            size_t tmpWritten = written + nextChunk.Str().size();
            if (tmpWritten >= MAX_SIZE) {
                break;
            }

            if (tmpWritten + failedJobInfo.Stderr.size() > MAX_SIZE) {
                nextChunk << failedJobInfo.Stderr.substr(failedJobInfo.Stderr.size() - (MAX_SIZE - tmpWritten));
            } else {
                nextChunk << failedJobInfo.Stderr;
            }
        }
        written += nextChunk.Str().size();
        output << nextChunk.Str();
    }
    return output;
}

////////////////////////////////////////////////////////////////////////////////

TYtError::TYtError()
    : Code_(0)
{ }

TYtError::TYtError(const TString& message)
    : Code_(NYT::NClusterErrorCodes::Generic)
    , Message_(message)
{ }

TYtError::TYtError(int code, const TString& message)
    : Code_(code)
    , Message_(message)
{ }

TYtError::TYtError(const TJsonValue& value)
{
    const TJsonValue::TMapType& map = value.GetMap();
    TJsonValue::TMapType::const_iterator it = map.find("message");
    if (it != map.end()) {
        Message_ = it->second.GetString();
    }

    it = map.find("code");
    if (it != map.end()) {
        Code_ = static_cast<int>(it->second.GetInteger());
    } else {
        Code_ = NYT::NClusterErrorCodes::Generic;
    }

    it = map.find("inner_errors");
    if (it != map.end()) {
        const TJsonValue::TArray& innerErrors = it->second.GetArray();
        for (const auto& innerError : innerErrors) {
            InnerErrors_.push_back(TYtError(innerError));
        }
    }

    it = map.find("attributes");
    if (it != map.end()) {
        auto attributes = NYT::NodeFromJsonValue(it->second);
        if (attributes.IsMap()) {
            Attributes_ = std::move(attributes.AsMap());
        }
    }
}

TYtError::TYtError(const TNode& node)
{
    const auto& map = node.AsMap();
    auto it = map.find("message");
    if (it != map.end()) {
        Message_ = it->second.AsString();
    }

    it = map.find("code");
    if (it != map.end()) {
        Code_ = static_cast<int>(it->second.AsInt64());
    } else {
        Code_ = NYT::NClusterErrorCodes::Generic;
    }

    it = map.find("inner_errors");
    if (it != map.end()) {
        const auto& innerErrors = it->second.AsList();
        for (const auto& innerError : innerErrors) {
            InnerErrors_.push_back(TYtError(innerError));
        }
    }

    it = map.find("attributes");
    if (it != map.end()) {
        auto& attributes = it->second;
        if (attributes.IsMap()) {
            Attributes_ = std::move(attributes.AsMap());
        }
    }
}

int TYtError::GetCode() const
{
    return Code_;
}

const TString& TYtError::GetMessage() const
{
    return Message_;
}

const TVector<TYtError>& TYtError::InnerErrors() const
{
    return InnerErrors_;
}

void TYtError::ParseFrom(const TString& jsonError)
{
    TJsonValue value;
    TStringInput input(jsonError);
    ReadJsonTree(&input, &value);
    *this = TYtError(value);
}

TSet<int> TYtError::GetAllErrorCodes() const
{
    TDeque<const TYtError*> queue = {this};
    TSet<int> result;
    while (!queue.empty()) {
        const auto* current = queue.front();
        queue.pop_front();
        result.insert(current->Code_);
        for (const auto& error : current->InnerErrors_) {
            queue.push_back(&error);
        }
    }
    return result;
}

bool TYtError::ContainsErrorCode(int code) const
{
    if (Code_ == code) {
        return true;
    }
    for (const auto& error : InnerErrors_) {
        if (error.ContainsErrorCode(code)) {
            return true;
        }
    }
    return false;
}


bool TYtError::ContainsText(const TStringBuf& text) const
{
    if (Message_.Contains(text)) {
        return true;
    }
    for (const auto& error : InnerErrors_) {
        if (error.ContainsText(text)) {
            return true;
        }
    }
    return false;
}

bool TYtError::HasAttributes() const
{
    return !Attributes_.empty();
}

const TNode::TMapType& TYtError::GetAttributes() const
{
    return Attributes_;
}

TString TYtError::GetYsonText() const
{
    TStringStream out;
    ::NYson::TYsonWriter writer(&out, NYson::EYsonFormat::Text);
    SerializeError(*this, &writer);
    return std::move(out.Str());
}

TString TYtError::ShortDescription() const
{
    TStringStream out;
    WriteErrorDescription(*this, &out);
    return std::move(out.Str());
}

TString TYtError::FullDescription() const
{
    TStringStream s;
    WriteErrorDescription(*this, &s);
    s << "; full error: " << GetYsonText();
    return s.Str();
}

////////////////////////////////////////////////////////////////////////////////

TErrorResponse::TErrorResponse(int httpCode, const TString& requestId)
    : HttpCode_(httpCode)
    , RequestId_(requestId)
{ }

bool TErrorResponse::IsOk() const
{
    return Error_.GetCode() == 0;
}

void TErrorResponse::SetRawError(const TString& message)
{
    Error_ = TYtError(message);
    Setup();
}

void TErrorResponse::SetError(TYtError error)
{
    Error_ = std::move(error);
    Setup();
}

void TErrorResponse::ParseFromJsonError(const TString& jsonError)
{
    Error_.ParseFrom(jsonError);
    Setup();
}

void TErrorResponse::SetIsFromTrailers(bool isFromTrailers)
{
    IsFromTrailers_ = isFromTrailers;
}

int TErrorResponse::GetHttpCode() const
{
    return HttpCode_;
}

bool TErrorResponse::IsFromTrailers() const
{
    return IsFromTrailers_;
}

bool TErrorResponse::IsTransportError() const
{
    return HttpCode_ == 503;
}

TString TErrorResponse::GetRequestId() const
{
    return RequestId_;
}

const TYtError& TErrorResponse::GetError() const
{
    return Error_;
}

bool TErrorResponse::IsResolveError() const
{
    return Error_.ContainsErrorCode(NClusterErrorCodes::NYTree::ResolveError);
}

bool TErrorResponse::IsAccessDenied() const
{
    return Error_.ContainsErrorCode(NClusterErrorCodes::NSecurityClient::AuthorizationError);
}

bool TErrorResponse::IsConcurrentTransactionLockConflict() const
{
    return Error_.ContainsErrorCode(NClusterErrorCodes::NCypressClient::ConcurrentTransactionLockConflict);
}

bool TErrorResponse::IsRequestRateLimitExceeded() const
{
    return Error_.ContainsErrorCode(NClusterErrorCodes::NSecurityClient::RequestQueueSizeLimitExceeded);
}

bool TErrorResponse::IsRequestQueueSizeLimitExceeded() const
{
    return Error_.ContainsErrorCode(NClusterErrorCodes::NRpc::RequestQueueSizeLimitExceeded);
}

bool TErrorResponse::IsChunkUnavailable() const
{
    return Error_.ContainsErrorCode(NClusterErrorCodes::NChunkClient::ChunkUnavailable);
}

bool TErrorResponse::IsRequestTimedOut() const
{
    return Error_.ContainsErrorCode(NClusterErrorCodes::Timeout);
}

bool TErrorResponse::IsNoSuchTransaction() const
{
    return Error_.ContainsErrorCode(NClusterErrorCodes::NTransactionClient::NoSuchTransaction);
}

bool TErrorResponse::IsConcurrentOperationsLimitReached() const
{
    return Error_.ContainsErrorCode(NClusterErrorCodes::NScheduler::TooManyOperations);
}

void TErrorResponse::Setup()
{
    TStringStream s;
    *this << Error_.FullDescription();
}

////////////////////////////////////////////////////////////////////////////////

TOperationFailedError::TOperationFailedError(
    EState state,
    TOperationId id,
    TYtError ytError,
    TVector<TFailedJobInfo> failedJobInfo)
    : State_(state)
    , OperationId_(id)
    , Error_(std::move(ytError))
    , FailedJobInfo_(std::move(failedJobInfo))
{
    *this << Error_.FullDescription();
    if (!FailedJobInfo_.empty()) {
        *this << DumpJobInfoForException(OperationId_, FailedJobInfo_);
    }
}

TOperationFailedError::EState TOperationFailedError::GetState() const
{
    return State_;
}

TOperationId TOperationFailedError::GetOperationId() const
{
    return OperationId_;
}

const TYtError& TOperationFailedError::GetError() const
{
    return Error_;
}

const TVector<TFailedJobInfo>& TOperationFailedError::GetFailedJobInfo() const
{
    return FailedJobInfo_;
}

} // namespace NYT
