#include "http.h"
#include "xml.h"

#include <ydb/library/services/services.pb.h>
#include <ydb/library/http_proxy/authorization/auth_helpers.h>
#include <ydb/core/ymq/actor/actor.h>
#include <ydb/core/ymq/actor/auth_factory.h>
#include <ydb/core/ymq/actor/events.h>
#include <ydb/core/ymq/actor/log.h>
#include <ydb/core/ymq/actor/serviceid.h>
#include <ydb/core/ymq/base/helpers.h>
#include <ydb/core/ymq/base/limits.h>
#include <ydb/core/ymq/base/secure_protobuf_printer.h>
#include <ydb/core/ymq/base/utils.h>

#include <ydb/library/actors/core/actorsystem.h>
#include <ydb/library/actors/core/log.h>
#include <library/cpp/http/server/response.h>
#include <library/cpp/http/misc/parsed_request.h>

#include <util/generic/guid.h>
#include <util/generic/hash.h>
#include <util/generic/set.h>
#include <util/generic/hash_set.h>
#include <util/network/init.h>
#include <util/string/ascii.h>
#include <util/string/builder.h>
#include <library/cpp/string_utils/quote/quote.h>
#include <util/string/split.h>
#include <library/cpp/string_utils/url/url.h>

namespace NKikimr::NSQS {

using NKikimrClient::TSqsRequest;
using NKikimrClient::TSqsResponse;

namespace {

constexpr TStringBuf AUTHORIZATION_HEADER = "authorization";
constexpr TStringBuf SECURITY_TOKEN_HEADER = "x-amz-security-token";
constexpr TStringBuf IAM_TOKEN_HEADER = "x-yacloud-subjecttoken";
constexpr TStringBuf FORWARDED_IP_HEADER = "x-forwarded-for";
constexpr TStringBuf REQUEST_ID_HEADER = "x-request-id";

const std::vector<TStringBuf> PRIVATE_TOKENS_HEADERS = {
    SECURITY_TOKEN_HEADER,
    IAM_TOKEN_HEADER,
};

const TString CREDENTIAL_PARAM = "credential";

const TSet<TString> ModifyPermissionsActions = {"GrantPermissions", "RevokePermissions", "SetPermissions"};

bool IsPrivateTokenHeader(TStringBuf headerName) {
    for (const TStringBuf h : PRIVATE_TOKENS_HEADERS) {
        if (AsciiEqualsIgnoreCase(h, headerName)) {
            return true;
        }
    }
    return false;
}

class THttpCallback : public IReplyCallback {
public:
    THttpCallback(THttpRequest* req, const TSqsRequest& requestParams)
        : Request_(req)
        , RequestParams_(requestParams)
    {
    }

    void DoSendReply(const TSqsResponse& resp) override {
        auto response = ResponseToAmazonXmlFormat(resp);
        LogRequest(resp, response);

        response.FolderId = resp.GetFolderId();
        response.IsFifo = resp.GetIsFifo();
        response.ResourceId = resp.GetResourceId();

        Request_->SendResponse(response);
    }


private:
    TString LogString(const TSqsResponse& resp) const {
        TStringBuilder rec;
        rec << "Request: " << SecureShortUtf8DebugString(RequestParams_)
            << ", Response: " << SecureShortUtf8DebugString(resp);
        return rec;
    }

    void LogRequest(const TSqsResponse& resp, const TSqsHttpResponse& xmlResp) const {
        const int status = xmlResp.StatusCode;
        const bool is500 = status >= 500 && status < 600;
        auto priority = is500 ? NActors::NLog::PRI_WARN : NActors::NLog::PRI_DEBUG;
        RLOG_SQS_REQ_BASE(*Request_->GetServer()->GetActorSystem(), priority, Request_->GetRequestId(), LogString(resp));
    }

private:
    THttpRequest* const Request_;
    const TSqsRequest RequestParams_;
};

class TPingHttpCallback : public IPingReplyCallback {
public:
    TPingHttpCallback(THttpRequest* req)
        : Request_(req)
    {
    }

    void DoSendReply() override {
        Request_->SendResponse(TSqsHttpResponse("pong", 200, PLAIN_TEXT_CONTENT_TYPE));
    }

private:
    THttpRequest* const Request_;
};

} // namespace

THttpRequest::THttpRequest(TAsyncHttpServer* p)
    : Parent_(p)
{
    Parent_->UpdateConnectionsCountCounter();
}

THttpRequest::~THttpRequest() {
    Parent_->UpdateConnectionsCountCounter();
}

void THttpRequest::SendResponse(const TSqsHttpResponse& r) {
    auto* parent = Parent_;
    auto& actorSystem = *Parent_->ActorSystem_;
    const TString reqId = RequestId_;
    Response_ = r;

    try {
        static_cast<IObjectInQueue*>(this)->Process(nullptr); // calls DoReply()
    } catch (...) {
        // Note: The 'this' pointer has been destroyed inside Process.
        RLOG_SQS_REQ_BASE_ERROR(actorSystem, reqId, "Error while sending response: " << CurrentExceptionMessage());
        INC_COUNTER(parent->HttpCounters_, InternalExceptions);
    }
}

void THttpRequest::WriteResponse(const TReplyParams& replyParams, const TSqsHttpResponse& response) {
    LogHttpRequestResponse(replyParams, response);
    THttpResponse httpResponse(static_cast<HttpCodes>(response.StatusCode));
    if (response.ContentType) {
        httpResponse.SetContent(response.Body, response.ContentType);
    }

    if (Parent_->Config.GetYandexCloudMode() && !IsPrivateRequest_) {
        // Send request attributes to the metering actor
        auto reportRequestAttributes = MakeHolder<TSqsEvents::TEvReportProcessedRequestAttributes>();

        auto& requestAttributes = reportRequestAttributes->Data;

        requestAttributes.HttpStatusCode = response.StatusCode;
        requestAttributes.IsFifo = response.IsFifo;
        requestAttributes.FolderId = response.FolderId;
        requestAttributes.RequestSizeInBytes = RequestSizeInBytes_;
        requestAttributes.ResponseSizeInBytes = response.Body.size();
        requestAttributes.SourceAddress = SourceAddress_;
        requestAttributes.ResourceId = response.ResourceId;
        requestAttributes.Action = Action_;

        Parent_->ActorSystem_->Send(MakeSqsMeteringServiceID(), reportRequestAttributes.Release());
    }

    httpResponse.OutTo(replyParams.Output);
}

TString THttpRequest::LogHttpRequestResponseCommonInfoString() {
    const TDuration duration = TInstant::Now() - StartTime_;
    TStringBuilder logString;
    logString << "Request done.";
    if (UserName_) {
        logString << " User [" << UserName_ << "]";
    }
    if (QueueName_) {
        logString << " Queue [" << QueueName_ << "]";
    }
    if (Action_ != EAction::Unknown) {
        logString << " Action [" << ActionToString(Action_) << "]";
    }
    logString << " IP [" << SourceAddress_ << "] Duration [" << duration.MilliSeconds() << "ms]";
    if (UserSid_) {
        logString << " Subject [" << UserSid_ << "]";
    }
    return logString;
}

TString THttpRequest::LogHttpRequestResponseDebugInfoString(const TReplyParams& replyParams, const TSqsHttpResponse& response) {
    TStringBuilder rec;
    // request
    rec << "Http request: {user: " << UserName_
        << ", action: " << ActionToString(Action_)
        << ", method=\"" << HttpMethod << "\", line=\"" << replyParams.Input.FirstLine() << "\"}";
    // response
    rec << ", http response: {code=" << response.StatusCode;
    if (response.StatusCode != 200) { // Write error description (it doesn't contain user fields that we can't write to log)
        rec << ", response=\"" << response.Body << "\"";
    }
    rec << "}";
    return rec;
}

void THttpRequest::LogHttpRequestResponse(const TReplyParams& replyParams, const TSqsHttpResponse& response) {
    auto& actorSystem = *Parent_->ActorSystem_;
    RLOG_SQS_BASE_INFO(actorSystem, LogHttpRequestResponseCommonInfoString());

    const bool is500 = response.StatusCode >= 500 && response.StatusCode < 600;
    auto priority = is500 ? NActors::NLog::PRI_WARN : NActors::NLog::PRI_DEBUG;
    RLOG_SQS_BASE(actorSystem, priority, LogHttpRequestResponseDebugInfoString(replyParams, response));
}

bool THttpRequest::DoReply(const TReplyParams& p) {
    // this function is called two times
    if (Response_.Defined()) {
        WriteResponse(p, *Response_);
        return true;
    }

    try {
        ParseHeaders(p.Input);

        if (SetupPing(p)) {
            return false;
        }

        ParseRequest(p.Input);

        const TDuration parseTime = TInstant::Now() - StartTime_;
        RLOG_SQS_BASE_DEBUG(*Parent_->ActorSystem_, "Parse time: [" << parseTime.MilliSeconds() << "ms]");

        if (!Parent_->Config.GetYandexCloudMode() && UserName_.empty()) {
            WriteResponse(p, MakeErrorXmlResponse(NErrors::MISSING_PARAMETER, Parent_->AggregatedUserCounters_.Get(), "No user name was provided."));
            return true;
        }

        if (SetupRequest()) {
            return false;
        } else {
            if (Response_.Defined()) {
                WriteResponse(p, *Response_);
            } else {
                WriteResponse(p, MakeErrorXmlResponse(NErrors::INTERNAL_FAILURE, Parent_->AggregatedUserCounters_.Get()));
            }
            return true;
        }
    } catch (...) {
        if (UserCounters_) {
            INC_COUNTER(UserCounters_, RequestExceptions);
        } else if (Parent_->HttpCounters_) {
            INC_COUNTER(Parent_->HttpCounters_, RequestExceptions);
        }

        RLOG_SQS_BASE_INFO(*Parent_->ActorSystem_, "http exception: "
            << "message=" << CurrentExceptionMessage());

        WriteResponse(p, MakeErrorXmlResponseFromCurrentException(Parent_->AggregatedUserCounters_.Get(), RequestId_));
        return true;
    }
}

TString THttpRequest::ExtractQueueNameFromPath(const TStringBuf path) {
    return NKikimr::NSQS::ExtractQueueNameFromPath(path, IsPrivateRequest_);
};

void THttpRequest::ExtractQueueAndAccountNames(const TStringBuf path) {
    if (Action_ == EAction::ModifyPermissions)
        return;

    if (Action_ == EAction::GetQueueUrl || Action_ == EAction::CreateQueue) {
        if (!QueryParams_.QueueName) {
            throw TSQSException(NErrors::MISSING_PARAMETER) << "No queue name was provided.";
        }

        QueueName_ = *QueryParams_.QueueName;
    } else {
        const auto pathAndQuery = QueryParams_.QueueUrl
            ? GetPathAndQuery(*QueryParams_.QueueUrl)
            : GetPathAndQuery(path);
        QueueName_ = ExtractQueueNameFromPath(pathAndQuery);
        AccountName_ = NKikimr::NSQS::ExtractAccountNameFromPath(pathAndQuery, IsPrivateRequest_);

        if (IsProxyAction(Action_)) {
            if (QueryParams_.QueueUrl && *QueryParams_.QueueUrl) {
                if (!QueueName_) {
                    throw TSQSException(NErrors::INVALID_PARAMETER_VALUE) << "Invalid queue url.";
                }
            } else {
                if (!pathAndQuery || pathAndQuery == "/") {
                    throw TSQSException(NErrors::MISSING_PARAMETER) << "No queue url was provided.";
                }
            }
        }
    }
}

TString THttpRequest::HttpHeadersLogString(const THttpInput& input) {
    TStringBuilder headersStr;
    for (const auto& header : input.Headers()) {
        if (!headersStr.empty()) {
            headersStr << ", ";
        } else {
            headersStr << "Http headers: ";
        }
        headersStr << header.Name();
        if (IsPrivateTokenHeader(header.Name())) {
            headersStr << "=" << header.Value().size() << " bytes";
        } else {
            headersStr << "=\"" << header.Value() << "\"";
        }
    }
    if (headersStr.empty()) {
        headersStr << "No http headers";
    }
    return headersStr;
}

void THttpRequest::ParseHeaders(const THttpInput& input) {
    TString sourceReqId;
    for (const auto& header : input.Headers()) {
        if (AsciiEqualsIgnoreCase(header.Name(), AUTHORIZATION_HEADER)) {
            ParseAuthorization(header.Value());
        } else if (AsciiEqualsIgnoreCase(header.Name(), SECURITY_TOKEN_HEADER)) {
            SecurityToken_ = header.Value();
        } else if (AsciiEqualsIgnoreCase(header.Name(), IAM_TOKEN_HEADER)) {
            IamToken_ = header.Value();
        } else if (AsciiEqualsIgnoreCase(header.Name(), FORWARDED_IP_HEADER)) {
            SourceAddress_ = header.Value();
        } else if (AsciiEqualsIgnoreCase(header.Name(), REQUEST_ID_HEADER)) {
            sourceReqId = header.Value();
        }
    }

    GenerateRequestId(sourceReqId);

    if (SourceAddress_.empty()) {
        ExtractSourceAddressFromSocket();
    }

    RLOG_SQS_BASE_TRACE(*Parent_->ActorSystem_, HttpHeadersLogString(input));
}

void THttpRequest::ParseAuthorization(const TString& value) {
    TMap<TString, TString> params = ParseAuthorizationParams(value);

    TString credential = params[CREDENTIAL_PARAM];
    const size_t slashPos = credential.find('/');
    if (slashPos == TString::npos) {
        UserName_ = credential;
    } else {
        UserName_ = credential.substr(0, slashPos);
    }
}

void THttpRequest::ParseCgiParameters(const TCgiParameters& params) {
    TParametersParser parser(&QueryParams_);

    for (auto pi = params.begin(); pi != params.end(); ++pi) {
        parser.Append(pi->first, pi->second);
    }
}

void THttpRequest::ParsePrivateRequestPathPrefix(const TStringBuf& path) {
    IsPrivateRequest_ = NKikimr::NSQS::IsPrivateRequest(path);
}

ui64 THttpRequest::CalculateRequestSizeInBytes(const THttpInput& input, const ui64 contentLength) const {
    ui64 requestSize = input.FirstLine().size();
    for (const auto& header : input.Headers()) {
        requestSize += header.Name().size() + header.Value().size();
    }
    if (input.Trailers()) {
        for (const auto& header : *input.Trailers()) {
            requestSize += header.Name().size() + header.Value().size();
        }
    }
    requestSize += contentLength;
    return requestSize;
}

void THttpRequest::ParseRequest(THttpInput& input) {
    if (Parent_->HttpCounters_ && UserName_) {
        UserCounters_ = Parent_->HttpCounters_->GetUserCounters(UserName_);
    }

    TParsedHttpFull parsed(input.FirstLine());
    HttpMethod = TString(parsed.Method);
    ui64 contentLength = 0;
    if (HttpMethod == "POST") {
        try {
            if (input.GetContentLength(contentLength)) {
                InputData.ConstructInPlace();
                InputData->Resize(contentLength);
                if (input.Load(InputData->Data(), (size_t)contentLength) != contentLength) {
                    throw TSQSException(NErrors::MALFORMED_QUERY_STRING) << "Can't load request body.";
                }
            } else {
                throw TSQSException(NErrors::MISSING_PARAMETER) << "No Content-Length.";
            }
        } catch (...) {
            RLOG_SQS_BASE_ERROR(*Parent_->ActorSystem_, "Failed to parse http request \"" << input.FirstLine() << "\": " << CurrentExceptionMessage());
        }
    }

    RLOG_SQS_BASE_DEBUG(*Parent_->ActorSystem_, "Incoming http request: " << input.FirstLine());

    ParsePrivateRequestPathPrefix(parsed.Path);

    RequestSizeInBytes_ = CalculateRequestSizeInBytes(input, contentLength);

    if (HttpMethod == "POST") {
        ParseCgiParameters(TCgiParameters(TStringBuf(InputData->Data(), contentLength)));
    } else if (HttpMethod == "GET") {
        ParseCgiParameters(TCgiParameters(parsed.Cgi));
    } else {
        throw TSQSException(NErrors::MALFORMED_QUERY_STRING) << "Unsupported method: \"" << parsed.Method << "\".";
    }

    if (QueryParams_.Action) {
        if (IsIn(ModifyPermissionsActions, *QueryParams_.Action)) {
            Action_ = EAction::ModifyPermissions;
        } else {
            Action_ = ActionFromString(*QueryParams_.Action);
        }

        THttpActionCounters* counters = GetActionCounters();
        INC_COUNTER(counters, Requests);
    } else {
        throw TSQSException(NErrors::MISSING_ACTION) << "Action param was not found.";
    }

    if (QueryParams_.FolderId) {
        FolderId_ = *QueryParams_.FolderId;
    }

    ExtractQueueAndAccountNames(parsed.Path);

    if (Parent_->Config.GetYandexCloudMode() && !IamToken_ && !FolderId_) {
        AwsSignature_.Reset(new TAwsRequestSignV4(input, parsed, InputData));
    }
}

#define HANDLE_SETUP_ACTION_CASE(NAME)                                           \
    case EAction::NAME: {                                                        \
        Y_CAT(Setup, NAME)(requestHolder->Y_CAT(Mutable, NAME)());               \
        CopyCredentials(requestHolder->Y_CAT(Mutable, NAME)(), Parent_->Config); \
        break;                                                                   \
    }

#define HANDLE_SETUP_PRIVATE_ACTION_CASE(NAME)                                   \
    case EAction::NAME: {                                                        \
        if (!IsPrivateRequest_) {                                                \
            RLOG_SQS_BASE_ERROR(*Parent_->ActorSystem_,                              \
                            "Attempt to call "                                   \
                            Y_STRINGIZE(NAME)                                    \
                            " action without private url path");                 \
            throw TSQSException(NErrors::INVALID_ACTION);                        \
        }                                                                        \
        Y_CAT(SetupPrivate, NAME)(requestHolder->Y_CAT(Mutable, NAME)());        \
        CopyCredentials(requestHolder->Y_CAT(Mutable, NAME)(), Parent_->Config); \
        break;                                                                   \
    }

bool HasPrivateActionParams(EAction action, const TParameters& params) {
    if (action == EAction::CreateQueue) {
        return params.CreateTimestampSeconds || params.CustomQueueName;
    }
    return false;
}

bool THttpRequest::SetupRequest() {
    auto requestHolder = MakeHolder<TSqsRequest>();
    requestHolder->SetRequestId(RequestId_);

    if (HasPrivateActionParams(Action_, QueryParams_) && !IsPrivateRequest_) {
        RLOG_SQS_BASE_ERROR(
            *Parent_->ActorSystem_,
            "Attempt to call private " << Action_ << " action format without private url path"
        );
        throw TSQSException(NErrors::INVALID_ACTION);
    }
    // Validate batches
    if (IsBatchAction(Action_)) {
        if (QueryParams_.BatchEntries.empty()) {
            throw TSQSException(NErrors::EMPTY_BATCH_REQUEST);
        }
        if (!IsPrivateAction(Action_) && QueryParams_.BatchEntries.size() > TLimits::MaxBatchSize) {
            throw TSQSException(NErrors::TOO_MANY_ENTRIES_IN_BATCH_REQUEST);
        }
        THashSet<TString> ids;
        for (const auto& entry : QueryParams_.BatchEntries) {
            if (!entry.second.Id || !*entry.second.Id) {
                throw TSQSException(NErrors::MISSING_PARAMETER) << "No id in batch entry.";
            }

            if (!ids.insert(*entry.second.Id).second) {
                throw TSQSException(NErrors::BATCH_ENTRY_IDS_NOT_DISTINCT);
            }
        }
    }

    switch (Action_) {
        HANDLE_SETUP_ACTION_CASE(ChangeMessageVisibility);
        HANDLE_SETUP_ACTION_CASE(ChangeMessageVisibilityBatch);
        HANDLE_SETUP_ACTION_CASE(CreateQueue);
        HANDLE_SETUP_ACTION_CASE(CreateUser);
        HANDLE_SETUP_ACTION_CASE(GetQueueAttributes);
        HANDLE_SETUP_ACTION_CASE(GetQueueUrl);
        HANDLE_SETUP_ACTION_CASE(DeleteMessage);
        HANDLE_SETUP_ACTION_CASE(DeleteMessageBatch);
        HANDLE_SETUP_ACTION_CASE(DeleteQueue);
        HANDLE_SETUP_ACTION_CASE(DeleteUser);
        HANDLE_SETUP_ACTION_CASE(ListPermissions);
        HANDLE_SETUP_ACTION_CASE(ListDeadLetterSourceQueues);
        HANDLE_SETUP_ACTION_CASE(ListQueues);
        HANDLE_SETUP_ACTION_CASE(ListUsers);
        HANDLE_SETUP_ACTION_CASE(ModifyPermissions);
        HANDLE_SETUP_ACTION_CASE(PurgeQueue);
        HANDLE_SETUP_ACTION_CASE(ReceiveMessage);
        HANDLE_SETUP_ACTION_CASE(SendMessage);
        HANDLE_SETUP_ACTION_CASE(SendMessageBatch);
        HANDLE_SETUP_ACTION_CASE(SetQueueAttributes);

        HANDLE_SETUP_PRIVATE_ACTION_CASE(DeleteQueueBatch);
        HANDLE_SETUP_PRIVATE_ACTION_CASE(CountQueues);
        HANDLE_SETUP_PRIVATE_ACTION_CASE(PurgeQueueBatch);
        HANDLE_SETUP_PRIVATE_ACTION_CASE(GetQueueAttributesBatch);

        case EAction::Unknown:
        case EAction::ActionsArraySize: // to avoid compiler warning
            Response_ = MakeErrorXmlResponse(NErrors::MISSING_ACTION, Parent_->AggregatedUserCounters_.Get(), TStringBuilder() << "Unknown action: \"" + *QueryParams_.Action << "\".");
            return false;
    }

    RLOG_SQS_BASE_DEBUG(*Parent_->ActorSystem_, "Create proxy action actor for request " << SecureShortUtf8DebugString(*requestHolder));

    const bool enableQueueLeader = Parent_->Config.HasEnableQueueMaster()
        ? Parent_->Config.GetEnableQueueMaster()
        : Parent_->Config.GetEnableQueueLeader();

    auto httpCallback = MakeHolder<THttpCallback>(this, *requestHolder);

    TAuthActorData data {
        .SQSRequest = std::move(requestHolder),
        .HTTPCallback = std::move(httpCallback),
        .UserSidCallback = [this](const TString& userSid) { UserSid_ = userSid; },
        .EnableQueueLeader = enableQueueLeader,
        .Action = Action_,
        .ExecutorPoolID = Parent_->PoolId_,
        .CloudID = AccountName_,
        .ResourceID = QueueName_,
        .Counters = Parent_->CloudAuthCounters_.Get(),
        .AWSSignature = std::move(AwsSignature_),
        .IAMToken = IamToken_,
        .FolderID = FolderId_
    };

    AppData(Parent_->ActorSystem_)->SqsAuthFactory->RegisterAuthActor(
        *Parent_->ActorSystem_,
        std::move(data));

    return true;
}

void THttpRequest::SetupChangeMessageVisibility(TChangeMessageVisibilityRequest* const req) {
    req->SetQueueName(QueueName_);
    req->MutableAuth()->SetUserName(UserName_);

    if (QueryParams_.ReceiptHandle) {
        req->SetReceiptHandle(CGIEscapeRet(*QueryParams_.ReceiptHandle));
    }
    if (QueryParams_.VisibilityTimeout) {
        req->SetVisibilityTimeout(*QueryParams_.VisibilityTimeout);
    }
}

void THttpRequest::SetupChangeMessageVisibilityBatch(TChangeMessageVisibilityBatchRequest* const req) {
    req->MutableAuth()->SetUserName(UserName_);
    req->SetQueueName(QueueName_);

    for (const auto& item : QueryParams_.BatchEntries) {
        const TParameters& params = item.second;
        TChangeMessageVisibilityRequest* const entry = req->AddEntries();

        if (params.Id) {
            entry->SetId(*params.Id);
        }
        if (params.ReceiptHandle) {
            entry->SetReceiptHandle(CGIEscapeRet(*params.ReceiptHandle));
        }
        if (params.VisibilityTimeout) {
            entry->SetVisibilityTimeout(*params.VisibilityTimeout);
        }
    }
}

void THttpRequest::SetupCreateQueue(TCreateQueueRequest* const req) {
    req->SetQueueName(QueueName_);
    req->MutableAuth()->SetUserName(UserName_);

    if (QueryParams_.CreateTimestampSeconds) {
        req->SetCreatedTimestamp(QueryParams_.CreateTimestampSeconds.GetRef());
    }
    if (QueryParams_.CustomQueueName) {
        req->SetCustomQueueName(QueryParams_.CustomQueueName.GetRef());
    }

    for (const auto& attr : QueryParams_.Attributes) {
        req->AddAttributes()->CopyFrom(attr.second);
    }
}

void THttpRequest::SetupCreateUser(TCreateUserRequest* const req) {
    req->MutableAuth()->SetUserName(UserName_);

    if (QueryParams_.UserName) {
        req->SetUserName(*QueryParams_.UserName);
    }
}

void THttpRequest::SetupDeleteMessage(TDeleteMessageRequest* const req) {
    req->SetQueueName(QueueName_);
    req->MutableAuth()->SetUserName(UserName_);

    if (QueryParams_.ReceiptHandle) {
        req->SetReceiptHandle(CGIEscapeRet(*QueryParams_.ReceiptHandle));
    }
}

void THttpRequest::SetupDeleteMessageBatch(TDeleteMessageBatchRequest* const req) {
    req->MutableAuth()->SetUserName(UserName_);
    req->SetQueueName(QueueName_);

    for (const auto& item : QueryParams_.BatchEntries) {
        const TParameters& params = item.second;
        TDeleteMessageRequest* const entry = req->AddEntries();

        if (params.Id) {
            entry->SetId(*params.Id);
        }
        if (params.ReceiptHandle) {
            entry->SetReceiptHandle(CGIEscapeRet(*params.ReceiptHandle));
        }
    }
}

void THttpRequest::SetupDeleteQueue(TDeleteQueueRequest* const req) {
    req->SetQueueName(QueueName_);
    req->MutableAuth()->SetUserName(UserName_);
}

void THttpRequest::SetupListPermissions(TListPermissionsRequest* const req) {
    if (QueryParams_.Path) {
        req->SetPath(*QueryParams_.Path);
    }
}

void THttpRequest::SetupListDeadLetterSourceQueues(TListDeadLetterSourceQueuesRequest* const req) {
    req->SetQueueName(QueueName_);
    req->MutableAuth()->SetUserName(UserName_);
}

void THttpRequest::SetupPrivateDeleteQueueBatch(TDeleteQueueBatchRequest* const req) {
    for (const auto& entry : QueryParams_.BatchEntries) {
        auto* protoEntry = req->AddEntries();
        const TParameters& params = entry.second;
        if (params.Id) {
            protoEntry->SetId(*params.Id);
        }
        if (params.QueueUrl) {
            protoEntry->SetQueueName(ExtractQueueNameFromPath(GetPathAndQuery(*params.QueueUrl)));
        }
    }
    req->MutableAuth()->SetUserName(UserName_);
}

void THttpRequest::SetupPrivateCountQueues(TCountQueuesRequest* const req) {
    req->MutableAuth()->SetUserName(UserName_);
}

void THttpRequest::SetupPrivatePurgeQueueBatch(TPurgeQueueBatchRequest* const req) {
    for (const auto& entry : QueryParams_.BatchEntries) {
        auto* protoEntry = req->AddEntries();
        const TParameters& params = entry.second;
        if (params.Id) {
            protoEntry->SetId(*params.Id);
        }
        if (params.QueueUrl) {
            protoEntry->SetQueueName(ExtractQueueNameFromPath(GetPathAndQuery(*params.QueueUrl)));
        }
    }
    req->MutableAuth()->SetUserName(UserName_);
}

void THttpRequest::SetupPrivateGetQueueAttributesBatch(TGetQueueAttributesBatchRequest* const req) {
    for (const auto& entry : QueryParams_.BatchEntries) {
        auto* protoEntry = req->AddEntries();
        const TParameters& params = entry.second;
        if (params.Id) {
            protoEntry->SetId(*params.Id);
        }
        if (params.QueueUrl) {
            protoEntry->SetQueueName(ExtractQueueNameFromPath(GetPathAndQuery(*params.QueueUrl)));
        }
    }
    for (const auto& name : QueryParams_.AttributeNames) {
        req->AddNames(name.second);
    }
    req->MutableAuth()->SetUserName(UserName_);
}

void THttpRequest::SetupDeleteUser(TDeleteUserRequest* const req) {
    req->MutableAuth()->SetUserName(UserName_);

    if (QueryParams_.UserName) {
        req->SetUserName(*QueryParams_.UserName);
    }
}

void THttpRequest::SetupGetQueueAttributes(TGetQueueAttributesRequest* const req) {
    req->SetQueueName(QueueName_);
    req->MutableAuth()->SetUserName(UserName_);

    for (const auto& name : QueryParams_.AttributeNames) {
        req->AddNames(name.second);
    }
}

void THttpRequest::SetupGetQueueUrl(TGetQueueUrlRequest* const req) {
    req->SetQueueName(QueueName_);
    req->MutableAuth()->SetUserName(UserName_);
}

void THttpRequest::SetupListQueues(TListQueuesRequest* const req) {
    req->MutableAuth()->SetUserName(UserName_);

    if (QueryParams_.QueueNamePrefix) {
        req->SetQueueNamePrefix(*QueryParams_.QueueNamePrefix);
    }
}

void THttpRequest::SetupListUsers(TListUsersRequest* const req) {
    req->MutableAuth()->SetUserName(UserName_);

    if (QueryParams_.UserNamePrefix) {
        req->SetUserNamePrefix(*QueryParams_.UserNamePrefix);
    }
}

template<typename TModifyPermissionsAction>
static void SetupModifyPermissionsAction(const TParameters& queryParams, TModifyPermissionsAction& action) {
    if (queryParams.Subject) {
        action.SetSubject(*queryParams.Subject);
    }

    for (const auto& [index, entry] : queryParams.BatchEntries) {
        if (entry.Action) {
            *action.MutablePermissionNames()->Add() = *entry.Action;
        }
    }
}

void THttpRequest::SetupModifyPermissions(TModifyPermissionsRequest* const req) {
    auto it = ModifyPermissionsActions.begin();
    if (*QueryParams_.Action == *it++) {
        SetupModifyPermissionsAction(QueryParams_, *req->MutableActions()->Add()->MutableGrant());
    } else if (*QueryParams_.Action == *it++)   {
        SetupModifyPermissionsAction(QueryParams_, *req->MutableActions()->Add()->MutableRevoke());
    } else if (*QueryParams_.Action == *it) {
        SetupModifyPermissionsAction(QueryParams_, *req->MutableActions()->Add()->MutableSet());
    }

    Y_ABORT_UNLESS(it != ModifyPermissionsActions.end());

    if (QueryParams_.Path) {
        req->SetResource(*QueryParams_.Path);
    }

    if (QueryParams_.Clear) {
        bool clear = false;
        if (TryFromString(*QueryParams_.Clear, clear)) {
            req->SetClearACL(clear);
        }
    }
}

void THttpRequest::SetupPurgeQueue(TPurgeQueueRequest* const req) {
    req->SetQueueName(QueueName_);
    req->MutableAuth()->SetUserName(UserName_);
}

void THttpRequest::SetupReceiveMessage(TReceiveMessageRequest* const req) {
    req->SetQueueName(QueueName_);
    req->MutableAuth()->SetUserName(UserName_);

    if (QueryParams_.MaxNumberOfMessages) {
        req->SetMaxNumberOfMessages(*QueryParams_.MaxNumberOfMessages);
    } else {
        req->SetMaxNumberOfMessages(1);
    }
    if (QueryParams_.ReceiveRequestAttemptId) {
        req->SetReceiveRequestAttemptId(*QueryParams_.ReceiveRequestAttemptId);
    }
    if (QueryParams_.VisibilityTimeout) {
        req->SetVisibilityTimeout(*QueryParams_.VisibilityTimeout);
    }
    if (QueryParams_.WaitTimeSeconds) {
        req->SetWaitTimeSeconds(*QueryParams_.WaitTimeSeconds);
    }

    for (const auto& name : QueryParams_.AttributeNames) {
        req->AddAttributeName(name.second);
    }
    for (const auto& item : QueryParams_.MessageAttributes) {
        req->AddMessageAttributeName(item.second.GetName());
    }
}

static void ValidateMessageAttribute(const TMessageAttribute& attr, bool allowYandexPrefix, bool& hasYandexPrefix) {
    if (!ValidateMessageAttributeName(attr.GetName(), hasYandexPrefix, allowYandexPrefix)) {
        throw TSQSException(NErrors::INVALID_PARAMETER_VALUE) << "Invalid message attribute name.";
    }
    if (attr.GetDataType().empty()) {
        throw TSQSException(NErrors::INVALID_PARAMETER_COMBINATION) << "No message attribute data type provided.";
    }
    if (attr.GetDataType().size() > 256) {
        throw TSQSException(NErrors::INVALID_PARAMETER_VALUE) << "Message attribute data type is too long.";
    }
    if (attr.GetStringValue().empty() && attr.GetBinaryValue().empty()) {
        throw TSQSException(NErrors::INVALID_PARAMETER_COMBINATION) << "No message attribute value provided.";
    }
    if (!attr.GetStringValue().empty() && !attr.GetBinaryValue().empty()) {
        throw TSQSException(NErrors::INVALID_PARAMETER_COMBINATION) << "Message attribute has both value and binary value.";
    }
}

static void ValidateMessageAttributes(const TMap<int, TMessageAttribute>& messageAttributes, bool allowYandexPrefix, bool& hasYandexPrefix) {
    THashSet<TStringBuf> attributeNames;
    for (const auto& item : messageAttributes) {
        ValidateMessageAttribute(item.second, allowYandexPrefix, hasYandexPrefix);
        if (!attributeNames.insert(item.second.GetName()).second) {
            throw TSQSException(NErrors::INVALID_PARAMETER_COMBINATION) << "Duplicated message attribute name.";
        }
    }
}

static TString FormatNames(const TMap<int, TMessageAttribute>& messageAttributes) {
    TStringBuilder names;
    for (const auto& item : messageAttributes) {
        if (!names.empty()) {
            names << ", ";
        }
        names << "\"" << item.second.GetName() << "\"";
    }
    return std::move(names);
}

void THttpRequest::SetupSendMessage(TSendMessageRequest* const req) {
    req->SetQueueName(QueueName_);
    req->MutableAuth()->SetUserName(UserName_);

    if (QueryParams_.DelaySeconds) {
        req->SetDelaySeconds(*QueryParams_.DelaySeconds);
    }
    if (QueryParams_.MessageBody) {
        req->SetMessageBody(*QueryParams_.MessageBody);
    }
    if (QueryParams_.MessageDeduplicationId) {
        req->SetMessageDeduplicationId(*QueryParams_.MessageDeduplicationId);
    }
    if (QueryParams_.MessageGroupId) {
        req->SetMessageGroupId(*QueryParams_.MessageGroupId);
    }

    bool hasYandexPrefix = false;
    ValidateMessageAttributes(QueryParams_.MessageAttributes, Parent_->Config.GetAllowYandexAttributePrefix(), hasYandexPrefix);
    if (hasYandexPrefix) {
        RLOG_SQS_BASE_WARN(*Parent_->ActorSystem_, "Attribute names contain yandex reserved prefix: " << FormatNames(QueryParams_.MessageAttributes));
    }

    for (const auto& item : QueryParams_.MessageAttributes) {
        req->AddMessageAttributes()->CopyFrom(item.second);
    }
}

void THttpRequest::SetupSendMessageBatch(TSendMessageBatchRequest* const req) {
    req->MutableAuth()->SetUserName(UserName_);
    req->SetQueueName(QueueName_);

    for (const auto& item : QueryParams_.BatchEntries) {
        const TParameters& params = item.second;
        TSendMessageRequest* const entry = req->AddEntries();

        if (params.DelaySeconds) {
            entry->SetDelaySeconds(*params.DelaySeconds);
        }
        if (params.Id) {
            entry->SetId(*params.Id);
        }
        if (params.MessageBody) {
            entry->SetMessageBody(*params.MessageBody);
        }
        if (params.MessageDeduplicationId) {
            entry->SetMessageDeduplicationId(*params.MessageDeduplicationId);
        }
        if (params.MessageGroupId) {
            entry->SetMessageGroupId(*params.MessageGroupId);
        }

        bool hasYandexPrefix = false;
        ValidateMessageAttributes(params.MessageAttributes, Parent_->Config.GetAllowYandexAttributePrefix(), hasYandexPrefix);
        if (hasYandexPrefix) {
            RLOG_SQS_BASE_WARN(*Parent_->ActorSystem_, "Attribute names contain yandex reserved prefix: " << FormatNames(params.MessageAttributes));
        }

        for (const auto& attr : params.MessageAttributes) {
            entry->AddMessageAttributes()->CopyFrom(attr.second);
        }
    }
}

void THttpRequest::SetupSetQueueAttributes(TSetQueueAttributesRequest* const req) {
    req->SetQueueName(QueueName_);
    req->MutableAuth()->SetUserName(UserName_);

    for (const auto& attr : QueryParams_.Attributes) {
        req->AddAttributes()->CopyFrom(attr.second);
    }
}

void THttpRequest::ExtractSourceAddressFromSocket() {
    struct sockaddr_in6 addr;
    socklen_t addrSize = sizeof(struct sockaddr_in6);
    if (getpeername(Socket(), (struct sockaddr*)&addr, &addrSize) != 0) {
        SourceAddress_ = "unknown";
    } else {
        char address[INET6_ADDRSTRLEN];
        if (inet_ntop(AF_INET6, &(addr.sin6_addr), address, INET6_ADDRSTRLEN) != nullptr) {
            SourceAddress_ = address;
        } else {
            SourceAddress_ = "unknown";
        }
    }
}

void THttpRequest::GenerateRequestId(const TString& sourceReqId) {
    TStringBuilder builder;
    builder << CreateGuidAsString();
    if (!sourceReqId.empty()) {
        builder << "-" << sourceReqId;
    }

    RequestId_ = std::move(builder);
}

THttpActionCounters* THttpRequest::GetActionCounters() const {
    if (Action_ <= EAction::Unknown || Action_ >= EAction::ActionsArraySize) {
        return nullptr;
    }
    if (!UserCounters_) {
        return nullptr;
    }
    return &UserCounters_->ActionCounters[Action_];
}

bool THttpRequest::SetupPing(const TReplyParams& params) {
    TParsedHttpFull parsed(params.Input.FirstLine());
    if (parsed.Method == "GET" && (parsed.Path == "/private/ping" || parsed.Path == "/private/ping/") && parsed.Cgi.empty()) {
        HttpMethod = TString(parsed.Method); // for logging
        Parent_->ActorSystem_->Register(CreatePingActor(MakeHolder<TPingHttpCallback>(this), RequestId_),
                                        NActors::TMailboxType::HTSwap, Parent_->PoolId_);
        return true;
    }
    return false;
}

///////////////////////////////////////////////////////////////////////////////

TAsyncHttpServer::TAsyncHttpServer(const NKikimrConfig::TSqsConfig& config)
    : THttpServer(this, MakeHttpServerOptions(config))
    , Config(config)
{}

TAsyncHttpServer::~TAsyncHttpServer() {
    Stop();
}

void TAsyncHttpServer::Initialize(
        NActors::TActorSystem* as, TIntrusivePtr<::NMonitoring::TDynamicCounters> sqsCounters,
        TIntrusivePtr<::NMonitoring::TDynamicCounters> ymqCounters, ui32 poolId
) {
    ActorSystem_ = as;
    HttpCounters_ = new THttpCounters(Config, sqsCounters->GetSubgroup("subsystem", "http"));
    if (Config.GetYandexCloudMode()) {
        CloudAuthCounters_ = MakeHolder<TCloudAuthCounters>(Config, sqsCounters->GetSubgroup("subsystem", "cloud_auth"));
    }
    AggregatedUserCounters_ = MakeIntrusive<TUserCounters>(
            Config, sqsCounters->GetSubgroup("subsystem", "core"), ymqCounters,
            nullptr, TOTAL_COUNTER_LABEL, nullptr, true
    );
    AggregatedUserCounters_->ShowDetailedCounters(TInstant::Max());
    PoolId_ = poolId;
}

void TAsyncHttpServer::Start() {
    if (!THttpServer::Start()) {
        Y_ABORT("Unable to start http server for SQS service on port %" PRIu16, Options().Port);
    }
}

TClientRequest* TAsyncHttpServer::CreateClient() {
    return new THttpRequest(this);
}

void TAsyncHttpServer::UpdateConnectionsCountCounter() {
    if (HttpCounters_) {
        *HttpCounters_->ConnectionsCount = GetClientCount();
    }
}

void TAsyncHttpServer::OnException() {
    LOG_SQS_BASE_ERROR(*ActorSystem_, "Exception in http server: " << CurrentExceptionMessage());
    INC_COUNTER(HttpCounters_, InternalExceptions);
}

THttpServerOptions TAsyncHttpServer::MakeHttpServerOptions(const NKikimrConfig::TSqsConfig& config) {
    const auto& cfg = config.GetHttpServerConfig();
    THttpServerOptions options;
    options.SetThreads(cfg.GetThreads());
    options.SetPort(cfg.GetPort());
    options.SetMaxConnections(cfg.GetMaxConnections());
    options.SetMaxQueueSize(cfg.GetMaxQueueSize());
    options.EnableKeepAlive(cfg.GetEnableKeepAlive());
    return options;
}

} // namespace NKikimr::NSQS
