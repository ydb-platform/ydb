#include "action.h"

namespace NKikimr::NSQS {

namespace {

template <class TReq>
void FillAuthInformation(const TReq& request, const TActionAuthFields& auth) {
    auth.SecurityToken = ExtractSecurityToken(request);
    auth.UserName = request.GetAuth().GetUserName();
    auth.FolderId = request.GetAuth().GetFolderId();
    auth.UserSID = request.GetAuth().GetUserSID();
    auth.MaskedToken = request.GetAuth().GetMaskedToken();
    auth.AuthType = request.GetAuth().GetAuthType();

    if (request.GetAuth().HasSourceAddress()) {
        auth.SourceAddress = request.GetAuth().GetSourceAddress();
    } else if constexpr (requires { request.GetSourceAddress(); }) {
        auth.SourceAddress = request.GetSourceAddress();
    } else {
        auth.SourceAddress.clear();
    }

    if (Cfg().GetYandexCloudMode() && !auth.FolderId) {
        auto items = ParseCloudSecurityToken(auth.SecurityToken);
        auth.UserName = std::get<0>(items);
        auth.FolderId = std::get<1>(items);
        auth.UserSID = std::get<2>(items);
    }
}

void AuditLogEntry(const TString& requestId, const TError* error,
                   const TString& userSID, const TString& userName, const TString& folderId,
                   const TString& queueName, EAction action)
{
    static const TString EmptyValue = "{none}";
    AUDIT_LOG(
        AUDIT_PART("component", "ymq")
        AUDIT_PART("request_id", requestId)
        AUDIT_PART("subject", (userSID ? userSID : EmptyValue))
        AUDIT_PART("account", userName)
        AUDIT_PART("cloud_id", userName, Cfg().GetYandexCloudMode())
        AUDIT_PART("folder_id", folderId, Cfg().GetYandexCloudMode())
        AUDIT_PART("resource_id", queueName, Cfg().GetYandexCloudMode())
        AUDIT_PART("operation", ActionToCloudConvMethod(action))
        AUDIT_PART("queue", queueName)
        AUDIT_PART("status", error ? "ERROR": "SUCCESS")
        AUDIT_PART("reason", error->GetMessage(), error)
        AUDIT_PART("detailed_status", error->GetErrorCode(), error)
    );
}

} // namespace

void FillAuthFromSqsRequest(const NKikimrClient::TSqsRequest& sourceSqsRequest,
                            NKikimrClient::TSqsResponse& response,
                            const TString& requestId,
                            const TActionAuthFields& auth)
{
    #define SQS_REQUEST_CASE(action)                                            \
        const auto& request = sourceSqsRequest.Y_CAT(Get, action)();            \
        auto subResponse = response.Y_CAT(Mutable, action)();                   \
        FillAuthInformation(request, auth);                                     \
        subResponse->SetRequestId(requestId);

    SQS_SWITCH_REQUEST_CUSTOM(sourceSqsRequest, ENUMERATE_ALL_ACTIONS, Y_ABORT_UNLESS(false));
    #undef SQS_REQUEST_CASE
}

void AuditLogSqsResponse(const NKikimrClient::TSqsResponse& response,
                         const TString& requestId,
                         const TString& userSID,
                         const TString& userName,
                         const TString& queueName,
                         EAction action)
{
    auto logEntry = [&](const auto& resp, const TString& reqId, const TError* error) {
        if (!error && resp.HasError()) {
            error = &resp.GetError();
        }
        AuditLogEntry(reqId, error, userSID, userName, response.GetFolderId(), queueName, action);
    };

    #define RESPONSE_CASE(action)                                       \
        case NKikimrClient::TSqsResponse::Y_CAT(k, action): {           \
            logEntry(response.Y_CAT(Get, action)(), requestId, nullptr); \
            break;                                                      \
        }

    #define RESPONSE_BATCH_CASE(action)                                                 \
        case NKikimrClient::TSqsResponse::Y_CAT(k, action): {                           \
            const auto& resp = response.Y_CAT(Get, action)();                           \
            const TError* globalError = resp.HasError() ? &resp.GetError() : nullptr;   \
            for (size_t i = 0; i < resp.EntriesSize(); ++i) {                           \
                TString reqId = TStringBuilder() << requestId << "_" << i;              \
                logEntry(resp.GetEntries()[i], reqId, globalError);                     \
            }                                                                           \
            break;                                                                      \
        }

    switch (response.GetResponseCase()) {
        RESPONSE_CASE(ChangeMessageVisibility)
        RESPONSE_BATCH_CASE(ChangeMessageVisibilityBatch)
        RESPONSE_CASE(CreateQueue)
        RESPONSE_CASE(CreateUser)
        RESPONSE_CASE(DeleteMessage)
        RESPONSE_BATCH_CASE(DeleteMessageBatch)
        RESPONSE_CASE(DeleteQueue)
        RESPONSE_CASE(DeleteUser)
        RESPONSE_CASE(ListPermissions)
        RESPONSE_CASE(GetQueueAttributes)
        RESPONSE_CASE(GetQueueUrl)
        RESPONSE_CASE(ListQueues)
        RESPONSE_CASE(ListUsers)
        RESPONSE_CASE(ModifyPermissions)
        RESPONSE_CASE(PurgeQueue)
        RESPONSE_CASE(ReceiveMessage)
        RESPONSE_CASE(SendMessage)
        RESPONSE_BATCH_CASE(SendMessageBatch)
        RESPONSE_CASE(SetQueueAttributes)
        RESPONSE_CASE(ListDeadLetterSourceQueues)
        RESPONSE_CASE(CountQueues)
        RESPONSE_CASE(ListQueueTags)
        RESPONSE_CASE(TagQueue)
        RESPONSE_CASE(UntagQueue)
    case NKikimrClient::TSqsResponse::kDeleteQueueBatch:
    case NKikimrClient::TSqsResponse::kGetQueueAttributesBatch:
    case NKikimrClient::TSqsResponse::kPurgeQueueBatch:
        // DeleteQueueBatch, GetQueueAttributesBatch, PurgeQueueBatch - generates not batch queries inside
    case NKikimrClient::TSqsResponse::RESPONSE_NOT_SET:
        break;
    }

    #undef RESPONSE_BATCH_CASE
    #undef RESPONSE_CASE
}

bool FillTopicSqsActionMetrics(EAction action, size_t errors, TDuration duration, TDuration workingDuration,
                               NKikimrPQ::TEvTopicSqsActionMetrics& metrics)
{
    const ui32 errorsCount = static_cast<ui32>(errors);
    const ui64 durationMs = duration.MilliSeconds();
    const ui64 workingDurationMs = workingDuration.MilliSeconds();

    auto fillProxyAction = [&](NKikimrPQ::TEvTopicSqsActionMetrics::TTopicSqsProxyActionMetrics* proxyAction) {
        proxyAction->SetErrorsCount(errorsCount);
        proxyAction->SetDurationMs(durationMs);
    };

    switch (action) {
    case EAction::ChangeMessageVisibility:
        fillProxyAction(metrics.MutableChangeMessageVisibility());
        break;
    case EAction::ChangeMessageVisibilityBatch:
        fillProxyAction(metrics.MutableChangeMessageVisibilityBatch());
        break;
    case EAction::DeleteMessage: {
        auto* m = metrics.MutableDeleteMessage();
        m->SetErrorsCount(errorsCount);
        m->SetDurationMs(durationMs);
        break;
    }
    case EAction::DeleteMessageBatch: {
        auto* m = metrics.MutableDeleteMessageBatch();
        m->SetErrorsCount(errorsCount);
        m->SetDurationMs(durationMs);
        break;
    }
    case EAction::GetQueueAttributes:
        fillProxyAction(metrics.MutableGetQueueAttributes());
        break;
    case EAction::GetQueueUrl:
        fillProxyAction(metrics.MutableGetQueueUrl());
        break;
    case EAction::PurgeQueue:
        fillProxyAction(metrics.MutablePurgeQueue());
        break;
    case EAction::ReceiveMessage: {
        auto* m = metrics.MutableReceiveMessage();
        m->SetErrorsCount(errorsCount);
        m->SetDurationMs(durationMs);
        m->SetWorkingDurationMs(workingDurationMs);
        break;
    }
    case EAction::SendMessage: {
        auto* m = metrics.MutableSendMessage();
        m->SetErrorsCount(errorsCount);
        m->SetDurationMs(durationMs);
        break;
    }
    case EAction::SendMessageBatch: {
        auto* m = metrics.MutableSendMessageBatch();
        m->SetErrorsCount(errorsCount);
        m->SetDurationMs(durationMs);
        break;
    }
    case EAction::SetQueueAttributes:
        fillProxyAction(metrics.MutableSetQueueAttributes());
        break;
    case EAction::ListDeadLetterSourceQueues:
        fillProxyAction(metrics.MutableListDeadLetterSourceQueues());
        break;
    case EAction::ListQueueTags:
        fillProxyAction(metrics.MutableListQueueTags());
        break;
    case EAction::TagQueue:
        fillProxyAction(metrics.MutableTagQueue());
        break;
    case EAction::UntagQueue:
        fillProxyAction(metrics.MutableUntagQueue());
        break;
    default:
        return false;
    }
    return true;
}

size_t CalculateSqsPathDepth(const TString& path) {
    const TString sanitizedResource = TFsPath(path).Fix().GetPath();
    size_t count = 0;
    for (size_t i = 0, sz = sanitizedResource.size(); i < sz; ++i) {
        if (sanitizedResource[i] == '/') {
            ++count;
        }
    }

    return count;
}

} // namespace NKikimr::NSQS
