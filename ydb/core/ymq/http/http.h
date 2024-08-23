#pragma once

#include "params.h"
#include "types.h"

#include <ydb/core/protos/config.pb.h>
#include <ydb/core/protos/sqs.pb.h>

#include <ydb/library/http_proxy/authorization/signature.h>
#include <ydb/core/ymq/base/counters.h>

#include <ydb/library/actors/core/actorsystem.h>
#include <library/cpp/http/server/http.h>

#include <util/generic/buffer.h>
#include <util/generic/maybe.h>
#include <library/cpp/cgiparam/cgiparam.h>

namespace NKikimr::NSQS {

class TAsyncHttpServer;
class THttpRequest;

class THttpRequest : public TRequestReplier {
public:
    THttpRequest(TAsyncHttpServer* p);
    ~THttpRequest();

    void SendResponse(const TSqsHttpResponse& r);

    const TString& GetRequestId() const {
        return RequestId_;
    }

    const TAsyncHttpServer* GetServer() const {
        return Parent_;
    }

private:
    bool DoReply(const TReplyParams& p) override;

    void WriteResponse(const TReplyParams& replyParams, const TSqsHttpResponse& response);

    TString LogHttpRequestResponseCommonInfoString();
    TString LogHttpRequestResponseDebugInfoString(const TReplyParams& replyParams, const TSqsHttpResponse& response);
    void LogHttpRequestResponse(const TReplyParams& replyParams, const TSqsHttpResponse& response);

private:
    template<typename T>
    void CopyCredentials(T* const request, const NKikimrConfig::TSqsConfig& config) {
        if (SecurityToken_ && !config.GetYandexCloudMode()) {
            // it's also TVM-compatible due to universal TicketParser
            request->MutableCredentials()->SetOAuthToken(SecurityToken_);
        }
    }

    TString GetRequestPathPart(TStringBuf path, size_t partIdx) const;
    TString ExtractQueueNameFromPath(const TStringBuf path);

    ui64 CalculateRequestSizeInBytes(const THttpInput& input, const ui64 contentLength) const;
    void ExtractQueueAndAccountNames(const TStringBuf path);

    TString HttpHeadersLogString(const THttpInput& input);
    void ParseHeaders(const THttpInput& input);
    void ParseAuthorization(const TString& value);
    void ParseRequest(THttpInput& input);
    void ParseCgiParameters(const TCgiParameters& params);
    void ParsePrivateRequestPathPrefix(const TStringBuf& path);

    bool SetupRequest();

    void SetupChangeMessageVisibility(TChangeMessageVisibilityRequest* const req);
    void SetupChangeMessageVisibilityBatch(TChangeMessageVisibilityBatchRequest* const req);
    void SetupCreateQueue(TCreateQueueRequest* const req);
    void SetupCreateUser(TCreateUserRequest* const req);
    void SetupGetQueueAttributes(TGetQueueAttributesRequest* const req);
    void SetupGetQueueUrl(TGetQueueUrlRequest* const req);
    void SetupDeleteMessage(TDeleteMessageRequest* const req);
    void SetupDeleteMessageBatch(TDeleteMessageBatchRequest* const req);
    void SetupDeleteQueue(TDeleteQueueRequest* const req);
    void SetupListPermissions(TListPermissionsRequest* const req);
    void SetupListDeadLetterSourceQueues(TListDeadLetterSourceQueuesRequest* const req);
    void SetupPrivateDeleteQueueBatch(TDeleteQueueBatchRequest* const req);
    void SetupPrivatePurgeQueueBatch(TPurgeQueueBatchRequest* const req);
    void SetupPrivateGetQueueAttributesBatch(TGetQueueAttributesBatchRequest* const req);
    void SetupDeleteUser(TDeleteUserRequest* const req);
    void SetupListQueues(TListQueuesRequest* const req);
    void SetupPrivateCountQueues(TCountQueuesRequest* const req);
    void SetupListUsers(TListUsersRequest* const req);
    void SetupModifyPermissions(TModifyPermissionsRequest* const req);
    void SetupReceiveMessage(TReceiveMessageRequest* const req);
    void SetupSendMessage(TSendMessageRequest* const req);
    void SetupSendMessageBatch(TSendMessageBatchRequest* const req);
    void SetupPurgeQueue(TPurgeQueueRequest* const req);
    void SetupSetQueueAttributes(TSetQueueAttributesRequest* const req);

    void ExtractSourceAddressFromSocket();

    void GenerateRequestId(const TString& sourceReqId);

    THttpActionCounters* GetActionCounters() const;

    // Checks whether request is ping and then starts ping actor.
    // If request is ping, returns true, otherwise - false.
    bool SetupPing(const TReplyParams& p);

private:
    TAsyncHttpServer* const Parent_;
    TIntrusivePtr<THttpUserCounters> UserCounters_;

    TParameters QueryParams_;
    EAction Action_ = EAction::Unknown;
    TString UserName_;
    TString AccountName_;
    TString QueueName_;
    TString SecurityToken_;
    TString IamToken_;
    TString FolderId_;
    TString ApiMethod_;

    THolder<TAwsRequestSignV4> AwsSignature_;

    TMaybe<TBuffer> InputData;
    TString HttpMethod;
    TMaybe<TSqsHttpResponse> Response_;
    TString RequestId_;

    // Source values parsed from headers
    TString SourceAddress_;

    ui64 RequestSizeInBytes_ = 0;

    bool IsPrivateRequest_ = false; // Has "/private" path prefix
    TInstant StartTime_ = TInstant::Now();

    TString UserSid_;
};

class TAsyncHttpServer
    : public THttpServer
    , public THttpServer::ICallBack
{
    friend THttpRequest;

public:
    TAsyncHttpServer(const NKikimrConfig::TSqsConfig& config);
    ~TAsyncHttpServer();

    void Initialize(
            NActors::TActorSystem* as,
            TIntrusivePtr<::NMonitoring::TDynamicCounters> sqsCounters,
            TIntrusivePtr<::NMonitoring::TDynamicCounters> ymqCounters,
            ui32 poolId);

    void Start();

    NActors::TActorSystem* GetActorSystem() const {
        return ActorSystem_;
    }

private:
    // THttpServer::ICallback
    TClientRequest* CreateClient() override;
    void OnException() override;
    static THttpServerOptions MakeHttpServerOptions(const NKikimrConfig::TSqsConfig& config);

    void UpdateConnectionsCountCounter();

private:
    const NKikimrConfig::TSqsConfig Config;
    NActors::TActorSystem* ActorSystem_ = nullptr;
    TIntrusivePtr<THttpCounters> HttpCounters_; // http subsystem counters
    THolder<TCloudAuthCounters> CloudAuthCounters_; // cloud_auth subsystem counters
    TIntrusivePtr<TUserCounters> AggregatedUserCounters_; // aggregated counters for user in core subsystem
    ui32 PoolId_ = 0;
};

} // namespace NKikimr::NSQS
