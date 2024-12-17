#pragma once

#include <ydb/core/base/ticket_parser.h>
#include "ydb/core/protos/sqs.pb.h"
#include "ydb/library/grpc/client/grpc_client_low.h"
#include "ydb/library/http_proxy/error/error.h"
#include <ydb/core/ymq/actor/auth_factory.h>
#include <ydb/library/ycloud/api/access_service.h>
#include <ydb/library/folder_service/proto/config.pb.h>
#include <ydb/library/folder_service/events.h>
#include <ydb/core/ymq/actor/events.h>

#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>

#include <ydb/core/http_proxy/events.h>

namespace NKikimr::NSQS {

constexpr TDuration CLOUD_AUTH_TIMEOUT = TDuration::Seconds(30);
constexpr TDuration CLOUD_AUTH_RETRY_PERIOD = TDuration::MilliSeconds(10);
constexpr TDuration CLOUD_AUTH_MAX_RETRY_PERIOD = TDuration::Seconds(5);

constexpr ui64 AUTHENTICATE_WAKEUP_TAG = 1;
constexpr ui64 AUTHORIZATION_WAKEUP_TAG = 2;
constexpr ui64 FOLDER_SERVICE_REQUEST_WAKEUP_TAG = 3;

struct TAccessKeySignature {
    TString AccessKeyId;
    TString SignedString;
    TString Signature;
    TString Region;
    TInstant SignedAt;
};

class TMultiAuthFactory : public IAuthFactory {
public:
    void Initialize(
        NActors::TActorSystemSetup::TLocalServices& services,
        const TAppData& appData,
        const TSqsConfig& config) final;

    void RegisterAuthActor(NActors::TActorSystem& system, TAuthActorData&& data) final;

    TCredentialsFactoryPtr CreateCredentialsProviderFactory(const TSqsConfig& config);

private:
    bool IsYandexCloudMode_ {false};
    TAuthFactory AuthFactory_ {};
    NYdb::TCredentialsProviderPtr CredentialsProvider_;
    bool UseResourceManagerFolderService_ {false};
};

class TBaseCloudAuthRequestProxy : public TActorBootstrapped<TBaseCloudAuthRequestProxy> {
public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::SQS_ACTOR;
    }

    TBaseCloudAuthRequestProxy(TAuthActorData&& data, TString infraToken)
        : RequestHolder_(std::move(data.SQSRequest))
        , Callback_(std::move(data.HTTPCallback))
        , RequestId_(RequestHolder_->GetRequestId())
        , EnableQueueLeader_(data.EnableQueueLeader)
        , PoolId_(data.ExecutorPoolID)
        , Signature_(std::move(data.AWSSignature))
        , Action_(std::move(data.Action))
        , IamToken_(std::move(data.IAMToken))
        , InfraToken_(std::move(infraToken))
        , FolderId_(std::move(data.FolderID))
        , CloudId_(std::move(data.CloudID))
        , ResourceId_(std::move(data.ResourceID))
        , Counters_(*data.Counters)
        , UserSidCallback_(std::move(data.UserSidCallback))
    {
        Y_ABORT_UNLESS(RequestId_);
    }

    virtual ~TBaseCloudAuthRequestProxy() = default;

    TError* MakeMutableError();
    void SendReplyAndDie();
    bool InitAndValidate();
    void GetCloudIdAndAuthorize();
    static const TErrorClass& GetErrorClass(const NYdbGrpc::TGrpcStatus& status);
    static bool IsTemporaryError(const NYdbGrpc::TGrpcStatus& status);
    bool CanRetry() const;
    bool CanRetry(const NYdbGrpc::TGrpcStatus& status) const;
    void ScheduleRetry(TDuration& duration, ui64 wakeupTag);
    void ScheduleAuthorizationRetry();
    void ScheduleAuthenticateRetry();
    void ScheduleFolderServiceRequestRetry();
    void HandleAuthenticationResult(NCloud::TEvAccessService::TEvAuthenticateResponse::TPtr& ev);
    void HandleAuthorizationResult(const TEvTicketParser::TEvAuthorizeTicketResult::TPtr& ev);
    void ProcessAuthorizationResult(const TEvTicketParser::TEvAuthorizeTicketResult& result);
    void HandleFolderServiceResponse(NKikimr::NFolderService::TEvFolderService::TEvGetCloudByFolderResponse::TPtr& ev);
    void HandleWakeup(TEvWakeup::TPtr& ev);
    void HandleQueueFolderIdAndCustomName(TSqsEvents::TEvQueueFolderIdAndCustomName::TPtr& ev);
    void OnFinishedRequest();

    template<typename TSignatureProto>
    void FillSignatureProto(TSignatureProto& signature) const;

    void ProcessAuthentication(TAutoPtr<::NActors ::IEventHandle> &ev);
    void ProcessAuthorization(TAutoPtr<::NActors::IEventHandle> &ev);
    void Authenticate();
    void Authorize();
    void RequestFolderService();
    void RetrieveCachedFolderId();

    template<typename TProto>
    void ProposeStaticCreds(TProto& req);
    void Bootstrap();

protected:
    enum class EActionClass {
        AccountFolderBound,
        QueueSpecified,
        CustomUIBatch
    };

    virtual void DoReply() = 0;
    virtual void SetError(const TErrorClass& errorClass, const TString& message = TString()) = 0;

    virtual void OnSuccessfulAuth() = 0;
    virtual void ChangeCounters(std::function<void()> func) = 0;

    THolder<NKikimrClient::TSqsRequest> RequestHolder_;
    THolder<IReplyCallback> Callback_;
    const TString RequestId_;
    const bool EnableQueueLeader_;
    const ui32 PoolId_;
    THolder<TAwsRequestSignV4> Signature_;
    THolder<TAccessKeySignature> AccessKeySignature_;
    const EAction Action_;
    TString PermissionName_;
    TString IamToken_;
    TString InfraToken_;
    TString FolderId_;
    TString CloudId_;
    TString UserSID_;
    TString ResourceId_;
    ui32 RequestsToWait_ = 0;
    EActionClass ActionClass_ = EActionClass::QueueSpecified;
    TCloudAuthCounters& Counters_;
    TInstant AuthenticateRequestStartTimestamp_;
    TInstant AuthorizeRequestStartTimestamp_;
    TInstant FolderServiceRequestStartTimestamp_;
    TInstant StartTime_;

    TDuration AuthenticateRetryPeriod_ = CLOUD_AUTH_RETRY_PERIOD;
    TDuration AuthorizeRetryPeriod_ = CLOUD_AUTH_RETRY_PERIOD;
    TDuration FolderServiceRequestRetryPeriod_ = CLOUD_AUTH_RETRY_PERIOD;

    NKikimrClient::TSqsResponse Response_;

    std::function<void(TString)> UserSidCallback_;
};

class TCloudAuthRequestProxy : public TBaseCloudAuthRequestProxy {
public:
    using TBaseCloudAuthRequestProxy::TBaseCloudAuthRequestProxy;
protected:
    void DoReply() override;
    void SetError(const TErrorClass& errorClass, const TString& message = TString()) override;
    void OnSuccessfulAuth() override;
    void ChangeCounters(std::function<void()> func) override;
};

class THttpProxyAuthRequestProxy : public TBaseCloudAuthRequestProxy {
public:
    THttpProxyAuthRequestProxy(TAuthActorData&& data, TString infraToken, TActorId requester)
        : TBaseCloudAuthRequestProxy(std::move(data), std::move(infraToken))
        , Requester_(std::move(requester))
    {
        Y_ABORT_UNLESS(RequestId_);
    }
protected:
    void DoReply() override;
    void SetError(const TErrorClass& errorClass, const TString& message = TString()) override;
    void OnSuccessfulAuth() override;
    void ChangeCounters(std::function<void()> func) override;
private:
    TActorId Requester_;
    TMaybe<NHttpProxy::TEvYmqCloudAuthResponse::TError> Error_;
};
}
