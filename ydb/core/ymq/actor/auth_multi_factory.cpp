#include "auth_multi_factory.h"

#include <ydb/core/base/ticket_parser.h>
#include <ydb/core/ymq/actor/cfg.h>
#include <ydb/core/ymq/actor/error.h>
#include <ydb/core/ymq/actor/proxy_actor.h>
#include <ydb/core/ymq/actor/serviceid.h>
#include <ydb/public/sdk/cpp/client/iam/common/iam.h>
#include <ydb/library/aclib/aclib.h>
#include <ydb/library/folder_service/events.h>
#include <ydb/library/ycloud/api/access_service.h>
#include <ydb/library/folder_service/proto/config.pb.h>

#include <library/cpp/logger/global/global.h>

#include <ydb/core/protos/auth.pb.h>

namespace NKikimr::NSQS {

bool UseMockedVersion(const NKikimrConfig::TSqsConfig& config) {
    return config.GetYandexCloudFolderServiceAddress().empty();
}

constexpr TDuration CLOUD_AUTH_TIMEOUT = TDuration::Seconds(30);
constexpr TDuration CLOUD_AUTH_RETRY_PERIOD = TDuration::MilliSeconds(10);
constexpr TDuration CLOUD_AUTH_MAX_RETRY_PERIOD = TDuration::Seconds(5);

constexpr ui64 AUTHENTICATE_WAKEUP_TAG = 1;
constexpr ui64 AUTHORIZATION_WAKEUP_TAG = 2;
constexpr ui64 FOLDER_SERVICE_REQUEST_WAKEUP_TAG = 3;

static const std::pair<EAction, TStringBuf> Action2Permission[] = {
    {EAction::ChangeMessageVisibility, "ymq.messages.changeVisibility"},
    {EAction::ChangeMessageVisibilityBatch, "ymq.messages.changeVisibility"},
    {EAction::CreateQueue, "ymq.queues.create"},
    {EAction::GetQueueAttributes, "ymq.queues.getAttributes"},
    {EAction::GetQueueAttributesBatch, "ymq.queues.getAttributes"},
    {EAction::GetQueueUrl, "ymq.queues.getUrl"},
    {EAction::DeleteMessage, "ymq.messages.delete"},
    {EAction::DeleteMessageBatch, "ymq.messages.delete"},
    {EAction::DeleteQueue, "ymq.queues.delete"},
    {EAction::DeleteQueueBatch, "ymq.queues.delete"},
    {EAction::ListQueues, "ymq.queues.list"},
    {EAction::CountQueues, "ymq.queues.list"},
    {EAction::PurgeQueue, "ymq.queues.purge"},
    {EAction::PurgeQueueBatch, "ymq.queues.purge"},
    {EAction::ReceiveMessage, "ymq.messages.receive"},
    {EAction::SendMessage, "ymq.messages.send"},
    {EAction::SendMessageBatch, "ymq.messages.send"},
    {EAction::SetQueueAttributes, "ymq.queues.setAttributes"},
    {EAction::ListDeadLetterSourceQueues, "ymq.queues.listDeadLetterSourceQueues"},
};

#define ENUMERATE_ACCOUNT_FOLDER_BOUND_ACTIONS(macro) \
    macro(CreateQueue)                                \
    macro(GetQueueUrl)                                \
    macro(ListQueues)

#define ENUMERATE_CUSTOM_UI_BATCH_ACTIONS(macro) \
    macro(CountQueues)                           \
    macro(DeleteQueueBatch)                      \
    macro(GetQueueAttributesBatch)               \
    macro(PurgeQueueBatch)

TString const ActionToPermissionName(const EAction action) {
    static const THashMap<EAction, TStringBuf> perms(std::begin(Action2Permission), std::end(Action2Permission));

    auto pi = perms.find(action);
    if (pi == perms.end()) {
        return {};
    }
    return TString(pi->second);
}

struct TAccessKeySignature {
    TString AccessKeyId;
    TString SignedString;
    TString Signature;
    TString Region;
    TInstant SignedAt;
};

class TCloudAuthRequestProxy
    : public TActorBootstrapped<TCloudAuthRequestProxy>
{
public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::SQS_ACTOR;
    }

    TCloudAuthRequestProxy(TAuthActorData&& data, TString infraToken)
        : RequestHolder_(std::move(data.SQSRequest))
        , Callback_(std::move(data.HTTPCallback))
        , RequestId_(RequestHolder_->GetRequestId())
        , EnableQueueLeader_(data.EnableQueueLeader)
        , PoolId_(data.ExecutorPoolID)
        , Signature_(std::move(data.AWSSignature))
        , Action_(data.Action)
        , IamToken_(data.IAMToken)
        , InfraToken_(infraToken)
        , FolderId_(data.FolderID)
        , CloudId_(data.CloudID)
        , ResourceId_(data.ResourceID)
        , Counters_(*data.Counters)
        , UserSidCallback_(std::move(data.UserSidCallback))
    {
        Y_ABORT_UNLESS(RequestId_);
    }

    TError* MakeMutableError() {
#define SQS_REQUEST_CASE(action) \
       return Response_.Y_CAT(Mutable, action)()->MutableError();

        SQS_SWITCH_REQUEST_CUSTOM(*RequestHolder_, ENUMERATE_ALL_ACTIONS, Y_ABORT_UNLESS(false)); // ALL actions are listed here
#undef SQS_REQUEST_CASE
    }

    void SetError(const TErrorClass& errorClass, const TString& message = TString()) {
        auto* error = MakeMutableError();
        ::NKikimr::NSQS::MakeError(error, errorClass, Sprintf("%s Request id to report: %s.", message.c_str(), RequestId_.c_str()));
    }

    void SendReplyAndDie() {
        Response_.SetFolderId(FolderId_);
        Response_.SetIsFifo(false);
        Response_.SetResourceId(ResourceId_);

        Callback_->DoSendReply(Response_);
        PassAway();
    }

    bool InitAndValidate() {
        PermissionName_ = ActionToPermissionName(Action_);
        if (!PermissionName_) {
            SetError(NErrors::INVALID_ACTION, "This action is disabled in YMQ.");

            return false;
        }

#define SQS_REQUEST_CASE(action) \
        ActionClass_ = EActionClass::AccountFolderBound;

        SQS_SWITCH_REQUEST_CUSTOM(*RequestHolder_, ENUMERATE_ACCOUNT_FOLDER_BOUND_ACTIONS, Y_ABORT_UNLESS(true));
#undef SQS_REQUEST_CASE

#define SQS_REQUEST_CASE(action) \
        ActionClass_ = EActionClass::CustomUIBatch;

        SQS_SWITCH_REQUEST_CUSTOM(*RequestHolder_, ENUMERATE_CUSTOM_UI_BATCH_ACTIONS, Y_ABORT_UNLESS(true));
#undef SQS_REQUEST_CASE

        if (Signature_) {
            AccessKeySignature_.Reset(new TAccessKeySignature);
            AccessKeySignature_->AccessKeyId = Signature_->GetAccessKeyId();
            AccessKeySignature_->SignedString = Signature_->GetStringToSign();
            AccessKeySignature_->Signature = Signature_->GetParsedSignature();
            AccessKeySignature_->Region = Signature_->GetRegion();

            if (Cfg().GetYandexCloudServiceRegion() != AccessKeySignature_->Region) {
                SetError(NErrors::INCOMPLETE_SIGNATURE, Sprintf("Credential should be scoped to a valid region, not '%s'.", AccessKeySignature_->Region.c_str()));
                return false;
            }

            if (TInstant::TryParseIso8601(Signature_->GetSigningTimestamp(), AccessKeySignature_->SignedAt)) {
                return true;
            }
        }

        if (IamToken_ && FolderId_) {
            // UI
            return true;
        }

        SetError(NErrors::ACCESS_DENIED, "Invalid auth parameters.");

        return false;
    }

    STATEFN(ProcessAuthentication) {
        switch (ev->GetTypeRewrite()) {
            hFunc(NCloud::TEvAccessService::TEvAuthenticateResponse, HandleAuthenticationResult);
            hFunc(TEvWakeup, HandleWakeup);
        }
    }

    void GetCloudIdAndAuthorize() {
        Become(&TThis::ProcessAuthorization);
        RequestsToWait_ = 2; // folder service + authorization

        RequestFolderService();
        Authorize();
    }

    static const TErrorClass& GetErrorClass(const NYdbGrpc::TGrpcStatus& status) {
        if (status.InternalError) {
            return NErrors::INTERNAL_FAILURE;
        }

        switch (status.GRpcStatusCode) {
            case grpc::StatusCode::PERMISSION_DENIED:
            case grpc::StatusCode::UNAUTHENTICATED:
                return NErrors::ACCESS_DENIED;
            case grpc::StatusCode::INVALID_ARGUMENT:
                return NErrors::INVALID_CLIENT_TOKEN_ID;
            case grpc::StatusCode::UNAVAILABLE:
                return NErrors::SERVICE_UNAVAILABLE;
            case grpc::StatusCode::DEADLINE_EXCEEDED:
                return NErrors::TIMEOUT;
            default:
                return NErrors::INTERNAL_FAILURE;
        }
    }

    static bool IsTemporaryError(const NYdbGrpc::TGrpcStatus& status) {
        return status.InternalError
            || status.GRpcStatusCode == grpc::StatusCode::UNKNOWN
            || status.GRpcStatusCode == grpc::StatusCode::DEADLINE_EXCEEDED
            || status.GRpcStatusCode == grpc::StatusCode::INTERNAL
            || status.GRpcStatusCode == grpc::StatusCode::UNAVAILABLE;
    }

    bool CanRetry() const {
        return TActivationContext::Now() < StartTime_ + CLOUD_AUTH_TIMEOUT;
    }

    bool CanRetry(const NYdbGrpc::TGrpcStatus& status) const {
        return CanRetry() && IsTemporaryError(status);
    }

    void ScheduleRetry(TDuration& duration, ui64 wakeupTag) {
        Schedule(duration, new TEvWakeup(wakeupTag));

        // Next period
        duration = Min(duration * 2, CLOUD_AUTH_MAX_RETRY_PERIOD);
    }

    void ScheduleAuthorizationRetry() {
        ScheduleRetry(AuthorizeRetryPeriod_, AUTHORIZATION_WAKEUP_TAG);
    }

    void ScheduleAuthenticateRetry() {
        ScheduleRetry(AuthenticateRetryPeriod_, AUTHENTICATE_WAKEUP_TAG);
    }

    void ScheduleFolderServiceRequestRetry() {
        ScheduleRetry(FolderServiceRequestRetryPeriod_, FOLDER_SERVICE_REQUEST_WAKEUP_TAG);
    }

    void HandleAuthenticationResult(NCloud::TEvAccessService::TEvAuthenticateResponse::TPtr& ev) {
        Counters_.IncCounter(NCloudAuth::EActionType::Authenticate,
                             NCloudAuth::ECredentialType::Signature,
                             ev->Get()->Status.GRpcStatusCode);
        auto now = TActivationContext::Now();
        Counters_.AuthenticateDuration->Collect((now - AuthenticateRequestStartTimestamp_).MilliSeconds());

        if (!ev->Get()->Status.Ok()) {
            RLOG_SQS_INFO("Authentication failed. GRpcStatusCode: "
                            << ev->Get()->Status.GRpcStatusCode
                            << ". InternalError: " << ev->Get()->Status.InternalError
                            << ". Message: \"" << ev->Get()->Status.Msg
                            << "\". Proto response: " << ev->Get()->Response);
            if (CanRetry(ev->Get()->Status)) {
                ScheduleAuthenticateRetry();
            } else {
                SetError(GetErrorClass(ev->Get()->Status), "IAM authentication error.");
                SendReplyAndDie();
            }
            return;
        } else if (!ev->Get()->Response.Getsubject().Hasservice_account()) {
            SetError(NErrors::ACCESS_DENIED, "(this error should be unreachable).");
            SendReplyAndDie();
            return;
        }

        FolderId_ = ev->Get()->Response.Getsubject().Getservice_account().Getfolder_id();

        GetCloudIdAndAuthorize();
    }

    STATEFN(ProcessAuthorization) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvTicketParser::TEvAuthorizeTicketResult, HandleAuthorizationResult);
            hFunc(NKikimr::NFolderService::TEvFolderService::TEvGetCloudByFolderResponse, HandleFolderServiceResponse);
            hFunc(TSqsEvents::TEvQueueFolderIdAndCustomName, HandleQueueFolderIdAndCustomName);
            hFunc(TEvWakeup, HandleWakeup);
        }
    }

    void HandleAuthorizationResult(const TEvTicketParser::TEvAuthorizeTicketResult::TPtr& ev) {
        ProcessAuthorizationResult(*ev->Get());
    }

    void ProcessAuthorizationResult(const TEvTicketParser::TEvAuthorizeTicketResult& result) {
        Counters_.IncAuthorizeCounter(
            (AccessKeySignature_ ? NCloudAuth::ECredentialType::Signature : NCloudAuth::ECredentialType::IamToken),
            result.Error
        );
        Counters_.AuthorizeDuration->Collect((TActivationContext::Now() - AuthorizeRequestStartTimestamp_).MilliSeconds());

        if (result.Error) {
            if (CanRetry() && result.Error.Retryable) {
                ScheduleAuthorizationRetry();
            } else {
                RLOG_SQS_INFO("Authorize failed. Error: " << result.Error.ToString());
                SetError(
                    result.Error.Retryable ? NErrors::SERVICE_UNAVAILABLE : NErrors::ACCESS_DENIED,
                    "IAM authorization error."
                );
                SendReplyAndDie();
            }
            return;
        }

        UserSID_ = result.Token->GetUserSID();
        UserSidCallback_(UserSID_);
        OnFinishedRequest();
    }

    void HandleFolderServiceResponse(NKikimr::NFolderService::TEvFolderService::TEvGetCloudByFolderResponse::TPtr& ev) {
        Counters_.IncCounter(NCloudAuth::EActionType::GetCloudId,
                             (AccessKeySignature_ ? NCloudAuth::ECredentialType::Signature : NCloudAuth::ECredentialType::IamToken),
                             ev->Get()->Status.GRpcStatusCode);

        auto now = TActivationContext::Now();
        Counters_.GetFolderIdDuration->Collect((now - FolderServiceRequestStartTimestamp_).MilliSeconds());

        if (!ev->Get()->Status.Ok() || ev->Get()->CloudId.empty()) {
            RLOG_SQS_INFO("Folder service answered with error. GRpcStatusCode: "
                            << ev->Get()->Status.GRpcStatusCode
                            << ". InternalError: " << ev->Get()->Status.InternalError
                            << ". Message: \"" << ev->Get()->Status.Msg);
            if (CanRetry(ev->Get()->Status)) {
                ScheduleFolderServiceRequestRetry();
            } else {
                SetError(GetErrorClass(ev->Get()->Status), "Folder service responded with an error.");
                SendReplyAndDie();
            }
            return;
        }

        CloudId_ = ev->Get()->CloudId;

        OnFinishedRequest();
    }

    void HandleWakeup(TEvWakeup::TPtr& ev) {
        switch (ev->Get()->Tag) {
        case AUTHORIZATION_WAKEUP_TAG:
            Authorize();
            break;
        case AUTHENTICATE_WAKEUP_TAG:
            Authenticate();
            break;
        case FOLDER_SERVICE_REQUEST_WAKEUP_TAG:
            RequestFolderService();
            break;
        default:
            Y_ABORT("Unknown wakeup tag: %lu", ev->Get()->Tag);
        }
    }

    void HandleQueueFolderIdAndCustomName(TSqsEvents::TEvQueueFolderIdAndCustomName::TPtr& ev) {
        if (ev->Get()->Throttled) {
            RLOG_SQS_INFO("Get queue folder id and custom name was throttled.");
            SetError(NErrors::THROTTLING_EXCEPTION, "Too many requests for nonexistent queue");
            SendReplyAndDie();
            return;
        }

        if (ev->Get()->Failed) {
            RLOG_SQS_INFO("Get queue folder id and custom name failed. Failed: " << ev->Get()->Failed << ". Exists: " << ev->Get()->Exists);
            SetError(NErrors::INTERNAL_FAILURE, "Internal folder service error.");
            SendReplyAndDie();
            return;
        }

        if (!ev->Get()->Exists) {
            RLOG_SQS_DEBUG("Get queue folder id and custom name failed: queue info not found");
            SetError(NErrors::ACCESS_DENIED, "Folder service error."); // do not expose valid queue urls
            SendReplyAndDie();
            return;
        }

        FolderId_ = ev->Get()->QueueFolderId;
        RequestsToWait_ = 1; // just authorization

        Authorize();
    }

    void OnFinishedRequest() {
        if (--RequestsToWait_ == 0) {
            OnSuccessfulAuth();
        }
    }

    template<typename TSignatureProto>
    void FillSignatureProto(TSignatureProto& signature) const {
        signature.set_access_key_id(AccessKeySignature_->AccessKeyId);
        signature.set_string_to_sign(AccessKeySignature_->SignedString);
        signature.set_signature(AccessKeySignature_->Signature);

        auto& v4params = *signature.mutable_v4_parameters();
        v4params.set_service("sqs");
        v4params.set_region(AccessKeySignature_->Region);

        const ui64 nanos = AccessKeySignature_->SignedAt.NanoSeconds();
        const ui64 seconds = nanos / 1000000000ull;
        const ui64 nanos_left = nanos % 1000000000ull;

        v4params.mutable_signed_at()->set_seconds(seconds);
        v4params.mutable_signed_at()->set_nanos(nanos_left);
    }

    void Authenticate() {
        THolder<NCloud::TEvAccessService::TEvAuthenticateRequest> request = MakeHolder<NCloud::TEvAccessService::TEvAuthenticateRequest>();
        request->RequestId = RequestId_;
        FillSignatureProto(*request->Request.mutable_signature());

        AuthenticateRequestStartTimestamp_ = TActivationContext::Now();
        Send(MakeSqsAccessServiceID(), std::move(request));
    }


    void Authorize() {
        TVector<std::pair<TString, TString>> attributes;
        attributes.emplace_back("folder_id", FolderId_);

        TVector<TEvTicketParser::TEvAuthorizeTicket::TEntry>  entries{{{PermissionName_}, attributes}};

        THolder<TEvTicketParser::TEvAuthorizeTicket> request;
        if (AccessKeySignature_) {
            TEvTicketParser::TEvAuthorizeTicket::TAccessKeySignature signature;
            signature.AccessKeyId = AccessKeySignature_->AccessKeyId;
            signature.StringToSign = AccessKeySignature_->SignedString;
            signature.Signature = AccessKeySignature_->Signature;
            signature.Service = "sqs";
            signature.Region = AccessKeySignature_->Region;
            signature.SignedAt = AccessKeySignature_->SignedAt;
            request = MakeHolder<TEvTicketParser::TEvAuthorizeTicket>(std::move(signature), "", entries);
        } else {
            request = MakeHolder<TEvTicketParser::TEvAuthorizeTicket>(IamToken_, "", entries);
        }

        AuthorizeRequestStartTimestamp_ = TActivationContext::Now();
        if (!UseMockedVersion(Cfg())) {
            Send(MakeTicketParserID(), request.Release());
        } else {
            TEvTicketParser::TEvAuthorizeTicketResult result("fake_token", nullptr);
            if (AccessKeySignature_ && AccessKeySignature_->AccessKeyId.empty()) {
                result.Error.Message = "mocked_auth_error: empty access key";
                result.Error.Retryable = false;
            } else if (AccessKeySignature_ && AccessKeySignature_->AccessKeyId == "TEST_ID_FOR_RETRYIES") {
                result.Error.Message = "mocked_auth_error: correct process retries";
                result.Error.Retryable = true;
            } else {
                result.Token = MakeIntrusive<NACLib::TUserToken>("fake_user_sid@as", TVector<TString>());
            }
            ProcessAuthorizationResult(result);
        }

    }

    void RequestFolderService() {
        FolderServiceRequestStartTimestamp_ = TActivationContext::Now();
        auto request = MakeHolder<NFolderService::TEvFolderService::TEvGetCloudByFolderRequest>();
        request.Get()->FolderId = FolderId_;
        request.Get()->RequestId = RequestId_;
        request.Get()->Token = InfraToken_;
        Send(MakeSqsFolderServiceID(), std::move(request));
    }

    void RetrieveCachedFolderId() {
        Become(&TThis::ProcessAuthorization);

        Send(MakeSqsServiceID(SelfId().NodeId()), new TSqsEvents::TEvGetQueueFolderIdAndCustomName(RequestId_, CloudId_, ResourceId_));
    }

    template<typename TProto>
    void ProposeStaticCreds(TProto& req) {
        // dirty hack, but it allows to proceed
        // TODO: refactor
        req.ClearCredentials();
        req.MutableCredentials()->SetStaticCreds(TString::Join(CloudId_, ":", FolderId_));
        req.MutableAuth()->SetUserName(CloudId_);
        req.MutableAuth()->SetFolderId(FolderId_);
        req.MutableAuth()->SetUserSID(UserSID_);
    }

    void OnSuccessfulAuth() {
#define SQS_REQUEST_CASE(action) \
        ProposeStaticCreds(*RequestHolder_->Y_CAT(Mutable, action)());

        SQS_SWITCH_REQUEST_CUSTOM(*RequestHolder_, ENUMERATE_ALL_ACTIONS, Y_ABORT_UNLESS(false));
#undef SQS_REQUEST_CASE

        Register(CreateProxyActionActor(*RequestHolder_, std::move(Callback_), EnableQueueLeader_), NActors::TMailboxType::HTSwap, PoolId_);
        PassAway();
    }

    void Bootstrap() {
        StartTime_ = TActivationContext::Now();
        if (!InitAndValidate()) {
            // error was set in InitAndValidate method
            SendReplyAndDie();
            return;
        }

        if (AccessKeySignature_) {
            switch (ActionClass_) {
                case EActionClass::AccountFolderBound: {
                    Become(&TThis::ProcessAuthentication);
                    Authenticate();
                    return;
                }
                case EActionClass::QueueSpecified: {
                    RetrieveCachedFolderId();
                    return;
                }
                case EActionClass::CustomUIBatch: {
                    SetError(NErrors::INVALID_ACTION, "This action is disabled.");
                    SendReplyAndDie();
                    return;
                }
            }
        } else {
            GetCloudIdAndAuthorize();
        }
    }

private:
    enum class EActionClass {
        AccountFolderBound,
        QueueSpecified,
        CustomUIBatch
    };

    THolder<NKikimrClient::TSqsRequest> RequestHolder_;
    THolder<IReplyCallback> Callback_;
    const TString RequestId_;
    const bool EnableQueueLeader_;
    const ui32 PoolId_;
    THolder<TAwsRequestSignV4> Signature_; // calculated outside
    THolder<TAccessKeySignature> AccessKeySignature_; // for access service actor only
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

void TMultiAuthFactory::Initialize(
    NActors::TActorSystemSetup::TLocalServices& services,
    const TAppData& appData,
    const NKikimrConfig::TSqsConfig& config)
{
    if (!config.GetYandexCloudMode()) {
        IsYandexCloudMode_ = false;
        AuthFactory_.Initialize(services, appData, config);
        return;
    }

    IsYandexCloudMode_ = true;
    CredentialsProvider_ = CreateCredentialsProviderFactory(config)->CreateProvider();

    const auto& rootCAPath = appData.AuthConfig.GetPathToRootCA();

    auto setupActor = [executorPoolID = appData.UserPoolId](IActor* const actor) {
        return TActorSetupCmd(actor, TMailboxType::HTSwap, executorPoolID);
    };

    IActor* const accessService = CreateSqsAccessService(
        config.GetYandexCloudAccessServiceAddress(),
        rootCAPath);

    services.emplace_back(MakeSqsAccessServiceID(), setupActor(accessService));

    IActor* folderService = nullptr;

    if (!UseMockedVersion(config)) {
        auto accessServiceAddr = config.GetYandexCloudFolderServiceAddress();
        auto resourceManagerAddr = config.GetYandexCloudResourceManagerServiceAddress();

        NKikimrProto::NFolderService::TFolderServiceConfig folderServiceConfig;
        folderServiceConfig.set_enable(true);
        if(!resourceManagerAddr.empty()) {
            UseResourceManagerFolderService_ = true;
            folderServiceConfig.SetResourceManagerEndpoint(resourceManagerAddr);
        } else {
            folderServiceConfig.SetEndpoint(accessServiceAddr);
        }
        folderServiceConfig.SetPathToRootCA(rootCAPath);
        folderService = appData.FolderServiceFactory(folderServiceConfig);
    } else {
        folderService = CreateMockSqsFolderService();
    }

    services.emplace_back(MakeSqsFolderServiceID(), setupActor(folderService));

    if (auto path = config.GetMeteringLogFilePath())
        DoInitGlobalLog(
            CreateOwningThreadedLogBackend(path, 0));

    IActor* const meteringService = CreateSqsMeteringService();

    services.emplace_back(MakeSqsMeteringServiceID(), setupActor(meteringService));
}

void TMultiAuthFactory::RegisterAuthActor(NActors::TActorSystem& system, TAuthActorData&& data)
{
    if (!IsYandexCloudMode_) {
        AuthFactory_.RegisterAuthActor(system, std::move(data));
        return;
    }

    const ui32 poolID = data.ExecutorPoolID;
    system.Register(                                                        //token needed only for ResourceManager
        new TCloudAuthRequestProxy(std::move(data), UseResourceManagerFolderService_ ? CredentialsProvider_->GetAuthInfo() : ""),
        NActors::TMailboxType::HTSwap,
        poolID);
}

TMultiAuthFactory::TCredentialsFactoryPtr
TMultiAuthFactory::CreateCredentialsProviderFactory(const NKikimrConfig::TSqsConfig& config)
{
    if (!config.HasAuthConfig())
        return AuthFactory_.CreateCredentialsProviderFactory(config);

    const auto& authCfg = config.GetAuthConfig();

    if (authCfg.LocalAuthConfig_case() != TSqsConfig::TYdbAuthConfig::kJwt)
        return AuthFactory_.CreateCredentialsProviderFactory(config);

    const auto& jwt = authCfg.GetJwt();

    NYdb::TIamJwtFilename params = {.JwtFilename = jwt.GetJwtFile()};

    if (jwt.HasIamEndpoint())
        if (TString endpoint = jwt.GetIamEndpoint(); !endpoint.empty())
            params.Endpoint = std::move(endpoint);

    return NYdb::CreateIamJwtFileCredentialsProviderFactory(std::move(params));
}
} // namespace NKikimr::NSQS
