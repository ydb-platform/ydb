#pragma once
#include "defs.h"

#include "actor.h"
#include "cfg.h"
#include "error.h"
#include "events.h"
#include "limits.h"
#include "log.h"
#include "proxy_actor.h"
#include "serviceid.h"
#include "schema.h"

#include <ydb/core/audit/audit_log.h>
#include <ydb/core/base/path.h>
#include <ydb/core/base/ticket_parser.h>
#include <ydb/core/quoter/public/quoter.h>
#include <ydb/core/protos/msgbus.pb.h>
#include <ydb/core/ymq/base/action.h>
#include <ydb/core/ymq/base/acl.h>
#include <ydb/core/ymq/base/counters.h>
#include <ydb/core/ymq/base/query_id.h>
#include <ydb/core/ymq/base/security.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>

#include <util/folder/path.h>
#include <util/generic/guid.h>
#include <util/generic/is_in.h>
#include <util/string/ascii.h>
#include <util/string/join.h>

namespace NKikimr::NSQS {

template <typename TDerived>
class TActionActor
    : public TActorBootstrapped<TDerived>
{
public:
    TActionActor(const NKikimrClient::TSqsRequest& sourceSqsRequest, EAction action, THolder<IReplyCallback> cb)
        : Action_(action)
        , RequestId_(sourceSqsRequest.GetRequestId())
        , Cb_(std::move(cb))
        , Shards_(1)
        , SourceSqsRequest_(sourceSqsRequest)
    {
        Y_ABORT_UNLESS(RequestId_);
    }

    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::SQS_ACTOR;
    }

    static constexpr bool NeedQueueAttributes() { // override it in TDerived if needed
        return false;
    }

    // For queue requests
    static constexpr bool NeedExistingQueue() {
        return true;
    }

    static constexpr bool CreateMissingAccount() {
        return false;
    }

    static constexpr bool NeedUserSpecified() {
        return true;
    }

    void DoBootstrap() {
        ui64 configurationFlags = 0;
        if (TDerived::NeedQueueAttributes()) {
            configurationFlags |= TSqsEvents::TEvGetConfiguration::EFlags::NeedQueueAttributes;
        }
        if (TProxyActor::NeedCreateProxyActor(Action_)) {
            configurationFlags |= TSqsEvents::TEvGetConfiguration::EFlags::NeedQueueLeader;
        }
        bool enableThrottling = (Action_ != EAction::CreateQueue);
        this->Send(MakeSqsServiceID(this->SelfId().NodeId()),
            MakeHolder<TSqsEvents::TEvGetConfiguration>(
                RequestId_,
                UserName_,
                GetQueueName(),
                FolderId_,
                enableThrottling,
                configurationFlags)
        );
    }

    void CreateAccountOnTheFly() const {
        // TODO: move to separate actor
        this->Register(
            new TCreateUserSchemaActor(Cfg().GetRoot(), UserName_, this->SelfId(), RequestId_, UserCounters_)
        );
    }

    void HandleAccountCreated(TSqsEvents::TEvUserCreated::TPtr& ev) {
        auto* detailedCounters = UserCounters_ ? UserCounters_->GetDetailedCounters() : nullptr;
        if (ev->Get()->Success) {
            INC_COUNTER(detailedCounters, CreateAccountOnTheFly_Success);
        } else {
            RLOG_SQS_ERROR("Failed to create cloud account on the fly. Account name: " << UserName_);
            MakeError(MutableErrorDesc(), NErrors::INTERNAL_FAILURE);
            INC_COUNTER(detailedCounters, CreateAccountOnTheFly_Errors);
            SendReplyAndDie();
            return;
        }

        DoBootstrap();
    }

    void Bootstrap(const NActors::TActorContext&) {    
        #define SQS_REQUEST_CASE(action)                                        \
            const auto& request = SourceSqsRequest_.Y_CAT(Get, action)();       \
            auto response = Response_.Y_CAT(Mutable, action)();                 \
            FillAuthInformation(request);                                       \
            response->SetRequestId(RequestId_);
            
        SQS_SWITCH_REQUEST_CUSTOM(SourceSqsRequest_, ENUMERATE_ALL_ACTIONS, Y_ABORT_UNLESS(false));
        #undef SQS_REQUEST_CASE 

        RLOG_SQS_DEBUG("Request started. Actor: " << this->SelfId()); // log new request id
        StartTs_ = TActivationContext::Now();

        const auto& cfg = Cfg();

        this->Become(&TActionActor::InitialState);

        // Set timeout
        if (cfg.GetRequestTimeoutMs()) {
            this->Schedule(TDuration::MilliSeconds(cfg.GetRequestTimeoutMs()), new TEvWakeup(REQUEST_TIMEOUT_WAKEUP_TAG), TimeoutCookie_.Get());
        }

        if (IsCloud()) {
            if (!FolderId_) {
                MakeError(MutableErrorDesc(), NErrors::INVALID_CLIENT_TOKEN_ID, "Failed to parse cloud_id/folder_id.");
                SendReplyAndDie();
                return;
            }

            if (TDerived::CreateMissingAccount()) {
                CreateAccountOnTheFly();

                return;
            }
        }

        DoBootstrap();
    }

protected:
    virtual void DoAction() = 0;

    virtual TError* MutableErrorDesc() = 0;

    virtual TString DoGetQueueName() const = 0;

    virtual bool Validate() {
        if (TDerived::NeedUserSpecified() && !UserName_) {
            MakeError(MutableErrorDesc(), NErrors::ACCESS_DENIED, "No account name.");
        }
        return DoValidate();
    }

    virtual bool DoValidate() {
        return true;
    }

    virtual bool IsFifoQueue() const {
        Y_ABORT_UNLESS(IsFifo_);
        return *IsFifo_;
    }

    virtual bool TablesFormat() const {
        Y_ABORT_UNLESS(TablesFormat_);
        return *TablesFormat_;
    }

    virtual void DoStart() { }

    virtual void DoFinish() { }

    virtual TString DumpState() {
        TStringBuilder ret;
        ret << "SecurityCheckRequestsToWaitFor: " << SecurityCheckRequestsToWaitFor_
            << " Shards: " << Shards_
            << " SchemeCache: " << SchemeCache_;
        if (QueueAttributes_) {
            ret << " QueueAttributes: " << *QueueAttributes_;
        }
        ret << "Response: " << Response_;
        return std::move(ret);
    }

    virtual bool HandleWakeup(TEvWakeup::TPtr& ev) {
        if (ev->Get()->Tag == REQUEST_TIMEOUT_WAKEUP_TAG) {
            HandleRequestTimeout();
            return true;
        }
        return false;
    }

    virtual void OnRequestTimeout() {
    }

    void HandleRequestTimeout() {
        OnRequestTimeout();

        RLOG_SQS_ERROR("Request timeout. User [" << UserName_ << "] Queue [" << GetQueueName() << "] Action [" << Action_ << "]. State: { " << DumpState() << " }");
        if (QueueCounters_) {
            INC_COUNTER_COUPLE(QueueCounters_, RequestTimeouts, request_timeouts_count_per_second);
        } else if (UserCounters_) {
            INC_COUNTER(UserCounters_, RequestTimeouts);
        } else {
            TIntrusivePtrCntrCouple rootCounters{
                    SqsCoreCounters_ ? SqsCoreCounters_ : GetSqsServiceCounters(AppData()->Counters, "core"),
                    GetYmqPublicCounters(AppData()->Counters)
            };
            auto[userCounters, queueCounters] = GetUserAndQueueCounters(rootCounters,
                                                                        TQueuePath(Cfg().GetRoot(), UserName_,
                                                                                   GetQueueName()));
            if (queueCounters.SqsCounters) {
                queueCounters.SqsCounters->GetCounter("RequestTimeouts", true)->Inc();
            } else if (userCounters.SqsCounters) {
                userCounters.SqsCounters->GetCounter("RequestTimeouts", true)->Inc();
            }
        }

        MakeError(MutableErrorDesc(), NErrors::TIMEOUT);
        SendReplyAndDie();
    }

    TString GetQueueName() const {
        return DoGetQueueName();
    }

    TQueuePath GetQueuePath() const {
        const TString root = Cfg().GetRoot();
        return TQueuePath(root, UserName_, DoGetQueueName());
    }

    TQueuePath GetUserPath() const {
        const TString root = Cfg().GetRoot();
        return TQueuePath(root, UserName_, TString());
    }

    TString MakeQueueUrl(const TString& name) const {
        return Join("/", RootUrl_, UserName_, name);
    }

    void SendReplyAndDie() {
        RLOG_SQS_TRACE("SendReplyAndDie from action actor " << Response_);
        auto* detailedCounters = UserCounters_ ? UserCounters_->GetDetailedCounters() : nullptr;
        const size_t errors = ErrorsCount(Response_, detailedCounters ? &detailedCounters->APIStatuses : nullptr);

        FinishTs_ = TActivationContext::Now();

        const TDuration duration = GetRequestDuration();
        const TDuration workingDuration = GetRequestWorkingDuration();
        if (QueueLeader_ && (IsActionForQueue(Action_) || IsActionForQueueYMQ(Action_))) {
            auto counterChangedEvent = MakeHolder<TSqsEvents::TEvActionCounterChanged>();
            counterChangedEvent->Record.set_action(Action_);
            counterChangedEvent->Record.set_durationms(duration.MilliSeconds());
            counterChangedEvent->Record.set_workingdurationms(workingDuration.MilliSeconds());
            counterChangedEvent->Record.set_errorscount(errors);
            this->Send(QueueLeader_, counterChangedEvent.Release());
        } else if (UserCounters_ && UserCounters_->NeedToShowDetailedCounters()) {
            TCountersCouple<TActionCounters*> userCounters{nullptr, nullptr};
            if (IsActionForUser(Action_)) {
                userCounters.SqsCounters = &UserCounters_->SqsActionCounters[Action_];
                if (errors) {
                    ADD_COUNTER(userCounters.SqsCounters, Errors, errors);
                } else {
                    INC_COUNTER(userCounters.SqsCounters, Success);
                }
            }
            if (IsActionForUserYMQ(Action_)) {
                userCounters.YmqCounters = &UserCounters_->YmqActionCounters[Action_];
                if (errors) {
                    ADD_COUNTER(userCounters.YmqCounters, Errors, errors);
                } else {
                    INC_COUNTER(userCounters.YmqCounters, Success);
                }
            }

            if (userCounters.Defined()) {
                COLLECT_HISTOGRAM_COUNTER_COUPLE(userCounters, Duration, duration.MilliSeconds());
                RLOG_SQS_DEBUG("Request " << Action_ << " working duration: " << workingDuration.MilliSeconds() << "ms");
                COLLECT_HISTOGRAM_COUNTER_COUPLE(userCounters, WorkingDuration, workingDuration.MilliSeconds());
            }
        }

        if (IsRequestSlow()) {
            PrintSlowRequestWarning();
        }
        Finish();

        if (Cfg().GetYandexCloudMode()) {
            Response_.SetFolderId(FolderId_);
            Response_.SetIsFifo(IsFifo_ ? *IsFifo_ : false);
            Response_.SetResourceId(GetQueueName());
        }

        AuditLog();

        Cb_->DoSendReply(Response_);
        PassAway();
    }

    void AuditLog() {
        if (IsModifySchemaRequest(SourceSqsRequest_)) {
            #define RESPONSE_CASE(action)                                       \
                case NKikimrClient::TSqsResponse::Y_CAT(k, action): {           \
                    AuditLogEntry(Response_.Y_CAT(Get, action)(), RequestId_);  \
                    break;                                                      \
                }

            #define RESPONSE_BATCH_CASE(action)                                                 \
                case NKikimrClient::TSqsResponse::Y_CAT(k, action): {                           \
                    const auto& resp = Response_.Y_CAT(Get, action)();                          \
                    const TError* globalError = resp.HasError() ? &resp.GetError() : nullptr;   \
                    for (size_t i = 0; i < resp.EntriesSize(); ++i) {                           \
                        TString reqId = TStringBuilder() << RequestId_ << "_" << i;             \
                        AuditLogEntry(resp.GetEntries()[i], reqId, globalError);                \
                    }                                                                           \
                    break;                                                                      \
                }

            switch (Response_.GetResponseCase()) {
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
    }

private:
    void AuditLogEntryImpl(const TString& requestId, const TError* error = nullptr) {
        static const TString EmptyValue = "{none}";
        AUDIT_LOG(
            AUDIT_PART("component", TString("ymq"))
            AUDIT_PART("request_id", requestId)
            AUDIT_PART("subject", (UserSID_ ? UserSID_ : EmptyValue))
            AUDIT_PART("account", UserName_)
            AUDIT_PART("cloud_id", UserName_, Cfg().GetYandexCloudMode())
            AUDIT_PART("folder_id", Response_.GetFolderId(), Cfg().GetYandexCloudMode())
            AUDIT_PART("resource_id", GetQueueName(), Cfg().GetYandexCloudMode())
            AUDIT_PART("operation", ActionToCloudConvMethod(Action_))
            AUDIT_PART("queue", GetQueueName())
            AUDIT_PART("status", TString(error ? "ERROR": "SUCCESS"))
            AUDIT_PART("reason", error->GetMessage(), error)
            AUDIT_PART("detailed_status", error->GetErrorCode(), error)
        );
    }

protected: 
    template <class TResponse>
    void AuditLogEntry(const TResponse& response, const TString& requestId, const TError* error = nullptr) {
        if (!error && response.HasError()) {
            error = &response.GetError();
        }
        AuditLogEntryImpl(requestId, error);
    }

    void PassAway() {
        if (TProxyActor::NeedCreateProxyActor(Action_)) {
            if (TString queueName = GetQueueName()) {
                this->Send(MakeSqsServiceID(this->SelfId().NodeId()), new TSqsEvents::TEvQueueLeaderDecRef());
            }
        }
        if (StartRequestWasCalled_ != FinishRequestWasCalled_) {
            RLOG_SQS_WARN("Start/Finish calls inconsistency. Start: " << StartRequestWasCalled_ << ", Finish: " << FinishRequestWasCalled_);
        }
        TActorBootstrapped<TDerived>::PassAway();
    }

    void DoRoutine() {
        RLOG_SQS_TRACE("DoRoutine");
        Start();
        if (Validate()) {
            DoAction();
        } else {
            SendReplyAndDie();
        }
    }

    // Duration of request
    virtual TDuration GetRequestDuration() const {
        return FinishTs_ - StartTs_;
    }

    // Duration of sleeps (waits) in request
    virtual TDuration GetRequestWaitDuration() const {
        return TDuration::Zero();
    }

    // Duration of request without sleeps
    virtual TDuration GetRequestWorkingDuration() const {
        return GetRequestDuration() - GetRequestWaitDuration();
    }

    virtual TString GetCustomACLPath() const {
        return GetQueuePath().GetQueuePath();
    }

    virtual bool IsRequestSlow() const {
        return GetRequestWorkingDuration() >= TDuration::MilliSeconds(Cfg().GetSlowRequestTimeMs());
    }

    void PrintSlowRequestWarning() {
        RLOG_SQS_INFO("Request [" << UserName_ << "] [" << GetQueueName() << "] [" << Action_ << "] is slow. Working duration: " << GetRequestWorkingDuration().MilliSeconds() << "ms");
    }

    TString SanitizeNodePath(const TString& path) const {
        TStringBuf sanitizedPath(path);
        // just skip SQS root path if there's such a prefix
        if (sanitizedPath.SkipPrefix(TStringBuf(Cfg().GetRoot()))) { // always skip SQS root prefix
            return TString(sanitizedPath);
        } else {
            Y_ABORT_UNLESS(false); // should never be applied in any other way
        }

        return {};
    }

    TString MakeAbsolutePath(const TString& relativePath) const {
        TStringBuilder fullPath;
        fullPath << Cfg().GetRoot();
        if (!relativePath.StartsWith("/")) {
            fullPath << "/";
        }
        fullPath << relativePath;

        return TString(fullPath);
    }

    size_t CalculatePathDepth(const TString& path) const {
        const TString sanitizedResource = TFsPath(path).Fix().GetPath();
        size_t count = 0;
        for (size_t i = 0, sz = sanitizedResource.size(); i < sz; ++i) {
            if (sanitizedResource[i] == '/') {
                ++count;
            }
        }

        return count;
    }

    bool IsCloud() const {
        return Cfg().GetYandexCloudMode();
    }

    bool IsInternalResource(const TString& path) const {
        return CalculatePathDepth(SanitizeNodePath(path)) > 2;
    }

    bool IsForbiddenPath(const TString& path) const {
        return path.Contains("..") || path.Contains("//") || IsInternalResource(path);
    }
    struct TActionCountersPack {
        TActionCounters* CoreCounters = nullptr;
        TActionCounters* YmqCounters = nullptr;
    };

    TCountersCouple<TActionCounters*> GetActionCounters() const {
        TCountersCouple<TActionCounters*> result{nullptr, nullptr};
        if (IsActionForQueue(Action_) && QueueCounters_) {
            if (IsActionForMessage(Action_) || QueueCounters_->NeedToShowDetailedCounters()) {
                result.SqsCounters = &QueueCounters_->SqsActionCounters[Action_];
            }
        } else if (IsActionForUser(Action_) && UserCounters_) {
            if (UserCounters_->NeedToShowDetailedCounters()) {
                result.SqsCounters = &UserCounters_->SqsActionCounters[Action_];
            }
        }
        if (IsActionForQueueYMQ(Action_) && QueueCounters_) {
            if (IsActionForMessage(Action_) || QueueCounters_->NeedToShowDetailedCounters()) {
                result.YmqCounters = &QueueCounters_->YmqActionCounters[Action_];
            }
        } else if (IsActionForUserYMQ(Action_) && UserCounters_) {
            if (UserCounters_->NeedToShowDetailedCounters()) {
                result.YmqCounters = &UserCounters_->YmqActionCounters[Action_];
            }
        }
        return result;
    }

    void RequestSchemeCache(const TString& path) {
        auto schemeCacheRequest = MakeHolder<NSchemeCache::TSchemeCacheNavigate>();
        NSchemeCache::TSchemeCacheNavigate::TEntry entry;

        entry.Path = SplitPath(path);
        entry.Operation = NSchemeCache::TSchemeCacheNavigate::OpPath;
        schemeCacheRequest->ResultSet.emplace_back(entry);

        this->Send(SchemeCache_, new TEvTxProxySchemeCache::TEvNavigateKeySet(schemeCacheRequest.Release()));
    }

private:
    STATEFN(InitialState) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TSqsEvents::TEvConfiguration, HandleConfiguration);
            hFunc(TSqsEvents::TEvUserCreated, HandleAccountCreated);
            hFunc(TEvWakeup, HandleWakeup);
        }
    }

    STATEFN(WaitAuthCheckMessages) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvTxProxySchemeCache::TEvNavigateKeySetResult, HandleSchemeCacheResponse);
            hFunc(TEvTicketParser::TEvAuthorizeTicketResult, HandleTicketParserResponse);
            hFunc(TEvWakeup, HandleWakeup);
        }
    }

    STATEFN(WaitQuotaState) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvQuota::TEvClearance, HandleQuota);
            hFunc(TEvWakeup, HandleWakeup);
        }
    }

    template<class TReq>
    void FillAuthInformation(const TReq& request) {
        SecurityToken_ = ExtractSecurityToken(request);
        UserName_ = request.GetAuth().GetUserName();
        FolderId_ = request.GetAuth().GetFolderId();
        UserSID_ = request.GetAuth().GetUserSID();
        
        if (IsCloud() && !FolderId_) {
            auto items = ParseCloudSecurityToken(SecurityToken_);
            UserName_ = std::get<0>(items);
            FolderId_ = std::get<1>(items);
            UserSID_ = std::get<2>(items);
        }
    }

    TString GetActionACLSourcePath() const {
        const EACLSourceType aclSourceType = GetActionACLSourceType(ToString(Action_));
        switch (aclSourceType) {
            case EACLSourceType::Unknown: {
                return {};
            }
            case EACLSourceType::RootDir: {
                return GetQueuePath().GetRootPath();
            }
            case EACLSourceType::AccountDir: {
                return GetQueuePath().GetUserPath();
            }
            case EACLSourceType::QueueDir: {
                return GetQueuePath().GetQueuePath();
            }
            case EACLSourceType::Custom: {
                return GetCustomACLPath();
            }
        }

        return {};
    }

    void RequestTicketParser() {
        this->Send(MakeTicketParserID(), new TEvTicketParser::TEvAuthorizeTicket(SecurityToken_));
    }

    bool IsACLProtectedAccount(const TString& accountName) const {
        if (accountName) {
            // temporary O(N) solution since the list contains up to 100 items
            return !IsIn(Cfg().GetAccountsWithoutMandatoryAuth(), accountName);
        }

        return true;
    }

    void HandleConfiguration(TSqsEvents::TEvConfiguration::TPtr& ev) {
        const TDuration confDuration = TActivationContext::Now() - StartTs_;
        RLOG_SQS_DEBUG("Get configuration duration: " << confDuration.MilliSeconds() << "ms");

        RootUrl_  = std::move(ev->Get()->RootUrl);
        UserExists_ = ev->Get()->UserExists;
        QueueExists_ = ev->Get()->QueueExists;
        QueueVersion_ = ev->Get()->QueueVersion;
        TablesFormat_ = ev->Get()->TablesFormat;
        Shards_   = ev->Get()->Shards;
        IsFifo_ = ev->Get()->Fifo;
        QueueAttributes_ = std::move(ev->Get()->QueueAttributes);
        SchemeCache_ = ev->Get()->SchemeCache;
        SqsCoreCounters_ = std::move(ev->Get()->SqsCoreCounters);
        QueueCounters_ = std::move(ev->Get()->QueueCounters);
        UserCounters_ = std::move(ev->Get()->UserCounters);
        QueueLeader_ = ev->Get()->QueueLeader;
        QuoterResources_ = std::move(ev->Get()->QuoterResources);

        RLOG_SQS_TRACE("Got configuration. Root url: " << RootUrl_
                        << ", Shards: " << Shards_
                        << ", Fail: " << ev->Get()->Fail);

        if (QueueCounters_) {
            auto* detailedCounters = QueueCounters_ ? QueueCounters_->GetDetailedCounters() : nullptr;
            COLLECT_HISTOGRAM_COUNTER(detailedCounters, GetConfiguration_Duration, confDuration.MilliSeconds());
        } else if (UserCounters_) {
            auto* detailedCounters = UserCounters_ ? UserCounters_->GetDetailedCounters() : nullptr;
            COLLECT_HISTOGRAM_COUNTER(detailedCounters, GetConfiguration_Duration, confDuration.MilliSeconds());
        }

        const bool needQueueAttributes = TDerived::NeedQueueAttributes();
        if (needQueueAttributes) {
            Y_ABORT_UNLESS(ev->Get()->Fail || !ev->Get()->QueueExists || QueueAttributes_.Defined());

            if (QueueAttributes_.Defined()) {
                RLOG_SQS_TRACE("Got configuration. Attributes: " << *QueueAttributes_);
            }
        }

        if (ev->Get()->Fail) {
            MakeError(MutableErrorDesc(), NErrors::INTERNAL_FAILURE, "Failed to get configuration.");
            SendReplyAndDie();
            return;
        }

        if (ev->Get()->Throttled) {
            MakeError(MutableErrorDesc(), NErrors::THROTTLING_EXCEPTION, "Too many requests for nonexistent queue.");
            SendReplyAndDie();
            return;
        }

        if (TDerived::NeedExistingQueue() && !ev->Get()->QueueExists) {
            MakeError(MutableErrorDesc(), NErrors::NON_EXISTENT_QUEUE);
            SendReplyAndDie();
            return;
        }

        Y_ABORT_UNLESS(SchemeCache_);

        bool isACLProtectedAccount = Cfg().GetForceAccessControl();
        if (!IsCloud() && (SecurityToken_ || (Cfg().GetForceAccessControl() && (isACLProtectedAccount = IsACLProtectedAccount(UserName_))))) {
            this->Become(&TActionActor::WaitAuthCheckMessages);
            const auto& actionACLSourcePath = GetActionACLSourcePath();
            if (!actionACLSourcePath || IsForbiddenPath(actionACLSourcePath)) {
                RLOG_SQS_ERROR("Bad ACL source path " << actionACLSourcePath << " for " << Action_ << " action");
                MakeError(MutableErrorDesc(), NErrors::ACCESS_DENIED);
                SendReplyAndDie();
                return;
            }

            if (!SecurityToken_) {
                MakeError(MutableErrorDesc(), NErrors::INVALID_CLIENT_TOKEN_ID, "No security token was provided.");
                SendReplyAndDie();
                return;
            }

            RequestSchemeCache(GetActionACLSourcePath()); // this also checks that requested queue (if any) does exist
            RequestTicketParser();
        } else {
            if (!isACLProtectedAccount) { // !IsCloud && !SecurityToken_ && account is in AccountsWithoutMandatoryAuth setting.
                INC_COUNTER(UserCounters_, UnauthenticatedAccess); // if !ForceAccessControl, this counter is not initialized.
            }
            // old habits
            DoGetQuotaAndProcess();
        }
    }

    void HandleSchemeCacheResponse(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev) {
        TEvTxProxySchemeCache::TEvNavigateKeySetResult* msg = ev->Get();
        const NSchemeCache::TSchemeCacheNavigate* navigate = msg->Request.Get();

        Y_ABORT_UNLESS(navigate->ResultSet.size() == 1);

        if (navigate->ErrorCount > 0) {
            const NSchemeCache::TSchemeCacheNavigate::EStatus status = navigate->ResultSet.front().Status;
            RLOG_SQS_ERROR("Failed to read ACL for " << GetActionACLSourcePath() << ". Scheme cache error: " << status);

            if (status == NSchemeCache::TSchemeCacheNavigate::EStatus::PathErrorUnknown) {
                MakeError(MutableErrorDesc(), NErrors::ACCESS_DENIED);
            } else {
                MakeError(MutableErrorDesc(), NErrors::INTERNAL_FAILURE);
            }

            SendReplyAndDie();
            return;
        }

        SecurityObject_ = navigate->ResultSet.front().SecurityObject;

        OnAuthCheckMessage();
    }

    void HandleTicketParserResponse(TEvTicketParser::TEvAuthorizeTicketResult::TPtr& ev) {
        const TEvTicketParser::TEvAuthorizeTicketResult& result(*ev->Get());
        if (!result.Error.empty()) {
            RLOG_SQS_ERROR("Got ticket parser error: " << result.Error << ". " << Action_ << " was rejected");
            MakeError(MutableErrorDesc(), NErrors::ACCESS_DENIED);
            SendReplyAndDie();
            return;
        } else {
            UserToken_ = ev->Get()->Token;
            Y_ABORT_UNLESS(UserToken_);
        }

        OnAuthCheckMessage();
    }

    void OnAuthCheckMessage() {
        --SecurityCheckRequestsToWaitFor_;

        if (SecurityCheckRequestsToWaitFor_ == 0) {
            const TString& actionName = ToString(Action_);
            const ui32 requiredAccess = GetActionRequiredAccess(actionName);
            UserSID_ = UserToken_->GetUserSID();
            if (requiredAccess != 0 && SecurityObject_ && !SecurityObject_->CheckAccess(requiredAccess, *UserToken_)) {
                if (Action_ == EAction::ModifyPermissions) {
                    // do not spam for other actions
                    RLOG_SQS_WARN("User " << UserSID_ << " tried to modify ACL for " << GetActionACLSourcePath() << ". Access denied");
                }
                MakeError(MutableErrorDesc(), NErrors::ACCESS_DENIED, Sprintf("%s on %s was denied for %s due to missing permission %s.",
                          actionName.c_str(), SanitizeNodePath(GetActionACLSourcePath()).c_str(), UserSID_.c_str(), GetActionMatchingACE(actionName).c_str()));
                SendReplyAndDie();
                return;
            }

            DoGetQuotaAndProcess();
        }
    }

    void DoGetQuotaAndProcess() {
        if (SourceSqsRequest_.GetRequestRateLimit() && Cfg().GetQuotingConfig().GetEnableQuoting() && QuoterResources_) {
            this->Become(&TActionActor::WaitQuotaState);
            RLOG_SQS_DEBUG("Requesting quota");
            QuotaRequestTs_ = TActivationContext::Now();
            ui64 quoterId = 0;
            ui64 resourceId = 0;
            TDuration deadline = TDuration::Max(); // defaut deadline is infinity
            auto resourceForAction = QuoterResources_->ActionsResources.find(Action_);
            if (resourceForAction != QuoterResources_->ActionsResources.end()) {
                quoterId = resourceForAction->second.QuoterId;
                resourceId = resourceForAction->second.ResourceId;
            } else {
                quoterId = QuoterResources_->OtherActions.QuoterId;
                resourceId = QuoterResources_->OtherActions.ResourceId;
            }
            if (Cfg().GetQuotingConfig().HasQuotaDeadlineMs()) {
                deadline = TDuration::MilliSeconds(Cfg().GetQuotingConfig().GetQuotaDeadlineMs());
            }
            this->Send(MakeQuoterServiceID(),
                new TEvQuota::TEvRequest(
                    TEvQuota::EResourceOperator::And,
                    { TEvQuota::TResourceLeaf(quoterId, resourceId, 1) },
                    deadline));
        } else {
            DoRoutine();
        }
    }

    void HandleQuota(TEvQuota::TEvClearance::TPtr& ev) {
        const TDuration quotaWaitDuration = TActivationContext::Now() - QuotaRequestTs_;
        switch (ev->Get()->Result) {
        case TEvQuota::TEvClearance::EResult::GenericError:
        case TEvQuota::TEvClearance::EResult::UnknownResource: {
            RLOG_SQS_ERROR("Failed to get quota: " << ev->Get()->Result);
            MakeError(MutableErrorDesc(), NErrors::INTERNAL_FAILURE);
            SendReplyAndDie();
            break;
        }
        case TEvQuota::TEvClearance::EResult::Deadline: {
            RLOG_SQS_WARN("Failed to get quota: deadline expired. Quota wait duration: " << quotaWaitDuration << ". Action: " << Action_);
            INC_COUNTER(QueueCounters_, RequestsThrottled);
            MakeError(MutableErrorDesc(), NErrors::THROTTLING_EXCEPTION);
            SendReplyAndDie();
            break;
        }
        case TEvQuota::TEvClearance::EResult::Success: {
            RLOG_SQS_DEBUG("Successfully got quota for request. Quota wait duration: " << quotaWaitDuration << ". Action: " << Action_);
            if (UserCounters_) {
                auto* detailedCounters = UserCounters_ ? UserCounters_->GetDetailedCounters() : nullptr;
                COLLECT_HISTOGRAM_COUNTER(detailedCounters, GetQuota_Duration, quotaWaitDuration.MilliSeconds());
            }
            DoRoutine();
            break;
        }
        }
    }

private:
    void Start() {
        auto actionCountersCouple = GetActionCounters();
        if (actionCountersCouple.SqsCounters) {
            if (IsActionForQueue(Action_) && QueueCounters_) {
                NeedReportSqsActionInflyCounter = QueueCounters_->NeedToShowDetailedCounters();
            } else if (IsActionForUser(Action_) && UserCounters_) {
                NeedReportSqsActionInflyCounter = UserCounters_->NeedToShowDetailedCounters();
            }
            if (NeedReportSqsActionInflyCounter) {
                INC_COUNTER(actionCountersCouple.SqsCounters, Infly);
            }
        }
        if (actionCountersCouple.YmqCounters) {
            if (IsActionForQueueYMQ(Action_) && QueueCounters_) {
                NeedReportYmqActionInflyCounter = QueueCounters_->NeedToShowDetailedCounters();
            } else if (IsActionForUserYMQ(Action_) && UserCounters_) {
                NeedReportYmqActionInflyCounter = UserCounters_->NeedToShowDetailedCounters();
            }
            if (NeedReportYmqActionInflyCounter) {
                INC_COUNTER(actionCountersCouple.YmqCounters, Infly);
            }
        }
        DoStart();
        StartRequestWasCalled_ = true;
    }

    void Finish() {
        auto actionCounters = GetActionCounters();
        if (NeedReportSqsActionInflyCounter) {
            DEC_COUNTER(actionCounters.SqsCounters, Infly);
        }
        if (NeedReportYmqActionInflyCounter && actionCounters.YmqCounters) {
            DEC_COUNTER(actionCounters.YmqCounters, Infly);
        }
        if (StartRequestWasCalled_) {
            DoFinish();
            FinishRequestWasCalled_ = true;
        }
    }

protected:
    static constexpr ui64 REQUEST_TIMEOUT_WAKEUP_TAG = 100;

    const EAction Action_;
    const TString RequestId_;
    THolder<IReplyCallback> Cb_;
    TString  RootUrl_;
    TString  UserName_;
    TString  SecurityToken_;
    TString  FolderId_;
    size_t SecurityCheckRequestsToWaitFor_ = 2;
    TIntrusivePtr<TSecurityObject> SecurityObject_;
    TIntrusiveConstPtr<NACLib::TUserToken> UserToken_;
    TString  UserSID_; // identifies the client who sent this request
    bool UserExists_ = false;
    bool QueueExists_ = false;
    ui64     Shards_;
    TMaybe<bool> IsFifo_;
    TMaybe<ui64> QueueVersion_;
    TMaybe<ui32> TablesFormat_;
    TInstant StartTs_;
    TInstant FinishTs_;
    TIntrusivePtr<::NMonitoring::TDynamicCounters> SqsCoreCounters_; // Raw counters interface. Is is not prefered to use them
    TIntrusivePtr<::NMonitoring::TDynamicCounters> YmqRootCounters_; // Raw counters interface. Is is not prefered to use them
    TIntrusivePtr<TUserCounters> UserCounters_;
    TIntrusivePtr<TQueueCounters> QueueCounters_;
    TMaybe<TSqsEvents::TQueueAttributes> QueueAttributes_;
    NKikimrClient::TSqsResponse Response_;
    TActorId SchemeCache_;
    TActorId QueueLeader_;
    bool StartRequestWasCalled_ = false;
    bool FinishRequestWasCalled_ = false;
    TInstant QuotaRequestTs_;
    TIntrusivePtr<TSqsEvents::TQuoterResourcesForActions> QuoterResources_;
    bool NeedReportSqsActionInflyCounter = false;
    bool NeedReportYmqActionInflyCounter = false;
    TSchedulerCookieHolder TimeoutCookie_ = ISchedulerCookie::Make2Way();
    NKikimrClient::TSqsRequest SourceSqsRequest_;
};

} // namespace NKikimr::NSQS
