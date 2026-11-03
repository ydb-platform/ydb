#include <ydb/core/base/iam_delegation.h>
#include <ydb/core/protos/replication.pb.h>
#include <ydb/core/security/iam_delegation/iam_delegation_service.h>
#include <ydb/core/security/token_manager/token_manager.h>
#include <ydb/core/security/iam_delegation/tablet/ut/ut_helpers.h>
#include <ydb/library/ycloud/api/operation_service.h>
#include <ydb/library/ycloud/api/service_control_service.h>

#include <library/cpp/testing/unittest/registar.h>

#include <google/rpc/error_details.pb.h>

#include <algorithm>
#include <deque>
#include <optional>

namespace NKikimr::NIamDelegation {
namespace {

using namespace NActors;
using TTokenStatus = TEvTokenManager::TStatus;
using TControl = NCloud::TEvServiceControlService;
using TOperation = NCloud::TEvOperationService;

// Real delegation actor and coroutine scheduler, virtual monotonic time. Only external actor
// requests are intercepted: no injected production factories, clocks or timeout settings.
struct TFixture {
    // Unlike edge mailboxes, an ordinary mailbox is observed even when a reply was already
    // queued before DispatchEvents. The observer below captures every result with virtual time.
    class TRecipient : public TActor<TRecipient> {
    public:
        TRecipient()
            : TActor(&TRecipient::StateWork)
        {}

        STFUNC(StateWork) {
            Y_UNUSED(ev);
        }
    };

    NTests::TTestContext Tablet;
    TTestBasicRuntime& Runtime = Tablet.GetRuntime();

    struct TResult {
        TDelegationResult Result;
        ui64 Cookie;
        TMonotonic Time;
    };

    TIamDelegationSettings Settings;
    TActorId Sender = Runtime.Register(new TRecipient());
    TActorId Manager = Runtime.AllocateEdgeActor();
    TActorId Service;
    ui32 Subscriptions = 0;
    TString SubscribedId;
    TActorId Subscriber;
    std::deque<TAutoPtr<IEventHandle>> Setups;
    std::deque<TAutoPtr<IEventHandle>> Revokes;
    std::deque<TAutoPtr<IEventHandle>> Polls;
    std::deque<TResult> Results;
    std::deque<std::pair<TDelegationOperationResult, ui64>> OperationResults;
    TVector<TMonotonic> PollTimes;
    TVector<TActorId> PoisonedClients;

    enum class ETokenManager { Present, Missing };
    enum class ECall { Setup, Revoke };

    explicit TFixture(ETokenManager tokenManager = ETokenManager::Present) {
        Runtime.SetScheduledEventFilter([](auto&, auto&, auto, auto&) { return false; });
        Settings.Config.SetDelegationTabletId(NTests::TTestContext::TabletId);
        Settings.Config.SetTokenServiceEndpoint("localhost:1");
        Settings.Config.SetServiceControlEndpoint("localhost:1");
        Settings.Config.SetEnableSsl(false);
        Settings.Config.SetServiceId("ydb");
        Settings.Config.SetMicroserviceId("data-plane");
        Settings.Config.SetResourceType("resource-manager.cloud");
        Settings.Config.SetSystemTokenName("delegation-system-account");
        if (tokenManager == ETokenManager::Present) {
            Runtime.RegisterService(MakeTokenManagerID(), Manager);
        }
        Runtime.SetObserverFunc([this](TAutoPtr<IEventHandle>& ev) {
            using EAction = TTestActorRuntimeBase::EEventAction;
            switch (ev->GetTypeRewrite()) {
                case TEvTokenManager::TEvSubscribeUpdateToken::EventType:
                    ++Subscriptions;
                    Subscriber = ev->Sender;
                    SubscribedId = ev->Get<TEvTokenManager::TEvSubscribeUpdateToken>()->Id;
                    return EAction::DROP;
                case TControl::TEvSetupDelegationRequest::EventType:
                    Setups.push_back(ev.Release());
                    return EAction::DROP;
                case TControl::TEvRevokeDelegationRequest::EventType:
                    Revokes.push_back(ev.Release());
                    return EAction::DROP;
                case TOperation::TEvGetOperationRequest::EventType:
                    PollTimes.push_back(Now());
                    Polls.push_back(ev.Release());
                    return EAction::DROP;
                case TEvIamDelegation::TEvSetupDelegationResult::EventType:
                    Results.push_back({ev->Get<TEvIamDelegation::TEvSetupDelegationResult>()->Result, ev->Cookie, Now()});
                    return EAction::DROP;
                case TEvIamDelegation::TEvRevokeDelegationResult::EventType:
                    Results.push_back({ev->Get<TEvIamDelegation::TEvRevokeDelegationResult>()->Result, ev->Cookie, Now()});
                    return EAction::DROP;
                case TEvIamDelegation::TEvDelegationOperationResult::EventType:
                    OperationResults.emplace_back(ev->Get<TEvIamDelegation::TEvDelegationOperationResult>()->Result, ev->Cookie);
                    return EAction::DROP;
                case TEvents::TEvPoison::EventType:
                    if (ev->Sender == Service) {
                        PoisonedClients.push_back(ev->Recipient);
                    }
                    break;
            }
            return EAction::PROCESS;
        });
        Service = Runtime.Register(CreateIamDelegationService(Settings));
        Runtime.EnableScheduleForActor(Service);
        // A missing local service routes the subscription to undelivered before the observer.
        if (tokenManager == ETokenManager::Present) {
            Until([&] { return Subscriptions == 1; });
        } else {
            TDispatchOptions options;
            options.CustomFinalCondition = [] { return true; };
            Runtime.DispatchEvents(options);
        }
    }

    ~TFixture() {
        Runtime.SetObserverFunc(TTestActorRuntimeBase::DefaultObserverFunc);
    }

    TMonotonic Now() const {
        return Runtime.GetCurrentMonotonicTime();
    }

    template<class TCondition>
    void Until(TCondition condition) {
        if (!condition()) {
            TDispatchOptions options;
            options.CustomFinalCondition = condition;
            options.FinalEvents.emplace_back([](IEventHandle&) { return false; });
            Runtime.DispatchEvents(options, TDuration::Minutes(5));
        }
        UNIT_ASSERT(condition());
    }

    void Update(TTokenStatus::ECode code = TTokenStatus::ECode::SUCCESS, const TString& token = "first-token",
            const TString& message = {}, const TString& id = "delegation-system-account")
    {
        Runtime.Send(new IEventHandle(Service, Manager, new TEvTokenManager::TEvUpdateToken(id, token, {code, message})));
    }

    static TDelegationSpec Spec() {
        return {.ServiceAccountId = "target-account", .CloudId = "cloud", .ReferrerId = "referrer"};
    }

    void Setup(ui64 cookie = 42) {
        Runtime.Send(new IEventHandle(Service, Sender, new TEvIamDelegation::TEvSetupDelegation(Spec(), "user-subject"), 0, cookie), 0, true);
    }

    void Revoke(ui64 cookie = 43) {
        Runtime.Send(new IEventHandle(Service, Sender, new TEvIamDelegation::TEvRevokeDelegation(Spec()), 0, cookie));
    }

    TAutoPtr<IEventHandle> Take(std::deque<TAutoPtr<IEventHandle>>& requests) {
        Until([&] { return !requests.empty(); });
        TAutoPtr<IEventHandle> result(requests.front().Release());
        requests.pop_front();
        return result;
    }

    template<class TResponse>
    void Reply(const IEventHandle& request, bool done = true, grpc::StatusCode code = grpc::StatusCode::OK,
            std::optional<ui64> cookie = {})
    {
        auto response = MakeHolder<TResponse>();
        response->Response.set_id("operation");
        response->Response.set_done(done);
        response->Status.GRpcStatusCode = code;
        Runtime.Send(new IEventHandle(request.Sender, request.Recipient, response.Release(), 0, cookie.value_or(request.Cookie)));
    }

    TResult Result(Ydb::StatusIds::StatusCode status = Ydb::StatusIds::SUCCESS, ui64 cookie = 42) {
        Until([&] { return !Results.empty(); });
        auto result = std::move(Results.front());
        Results.pop_front();
        UNIT_ASSERT_VALUES_EQUAL_C(result.Result.Status, status, result.Result.Issues.ToOneLineString());
        UNIT_ASSERT_VALUES_EQUAL(result.Cookie, cookie);
        return result;
    }

    static TDelegationBinding Binding() {
        return {.ServiceAccountId = "target-account", .CloudId = "cloud", .ReferrerId = "referrer",
            .ServiceId = "persisted-service", .MicroserviceId = "persisted-microservice",
            .ResourceType = "persisted-resource-type", .ReferrerType = "persisted-referrer-type", .ReferencePolicy = EReferencePolicy::WithReferences};
    }

    void Submit(EDelegationOperationKind kind = EDelegationOperationKind::Setup) {
        Runtime.Send(new IEventHandle(Service, Sender,
            new TEvIamDelegation::TEvSubmitDelegationOperation(kind, Binding(), "user-subject", "durable-correlation-id"), 0, 73));
    }

    void Poll(const TString& operationId = "operation") {
        Runtime.Send(new IEventHandle(Service, Sender, new TEvIamDelegation::TEvPollDelegationOperation(operationId), 0, 73));
    }

    TDelegationOperationResult OperationResult(EDelegationOperationOutcome outcome) {
        Until([&] { return !OperationResults.empty(); });
        auto result = std::move(OperationResults.front());
        OperationResults.pop_front();
        UNIT_ASSERT_VALUES_EQUAL(result.second, 73u);
        UNIT_ASSERT(result.first.Outcome == outcome);
        return std::move(result.first);
    }

    TMonotonic BeginPolling(ECall call = ECall::Setup) {
        Update();
        if (call == ECall::Revoke) {
            Revoke();
            auto request = Take(Revokes);
            Reply<TControl::TEvRevokeDelegationResponse>(*request, false);
        } else {
            Setup();
            auto request = Take(Setups);
            Reply<TControl::TEvSetupDelegationResponse>(*request, false);
        }
        return Now() + TIamDelegationSettings::OperationPollTimeout;
    }

    void BeforeDeadline(TMonotonic deadline) {
        Runtime.SimulateSleep(deadline - Now() - TDuration::MicroSeconds(1));
        UNIT_ASSERT(Results.empty());
        UNIT_ASSERT(Now() < deadline);
    }

    void ExpectDeadline(TMonotonic deadline, ui64 cookie = 42) {
        const auto result = Result(Ydb::StatusIds::TIMEOUT, cookie);
        UNIT_ASSERT_VALUES_EQUAL(result.Time, deadline);
        UNIT_ASSERT_STRING_CONTAINS(result.Result.Issues.ToOneLineString(), "polling deadline");
        for (const auto time : PollTimes) {
            UNIT_ASSERT(time < deadline);
        }
    }

    void Poison() {
        Runtime.Send(new IEventHandle(Service, Sender, new TEvents::TEvPoison()));
        const auto edge = Runtime.AllocateEdgeActor();
        Runtime.Send(new IEventHandle(Service, edge, new TEvIamDelegation::TEvSetupDelegation(Spec(), "user-subject"),
            IEventHandle::FlagTrackDelivery, 123));
        auto undelivered = Runtime.GrabEdgeEvent<TEvents::TEvUndelivered>(edge, TDuration::Seconds(1));
        UNIT_ASSERT(undelivered);
        UNIT_ASSERT_VALUES_EQUAL(undelivered->Cookie, 123u);
        Until([&] { return PoisonedClients.size() >= 2; });
        UNIT_ASSERT(PoisonedClients[0] != PoisonedClients[1]);
    }
};

enum class EFailureDelivery { SubmissionStatus, PolledOperation };

void CheckSafeFailureReasons(EFailureDelivery delivery) {
    TFixture f;
    f.Update();
    google::rpc::Status error;
    error.set_code(grpc::StatusCode::PERMISSION_DENIED);
    error.set_message("credential-sentinel-do-not-persist");
    google::rpc::PreconditionFailure details;
    for (const TString type : {
        "BAD_SERVICE_ACCOUNT_CLOUD", "SERVICE_NOT_ENABLED", "SYSTEM_FOLDER_NOT_FOUND", "AGENT_SA_NOT_FOUND",
        "RESOURCE_CONTAINER_SETTINGS_NOT_FOUND", "RESOURCE_CONTAINER_NOT_FOUND", "SERVICE_ACCOUNT_NOT_FOUND",
        "SUBJECT_NOT_FOUND", "BAD_SUBJECT_TYPE", "credential-sentinel-do-not-persist"}) {
        auto* violation = details.add_violations();
        violation->set_type(type);
        violation->set_description("credential-sentinel-do-not-persist");
    }
    for (ui32 i = 0; i < 128; ++i) {
        auto* violation = details.add_violations();
        violation->set_type("BAD_SERVICE_ACCOUNT_CLOUD");
    }
    error.add_details()->PackFrom(details);
    if (delivery == EFailureDelivery::SubmissionStatus) {
        f.Submit();
        auto request = f.Take(f.Setups);
        auto response = MakeHolder<TControl::TEvSetupDelegationResponse>();
        response->Status.GRpcStatusCode = grpc::StatusCode::PERMISSION_DENIED;
        response->Status.Msg = error.message();
        response->Status.Details = error.SerializeAsString();
        f.Runtime.Send(new IEventHandle(request->Sender, request->Recipient, response.Release(), 0, request->Cookie));
    } else {
        f.Poll("saved-operation");
        auto request = f.Take(f.Polls);
        auto response = MakeHolder<TOperation::TEvGetOperationResponse>();
        response->Response.set_id("saved-operation");
        response->Response.set_done(true);
        *response->Response.mutable_error() = error;
        f.Runtime.Send(new IEventHandle(request->Sender, request->Recipient, response.Release(), 0, request->Cookie));
    }
    const auto result = f.OperationResult(EDelegationOperationOutcome::Failed);
    UNIT_ASSERT_VALUES_EQUAL(result.Status, Ydb::StatusIds::UNAUTHORIZED);
    UNIT_ASSERT_VALUES_EQUAL(result.FailureReasons.size(), 6);
    for (const auto reason : {
        EDelegationFailureReason::WrongCloud, EDelegationFailureReason::ServiceNotEnabled,
        EDelegationFailureReason::CloudNotFound, EDelegationFailureReason::ServiceAccountNotFound,
        EDelegationFailureReason::SubjectNotDelegatable, EDelegationFailureReason::PermissionDenied}) {
        UNIT_ASSERT(std::find(result.FailureReasons.begin(), result.FailureReasons.end(), reason) != result.FailureReasons.end());
    }
    UNIT_ASSERT(f.Setups.empty());
    UNIT_ASSERT(f.Revokes.empty());
}

} // namespace

Y_UNIT_TEST_SUITE(IamDelegationActor) {
    Y_UNIT_TEST(SubscribesOnceAndUsesConfiguredSystemIdentity) {
        TFixture f;
        UNIT_ASSERT_VALUES_EQUAL(f.Subscriber, f.Service);
        UNIT_ASSERT_VALUES_EQUAL(f.SubscribedId, "delegation-system-account");
        f.Setup();
        f.Update(TTokenStatus::ECode::NOT_READY);
        f.Runtime.SimulateSleep(TDuration::Seconds(1));
        UNIT_ASSERT(f.Setups.empty());
        UNIT_ASSERT(f.Results.empty());
        // Updates for another provider must not authorize this service.
        f.Update(TTokenStatus::ECode::SUCCESS, "other-token", {}, "access-service-account");
        f.Runtime.SimulateSleep(TDuration::Seconds(1));
        UNIT_ASSERT(f.Setups.empty());
        f.Update();
        auto setup = f.Take(f.Setups);
        const auto& request = *setup->Get<TControl::TEvSetupDelegationRequest>();
        UNIT_ASSERT_VALUES_EQUAL(request.Token, "first-token");
        UNIT_ASSERT_VALUES_EQUAL(request.Request.target_service_account_id(), "target-account");
        UNIT_ASSERT_VALUES_EQUAL(request.Request.on_behalf_of_subject_id(), "user-subject");
        f.Reply<TControl::TEvSetupDelegationResponse>(*setup);
        f.Result();
        f.Revoke();
        auto revoke = f.Take(f.Revokes);
        UNIT_ASSERT_VALUES_EQUAL(revoke->Get<TControl::TEvRevokeDelegationRequest>()->Token, "first-token");
        f.Reply<TControl::TEvRevokeDelegationResponse>(*revoke);
        f.Result(Ydb::StatusIds::SUCCESS, 43);
        UNIT_ASSERT_VALUES_EQUAL(f.Subscriptions, 1u);
    }

    Y_UNIT_TEST(RotationIsUsedOnNextIndependentRequest) {
        TFixture f;
        f.Update();
        f.Setup();
        auto first = f.Take(f.Setups);
        f.Update(TTokenStatus::ECode::SUCCESS, "rotated-token");
        f.Reply<TControl::TEvSetupDelegationResponse>(*first, false, grpc::StatusCode::UNAVAILABLE);
        f.Result(Ydb::StatusIds::UNAVAILABLE);
        f.Setup(44);
        auto retry = f.Take(f.Setups);
        UNIT_ASSERT_VALUES_EQUAL(retry->Get<TControl::TEvSetupDelegationRequest>()->Token, "rotated-token");
        UNIT_ASSERT(retry->Get<TControl::TEvSetupDelegationRequest>()->RequestId !=
            first->Get<TControl::TEvSetupDelegationRequest>()->RequestId);
        f.Reply<TControl::TEvSetupDelegationResponse>(*retry);
        f.Result(Ydb::StatusIds::SUCCESS, 44);
        UNIT_ASSERT_VALUES_EQUAL(f.Subscriptions, 1u);
    }

    Y_UNIT_TEST(ErrorInvalidatesCachedTokenUntilSuccess) {
        TFixture f;
        f.Update();
        // Token manager can include the old token with an error; it must not be reused.
        f.Update(TTokenStatus::ECode::ERROR, "first-token", "refresh failed");
        f.Setup();
        auto error = f.Result(Ydb::StatusIds::UNAVAILABLE);
        UNIT_ASSERT_STRING_CONTAINS(error.Result.Issues.ToOneLineString(), "system service account token is unavailable");
        UNIT_ASSERT(!error.Result.Issues.ToOneLineString().Contains("refresh failed"));
        UNIT_ASSERT(f.Setups.empty());
        f.Update(TTokenStatus::ECode::NOT_READY, "first-token");
        f.Setup(44);
        f.Runtime.SimulateSleep(TDuration::Seconds(1));
        UNIT_ASSERT(f.Setups.empty());
        f.Update(TTokenStatus::ECode::SUCCESS, "recovered-token");
        auto setup = f.Take(f.Setups);
        UNIT_ASSERT_VALUES_EQUAL(setup->Get<TControl::TEvSetupDelegationRequest>()->Token, "recovered-token");
        f.Reply<TControl::TEvSetupDelegationResponse>(*setup);
        f.Result(Ydb::StatusIds::SUCCESS, 44);
        UNIT_ASSERT_VALUES_EQUAL(f.Subscriptions, 1u);
    }

    Y_UNIT_TEST(EmptySuccessfulTokenCannotAuthorizeCalls) {
        TFixture f;
        f.Update(TTokenStatus::ECode::SUCCESS, "");
        f.Setup();
        auto result = f.Result(Ydb::StatusIds::UNAVAILABLE);
        UNIT_ASSERT_STRING_CONTAINS(result.Result.Issues.ToOneLineString(), "token is empty");
        UNIT_ASSERT(f.Setups.empty());
    }

    Y_UNIT_TEST(NotReadyHasBoundedWaitAndLateSuccessRecovers) {
        TFixture f;
        const auto start = f.Now();
        f.Setup();
        const auto result = f.Result(Ydb::StatusIds::UNAVAILABLE);
        UNIT_ASSERT_STRING_CONTAINS(result.Result.Issues.ToOneLineString(), "timeout while obtaining");
        UNIT_ASSERT(result.Time >= start + TIamDelegationSettings::RequestTimeout);
        UNIT_ASSERT(result.Time < start + TDuration::Seconds(60));
        UNIT_ASSERT(f.Setups.empty());
        UNIT_ASSERT_VALUES_EQUAL(f.Subscriptions, 1u);
        f.Update();
        f.Setup(44);
        auto request = f.Take(f.Setups);
        f.Reply<TControl::TEvSetupDelegationResponse>(*request);
        f.Result(Ydb::StatusIds::SUCCESS, 44);
    }

    Y_UNIT_TEST(UnknownProviderIsReportedByRealTokenManager) {
        TFixture f;
        // Exercise the actual manager's missing-provider response, without a network provider.
        f.Runtime.RegisterService(MakeTokenManagerID(), f.Runtime.Register(CreateTokenManager({})));
        f.Runtime.SetObserverFunc(TTestActorRuntimeBase::DefaultObserverFunc);
        const auto service = f.Runtime.Register(CreateIamDelegationService(f.Settings));
        f.Runtime.EnableScheduleForActor(service);
        const auto edge = f.Runtime.AllocateEdgeActor();
        // Queue the request behind bootstrap instead of bypassing the actor's initial state.
        f.Runtime.Send(new IEventHandle(service, edge, new TEvIamDelegation::TEvSetupDelegation(f.Spec(), "user-subject"), 0, 71), 0, true);
        auto result = f.Runtime.GrabEdgeEvent<TEvIamDelegation::TEvSetupDelegationResult>(edge, TDuration::Seconds(20));
        UNIT_ASSERT(result);
        UNIT_ASSERT_VALUES_EQUAL(result->Cookie, 71u);
        UNIT_ASSERT_VALUES_EQUAL(result->Get()->Result.Status, Ydb::StatusIds::UNAVAILABLE);
        UNIT_ASSERT_STRING_CONTAINS(result->Get()->Result.Issues.ToOneLineString(), "system service account token is unavailable");
        UNIT_ASSERT(!result->Get()->Result.Issues.ToOneLineString().Contains("delegation-system-account"));
    }

    Y_UNIT_TEST(MissingTokenManagerIsBounded) {
        TFixture f(TFixture::ETokenManager::Missing);
        f.Setup();
        auto result = f.Result(Ydb::StatusIds::UNAVAILABLE);
        UNIT_ASSERT_STRING_CONTAINS(result.Result.Issues.ToOneLineString(), "system service account token is unavailable");
        UNIT_ASSERT(f.Setups.empty());
    }

    Y_UNIT_TEST(ConcurrentRequestsMatchOnlyTheirOwnCookies) {
        TFixture f;
        f.Setup(41);
        f.Setup(42);
        f.Runtime.SimulateSleep(TDuration::Seconds(1));
        UNIT_ASSERT(f.Setups.empty());
        f.Update(); // one SUCCESS resumes both subscription waiters
        auto first = f.Take(f.Setups);
        auto second = f.Take(f.Setups);
        UNIT_ASSERT(first->Cookie != second->Cookie);
        f.Reply<TControl::TEvSetupDelegationResponse>(*first, true, grpc::StatusCode::OK, first->Cookie + 100000);
        f.Runtime.SimulateSleep(TDuration::MicroSeconds(1));
        UNIT_ASSERT(f.Results.empty());
        f.Reply<TControl::TEvSetupDelegationResponse>(*second);
        f.Result(Ydb::StatusIds::SUCCESS, 42);
        f.Reply<TControl::TEvSetupDelegationResponse>(*first);
        f.Result(Ydb::StatusIds::SUCCESS, 41);
        UNIT_ASSERT_VALUES_EQUAL(f.Subscriptions, 1u);
    }

    Y_UNIT_TEST(UndeliveredMutationIsNotReplayedAndLateRepliesAreIgnored) {
        TFixture f;
        f.Update();
        f.Setup();
        TAutoPtr<IEventHandle> last;
        for (ui32 attempt = 0; attempt < 1; ++attempt) {
            last = f.Take(f.Setups);
            f.Runtime.Send(new IEventHandle(last->Sender, last->Recipient,
                new TEvents::TEvUndelivered(last->GetTypeRewrite(), TEvents::TEvUndelivered::ReasonActorUnknown), 0, last->Cookie));
        }
        const auto result = f.Result(Ydb::StatusIds::UNAVAILABLE);
        UNIT_ASSERT_STRING_CONTAINS(result.Result.Issues.ToOneLineString(), "IAM client is unavailable");
        f.Reply<TControl::TEvSetupDelegationResponse>(*last);
        f.Runtime.SimulateSleep(TDuration::Seconds(1));
        UNIT_ASSERT(f.Results.empty());
        UNIT_ASSERT(f.Setups.empty());
    }

    Y_UNIT_TEST(NoPollStartsAtTheDeadline) {
        for (const auto call : {TFixture::ECall::Setup, TFixture::ECall::Revoke}) {
            TFixture f;
            const auto deadline = f.BeginPolling(call);
            auto poll = f.Take(f.Polls);
            f.Runtime.AdvanceCurrentTime(deadline - f.Now() - TIamDelegationSettings::OperationPollInterval);
            f.Reply<TOperation::TEvGetOperationResponse>(*poll, false);
            f.BeforeDeadline(deadline);
            f.ExpectDeadline(deadline, call == TFixture::ECall::Revoke ? 43 : 42);
            UNIT_ASSERT_VALUES_EQUAL(f.PollTimes.size(), 1u);
        }
    }

    Y_UNIT_TEST(PollResponseWaitIsClippedToDeadline) {
        TFixture f;
        const auto deadline = f.BeginPolling();
        f.Runtime.AdvanceCurrentTime(TIamDelegationSettings::OperationPollTimeout - TDuration::Seconds(2));
        auto poll = f.Take(f.Polls); // only two seconds remain, less than RequestTimeout
        f.BeforeDeadline(deadline);
        f.ExpectDeadline(deadline);
        f.Reply<TOperation::TEvGetOperationResponse>(*poll); // late success must not create a second reply
        f.Runtime.SimulateSleep(TDuration::Seconds(20));
        UNIT_ASSERT(f.Results.empty());
        UNIT_ASSERT_VALUES_EQUAL(f.PollTimes.size(), 1u);
    }

    Y_UNIT_TEST(CredentialsCannotOverrunPollDeadline) {
        TFixture f;
        const auto deadline = f.BeginPolling();
        f.Update(TTokenStatus::ECode::NOT_READY, "old-token");
        f.Runtime.AdvanceCurrentTime(TIamDelegationSettings::OperationPollTimeout - TDuration::Seconds(2));
        f.BeforeDeadline(deadline);
        f.ExpectDeadline(deadline);
        UNIT_ASSERT(f.PollTimes.empty());
        f.Update();
        f.Runtime.SimulateSleep(TDuration::Seconds(20));
        UNIT_ASSERT(f.Results.empty());
        UNIT_ASSERT(f.PollTimes.empty());
    }

    Y_UNIT_TEST(RetryBackoffCannotStartAPollAfterDeadline) {
        TFixture f;
        const auto deadline = f.BeginPolling();
        auto poll = f.Take(f.Polls);
        f.Runtime.AdvanceCurrentTime(deadline - f.Now() - TDuration::MicroSeconds(1));
        f.Reply<TOperation::TEvGetOperationResponse>(*poll, false, grpc::StatusCode::UNAVAILABLE);
        f.ExpectDeadline(deadline);
        UNIT_ASSERT_VALUES_EQUAL(f.PollTimes.size(), 1u);
    }

    Y_UNIT_TEST(ReplyAtDeadlineCannotStartARetry) {
        TFixture f;
        const auto deadline = f.BeginPolling();
        auto poll = f.Take(f.Polls);
        f.Runtime.AdvanceCurrentTime(deadline - f.Now());
        f.Reply<TOperation::TEvGetOperationResponse>(*poll, false, grpc::StatusCode::UNAVAILABLE);
        f.ExpectDeadline(deadline);
        UNIT_ASSERT_VALUES_EQUAL(f.PollTimes.size(), 1u);
    }

    Y_UNIT_TEST(InitialRequestHasItsOwnBudgetBeforePollingStarts) {
        TFixture f;
        f.Update();
        f.Setup();
        auto setup = f.Take(f.Setups);
        const auto initialStart = f.Now();
        f.Runtime.AdvanceCurrentTime(TDuration::Seconds(5));
        f.Reply<TControl::TEvSetupDelegationResponse>(*setup, false);
        const auto deadline = f.Now() + TIamDelegationSettings::OperationPollTimeout;
        f.Runtime.AdvanceCurrentTime(TIamDelegationSettings::OperationPollTimeout - TDuration::Seconds(2));
        auto poll = f.Take(f.Polls);
        f.ExpectDeadline(deadline);
        UNIT_ASSERT_VALUES_EQUAL(deadline - initialStart, TDuration::Seconds(65));
    }

    Y_UNIT_TEST(PoisonDuringCredentialsWaitDisarmsTheSubscriptionWait) {
        TFixture f;
        f.Setup();
        f.Poison();
        f.Update(); // token manager retains the long-lived subscriber ID; late updates are harmless
        f.Runtime.SimulateSleep(TDuration::Minutes(2));
        UNIT_ASSERT(f.Setups.empty());
        UNIT_ASSERT(f.Results.empty());
    }

    Y_UNIT_TEST(PoisonDuringPollDisarmsPendingCallsAndTimers) {
        TFixture f;
        f.BeginPolling();
        auto poll = f.Take(f.Polls);
        f.Poison();
        f.Reply<TOperation::TEvGetOperationResponse>(*poll);
        f.Update();
        f.Runtime.SimulateSleep(TDuration::Minutes(2));
        UNIT_ASSERT(f.Results.empty());
        UNIT_ASSERT_VALUES_EQUAL(f.PollTimes.size(), 1u);
    }

    Y_UNIT_TEST(RuntimeShutdownWhileWaitingForCredentials) {
        TFixture f;
        f.Setup();
        f.Runtime.SimulateSleep(TDuration::Seconds(1));
        UNIT_ASSERT(f.Setups.empty());
        UNIT_ASSERT(f.Results.empty());
        // Destruction tears down the actor system with a live async event wait.
    }
}

enum class EOperationDiagnosticSurface { SubmissionTransport, PollTransport, CompletedOperation };

void CheckOperationDiagnostics(EOperationDiagnosticSurface surface) {
    TFixture f;
    f.Update();
    google::rpc::Status error;
    error.set_code(grpc::StatusCode::PERMISSION_DENIED);
    error.set_message("credential-diagnostic-sentinel raw error");
    google::rpc::PreconditionFailure details;
    auto* known = details.add_violations();
    known->set_type("BAD_SERVICE_ACCOUNT_CLOUD");
    known->set_description("credential-diagnostic-sentinel known description");
    auto* unknown = details.add_violations();
    unknown->set_type("credential-diagnostic-sentinel unknown type");
    unknown->set_description("credential-diagnostic-sentinel unknown description");
    error.add_details()->PackFrom(details);
    if (surface == EOperationDiagnosticSurface::SubmissionTransport) {
        f.Submit();
        auto request = f.Take(f.Setups);
        auto reply = MakeHolder<TControl::TEvSetupDelegationResponse>();
        reply->Status.GRpcStatusCode = grpc::StatusCode::PERMISSION_DENIED;
        reply->Status.Msg = error.message();
        reply->Status.Details = error.SerializeAsString();
        f.Runtime.Send(new IEventHandle(request->Sender, request->Recipient, reply.Release(), 0, request->Cookie));
    } else {
        f.Poll("saved-operation");
        auto request = f.Take(f.Polls);
        auto reply = MakeHolder<TOperation::TEvGetOperationResponse>();
        if (surface == EOperationDiagnosticSurface::PollTransport) {
            reply->Status.GRpcStatusCode = grpc::StatusCode::PERMISSION_DENIED;
            reply->Status.Msg = error.message();
            reply->Status.Details = error.SerializeAsString();
        } else {
            reply->Response.set_id("saved-operation");
            reply->Response.set_done(true);
            *reply->Response.mutable_error() = error;
        }
        f.Runtime.Send(new IEventHandle(request->Sender, request->Recipient, reply.Release(), 0, request->Cookie));
    }
    const auto result = f.OperationResult(surface == EOperationDiagnosticSurface::PollTransport
        ? EDelegationOperationOutcome::Unknown : EDelegationOperationOutcome::Failed);
    UNIT_ASSERT_VALUES_EQUAL(result.Status, Ydb::StatusIds::UNAUTHORIZED);
    if (surface != EOperationDiagnosticSurface::SubmissionTransport) {
        UNIT_ASSERT_VALUES_EQUAL(result.OperationId, "saved-operation");
    }
    const auto rendered = result.Issues.ToOneLineString();
    UNIT_ASSERT_C(!rendered.Contains("credential-diagnostic-sentinel"), rendered);
    UNIT_ASSERT_STRING_CONTAINS(rendered, "RESOURCE");
    UNIT_ASSERT(rendered.size() < 1024);
    UNIT_ASSERT(f.Setups.empty());
    UNIT_ASSERT(f.Revokes.empty());
}


Y_UNIT_TEST_SUITE(IamDelegationRecovery) {
    Y_UNIT_TEST(SubmissionIssuesRetainSafeHintWithoutRawTransportDiagnostics) {
        CheckOperationDiagnostics(EOperationDiagnosticSurface::SubmissionTransport);
    }

    Y_UNIT_TEST(PollTransportIssuesStayRedactedWithoutResolvingUncertainty) {
        CheckOperationDiagnostics(EOperationDiagnosticSurface::PollTransport);
    }

    Y_UNIT_TEST(CompletedOperationIssuesExcludeRawMessageAndDescriptions) {
        CheckOperationDiagnostics(EOperationDiagnosticSurface::CompletedOperation);
    }

    Y_UNIT_TEST(SubmissionRefusalExtractsOnlyBoundedSafeReasons) {
        CheckSafeFailureReasons(EFailureDelivery::SubmissionStatus);
    }

    Y_UNIT_TEST(PolledRefusalExtractsOnlyBoundedSafeReasons) {
        CheckSafeFailureReasons(EFailureDelivery::PolledOperation);
    }

    Y_UNIT_TEST(TerminalOperationWithSuccessErrorCodeRemainsUnknown) {
        TFixture f;
        f.Update();
        f.Poll("saved-operation");
        auto request = f.Take(f.Polls);
        auto response = MakeHolder<TOperation::TEvGetOperationResponse>();
        response->Response.set_id("saved-operation");
        response->Response.set_done(true);
        response->Response.mutable_error()->set_code(grpc::StatusCode::OK);
        f.Runtime.Send(new IEventHandle(request->Sender, request->Recipient, response.Release(), 0, request->Cookie));
        const auto result = f.OperationResult(EDelegationOperationOutcome::Unknown);
        UNIT_ASSERT_VALUES_EQUAL(result.OperationId, "saved-operation");
        UNIT_ASSERT_VALUES_EQUAL(result.Status, Ydb::StatusIds::INTERNAL_ERROR);
        UNIT_ASSERT(f.Setups.empty());
        UNIT_ASSERT(f.Revokes.empty());
    }

    Y_UNIT_TEST(CredentialsFailureBeforeSubmitProvesNoRemoteMutation) {
        for (ui32 failure = 0; failure != 3; ++failure) {
            TFixture f;
            if (failure == 1) {
                f.Update(TTokenStatus::ECode::ERROR, {}, "credentials unavailable");
            } else if (failure == 2) {
                f.Update(TTokenStatus::ECode::SUCCESS, {});
            }
            f.Submit();
            const auto result = f.OperationResult(EDelegationOperationOutcome::Failed);
            UNIT_ASSERT_VALUES_EQUAL(result.Status, Ydb::StatusIds::UNAVAILABLE);
            UNIT_ASSERT(result.OperationId.empty());
            UNIT_ASSERT(f.Setups.empty());
            UNIT_ASSERT(f.Polls.empty());
            // A new setup is safe after credentials recover: no earlier call
            // was dispatched that could complete behind this one.
            f.Update();
            f.Submit();
            auto request = f.Take(f.Setups);
            f.Reply<TControl::TEvSetupDelegationResponse>(*request);
            f.OperationResult(EDelegationOperationOutcome::Succeeded);
        }
    }

    Y_UNIT_TEST(CredentialsFailureDuringPollKeepsKnownOperationUnknown) {
        TFixture f;
        f.Update(TTokenStatus::ECode::ERROR, {}, "credentials unavailable");
        f.Poll("accepted-before-credential-failure");
        const auto result = f.OperationResult(EDelegationOperationOutcome::Unknown);
        UNIT_ASSERT_VALUES_EQUAL(result.OperationId, "accepted-before-credential-failure");
        UNIT_ASSERT(f.Setups.empty());
        UNIT_ASSERT(f.Polls.empty());
    }

    Y_UNIT_TEST(OnlyDefiniteUndeliveredSubmitIsFailed) {
        for (bool unsure : {false, true}) {
            TFixture f;
            f.Update();
            f.Submit();
            auto request = f.Take(f.Setups);
            f.Runtime.Send(new IEventHandle(request->Sender, request->Recipient,
                new TEvents::TEvUndelivered(TControl::TEvSetupDelegationRequest::EventType,
                    TEvents::TEvUndelivered::ReasonActorUnknown, unsure), 0, request->Cookie));
            const auto result = f.OperationResult(unsure
                ? EDelegationOperationOutcome::Unknown : EDelegationOperationOutcome::Failed);
            UNIT_ASSERT(result.OperationId.empty());
            UNIT_ASSERT(f.Setups.empty());
        }
    }

    Y_UNIT_TEST(SubmitPreservesBindingAndReturnsAcceptedWithoutPolling) {
        TFixture f;
        f.Update();
        f.Submit();
        auto request = f.Take(f.Setups);
        const auto& event = *request->Get<TControl::TEvSetupDelegationRequest>();
        UNIT_ASSERT_VALUES_EQUAL(event.Request.service_id(), "persisted-service");
        UNIT_ASSERT_VALUES_EQUAL(event.Request.microservice_id(), "persisted-microservice");
        UNIT_ASSERT_VALUES_EQUAL(event.Request.resource().type(), "persisted-resource-type");
        UNIT_ASSERT_VALUES_EQUAL(event.Request.referrer().type(), "persisted-referrer-type");
        UNIT_ASSERT(event.Request.with_references());
        UNIT_ASSERT_VALUES_EQUAL(event.Request.on_behalf_of_subject_id(), "user-subject");
        UNIT_ASSERT_VALUES_EQUAL(event.RequestId, "durable-correlation-id");
        f.Reply<TControl::TEvSetupDelegationResponse>(*request, false);
        const auto result = f.OperationResult(EDelegationOperationOutcome::Accepted);
        UNIT_ASSERT_VALUES_EQUAL(result.OperationId, "operation");
        UNIT_ASSERT(f.Polls.empty());
    }

    Y_UNIT_TEST(LostSubmitResponseRemainsUnknownAndIsNotRetried) {
        TFixture f;
        f.Update();
        f.Submit();
        auto request = f.Take(f.Setups);
        const auto result = f.OperationResult(EDelegationOperationOutcome::Unknown);
        UNIT_ASSERT(result.OperationId.empty());
        UNIT_ASSERT(f.Setups.empty());
        UNIT_ASSERT(f.Polls.empty());
        f.Reply<TControl::TEvSetupDelegationResponse>(*request, false);
        f.Poll("known-operation");
        auto poll = f.Take(f.Polls);
        UNIT_ASSERT(f.OperationResults.empty()); // late submit response is not reclassified as accepted
        UNIT_ASSERT_VALUES_EQUAL(poll->Get<TOperation::TEvGetOperationRequest>()->Request.operation_id(), "known-operation");
        UNIT_ASSERT(f.Setups.empty());
    }

    Y_UNIT_TEST(ExplicitInitialRefusalIsFailed) {
        TFixture f;
        f.Update();
        f.Submit();
        auto request = f.Take(f.Setups);
        f.Reply<TControl::TEvSetupDelegationResponse>(*request, true, grpc::StatusCode::PERMISSION_DENIED);
        const auto result = f.OperationResult(EDelegationOperationOutcome::Failed);
        UNIT_ASSERT_VALUES_EQUAL(result.Status, Ydb::StatusIds::UNAUTHORIZED);
        UNIT_ASSERT(f.Setups.empty());
    }

    Y_UNIT_TEST(LegacyCallsDistinguishCredentialsFromPermissionsWithoutRetry) {
        for (auto code : {grpc::StatusCode::UNAUTHENTICATED, grpc::StatusCode::PERMISSION_DENIED}) {
            TFixture f;
            f.Update();
            f.Setup();
            auto request = f.Take(f.Setups);
            f.Reply<TControl::TEvSetupDelegationResponse>(*request, true, code);
            const auto result = f.Result(Ydb::StatusIds::UNAUTHORIZED).Result;
            UNIT_ASSERT_STRING_CONTAINS(result.Issues.ToOneLineString(), code == grpc::StatusCode::UNAUTHENTICATED
                ? "authentication failed" : "permission denied");
            UNIT_ASSERT(f.Setups.empty());
        }
    }

    Y_UNIT_TEST(SubmissionRefusalsKeepDistinctSafeAuthenticationReasons) {
        for (auto code : {grpc::StatusCode::UNAUTHENTICATED, grpc::StatusCode::PERMISSION_DENIED}) {
            TFixture f;
            f.Update();
            f.Submit();
            auto request = f.Take(f.Setups);
            f.Reply<TControl::TEvSetupDelegationResponse>(*request, true, code);
            const auto result = f.OperationResult(EDelegationOperationOutcome::Failed);
            UNIT_ASSERT_VALUES_EQUAL(result.FailureReasons.size(), 1);
            UNIT_ASSERT_STRING_CONTAINS(result.Issues.ToOneLineString(), code == grpc::StatusCode::UNAUTHENTICATED
                ? "authentication failed" : "permission denied");
            UNIT_ASSERT(f.Setups.empty());
        }
    }

    Y_UNIT_TEST(PollAuthenticationRefusalsKeepTheRemoteOutcomeUnknown) {
        for (auto code : {grpc::StatusCode::UNAUTHENTICATED, grpc::StatusCode::PERMISSION_DENIED}) {
            TFixture f;
            f.Update();
            f.Poll("saved-operation");
            auto request = f.Take(f.Polls);
            f.Reply<TOperation::TEvGetOperationResponse>(*request, false, code);
            const auto result = f.OperationResult(EDelegationOperationOutcome::Unknown);
            UNIT_ASSERT_VALUES_EQUAL(result.OperationId, "saved-operation");
            UNIT_ASSERT_VALUES_EQUAL(result.FailureReasons.size(), 1);
            UNIT_ASSERT_STRING_CONTAINS(result.Issues.ToOneLineString(), code == grpc::StatusCode::UNAUTHENTICATED
                ? "authentication failed" : "permission denied");
            const auto message = result.Issues.ToOneLineString();
            if (code == grpc::StatusCode::PERMISSION_DENIED) {
                UNIT_ASSERT_STRING_CONTAINS(message, "original initiating system service account");
                UNIT_ASSERT_STRING_CONTAINS(message, "<service>.<resource>.get");
            }
            UNIT_ASSERT(!message.Contains("iam.serviceAccounts.use"));
            UNIT_ASSERT(!message.Contains("internal.service-control.delegator"));
            UNIT_ASSERT(f.Setups.empty());
            UNIT_ASSERT(f.Revokes.empty());
        }
    }

    Y_UNIT_TEST(SetupPermissionRefusalNamesUserAndServiceRights) {
        TFixture f;
        f.Update();
        f.Submit();
        auto request = f.Take(f.Setups);
        f.Reply<TControl::TEvSetupDelegationResponse>(*request, true, grpc::StatusCode::PERMISSION_DENIED);
        const auto result = f.OperationResult(EDelegationOperationOutcome::Failed);
        const auto message = result.Issues.ToOneLineString();
        UNIT_ASSERT_STRING_CONTAINS(message, "iam.serviceAccounts.use");
        UNIT_ASSERT_STRING_CONTAINS(message, "iam.serviceAccounts.user");
        UNIT_ASSERT_STRING_CONTAINS(message, "internal.service-control.delegator");
        UNIT_ASSERT_STRING_CONTAINS(message, "iam.gizmo");
        UNIT_ASSERT_STRING_CONTAINS(message, "service-control.services.referenceUpdater");
    }

    Y_UNIT_TEST(RevokePermissionRefusalNamesTheServiceRole) {
        TFixture f;
        f.Update();
        f.Revoke();
        auto request = f.Take(f.Revokes);
        f.Reply<TControl::TEvRevokeDelegationResponse>(*request, true, grpc::StatusCode::PERMISSION_DENIED);
        const auto message = f.Result(Ydb::StatusIds::UNAUTHORIZED, 43).Result.Issues.ToOneLineString();
        UNIT_ASSERT_STRING_CONTAINS(message, "internal.service-control.delegator");
        UNIT_ASSERT_STRING_CONTAINS(message, "iam.gizmo");
        UNIT_ASSERT(!message.Contains("iam.serviceAccounts.use"));
    }

    Y_UNIT_TEST(PollPermissionFailurePreservesUnknownOperation) {
        TFixture f;
        f.Update();
        f.Poll();
        auto request = f.Take(f.Polls);
        f.Reply<TOperation::TEvGetOperationResponse>(*request, false, grpc::StatusCode::PERMISSION_DENIED);
        const auto result = f.OperationResult(EDelegationOperationOutcome::Unknown);
        UNIT_ASSERT_VALUES_EQUAL(result.OperationId, "operation");
        UNIT_ASSERT_VALUES_EQUAL(result.Status, Ydb::StatusIds::UNAUTHORIZED);
        UNIT_ASSERT(f.Setups.empty());
        UNIT_ASSERT(f.Revokes.empty());
    }

    Y_UNIT_TEST(PollTerminalOperationErrorIsFailed) {
        TFixture f;
        f.Update();
        f.Poll();
        auto request = f.Take(f.Polls);
        auto response = MakeHolder<TOperation::TEvGetOperationResponse>();
        response->Response.set_id("operation");
        response->Response.set_done(true);
        response->Response.mutable_error()->set_code(grpc::StatusCode::PERMISSION_DENIED);
        response->Response.mutable_error()->set_message("denied");
        f.Runtime.Send(new IEventHandle(request->Sender, request->Recipient, response.Release(), 0, request->Cookie));
        const auto result = f.OperationResult(EDelegationOperationOutcome::Failed);
        UNIT_ASSERT_VALUES_EQUAL(result.OperationId, "operation");
        UNIT_ASSERT_VALUES_EQUAL(result.Status, Ydb::StatusIds::UNAUTHORIZED);
    }

    Y_UNIT_TEST(PollRejectsDifferentOperationId) {
        TFixture f;
        f.Update();
        f.Poll("persisted-operation");
        auto request = f.Take(f.Polls);
        f.Reply<TOperation::TEvGetOperationResponse>(*request);
        const auto result = f.OperationResult(EDelegationOperationOutcome::Unknown);
        UNIT_ASSERT_VALUES_EQUAL(result.OperationId, "persisted-operation");
        UNIT_ASSERT_VALUES_EQUAL(result.Status, Ydb::StatusIds::INTERNAL_ERROR);
    }

    Y_UNIT_TEST(RevokeUsesPersistedBindingAndMissingDelegationIsSuccess) {
        TFixture f;
        f.Update();
        f.Submit(EDelegationOperationKind::Revoke);
        auto request = f.Take(f.Revokes);
        const auto& proto = request->Get<TControl::TEvRevokeDelegationRequest>()->Request;
        UNIT_ASSERT_VALUES_EQUAL(proto.service_id(), "persisted-service");
        UNIT_ASSERT_VALUES_EQUAL(proto.microservice_id(), "persisted-microservice");
        UNIT_ASSERT_VALUES_EQUAL(proto.resource().type(), "persisted-resource-type");
        UNIT_ASSERT_VALUES_EQUAL(proto.referrer().type(), "persisted-referrer-type");
        UNIT_ASSERT(proto.with_references());
        f.Reply<TControl::TEvRevokeDelegationResponse>(*request, true, grpc::StatusCode::NOT_FOUND);
        f.OperationResult(EDelegationOperationOutcome::Succeeded);
    }

    Y_UNIT_TEST(SetupConflictDoesNotProveOriginalOperationFailed) {
        for (auto code : {grpc::StatusCode::FAILED_PRECONDITION, grpc::StatusCode::ALREADY_EXISTS}) {
            TFixture f;
            f.Update();
            f.Submit();
            auto request = f.Take(f.Setups);
            f.Reply<TControl::TEvSetupDelegationResponse>(*request, false, code);
            f.OperationResult(EDelegationOperationOutcome::Unknown);
            UNIT_ASSERT(f.Setups.empty());
        }
    }
}

Y_UNIT_TEST_SUITE(IamDelegationSettings) {
    Y_UNIT_TEST(ProtobufAndReplicationFallbackPreserveIdentity) {
        NKikimrConfig::TIamConfig config;
        config.SetServiceControlEndpoint("control");
        config.SetResourceManagerEndpoint("resource-manager");
        config.SetSystemTokenName("system-account");
        config.SetDelegationTabletId(NTests::TTestContext::TabletId);
        NKikimrReplication::TReplicationDefaults defaults;
        auto& shared = *defaults.MutableIamServiceControl();
        shared.SetEndpoint("token");
        shared.SetServiceId("ydb");
        shared.SetMicroserviceId("data-plane");
        shared.SetResourceType("resource-manager.cloud");
        shared.SetEnableSsl(false);
        const auto settings = TIamDelegationSettings::FromConfig(config, defaults);
        UNIT_ASSERT(settings.ValidateForDelegation().empty());
        UNIT_ASSERT(settings.CanResolveCloud());
        UNIT_ASSERT_VALUES_EQUAL(settings.Config.GetTokenServiceEndpoint(), "token");
        UNIT_ASSERT_VALUES_EQUAL(settings.Config.GetServiceId(), "ydb");
        UNIT_ASSERT_VALUES_EQUAL(settings.Config.GetMicroserviceId(), "data-plane");
        UNIT_ASSERT_VALUES_EQUAL(settings.Config.GetResourceType(), "resource-manager.cloud");
        UNIT_ASSERT_VALUES_EQUAL(settings.Config.GetSystemTokenName(), "system-account");
        UNIT_ASSERT(!settings.Config.GetEnableSsl());
        UNIT_ASSERT(!config.HasServiceId()); // normalization does not mutate caller config

        config.SetEnableSsl(true);
        config.SetTokenServiceEndpoint("own-token");
        config.SetServiceId("own-service");
        config.SetMicroserviceId("own-microservice");
        config.SetResourceType("own-resource");
        const auto explicitSettings = TIamDelegationSettings::FromConfig(config, defaults);
        UNIT_ASSERT_VALUES_EQUAL(explicitSettings.Config.SerializeAsString(), config.SerializeAsString());
        const auto copy = TIamDelegationSettings::FromConfig(config);
        UNIT_ASSERT_VALUES_EQUAL(copy.Config.SerializeAsString(), config.SerializeAsString());
    }

    Y_UNIT_TEST(ValidationAndCloudResolutionRemainSeparate) {
        NKikimrConfig::TIamConfig config;
        config.SetTokenServiceEndpoint("token");
        config.SetServiceId("ydb");
        config.SetMicroserviceId("data-plane");
        config.SetResourceType("resource-manager.cloud");
        auto settings = TIamDelegationSettings::FromConfig(config);
        UNIT_ASSERT(settings.Validate().empty());
        UNIT_ASSERT_STRING_CONTAINS(settings.ValidateForDelegation(), "ServiceControlEndpoint");
        UNIT_ASSERT_STRING_CONTAINS(settings.ValidateForDelegation(), "SystemTokenName");
        UNIT_ASSERT(!settings.CanResolveCloud());
        config.SetServiceControlEndpoint("control");
        config.SetSystemTokenName("system-account");
        config.SetDelegationTabletId(NTests::TTestContext::TabletId);
        settings = TIamDelegationSettings::FromConfig(config);
        UNIT_ASSERT(settings.ValidateForDelegation().empty());
        UNIT_ASSERT(!settings.CanResolveCloud());
        settings.Config.SetResourceManagerEndpoint("resource-manager");
        UNIT_ASSERT(settings.CanResolveCloud());
    }
}

} // namespace NKikimr::NIamDelegation
