#include <ydb/core/fq/libs/ydb/local_table_client.h>

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/table/table.h>
#include <library/cpp/threading/future/core/future.h>
#include <ydb/core/fq/libs/ydb/local_session.h>
#include <library/cpp/retry/retry_policy.h>

#include <ydb/core/fq/libs/ydb/table_client.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/actorsystem.h>
#include <ydb/library/actors/core/events.h>
#include <ydb/library/actors/core/hfunc.h>

namespace NFq {

namespace {

class TRetryOperationActor : public NActors::TActorBootstrapped<TRetryOperationActor> {

    using IRetryPolicy = IRetryPolicy<const NYdb::TStatus&>;

    struct TEvPrivate {
        // Event ids
        enum EEv : ui32 {
            EvResult = EventSpaceBegin(NActors::TEvents::ES_PRIVATE),
            EvEnd
        };
        static_assert(EvEnd < EventSpaceEnd(NActors::TEvents::ES_PRIVATE), "expect EvEnd < EventSpaceEnd(NActors::TEvents::ES_PRIVATE)");
        struct TEvResult : NActors::TEventLocal<TEvResult, EvResult> {
            explicit TEvResult(const NYdb::TStatus& status) 
                : Status(status) {
            }
            NYdb::TStatus Status;
        };
    };

public:
    TRetryOperationActor(
        NThreading::TPromise<NYdb::TStatus> promise,
        TOperationFunc&& operation,
        const NYdb::NRetry::TRetryOperationSettings& settings)
        : Promise(promise)
        , RetryPolicy(IRetryPolicy::GetExponentialBackoffPolicy(
            Retryable, TDuration::MilliSeconds(10), 
            TDuration::MilliSeconds(200),
            settings.MaxTimeout_,
            settings.MaxRetries_, settings.MaxTimeout_
        ))
        , Operation(operation) {
    }

    ~TRetryOperationActor() override {
        if (!Promise.HasValue()) {
            auto status = NYdb::TStatus(NYdb::EStatus::INTERNAL_ERROR,
                NYdb::NIssue::TIssues({NYdb::NIssue::TIssue("Destructor calling")}));
            Promise.SetValue(status);
        }
    }

    void Bootstrap() {
        Become(&TRetryOperationActor::StateFunc);
        StartOperation();
    }

    STRICT_STFUNC(StateFunc,
        hFunc(NActors::TEvents::TEvWakeup, Wakeup);
        hFunc(TEvPrivate::TEvResult, Handle);
    )

private:
    void Wakeup(NActors::TEvents::TEvWakeup::TPtr&) {
        StartOperation();
    }

    void Handle(TEvPrivate::TEvResult::TPtr& ev) {
        const auto& status = ev->Get()->Status;
        if (!status.IsSuccess()) {
            ScheduleRetry(status);
        } else {
            Promise.SetValue(status);
            PassAway();
        }
    }

    void StartOperation() {        
        auto session = CreateLocalSession();
        auto future = Operation(session);
        future.Subscribe([selfId = SelfId(), actorSystem =  NActors::TActivationContext::ActorSystem()](const NYdb::TAsyncStatus& result){
            actorSystem->Send(selfId, new TEvPrivate::TEvResult(result.GetValue()));
        });
    }

    void ScheduleRetry(const NYdb::TStatus& status) {
        if (RetryState == nullptr) {
            RetryState = RetryPolicy->CreateRetryState();
        }
        if (auto delay = RetryState->GetNextRetryDelay(status)) {
            Schedule(*delay, new NActors::TEvents::TEvWakeup());
        } else {
            NYdb::NIssue::TIssues issues;
            NYdb::NIssue::TIssue issue("MaxRetries is reached");
            for (const auto& i : status.GetIssues()) {
                issue.AddSubIssue(MakeIntrusive<NYdb::NIssue::TIssue>(i));
            }
            issues.AddIssue(std::move(issue));
            Promise.SetValue(NYdb::TStatus(NYdb::EStatus::INTERNAL_ERROR, std::move(issues)));
            PassAway();
        }
    }

    static ERetryErrorClass Retryable(const NYdb::TStatus& status) {
        if (status.IsSuccess()) {
            return ERetryErrorClass::NoRetry;
        }

        if (status.IsTransportError()) {
            return ERetryErrorClass::ShortRetry;
        }

        const NYdb::EStatus st = status.GetStatus();
        if (st == NYdb::EStatus::INTERNAL_ERROR
            || st == NYdb::EStatus::UNAVAILABLE
            || st == NYdb::EStatus::TIMEOUT
            || st == NYdb::EStatus::BAD_SESSION
            || st == NYdb::EStatus::SESSION_EXPIRED
            || st == NYdb::EStatus::SESSION_BUSY
            || st == NYdb::EStatus::ABORTED) {
            return ERetryErrorClass::ShortRetry;
        }

        if (st == NYdb::EStatus::OVERLOADED) {
            return ERetryErrorClass::LongRetry;
        }

        return ERetryErrorClass::NoRetry;
    }

private:
    NThreading::TPromise<NYdb::TStatus> Promise;
    const IRetryPolicy::TPtr RetryPolicy;
    IRetryPolicy::IRetryState::TPtr RetryState;
    TOperationFunc Operation;
};

struct TLocalYdbTableClient : public IYdbTableClient {

    NYdb::TAsyncStatus RetryOperation(
        TOperationFunc&& operation,
        const NYdb::NRetry::TRetryOperationSettings& settings = NYdb::NRetry::TRetryOperationSettings()) override {
        auto promise = NThreading::NewPromise<NYdb::TStatus>();
        NActors::TActivationContext::Register(new TRetryOperationActor(promise, std::move(operation), settings));
        return promise.GetFuture();
    }
};

} // namespace

IYdbTableClient::TPtr CreateLocalTableClient() {
    return MakeIntrusive<TLocalYdbTableClient>();
}

} // namespace NFq
