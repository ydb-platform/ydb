#pragma once

#include "settings.h"

#include <library/cpp/colorizer/colors.h>
#include <library/cpp/threading/future/core/future.h>

#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/events.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/public/api/protos/ydb_status_codes.pb.h>

#include <yql/essentials/public/issue/yql_issue.h>
#include <yql/essentials/public/issue/yql_issue_message.h>

#include <queue>

namespace NKikimrRun {

struct TEvPrivate {
    enum EEv : ui32 {
        EvStartAsyncQuery = EventSpaceBegin(NActors::TEvents::ES_PRIVATE),
        EvAsyncQueryFinished,
        EvFinalizeAsyncQueryRunner,
        EvEnd
    };

    static_assert(EvEnd < EventSpaceEnd(NActors::TEvents::ES_PRIVATE), "expect EvEnd < EventSpaceEnd(NActors::TEvents::ES_PRIVATE)");

    template <typename TQueryRequest>
    struct TEvStartAsyncQuery : public NActors::TEventLocal<TEvStartAsyncQuery<TQueryRequest>, EvStartAsyncQuery> {
        TEvStartAsyncQuery(TQueryRequest request, NThreading::TPromise<void> startPromise)
            : Request(std::move(request))
            , StartPromise(startPromise)
        {}

        TQueryRequest Request;
        NThreading::TPromise<void> StartPromise;
    };

    template <typename TQueryResponse>
    struct TEvAsyncQueryFinished : public NActors::TEventLocal<TEvAsyncQueryFinished<TQueryResponse>, EvAsyncQueryFinished> {
        TEvAsyncQueryFinished(ui64 requestId, TQueryResponse result)
            : RequestId(requestId)
            , Result(std::move(result))
        {}

        const ui64 RequestId;
        const TQueryResponse Result;
    };

    struct TEvFinalizeAsyncQueryRunner : public NActors::TEventLocal<TEvFinalizeAsyncQueryRunner, EvFinalizeAsyncQueryRunner> {
        explicit TEvFinalizeAsyncQueryRunner(NThreading::TPromise<void> finalizePromise)
            : FinalizePromise(finalizePromise)
        {}

        NThreading::TPromise<void> FinalizePromise;
    };
};

template <typename TQueryRequest, typename TQueryResponse>
class TAsyncQueryRunnerActorBase : public NActors::TActor<TAsyncQueryRunnerActorBase<TQueryRequest, TQueryResponse>> {
    using TBase = NActors::TActor<TAsyncQueryRunnerActorBase<TQueryRequest, TQueryResponse>>;

    struct TRequestInfo {
        TInstant StartTime;
        NThreading::TFuture<TQueryResponse> RequestFuture;
    };

public:
    TAsyncQueryRunnerActorBase(const TAsyncQueriesSettings& settings)
        : TBase(&TAsyncQueryRunnerActorBase::StateFunc)
        , Settings_(settings)
    {
        RunningRequests_.reserve(Settings_.InFlightLimit);
    }

    STRICT_STFUNC(StateFunc,
        hFunc(TEvPrivate::TEvStartAsyncQuery<TQueryRequest>, Handle);
        hFunc(TEvPrivate::TEvAsyncQueryFinished<TQueryResponse>, Handle);
        hFunc(TEvPrivate::TEvFinalizeAsyncQueryRunner, Handle);
    )

    void Handle(TEvPrivate::TEvStartAsyncQuery<TQueryRequest>::TPtr& ev) {
        DelayedRequests_.emplace(std::move(ev));
        StartDelayedRequests();
    }

    void Handle(TEvPrivate::TEvAsyncQueryFinished<TQueryResponse>::TPtr& ev) {
        const ui64 requestId = ev->Get()->RequestId;
        RequestsLatency_ += TInstant::Now() - RunningRequests_[requestId].StartTime;
        RunningRequests_.erase(requestId);

        const auto& response = ev->Get()->Result;

        if (response.IsSuccess()) {
            Completed_++;
            if (Settings_.Verbose == TAsyncQueriesSettings::EVerbose::EachQuery) {
                Cout << CoutColors_.Green() << TInstant::Now().ToIsoStringLocal() << " Request #" << requestId << " completed. " << CoutColors_.Yellow() << GetInfoString() << CoutColors_.Default() << Endl;
            }
        } else {
            Failed_++;
            Cout << CoutColors_.Red() << TInstant::Now().ToIsoStringLocal() << " Request #" << requestId << " failed " << response.GetStatus() << ". " << CoutColors_.Yellow() << GetInfoString() << "\n" << CoutColors_.Red() << "Issues:\n" << response.GetError() << CoutColors_.Default();
        }

        if (Settings_.Verbose == TAsyncQueriesSettings::EVerbose::Final && TInstant::Now() - LastReportTime_ > TDuration::Seconds(1)) {
            Cout << CoutColors_.Green() << TInstant::Now().ToIsoStringLocal() << " Finished " << Failed_ + Completed_ << " requests. " << CoutColors_.Yellow() << GetInfoString() << CoutColors_.Default() << Endl;
            LastReportTime_ = TInstant::Now();
        }

        StartDelayedRequests();
        TryFinalize();
    }

    void Handle(TEvPrivate::TEvFinalizeAsyncQueryRunner::TPtr& ev) {
        FinalizePromise_ = ev->Get()->FinalizePromise;
        if (!TryFinalize()) {
            Cout << CoutColors_.Yellow() << TInstant::Now().ToIsoStringLocal() << " Waiting for " << DelayedRequests_.size() + RunningRequests_.size() << " async queries..." << CoutColors_.Default() << Endl;
        }
    }

protected:
    virtual void RunQuery(TQueryRequest&& request, NThreading::TPromise<TQueryResponse> promise) = 0;

private:
    void StartDelayedRequests() {
        while (!DelayedRequests_.empty() && (!Settings_.InFlightLimit || RunningRequests_.size() < Settings_.InFlightLimit)) {
            auto request = std::move(DelayedRequests_.front());
            DelayedRequests_.pop();

            auto promise = NThreading::NewPromise<TQueryResponse>();
            RunQuery(std::move(request->Get()->Request), promise);
            RunningRequests_[RequestId_] = {
                .StartTime = TInstant::Now(),
                .RequestFuture = promise.GetFuture().Subscribe([id = RequestId_, this](const NThreading::TFuture<TQueryResponse>& f) {
                    this->Send(this->SelfId(), new TEvPrivate::TEvAsyncQueryFinished(id, std::move(f.GetValue())));
                })
            };

            MaxInFlight_ = std::max(MaxInFlight_, RunningRequests_.size());
            if (Settings_.Verbose == TAsyncQueriesSettings::EVerbose::EachQuery) {
                Cout << CoutColors_.Cyan() << TInstant::Now().ToIsoStringLocal() << " Request #" << RequestId_ << " started. " << CoutColors_.Yellow() << GetInfoString() << CoutColors_.Default() << "\n";
            }

            RequestId_++;
            request->Get()->StartPromise.SetValue();
        }
    }

    bool TryFinalize() {
        if (!FinalizePromise_ || !RunningRequests_.empty()) {
            return false;
        }

        if (Settings_.Verbose == TAsyncQueriesSettings::EVerbose::Final) {
            Cout << CoutColors_.Cyan() << TInstant::Now().ToIsoStringLocal() << " All async requests finished. " << CoutColors_.Yellow() << GetInfoString() << CoutColors_.Default() << "\n";
        }

        FinalizePromise_->SetValue();
        this->PassAway();
        return true;
    }

    TString GetInfoString() const {
        TStringBuilder result = TStringBuilder() << "completed: " << Completed_ << ", failed: " << Failed_ << ", in flight: " << RunningRequests_.size() << ", max in flight: " << MaxInFlight_ << ", spend time: " << TInstant::Now() - StartTime_;
        if (const auto amountRequests = Completed_ + Failed_) {
            result << ", average latency: " << RequestsLatency_ / amountRequests;
        }
        return result;
    }

private:
    const TAsyncQueriesSettings Settings_;
    const TInstant StartTime_ = TInstant::Now();
    const NColorizer::TColors CoutColors_ = NColorizer::AutoColors(Cout);

    std::optional<NThreading::TPromise<void>> FinalizePromise_;
    std::queue<typename TEvPrivate::TEvStartAsyncQuery<TQueryRequest>::TPtr> DelayedRequests_;
    std::unordered_map<ui64, TRequestInfo> RunningRequests_;
    TInstant LastReportTime_ = TInstant::Now();

    ui64 RequestId_ = 1;
    ui64 MaxInFlight_ = 0;
    ui64 Completed_ = 0;
    ui64 Failed_ = 0;
    TDuration RequestsLatency_;
};

}  // namespace NKikimrRun
