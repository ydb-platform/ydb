#include "actors.h"

#include <library/cpp/colorizer/colors.h>

#include <ydb/core/kqp/common/simple/services.h>
#include <ydb/core/kqp/rm_service/kqp_rm_service.h>


namespace NKqpRun {

namespace {

class TRunScriptActorMock : public NActors::TActorBootstrapped<TRunScriptActorMock> {
public:
    TRunScriptActorMock(TQueryRequest request, NThreading::TPromise<TQueryResponse> promise, TProgressCallback progressCallback)
        : TargetNode_(request.TargetNode)
        , Request_(std::move(request.Event))
        , Promise_(promise)
        , ResultRowsLimit_(std::numeric_limits<ui64>::max())
        , ResultSizeLimit_(std::numeric_limits<i64>::max())
        , ProgressCallback_(progressCallback)
    {
        if (request.ResultRowsLimit) {
            ResultRowsLimit_ = request.ResultRowsLimit;
        }
        if (request.ResultSizeLimit) {
            ResultSizeLimit_ = request.ResultSizeLimit;
        }
    }

    void Bootstrap() {
        NActors::ActorIdToProto(SelfId(), Request_->Record.MutableRequestActorId());
        Send(NKikimr::NKqp::MakeKqpProxyID(TargetNode_), std::move(Request_));

        Become(&TRunScriptActorMock::StateFunc);
    }

    STRICT_STFUNC(StateFunc,
        hFunc(NKikimr::NKqp::TEvKqpExecuter::TEvStreamData, Handle);
        hFunc(NKikimr::NKqp::TEvKqp::TEvQueryResponse, Handle);
        hFunc(NKikimr::NKqp::TEvKqpExecuter::TEvExecuterProgress, Handle);
    )
    
    void Handle(NKikimr::NKqp::TEvKqpExecuter::TEvStreamData::TPtr& ev) {
        auto response = MakeHolder<NKikimr::NKqp::TEvKqpExecuter::TEvStreamDataAck>();
        response->Record.SetSeqNo(ev->Get()->Record.GetSeqNo());
        response->Record.SetFreeSpace(ResultSizeLimit_);

        auto resultSetIndex = ev->Get()->Record.GetQueryResultIndex();
        if (resultSetIndex >= ResultSets_.size()) {
            ResultSets_.resize(resultSetIndex + 1);
            ResultSetSizes_.resize(resultSetIndex + 1, 0);
        }

        if (!ResultSets_[resultSetIndex].truncated()) {
            ui64& resultSetSize = ResultSetSizes_[resultSetIndex];
            for (auto& row : *ev->Get()->Record.MutableResultSet()->mutable_rows()) {
                if (static_cast<ui64>(ResultSets_[resultSetIndex].rows_size()) >= ResultRowsLimit_) {
                    ResultSets_[resultSetIndex].set_truncated(true);
                    break;
                }

                auto rowSize = row.ByteSizeLong();
                if (resultSetSize + rowSize > ResultSizeLimit_) {
                    ResultSets_[resultSetIndex].set_truncated(true);
                    break;
                }

                resultSetSize += rowSize;
                *ResultSets_[resultSetIndex].add_rows() = std::move(row);
            }
            if (!ResultSets_[resultSetIndex].columns_size()) {
                *ResultSets_[resultSetIndex].mutable_columns() = ev->Get()->Record.GetResultSet().columns();
            }
        }

        Send(ev->Sender, response.Release());
    }

    void Handle(NKikimr::NKqp::TEvKqp::TEvQueryResponse::TPtr& ev) {
        Promise_.SetValue(TQueryResponse{.Response = std::move(ev), .ResultSets = std::move(ResultSets_)});
        PassAway();
    }

    void Handle(NKikimr::NKqp::TEvKqpExecuter::TEvExecuterProgress::TPtr& ev) {
        if (ProgressCallback_) {
            ProgressCallback_(ev->Get()->Record);
        }
    }

private:
    ui32 TargetNode_ = 0;
    std::unique_ptr<NKikimr::NKqp::TEvKqp::TEvQueryRequest> Request_;
    NThreading::TPromise<TQueryResponse> Promise_;
    ui64 ResultRowsLimit_;
    ui64 ResultSizeLimit_;
    TProgressCallback ProgressCallback_;
    std::vector<Ydb::ResultSet> ResultSets_;
    std::vector<ui64> ResultSetSizes_;
};

class TAsyncQueryRunnerActor : public NActors::TActor<TAsyncQueryRunnerActor> {
    using TBase = NActors::TActor<TAsyncQueryRunnerActor>;

    struct TRequestInfo {
        TInstant StartTime;
        NThreading::TFuture<TQueryResponse> RequestFuture;
    };

public:
    TAsyncQueryRunnerActor(const TAsyncQueriesSettings& settings)
        : TBase(&TAsyncQueryRunnerActor::StateFunc)
        , Settings_(settings)
    {
        RunningRequests_.reserve(Settings_.InFlightLimit);
    }

    STRICT_STFUNC(StateFunc,
        hFunc(TEvPrivate::TEvStartAsyncQuery, Handle);
        hFunc(TEvPrivate::TEvAsyncQueryFinished, Handle);
        hFunc(TEvPrivate::TEvFinalizeAsyncQueryRunner, Handle);
    )

    void Handle(TEvPrivate::TEvStartAsyncQuery::TPtr& ev) {
        DelayedRequests_.emplace(std::move(ev));
        StartDelayedRequests();
    }

    void Handle(TEvPrivate::TEvAsyncQueryFinished::TPtr& ev) {
        const ui64 requestId = ev->Get()->RequestId;
        RequestsLatency += TInstant::Now() - RunningRequests_[requestId].StartTime;
        RunningRequests_.erase(requestId);

        const auto& response = ev->Get()->Result.Response->Get()->Record.GetRef();
        const auto status = response.GetYdbStatus();

        if (status == Ydb::StatusIds::SUCCESS) {
            Completed_++;
            if (Settings_.Verbose == TAsyncQueriesSettings::EVerbose::EachQuery) {
                Cout << CoutColors_.Green() << TInstant::Now().ToIsoStringLocal() << " Request #" << requestId << " completed. " << CoutColors_.Yellow() << GetInfoString() << CoutColors_.Default() << Endl;
            }
        } else {
            Failed_++;
            NYql::TIssues issues;
            NYql::IssuesFromMessage(response.GetResponse().GetQueryIssues(), issues);
            Cout << CoutColors_.Red() << TInstant::Now().ToIsoStringLocal() << " Request #" << requestId << " failed " << status << ". " << CoutColors_.Yellow() << GetInfoString() << "\n" << CoutColors_.Red() << "Issues:\n" << issues.ToString() << CoutColors_.Default();
        }

        if (Settings_.Verbose == TAsyncQueriesSettings::EVerbose::Final && TInstant::Now() - LastReportTime > TDuration::Seconds(1)) {
            Cout << CoutColors_.Green() << TInstant::Now().ToIsoStringLocal() << " Finished " << Failed_ + Completed_ << " requests. " << CoutColors_.Yellow() << GetInfoString() << CoutColors_.Default() << Endl;
            LastReportTime = TInstant::Now();
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

private:
    void StartDelayedRequests() {
        while (!DelayedRequests_.empty() && (!Settings_.InFlightLimit || RunningRequests_.size() < Settings_.InFlightLimit)) {
            auto request = std::move(DelayedRequests_.front());
            DelayedRequests_.pop();

            auto promise = NThreading::NewPromise<TQueryResponse>();
            Register(CreateRunScriptActorMock(std::move(request->Get()->Request), promise, nullptr));
            RunningRequests_[RequestId_] = {
                .StartTime = TInstant::Now(),
                .RequestFuture = promise.GetFuture().Subscribe([id = RequestId_, this](const NThreading::TFuture<TQueryResponse>& f) {
                    Send(SelfId(), new TEvPrivate::TEvAsyncQueryFinished(id, std::move(f.GetValue())));
                })
            };

            MaxInFlight_ = std::max(MaxInFlight_, RunningRequests_.size());
            if (Settings_.Verbose == TAsyncQueriesSettings::EVerbose::EachQuery) {
                Cout << TStringBuilder() << CoutColors_.Cyan() << TInstant::Now().ToIsoStringLocal() << " Request #" << RequestId_ << " started. " << CoutColors_.Yellow() << GetInfoString() << CoutColors_.Default() << "\n";
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
            Cout << TStringBuilder() << CoutColors_.Cyan() << TInstant::Now().ToIsoStringLocal() << " All async requests finished. " << CoutColors_.Yellow() << GetInfoString() << CoutColors_.Default() << "\n";
        }

        FinalizePromise_->SetValue();
        PassAway();
        return true;
    }

    TString GetInfoString() const {
        TStringBuilder result = TStringBuilder() << "completed: " << Completed_ << ", failed: " << Failed_ << ", in flight: " << RunningRequests_.size() << ", max in flight: " << MaxInFlight_ << ", spend time: " << TInstant::Now() - StartTime_;
        if (const auto amountRequests = Completed_ + Failed_) {
            result << ", average latency: " << RequestsLatency / amountRequests;
        }
        return result;
    }

private:
    const TAsyncQueriesSettings Settings_;
    const TInstant StartTime_ = TInstant::Now();
    const NColorizer::TColors CoutColors_ = NColorizer::AutoColors(Cout);

    std::optional<NThreading::TPromise<void>> FinalizePromise_;
    std::queue<TEvPrivate::TEvStartAsyncQuery::TPtr> DelayedRequests_;
    std::unordered_map<ui64, TRequestInfo> RunningRequests_;
    TInstant LastReportTime = TInstant::Now();

    ui64 RequestId_ = 1;
    ui64 MaxInFlight_ = 0;
    ui64 Completed_ = 0;
    ui64 Failed_ = 0;
    TDuration RequestsLatency;
};

class TResourcesWaiterActor : public NActors::TActorBootstrapped<TResourcesWaiterActor> {
    static constexpr TDuration REFRESH_PERIOD = TDuration::MilliSeconds(10);

public:
    TResourcesWaiterActor(NThreading::TPromise<void> promise, i32 expectedNodeCount)
        : ExpectedNodeCount_(expectedNodeCount)
        , Promise_(promise)
    {}

    void Bootstrap() {
        Become(&TResourcesWaiterActor::StateFunc);
        CheckResourcesPublish();
    }

    void Handle(NActors::TEvents::TEvWakeup::TPtr&) {
        CheckResourcesPublish();
    }

    void Handle(TEvPrivate::TEvResourcesInfo::TPtr& ev) {
        if (ev->Get()->NodeCount == ExpectedNodeCount_) {
            Promise_.SetValue();
            PassAway();
            return;
        }

        Schedule(REFRESH_PERIOD, new NActors::TEvents::TEvWakeup());
    }

    STRICT_STFUNC(StateFunc,
        hFunc(NActors::TEvents::TEvWakeup, Handle);
        hFunc(TEvPrivate::TEvResourcesInfo, Handle);
    )

private:
    void CheckResourcesPublish() {
        GetResourceManager();

        if (!ResourceManager_) {
            Schedule(REFRESH_PERIOD, new NActors::TEvents::TEvWakeup());
            return;
        }

        UpdateResourcesInfo();
    }

    void GetResourceManager() {
        if (ResourceManager_) {
            return;
        }
        ResourceManager_ = NKikimr::NKqp::TryGetKqpResourceManager(SelfId().NodeId());
    }

    void UpdateResourcesInfo() const {
        ResourceManager_->RequestClusterResourcesInfo(
        [selfId = SelfId(), actorContext = ActorContext()](TVector<NKikimrKqp::TKqpNodeResources>&& resources) {
            actorContext.Send(selfId, new TEvPrivate::TEvResourcesInfo(resources.size()));
        });
    }

private:
    const i32 ExpectedNodeCount_;
    NThreading::TPromise<void> Promise_;

    std::shared_ptr<NKikimr::NKqp::NRm::IKqpResourceManager> ResourceManager_;
};

}  // anonymous namespace

NActors::IActor* CreateRunScriptActorMock(TQueryRequest request, NThreading::TPromise<TQueryResponse> promise, TProgressCallback progressCallback) {
    return new TRunScriptActorMock(std::move(request), promise, progressCallback);
}

NActors::IActor* CreateAsyncQueryRunnerActor(const TAsyncQueriesSettings& settings) {
    return new TAsyncQueryRunnerActor(settings);
}

NActors::IActor* CreateResourcesWaiterActor(NThreading::TPromise<void> promise, i32 expectedNodeCount) {
    return new TResourcesWaiterActor(promise, expectedNodeCount);
}

}  // namespace NKqpRun
