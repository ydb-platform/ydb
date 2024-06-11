#include "actors.h"

#include <ydb/core/kqp/common/simple/services.h>
#include <ydb/core/kqp/rm_service/kqp_rm_service.h>


namespace NKqpRun {

namespace {

class TRunScriptActorMock : public NActors::TActorBootstrapped<TRunScriptActorMock> {
public:
    TRunScriptActorMock(THolder<NKikimr::NKqp::TEvKqp::TEvQueryRequest> request,
        NThreading::TPromise<TQueryResponse> promise, ui64 resultRowsLimit, ui64 resultSizeLimit,
        TProgressCallback progressCallback)
        : Request_(std::move(request))
        , Promise_(promise)
        , ResultRowsLimit_(std::numeric_limits<ui64>::max())
        , ResultSizeLimit_(std::numeric_limits<i64>::max())
        , ProgressCallback_(progressCallback)
    {
        if (resultRowsLimit) {
            ResultRowsLimit_ = resultRowsLimit;
        }
        if (resultSizeLimit) {
            ResultSizeLimit_ = resultSizeLimit;
        }
    }

    void Bootstrap() {
        NActors::ActorIdToProto(SelfId(), Request_->Record.MutableRequestActorId());
        Send(NKikimr::NKqp::MakeKqpProxyID(SelfId().NodeId()), std::move(Request_));

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
    THolder<NKikimr::NKqp::TEvKqp::TEvQueryRequest> Request_;
    NThreading::TPromise<TQueryResponse> Promise_;
    ui64 ResultRowsLimit_;
    ui64 ResultSizeLimit_;
    TProgressCallback ProgressCallback_;
    std::vector<Ydb::ResultSet> ResultSets_;
    std::vector<ui64> ResultSetSizes_;
};

class TResourcesWaiterActor : public NActors::TActorBootstrapped<TResourcesWaiterActor> {
    struct TEvPrivate {
        enum EEv : ui32 {
            EvResourcesInfo = EventSpaceBegin(NActors::TEvents::ES_PRIVATE),

            EvEnd
        };

        static_assert(EvEnd < EventSpaceEnd(NActors::TEvents::ES_PRIVATE), "expect EvEnd < EventSpaceEnd(NActors::TEvents::ES_PRIVATE)");

        struct TEvResourcesInfo : public NActors::TEventLocal<TEvResourcesInfo, EvResourcesInfo> {
            explicit TEvResourcesInfo(i32 nodeCount)
                : NodeCount(nodeCount)
            {}

            const i32 NodeCount;
        };
    };

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

NActors::IActor* CreateRunScriptActorMock(THolder<NKikimr::NKqp::TEvKqp::TEvQueryRequest> request,
    NThreading::TPromise<TQueryResponse> promise, ui64 resultRowsLimit, ui64 resultSizeLimit,
    TProgressCallback progressCallback) {
    return new TRunScriptActorMock(std::move(request), promise, resultRowsLimit, resultSizeLimit, progressCallback);
}

NActors::IActor* CreateResourcesWaiterActor(NThreading::TPromise<void> promise, i32 expectedNodeCount) {
    return new TResourcesWaiterActor(promise, expectedNodeCount);
}

}  // namespace NKqpRun
