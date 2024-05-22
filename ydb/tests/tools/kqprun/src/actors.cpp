#include "actors.h"

#include <ydb/core/kqp/common/simple/services.h>


namespace NKqpRun {

namespace {

class TRunScriptActorMock : public NActors::TActorBootstrapped<TRunScriptActorMock> {
public:
    TRunScriptActorMock(THolder<NKikimr::NKqp::TEvKqp::TEvQueryRequest> request,
        NThreading::TPromise<NKikimr::NKqp::TEvKqp::TEvQueryResponse::TPtr> promise,
        ui64 resultRowsLimit, ui64 resultSizeLimit, std::vector<Ydb::ResultSet>& resultSets, TString& queryPlan)
        : Request_(std::move(request))
        , Promise_(promise)
        , ResultRowsLimit_(std::numeric_limits<ui64>::max())
        , ResultSizeLimit_(std::numeric_limits<i64>::max())
        , ResultSets_(resultSets)
        , QueryPlan_(queryPlan)
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
        }

        if (!ResultSets_[resultSetIndex].truncated()) {
            for (auto& row : *ev->Get()->Record.MutableResultSet()->mutable_rows()) {
                if (static_cast<ui64>(ResultSets_[resultSetIndex].rows_size()) >= ResultRowsLimit_) {
                    ResultSets_[resultSetIndex].set_truncated(true);
                    break;
                }

                if (ResultSets_[resultSetIndex].ByteSizeLong() + row.ByteSizeLong() > ResultSizeLimit_) {
                    ResultSets_[resultSetIndex].set_truncated(true);
                    break;
                }

                *ResultSets_[resultSetIndex].add_rows() = std::move(row);
            }
            *ResultSets_[resultSetIndex].mutable_columns() = ev->Get()->Record.GetResultSet().columns();
        }

        Send(ev->Sender, response.Release());
    }
    
    void Handle(NKikimr::NKqp::TEvKqp::TEvQueryResponse::TPtr& ev) {
        Promise_.SetValue(std::move(ev));
        PassAway();
    }

    void Handle(NKikimr::NKqp::TEvKqpExecuter::TEvExecuterProgress::TPtr& ev) {
        QueryPlan_ = ev->Get()->Record.GetQueryPlan();
    }

private:
    THolder<NKikimr::NKqp::TEvKqp::TEvQueryRequest> Request_;
    NThreading::TPromise<NKikimr::NKqp::TEvKqp::TEvQueryResponse::TPtr> Promise_;
    ui64 ResultRowsLimit_;
    ui64 ResultSizeLimit_;
    std::vector<Ydb::ResultSet>& ResultSets_;
    TString& QueryPlan_;
};

}  // anonymous namespace

NActors::IActor* CreateRunScriptActorMock(THolder<NKikimr::NKqp::TEvKqp::TEvQueryRequest> request,
    NThreading::TPromise<NKikimr::NKqp::TEvKqp::TEvQueryResponse::TPtr> promise,
    ui64 resultRowsLimit, ui64 resultSizeLimit, std::vector<Ydb::ResultSet>& resultSets, TString& queryPlan) {
    return new TRunScriptActorMock(std::move(request), promise, resultRowsLimit, resultSizeLimit, resultSets, queryPlan);
}

}  // namespace NKqpRun
