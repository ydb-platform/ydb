#include <ydb/core/fq/libs/ydb/query_actor.h>
#include <ydb/core/fq/libs/actors/logging/log.h>

#include <ydb/library/actors/core/events.h>
#include <ydb/library/query_actor/query_actor.h>

#include <ydb/public/api/protos/ydb_status_codes.pb.h>
#include <yql/essentials/public/issue/yql_issue.h>
#include <library/cpp/threading/future/core/future.h>

#define LOG_T(stream) LOG_TRACE_S(*NActors::TlsActivationContext, LogComponent, LogPrefix() << stream)

namespace NFq {

class TQuerySession final : public NKikimr::TQueryBase {
    using TBase = NKikimr::TQueryBase;

    struct TDataQuery{
        TString Sql;
        std::shared_ptr<NYdb::TParamsBuilder> Params;
        TEvQuerySession::TTxControl TxControl;
        NThreading::TPromise<NYdb::NTable::TDataQueryResult> Promise;
    };

public:
    TQuerySession()
        : NKikimr::TQueryBase(NKikimrServices::STREAMS_STORAGE_SERVICE, {}, {}, false, true) {
    }

private:

    TString LogPrefix() const {
        return "TQuerySession: ";
    }

    TAutoPtr<NActors::IEventHandle> AfterRegister(const NActors::TActorId &self, const NActors::TActorId& parentId) override {
        Y_UNUSED(parentId);
        Become(&TQuerySession::StateWork);
        return new NActors::IEventHandle(self, self, new NActors::TEvents::TEvBootstrap());
    }
    
    STATEFN(StateWork) {
        switch (ev->GetTypeRewrite()) {
            cFunc(NActors::TEvents::TEvBootstrap::EventType, DoBootstrap);
            hFunc(TEvQuerySession::TEvExecuteDataQuery, Handle);
            hFunc(TEvQuerySession::TEvRollbackTransaction, Handle);
            sFunc(NActors::TEvents::TEvPoison, DoPassAway);
            default:
                TBase::StateFunc(ev);
        }
    }
    
    void DoBootstrap() {
        TBase::Bootstrap();
        Become(&TQuerySession::StateWork);
    }

    void Handle(TEvQuerySession::TEvExecuteDataQuery::TPtr& ev) {
        Y_ABORT_UNLESS(!IsExecuting);
        Y_ABORT_UNLESS(!DataQuery);

        DataQuery = TDataQuery{ev->Get()->Sql, ev->Get()->Params, ev->Get()->TxControl, ev->Get()->Promise};
        ProcessQueries();
    }

    void Handle(TEvQuerySession::TEvRollbackTransaction::TPtr& /*ev*/) {
        Y_ABORT_UNLESS(!DataQuery);
        Finish(Ydb::StatusIds::INTERNAL_ERROR, "Rollback transaction", true);
    }

    void OnRunQuery() final {
        ReadyToExecute = true;
        ProcessQueries();
    }

    void OnQueryResult() final {
        Y_ABORT_UNLESS(IsExecuting);
        auto promise = DataQuery->Promise;
        DataQuery = std::nullopt;
        auto status = NYdb::TStatus(NYdb::EStatus::SUCCESS, NYdb::NIssue::TIssues());
        
        IsExecuting = false;
        promise.SetValue(NYdb::NTable::TDataQueryResult(std::move(status), std::move(ResultSets), std::nullopt, std::nullopt, false, std::nullopt));
        if (IsFinishing) {
            Finish();
        }
    }

    void OnFinish(Ydb::StatusIds::StatusCode statusCode, NYql::TIssues&& issues) final {
        if (DataQuery) {          
            NYdb::TStatus status(static_cast<NYdb::EStatus>(statusCode), NYdb::NAdapters::ToSdkIssues(issues));
            DataQuery->Promise.SetValue(NYdb::NTable::TDataQueryResult(std::move(status), std::move(ResultSets), std::nullopt, std::nullopt, false, std::nullopt));
        }
        DataQuery = std::nullopt;
    }

    void ProcessQueries() {
        if (IsExecuting || !ReadyToExecute || !DataQuery) {
            return;
        }
        IsExecuting = true;

        TQueryBase::TTxControl tx;
        if (DataQuery->TxControl.Begin) {
            tx.Begin(true);
        }
        if (DataQuery->TxControl.Commit) {
            tx.Commit(true);
        }
        if (DataQuery->TxControl.Continue) {
            tx.Continue(true);
        }
        if (DataQuery->TxControl.SnapshotRead) {
            tx.SnapshotRead(true);
        }
        LOG_T("Run query " << DataQuery->Sql << " commit " << tx.Commit_);
        RunDataQuery(DataQuery->Sql, DataQuery->Params.get(), tx);
    }

    void DoPassAway() {
        if (DataQuery) {
            IsFinishing = true;
            return;
        }
        Finish();
    }

private:
    std::optional<TDataQuery> DataQuery;
    bool IsExecuting = false;
    bool ReadyToExecute = false;
    bool IsFinishing = false;
};

std::unique_ptr<NActors::IActor> MakeQuerySession() {
    return std::unique_ptr<NActors::IActor>(new TQuerySession());
}


} // namespace NFq
