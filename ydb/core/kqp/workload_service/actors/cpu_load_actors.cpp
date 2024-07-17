#include "actors.h"

#include <ydb/core/kqp/workload_service/common/events.h>

#include <ydb/library/query_actor/query_actor.h>


namespace NKikimr::NKqp::NWorkload {

namespace {

class TCpuLoadFetcherActor : public NKikimr::TQueryBase {
    using TBase = NKikimr::TQueryBase;

public:
    TCpuLoadFetcherActor()
        : TBase(NKikimrServices::KQP_WORKLOAD_SERVICE)
    {
        SetOperationInfo(__func__, "");
    }

    void OnRunQuery() override {
        TString sql = TStringBuilder() << R"(
            -- TCpuLoadFetcherActor::OnRunQuery

            SELECT
                SUM(CpuThreads) AS ThreadsCount,
                SUM(CpuThreads * (1.0 - CpuIdle)) AS TotalLoad
            FROM `.sys/nodes`;
        )";

        RunDataQuery(sql);
    }

    void OnQueryResult() override {
        if (ResultSets.size() != 1) {
            Finish(Ydb::StatusIds::INTERNAL_ERROR, "Unexpected database response");
            return;
        }

        NYdb::TResultSetParser result(ResultSets[0]);
        if (!result.TryNextRow()) {
            Finish(Ydb::StatusIds::INTERNAL_ERROR, "Unexpected database response");
            return;
        }

        ThreadsCount = result.ColumnParser("ThreadsCount").GetOptionalUint64().GetOrElse(0);
        TotalLoad = result.ColumnParser("TotalLoad").GetOptionalDouble().GetOrElse(0.0);

        if (!ThreadsCount) {
            Finish(Ydb::StatusIds::NOT_FOUND, "Cpu info not found");
            return;
        }

        Finish();
    }

    void OnFinish(Ydb::StatusIds::StatusCode status, NYql::TIssues&& issues) override {
        if (status == Ydb::StatusIds::SUCCESS) {
            Send(Owner, new TEvPrivate::TEvCpuLoadResponse(Ydb::StatusIds::SUCCESS, TotalLoad / ThreadsCount, ThreadsCount, std::move(issues)));
        } else {
            Send(Owner, new TEvPrivate::TEvCpuLoadResponse(status, 0.0, 0, std::move(issues)));
        }
    }

private:
    double TotalLoad = 0.0;
    ui64 ThreadsCount = 0;
};

}  // anonymous namespace

IActor* CreateCpuLoadFetcherActor(const TActorId& replyActorId) {
    return new TQueryRetryActor<TCpuLoadFetcherActor, TEvPrivate::TEvCpuLoadResponse>(replyActorId);
}

}  // NKikimr::NKqp::NWorkload
