#include "run_query.h"

#include <ydb/library/actors/core/executor_thread.h>

namespace NKikimr::NSQS {

    void RunYqlQuery(
        const TString& query,
        std::optional<NYdb::TParams> params,
        bool readonly,
        TDuration sendAfter,
        const TString& database,
        const TActorContext& ctx
    ) {
        auto ev = MakeHolder<NKqp::TEvKqp::TEvQueryRequest>();
        auto* request = ev->Record.MutableRequest();

        request->SetAction(NKikimrKqp::QUERY_ACTION_EXECUTE);
        request->SetType(NKikimrKqp::QUERY_TYPE_SQL_DML);
        request->SetKeepSession(false);
        request->SetQuery(query);
        request->SetUsePublicResponseDataFormat(true);

        if (database) {
            request->SetDatabase(database);
        }

        request->MutableQueryCachePolicy()->set_keep_in_cache(true);

        if (readonly) {
            request->MutableTxControl()->mutable_begin_tx()->mutable_stale_read_only();
        } else {
            request->MutableTxControl()->mutable_begin_tx()->mutable_serializable_read_write();
        }
        request->MutableTxControl()->set_commit_tx(true);

        if (params) {
            request->MutableYdbParameters()->swap(*(NYdb::TProtoAccessor::GetProtoMapPtr(params.value())));
        }

        auto kqpActor = NKqp::MakeKqpProxyID(ctx.SelfID.NodeId());
        if (sendAfter == TDuration::Zero()) {
            ctx.Send(kqpActor, ev.Release());
        } else {
            ctx.ExecutorThread.Schedule(sendAfter, new IEventHandle(kqpActor, ctx.SelfID, ev.Release()));
        }
    }


} // namespace NKikimr::NSQS
