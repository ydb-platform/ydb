#include <ydb/core/kqp/common/events/events.h>
#include <ydb/core/kqp/executer_actor/kqp_executer.h>


namespace NKqpRun {

using TProgressCallback = std::function<void(const NKikimrKqp::TEvExecuterProgress&)>;

NActors::IActor* CreateRunScriptActorMock(THolder<NKikimr::NKqp::TEvKqp::TEvQueryRequest> request,
    NThreading::TPromise<NKikimr::NKqp::TEvKqp::TEvQueryResponse::TPtr> promise,
    ui64 resultRowsLimit, ui64 resultSizeLimit, std::vector<Ydb::ResultSet>& resultSets,
    TProgressCallback progressCallback);

NActors::IActor* CreateResourcesWaiterActor(NThreading::TPromise<void> promise, i32 expectedNodeCount);

}  // namespace NKqpRun
