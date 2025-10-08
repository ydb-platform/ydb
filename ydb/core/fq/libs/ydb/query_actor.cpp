#include <ydb/core/fq/libs/ydb/query_actor.h>
#include <ydb/library/actors/core/events.h>
#include <ydb/public/api/protos/ydb_status_codes.pb.h>

#include <yql/essentials/public/issue/yql_issue.h>
#include <ydb/library/query_actor/query_actor.h>


namespace NFq {

struct TEvPrivate {
    // Event ids
    enum EEv : ui32 {
        EvStart = EventSpaceBegin(NActors::TEvents::ES_PRIVATE),
        EvExecuteDataQueryResult,
        EvEnd
    };

    static_assert(EvEnd < EventSpaceEnd(NActors::TEvents::ES_PRIVATE), "expect EvEnd < EventSpaceEnd(TEvents::ES_PRIVATE)");

    template <typename TEv, ui32 EventType>
    struct TEvResultBase : public NActors::TEventLocal<TEv, EventType> {
        explicit TEvResultBase(Ydb::StatusIds::StatusCode status, NYql::TIssues issues = {})
            : Status(status)
            , Issues(std::move(issues))
        {}

        const Ydb::StatusIds::StatusCode Status;
        const NYql::TIssues Issues;
    };


    struct TEvExecuteDataQueryResult : public TEvResultBase<TEvExecuteDataQueryResult, EvExecuteDataQueryResult> {
        using TEvResultBase::TEvResultBase;
    };

};



// class TDataQuery : public NActors::TActorBootstrapped<TDataQuery> {
// public:
//     TDataQuery(
//         NThreading::TPromise<NYdb::NTable::TDataQueryResult> promise,
//         const TString& sql,
//         std::shared_ptr<NYdb::TParamsBuilder> params,
//         NYdb::NTable::TTxControl txControl)
//         : Promise(promise)
//         , Sql(sql)
//         , Params(params)
//         , TxControl(txControl) {
//     }     
    
//     void Bootstrap() {
//         Become(&TDataQuery::StateFunc);
//         Cerr << "Bootstrap NewDataQuery" << Endl;
//         NActors::TActivationContext::AsActorContext().Register(new TExecuteDataQuery::TRetry(SelfId(), "", "", Sql, Params, TxControl));

//     }
//     STFUNC(StateFunc) {
//         switch (ev->GetTypeRewrite()) {
//             hFunc(TEvPrivate::TEvExecuteDataQueryResult, Handle);
//         }
//     }

//     void Handle(TEvPrivate::TEvExecuteDataQueryResult::TPtr& /*ev*/) {
//        Cerr << "TEvExecuteDataQueryResult " << Endl;
//     }
// private: 
//     NThreading::TPromise<NYdb::NTable::TDataQueryResult> Promise;
//     const TString Sql;
//     std::shared_ptr<NYdb::TParamsBuilder> Params;
//     const NYdb::NTable::TTxControl TxControl;
// };


// std::unique_ptr<NActors::IActor> NewDataQuery(
//     NThreading::TPromise<NYdb::NTable::TDataQueryResult> promise,
//     const TString& sql,
//     std::shared_ptr<NYdb::TParamsBuilder> params,
//     NYdb::NTable::TTxControl txControl) {
//     Cerr << "NewDataQuery" << Endl;

//     return std::unique_ptr<NActors::IActor>(new TDataQuery(promise, sql, params, txControl));
//     //return std::unique_ptr<NActors::IActor>(new TRowDispatcher(
// }



std::unique_ptr<TQueryActor> MakeQueryActor() {
    Cerr << "NewDataQuery" << Endl;

    return std::unique_ptr<TQueryActor>(new TQueryActor());
    //return std::unique_ptr<NActors::IActor>(new TRowDispatcher(
}


} // namespace NFq
