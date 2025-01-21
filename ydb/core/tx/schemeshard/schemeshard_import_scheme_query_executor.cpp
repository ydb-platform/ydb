#include "schemeshard_import_helpers.h"
#include "schemeshard_import_scheme_query_executor.h"
#include "schemeshard_private.h"

#include <ydb/core/base/appdata_fwd.h>
#include <ydb/core/kqp/common/events/events.h>
#include <ydb/core/kqp/common/simple/services.h>
#include <ydb/core/kqp/query_data/kqp_prepared_query.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>

#include <library/cpp/time_provider/time_provider.h>

using namespace NKikimr::NKqp;

namespace NKikimr::NSchemeShard {

class TSchemeQueryExecutor: public TActorBootstrapped<TSchemeQueryExecutor> {

    std::unique_ptr<TEvKqp::TEvCompileRequest> BuildCompileRequest() {
        UserToken.Reset(MakeIntrusive<NACLib::TUserToken>(""));

        TKqpQuerySettings querySettings(NKikimrKqp::EQueryType::QUERY_TYPE_SQL_GENERIC_QUERY);
        querySettings.IsInternalCall = true;

        GUCSettings = std::make_shared<TGUCSettings>();

        TKqpQueryId query(
            TString(DefaultKikimrPublicClusterName), // cluster
            Database, // database
            "", // database id
            SchemeQuery, // query text
            querySettings, // query settings
            nullptr, // query parameter types
            *GUCSettings // GUC settings
        );

        // TO DO: pass cancel after from the import operation
        auto deadline = TAppData::TimeProvider->Now() + TDuration::Minutes(1);
        TKqpCounters kqpCounters(AppData()->Counters, &TlsActivationContext->AsActorContext());
        IsInterestedInResult = std::make_shared<std::atomic<bool>>(true);
        UserRequestContext.Reset(MakeIntrusive<TUserRequestContext>());

        return std::make_unique<TEvKqp::TEvCompileRequest>(
            UserToken, // user token
            "", // client address
            Nothing(), // uid
            query, // TKqpQueryId
            false, // keep in cache
            true, // is query action == prepare?
            false, // per statement result
            deadline, // deadline
            kqpCounters.GetDbCounters(Database), // db counters
            GUCSettings, // GUC settings
            Nothing(), // application name
            IsInterestedInResult, // is still interested in result?
            UserRequestContext // user request context
        );
    }

    void PrepareSchemeQuery() {
        if (!Send(MakeKqpCompileServiceID(SelfId().NodeId()), BuildCompileRequest().release())) {
            return Reply(Ydb::StatusIds::INTERNAL_ERROR, "cannot send query request");
        }
        Become(&TThis::StateExecute);
    }

    void HandleCompileResponse(const TEvKqp::TEvCompileResponse::TPtr& ev) {
        const auto* result = ev->Get()->CompileResult.get();
        if (!result) {
            // TO DO: figure out the proper status for this situation.
            // Probably, just change the reply event to contain a plain bool status.
            return Reply(Ydb::StatusIds::BAD_REQUEST, "no compile result");
        }

        LOG_D("TSchemeQueryExecutor HandleCompileResponse"
            << ", self: " << SelfId()
            << ", status: " << result->Status;
        );

        if (result->Status != Ydb::StatusIds::SUCCESS) {
            return Reply(result->Status, result->Issues.ToOneLineString());
        }
        if (!result->PreparedQuery) {
            return Reply(Ydb::StatusIds::BAD_REQUEST, "no prepared query");
        }
        const auto& transactions = result->PreparedQuery->GetPhysicalQuery().GetTransactions();
        if (transactions.empty()) {
            return Reply(Ydb::StatusIds::BAD_REQUEST, "empty transactions");
        }
        if (!transactions[0].HasSchemeOperation()) {
            return Reply(Ydb::StatusIds::BAD_REQUEST, "no scheme operations");
        }
        if (!transactions[0].GetSchemeOperation().HasCreateView()) {
            return Reply(Ydb::StatusIds::BAD_REQUEST, "no create view operation");
        }
        const auto& createView = transactions[0].GetSchemeOperation().GetCreateView();
        Reply(result->Status, createView);
    }

    void Reply(Ydb::StatusIds::StatusCode status, std::variant<TString, NKikimrSchemeOp::TModifyScheme> result) {
        auto logMessage = TStringBuilder() << "TSchemeQueryExecutor Reply"
            << ", self: " << SelfId()
            << ", success: " << status;
        LOG_I(logMessage);

        std::visit([&]<typename T>(T& rresult) {
            if constexpr (std::is_same_v<T, TString>) {
                logMessage << ", error: " << rresult;
            } else if constexpr (std::is_same_v<T, NKikimrSchemeOp::TModifyScheme>) {
                logMessage << ", prepared query: " << rresult.ShortDebugString().Quote();
            }
            LOG_D(logMessage);
            Send(ReplyTo, new TEvPrivate::TEvImportSchemeQueryResult(ImportId, ItemIdx, status, std::move(rresult)));
        }, result);

        PassAway();
    }

public:

    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::IMPORT_SCHEME_QUERY_EXECUTOR;
    }

    TSchemeQueryExecutor(
        TActorId replyTo,
        ui64 importId,
        ui32 itemIdx,
        const TString& schemeQuery,
        const TString& database
    )
        : ReplyTo(replyTo)
        , ImportId(importId)
        , ItemIdx(itemIdx)
        , SchemeQuery(schemeQuery)
        , Database(database)
    {
    }

    void Bootstrap() {
        PrepareSchemeQuery();
    }

    STATEFN(StateBase) {
        switch (ev->GetTypeRewrite()) {
            sFunc(TEvents::TEvPoisonPill, PassAway);
        }
    }

    STATEFN(StateExecute) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvKqp::TEvCompileResponse, HandleCompileResponse);
        default:
            return StateBase(ev);
        }
    }

private:

    TActorId ReplyTo;
    ui64 ImportId;
    ui32 ItemIdx;
    TString SchemeQuery;
    TString Database;

    // Pointer type event arguments need to live until we receive the compilation response.
    TIntrusiveConstPtr<NACLib::TUserToken> UserToken;
    TGUCSettings::TPtr GUCSettings;
    std::shared_ptr<std::atomic<bool>> IsInterestedInResult;
    TIntrusivePtr<TUserRequestContext> UserRequestContext;

}; // TSchemeQueryExecutor

IActor* CreateSchemeQueryExecutor(NActors::TActorId replyTo, ui64 importId, ui32 itemIdx, const TString& schemeQuery, const TString& database) {
    return new TSchemeQueryExecutor(replyTo, importId, itemIdx, schemeQuery, database);
}

} // NKikimr::NSchemeShard
