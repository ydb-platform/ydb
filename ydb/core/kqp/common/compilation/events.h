#pragma once
#include "result.h"

#include <ydb/library/actors/core/event_local.h>
#include <ydb/library/aclib/aclib.h>
#include <ydb/library/yql/ast/yql_expr.h>
#include <ydb/core/kqp/common/simple/temp_tables.h>
#include <ydb/core/kqp/common/simple/kqp_event_ids.h>
#include <ydb/core/kqp/common/simple/query_id.h>
#include <ydb/core/kqp/common/simple/query_ast.h>
#include <ydb/core/kqp/common/kqp_user_request_context.h>
#include <ydb/core/kqp/counters/kqp_counters.h>

namespace NKikimr::NKqp::NPrivateEvents {

struct TEvCompileRequest: public TEventLocal<TEvCompileRequest, TKqpEvents::EvCompileRequest> {
    TEvCompileRequest(const TIntrusiveConstPtr<NACLib::TUserToken>& userToken, const TMaybe<TString>& uid,
        TMaybe<TKqpQueryId>&& query, bool keepInCache, bool isQueryActionPrepare, bool perStatementResult, TInstant deadline,
        TKqpDbCountersPtr dbCounters, const TGUCSettings::TPtr& gUCSettings, const TMaybe<TString>& applicationName,
        std::shared_ptr<std::atomic<bool>> intrestedInResult, const TIntrusivePtr<TUserRequestContext>& userRequestContext, NLWTrace::TOrbit orbit = {},
        TKqpTempTablesState::TConstPtr tempTablesState = nullptr, bool collectDiagnostics = false, TMaybe<TQueryAst> queryAst = Nothing(),
        bool split = false, NYql::TExprContext* splitCtx = nullptr, NYql::TExprNode::TPtr splitExpr = nullptr)
        : UserToken(userToken)
        , Uid(uid)
        , Query(std::move(query))
        , KeepInCache(keepInCache)
        , IsQueryActionPrepare(isQueryActionPrepare)
        , PerStatementResult(perStatementResult)
        , Deadline(deadline)
        , DbCounters(dbCounters)
        , GUCSettings(gUCSettings)
        , ApplicationName(applicationName)
        , UserRequestContext(userRequestContext)
        , Orbit(std::move(orbit))
        , TempTablesState(std::move(tempTablesState))
        , IntrestedInResult(std::move(intrestedInResult))
        , CollectDiagnostics(collectDiagnostics)
        , QueryAst(queryAst)
        , Split(split)
        , SplitCtx(splitCtx)
        , SplitExpr(splitExpr)
    {
        Y_ENSURE(Uid.Defined() != Query.Defined());
    }

    TIntrusiveConstPtr<NACLib::TUserToken> UserToken;
    TMaybe<TString> Uid;
    TMaybe<TKqpQueryId> Query;
    bool KeepInCache = false;
    bool IsQueryActionPrepare = false;
    bool PerStatementResult = false;
    // it is allowed for local event to use absolute time (TInstant) instead of time interval (TDuration)
    TInstant Deadline;
    TKqpDbCountersPtr DbCounters;
    TMaybe<bool> DocumentApiRestricted;
    TGUCSettings::TPtr GUCSettings;
    TMaybe<TString> ApplicationName;

    TIntrusivePtr<TUserRequestContext> UserRequestContext;
    NLWTrace::TOrbit Orbit;

    TKqpTempTablesState::TConstPtr TempTablesState;
    std::shared_ptr<std::atomic<bool>> IntrestedInResult;

    bool CollectDiagnostics = false;

    TMaybe<TQueryAst> QueryAst;
    bool Split = false;

    NYql::TExprContext* SplitCtx = nullptr;
    NYql::TExprNode::TPtr SplitExpr = nullptr;
};

struct TEvRecompileRequest: public TEventLocal<TEvRecompileRequest, TKqpEvents::EvRecompileRequest> {
    TEvRecompileRequest(const TIntrusiveConstPtr<NACLib::TUserToken>& userToken, const TString& uid,
        const TMaybe<TKqpQueryId>& query, bool isQueryActionPrepare, TInstant deadline,
        TKqpDbCountersPtr dbCounters, const TGUCSettings::TPtr& gUCSettings, const TMaybe<TString>& applicationName,
        std::shared_ptr<std::atomic<bool>> intrestedInResult, const TIntrusivePtr<TUserRequestContext>& userRequestContext,
        NLWTrace::TOrbit orbit = {}, TKqpTempTablesState::TConstPtr tempTablesState = nullptr, TMaybe<TQueryAst> queryAst = Nothing(),
        bool split = false, NYql::TExprContext* splitCtx = nullptr, NYql::TExprNode::TPtr splitExpr = nullptr)
        : UserToken(userToken)
        , Uid(uid)
        , Query(query)
        , IsQueryActionPrepare(isQueryActionPrepare)
        , Deadline(deadline)
        , DbCounters(dbCounters)
        , GUCSettings(gUCSettings)
        , ApplicationName(applicationName)
        , UserRequestContext(userRequestContext)
        , Orbit(std::move(orbit))
        , TempTablesState(std::move(tempTablesState))
        , IntrestedInResult(std::move(intrestedInResult))
        , QueryAst(queryAst)
        , Split(split)
        , SplitCtx(splitCtx)
        , SplitExpr(splitExpr)
    {
    }

    TIntrusiveConstPtr<NACLib::TUserToken> UserToken;
    TString Uid;
    TMaybe<TKqpQueryId> Query;
    bool IsQueryActionPrepare = false;

    TInstant Deadline;
    TKqpDbCountersPtr DbCounters;
    TGUCSettings::TPtr GUCSettings;
    TMaybe<TString> ApplicationName;

    TIntrusivePtr<TUserRequestContext> UserRequestContext;
    NLWTrace::TOrbit Orbit;

    TKqpTempTablesState::TConstPtr TempTablesState;
    std::shared_ptr<std::atomic<bool>> IntrestedInResult;

    TMaybe<TQueryAst> QueryAst;
    bool Split = false;

    NYql::TExprContext* SplitCtx = nullptr;
    NYql::TExprNode::TPtr SplitExpr = nullptr;
};

struct TEvCompileResponse: public TEventLocal<TEvCompileResponse, TKqpEvents::EvCompileResponse> {
    TEvCompileResponse(const TKqpCompileResult::TConstPtr& compileResult, NLWTrace::TOrbit orbit = {}, const std::optional<TString>& replayMessage = std::nullopt)
        : CompileResult(compileResult)
        , ReplayMessage(replayMessage)
        , Orbit(std::move(orbit)) {
    }

    TKqpCompileResult::TConstPtr CompileResult;
    TKqpStatsCompile Stats;
    std::optional<TString> ReplayMessage;
    std::optional<TString> ReplayMessageUserView;

    NLWTrace::TOrbit Orbit;
};

struct TEvParseResponse: public TEventLocal<TEvParseResponse, TKqpEvents::EvParseResponse> {
    TEvParseResponse(const TKqpQueryId& query, TVector<TQueryAst> astStatements, NLWTrace::TOrbit orbit = {})
        : AstStatements(std::move(astStatements))
        , Query(query)
        , Orbit(std::move(orbit)) {}

    TVector<TQueryAst> AstStatements;
    TKqpQueryId Query;
    NLWTrace::TOrbit Orbit;
};

struct TEvSplitResponse: public TEventLocal<TEvSplitResponse, TKqpEvents::EvSplitResponse> {
    TEvSplitResponse(const TKqpQueryId& query, TVector<NYql::TExprNode::TPtr> exprs, NYql::TExprNode::TPtr world, THolder<NYql::TExprContext> ctx)
        : Query(query)
        , Ctx(std::move(ctx))
        , Exprs(std::move(exprs))
        , World(std::move(world)) {}

    TKqpQueryId Query;
    THolder<NYql::TExprContext> Ctx;
    TVector<NYql::TExprNode::TPtr> Exprs;
    NYql::TExprNode::TPtr World;
};

struct TEvCompileInvalidateRequest: public TEventLocal<TEvCompileInvalidateRequest,
    TKqpEvents::EvCompileInvalidateRequest> {
    TEvCompileInvalidateRequest(const TString& uid, TKqpDbCountersPtr dbCounters)
        : Uid(uid)
        , DbCounters(dbCounters) {
    }

    TString Uid;
    TKqpDbCountersPtr DbCounters;
};

} // namespace NKikimr::NKqp
