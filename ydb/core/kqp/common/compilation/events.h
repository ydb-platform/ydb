#pragma once
#include "result.h"

#include <ydb/library/actors/core/event_local.h>
#include <ydb/library/aclib/aclib.h>
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
        TKqpDbCountersPtr dbCounters, const TMaybe<TString>& applicationName, std::shared_ptr<std::atomic<bool>> intrestedInResult, 
        const TIntrusivePtr<TUserRequestContext>& userRequestContext, NLWTrace::TOrbit orbit = {},
        TKqpTempTablesState::TConstPtr tempTablesState = nullptr, bool collectDiagnostics = false, TMaybe<TQueryAst> queryAst = Nothing())
        : UserToken(userToken)
        , Uid(uid)
        , Query(std::move(query))
        , KeepInCache(keepInCache)
        , IsQueryActionPrepare(isQueryActionPrepare)
        , PerStatementResult(perStatementResult)
        , Deadline(deadline)
        , DbCounters(dbCounters)
        , ApplicationName(applicationName)
        , UserRequestContext(userRequestContext)
        , Orbit(std::move(orbit))
        , TempTablesState(std::move(tempTablesState))
        , IntrestedInResult(std::move(intrestedInResult))
        , CollectDiagnostics(collectDiagnostics)
        , QueryAst(queryAst)
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
    TMaybe<TString> ApplicationName;

    TIntrusivePtr<TUserRequestContext> UserRequestContext;
    NLWTrace::TOrbit Orbit;

    TKqpTempTablesState::TConstPtr TempTablesState;
    std::shared_ptr<std::atomic<bool>> IntrestedInResult;

    bool CollectDiagnostics = false;

    TMaybe<TQueryAst> QueryAst;
};

struct TEvRecompileRequest: public TEventLocal<TEvRecompileRequest, TKqpEvents::EvRecompileRequest> {
    TEvRecompileRequest(const TIntrusiveConstPtr<NACLib::TUserToken>& userToken, const TString& uid,
        const TMaybe<TKqpQueryId>& query, bool isQueryActionPrepare, TInstant deadline,
        TKqpDbCountersPtr dbCounters, const TMaybe<TString>& applicationName, std::shared_ptr<std::atomic<bool>> intrestedInResult,
        const TIntrusivePtr<TUserRequestContext>& userRequestContext, NLWTrace::TOrbit orbit = {},
        TKqpTempTablesState::TConstPtr tempTablesState = nullptr)
        : UserToken(userToken)
        , Uid(uid)
        , Query(query)
        , IsQueryActionPrepare(isQueryActionPrepare)
        , Deadline(deadline)
        , DbCounters(dbCounters)
        , ApplicationName(applicationName)
        , UserRequestContext(userRequestContext)
        , Orbit(std::move(orbit))
        , TempTablesState(std::move(tempTablesState))
        , IntrestedInResult(std::move(intrestedInResult))
    {
    }

    TIntrusiveConstPtr<NACLib::TUserToken> UserToken;
    TString Uid;
    TMaybe<TKqpQueryId> Query;
    bool IsQueryActionPrepare = false;

    TInstant Deadline;
    TKqpDbCountersPtr DbCounters;
    TMaybe<TString> ApplicationName;

    TIntrusivePtr<TUserRequestContext> UserRequestContext;
    NLWTrace::TOrbit Orbit;

    TKqpTempTablesState::TConstPtr TempTablesState;
    std::shared_ptr<std::atomic<bool>> IntrestedInResult;
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
