#pragma once
#include "result.h"

#include <library/cpp/actors/core/event_local.h>
#include <ydb/library/aclib/aclib.h>
#include <ydb/core/kqp/common/simple/temp_tables.h>
#include <ydb/core/kqp/common/simple/kqp_event_ids.h>
#include <ydb/core/kqp/common/simple/query_id.h>
#include <ydb/core/kqp/counters/kqp_counters.h>

namespace NKikimr::NKqp::NPrivateEvents {

struct TEvCompileRequest: public TEventLocal<TEvCompileRequest, TKqpEvents::EvCompileRequest> {
    TEvCompileRequest(const TIntrusiveConstPtr<NACLib::TUserToken>& userToken, const TMaybe<TString>& uid,
        TMaybe<TKqpQueryId>&& query, bool keepInCache, TInstant deadline,
        TKqpDbCountersPtr dbCounters, NLWTrace::TOrbit orbit = {},
        TKqpTempTablesState::TConstPtr tempTablesState = nullptr)
        : UserToken(userToken)
        , Uid(uid)
        , Query(std::move(query))
        , KeepInCache(keepInCache)
        , Deadline(deadline)
        , DbCounters(dbCounters)
        , Orbit(std::move(orbit))
        , TempTablesState(std::move(tempTablesState)) {
        Y_ENSURE(Uid.Defined() != Query.Defined());
    }

    TIntrusiveConstPtr<NACLib::TUserToken> UserToken;
    TMaybe<TString> Uid;
    TMaybe<TKqpQueryId> Query;
    bool KeepInCache = false;
    // it is allowed for local event to use absolute time (TInstant) instead of time interval (TDuration)
    TInstant Deadline;
    TKqpDbCountersPtr DbCounters;
    TMaybe<bool> DocumentApiRestricted;

    NLWTrace::TOrbit Orbit;

    TKqpTempTablesState::TConstPtr TempTablesState;
};

struct TEvRecompileRequest: public TEventLocal<TEvRecompileRequest, TKqpEvents::EvRecompileRequest> {
    TEvRecompileRequest(const TIntrusiveConstPtr<NACLib::TUserToken>& userToken, const TString& uid,
        const TMaybe<TKqpQueryId>& query, TInstant deadline,
        TKqpDbCountersPtr dbCounters, NLWTrace::TOrbit orbit = {},
        TKqpTempTablesState::TConstPtr tempTablesState = nullptr)
        : UserToken(userToken)
        , Uid(uid)
        , Query(query)
        , Deadline(deadline)
        , DbCounters(dbCounters)
        , Orbit(std::move(orbit))
        , TempTablesState(std::move(tempTablesState)) {
    }

    TIntrusiveConstPtr<NACLib::TUserToken> UserToken;
    TString Uid;
    TMaybe<TKqpQueryId> Query;

    TInstant Deadline;
    TKqpDbCountersPtr DbCounters;

    NLWTrace::TOrbit Orbit;

    TKqpTempTablesState::TConstPtr TempTablesState;
};

struct TEvCompileResponse: public TEventLocal<TEvCompileResponse, TKqpEvents::EvCompileResponse> {
    TEvCompileResponse(const TKqpCompileResult::TConstPtr& compileResult, NLWTrace::TOrbit orbit = {})
        : CompileResult(compileResult)
        , Orbit(std::move(orbit)) {
    }

    TKqpCompileResult::TConstPtr CompileResult;
    NKqpProto::TKqpStatsCompile Stats;
    std::optional<TString> ReplayMessage;

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
