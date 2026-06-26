#pragma once

#include <ydb/core/protos/kqp_stats.pb.h>
#include <ydb/library/actors/core/actorsystem_fwd.h>
#include <ydb/library/actors/core/events.h>
#include <ydb/library/actors/core/event_local.h>
#include <ydb/public/api/protos/ydb_status_codes.pb.h>

#include <yql/essentials/public/issue/yql_issue.h>

#include <util/generic/ptr.h>
#include <util/generic/string.h>
#include <util/system/types.h>

#include <memory>
#include <optional>

namespace NKikimrConfig {

class TQueryServiceConfig;

} // namespace NKikimrConfig

namespace NKikimrKqp {

class TQueryPhysicalGraph;

} // namespace NKikimrKqp

namespace NKikimr::NKqp {

class TKqpCounters;
struct TUserRequestContext;

namespace NPrivate {

struct TExecutionInfo {
    std::optional<TString> QueryPlan;
    std::optional<TString> QueryAst;
    std::optional<NKqpProto::TKqpStatsQuery> QueryStats;
};

struct TFinishInfo {
    void Update(const Ydb::StatusIds::StatusCode status, NYql::TIssues issues);

    bool IsFinished() const;

    bool IsSuccess() const;

    bool IsFailed() const;

    std::optional<Ydb::StatusIds::StatusCode> Status;
    NYql::TIssues Issues;
};

struct TEvRunScriptPrivate {
    enum EEv : ui32 {
        EvScriptLeaseWatcherFinished = EventSpaceBegin(NActors::TEvents::ES_PRIVATE),
        EvScriptResultHandlerFinished,
        EvEnd
    };

    static_assert(EvEnd < EventSpaceEnd(NActors::TEvents::ES_PRIVATE), "expect EvEnd < EventSpaceEnd(NActors::TEvents::ES_PRIVATE)");

    struct TEvScriptLeaseWatcherFinished : public NActors::TEventLocal<TEvScriptLeaseWatcherFinished, EvScriptLeaseWatcherFinished> {
        TEvScriptLeaseWatcherFinished(const Ydb::StatusIds::StatusCode status, NYql::TIssues issues)
            : Status(status)
            , Issues(std::move(issues))
        {}

        const Ydb::StatusIds::StatusCode Status;
        const NYql::TIssues Issues;
    };

    struct TEvScriptResultHandlerFinished : public NActors::TEventLocal<TEvScriptResultHandlerFinished, EvScriptResultHandlerFinished> {
        TEvScriptResultHandlerFinished(const Ydb::StatusIds::StatusCode status, TExecutionInfo&& info, NYql::TIssues issues)
            : Status(status)
            , Info(std::move(info))
            , Issues(std::move(issues))
        {}

        const Ydb::StatusIds::StatusCode Status;
        TExecutionInfo Info;
        NYql::TIssues Issues;
    };
};

struct TScriptExecutionContext {
    using TPtr = std::shared_ptr<TScriptExecutionContext>;

    const TIntrusivePtr<TUserRequestContext> UserRequestContext;
    const TIntrusivePtr<TKqpCounters> Counters;
    const i64 LeaseGeneration = 0;
    const TDuration LeaseDuration;
    const TDuration ResultsTtl;
    const TDuration Timeout;
};

// Periodically update lease deadline in database, and detect lease lost
NActors::IActor* CreateScriptLeaseWatcherActor(TScriptExecutionContext::TPtr ctx);

// Handle events from kqp session actor and data executor such as result data, progress pings etc.
NActors::IActor* CreateScriptResultHandlerActor(TScriptExecutionContext::TPtr ctx, std::optional<NKikimrKqp::TQueryPhysicalGraph> physicalGraph, NKikimrConfig::TQueryServiceConfig queryServiceConfig);

} // namespace NPrivate

} // namespace NKikimr::NKqp
