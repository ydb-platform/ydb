#pragma once

#include <ydb/library/actors/core/events.h>
#include <ydb/library/actors/core/event_local.h>
#include <ydb/core/client/minikql_compile/yql_expr_minikql.h>
#include <ydb/core/client/scheme_cache_lib/yql_db_scheme_resolver.h>

#include <util/generic/ptr.h>

namespace NActors {
class IActor;
} // namespace NActors

namespace NKikimr {

struct TMiniKQLCompileServiceEvents {
    enum {
        Compile = EventSpaceBegin(NActors::TEvents::ES_USERSPACE),
        CompileStatus,
        End
    };

    static_assert(End < EventSpaceEnd(NActors::TEvents::ES_USERSPACE), "expect End < EventSpaceEnd(TEvents::ES_USERSPACE)");

    struct TEvCompile : public NActors::TEventLocal<TEvCompile, Compile> {
        explicit TEvCompile(const TString& program);

        TString Program;
        bool ForceRefresh = false;
        THashMap<TString, ui64> CompileResolveCookies;
    };

    struct TEvCompileStatus : public NActors::TEventLocal<TEvCompileStatus, CompileStatus> {

        TEvCompileStatus(const TString& program, const NYql::TMiniKQLCompileResult& result);

        TString Program;
        NYql::TMiniKQLCompileResult Result;
        THashMap<TString, ui64> CompileResolveCookies;
    };
};

NActors::TActorId MakeMiniKQLCompileServiceID();
const NActors::TActorId& GetMiniKQLCompileServiceID();
NActors::IActor* CreateMiniKQLCompileService(size_t compileInflightLimit);
NActors::IActor* CreateMiniKQLCompileService(size_t compileInflightLimit, THolder<NYql::IDbSchemeResolver>&& dbSchemeResolver);

} // namespace NKikimr
