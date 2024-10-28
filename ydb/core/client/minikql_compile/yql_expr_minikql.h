#pragma once

#include "compile_result.h"
#include "compile_context.h"
#include "db_key_resolver.h"

#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/events.h>
#include <ydb/library/yql/ast/yql_expr.h>

#include <library/cpp/threading/future/future.h>

#include <util/generic/bt_exception.h>


namespace NKikimr {

namespace NMiniKQL {
class TProgramBuilder;
class IFunctionRegistry;
} // namespace NMiniKQL

} // namespace NKikimr

namespace NYql {

struct TExprContainer : public TAtomicRefCount<TExprContainer> {
    using TPtr = TIntrusivePtr<TExprContainer>;

    TExprContext Context;
    TExprNode::TPtr Root;
};

NThreading::TFuture<TConvertResult>
ConvertToMiniKQL(TExprContainer::TPtr expr,
                 const NKikimr::NMiniKQL::IFunctionRegistry* functionRegistry,
                 const NKikimr::NMiniKQL::TTypeEnvironment* typeEnv,
                 IDbSchemeResolver* dbSchemeResolver);

struct TMiniKQLCompileActorEvents {
    enum {
        CompileResult = EventSpaceBegin(NActors::TEvents::ES_PRIVATE),
        End
    };
    static_assert(End < EventSpaceEnd(NActors::TEvents::ES_PRIVATE), "expect End < EventSpaceEnd(TEvents::ES_PRIVATE)");
    struct TEvCompileResult : public NActors::TEventLocal<TEvCompileResult, CompileResult> {
        explicit TEvCompileResult(const TMiniKQLCompileResult& result, THashMap<TString, ui64> &&resolveCookies);

        TMiniKQLCompileResult Result;
        THashMap<TString, ui64> CompileResolveCookies;
    };
};

NActors::IActor*
CreateCompileActor(const TString& program,
                   const NKikimr::NMiniKQL::TTypeEnvironment* typeEnv,
                   IDbSchemeResolver* dbSchemeResolver,
                   NActors::TActorId responseTo,
                   THashMap<TString, ui64> &&resolveRefreshCookies,
                   bool forceCacheRefresh); // TEvCompileResult.

} // namespace NYql
