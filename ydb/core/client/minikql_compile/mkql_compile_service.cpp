#include "mkql_compile_service.h"

#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/executor_thread.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/core/client/scheme_cache_lib/yql_db_scheme_resolver.h>
#include <ydb/library/services/services.pb.h>
#include <ydb/core/tx/scheme_board/cache.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>
#include <ydb/core/base/appdata.h>
#include <ydb/core/base/counters.h>

#include <util/generic/queue.h>

namespace NKikimr {

TMiniKQLCompileServiceEvents::TEvCompile::TEvCompile(const TString& program)
    : Program(program)
{}

TMiniKQLCompileServiceEvents::TEvCompileStatus::TEvCompileStatus(const TString& pgm, const NYql::TMiniKQLCompileResult& result)
    : Program(pgm)
    , Result(result)
{}

// Actor with queue of programs to compile. Creates TMiniKQLCompileActor for each program.
class TMiniKQLCompileService : public NActors::TActorBootstrapped<TMiniKQLCompileService> {
public:
    struct TCompileContext : public TSimpleRefCount<TCompileContext>{
        using TPtr = TIntrusivePtr<TCompileContext>;

        TCompileContext(const TString& pgm,
            TActorId sender,
            const TAlignedPagePoolCounters& allocPoolCounters,
            const NMiniKQL::IFunctionRegistry* functionRegistry)
            : Program(pgm)
            , ResponseTo(sender)
            , Alloc(__LOCATION__, allocPoolCounters, functionRegistry->SupportsSizedAllocators())
            , TypeEnv(Alloc)
            , Cookie(0)
            , Retried(false)
        {
            Alloc.Release();
        }

        ~TCompileContext()
        {
            Alloc.Acquire();
        }

        TString Program;
        TActorId ResponseTo;
        NMiniKQL::TScopedAlloc Alloc;
        NMiniKQL::TTypeEnvironment TypeEnv;
        ui64 Cookie;
        bool Retried;
        THashMap<TString, ui64> CompileResolveCookies;
        bool ForceRefresh;
    };

    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::MINIKQL_COMPILE_SERVICE;
    }

    TMiniKQLCompileService(size_t compileInflightLimit, THolder<NYql::IDbSchemeResolver>&& dbSchemeResolver)
        : COMPILE_INFLIGHT_LIMIT(compileInflightLimit)
        , DbSchemeResolver(std::move(dbSchemeResolver))
    {
    }

    void Bootstrap(const TActorContext& ctx) {

        Counters = GetServiceCounters(AppData(ctx)->Counters, "compile")->GetSubgroup("subsystem", "cache");
        AllocPoolCounters = TAlignedPagePoolCounters(AppData(ctx)->Counters, "compile");

        if (!DbSchemeResolver) {
            DbSchemeResolver.Reset(MakeDbSchemeResolver(ctx));
        }

        Become(&TThis::StateWork);
    }

    NYql::IDbSchemeResolver* MakeDbSchemeResolver(const TActorContext& ctx) {
        auto cacheConfig = MakeIntrusive<NSchemeCache::TSchemeCacheConfig>(AppData(ctx), Counters);
        SchemeCache = ctx.ExecutorThread.RegisterActor(CreateSchemeBoardSchemeCache(cacheConfig.Get()));
        return NSchCache::CreateDbSchemeResolver(ctx.ExecutorThread.ActorSystem, SchemeCache);
    }

    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TMiniKQLCompileServiceEvents::TEvCompile, Handle);
            HFunc(NYql::TMiniKQLCompileActorEvents::TEvCompileResult, Handle);
            cFunc(TEvents::TEvPoison::EventType, PassAway);
            default:
                Y_ABORT("");
        }
    }

private:
    void Handle(TMiniKQLCompileServiceEvents::TEvCompile::TPtr& ev, const TActorContext& ctx) {
        TMiniKQLCompileServiceEvents::TEvCompile *msg = ev->Get();
        TCompileContext::TPtr c(new TCompileContext(msg->Program, ev->Sender, AllocPoolCounters, AppData(ctx)->FunctionRegistry));
        c->Cookie = ev->Cookie;
        c->CompileResolveCookies = std::move(msg->CompileResolveCookies);
        c->ForceRefresh = msg->ForceRefresh;
        CompileQueue.push(c);
        Compile(ctx);
    }

    void Handle(NYql::TMiniKQLCompileActorEvents::TEvCompileResult::TPtr& ev, const TActorContext& ctx) {
        auto *msg = ev->Get();
        auto it = Compiling.find(ev->Sender);
        Y_ABORT_UNLESS(it != Compiling.end());

        TCompileContext::TPtr cptr = it->second;
        Compiling.erase(it);

        const bool hasErrors = !msg->Result.Errors.Empty();
        if (!hasErrors || cptr->Retried || cptr->ForceRefresh) {
            auto* compileStatusEv = new TMiniKQLCompileServiceEvents::TEvCompileStatus(cptr->Program, msg->Result);
            compileStatusEv->CompileResolveCookies = std::move(msg->CompileResolveCookies);
            ctx.ExecutorThread.Send(new IEventHandle(cptr->ResponseTo, ctx.SelfID, compileStatusEv,
                0, cptr->Cookie));
            Compile(ctx);
        } else {
            // recreate compile context
            const TAppData *appData = AppData(ctx);
            TCompileContext::TPtr c = new TCompileContext(cptr->Program, cptr->ResponseTo, AllocPoolCounters, appData->FunctionRegistry);
            c->Cookie = cptr->Cookie;
            c->Retried = true;

            auto *compileActor = NYql::CreateCompileActor(c->Program, &c->TypeEnv, DbSchemeResolver.Get(), ctx.SelfID, std::move(msg->CompileResolveCookies), false);
            const TActorId actId = ctx.ExecutorThread.RegisterActor(compileActor, TMailboxType::HTSwap, appData->UserPoolId);
            Compiling.insert(TCompilingMap::value_type(actId, c));
        }
    }

    void Compile(const TActorContext& ctx) {
        if (IsCompileLimitReached() || CompileQueue.empty()) {
            return;
        }
        auto next = CompileQueue.front();
        auto* act = NYql::CreateCompileActor(
            next->Program,
            &next->TypeEnv,
            DbSchemeResolver.Get(),
            ctx.SelfID,
            std::move(next->CompileResolveCookies),
            next->ForceRefresh);
        auto *appData = AppData(ctx);
        auto actId = ctx.ExecutorThread.RegisterActor(act, TMailboxType::HTSwap, appData->UserPoolId);
        Compiling.insert(TCompilingMap::value_type(actId, next));
        CompileQueue.pop();
    }

    bool IsCompileLimitReached() const {
        return Compiling.size() >= COMPILE_INFLIGHT_LIMIT;
    }

private:
    const size_t COMPILE_INFLIGHT_LIMIT;
    TQueue<TCompileContext::TPtr> CompileQueue;
    using TCompilingMap = THashMap<TActorId, TCompileContext::TPtr>;
    TCompilingMap Compiling;

    TIntrusivePtr<::NMonitoring::TDynamicCounters> Counters;
    TAlignedPagePoolCounters AllocPoolCounters;
    TActorId SchemeCache;
    THolder<NYql::IDbSchemeResolver> DbSchemeResolver;
};


TActorId MakeMiniKQLCompileServiceID() {
    const char x[12] = "MKQLCompile";
    return TActorId(0, TStringBuf(x, 12));
}

const TActorId& GetMiniKQLCompileServiceID() {
    static TActorId miniKQLCompileServiceID = MakeMiniKQLCompileServiceID();
    return miniKQLCompileServiceID;
}

IActor* CreateMiniKQLCompileService(size_t compileInflightLimit) {
    THolder<NYql::IDbSchemeResolver> resolver;
    return new TMiniKQLCompileService(compileInflightLimit, std::move(resolver));
}

IActor* CreateMiniKQLCompileService(size_t compileInflightLimit, THolder<NYql::IDbSchemeResolver>&& dbSchemeResolver) {
    return new TMiniKQLCompileService(compileInflightLimit, std::move(dbSchemeResolver));
}

} // namespace NKikimr
