#pragma once
#include <ydb/library/yql/utils/log/log.h>
#include <ydb/library/yql/utils/log/tls_backend.h>

#include <ydb/library/services/services.pb.h>

#include <ydb/library/actors/core/actorsystem.h>
#include <ydb/library/actors/core/actor.h>

namespace NYql {
namespace NDq {

class TActorYqlLogBackend : public TLogBackend {
public:
    TActorYqlLogBackend(
        const NActors::TActorContext& actorCtx,
        int component,
        const TString& sessionId,
        const TString& traceId)
        : ActorCtxOrSystem(&actorCtx)
        , Component(component)
        , SessionId(sessionId)
        , TraceId(traceId) {}

    TActorYqlLogBackend(
        const NActors::TActorSystem* actorSystem,
        int component,
        const TString& sessionId,
        const TString& traceId)
        : ActorCtxOrSystem(actorSystem)
        , Component(component)
        , SessionId(sessionId)
        , TraceId(traceId) {}

    void WriteData(const TLogRecord& rec) override;
    void ReopenLog() override {}

private:
    std::variant<const NActors::TActorContext*, const NActors::TActorSystem*> ActorCtxOrSystem;
    int Component;
    TString SessionId;
    TString TraceId;
};

class TYqlLogScope : public NYql::NLog::TScopedBackend<TActorYqlLogBackend> {
public:
    TYqlLogScope(const NActors::TActorContext& actorCtx, int component, const TString& sessionId, const TString& traceId = "")
        : NYql::NLog::TScopedBackend<TActorYqlLogBackend>(actorCtx, component, sessionId, traceId) {};

    TYqlLogScope(const NActors::TActorSystem* actorSystem, int component, const TString& sessionId, const TString& traceId = "")
        : NYql::NLog::TScopedBackend<TActorYqlLogBackend>(actorSystem, component, sessionId, traceId) {};
};

class TLogWrapReceive: public NActors::TDecorator {
public:
    TLogWrapReceive(NActors::IActor* actor, const TString& sessionId, int component = NKikimrServices::YQL_PROXY)
        : NActors::TDecorator(THolder(actor))
        , SessionId(sessionId)
        , Component(component)
    { }

    bool DoBeforeReceiving(TAutoPtr<NActors::IEventHandle>& /*ev*/, const NActors::TActorContext& ctx) override {
        LogScope.ConstructInPlace(ctx, Component, SessionId);
        return true;
    }

    void DoAfterReceiving(const NActors::TActorContext& /*ctx*/) override {
        LogScope.Clear();
    }

private:
    TString SessionId;
    int Component;
    TMaybe<TYqlLogScope> LogScope;
};

void SetYqlLogLevels(const NActors::NLog::EPriority& priority);

} // namespace NDq
} // namespace NYql
