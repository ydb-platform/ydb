#pragma once

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/log.h>
#include <ydb/library/services/services.pb.h>

#define LOG_PREFIX TStringBuilder() << "[" << TabletId << "] " << GetLogPrefix()
#define LOG_T(stream) LOG_TRACE_S (*TlsActivationContext, Service, LOG_PREFIX << stream)
#define LOG_D(stream) LOG_DEBUG_S (*TlsActivationContext, Service, LOG_PREFIX << stream)
#define LOG_I(stream) LOG_INFO_S  (*TlsActivationContext, Service, LOG_PREFIX << stream)
#define LOG_N(stream) LOG_NOTICE_S(*TlsActivationContext, Service, LOG_PREFIX << stream)
#define LOG_W(stream) LOG_WARN_S  (*TlsActivationContext, Service, LOG_PREFIX << stream)
#define LOG_E(stream) LOG_ERROR_S (*TlsActivationContext, Service, LOG_PREFIX << stream)
#define LOG_C(stream) LOG_CRIT_S  (*TlsActivationContext, Service, LOG_PREFIX << stream)

#define PQ_ENSURE(condition) AFL_ENSURE(condition)("tablet_id", TabletId)

namespace NKikimr::NPQ {

using namespace NActors;

template<typename TDerived>
class TBaseActor : public TActorBootstrapped<TDerived>
                 , public IActorExceptionHandler {
public:
    using TBase = TActorBootstrapped<TDerived>;
    using TThis = TBaseActor<TDerived>;

    TBaseActor(ui64 tabletId, TActorId tabletActorId, NKikimrServices::EServiceKikimr service)
        : TabletId(tabletId)
        , TabletActorId(tabletActorId)
        , Service(service)
    {
    }

    bool OnUnhandledException(const std::exception& exc) override  {
        LOG_C("unhandled exception " << TypeName(exc) << ": " << exc.what() << Endl
                << TBackTrace::FromCurrentException().PrintToString());

        TThis::Send(TabletActorId, new TEvents::TEvPoison());
        TThis::PassAway();

        return true;
    }

    virtual TStringBuf GetLogPrefix() const = 0;

protected:
    const ui64 TabletId;
    const TActorId TabletActorId;
    const NKikimrServices::EServiceKikimr Service;
};

}
