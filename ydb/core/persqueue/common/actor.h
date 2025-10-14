#pragma once

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/log.h>
#include <ydb/library/services/services.pb.h>

#define LOG_PREFIX_INT TStringBuilder() << "[" << TabletId << "]" << GetLogPrefix()
#define LOG_T(stream) LOG_TRACE_S (*NActors::TlsActivationContext, Service, LOG_PREFIX_INT << stream)
#define LOG_D(stream) LOG_DEBUG_S (*NActors::TlsActivationContext, Service, LOG_PREFIX_INT << stream)
#define LOG_I(stream) LOG_INFO_S  (*NActors::TlsActivationContext, Service, LOG_PREFIX_INT << stream)
#define LOG_N(stream) LOG_NOTICE_S(*NActors::TlsActivationContext, Service, LOG_PREFIX_INT << stream)
#define LOG_W(stream) LOG_WARN_S  (*NActors::TlsActivationContext, Service, LOG_PREFIX_INT << stream)
#define LOG_E(stream) LOG_ERROR_S (*NActors::TlsActivationContext, Service, LOG_PREFIX_INT << stream)
#define LOG_C(stream) LOG_CRIT_S  (*NActors::TlsActivationContext, Service, LOG_PREFIX_INT << stream)
#define LOG_A(stream) LOG_ALERT_S (*NActors::TlsActivationContext, Service, LOG_PREFIX_INT << stream)
#define LOG(level, stream) LOG_LOG_S (*NActors::TlsActivationContext, level, Service, LOG_PREFIX_INT << stream)

namespace NKikimr::NPQ {

namespace NPrivate {
    class ILogPrefixBase {
    public:
        virtual const TString& GetLogPrefix() const = 0;
    protected:
        ~ILogPrefixBase() = default;
    };
};

template<typename TDerived>
class TBaseActor : public NActors::TActorBootstrapped<TDerived>
                 , public NActors::IActorExceptionHandler
                 , virtual public NPrivate::ILogPrefixBase {
public:
    using TBase = NActors::TActorBootstrapped<TDerived>;
    using TThis = TDerived;

    TBaseActor(ui64 tabletId, NActors::TActorId tabletActorId, NKikimrServices::EServiceKikimr service)
        : TabletId(tabletId)
        , TabletActorId(tabletActorId)
        , Service(service)
    {
    }

    bool OnUnhandledException(const std::exception& exc) override  {
        LOG_C("unhandled exception " << TypeName(exc) << ": " << exc.what() << Endl
                << TBackTrace::FromCurrentException().PrintToString());

        TDerived& self = static_cast<TDerived&>(*this);
        self.Send(TabletActorId, new NActors::TEvents::TEvPoison());
        self.PassAway();

        return true;
    }

protected:
    const ui64 TabletId;
    const NActors::TActorId TabletActorId;
    const NKikimrServices::EServiceKikimr Service;
};


class TConstantLogPrefix: virtual NPrivate::ILogPrefixBase {
public:
    const TString& GetLogPrefix() const final;
    virtual TString BuildLogPrefix() const = 0;
private:
    mutable TMaybe<TString> LogPrefix_;
};

}
