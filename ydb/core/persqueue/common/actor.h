#pragma once

#include <ydb/core/base/appdata.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/log.h>
#include <ydb/library/services/services.pb.h>

#define NPQ_LOG_PREFIX LogBuilder() << GetLogPrefix()
#define LOG(level, stream) LOG_LOG_S (*NActors::TlsActivationContext, level, Service, NPQ_LOG_PREFIX << stream)
#define LOG_T(stream) LOG_TRACE_S (*NActors::TlsActivationContext, Service, NPQ_LOG_PREFIX << stream)
#define LOG_D(stream) LOG_DEBUG_S (*NActors::TlsActivationContext, Service, NPQ_LOG_PREFIX << stream)
#define LOG_I(stream) LOG_INFO_S  (*NActors::TlsActivationContext, Service, NPQ_LOG_PREFIX << stream)
#define LOG_N(stream) LOG_NOTICE_S(*NActors::TlsActivationContext, Service, NPQ_LOG_PREFIX << stream)
#define LOG_W(stream) LOG_WARN_S  (*NActors::TlsActivationContext, Service, NPQ_LOG_PREFIX << stream)
#define LOG_E(stream) LOG_ERROR_S (*NActors::TlsActivationContext, Service, NPQ_LOG_PREFIX << stream)
#define LOG_C(stream) LOG_CRIT_S  (*NActors::TlsActivationContext, Service, NPQ_LOG_PREFIX << stream)
#define LOG_A(stream) LOG_ALERT_S (*NActors::TlsActivationContext, Service, NPQ_LOG_PREFIX << stream)

namespace NKikimr::NPQ {

void DoLogUnhandledException(NKikimrServices::EServiceKikimr service, const TStringBuf prefix, const std::exception& exc);

namespace NPrivate {
    class ILogPrefixBase {
    public:
        virtual const TString& GetLogPrefix() const = 0;
    protected:
        ~ILogPrefixBase() = default;
    };

    void IncrementUnhandledExceptionCounter(const NActors::TActorContext& ctx);
};

template<typename TDerived>
class TBaseActor : public NActors::TActorBootstrapped<TDerived>
                 , public NActors::IActorExceptionHandler
                 , virtual public NPrivate::ILogPrefixBase {
public:
    using TBase = NActors::TActorBootstrapped<TDerived>;
    using TThis = TDerived;

    TBaseActor(NKikimrServices::EServiceKikimr service)
        : Service(service)
    {
    }

    bool OnUnhandledException(const std::exception& exc) override {
        if (AppData()->FeatureFlags.GetEnableTabletRestartOnUnhandledExceptions()) {
            DoLogUnhandledException(Service, NPQ_LOG_PREFIX, exc);

            NPrivate::IncrementUnhandledExceptionCounter(this->ActorContext());
            this->PassAway();

            return true;
        }

        return false;
    }

    TStringBuilder LogBuilder() const {
        return TStringBuilder() << "[" << TBase::SelfId() << "]";
    }

protected:
    template <typename TEv>
    TString EventStr(const char * func, const TEv& ev) {
        return TStringBuilder() << func << " event# " << ev->GetTypeRewrite() << " (" << ev->GetTypeName() << ") "
            << ", Sender " << ev->Sender.ToString() << ", Recipient " << ev->Recipient.ToString()
            << ", Cookie: " << ev->Cookie;
    }

protected:
    const NKikimrServices::EServiceKikimr Service;
};


template<typename TDerived>
class TBaseTabletActor : public TBaseActor<TDerived> {
public:
    using TBase = TBaseActor<TDerived>;
    using TThis = TDerived;

    TBaseTabletActor(ui64 tabletId, NActors::TActorId tabletActorId, NKikimrServices::EServiceKikimr service)
        : TBaseActor<TDerived>(service)
        , TabletId(tabletId)
        , TabletActorId(tabletActorId)
    {
    }

    bool OnUnhandledException(const std::exception& exc) override  {
        RestartTablet();
        return TBase::OnUnhandledException(exc);
    }

    void RestartTablet() {
        TDerived& self = static_cast<TDerived&>(*this);
        self.Send(TabletActorId, new NActors::TEvents::TEvPoison());
    }

    TStringBuilder LogBuilder() const {
        return TStringBuilder() << "[" << TabletId << "]";
    }

protected:
    const ui64 TabletId;
    const NActors::TActorId TabletActorId;
};


class TConstantLogPrefix: virtual NPrivate::ILogPrefixBase {
public:
    const TString& GetLogPrefix() const final;
    virtual TString BuildLogPrefix() const {
        return " ";
    }

private:
    mutable TMaybe<TString> LogPrefix_;
};

}
