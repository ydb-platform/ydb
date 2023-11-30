#include "dnsresolver.h"

#include <ydb/library/actors/core/hfunc.h>

namespace NActors {
namespace NDnsResolver {

    class TOnDemandDnsResolver : public TActor<TOnDemandDnsResolver> {
    public:
        TOnDemandDnsResolver(TOnDemandDnsResolverOptions options)
            : TActor(&TThis::StateWork)
            , Options(std::move(options))
        { }

        static constexpr EActivityType ActorActivityType() {
            return EActivityType::DNS_RESOLVER;
        }

    private:
        STRICT_STFUNC(StateWork, {
            cFunc(TEvents::TEvPoison::EventType, PassAway);
            fFunc(TEvDns::TEvGetHostByName::EventType, Forward);
            fFunc(TEvDns::TEvGetAddr::EventType, Forward);
        });

        void Forward(STATEFN_SIG) {
            ev->Rewrite(ev->GetTypeRewrite(), GetUpstream());
            TActivationContext::Send(ev.Release());
        }

    private:
        TActorId GetUpstream() {
            if (Y_UNLIKELY(!CachingResolverId)) {
                if (Y_LIKELY(!SimpleResolverId)) {
                    SimpleResolverId = RegisterWithSameMailbox(CreateSimpleDnsResolver(Options));
                }
                CachingResolverId = RegisterWithSameMailbox(CreateCachingDnsResolver(SimpleResolverId, Options));
            }
            return CachingResolverId;
        }

        void PassAway() override {
            if (CachingResolverId) {
                Send(CachingResolverId, new TEvents::TEvPoison);
                CachingResolverId = { };
            }
            if (SimpleResolverId) {
                Send(SimpleResolverId, new TEvents::TEvPoison);
                SimpleResolverId = { };
            }
        }

    private:
        TOnDemandDnsResolverOptions Options;
        TActorId SimpleResolverId;
        TActorId CachingResolverId;
    };

    IActor* CreateOnDemandDnsResolver(TOnDemandDnsResolverOptions options) {
        return new TOnDemandDnsResolver(std::move(options));
    }

} // namespace NDnsResolver
} // namespace NActors
