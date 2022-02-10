#include "async_updater.h" 
 
#include "utils.h" 
 
#include <library/cpp/tvmauth/client/exception.h> 
 
#include <util/string/builder.h> 
#include <util/system/spin_wait.h> 
 
namespace NTvmAuth { 
    TAsyncUpdaterBase::TAsyncUpdaterBase() { 
        ServiceTicketsDurations_.RefreshPeriod = TDuration::Hours(1); 
        ServiceTicketsDurations_.Expiring = TDuration::Hours(2); 
        ServiceTicketsDurations_.Invalid = TDuration::Hours(11); 
 
        PublicKeysDurations_.RefreshPeriod = TDuration::Days(1); 
        PublicKeysDurations_.Expiring = TDuration::Days(2); 
        PublicKeysDurations_.Invalid = TDuration::Days(6); 
    } 
 
    NRoles::TRolesPtr TAsyncUpdaterBase::GetRoles() const { 
        ythrow TIllegalUsage() << "not implemented"; 
    } 
 
    TInstant TAsyncUpdaterBase::GetUpdateTimeOfPublicKeys() const { 
        return PublicKeysTime_.Get(); 
    } 
 
    TInstant TAsyncUpdaterBase::GetUpdateTimeOfServiceTickets() const { 
        return ServiceTicketsTime_.Get(); 
    } 
 
    TInstant TAsyncUpdaterBase::GetUpdateTimeOfRoles() const { 
        return RolesTime_.Get(); 
    } 
 
    TInstant TAsyncUpdaterBase::GetInvalidationTimeOfPublicKeys() const { 
        TInstant ins = GetUpdateTimeOfPublicKeys(); 
        return ins == TInstant() ? TInstant() : ins + PublicKeysDurations_.Invalid; 
    } 
 
    TInstant TAsyncUpdaterBase::GetInvalidationTimeOfServiceTickets() const { 
        TServiceTicketsPtr c = GetCachedServiceTickets(); 
        return c ? c->InvalidationTime : TInstant(); 
    } 
 
    bool TAsyncUpdaterBase::ArePublicKeysInvalid(TInstant now) const { 
        return IsInvalid(GetInvalidationTimeOfPublicKeys(), now); 
    } 
 
    bool TAsyncUpdaterBase::AreServiceTicketsInvalid(TInstant now) const { 
        TServiceTicketsPtr c = GetCachedServiceTickets();
        // Empty set of tickets is allways valid.
        return c && !c->TicketsById.empty() && IsInvalid(GetInvalidationTimeOfServiceTickets(), now); 
    } 
 
    bool TAsyncUpdaterBase::IsInvalid(TInstant invTime, TInstant now) { 
        return invTime - 
                   TDuration::Minutes(1) // lag for closing from balancer 
               < now; 
    } 
 
    void TAsyncUpdaterBase::SetBbEnv(EBlackboxEnv original, TMaybe<EBlackboxEnv> overrided) { 
        if (overrided) { 
            Y_ENSURE_EX(NUtils::CheckBbEnvOverriding(original, *overrided), 
                        TBrokenTvmClientSettings() << "Overriding of BlackboxEnv is illegal: " 
                                                   << original << " -> " << *overrided); 
        } 
 
        Envs_.store({original, overrided}, std::memory_order_relaxed); 
    } 
 
    TServiceTicketsPtr TAsyncUpdaterBase::GetCachedServiceTickets() const { 
        return ServiceTickets_.Get(); 
    } 
 
    TServiceContextPtr TAsyncUpdaterBase::GetCachedServiceContext() const { 
        return ServiceContext_.Get(); 
    } 
 
    TUserContextPtr TAsyncUpdaterBase::GetCachedUserContext(TMaybe<EBlackboxEnv> overridenEnv) const { 
        TAllUserContextsPtr ctx = AllUserContexts_.Get(); 
        if (!ctx) { 
            return nullptr; 
        } 
 
        const TEnvs envs = Envs_.load(std::memory_order_relaxed); 
        if (!envs.Original) { 
            return nullptr; 
        } 
 
        EBlackboxEnv env = *envs.Original; 
 
        if (overridenEnv) { 
            Y_ENSURE_EX(NUtils::CheckBbEnvOverriding(*envs.Original, *overridenEnv), 
                        TBrokenTvmClientSettings() << "Overriding of BlackboxEnv is illegal: " 
                                                   << *envs.Original << " -> " << *overridenEnv); 
            env = *overridenEnv; 
        } else if (envs.Overrided) { 
            env = *envs.Overrided; 
        } 
 
        return ctx->Get(env); 
    } 
 
    void TAsyncUpdaterBase::SetServiceTickets(TServiceTicketsPtr c) { 
        ServiceTickets_.Set(std::move(c)); 
    } 
 
    void TAsyncUpdaterBase::SetServiceContext(TServiceContextPtr c) { 
        ServiceContext_.Set(std::move(c)); 
    } 
 
    void TAsyncUpdaterBase::SetUserContext(TStringBuf publicKeys) { 
        AllUserContexts_.Set(MakeIntrusiveConst<TAllUserContexts>(publicKeys)); 
    } 
 
    void TAsyncUpdaterBase::SetUpdateTimeOfPublicKeys(TInstant ins) { 
        PublicKeysTime_.Set(ins); 
    } 
 
    void TAsyncUpdaterBase::SetUpdateTimeOfServiceTickets(TInstant ins) { 
        ServiceTicketsTime_.Set(ins); 
    } 
 
    void TAsyncUpdaterBase::SetUpdateTimeOfRoles(TInstant ins) { 
        RolesTime_.Set(ins); 
    } 
 
    bool TAsyncUpdaterBase::IsServiceTicketMapOk(TServiceTicketsPtr c, size_t expectedTicketCount, bool strict) { 
        return c && 
               (strict 
                    ? c->TicketsById.size() == expectedTicketCount 
                    : !c->TicketsById.empty()); 
    } 
 
    TAllUserContexts::TAllUserContexts(TStringBuf publicKeys) { 
        auto add = [&, this](EBlackboxEnv env) { 
            Ctx_[(size_t)env] = MakeIntrusiveConst<TUserContext>(env, publicKeys); 
        }; 
 
        add(EBlackboxEnv::Prod); 
        add(EBlackboxEnv::Test); 
        add(EBlackboxEnv::ProdYateam); 
        add(EBlackboxEnv::TestYateam); 
        add(EBlackboxEnv::Stress); 
    } 
 
    TUserContextPtr TAllUserContexts::Get(EBlackboxEnv env) const { 
        return Ctx_[(size_t)env]; 
    } 
} 
