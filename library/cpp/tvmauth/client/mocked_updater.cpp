#include "mocked_updater.h" 
 
#include <library/cpp/tvmauth/unittest.h> 
 
namespace NTvmAuth { 
    TMockedUpdater::TSettings TMockedUpdater::TSettings::CreateDeafult() { 
        TMockedUpdater::TSettings res; 
 
        res.SelfTvmId = 100500; 
 
        res.Backends = { 
            { 
                /*.Alias_ = */ "my_dest", 
                /*.Id_ = */ 42, 
                /*.Value_ = */ "3:serv:CBAQ__________9_IgYIlJEGECo:O9-vbod_8czkKrpwJAZCI8UgOIhNr2xKPcS-LWALrVC224jga2nIT6vLiw6q3d6pAT60g9K7NB39LEmh7vMuePtUMjzuZuL-uJg17BsH2iTLCZSxDjWxbU9piA2T6u607jiSyiy-FI74pEPqkz7KKJ28aPsefuC1VUweGkYFzNY", 
            }, 
        }; 
 
        res.BadBackends = { 
            { 
                /*.Alias_ = */ "my_bad_dest", 
                /*.Id_ = */ 43, 
                /*.Value_ = */ "Dst is not found", 
            }, 
        }; 
 
        return res; 
    } 
 
    TMockedUpdater::TMockedUpdater(const TSettings& settings) 
        : Roles_(settings.Roles) 
    { 
        SetServiceContext(MakeIntrusiveConst<TServiceContext>(TServiceContext::CheckingFactory( 
            settings.SelfTvmId, 
            NUnittest::TVMKNIFE_PUBLIC_KEYS))); 
 
        SetBbEnv(settings.UserTicketEnv); 
        SetUserContext(NUnittest::TVMKNIFE_PUBLIC_KEYS); 
 
        TServiceTickets::TMapIdStr tickets, errors; 
        TServiceTickets::TMapAliasId aliases; 
 
        for (const TSettings::TTuple& t : settings.Backends) { 
            tickets[t.Id] = t.Value; 
            aliases[t.Alias] = t.Id; 
        } 
        for (const TSettings::TTuple& t : settings.BadBackends) { 
            errors[t.Id] = t.Value; 
            aliases[t.Alias] = t.Id; 
        } 
 
        SetServiceTickets(MakeIntrusiveConst<TServiceTickets>( 
            std::move(tickets), 
            std::move(errors), 
            std::move(aliases))); 
 
        SetUpdateTimeOfPublicKeys(TInstant::Now()); 
        SetUpdateTimeOfServiceTickets(TInstant::Now()); 
    } 
} 
