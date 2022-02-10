#pragma once 
 
#include "async_updater.h" 
 
#include <library/cpp/tvmauth/client/exception.h> 
 
#include <library/cpp/tvmauth/checked_service_ticket.h> 
#include <library/cpp/tvmauth/src/service_impl.h> 
 
namespace NTvmAuth { 
    class TSrcChecker { 
    public: 
        TSrcChecker(TAsyncUpdaterPtr updater) 
            : Updater_(std::move(updater)) 
        { 
            Y_ENSURE(Updater_); 
            GetCache(); 
        } 
 
        /*! 
         * Checking must be enabled in TClientSettings 
         * Can throw exception if cache is out of date or wrong config 
         * @param ticket 
         */ 
        TCheckedServiceTicket Check(TCheckedServiceTicket ticket) const { 
            NRoles::TConsumerRolesPtr roles = GetCache()->GetRolesForService(ticket); 
            if (roles) { 
                return ticket; 
            } 
 
            TServiceTicketImplPtr impl = THolder(NInternal::TCanningKnife::GetS(ticket));
            impl->SetStatus(ETicketStatus::NoRoles); 
            return TCheckedServiceTicket(std::move(impl)); 
        } 
 
    private: 
        NRoles::TRolesPtr GetCache() const { 
            NRoles::TRolesPtr c = Updater_->GetRoles(); 
            Y_ENSURE_EX(c, TBrokenTvmClientSettings() << "Need to use TClientSettings::EnableRolesFetching()"); 
            return c; 
        } 
 
    private: 
        TAsyncUpdaterPtr Updater_; 
    }; 
} 
