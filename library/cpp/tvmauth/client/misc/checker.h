#pragma once 
 
#include "async_updater.h" 
 
#include <library/cpp/tvmauth/client/exception.h> 
 
#include <library/cpp/tvmauth/checked_service_ticket.h> 
#include <library/cpp/tvmauth/checked_user_ticket.h> 
 
namespace NTvmAuth { 
    class TServiceTicketChecker { 
    public: 
        TServiceTicketChecker(TAsyncUpdaterPtr updater) 
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
        TCheckedServiceTicket Check(TStringBuf ticket) const { 
            return GetCache()->Check(ticket); 
        } 
 
    private: 
        TServiceContextPtr GetCache() const { 
            TServiceContextPtr c = Updater_->GetCachedServiceContext(); 
            Y_ENSURE_EX(c, TBrokenTvmClientSettings() << "Need to use TClientSettings::EnableServiceTicketChecking()"); 
            return c; 
        } 
 
    private: 
        TAsyncUpdaterPtr Updater_; 
    }; 
 
    class TUserTicketChecker { 
    public: 
        TUserTicketChecker(TAsyncUpdaterPtr updater) 
            : Updater_(std::move(updater)) 
        { 
            Y_ENSURE(Updater_); 
            GetCache({}); 
        } 
 
        /*! 
         * Blackbox enviroment must be cofingured in TClientSettings 
         * Can throw exception if cache is out of date or wrong config 
         */ 
        TCheckedUserTicket Check(TStringBuf ticket, TMaybe<EBlackboxEnv> overridenEnv) const { 
            return GetCache(overridenEnv)->Check(ticket); 
        } 
 
    private: 
        TUserContextPtr GetCache(TMaybe<EBlackboxEnv> overridenEnv) const { 
            TUserContextPtr c = Updater_->GetCachedUserContext(overridenEnv); 
            Y_ENSURE_EX(c, TBrokenTvmClientSettings() << "Need to use TClientSettings::EnableUserTicketChecking()"); 
            return c; 
        } 
 
    private: 
        TAsyncUpdaterPtr Updater_; 
    }; 
} 
