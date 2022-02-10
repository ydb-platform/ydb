#pragma once 
 
#include "checker.h" 
 
namespace NTvmAuth { 
    class TServiceTicketGetter { 
    public: 
        TServiceTicketGetter(TAsyncUpdaterPtr updater) 
            : Updater_(std::move(updater)) 
        { 
            Y_ENSURE(Updater_); 
            GetCache(); 
        } 
 
        /*! 
         * Fetching must enabled in TClientSettings 
         * Can throw exception if cache is invalid or wrong config 
         * @param dst 
         */ 
        TString GetTicket(const TClientSettings::TAlias& dst) const { 
            TServiceTicketsPtr c = GetCache(); 
            return GetTicketImpl(dst, c->TicketsByAlias, c->ErrorsByAlias, c->UnfetchedAliases); 
        } 
 
        TString GetTicket(const TTvmId dst) const { 
            TServiceTicketsPtr c = GetCache(); 
            return GetTicketImpl(dst, c->TicketsById, c->ErrorsById, c->UnfetchedIds); 
        } 
 
    private: 
        template <class Key, class Cont, class UnfetchedCont>
        TString GetTicketImpl(const Key& dst, const Cont& tickets, const Cont& errors, const UnfetchedCont& unfetched) const {
            auto it = tickets.find(dst); 
            if (it != tickets.end()) { 
                return it->second; 
            } 
 
            it = errors.find(dst); 
            if (it != errors.end()) { 
                ythrow TMissingServiceTicket() 
                    << "Failed to get ticket for '" << dst << "': " 
                    << it->second; 
            } 
 
            if (unfetched.contains(dst)) {
                ythrow TMissingServiceTicket()
                    << "Failed to get ticket for '" << dst << "': this dst was not fetched yet.";
            }

            ythrow TBrokenTvmClientSettings() 
                << "Destination '" << dst << "' was not specified in settings. " 
                << "Check your settings (if you use Qloud/YP/tvmtool - check it's settings)"; 
        } 
 
    private: 
        TServiceTicketsPtr GetCache() const { 
            TServiceTicketsPtr c = Updater_->GetCachedServiceTickets(); 
            Y_ENSURE_EX(c, TBrokenTvmClientSettings() 
                               << "Need to use TClientSettings::EnableServiceTicketsFetchOptions()"); 
            return c; 
        } 
 
    private: 
        TAsyncUpdaterPtr Updater_; 
    }; 
} 
