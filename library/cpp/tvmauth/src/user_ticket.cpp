#include "user_impl.h" 
 
#include <library/cpp/tvmauth/checked_user_ticket.h> 
 
namespace NTvmAuth { 
    static const char* EX_MSG = "Ticket already moved out"; 
 
    TCheckedUserTicket::TCheckedUserTicket(THolder<TCheckedUserTicket::TImpl> impl) 
        : Impl_(std::move(impl)) 
    { 
    } 
 
    TCheckedUserTicket::TCheckedUserTicket(TCheckedUserTicket&& o) = default; 
    TCheckedUserTicket::~TCheckedUserTicket() = default; 
    TCheckedUserTicket& TCheckedUserTicket::operator=(TCheckedUserTicket&& o) = default; 
 
    TCheckedUserTicket::operator bool() const { 
        Y_ENSURE(Impl_, EX_MSG); 
        return Impl_->operator bool(); 
    } 
 
    const TUids& TCheckedUserTicket::GetUids() const { 
        Y_ENSURE(Impl_, EX_MSG); 
        return Impl_->GetUids(); 
    } 
 
    TUid TCheckedUserTicket::GetDefaultUid() const { 
        Y_ENSURE(Impl_, EX_MSG); 
        return Impl_->GetDefaultUid(); 
    } 
 
    const TScopes& TCheckedUserTicket::GetScopes() const { 
        Y_ENSURE(Impl_, EX_MSG); 
        return Impl_->GetScopes(); 
    } 
 
    bool TCheckedUserTicket::HasScope(TStringBuf scopeName) const { 
        Y_ENSURE(Impl_, EX_MSG); 
        return Impl_->HasScope(scopeName); 
    } 
 
    ETicketStatus TCheckedUserTicket::GetStatus() const { 
        Y_ENSURE(Impl_, EX_MSG); 
        return Impl_->GetStatus(); 
    } 
 
    TString TCheckedUserTicket::DebugInfo() const { 
        Y_ENSURE(Impl_, EX_MSG); 
        return Impl_->DebugInfo(); 
    } 
 
    EBlackboxEnv TCheckedUserTicket::GetEnv() const { 
        Y_ENSURE(Impl_, EX_MSG); 
        return Impl_->GetEnv(); 
    } 
} 
