#include <library/cpp/tvmauth/checked_user_ticket.h> 
#include <library/cpp/tvmauth/src/user_impl.h> 
 
namespace NTvmAuth { 
    static const char* EX_MSG = "UserContext already moved out"; 
 
    TUserContext::TUserContext(EBlackboxEnv env, TStringBuf tvmKeysResponse) 
        : Impl_(MakeHolder<TImpl>(env, tvmKeysResponse)) 
    { 
    } 
 
    TUserContext::TUserContext(TUserContext&& o) = default; 
    TUserContext& TUserContext::operator=(TUserContext&& o) = default; 
    TUserContext::~TUserContext() = default; 
 
    TCheckedUserTicket TUserContext::Check(TStringBuf ticketBody) const { 
        Y_ENSURE(Impl_, EX_MSG); 
        return Impl_->Check(ticketBody); 
    } 
} 
