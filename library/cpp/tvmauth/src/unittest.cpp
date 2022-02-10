#include "service_impl.h" 
#include "user_impl.h" 
 
#include <library/cpp/tvmauth/unittest.h> 
 
namespace NTvmAuth::NUnittest { 
    TCheckedServiceTicket CreateServiceTicket(ETicketStatus status, TTvmId src, TMaybe<TUid> issuerUid) { 
        return TCheckedServiceTicket(TCheckedServiceTicket::TImpl::CreateTicketForTests(status, src, issuerUid)); 
    } 
 
    TCheckedUserTicket CreateUserTicket(ETicketStatus status, TUid defaultUid, const TScopes& scopes, const TUids& uids, EBlackboxEnv env) { 
        return TCheckedUserTicket(TCheckedUserTicket::TImpl::CreateTicketForTests(status, defaultUid, scopes, uids, env)); 
    } 
} 
