#include "context.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

bool operator==(const TClientContext& lhs, const TClientContext& rhs)
{
    return lhs.ServerName == rhs.ServerName &&
           lhs.Token == rhs.Token &&
           lhs.ImpersonationUser == rhs.ImpersonationUser &&
           lhs.ServiceTicketAuth == rhs.ServiceTicketAuth &&
           lhs.HttpClient == rhs.HttpClient &&
           lhs.UseTLS == rhs.UseTLS &&
           lhs.TvmOnly == rhs.TvmOnly &&
           lhs.ProxyAddress == rhs.ProxyAddress;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
