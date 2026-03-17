#include <Common/checkSSLReturnCode.h>
#include "clickhouse_config.h"

#if USE_SSL
#include <DBPoco/Net/SecureStreamSocket.h>
#endif

namespace DB
{

bool checkSSLWantRead([[maybe_unused]] ssize_t ret)
{
#if USE_SSL
    return ret == DBPoco::Net::SecureStreamSocket::ERR_SSL_WANT_READ;
#else
    return false;
#endif
}

bool checkSSLWantWrite([[maybe_unused]] ssize_t ret)
{
#if USE_SSL
    return ret == DBPoco::Net::SecureStreamSocket::ERR_SSL_WANT_WRITE;
#else
    return false;
#endif
}

}
