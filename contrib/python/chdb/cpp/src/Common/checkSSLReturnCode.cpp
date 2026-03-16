#include <Common/checkSSLReturnCode.h>
#include "clickhouse_config.h"

#if USE_SSL
#include <CHDBPoco/Net/SecureStreamSocket.h>
#endif

namespace DB_CHDB
{

bool checkSSLWantRead([[maybe_unused]] ssize_t ret)
{
#if USE_SSL
    return ret == CHDBPoco::Net::SecureStreamSocket::ERR_SSL_WANT_READ;
#else
    return false;
#endif
}

bool checkSSLWantWrite([[maybe_unused]] ssize_t ret)
{
#if USE_SSL
    return ret == CHDBPoco::Net::SecureStreamSocket::ERR_SSL_WANT_WRITE;
#else
    return false;
#endif
}

}
