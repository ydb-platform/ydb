#include "UseSSL.h"

#include "clickhouse_config.h"

#if USE_SSL
#    include <CHDBPoco/Net/SSLManager.h>
#endif

namespace DB_CHDB
{
UseSSL::UseSSL()
{
#if USE_SSL
    CHDBPoco::Net::initializeSSL();
#endif
}

UseSSL::~UseSSL()
{
#if USE_SSL
    CHDBPoco::Net::uninitializeSSL();
#endif
}
}
