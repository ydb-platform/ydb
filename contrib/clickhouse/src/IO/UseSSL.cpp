#include "UseSSL.h"

#include "clickhouse_config.h"

#if USE_SSL
#    include <DBPoco/Net/SSLManager.h>
#endif

namespace DB
{
UseSSL::UseSSL()
{
#if USE_SSL
    DBPoco::Net::initializeSSL();
#endif
}

UseSSL::~UseSSL()
{
#if USE_SSL
    DBPoco::Net::uninitializeSSL();
#endif
}
}
