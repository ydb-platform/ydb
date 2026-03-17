#include <DBPoco/Net/DNS.h>
#include <base/getFQDNOrHostName.h>


namespace
{
    std::string getFQDNOrHostNameImpl()
    {
#if defined(OS_DARWIN)
        return DBPoco::Net::DNS::hostName();
#else
        try
        {
            return DBPoco::Net::DNS::thisHost().name();
        }
        catch (...)
        {
            return DBPoco::Net::DNS::hostName();
        }
#endif
    }
}


const std::string & getFQDNOrHostName()
{
    static std::string result = getFQDNOrHostNameImpl();
    return result;
}
