#include <CHDBPoco/Net/DNS.h>
#include <base/getFQDNOrHostName.h>


namespace
{
    std::string getFQDNOrHostNameImpl()
    {
#if defined(OS_DARWIN)
        return CHDBPoco::Net::DNS::hostName();
#else
        try
        {
            return CHDBPoco::Net::DNS::thisHost().name();
        }
        catch (...)
        {
            return CHDBPoco::Net::DNS::hostName();
        }
#endif
    }
}


const std::string & getFQDNOrHostName()
{
    static std::string result = getFQDNOrHostNameImpl();
    return result;
}
