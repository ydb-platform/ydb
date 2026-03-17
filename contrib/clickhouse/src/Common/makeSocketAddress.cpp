#include <Common/makeSocketAddress.h>
#include <Common/logger_useful.h>
#include <DBPoco/Net/NetException.h>

namespace DB
{

DBPoco::Net::SocketAddress makeSocketAddress(const std::string & host, uint16_t port, DBPoco::Logger * log)
{
    DBPoco::Net::SocketAddress socket_address;
    try
    {
        socket_address = DBPoco::Net::SocketAddress(host, port);
    }
    catch (const DBPoco::Net::DNSException & e)
    {
        const auto code = e.code();
        if (code == EAI_FAMILY
#if defined(EAI_ADDRFAMILY)
                    || code == EAI_ADDRFAMILY
#endif
        )
        {
            LOG_ERROR(log, "Cannot resolve listen_host ({}), error {}: {}. "
                "If it is an IPv6 address and your host has disabled IPv6, then consider to "
                "specify IPv4 address to listen in <listen_host> element of configuration "
                "file. Example: <listen_host>0.0.0.0</listen_host>",
                host, e.code(), e.message());
        }

        throw;
    }
    return socket_address;
}

DBPoco::Net::SocketAddress makeSocketAddress(const std::string & host, uint16_t port, LoggerPtr log)
{
    return makeSocketAddress(host, port, log.get());
}

}
