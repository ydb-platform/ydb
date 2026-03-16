#pragma once

#include <DBPoco/Net/SocketAddress.h>
#include <Common/Logger.h>

namespace DBPoco { class Logger; }

namespace DB
{

DBPoco::Net::SocketAddress makeSocketAddress(const std::string & host, uint16_t port, DBPoco::Logger * log);

DBPoco::Net::SocketAddress makeSocketAddress(const std::string & host, uint16_t port, LoggerPtr log);

}
