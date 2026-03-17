#pragma once

#include <CHDBPoco/Net/SocketAddress.h>
#include <Common/Logger.h>

namespace CHDBPoco { class Logger; }

namespace DB_CHDB
{

CHDBPoco::Net::SocketAddress makeSocketAddress(const std::string & host, uint16_t port, CHDBPoco::Logger * log);

CHDBPoco::Net::SocketAddress makeSocketAddress(const std::string & host, uint16_t port, LoggerPtr log);

}
