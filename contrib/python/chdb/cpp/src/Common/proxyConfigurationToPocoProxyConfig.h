#pragma once

#include <CHDBPoco/Net/HTTPClientSession.h>
#include <Common/ProxyConfiguration.h>

namespace DB_CHDB
{

CHDBPoco::Net::HTTPClientSession::ProxyConfig proxyConfigurationToPocoProxyConfig(const DB_CHDB::ProxyConfiguration & proxy_configuration);

std::string buildPocoNonProxyHosts(const std::string & no_proxy_hosts_string);

}
