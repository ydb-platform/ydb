#pragma once

#include <Server/IServer.h>

#include <Daemon/BaseDaemon.h>
#include <Server/HTTP/HTTPContext.h>
#include <Server/TCPProtocolStackFactory.h>
#include <Server/ServerType.h>
#include <DBPoco/Net/HTTPServerParams.h>

/** Server provides three interfaces:
  * 1. HTTP - simple interface for any applications.
  * 2. TCP - interface for native clickhouse-client and for server to server internal communications.
  *    More rich and efficient, but less compatible
  *     - data is transferred by columns;
  *     - data is transferred compressed;
  *    Allows to get more information in response.
  * 3. Interserver HTTP - for replication.
  */

namespace DBPoco
{
    namespace Net
    {
        class ServerSocket;
    }
}

namespace DB
{
class AsynchronousMetrics;
class ProtocolServerAdapter;

class Server : public BaseDaemon, public IServer
{
public:
    using ServerApplication::run;

    DBPoco::Util::LayeredConfiguration & config() const override
    {
        return BaseDaemon::config();
    }

    DBPoco::Logger & logger() const override
    {
        return BaseDaemon::logger();
    }

    ContextMutablePtr context() const override
    {
        return global_context;
    }

    bool isCancelled() const override
    {
        return BaseDaemon::isCancelled();
    }

    void defineOptions(DBPoco::Util::OptionSet & _options) override;

protected:
    int run() override;

    void initialize(Application & self) override;

    void uninitialize() override;

    int main(const std::vector<std::string> & args) override;

    std::string getDefaultCorePath() const override;

private:
    ContextMutablePtr global_context;
    /// Updated/recent config, to compare http_handlers
    ConfigurationPtr latest_config;

    HTTPContextPtr httpContext() const;

    DBPoco::Net::SocketAddress socketBindListen(
        const DBPoco::Util::AbstractConfiguration & config,
        DBPoco::Net::ServerSocket & socket,
        const std::string & host,
        UInt16 port,
        [[maybe_unused]] bool secure = false) const;

    std::unique_ptr<TCPProtocolStackFactory> buildProtocolStackFromConfig(
        const DBPoco::Util::AbstractConfiguration & config,
        const std::string & protocol,
        DBPoco::Net::HTTPServerParams::Ptr http_params,
        AsynchronousMetrics & async_metrics,
        bool & is_secure);

    using CreateServerFunc = std::function<ProtocolServerAdapter(UInt16)>;
    void createServer(
        DBPoco::Util::AbstractConfiguration & config,
        const std::string & listen_host,
        const char * port_name,
        bool listen_try,
        bool start_server,
        std::vector<ProtocolServerAdapter> & servers,
        CreateServerFunc && func) const;

    void createServers(
        DBPoco::Util::AbstractConfiguration & config,
        const Strings & listen_hosts,
        bool listen_try,
        DBPoco::ThreadPool & server_pool,
        AsynchronousMetrics & async_metrics,
        std::vector<ProtocolServerAdapter> & servers,
        bool start_servers = false,
        const ServerType & server_type = ServerType(ServerType::Type::QUERIES_ALL));

    void createInterserverServers(
        DBPoco::Util::AbstractConfiguration & config,
        const Strings & interserver_listen_hosts,
        bool listen_try,
        DBPoco::ThreadPool & server_pool,
        AsynchronousMetrics & async_metrics,
        std::vector<ProtocolServerAdapter> & servers,
        bool start_servers = false,
        const ServerType & server_type = ServerType(ServerType::Type::QUERIES_ALL));

    void updateServers(
        DBPoco::Util::AbstractConfiguration & config,
        DBPoco::ThreadPool & server_pool,
        AsynchronousMetrics & async_metrics,
        std::vector<ProtocolServerAdapter> & servers,
        std::vector<ProtocolServerAdapter> & servers_to_start_before_tables);

    void stopServers(
        std::vector<ProtocolServerAdapter> & servers,
        const ServerType & server_type) const;
};

}
