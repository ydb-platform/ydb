#pragma once

#include <string>
#include <time.h>
#include <DBPoco/Net/StreamSocket.h>
#include <DBPoco/Net/SocketStream.h>
#include <DBPoco/Util/Application.h>
#include <Common/logger_useful.h>


/// Writes to Graphite in the following format
/// path value timestamp\n
/// path can be arbitrary nested. Elements are separated by '.'
/// Example: root_path.server_name.sub_path.key
class GraphiteWriter
{
public:
    explicit GraphiteWriter(const std::string & config_name, const std::string & sub_path = "");

    template <typename T> using KeyValuePair = std::pair<std::string, T>;
    template <typename T> using KeyValueVector = std::vector<KeyValuePair<T>>;

    template <typename T> void write(const std::string & key, const T & value,
                                     time_t timestamp = 0, const std::string & custom_root_path = "")
    {
        writeImpl(KeyValuePair<T>{ key, value }, timestamp, custom_root_path);
    }

    template <typename T> void write(const KeyValueVector<T> & key_val_vec,
                                     time_t timestamp = 0, const std::string & custom_root_path = "")
    {
        writeImpl(key_val_vec, timestamp, custom_root_path);
    }

private:
    template <typename T>
    void writeImpl(const T & data, time_t timestamp, const std::string & custom_root_path)
    {
        if (!timestamp)
            timestamp = time(nullptr);

        try
        {
            DBPoco::Net::SocketAddress socket_address(host, port);
            DBPoco::Net::StreamSocket socket(socket_address);
            socket.setSendTimeout(DBPoco::Timespan(static_cast<DBPoco::Int64>(timeout * 1000000)));
            DBPoco::Net::SocketStream str(socket);

            out(str, data, timestamp, custom_root_path);
        }
        catch (const DBPoco::Exception & e)
        {
            LOG_WARNING(&DBPoco::Util::Application::instance().logger(), "Fail to write to Graphite {}:{}. e.what() = {}, e.message() = {}", host, port, e.what(), e.message());
        }
    }

    template <typename T>
    void out(std::ostream & os, const KeyValuePair<T> & key_val, time_t timestamp, const std::string & custom_root_path)
    {
        os << (custom_root_path.empty() ? root_path : custom_root_path) <<
            '.' << key_val.first << ' ' << key_val.second << ' ' << timestamp << '\n';
    }

    template <typename T>
    void out(std::ostream & os, const KeyValueVector<T> & key_val_vec, time_t timestamp, const std::string & custom_root_path)
    {
        for (const auto & key_val : key_val_vec)
            out(os, key_val, timestamp, custom_root_path);
    }

    std::string root_path;

    int port;
    std::string host;
    double timeout;
};
