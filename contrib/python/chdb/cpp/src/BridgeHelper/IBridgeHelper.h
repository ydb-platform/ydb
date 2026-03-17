#pragma once

#include <Interpreters/Context.h>
#include <CHDBPoco/Logger.h>
#include <CHDBPoco/Util/AbstractConfiguration.h>
#include <CHDBPoco/Net/HTTPRequest.h>
#include <Common/ShellCommand.h>


namespace DB_CHDB
{

/// Base class for server-side bridge helpers, e.g. xdbc-bridge and library-bridge.
/// Contains helper methods to check/start bridge sync
class IBridgeHelper: protected WithContext
{

public:
    static constexpr auto DEFAULT_HOST = "127.0.0.1";
    static constexpr auto DEFAULT_FORMAT = "RowBinary";
    static constexpr auto PING_OK_ANSWER = "Ok.";

    static const inline std::string PING_METHOD = CHDBPoco::Net::HTTPRequest::HTTP_GET;
    static const inline std::string MAIN_METHOD = CHDBPoco::Net::HTTPRequest::HTTP_POST;

    explicit IBridgeHelper(ContextPtr context_) : WithContext(context_) {}

    virtual ~IBridgeHelper() = default;

    virtual CHDBPoco::URI getMainURI() const = 0;

    virtual CHDBPoco::URI getPingURI() const = 0;

    void startBridgeSync();

protected:
    /// Check bridge is running. Can also check something else in the mean time.
    virtual bool bridgeHandShake() = 0;

    virtual String serviceAlias() const = 0;

    virtual String serviceFileName() const = 0;

    virtual unsigned getDefaultPort() const = 0;

    virtual bool startBridgeManually() const = 0;

    virtual void startBridge(std::unique_ptr<ShellCommand> cmd) const = 0;

    virtual String configPrefix() const = 0;

    virtual const CHDBPoco::Util::AbstractConfiguration & getConfig() const = 0;

    virtual LoggerPtr getLog() const = 0;

    virtual CHDBPoco::Timespan getHTTPTimeout() const = 0;

    virtual CHDBPoco::URI createBaseURI() const = 0;


private:
    std::unique_ptr<ShellCommand> startBridgeCommand();
};

}
