#pragma once


#include <DBPoco/Util/Application.h>
#include <Client/ClientBase.h>
#include <Client/Suggest.h>
#include <Common/NamePrompter.h>
#include <DBPoco/ConsoleChannel.h>
#include <DBPoco/SimpleFileChannel.h>
#include <DBPoco/SplitterChannel.h>

#include <boost/program_options.hpp>

#include <vector>

namespace po = boost::program_options;

namespace DB
{

void interruptSignalHandler(int signum);

/**
 * The base class for client appliucations such as
 * clickhouse-client or clickhouse-local.
 * The main purpose and responsibility of it is dealing with
 * application-specific stuff such as command line arguments parsing
 * and setting up signal handlers, so queries will be cancelled after
 * Ctrl+C is pressed.
 */
class ClientApplicationBase : public ClientBase, public DBPoco::Util::Application, public IHints<2>
{
public:
    using ClientBase::processOptions;
    using Arguments = ClientBase::Arguments;

    static ClientApplicationBase & getInstance();

    ClientApplicationBase();
    ~ClientApplicationBase() override;

    void init(int argc, char ** argv);
    std::vector<String> getAllRegisteredNames() const override { return cmd_options; }

protected:
    DBPoco::Util::LayeredConfiguration & getClientConfiguration() override;
    void setupSignalHandler() override;
    bool isEmbeeddedClient() const override;
    void addMultiquery(std::string_view query, Arguments & common_arguments) const;

    virtual void readArguments(int argc, char ** argv, Arguments & common_arguments, std::vector<Arguments> &, std::vector<Arguments> &) = 0;

private:
    void parseAndCheckOptions(OptionsDescription & options_description, po::variables_map & options, Arguments & arguments);

    /// Add all options names to the IHints so that we can suggest something meaningful
    /// in case of typo.
    void addOptionsToHints(const OptionsDescription & options_description);

    std::vector<String> cmd_options;

    LoggerPtr fatal_log;
    DBPoco::AutoPtr<DBPoco::SplitterChannel> fatal_channel_ptr;
    DBPoco::AutoPtr<DBPoco::Channel> fatal_console_channel_ptr;
    DBPoco::AutoPtr<DBPoco::Channel> fatal_file_channel_ptr;
    DBPoco::Thread signal_listener_thread;
    std::unique_ptr<DBPoco::Runnable> signal_listener;
};


}
