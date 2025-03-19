#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/client.h>

#include <library/cpp/getopt/last_getopt.h>

struct TOptions {
    std::string Endpoint;
    std::string Database;
    std::string TopicPath;
    std::string ConsumerName;
    bool CommitAfterProcessing = false;
    bool DisableClusterDiscovery = false;
    bool UseSecureConnection = false;

    TOptions(int argc, const char* argv[]) {
        NLastGetopt::TOpts opts = NLastGetopt::TOpts::Default();
        opts.AddHelpOption('h');
        opts.AddLongOption('e', "endpoint", "YDB endpoint").Required().RequiredArgument("HOST:PORT")
            .StoreResult(&Endpoint);
        opts.AddLongOption('d', "database", "YDB database name").DefaultValue("/local").RequiredArgument("PATH")
            .StoreResult(&Database);
        opts.AddLongOption('t', "topic-path", "Topic path for reading").Required().RequiredArgument("PATH")
            .StoreResult(&TopicPath);
        opts.AddLongOption('c', "consumer", "Consumer name").Required().RequiredArgument("CONSUMER")
            .StoreResult(&ConsumerName);
        opts.AddLongOption("commit-after-processing", "Commit data after processing")
            .SetFlag(&CommitAfterProcessing).NoArgument();
        opts.AddLongOption("secure-connection", "Use secure connection")
            .SetFlag(&UseSecureConnection).NoArgument();
        opts.SetFreeArgsNum(0);

        NLastGetopt::TOptsParseResult res(&opts, argc, argv);
    }
};

std::shared_ptr<NYdb::NTopic::IReadSession> ReadSession;

void StopHandler(int) {
    std::cerr << "Stopping session" << std::endl;
    if (ReadSession) {
        ReadSession->Close(TDuration::Seconds(3));
    } else {
        exit(1);
    }
}

int main(int argc, const char* argv[]) {
    signal(SIGINT, &StopHandler);
    signal(SIGTERM, &StopHandler);

    TOptions opts(argc, argv);

    // Create driver instance.
    auto driverConfig = NYdb::TDriverConfig()
        .SetNetworkThreadsNum(2)
        .SetEndpoint(opts.Endpoint)
        .SetDatabase(opts.Database)
        .SetAuthToken(std::getenv("YDB_TOKEN") ? std::getenv("YDB_TOKEN") : "")
        .SetLog(std::unique_ptr<TLogBackend>(CreateLogBackend("cerr").Release()));

    if (opts.UseSecureConnection) {
        driverConfig.UseSecureConnection();
    }

    NYdb::TDriver driver(driverConfig);

    // Create topic client.
    NYdb::NTopic::TTopicClient topicClient(driver);

    // [BEGIN Create read session]
    // Create read session.
    NYdb::NTopic::TReadSessionSettings settings;
    settings
        .ConsumerName(opts.ConsumerName)
        .AppendTopics(opts.TopicPath);

    // auto handlers = TEventHandlers::SimpleDataHandlers();

    // settings.SetEventHandlers(handlers);

    // settings.SetSimpleDataHandlers(
    settings.EventHandlers_.SimpleDataHandlers(
            [](NYdb::NTopic::TReadSessionEvent::TDataReceivedEvent& event) {
            std::cerr << "Data event " << DebugString(event);
        }, opts.CommitAfterProcessing);

    ReadSession = topicClient.CreateReadSession(settings);
    // [END Create read session]

    std::cerr << "Session was created" << std::endl;

    // Wait SessionClosed event.
    ReadSession->GetEvent(/*block = */true);

    // Stop the driver.
    driver.Stop();
}
