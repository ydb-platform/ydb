#include <ydb/public/sdk/cpp/client/ydb_persqueue_public/persqueue.h>

#include <library/cpp/getopt/last_getopt.h>
#include <library/cpp/threading/future/future.h>

#include <util/stream/output.h>
#include <util/system/env.h>

#include <atomic>
#include <thread>

struct TOptions {
    TString Endpoint;
    TString Database;
    TString TopicPath;
    TString ConsumerName;
    bool CommitAfterProcessing = false;
    bool DisableClusterDiscovery = false;
    size_t MessagesCount = std::numeric_limits<size_t>::max();
    TDuration Timeout = TDuration::Seconds(30);

    TOptions(int argc, const char* argv[]) {
        NLastGetopt::TOpts opts = NLastGetopt::TOpts::Default();
        opts.AddHelpOption('h');
        opts.AddLongOption('e', "endpoint", "YDB endpoint").Required().RequiredArgument("HOST:PORT")
            .StoreResult(&Endpoint);
        opts.AddLongOption('d', "database", "YDB database name").DefaultValue("/Root").RequiredArgument("PATH")
            .StoreResult(&Database);
        opts.AddLongOption('t', "topic-path", "Topic path for reading").Required().RequiredArgument("PATH")
            .StoreResult(&TopicPath);
        opts.AddLongOption('c', "consumer-name", "Consumer name").Required().RequiredArgument("CONSUMER")
            .StoreResult(&ConsumerName);
        opts.AddLongOption("commit-after-processing", "Commit data after processing")
            .SetFlag(&CommitAfterProcessing).NoArgument();
        opts.AddLongOption("disable-cluster-discovery", "Disable cluster discovery")
            .SetFlag(&DisableClusterDiscovery).NoArgument();
        opts.AddLongOption("messages-count", "Wait for specified messages count").RequiredArgument("COUNT")
            .StoreResult(&MessagesCount);
        opts.AddLongOption("timeout", "When no data arrives during this timeout session will be closed").RequiredArgument("DURATION")
            .StoreResult(&Timeout).DefaultValue(TDuration::Seconds(30));
        opts.SetFreeArgsNum(0);

        NLastGetopt::TOptsParseResult res(&opts, argc, argv);
    }
};

int main(int argc, const char* argv[]) {
    TOptions opts(argc, argv);

    // Create driver instance.
    auto driverConfig = NYdb::TDriverConfig()
        .SetNetworkThreadsNum(2)
        .SetEndpoint(opts.Endpoint)
        .SetDatabase(opts.Database)
        .SetLog(CreateLogBackend("cerr"));
    NYdb::TDriver driver(driverConfig);

    NYdb::NPersQueue::TPersQueueClient persqueueClient(driver);

    NYdb::NPersQueue::TReadSessionSettings settings;
    settings
        .DisableClusterDiscovery(opts.DisableClusterDiscovery)
        .ConsumerName(opts.ConsumerName)
        .AppendTopics(opts.TopicPath);

    std::shared_ptr<NYdb::NPersQueue::IReadSession> readSession;
    size_t messagesReceived = 0;
    THashMap<ui64, ui64> maxReceivedOffsets; // partition id -> max received offset
    THashMap<ui64, ui64> committedOffsets; // partition id -> confirmed offset
    std::atomic<bool> closing = false;
    std::atomic<TInstant::TValue> lastDataReceiveTime = TInstant::Now().GetValue();

    settings
        .EventHandlers_.SimpleDataHandlers(
            [&messagesReceived, &maxReceivedOffsets, &closing, &lastDataReceiveTime](NYdb::NPersQueue::TReadSessionEvent::TDataReceivedEvent& event) mutable
            {
                if (closing) {
                    return;
                }

                lastDataReceiveTime = TInstant::Now().GetValue();
                ui64& maxReceivedOffset = maxReceivedOffsets[event.GetPartitionStream()->GetPartitionId()];
                for (const auto& msg : event.GetMessages()) {
                    ++messagesReceived;
                    maxReceivedOffset = Max(maxReceivedOffset, msg.GetOffset());
                    Cout << msg.GetData() << Endl;
                }
            },
            opts.CommitAfterProcessing);

    auto commitAckHandler = settings.EventHandlers_.CommitAcknowledgementHandler_;
    settings.EventHandlers_.CommitAcknowledgementHandler(
        [&readSession, &messagesReceived, &maxReceivedOffsets, &committedOffsets, maxMessagesCount = opts.MessagesCount, &commitAckHandler, &closing, &commitAfterProcessing = opts.CommitAfterProcessing](NYdb::NPersQueue::TReadSessionEvent::TCommitAcknowledgementEvent& event) mutable
        {
            committedOffsets[event.GetPartitionStream()->GetPartitionId()] = event.GetCommittedOffset();
            if (messagesReceived >= maxMessagesCount) {
                bool allCommitted = true;
                if (commitAfterProcessing) {
                    for (const auto [partitionId, maxReceivedOffset] : maxReceivedOffsets) {
                        if (maxReceivedOffset >= committedOffsets[partitionId]) {
                            allCommitted = false;
                            break;
                        }
                    }
                }
                bool prev = false;
                if (allCommitted && !closing && closing.compare_exchange_strong(prev, true)) {
                    closing = true;
                    Cerr << "Closing session. Got " << messagesReceived << " messages" << Endl;
                    readSession->Close(TDuration::Seconds(5));
                    Cerr << "Session closed" << Endl;
                }
            }
            if (commitAckHandler) {
                commitAckHandler(event);
            }
        });

    readSession = persqueueClient.CreateReadSession(settings);

    // Timeout tracking thread
    NThreading::TPromise<void> barrier = NThreading::NewPromise<void>();
    std::thread timeoutTrackingThread(
        [barrierFuture = barrier.GetFuture(), &closing, &lastDataReceiveTime, readSession, timeout = opts.Timeout]() mutable {
            if (timeout == TDuration::Zero()) {
                return;
            }

            bool futureIsSignalled = false;
            while (!closing && !futureIsSignalled) {
                const TInstant lastDataTime = TInstant::FromValue(lastDataReceiveTime);
                const TInstant now = TInstant::Now();
                const TInstant deadline = lastDataTime + timeout;
                bool prev = false;
                if (now > deadline && closing.compare_exchange_strong(prev, true)) {
                    Cerr << "Closing session. No data during " << timeout << Endl;
                    readSession->Close(TDuration::Seconds(5));
                    Cerr << "Session closed" << Endl;
                    break;
                }
                futureIsSignalled = barrierFuture.Wait(deadline);
            }
    });

    auto maybeEvent = readSession->GetEvent(true);

    barrier.SetValue();

    Cerr << "Stopping driver..." << Endl;
    driver.Stop();

    Cerr << "Driver stopped. Exit" << Endl;

    timeoutTrackingThread.join();

    if (!maybeEvent) {
        Cerr << "No finish event" << Endl;
        return 1;
    }
    if (!std::holds_alternative<NYdb::NPersQueue::TSessionClosedEvent>(*maybeEvent)) {
        Cerr << "TSessionClosedEvent expected, but got event: " << DebugString(*maybeEvent) << Endl;
        return 2;
    }

    const NYdb::NPersQueue::TSessionClosedEvent& closedEvent = std::get<NYdb::NPersQueue::TSessionClosedEvent>(*maybeEvent);
    Cerr << "Session closed event: " << closedEvent.DebugString() << Endl;
    if (closedEvent.IsSuccess() || (closing && closedEvent.GetStatus() == NYdb::EStatus::ABORTED)) {
        return 0;
    } else {
        return 3;
    }
}
