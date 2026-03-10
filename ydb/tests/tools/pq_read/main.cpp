#include <ydb/public/sdk/cpp/src/client/persqueue_public/persqueue.h>

#include <library/cpp/getopt/last_getopt.h>
#include <library/cpp/threading/future/future.h>

#include <util/stream/output.h>
#include <util/system/env.h>
#include <util/system/mutex.h>

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
        .SetLog(std::unique_ptr<TLogBackend>(CreateLogBackend("cerr").Release()));
    NYdb::TDriver driver(driverConfig);

    NYdb::NPersQueue::TPersQueueClient persqueueClient(driver);

    NYdb::NPersQueue::TReadSessionSettings settings;
    settings
        .DisableClusterDiscovery(opts.DisableClusterDiscovery)
        .ConsumerName(opts.ConsumerName)
        .AppendTopics(std::string{opts.TopicPath});

    struct TState {
        TMutex Lock;
        std::shared_ptr<NYdb::NPersQueue::IReadSession> ReadSession;
        size_t MessagesReceived = 0;
        THashMap<ui64, ui64> MaxReceivedOffsets; // partition id -> max received offset
        THashMap<ui64, ui64> CommittedOffsets; // partition id -> confirmed offset
        std::atomic<bool> Closing = false;
        std::atomic<TInstant::TValue> LastDataReceiveTime;
    };
    auto state = std::make_shared<TState>();
    state->LastDataReceiveTime = TInstant::Now().GetValue();

    settings
        .EventHandlers_.SimpleDataHandlers(
            [state](NYdb::NPersQueue::TReadSessionEvent::TDataReceivedEvent& event) mutable
            {
                if (state->Closing) {
                    return;
                }

                state->LastDataReceiveTime = TInstant::Now().GetValue();
                TGuard lock(state->Lock); // required for map access and Cout serializing
                ui64& maxReceivedOffset = state->MaxReceivedOffsets[event.GetPartitionStream()->GetPartitionId()];
                for (const auto& msg : event.GetMessages()) {
                    ++state->MessagesReceived;
                    maxReceivedOffset = Max(maxReceivedOffset, msg.GetOffset());
                    Cout << msg.GetData() << Endl;
                }
            },
            opts.CommitAfterProcessing);

    auto commitAckHandler = settings.EventHandlers_.CommitAcknowledgementHandler_;
    settings.EventHandlers_.CommitAcknowledgementHandler(
        [state, maxMessagesCount = opts.MessagesCount, commitAckHandler, commitAfterProcessing = opts.CommitAfterProcessing](NYdb::NPersQueue::TReadSessionEvent::TCommitAcknowledgementEvent& event) mutable
        {
            size_t messagesReceived;
            {
                TGuard lock(state->Lock);
                state->CommittedOffsets[event.GetPartitionStream()->GetPartitionId()] = event.GetCommittedOffset();
                messagesReceived = state->MessagesReceived;
            }
            if (messagesReceived >= maxMessagesCount) {
                bool allCommitted = true;
                if (commitAfterProcessing) {
                    TGuard lock(state->Lock);
                    for (const auto [partitionId, maxReceivedOffset] : state->MaxReceivedOffsets) {
                        if (maxReceivedOffset >= state->CommittedOffsets[partitionId]) {
                            allCommitted = false;
                            break;
                        }
                    }
                }
                bool prev = false;
                if (allCommitted && state->Closing.compare_exchange_strong(prev, true)) {
                    Cerr << "Closing session. Got " << state->MessagesReceived << " messages" << Endl;
                    state->ReadSession->Close(TDuration::Seconds(5));
                    Cerr << "Session closed" << Endl;
                }
            }
            if (commitAckHandler) {
                commitAckHandler(event);
            }
        });

    state->ReadSession = persqueueClient.CreateReadSession(settings);

    // Timeout tracking thread
    NThreading::TPromise<void> barrier = NThreading::NewPromise<void>();
    std::thread timeoutTrackingThread(
        [barrierFuture = barrier.GetFuture(), state, timeout = opts.Timeout]() mutable {
            if (timeout == TDuration::Zero()) {
                return;
            }

            bool futureIsSignalled = false;
            while (!state->Closing && !futureIsSignalled) {
                const TInstant lastDataTime = TInstant::FromValue(state->LastDataReceiveTime);
                const TInstant now = TInstant::Now();
                const TInstant deadline = lastDataTime + timeout;
                bool prev = false;
                if (now > deadline && state->Closing.compare_exchange_strong(prev, true)) {
                    Cerr << "Closing session. No data during " << timeout << Endl;
                    state->ReadSession->Close(TDuration::Seconds(5));
                    Cerr << "Session closed" << Endl;
                    break;
                }
                futureIsSignalled = barrierFuture.Wait(deadline);
            }
    });

    auto maybeEvent = state->ReadSession->GetEvent(true);

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
    if (closedEvent.IsSuccess() || (state->Closing && closedEvent.GetStatus() == NYdb::EStatus::ABORTED)) {
        return 0;
    } else {
        return 3;
    }
}
