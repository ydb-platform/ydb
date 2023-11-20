#include "simple_proto.h"

#include <library/cpp/messagebus/test/perftest/messages.pb.h>

#include <library/cpp/messagebus/text_utils.h>
#include <library/cpp/messagebus/thread_extra.h>
#include <library/cpp/messagebus/ybus.h>
#include <library/cpp/messagebus/oldmodule/module.h>
#include <library/cpp/messagebus/protobuf/ybusbuf.h>
#include <library/cpp/messagebus/www/www.h>

#include <library/cpp/deprecated/threadable/threadable.h>
#include <library/cpp/execprofile/profile.h>
#include <library/cpp/getopt/opt.h>
#include <library/cpp/lwtrace/start.h>
#include <library/cpp/sighandler/async_signals_handler.h>
#include <library/cpp/threading/future/legacy_future.h>

#include <util/generic/ptr.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/generic/yexception.h>
#include <util/random/random.h>
#include <util/stream/file.h>
#include <util/stream/output.h>
#include <util/stream/str.h>
#include <util/string/split.h>
#include <util/system/event.h>
#include <util/system/sysstat.h>
#include <util/system/thread.h>
#include <util/thread/lfqueue.h>

#include <signal.h>
#include <stdlib.h>

using namespace NBus;

///////////////////////////////////////////////////////
/// \brief Configuration parameters of the test

const int DEFAULT_PORT = 55666;

struct TPerftestConfig {
    TString Nodes; ///< node1:port1,node2:port2
    int ClientCount;
    int MessageSize; ///< size of message to send
    int Delay;       ///< server delay (milliseconds)
    float Failure;   ///< simulated failure rate
    int ServerPort;
    int Run;
    bool ServerUseModules;
    bool ExecuteOnMessageInWorkerPool;
    bool ExecuteOnReplyInWorkerPool;
    bool UseCompression;
    bool Profile;
    unsigned WwwPort;

    TPerftestConfig();

    void Print() {
        fprintf(stderr, "ClientCount=%d\n", ClientCount);
        fprintf(stderr, "ServerPort=%d\n", ServerPort);
        fprintf(stderr, "Delay=%d usecs\n", Delay);
        fprintf(stderr, "MessageSize=%d bytes\n", MessageSize);
        fprintf(stderr, "Failure=%.3f%%\n", Failure * 100.0);
        fprintf(stderr, "Runtime=%d seconds\n", Run);
        fprintf(stderr, "ServerUseModules=%s\n", ServerUseModules ? "true" : "false");
        fprintf(stderr, "ExecuteOnMessageInWorkerPool=%s\n", ExecuteOnMessageInWorkerPool ? "true" : "false");
        fprintf(stderr, "ExecuteOnReplyInWorkerPool=%s\n", ExecuteOnReplyInWorkerPool ? "true" : "false");
        fprintf(stderr, "UseCompression=%s\n", UseCompression ? "true" : "false");
        fprintf(stderr, "Profile=%s\n", Profile ? "true" : "false");
        fprintf(stderr, "WwwPort=%u\n", WwwPort);
    }
};

extern TPerftestConfig* TheConfig;
extern bool TheExit;

TVector<TNetAddr> ServerAddresses;

struct TConfig {
    TBusQueueConfig ServerQueueConfig;
    TBusQueueConfig ClientQueueConfig;
    TBusServerSessionConfig ServerSessionConfig;
    TBusClientSessionConfig ClientSessionConfig;
    bool SimpleProtocol;

private:
    void ConfigureDefaults(TBusQueueConfig& config) {
        config.NumWorkers = 4;
    }

    void ConfigureDefaults(TBusSessionConfig& config) {
        config.MaxInFlight = 10000;
        config.SendTimeout = TDuration::Seconds(20).MilliSeconds();
        config.TotalTimeout = TDuration::Seconds(60).MilliSeconds();
    }

public:
    TConfig()
        : SimpleProtocol(false)
    {
        ConfigureDefaults(ServerQueueConfig);
        ConfigureDefaults(ClientQueueConfig);
        ConfigureDefaults(ServerSessionConfig);
        ConfigureDefaults(ClientSessionConfig);
    }

    void Print() {
        // TODO: do not print server if only client and vice verse
        Cerr << "server queue config:\n";
        Cerr << IndentText(ServerQueueConfig.PrintToString());
        Cerr << "server session config:" << Endl;
        Cerr << IndentText(ServerSessionConfig.PrintToString());
        Cerr << "client queue config:\n";
        Cerr << IndentText(ClientQueueConfig.PrintToString());
        Cerr << "client session config:" << Endl;
        Cerr << IndentText(ClientSessionConfig.PrintToString());
        Cerr << "simple protocol: " << SimpleProtocol << "\n";
    }
};

TConfig Config;

////////////////////////////////////////////////////////////////
/// \brief Fast message

using TPerftestRequest = TBusBufferMessage<TPerftestRequestRecord, 77>;
using TPerftestResponse = TBusBufferMessage<TPerftestResponseRecord, 79>;

static size_t RequestSize() {
    return RandomNumber<size_t>(TheConfig->MessageSize * 2 + 1);
}

TAutoPtr<TBusMessage> NewRequest() {
    if (Config.SimpleProtocol) {
        TAutoPtr<TSimpleMessage> r(new TSimpleMessage);
        r->SetCompressed(TheConfig->UseCompression);
        r->Payload = 10;
        return r.Release();
    } else {
        TAutoPtr<TPerftestRequest> r(new TPerftestRequest);
        r->SetCompressed(TheConfig->UseCompression);
        // TODO: use random content for better compression test
        r->Record.SetData(TString(RequestSize(), '?'));
        return r.Release();
    }
}

void CheckRequest(TPerftestRequest* request) {
    const TString& data = request->Record.GetData();
    for (size_t i = 0; i != data.size(); ++i) {
        Y_ABORT_UNLESS(data.at(i) == '?', "must be question mark");
    }
}

TAutoPtr<TPerftestResponse> NewResponse(TPerftestRequest* request) {
    TAutoPtr<TPerftestResponse> r(new TPerftestResponse);
    r->SetCompressed(TheConfig->UseCompression);
    r->Record.SetData(TString(request->Record.GetData().size(), '.'));
    return r;
}

void CheckResponse(TPerftestResponse* response) {
    const TString& data = response->Record.GetData();
    for (size_t i = 0; i != data.size(); ++i) {
        Y_ABORT_UNLESS(data.at(i) == '.', "must be dot");
    }
}

////////////////////////////////////////////////////////////////////
/// \brief Fast protocol that common between client and server
class TPerftestProtocol: public TBusBufferProtocol {
public:
    TPerftestProtocol()
        : TBusBufferProtocol("TPerftestProtocol", TheConfig->ServerPort)
    {
        RegisterType(new TPerftestRequest);
        RegisterType(new TPerftestResponse);
    }
};

class TPerftestServer;
class TPerftestUsingModule;
class TPerftestClient;

struct TTestStats {
    TInstant Start;

    TAtomic Messages;
    TAtomic Errors;
    TAtomic Replies;

    void IncMessage() {
        AtomicIncrement(Messages);
    }
    void IncReplies() {
        AtomicDecrement(Messages);
        AtomicIncrement(Replies);
    }
    int NumMessage() {
        return AtomicGet(Messages);
    }
    void IncErrors() {
        AtomicDecrement(Messages);
        AtomicIncrement(Errors);
    }
    int NumErrors() {
        return AtomicGet(Errors);
    }
    int NumReplies() {
        return AtomicGet(Replies);
    }

    double GetThroughput() {
        return NumReplies() * 1000000.0 / (TInstant::Now() - Start).MicroSeconds();
    }

public:
    TTestStats()
        : Start(TInstant::Now())
        , Messages(0)
        , Errors(0)
        , Replies(0)
    {
    }

    void PeriodicallyPrint();
};

TTestStats Stats;

////////////////////////////////////////////////////////////////////
/// \brief Fast of the client session
class TPerftestClient : IBusClientHandler {
public:
    TBusClientSessionPtr Session;
    THolder<TBusProtocol> Proto;
    TBusMessageQueuePtr Bus;
    TVector<TBusClientConnectionPtr> Connections;

public:
    /// constructor creates instances of protocol and session
    TPerftestClient() {
        /// create or get instance of message queue, need one per application
        Bus = CreateMessageQueue(Config.ClientQueueConfig, "client");

        if (Config.SimpleProtocol) {
            Proto.Reset(new TSimpleProtocol);
        } else {
            Proto.Reset(new TPerftestProtocol);
        }

        Session = TBusClientSession::Create(Proto.Get(), this, Config.ClientSessionConfig, Bus);

        for (unsigned i = 0; i < ServerAddresses.size(); ++i) {
            Connections.push_back(Session->GetConnection(ServerAddresses[i]));
        }
    }

    /// dispatch of requests is done here
    void Work() {
        SetCurrentThreadName("FastClient::Work");

        while (!TheExit) {
            TBusClientConnection* connection;
            if (Connections.size() == 1) {
                connection = Connections.front().Get();
            } else {
                connection = Connections.at(RandomNumber<size_t>()).Get();
            }

            TBusMessage* message = NewRequest().Release();
            int ret = connection->SendMessage(message, true);

            if (ret == MESSAGE_OK) {
                Stats.IncMessage();
            } else if (ret == MESSAGE_BUSY) {
                //delete message;
                //Sleep(TDuration::MilliSeconds(1));
                //continue;
                Y_ABORT("unreachable");
            } else if (ret == MESSAGE_SHUTDOWN) {
                delete message;
            } else {
                delete message;
                Stats.IncErrors();
            }
        }
    }

    void Stop() {
        Session->Shutdown();
    }

    /// actual work is being done here
    void OnReply(TAutoPtr<TBusMessage> mess, TAutoPtr<TBusMessage> reply) override {
        Y_UNUSED(mess);

        if (Config.SimpleProtocol) {
            VerifyDynamicCast<TSimpleMessage*>(reply.Get());
        } else {
            TPerftestResponse* typed = VerifyDynamicCast<TPerftestResponse*>(reply.Get());

            CheckResponse(typed);
        }

        Stats.IncReplies();
    }

    /// message that could not be delivered
    void OnError(TAutoPtr<TBusMessage> mess, EMessageStatus status) override {
        Y_UNUSED(mess);
        Y_UNUSED(status);

        if (TheExit) {
            return;
        }

        Stats.IncErrors();

        // Y_ASSERT(TheConfig->Failure > 0.0);
    }
};

class TPerftestServerCommon {
public:
    THolder<TBusProtocol> Proto;

    TBusMessageQueuePtr Bus;

    TBusServerSessionPtr Session;

protected:
    TPerftestServerCommon(const char* name)
        : Session()
    {
        if (Config.SimpleProtocol) {
            Proto.Reset(new TSimpleProtocol);
        } else {
            Proto.Reset(new TPerftestProtocol);
        }

        /// create or get instance of single message queue, need one for application
        Bus = CreateMessageQueue(Config.ServerQueueConfig, name);
    }

public:
    void Stop() {
        Session->Shutdown();
    }
};

struct TAsyncRequest {
    TBusMessage* Request;
    TInstant ReceivedTime;
};

/////////////////////////////////////////////////////////////////////
/// \brief Fast of the server session
class TPerftestServer: public TPerftestServerCommon, public IBusServerHandler {
public:
    TLockFreeQueue<TAsyncRequest> AsyncRequests;

public:
    TPerftestServer()
        : TPerftestServerCommon("server")
    {
        /// register destination session
        Session = TBusServerSession::Create(Proto.Get(), this, Config.ServerSessionConfig, Bus);
        Y_ASSERT(Session && "probably somebody is listening on the same port");
    }

    /// when message comes, send reply
    void OnMessage(TOnMessageContext& mess) override {
        if (Config.SimpleProtocol) {
            TSimpleMessage* typed = VerifyDynamicCast<TSimpleMessage*>(mess.GetMessage());
            TAutoPtr<TSimpleMessage> response(new TSimpleMessage);
            response->Payload = typed->Payload;
            mess.SendReplyMove(response);
            return;
        }

        TPerftestRequest* typed = VerifyDynamicCast<TPerftestRequest*>(mess.GetMessage());

        CheckRequest(typed);

        /// forget replies for few messages, see what happends
        if (TheConfig->Failure > RandomNumber<double>()) {
            return;
        }

        /// sleep requested time
        if (TheConfig->Delay) {
            TAsyncRequest request;
            request.Request = mess.ReleaseMessage();
            request.ReceivedTime = TInstant::Now();
            AsyncRequests.Enqueue(request);
            return;
        }

        TAutoPtr<TPerftestResponse> reply(NewResponse(typed));
        /// sent empty reply for each message
        mess.SendReplyMove(reply);
        // TODO: count results
    }

    void Stop() {
        TPerftestServerCommon::Stop();
    }
};

class TPerftestUsingModule: public TPerftestServerCommon, public TBusModule {
public:
    TPerftestUsingModule()
        : TPerftestServerCommon("server")
        , TBusModule("fast")
    {
        Y_ABORT_UNLESS(CreatePrivateSessions(Bus.Get()), "failed to initialize dupdetect module");
        Y_ABORT_UNLESS(StartInput(), "failed to start input");
    }

    ~TPerftestUsingModule() override {
        Shutdown();
    }

private:
    TJobHandler Start(TBusJob* job, TBusMessage* mess) override {
        TPerftestRequest* typed = VerifyDynamicCast<TPerftestRequest*>(mess);
        CheckRequest(typed);

        /// sleep requested time
        if (TheConfig->Delay) {
            usleep(TheConfig->Delay);
        }

        /// forget replies for few messages, see what happends
        if (TheConfig->Failure > RandomNumber<double>()) {
            return nullptr;
        }

        job->SendReply(NewResponse(typed).Release());
        return nullptr;
    }

    TBusServerSessionPtr CreateExtSession(TBusMessageQueue& queue) override {
        return Session = CreateDefaultDestination(queue, Proto.Get(), Config.ServerSessionConfig);
    }
};

// ./perftest/perftest -s 11456 -c localhost:11456 -r 60 -n 4 -i 5000

using namespace std;
using namespace NBus;

static TNetworkAddress ParseNetworkAddress(const char* string) {
    TString Name;
    int Port;

    const char* port = strchr(string, ':');

    if (port != nullptr) {
        Name.append(string, port - string);
        Port = atoi(port + 1);
    } else {
        Name.append(string);
        Port = TheConfig->ServerPort != 0 ? TheConfig->ServerPort : DEFAULT_PORT;
    }

    return TNetworkAddress(Name, Port);
}

TVector<TNetAddr> ParseNodes(const TString nodes) {
    TVector<TNetAddr> r;

    TVector<TString> hosts;

    size_t numh = Split(nodes.data(), ",", hosts);

    for (int i = 0; i < int(numh); i++) {
        const TNetworkAddress& networkAddress = ParseNetworkAddress(hosts[i].data());
        Y_ABORT_UNLESS(networkAddress.Begin() != networkAddress.End(), "no addresses");
        r.push_back(TNetAddr(networkAddress, &*networkAddress.Begin()));
    }

    return r;
}

TPerftestConfig::TPerftestConfig() {
    TBusSessionConfig defaultConfig;

    ServerPort = DEFAULT_PORT;
    Delay = 0; // artificial delay inside server OnMessage()
    MessageSize = 200;
    Failure = 0.00;
    Run = 60; // in seconds
    Nodes = "localhost";
    ServerUseModules = false;
    ExecuteOnMessageInWorkerPool = defaultConfig.ExecuteOnMessageInWorkerPool;
    ExecuteOnReplyInWorkerPool = defaultConfig.ExecuteOnReplyInWorkerPool;
    UseCompression = false;
    Profile = false;
    WwwPort = 0;
}

TPerftestConfig* TheConfig = new TPerftestConfig();
bool TheExit = false;

TSystemEvent StopEvent;

TSimpleSharedPtr<TPerftestServer> Server;
TSimpleSharedPtr<TPerftestUsingModule> ServerUsingModule;

TVector<TSimpleSharedPtr<TPerftestClient>> Clients;
TMutex ClientsLock;

void stopsignal(int /*sig*/) {
    fprintf(stderr, "\n-------------------- exiting ------------------\n");
    TheExit = true;
    StopEvent.Signal();
}

// -s <num> - start server on port <num>
// -c <node:port,node:port> - start client

void TTestStats::PeriodicallyPrint() {
    SetCurrentThreadName("print-stats");

    for (;;) {
        StopEvent.WaitT(TDuration::Seconds(1));
        if (TheExit)
            break;

        TVector<TSimpleSharedPtr<TPerftestClient>> clients;
        {
            TGuard<TMutex> guard(ClientsLock);
            clients = Clients;
        }

        fprintf(stderr, "replies=%d errors=%d throughput=%.3f mess/sec\n",
                NumReplies(), NumErrors(), GetThroughput());
        if (!!Server) {
            fprintf(stderr, "server: q: %u %s\n",
                    (unsigned)Server->Bus->GetExecutor()->GetWorkQueueSize(),
                    Server->Session->GetStatusSingleLine().data());
        }
        if (!!ServerUsingModule) {
            fprintf(stderr, "server: q: %u %s\n",
                    (unsigned)ServerUsingModule->Bus->GetExecutor()->GetWorkQueueSize(),
                    ServerUsingModule->Session->GetStatusSingleLine().data());
        }
        for (const auto& client : clients) {
            fprintf(stderr, "client: q: %u %s\n",
                    (unsigned)client->Bus->GetExecutor()->GetWorkQueueSize(),
                    client->Session->GetStatusSingleLine().data());
        }

        TStringStream stats;

        bool first = true;
        if (!!Server) {
            if (!first) {
                stats << "\n";
            }
            first = false;
            stats << "server:\n";
            stats << IndentText(Server->Bus->GetStatus());
        }
        if (!!ServerUsingModule) {
            if (!first) {
                stats << "\n";
            }
            first = false;
            stats << "server using modules:\n";
            stats << IndentText(ServerUsingModule->Bus->GetStatus());
        }
        for (const auto& client : clients) {
            if (!first) {
                stats << "\n";
            }
            first = false;
            stats << "client:\n";
            stats << IndentText(client->Bus->GetStatus());
        }

        TUnbufferedFileOutput("stats").Write(stats.Str());
    }
}

int main(int argc, char* argv[]) {
    NLWTrace::StartLwtraceFromEnv();

    /* unix foo */
    setvbuf(stdout, nullptr, _IONBF, 0);
    setvbuf(stderr, nullptr, _IONBF, 0);
    Umask(0);
    SetAsyncSignalHandler(SIGINT, stopsignal);
    SetAsyncSignalHandler(SIGTERM, stopsignal);
#ifndef _win_
    SetAsyncSignalHandler(SIGUSR1, stopsignal);
#endif
    signal(SIGPIPE, SIG_IGN);

    NLastGetopt::TOpts opts = NLastGetopt::TOpts::Default();
    opts.AddLongOption('s', "server-port", "server port").RequiredArgument("port").StoreResult(&TheConfig->ServerPort);
    opts.AddCharOption('m', "average message size").RequiredArgument("size").StoreResult(&TheConfig->MessageSize);
    opts.AddLongOption('c', "server-host", "server hosts").RequiredArgument("host[,host]...").StoreResult(&TheConfig->Nodes);
    opts.AddCharOption('f', "failure rate (rational number between 0 and 1)").RequiredArgument("rate").StoreResult(&TheConfig->Failure);
    opts.AddCharOption('w', "delay before reply").RequiredArgument("microseconds").StoreResult(&TheConfig->Delay);
    opts.AddCharOption('r', "run duration").RequiredArgument("seconds").StoreResult(&TheConfig->Run);
    opts.AddLongOption("client-count", "amount of clients").RequiredArgument("count").StoreResult(&TheConfig->ClientCount).DefaultValue("1");
    opts.AddLongOption("server-use-modules").StoreResult(&TheConfig->ServerUseModules, true);
    opts.AddLongOption("on-message-in-pool", "execute OnMessage callback in worker pool")
        .RequiredArgument("BOOL")
        .StoreResult(&TheConfig->ExecuteOnMessageInWorkerPool);
    opts.AddLongOption("on-reply-in-pool", "execute OnReply callback in worker pool")
        .RequiredArgument("BOOL")
        .StoreResult(&TheConfig->ExecuteOnReplyInWorkerPool);
    opts.AddLongOption("compression", "use compression").RequiredArgument("BOOL").StoreResult(&TheConfig->UseCompression);
    opts.AddLongOption("simple-proto").SetFlag(&Config.SimpleProtocol);
    opts.AddLongOption("profile").SetFlag(&TheConfig->Profile);
    opts.AddLongOption("www-port").RequiredArgument("PORT").StoreResult(&TheConfig->WwwPort);
    opts.AddHelpOption();

    Config.ServerQueueConfig.ConfigureLastGetopt(opts, "server-");
    Config.ServerSessionConfig.ConfigureLastGetopt(opts, "server-");
    Config.ClientQueueConfig.ConfigureLastGetopt(opts, "client-");
    Config.ClientSessionConfig.ConfigureLastGetopt(opts, "client-");

    opts.SetFreeArgsMax(0);

    NLastGetopt::TOptsParseResult parseResult(&opts, argc, argv);

    TheConfig->Print();
    Config.Print();

    if (TheConfig->Profile) {
        BeginProfiling();
    }

    TIntrusivePtr<TBusWww> www(new TBusWww);

    ServerAddresses = ParseNodes(TheConfig->Nodes);

    if (TheConfig->ServerPort) {
        if (TheConfig->ServerUseModules) {
            ServerUsingModule = new TPerftestUsingModule();
            www->RegisterModule(ServerUsingModule.Get());
        } else {
            Server = new TPerftestServer();
            www->RegisterServerSession(Server->Session);
        }
    }

    TVector<TSimpleSharedPtr<NThreading::TLegacyFuture<void, false>>> futures;

    if (ServerAddresses.size() > 0 && TheConfig->ClientCount > 0) {
        for (int i = 0; i < TheConfig->ClientCount; ++i) {
            TGuard<TMutex> guard(ClientsLock);
            Clients.push_back(new TPerftestClient);
            futures.push_back(new NThreading::TLegacyFuture<void, false>(std::bind(&TPerftestClient::Work, Clients.back())));
            www->RegisterClientSession(Clients.back()->Session);
        }
    }

    futures.push_back(new NThreading::TLegacyFuture<void, false>(std::bind(&TTestStats::PeriodicallyPrint, std::ref(Stats))));

    THolder<TBusWwwHttpServer> wwwServer;
    if (TheConfig->WwwPort != 0) {
        wwwServer.Reset(new TBusWwwHttpServer(www, TheConfig->WwwPort));
    }

    /* sit here until signal terminate our process */
    StopEvent.WaitT(TDuration::Seconds(TheConfig->Run));
    TheExit = true;
    StopEvent.Signal();

    if (!!Server) {
        Cerr << "Stopping server\n";
        Server->Stop();
    }
    if (!!ServerUsingModule) {
        Cerr << "Stopping server (using modules)\n";
        ServerUsingModule->Stop();
    }

    TVector<TSimpleSharedPtr<TPerftestClient>> clients;
    {
        TGuard<TMutex> guard(ClientsLock);
        clients = Clients;
    }

    if (!clients.empty()) {
        Cerr << "Stopping clients\n";

        for (auto& client : clients) {
            client->Stop();
        }
    }

    wwwServer.Destroy();

    for (const auto& future : futures) {
        future->Get();
    }

    if (TheConfig->Profile) {
        EndProfiling();
    }

    Cerr << "***SUCCESS***\n";
    return 0;
}
