#include <ydb/library/actors/core/manual_test/event_serialize_bench/bench.pb.h>

#include <ydb/library/actors/core/actorsystem.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/event_flat.h>
#include <ydb/library/actors/core/event_pb.h>
#include <ydb/library/actors/core/events.h>
#include <ydb/library/actors/core/executor_pool_basic.h>
#include <ydb/library/actors/core/log.h>
#include <ydb/library/actors/core/scheduler_basic.h>
#include <ydb/library/actors/dnsresolver/dnsresolver.h>
#include <ydb/library/actors/interconnect/interconnect.h>
#include <ydb/library/actors/interconnect/interconnect_common.h>
#include <ydb/library/actors/interconnect/interconnect_tcp_proxy.h>
#include <ydb/library/actors/interconnect/interconnect_tcp_server.h>
#include <ydb/library/actors/interconnect/poller/poller_actor.h>
#include <library/cpp/monlib/dynamic_counters/counters.h>

#include <util/datetime/base.h>
#include <util/generic/string.h>
#include <util/generic/xrange.h>
#include <util/string/builder.h>
#include <util/string/cast.h>
#include <util/string/printf.h>
#include <util/system/sigset.h>

#include <chrono>
#include <future>

using namespace NActors;
using namespace NActors::NDnsResolver;

namespace {

enum EBenchEvents {
    EvBenchPbMessage = EventSpaceBegin(TEvents::ES_PRIVATE),
    EvBenchFlatMessage,
    EvBenchAck,
    EvBenchFlush,
};

constexpr ui32 Node1 = 1;
constexpr ui32 Node2 = 2;

TActorId MakeBenchReceiverServiceId(ui32 nodeId) {
    return TActorId(nodeId, TStringBuf("bnchrecv"));
}

struct TBenchConfig {
    TString Scenario = "all";
    TString Format = "all";
    TDuration Duration = TDuration::Seconds(5);
    ui64 Window = 10000;
    ui32 AckBatch = 256;
    ui32 Threads = 2;
    ui16 PortBase = 19000;
};

struct TBenchResult {
    TString Scenario;
    TString Format;
    ui64 SentMessages = 0;
    ui64 AckedMessages = 0;
    ui64 Checksum = 0;
    ui32 SerializedSizeStart = 0;
    ui32 SerializedSizeEnd = 0;
    double Seconds = 0;
    bool Connected = true;
};

struct TEvBenchPbMessage
    : TEventPB<TEvBenchPbMessage, NActorsBench::TFiveU64, EvBenchPbMessage>
{
    using TBase = TEventPB<TEvBenchPbMessage, NActorsBench::TFiveU64, EvBenchPbMessage>;

    TEvBenchPbMessage() = default;

    explicit TEvBenchPbMessage(ui64 base) {
        Record.SetValue1(base + 1);
        Record.SetValue2(base + 2);
        Record.SetValue3(base + 3);
        Record.SetValue4(base + 4);
        Record.SetValue5(base + 5);
    }
};

struct TEvBenchFlatMessage : TEventFlat<TEvBenchFlatMessage> {
    using TBase = TEventFlat<TEvBenchFlatMessage>;

    static constexpr ui32 EventType = EvBenchFlatMessage;

    using TValue1Tag = TBase::FixedField<ui64, 0>;
    using TValue2Tag = TBase::FixedField<ui64, 1>;
    using TValue3Tag = TBase::FixedField<ui64, 2>;
    using TValue4Tag = TBase::FixedField<ui64, 3>;
    using TValue5Tag = TBase::FixedField<ui64, 4>;

    using TSchemeV1 = TBase::Scheme<TValue1Tag, TValue2Tag, TValue3Tag, TValue4Tag, TValue5Tag>;
    using TScheme = TBase::Versions<TSchemeV1>;

    friend class TEventFlat<TEvBenchFlatMessage>;

    auto Value1() { return TBase::template Field<TValue1Tag>(); }
    auto Value1() const { return TBase::template Field<TValue1Tag>(); }
    auto Value2() { return TBase::template Field<TValue2Tag>(); }
    auto Value2() const { return TBase::template Field<TValue2Tag>(); }
    auto Value3() { return TBase::template Field<TValue3Tag>(); }
    auto Value3() const { return TBase::template Field<TValue3Tag>(); }
    auto Value4() { return TBase::template Field<TValue4Tag>(); }
    auto Value4() const { return TBase::template Field<TValue4Tag>(); }
    auto Value5() { return TBase::template Field<TValue5Tag>(); }
    auto Value5() const { return TBase::template Field<TValue5Tag>(); }

    static TEvBenchFlatMessage* Make(ui64 base) {
        THolder<TEvBenchFlatMessage> holder(TBase::MakeEvent());
        auto frontend = holder->GetFrontend<TSchemeV1>();
        frontend.template Field<TValue1Tag>() = base + 1;
        frontend.template Field<TValue2Tag>() = base + 2;
        frontend.template Field<TValue3Tag>() = base + 3;
        frontend.template Field<TValue4Tag>() = base + 4;
        frontend.template Field<TValue5Tag>() = base + 5;
        return holder.Release();
    }
};

struct TEvBenchAck : TEventFlat<TEvBenchAck> {
    using TBase = TEventFlat<TEvBenchAck>;

    static constexpr ui32 EventType = EvBenchAck;

    using TCountTag = TBase::FixedField<ui32, 0>;
    using TChecksumTag = TBase::FixedField<ui64, 1>;

    using TSchemeV1 = TBase::Scheme<TCountTag, TChecksumTag>;
    using TScheme = TBase::Versions<TSchemeV1>;

    friend class TEventFlat<TEvBenchAck>;

    auto Count() { return TBase::template Field<TCountTag>(); }
    auto Count() const { return TBase::template Field<TCountTag>(); }
    auto Checksum() { return TBase::template Field<TChecksumTag>(); }
    auto Checksum() const { return TBase::template Field<TChecksumTag>(); }

    static TEvBenchAck* Make(ui32 count, ui64 checksum) {
        THolder<TEvBenchAck> holder(TBase::MakeEvent());
        holder->Count() = count;
        holder->Checksum() = checksum;
        return holder.Release();
    }
};

struct TEvBenchFlush : TEventFlat<TEvBenchFlush> {
    using TBase = TEventFlat<TEvBenchFlush>;

    static constexpr ui32 EventType = EvBenchFlush;

    using TTag = TBase::FixedField<ui32, 0>;

    using TSchemeV1 = TBase::Scheme<TTag>;
    using TScheme = TBase::Versions<TSchemeV1>;

    friend class TEventFlat<TEvBenchFlush>;

    auto Value() { return TBase::template Field<TTag>(); }
    auto Value() const { return TBase::template Field<TTag>(); }

    static TEvBenchFlush* Make() {
        THolder<TEvBenchFlush> holder(TBase::MakeEvent());
        holder->Value() = 0;
        return holder.Release();
    }
};

template <class TEvent>
struct TMessageTraits;

template <>
struct TMessageTraits<TEvBenchPbMessage> {
    using TEvent = TEvBenchPbMessage;

    static constexpr TStringBuf Name() {
        return "protobuf";
    }

    static TEvent* Make(ui64 base) {
        return new TEvent(base);
    }

    static ui64 Read(const TEvent& ev) {
        const auto& r = ev.Record;
        return r.GetValue1() + r.GetValue2() + r.GetValue3() + r.GetValue4() + r.GetValue5();
    }

    static ui32 SerializedSize(ui64 base) {
        TEvent ev(base);
        return ev.CalculateSerializedSize();
    }
};

template <>
struct TMessageTraits<TEvBenchFlatMessage> {
    using TEvent = TEvBenchFlatMessage;

    static constexpr TStringBuf Name() {
        return "flat";
    }

    static TEvent* Make(ui64 base) {
        return TEvent::Make(base);
    }

    static ui64 Read(const TEvent& ev) {
        auto frontend = ev.template GetFrontend<typename TEvent::TSchemeV1>();
        return static_cast<ui64>(frontend.template Field<typename TEvent::TValue1Tag>())
            + static_cast<ui64>(frontend.template Field<typename TEvent::TValue2Tag>())
            + static_cast<ui64>(frontend.template Field<typename TEvent::TValue3Tag>())
            + static_cast<ui64>(frontend.template Field<typename TEvent::TValue4Tag>())
            + static_cast<ui64>(frontend.template Field<typename TEvent::TValue5Tag>());
    }

    static ui32 SerializedSize(ui64 base) {
        THolder<TEvent> ev(TEvent::Make(base));
        return ev->CalculateSerializedSize();
    }
};

template <class TTraits>
class TReceiverActor : public TActorBootstrapped<TReceiverActor<TTraits>> {
    using TMessage = typename TTraits::TEvent;

public:
    explicit TReceiverActor(ui32 ackBatch)
        : AckBatch(ackBatch)
    {}

    void Bootstrap() {
        this->Become(&TReceiverActor::StateWork);
    }

private:
    void Handle(typename TMessage::TPtr& ev) {
        PendingCount += 1;
        PendingChecksum += TTraits::Read(*ev->Get());

        if (PendingCount >= AckBatch) {
            SendAck(ev->Sender);
        }
    }

    void Handle(TEvBenchFlush::TPtr& ev) {
        if (PendingCount) {
            SendAck(ev->Sender);
        }
    }

    void SendAck(const TActorId& recipient) {
        this->Send(recipient, TEvBenchAck::Make(PendingCount, PendingChecksum));
        PendingCount = 0;
        PendingChecksum = 0;
    }

    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TMessage, Handle);
            hFunc(TEvBenchFlush, Handle);
        }
    }

private:
    const ui32 AckBatch;
    ui32 PendingCount = 0;
    ui64 PendingChecksum = 0;
};

template <class TTraits>
class TSenderActor : public TActorBootstrapped<TSenderActor<TTraits>> {
    using TMessage = typename TTraits::TEvent;

public:
    TSenderActor(
            TActorId target,
            TDuration duration,
            ui64 window,
            TString scenarioName,
            std::promise<TBenchResult> promise)
        : Target(target)
        , Duration(duration)
        , Window(window)
        , ScenarioName(std::move(scenarioName))
        , Promise(std::move(promise))
    {}

    void Bootstrap() {
        this->Become(&TSenderActor::StateWork);
        if (Target.NodeId() != this->SelfId().NodeId()) {
            this->Send(TActivationContext::InterconnectProxy(Target.NodeId()), new TEvInterconnect::TEvConnectNode, IEventHandle::FlagTrackDelivery);
        } else {
            StartBenchmark();
        }
    }

private:
    void HandleConnected(TEvInterconnect::TEvNodeConnected::TPtr& ev) {
        if (ev->Get()->NodeId == Target.NodeId() && !Started) {
            StartBenchmark();
        }
    }

    void HandleDisconnected(TEvInterconnect::TEvNodeDisconnected::TPtr& ev) {
        if (ev->Get()->NodeId == Target.NodeId()) {
            Finish(false);
        }
    }

    void HandleUndelivered(TEvents::TEvUndelivered::TPtr&) {
        Finish(false);
    }

    void HandleAck(TEvBenchAck::TPtr& ev) {
        const ui32 count = static_cast<ui32>(ev->Get()->Count());
        const ui64 checksum = static_cast<ui64>(ev->Get()->Checksum());

        Y_ABORT_UNLESS(InFlight >= count);
        InFlight -= count;
        AckedMessages += count;
        Checksum += checksum;

        if (!Stopping) {
            SendBurst(count);
        } else if (InFlight == 0) {
            Finish(true);
        }
    }

    void HandleWakeup() {
        Stopping = true;
        this->Send(Target, TEvBenchFlush::Make());
        if (InFlight == 0) {
            Finish(true);
        }
    }

    void StartBenchmark() {
        Started = true;
        Start = TInstant::Now();
        this->Schedule(Duration, new TEvents::TEvWakeup());
        SendBurst(Window);
    }

    void SendBurst(ui64 count) {
        for (ui64 i = 0; i < count; ++i) {
            this->Send(Target, TTraits::Make(NextBaseValue++));
            ++InFlight;
            ++SentMessages;
        }
    }

    void Finish(bool connected) {
        if (Finished) {
            return;
        }
        Finished = true;

        TBenchResult result;
        result.Scenario = ScenarioName;
        result.Format = TString(TTraits::Name());
        result.SentMessages = SentMessages;
        result.AckedMessages = AckedMessages;
        result.Checksum = Checksum;
        result.SerializedSizeStart = TTraits::SerializedSize(1);
        result.SerializedSizeEnd = TTraits::SerializedSize(NextBaseValue > 1 ? NextBaseValue - 1 : 1);
        result.Connected = connected;
        result.Seconds = Started ? (TInstant::Now() - Start).SecondsFloat() : 0.0;

        Promise.set_value(std::move(result));
        this->PassAway();
    }

    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvBenchAck, HandleAck);
            hFunc(TEvInterconnect::TEvNodeConnected, HandleConnected);
            hFunc(TEvInterconnect::TEvNodeDisconnected, HandleDisconnected);
            hFunc(TEvents::TEvUndelivered, HandleUndelivered);
            cFunc(TEvents::TSystem::Wakeup, HandleWakeup);
        }
    }

private:
    const TActorId Target;
    const TDuration Duration;
    const ui64 Window;
    const TString ScenarioName;
    std::promise<TBenchResult> Promise;

    bool Started = false;
    bool Stopping = false;
    bool Finished = false;
    TInstant Start;
    ui64 NextBaseValue = 1;
    ui64 InFlight = 0;
    ui64 SentMessages = 0;
    ui64 AckedMessages = 0;
    ui64 Checksum = 0;
};

THolder<TActorSystemSetup> BuildLocalActorSystemSetup(ui32 nodeId, ui32 threads) {
    auto setup = MakeHolder<TActorSystemSetup>();

    setup->NodeId = nodeId;
    setup->ExecutorsCount = 1;
    setup->Executors.Reset(new TAutoPtr<IExecutorPool>[1]);
    setup->Executors[0] = new TBasicExecutorPool(0, threads, 50);
    setup->Scheduler = new TBasicSchedulerThread(TSchedulerConfig(512, 0));

    return setup;
}

THolder<TActorSystemSetup> BuildInterconnectActorSystemSetup(
        ui32 nodeId,
        ui32 threads,
        ui16 portBase,
        NMonitoring::TDynamicCounters& counters)
{
    auto setup = MakeHolder<TActorSystemSetup>();

    setup->NodeId = nodeId;
    setup->ExecutorsCount = 1;
    setup->Executors.Reset(new TAutoPtr<IExecutorPool>[1]);
    setup->Executors[0] = new TBasicExecutorPool(0, threads, 50);
    setup->Scheduler = new TBasicSchedulerThread(TSchedulerConfig(512, 0));

    setup->LocalServices.emplace_back(MakePollerActorId(), TActorSetupCmd(CreatePollerActor(), TMailboxType::ReadAsFilled, 0));
    setup->LocalServices.emplace_back(MakeDnsResolverActorId(), TActorSetupCmd(CreateOnDemandDnsResolver(), TMailboxType::ReadAsFilled, 0));

    TIntrusivePtr<TTableNameserverSetup> nameserverTable = new TTableNameserverSetup();
    nameserverTable->StaticNodeTable[Node1] = std::make_pair("127.0.0.1", portBase + Node1);
    nameserverTable->StaticNodeTable[Node2] = std::make_pair("127.0.0.1", portBase + Node2);
    setup->LocalServices.emplace_back(GetNameserviceActorId(), TActorSetupCmd(CreateNameserverTable(nameserverTable), TMailboxType::ReadAsFilled, 0));

    TIntrusivePtr<TInterconnectProxyCommon> icCommon = new TInterconnectProxyCommon();
    icCommon->NameserviceId = GetNameserviceActorId();
    icCommon->MonCounters = counters.GetSubgroup("counters", "interconnect");
    icCommon->TechnicalSelfHostName = "127.0.0.1";

    setup->Interconnect.ProxyActors.resize(Node2 + 1);
    for (ui32 peerNodeId : {Node1, Node2}) {
        if (peerNodeId == nodeId) {
            IActor* listener = new TInterconnectListenerTCP("127.0.0.1", portBase + nodeId, icCommon);
            setup->LocalServices.emplace_back(
                MakeInterconnectListenerActorId(false),
                TActorSetupCmd(listener, TMailboxType::ReadAsFilled, 0));
        } else {
            setup->Interconnect.ProxyActors[peerNodeId] = TActorSetupCmd(
                new TInterconnectProxyTCP(peerNodeId, icCommon),
                TMailboxType::ReadAsFilled,
                0);
        }
    }

    return setup;
}

template <class TTraits>
TBenchResult RunLocalBenchmark(const TBenchConfig& config) {
    THolder<TActorSystemSetup> setup = BuildLocalActorSystemSetup(Node1, config.Threads);
    TActorSystem actorSystem(setup);
    actorSystem.Start();

    const TActorId receiverId = actorSystem.Register(new TReceiverActor<TTraits>(config.AckBatch));
    actorSystem.RegisterLocalService(MakeBenchReceiverServiceId(Node1), receiverId);

    std::promise<TBenchResult> promise;
    std::future<TBenchResult> future = promise.get_future();
    actorSystem.Register(new TSenderActor<TTraits>(
        MakeBenchReceiverServiceId(Node1),
        config.Duration,
        config.Window,
        "local",
        std::move(promise)));

    const auto status = future.wait_for(std::chrono::seconds(config.Duration.Seconds() + 30));
    Y_ABORT_UNLESS(status == std::future_status::ready);
    TBenchResult result = future.get();

    actorSystem.Stop();
    actorSystem.Cleanup();
    return result;
}

template <class TTraits>
TBenchResult RunInterconnectBenchmark(const TBenchConfig& config) {
    TIntrusivePtr<NMonitoring::TDynamicCounters> counters1 = new NMonitoring::TDynamicCounters();
    TIntrusivePtr<NMonitoring::TDynamicCounters> counters2 = new NMonitoring::TDynamicCounters();

    THolder<TActorSystemSetup> setup2 = BuildInterconnectActorSystemSetup(Node2, config.Threads, config.PortBase, *counters2);
    THolder<TActorSystemSetup> setup1 = BuildInterconnectActorSystemSetup(Node1, config.Threads, config.PortBase, *counters1);
    TActorSystem node2(setup2);
    TActorSystem node1(setup1);

    node2.Start();
    node1.Start();

    const TActorId receiverId = node2.Register(new TReceiverActor<TTraits>(config.AckBatch));
    node2.RegisterLocalService(MakeBenchReceiverServiceId(Node2), receiverId);

    std::promise<TBenchResult> promise;
    std::future<TBenchResult> future = promise.get_future();
    node1.Register(new TSenderActor<TTraits>(
        MakeBenchReceiverServiceId(Node2),
        config.Duration,
        config.Window,
        "interconnect",
        std::move(promise)));

    const auto status = future.wait_for(std::chrono::seconds(config.Duration.Seconds() + 30));
    Y_ABORT_UNLESS(status == std::future_status::ready);
    TBenchResult result = future.get();

    node1.Stop();
    node2.Stop();
    node1.Cleanup();
    node2.Cleanup();
    return result;
}

void PrintUsage(const char* argv0) {
    Cout
        << "Usage: " << argv0 << " [--scenario local|interconnect|all] [--format protobuf|flat|all]" << Endl
        << "       [--duration-sec N] [--window N] [--ack-batch N] [--threads N] [--port-base N]" << Endl;
}

bool ParseArgs(int argc, char** argv, TBenchConfig& config) {
    for (int i = 1; i < argc; ++i) {
        TStringBuf arg(argv[i]);
        const size_t pos = arg.find('=');
        const TStringBuf key = pos == TStringBuf::npos ? arg : arg.Head(pos);
        const TStringBuf value = pos == TStringBuf::npos ? TStringBuf() : arg.Skip(pos + 1);

        if (key == "--help") {
            return false;
        } else if (key == "--scenario" && value) {
            config.Scenario = TString(value);
        } else if (key == "--format" && value) {
            config.Format = TString(value);
        } else if (key == "--duration-sec" && value) {
            config.Duration = TDuration::Seconds(FromString<ui32>(value));
        } else if (key == "--window" && value) {
            config.Window = FromString<ui64>(value);
        } else if (key == "--ack-batch" && value) {
            config.AckBatch = FromString<ui32>(value);
        } else if (key == "--threads" && value) {
            config.Threads = FromString<ui32>(value);
        } else if (key == "--port-base" && value) {
            config.PortBase = FromString<ui16>(value);
        } else {
            Cerr << "Unknown argument: " << arg << Endl;
            return false;
        }
    }

    Y_ABORT_UNLESS(config.Window > 0);
    Y_ABORT_UNLESS(config.AckBatch > 0);
    Y_ABORT_UNLESS(config.Threads > 0);
    Y_ABORT_UNLESS(config.Scenario == "all" || config.Scenario == "local" || config.Scenario == "interconnect");
    Y_ABORT_UNLESS(config.Format == "all" || config.Format == "protobuf" || config.Format == "flat");
    return true;
}

template <class TTraits>
void MaybeRunScenario(const TBenchConfig& config, TStringBuf scenarioName) {
    if (config.Format != "all" && config.Format != TTraits::Name()) {
        return;
    }

    TBenchResult result;
    if (scenarioName == "local") {
        result = RunLocalBenchmark<TTraits>(config);
    } else if (scenarioName == "interconnect") {
        result = RunInterconnectBenchmark<TTraits>(config);
    } else {
        Y_ABORT("Unexpected scenario");
    }

    const double messageRate = result.Seconds > 0 ? result.AckedMessages / result.Seconds : 0.0;
    const double mibRate = result.Seconds > 0
        ? (result.AckedMessages * result.SerializedSizeEnd) / result.Seconds / (1024.0 * 1024.0)
        : 0.0;

    Cout
        << "scenario=" << result.Scenario
        << " format=" << result.Format
        << " connected=" << (result.Connected ? "true" : "false")
        << " sample_size_start=" << result.SerializedSizeStart
        << " sample_size_end=" << result.SerializedSizeEnd
        << " sent=" << result.SentMessages
        << " acked=" << result.AckedMessages
        << " seconds=" << Sprintf("%.3f", result.Seconds)
        << " msg_per_sec=" << Sprintf("%.2f", messageRate)
        << " mib_per_sec=" << Sprintf("%.2f", mibRate)
        << " checksum=" << result.Checksum
        << Endl;
}

void RunBenchmarks(const TBenchConfig& config) {
    Cout
        << "duration=" << config.Duration
        << " window=" << config.Window
        << " ack_batch=" << config.AckBatch
        << " threads=" << config.Threads
        << " port_base=" << config.PortBase
        << Endl;

    const bool runLocal = config.Scenario == "all" || config.Scenario == "local";
    const bool runInterconnect = config.Scenario == "all" || config.Scenario == "interconnect";

    if (runLocal) {
        MaybeRunScenario<TMessageTraits<TEvBenchPbMessage>>(config, "local");
        MaybeRunScenario<TMessageTraits<TEvBenchFlatMessage>>(config, "local");
    }

    if (runInterconnect) {
        MaybeRunScenario<TMessageTraits<TEvBenchPbMessage>>(config, "interconnect");
        MaybeRunScenario<TMessageTraits<TEvBenchFlatMessage>>(config, "interconnect");
    }
}

} // namespace

int main(int argc, char** argv) {
#ifdef _unix_
    signal(SIGPIPE, SIG_IGN);
#endif

    TBenchConfig config;
    if (!ParseArgs(argc, argv, config)) {
        PrintUsage(argv[0]);
        return 1;
    }

    RunBenchmarks(config);
    return 0;
}
