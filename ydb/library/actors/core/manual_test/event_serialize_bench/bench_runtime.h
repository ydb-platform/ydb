#pragma once

#include "bench_traits.h"
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
        result.Test = TString(TTraits::TestName());
        result.EffectiveWindow = Window;
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

ui64 CollectActorSystemCpuUs(const TActorSystem& actorSystem) {
    ui64 totalCpuUs = 0;
    const auto& statsSubSystem = GetActorSystemStats(actorSystem);
    const ui32 poolCount = actorSystem.GetBasicExecutorPools().size();
    for (ui32 poolId = 0; poolId < poolCount; ++poolId) {
        TExecutorPoolStats poolStats;
        TVector<TExecutorThreadStats> stats;
        TVector<TExecutorThreadStats> sharedStats;
        statsSubSystem.GetPoolStats(poolId, poolStats, stats, sharedStats);
        for (const auto& item : stats) {
            totalCpuUs += item.CpuUs;
        }
        for (const auto& item : sharedStats) {
            totalCpuUs += item.CpuUs;
        }
    }
    return totalCpuUs;
}

double CalcActorCpuUtilPct(ui64 actorCpuUs, double seconds, ui32 totalThreads) {
    if (seconds <= 0.0 || totalThreads == 0) {
        return 0.0;
    }
    const double capacityUs = seconds * 1000000.0 * totalThreads;
    return capacityUs > 0.0 ? actorCpuUs * 100.0 / capacityUs : 0.0;
}

template <class TTraits>
TBenchResult RunLocalBenchmark(const TBenchConfig& config) {
    const ui64 window = TTraits::EffectiveWindow(config.Window);
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
        window,
        "local",
        std::move(promise)));

    const auto status = future.wait_for(std::chrono::seconds(config.Duration.Seconds() + 30));
    Y_ABORT_UNLESS(status == std::future_status::ready);
    TBenchResult result = future.get();
    result.ActorCpuUs = CollectActorSystemCpuUs(actorSystem);
    result.ActorCpuUtilPct = CalcActorCpuUtilPct(result.ActorCpuUs, result.Seconds, config.Threads);

    actorSystem.Stop();
    actorSystem.Cleanup();
    return result;
}

template <class TTraits>
TBenchResult RunInterconnectBenchmark(const TBenchConfig& config) {
    const ui64 window = TTraits::EffectiveWindow(config.Window);
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
        window,
        "interconnect",
        std::move(promise)));

    const auto status = future.wait_for(std::chrono::seconds(config.Duration.Seconds() + 30));
    Y_ABORT_UNLESS(status == std::future_status::ready);
    TBenchResult result = future.get();
    result.ActorCpuUs = CollectActorSystemCpuUs(node1) + CollectActorSystemCpuUs(node2);
    result.ActorCpuUtilPct = CalcActorCpuUtilPct(result.ActorCpuUs, result.Seconds, config.Threads * 2);

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
        << " test=" << result.Test
        << " effective_window=" << result.EffectiveWindow
        << " connected=" << (result.Connected ? "true" : "false")
        << " sample_size_start=" << result.SerializedSizeStart
        << " sample_size_end=" << result.SerializedSizeEnd
        << " sent=" << result.SentMessages
        << " acked=" << result.AckedMessages
        << " seconds=" << Sprintf("%.3f", result.Seconds)
        << " msg_per_sec=" << Sprintf("%.2f", messageRate)
        << " mib_per_sec=" << Sprintf("%.2f", mibRate)
        << " actor_cpu_us=" << result.ActorCpuUs
        << " actor_cpu_util_pct=" << Sprintf("%.2f", result.ActorCpuUtilPct)
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
        MaybeRunScenario<TMessageTraits<TEvBenchPbThirtyMessage>>(config, "local");
        MaybeRunScenario<TMessageTraits<TEvBenchFlatThirtyMessage>>(config, "local");
        MaybeRunScenario<TArrayPbTraits<30, 5>>(config, "local");
        MaybeRunScenario<TArrayFlatTraits<30, 5>>(config, "local");
        MaybeRunScenario<TArrayPbTraits<1000, 1000>>(config, "local");
        MaybeRunScenario<TArrayFlatTraits<1000, 1000>>(config, "local");
        MaybeRunScenario<TMessageTraits<TEvBenchPbStructArrayMessage>>(config, "local");
        MaybeRunScenario<TMessageTraits<TEvBenchFlatStructArrayMessage>>(config, "local");
        MaybeRunScenario<TPbVPutLikeTraits<Payload256>>(config, "local");
        MaybeRunScenario<TFlatVPutLikeTraits<Payload256>>(config, "local");
        MaybeRunScenario<TMessageTraits<TEvBenchPbVPutLikeMessage>>(config, "local");
        MaybeRunScenario<TMessageTraits<TEvBenchFlatVPutLikeMessage>>(config, "local");
        MaybeRunScenario<TPayloadPbTraits<Payload4KB>>(config, "local");
        MaybeRunScenario<TPayloadFlatTraits<Payload4KB>>(config, "local");
    }

    if (runInterconnect) {
        MaybeRunScenario<TMessageTraits<TEvBenchPbMessage>>(config, "interconnect");
        MaybeRunScenario<TMessageTraits<TEvBenchFlatMessage>>(config, "interconnect");
        MaybeRunScenario<TMessageTraits<TEvBenchPbThirtyMessage>>(config, "interconnect");
        MaybeRunScenario<TMessageTraits<TEvBenchFlatThirtyMessage>>(config, "interconnect");
        MaybeRunScenario<TArrayPbTraits<30, 5>>(config, "interconnect");
        MaybeRunScenario<TArrayFlatTraits<30, 5>>(config, "interconnect");
        MaybeRunScenario<TArrayPbTraits<1000, 1000>>(config, "interconnect");
        MaybeRunScenario<TArrayFlatTraits<1000, 1000>>(config, "interconnect");
        MaybeRunScenario<TMessageTraits<TEvBenchPbStructArrayMessage>>(config, "interconnect");
        MaybeRunScenario<TMessageTraits<TEvBenchFlatStructArrayMessage>>(config, "interconnect");
        MaybeRunScenario<TPbVPutLikeTraits<Payload256>>(config, "interconnect");
        MaybeRunScenario<TFlatVPutLikeTraits<Payload256>>(config, "interconnect");
        MaybeRunScenario<TMessageTraits<TEvBenchPbVPutLikeMessage>>(config, "interconnect");
        MaybeRunScenario<TMessageTraits<TEvBenchFlatVPutLikeMessage>>(config, "interconnect");
        MaybeRunScenario<TPayloadPbTraits<Payload4KB>>(config, "interconnect");
        MaybeRunScenario<TPayloadFlatTraits<Payload4KB>>(config, "interconnect");
    }
}

} // namespace
