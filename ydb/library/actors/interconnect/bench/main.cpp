// Interconnect end-to-end benchmark: drives sustained request/response traffic between two in-process
// nodes over a real loopback TCP connection and reports throughput, round-trip latency, CPU cost and --
// for session v2 -- the io_uring engine's internal breakdown.
//
// Both endpoints live in this process and the traffic goes through the local kernel TCP stack, so the
// absolute MB/s figure can be limited by something entirely unrelated to the interconnect code. The CPU
// cost per byte/message and the v2 engine phase breakdown are the numbers that actually move when the
// data plane changes.

#include <ydb/library/actors/interconnect/load.h>
#include <ydb/library/actors/interconnect/uring_context.h>
#include <ydb/library/actors/interconnect/ut/lib/ic_test_cluster.h>

#include <ydb/library/actors/core/log.h>
#include <ydb/library/actors/util/affinity.h>
#include <ydb/library/actors/util/cpu_topology.h>
#include <ydb/library/actors/util/cpumask.h>

#include <library/cpp/getopt/small/last_getopt.h>
#include <library/cpp/monlib/dynamic_counters/counters.h>

#include <util/datetime/base.h>
#include <util/generic/algorithm.h>
#include <util/generic/hash.h>
#include <util/generic/hash_set.h>
#include <util/generic/vector.h>
#include <util/stream/output.h>
#include <util/string/printf.h>
#include <util/system/rusage.h>

#include <condition_variable>
#include <inttypes.h>
#include <mutex>
#include <optional>

using namespace NActors;

namespace {

    constexpr ui32 ClientNode = 1;
    constexpr ui32 ServerNode = 2;
    constexpr ui32 NumNodes = 2;

    struct TConfig {
        bool UseV2 = true;
        TDuration Duration = TDuration::Seconds(30);
        TDuration Warmup = TDuration::Seconds(5);
        ui32 PayloadMin = 4096;
        ui32 PayloadMax = 4096;
        ui32 InFly = 16;
        ui32 NumChannels = 1;
        ui32 NumLoadActors = 4;
        ui32 NumThreads = 4;
        ui32 Inflight = TNode::DefaultInflight();
        ui32 TcpSocketBufferSize = 0; // 0 -> harness default
        bool ProtobufPayload = true;
        bool EnableRdma = false;
        bool Verbose = false;

        // CPU pinning. Applied to the whole process before any actor/uring threads are started so they
        // inherit the mask and first-touch allocations stay on the chosen NUMA node. Empty = no pinning.
        TString CpuMask;              // explicit list, e.g. "0-5,12-17"
        std::optional<ui32> NumaNode; // pick CPUs of this NUMA node (ignored when CpuMask is set)
        bool Pin = false;             // shorthand: pin to NUMA 0 (or the only node) preferring physical cores
        bool AllowSmt = false;        // when deriving a mask from --numa-node/--pin, keep hyperthread siblings

        // Filled in by ApplyCpuPinning once the effective mask is known, for the report header.
        TString EffectiveCpuMask;
        ui32 EffectiveCpuCount = 0;

        // v2-only knobs. One shard by default: a two-node run has a single connection per node, and every
        // extra shard adds an idle worker thread (plus, under SQPOLL, a spinning kernel thread) whose CPU
        // is charged to this process and would swamp the per-byte CPU figures.
        ui32 UringShards = 1;
        ui32 RingsPerShard = 1;
        bool SqPoll = true;
        bool Checksum = false;
        bool Preserialize = false;
    };

    TConfig ParseOptions(int argc, char** argv) {
        TConfig cfg;
        bool useV1 = false;

        NLastGetopt::TOpts opts = NLastGetopt::TOpts::Default();
        opts.SetTitle("Interconnect throughput/latency/CPU benchmark over a loopback two-node cluster.");
        opts.AddHelpOption('h');

        opts.AddLongOption("v1", "use interconnect session v1 instead of v2")
            .NoArgument().SetFlag(&useV1);
        opts.AddLongOption("duration", "total run time, including warm-up")
            .RequiredArgument("TIME").DefaultValue("30s")
            .Handler1T<TString>([&](const TString& s) { cfg.Duration = TDuration::Parse(s); });
        opts.AddLongOption("warmup", "initial period excluded from the reported statistics")
            .RequiredArgument("TIME").DefaultValue("5s")
            .Handler1T<TString>([&](const TString& s) { cfg.Warmup = TDuration::Parse(s); });
        opts.AddLongOption("payload-min", "minimum message payload size, bytes")
            .RequiredArgument("NUM").StoreResult(&cfg.PayloadMin).DefaultValue(cfg.PayloadMin);
        opts.AddLongOption("payload-max", "maximum message payload size, bytes")
            .RequiredArgument("NUM").StoreResult(&cfg.PayloadMax).DefaultValue(cfg.PayloadMax);
        opts.AddLongOption("payload", "shorthand setting both payload-min and payload-max")
            .RequiredArgument("NUM")
            .Handler1T<ui32>([&](ui32 v) { cfg.PayloadMin = cfg.PayloadMax = v; });
        opts.AddLongOption("in-fly", "in-flight messages per load actor")
            .RequiredArgument("NUM").StoreResult(&cfg.InFly).DefaultValue(cfg.InFly);
        opts.AddLongOption("channels", "number of distinct interconnect channels to spread load over")
            .RequiredArgument("NUM").StoreResult(&cfg.NumChannels).DefaultValue(cfg.NumChannels);
        opts.AddLongOption("load-actors", "number of concurrent load actors on the client node")
            .RequiredArgument("NUM").StoreResult(&cfg.NumLoadActors).DefaultValue(cfg.NumLoadActors);
        opts.AddLongOption("ic-threads", "executor pool threads per node")
            .RequiredArgument("NUM").StoreResult(&cfg.NumThreads).DefaultValue(cfg.NumThreads);
        opts.AddLongOption("inflight", "TotalInflightAmountOfData, bytes")
            .RequiredArgument("NUM").StoreResult(&cfg.Inflight).DefaultValue(cfg.Inflight);
        opts.AddLongOption("tcp-buffer", "TCPSocketBufferSize, bytes (0 keeps the harness default)")
            .RequiredArgument("NUM").StoreResult(&cfg.TcpSocketBufferSize).DefaultValue(cfg.TcpSocketBufferSize);
        opts.AddLongOption("inline-payload", "carry the payload inside the protobuf instead of a separate rope")
            .NoArgument().Handler0([&] { cfg.ProtobufPayload = false; });
        opts.AddLongOption("rdma", "leave RDMA enabled (off by default: it is probed on every run and is "
                "irrelevant to the v2 data plane)")
            .NoArgument().SetFlag(&cfg.EnableRdma);
        opts.AddLongOption("verbose", "let the actor system log to stderr")
            .NoArgument().SetFlag(&cfg.Verbose);

        opts.AddLongOption("uring-shards", "v2: io_uring engine worker threads")
            .RequiredArgument("NUM").StoreResult(&cfg.UringShards).DefaultValue(cfg.UringShards);
        opts.AddLongOption("rings-per-shard", "v2: io_uring rings per worker thread")
            .RequiredArgument("NUM").StoreResult(&cfg.RingsPerShard).DefaultValue(cfg.RingsPerShard);
        opts.AddLongOption("no-sqpoll", "v2: submit from the worker thread instead of an SQPOLL kernel thread")
            .NoArgument().Handler0([&] { cfg.SqPoll = false; });
        opts.AddLongOption("checksum", "v2: checksum every event (XXH3)")
            .NoArgument().SetFlag(&cfg.Checksum);
        opts.AddLongOption("preserialize", "v2: serialize events on the session mailbox before the engine sees them")
            .NoArgument().SetFlag(&cfg.Preserialize);

        opts.AddLongOption("pin", "pin the process to NUMA node 0 (or the only node), preferring physical cores. "
                "Do this before comparing runs on a multi-socket machine.")
            .NoArgument().SetFlag(&cfg.Pin);
        opts.AddLongOption("numa-node", "pin the process to the CPUs of this NUMA node (preferring physical cores "
                "unless --smt is set). Overrides --pin's node selection.")
            .RequiredArgument("N")
            .Handler1T<ui32>([&](ui32 n) { cfg.NumaNode = n; });
        opts.AddLongOption("cpu-mask", "pin the process to an explicit CPU list (e.g. 0-5,12-17). Overrides "
                "--numa-node/--pin.")
            .RequiredArgument("LIST").StoreResult(&cfg.CpuMask);
        opts.AddLongOption("smt", "when deriving a mask from --pin/--numa-node, keep hyperthread siblings too "
                "(default: physical cores only)")
            .NoArgument().SetFlag(&cfg.AllowSmt);

        opts.SetFreeArgsNum(0);
        const NLastGetopt::TOptsParseResult parsed(&opts, argc, argv);
        Y_UNUSED(parsed);

        cfg.UseV2 = !useV1;
        cfg.PayloadMax = Max(cfg.PayloadMax, cfg.PayloadMin);
        cfg.NumChannels = Max<ui32>(1, cfg.NumChannels);
        cfg.NumLoadActors = Max<ui32>(1, cfg.NumLoadActors);
        if (cfg.Warmup >= cfg.Duration) {
            ythrow yexception() << "--warmup must be less than --duration";
        }
        if (!cfg.CpuMask.empty() && cfg.NumaNode) {
            ythrow yexception() << "--cpu-mask and --numa-node are mutually exclusive";
        }
        return cfg;
    }

    TString ToCpuListString(const TCpuMask& mask) {
        TStringStream out;
        bool firstRange = true;
        for (TCpuId cpu = 0; cpu < mask.Size();) {
            if (!mask.IsSet(cpu)) {
                ++cpu;
                continue;
            }
            const TCpuId begin = cpu;
            TCpuId end = cpu;
            while (end + 1 < mask.Size() && mask.IsSet(end + 1)) {
                ++end;
            }
            if (!firstRange) {
                out << ',';
            }
            firstRange = false;
            if (begin == end) {
                out << begin;
            } else {
                out << begin << '-' << end;
            }
            cpu = end + 1;
        }
        return out.Str();
    }

    // Keep one logical CPU per physical core: the lowest-numbered sibling of each ThreadSiblings group.
    // Sharing a core with an SMT sibling is a common source of run-to-run jitter on busy boxes.
    TCpuMask PreferPhysicalCores(const TCpuMask& mask, const TCpuTopology& topology) {
        TCpuMask result;
        THashSet<ui32> seenCores;
        for (TCpuId cpu = 0; cpu < mask.Size(); ++cpu) {
            if (!mask.IsSet(cpu)) {
                continue;
            }
            const TLogicalCpuInfo* info = topology.FindCpu(cpu);
            if (!info) {
                result.Set(cpu);
                continue;
            }
            // Pick the lowest sibling that is also in the requested mask; skip the rest of the group.
            TCpuId representative = Max<TCpuId>();
            for (TCpuId sibling = 0; sibling < info->ThreadSiblings.Size(); ++sibling) {
                if (info->ThreadSiblings.IsSet(sibling) && mask.IsSet(sibling)) {
                    representative = Min(representative, sibling);
                }
            }
            if (representative == Max<TCpuId>()) {
                representative = cpu;
            }
            if (info->CoreId != UnknownCpuTopologyId) {
                if (!seenCores.insert(info->CoreId).second) {
                    continue;
                }
            } else if (cpu != representative) {
                continue;
            }
            result.Set(representative);
        }
        return result;
    }

    TCpuMask ResolveCpuMask(const TConfig& cfg, const TCpuTopology& topology) {
        if (!cfg.CpuMask.empty()) {
            return TCpuMask(cfg.CpuMask);
        }

        std::optional<ui32> numaNode = cfg.NumaNode;
        if (!numaNode && cfg.Pin) {
            if (topology.NumaNodes.empty()) {
                ythrow yexception() << "CPU topology has no NUMA nodes; pass --cpu-mask explicitly";
            }
            numaNode = topology.NumaNodes.front().Id;
        }
        if (!numaNode) {
            return {};
        }

        const TCpuTopologyGroup* group = nullptr;
        for (const auto& node : topology.NumaNodes) {
            if (node.Id == *numaNode) {
                group = &node;
                break;
            }
        }
        if (!group) {
            TStringStream available;
            for (size_t i = 0; i < topology.NumaNodes.size(); ++i) {
                if (i) {
                    available << ',';
                }
                available << topology.NumaNodes[i].Id;
            }
            ythrow yexception() << "NUMA node " << *numaNode << " not found; available: " << available.Str();
        }

        return cfg.AllowSmt ? group->Cpus : PreferPhysicalCores(group->Cpus, topology);
    }

    // Must run before any actor-system / uring threads are created: sched_setaffinity on the process is
    // inherited by subsequent pthread_create calls, and first-touch mallocs land on the chosen node.
    // Kernel SQPOLL threads (IORING_SETUP_SQPOLL without IORING_SETUP_SQ_AFF) are NOT bound by this and
    // can still wander -- prefer --no-sqpoll when comparing runs.
    void ApplyCpuPinning(TConfig& cfg) {
        if (cfg.CpuMask.empty() && !cfg.NumaNode && !cfg.Pin) {
            return;
        }

        auto topology = ParseCpuTopology();
        if (!topology) {
            ythrow yexception() << "failed to parse CPU topology: " << topology.error();
        }

        const TCpuMask mask = ResolveCpuMask(cfg, *topology);
        if (mask.IsEmpty()) {
            ythrow yexception() << "resolved CPU mask is empty";
        }

        TAffinity affinity(mask);
        affinity.Set(/*pid=*/0);

        cfg.EffectiveCpuMask = ToCpuListString(mask);
        cfg.EffectiveCpuCount = mask.CpuCount();

        // Rough headroom check: 2 nodes x (ic threads + 1 IO) + 2 uring shards + main. Under-provisioning
        // does not break the run, but it forces time-sharing that swamps any data-plane signal.
        const ui32 threadsNeeded = 2 * (cfg.NumThreads + 1) + (cfg.UseV2 ? 2 * cfg.UringShards : 0) + 1;
        if (cfg.EffectiveCpuCount < threadsNeeded) {
            Cerr << "warning: pinned to " << cfg.EffectiveCpuCount << " CPU(s) but the default layout wants "
                 << "about " << threadsNeeded << "; consider raising --cpu-mask or lowering --ic-threads / "
                 << "--uring-shards" << Endl;
        }
        if (cfg.UseV2 && cfg.SqPoll) {
            Cerr << "warning: SQPOLL kernel threads ignore process affinity unless the engine pins them "
                 << "via IORING_SETUP_SQ_AFF; use --no-sqpoll for reproducible runs" << Endl;
        }
    }

    // Collects one TLoadActorStats per load actor; the callbacks fire on actor-system threads as each
    // load actor dies at the end of its run.
    class TStatsSink {
    public:
        void Add(const NInterconnect::TLoadActorStats& stats) {
            std::lock_guard<std::mutex> guard(Mutex);
            Stats.push_back(stats);
            Cond.notify_all();
        }

        bool WaitFor(size_t count, TDuration timeout) {
            std::unique_lock<std::mutex> guard(Mutex);
            return Cond.wait_for(guard, std::chrono::microseconds(timeout.MicroSeconds()),
                [&] { return Stats.size() >= count; });
        }

        std::vector<NInterconnect::TLoadActorStats> Get() {
            std::lock_guard<std::mutex> guard(Mutex);
            return Stats;
        }

    private:
        std::mutex Mutex;
        std::condition_variable Cond;
        std::vector<NInterconnect::TLoadActorStats> Stats;
    };

    ////////////////////////////////////////////////////////////////////////////////////////////////////
    // v2 io_uring engine counters
    ////////////////////////////////////////////////////////////////////////////////////////////////////

    // Plain counters, summed over every node. All shards publish into a single "shard" subgroup, so one
    // lookup per node already covers the whole engine.
    const char* const UringCounterNames[] = {
        "EventsSent",
        "EventsReceivedActorSystem",
        "EventsReceivedCallback",
        "BytesSent",
        "BytesReceived",
        "BytesCopied",
        "BytesAliased",
        "SQEAllocated",
        "SubmitCount",
        "CQEProcessed",
        "EventWakeups",
        "ReadUnavail",
        "WriteUnavail",
        "OutOfOrderCameIn",
    };

    // Per-phase worker-thread time, in nanoseconds. Together these account for all of a shard worker's
    // wall time, so the shares are directly comparable.
    const char* const UringPhaseNames[] = {
        "SerializeEventTotalTime",
        "ApplyBytesWrittenTotalTime",
        "ApplyBytesReadTotalTime",
        "ReceiveCallbackTotalTime",
        "SubmitWaitTotalTime",
        "CompleteWaitTotalTime",
        "OtherTotalTime",
    };

    struct TUringSnapshot {
        THashMap<TString, ui64> Counters;
        TVector<std::pair<double, ui64>> EventToWire; // {bucket upper bound in ns, count}

        ui64 Get(TStringBuf name) const {
            const auto it = Counters.find(TString(name));
            return it != Counters.end() ? it->second : 0;
        }
    };

    NMonitoring::TDynamicCounterPtr FindShardCounters(const NMonitoring::TDynamicCounterPtr& root, ui32 nodeId) {
        auto node = root->FindSubgroup("nodeId", ToString(nodeId));
        if (!node) {
            return nullptr;
        }
        auto uring = node->FindSubgroup("subsystem", "uring");
        return uring ? uring->FindSubgroup("shard", "0") : nullptr;
    }

    TUringSnapshot TakeUringSnapshot(const NMonitoring::TDynamicCounterPtr& root) {
        TUringSnapshot snapshot;
        for (ui32 nodeId = 1; nodeId <= NumNodes; ++nodeId) {
            auto shard = FindShardCounters(root, nodeId);
            if (!shard) {
                continue;
            }
            for (const char* name : UringCounterNames) {
                if (auto counter = shard->FindCounter(name)) {
                    snapshot.Counters[name] += counter->Val();
                }
            }
            for (const char* name : UringPhaseNames) {
                if (auto counter = shard->FindCounter(TString("TotalTime/") + name)) {
                    snapshot.Counters[name] += counter->Val();
                }
            }
            if (auto histogram = shard->FindHistogram("EventToWireTime")) {
                auto values = histogram->Snapshot();
                snapshot.EventToWire.resize(Max<size_t>(snapshot.EventToWire.size(), values->Count()));
                for (ui32 i = 0; i < values->Count(); ++i) {
                    snapshot.EventToWire[i].first = values->UpperBound(i);
                    snapshot.EventToWire[i].second += values->Value(i);
                }
            }
        }
        return snapshot;
    }

    TUringSnapshot Subtract(const TUringSnapshot& end, const TUringSnapshot& begin) {
        TUringSnapshot delta = end;
        for (auto& [name, value] : delta.Counters) {
            value -= begin.Get(name);
        }
        for (size_t i = 0; i < delta.EventToWire.size() && i < begin.EventToWire.size(); ++i) {
            delta.EventToWire[i].second -= begin.EventToWire[i].second;
        }
        return delta;
    }

    // Bucket-granular quantile: returns the upper bound of the bucket the quantile falls into, so the
    // answer is an upper estimate rounded up to the next power-of-two bucket edge.
    double HistogramQuantile(const TVector<std::pair<double, ui64>>& buckets, double quantile) {
        ui64 total = 0;
        for (const auto& [bound, count] : buckets) {
            total += count;
        }
        if (!total) {
            return 0;
        }
        const ui64 target = Max<ui64>(1, quantile * total);
        ui64 acc = 0;
        for (const auto& [bound, count] : buckets) {
            acc += count;
            if (acc >= target) {
                return bound;
            }
        }
        return buckets.back().first;
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////
    // reporting
    ////////////////////////////////////////////////////////////////////////////////////////////////////

    void Line(TStringBuf key, const TString& value) {
        Cout << Sprintf("%-32s%s", TString(key).c_str(), value.c_str()) << Endl;
    }

    TString Percent(ui64 part, ui64 whole) {
        return whole ? Sprintf("%.1f%%", 100.0 * part / whole) : TString("n/a");
    }

    double Seconds(TDuration duration) {
        return duration.MicroSeconds() / 1e6;
    }

    void ReportConfig(const TConfig& cfg, bool v2Negotiated) {
        Cout << "=== configuration ===" << Endl;
        Line("session:", Sprintf("%s requested, %s negotiated",
            cfg.UseV2 ? "v2" : "v1", v2Negotiated ? "v2" : "v1"));
        Line("duration:", Sprintf("%.3f s (first %.3f s excluded as warm-up)",
            Seconds(cfg.Duration), Seconds(cfg.Warmup)));
        Line("payload:", cfg.PayloadMin == cfg.PayloadMax
            ? Sprintf("%u bytes, %s", cfg.PayloadMin, cfg.ProtobufPayload ? "separate rope" : "inline in protobuf")
            : Sprintf("%u..%u bytes, %s", cfg.PayloadMin, cfg.PayloadMax,
                cfg.ProtobufPayload ? "separate rope" : "inline in protobuf"));
        Line("load actors:", Sprintf("%u over %u channel(s), %u in flight each",
            cfg.NumLoadActors, cfg.NumChannels, cfg.InFly));
        Line("executor threads:", Sprintf("%u per node", cfg.NumThreads));
        Line("ic inflight limit:", Sprintf("%u bytes", cfg.Inflight));
        Line("rdma:", cfg.EnableRdma ? "enabled" : "disabled");
        if (!cfg.EffectiveCpuMask.empty()) {
            Line("cpu affinity:", Sprintf("%s (%u CPU(s), process-wide)",
                cfg.EffectiveCpuMask.c_str(), cfg.EffectiveCpuCount));
        } else {
            Line("cpu affinity:", "unrestricted (pass --pin on multi-socket machines)");
        }
        if (cfg.UseV2) {
            Line("v2 engine:", Sprintf("%u shard(s) x %u ring(s), sqpoll %s, checksum %s, preserialize %s",
                cfg.UringShards, cfg.RingsPerShard, cfg.SqPoll ? "on" : "off",
                cfg.Checksum ? "on" : "off", cfg.Preserialize ? "on" : "off"));
        }
    }

    void ReportTraffic(const std::vector<NInterconnect::TLoadActorStats>& stats, TDuration window) {
        ui64 bytes = 0;
        ui64 messages = 0;
        ui64 dropped = 0;
        for (const auto& item : stats) {
            bytes += item.ThroughputBytes;
            messages += item.ThroughputSamples;
            dropped += item.NumDropped;
        }
        const double seconds = Seconds(window);

        Cout << Endl << "=== traffic ===" << Endl;
        Line("measurement window:", Sprintf("%.3f s", seconds));
        Line("application bytes:", Sprintf("%" PRIu64 " (request + response, as accounted by the load actors)", bytes));
        Line("throughput:", Sprintf("%.2f MB/s", bytes / seconds / 1e6));
        Line("messages:", Sprintf("%" PRIu64, messages));
        Line("message rate:", Sprintf("%.0f msg/s", messages / seconds));
        Line("dropped:", Sprintf("%" PRIu64, dropped));
    }

    void ReportLatency(const std::vector<NInterconnect::TLoadActorStats>& stats) {
        Cout << Endl << "=== round-trip latency ===" << Endl;

        // Every load actor reports the same fixed quantile ladder over its own trailing window; report
        // the worst load actor per quantile, which is the number a user would actually feel.
        THashMap<int, ui64> worst;
        TVector<double> quantiles;
        ui64 samples = 0;
        for (const auto& item : stats) {
            samples += item.RttSamples;
            for (const auto& [quantile, value] : item.LatencyPercentilesUs) {
                const int key = quantile * 10000;
                if (!worst.contains(key)) {
                    quantiles.push_back(quantile);
                }
                worst[key] = Max(worst[key], value);
            }
        }
        if (quantiles.empty()) {
            Line("(no samples)", "");
            return;
        }
        Sort(quantiles);
        Line("samples:", Sprintf("%" PRIu64 " (trailing window per load actor)", samples));
        for (const double quantile : quantiles) {
            Line(Sprintf("p%g:", quantile * 100), Sprintf("%" PRIu64 " us", worst[int(quantile * 10000)]));
        }
        // The load actor timestamps with TActorContext::Monotonic(), which reads a periodically refreshed
        // cached value rather than the clock, so these samples are quantized to the actor system's update
        // interval -- hence a p50 of zero next to a p90 of about a millisecond. Treat the shape as
        // indicative and use the v2 event-to-wire histogram below for fine-grained latency.
        Line("note:", "quantized to the actor system monotonic clock tick");
    }

    void ReportCpu(const TConfig& cfg, TDuration user, TDuration sys, TDuration window, ui64 bytes, ui64 messages) {
        const double cpuSeconds = Seconds(user + sys);
        const double windowSeconds = Seconds(window);

        Cout << Endl << "=== cpu (whole process: both endpoints) ===" << Endl;
        Line("cpu time:", Sprintf("%.3f s user + %.3f s sys = %.3f s", Seconds(user), Seconds(sys), cpuSeconds));
        Line("cores busy:", Sprintf("%.2f", windowSeconds ? cpuSeconds / windowSeconds : 0.0));
        if (bytes) {
            Line("cpu per MB:", Sprintf("%.3f ms", cpuSeconds * 1e3 / (bytes / 1e6)));
        }
        if (messages) {
            Line("cpu per message:", Sprintf("%.3f us", cpuSeconds * 1e6 / messages));
        }
        if (cfg.UseV2 && cfg.SqPoll) {
            Line("note:", Sprintf("includes %u spinning SQPOLL kernel thread(s) per node; "
                "use --no-sqpoll to attribute that work to the worker instead",
                cfg.UringShards * cfg.RingsPerShard));
        }
    }

    void ReportUringEngine(const TUringSnapshot& delta) {
        Cout << Endl << "=== v2 io_uring engine (summed over both nodes) ===" << Endl;

        const ui64 bytesSent = delta.Get("BytesSent");
        const ui64 copied = delta.Get("BytesCopied");
        const ui64 aliased = delta.Get("BytesAliased");
        const ui64 serialized = copied + aliased;

        Line("bytes sent:", Sprintf("%" PRIu64, bytesSent));
        Line("bytes received:", Sprintf("%" PRIu64, delta.Get("BytesReceived")));
        Line("bytes copied:", Sprintf("%" PRIu64 " (%s of serialized)", copied, Percent(copied, serialized).c_str()));
        Line("bytes aliased:", Sprintf("%" PRIu64 " (%s of serialized)", aliased, Percent(aliased, serialized).c_str()));
        Line("events sent:", Sprintf("%" PRIu64, delta.Get("EventsSent")));
        Line("events received:", Sprintf("%" PRIu64 " via actor system, %" PRIu64 " via direct callback",
            delta.Get("EventsReceivedActorSystem"), delta.Get("EventsReceivedCallback")));
        Line("io_uring submits:", Sprintf("%" PRIu64 " submit(s) for %" PRIu64 " sqe(s), %" PRIu64 " completion(s)",
            delta.Get("SubmitCount"), delta.Get("SQEAllocated"), delta.Get("CQEProcessed")));
        Line("worker wakeups:", Sprintf("%" PRIu64 " via eventfd", delta.Get("EventWakeups")));
        Line("io unavailable:", Sprintf("%" PRIu64 " read, %" PRIu64 " write (out of sqe/buffer resources)",
            delta.Get("ReadUnavail"), delta.Get("WriteUnavail")));

        ui64 totalPhase = 0;
        for (const char* name : UringPhaseNames) {
            totalPhase += delta.Get(name);
        }
        Cout << Endl;
        Line("worker thread time:", Sprintf("%.3f s across all shards", totalPhase / 1e9));
        for (const char* name : UringPhaseNames) {
            const ui64 value = delta.Get(name);
            Line(Sprintf("  %s:", name), Sprintf("%.3f s (%s)", value / 1e9, Percent(value, totalPhase).c_str()));
        }

        if (!delta.EventToWire.empty()) {
            Cout << Endl;
            Line("event-to-wire latency:", "(bucket upper bounds)");
            for (const double quantile : {0.5, 0.9, 0.99, 0.999, 1.0}) {
                const double ns = HistogramQuantile(delta.EventToWire, quantile);
                Line(Sprintf("  p%g:", quantile * 100),
                    ns == Max<double>() ? TString("+inf") : Sprintf("%.1f us", ns / 1e3));
            }
        }
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////

    bool IsSessionV2(TTestICCluster& cluster, ui32 me, ui32 peer) {
        auto response = cluster.GetSessionDbg(me, peer);
        if (!response.Wait(TDuration::Seconds(5))) {
            ythrow yexception() << "timed out reading the session debug page of node " << me << " -> " << peer;
        }
        return response.GetValueSync().Contains("Session (v2)");
    }

    int Run(const TConfig& cfg) {
        if (cfg.UseV2 && !TUringContext::IsAvailable()) {
            Cerr << "io_uring is not available on this machine; session v2 cannot be used. "
                    "Re-run with --v1 to benchmark the v1 session." << Endl;
            return 1;
        }

        auto settingsCustomizer = [&cfg](ui32, TInterconnectSettings& settings) {
            settings.V2.Enable = cfg.UseV2;
            settings.V2.ChecksumEvents = cfg.Checksum;
            settings.V2.EnableSQPOLL = cfg.SqPoll;
            settings.V2.EnablePreserializeEvents = cfg.Preserialize;
            settings.V2.Threads = cfg.UringShards;
            settings.V2.RingsPerShard = cfg.RingsPerShard;
            if (cfg.TcpSocketBufferSize) {
                settings.TCPSocketBufferSize = cfg.TcpSocketBufferSize;
            }
        };
        TNode::TLogBackendFactory logBackendFactory;
        if (!cfg.Verbose) {
            logBackendFactory = [] { return CreateNullBackend(); };
        }

        // Declared before the cluster: load actors may still report while the actor systems are torn down.
        TStatsSink sink;

        TTestICCluster cluster(NumNodes, TChannelsConfig(), /*tiSettings=*/nullptr, /*loggerSettings=*/nullptr,
            cfg.EnableRdma ? TTestICCluster::EMPTY : TTestICCluster::DISABLE_RDMA,
            TTestICCluster::TCheckerFactory{}, /*deadPeerTimeout=*/TDuration::Seconds(5),
            cfg.Inflight, settingsCustomizer, logBackendFactory, cfg.NumThreads);

        // The load actor queries its own node's responder for the shared traffic counter before it starts
        // sending, so a responder is needed on the client node too, not just on the peer.
        for (ui32 nodeId = 1; nodeId <= NumNodes; ++nodeId) {
            cluster.GetNode(nodeId)->RegisterServiceActor(NInterconnect::MakeLoadResponderActorId(nodeId),
                NInterconnect::CreateLoadResponderActor());
        }

        for (ui32 i = 0; i < cfg.NumLoadActors; ++i) {
            NInterconnect::TLoadParams params;
            params.Name = Sprintf("bench-%u", i);
            params.Channel = 1 + i % cfg.NumChannels;
            params.NodeHops = {ServerNode};
            params.SizeMin = cfg.PayloadMin;
            params.SizeMax = cfg.PayloadMax;
            params.InFlyMax = cfg.InFly;
            params.IntervalMin = TDuration::Zero();
            params.IntervalMax = TDuration::Zero();
            params.SoftLoad = false;
            params.Duration = cfg.Duration;
            params.UseProtobufWithPayload = cfg.ProtobufPayload;
            params.RdmaMode = 0;
            params.DelayBeforeMeasurements = cfg.Warmup;

            cluster.RegisterActor(NInterconnect::CreateLoadActor(params,
                [&sink](const TActorContext&, TString&&, const NInterconnect::TLoadActorStats& stats) {
                    sink.Add(stats);
                }), ClientNode);
        }

        // Let the connection come up and the traffic settle; the load actors independently exclude the
        // same warm-up period from their own statistics.
        Sleep(cfg.Warmup);

        const bool v2Negotiated = IsSessionV2(cluster, ClientNode, ServerNode);
        if (cfg.UseV2 && !v2Negotiated) {
            Cerr << "session v2 was requested but v1 was negotiated; refusing to report misleading numbers" << Endl;
            return 1;
        }

        const TUringSnapshot uringBegin = TakeUringSnapshot(cluster.GetCounters());
        const TRusage rusageBegin = TRusage::Get();
        const TInstant begin = TInstant::Now();

        // Each load actor poisons itself Duration after its own bootstrap; allow generous slack so a
        // slow start never truncates the run.
        if (!sink.WaitFor(cfg.NumLoadActors, cfg.Duration + TDuration::Seconds(60))) {
            Cerr << "timed out waiting for the load actors to finish" << Endl;
            return 1;
        }

        const TDuration window = TInstant::Now() - begin;
        const TRusage rusageEnd = TRusage::Get();
        const TUringSnapshot uringDelta = Subtract(TakeUringSnapshot(cluster.GetCounters()), uringBegin);

        const std::vector<NInterconnect::TLoadActorStats> stats = sink.Get();
        ui64 bytes = 0;
        ui64 messages = 0;
        for (const auto& item : stats) {
            bytes += item.ThroughputBytes;
            messages += item.ThroughputSamples;
        }

        ReportConfig(cfg, v2Negotiated);
        ReportTraffic(stats, window);
        ReportLatency(stats);
        ReportCpu(cfg, rusageEnd.Utime - rusageBegin.Utime, rusageEnd.Stime - rusageBegin.Stime, window, bytes,
            messages);
        if (v2Negotiated) {
            ReportUringEngine(uringDelta);
        }

        return 0;
    }

} // namespace

int main(int argc, char** argv) {
    try {
        TConfig cfg = ParseOptions(argc, argv);
        // Pin before Run allocates anything and starts threads, so affinity and first-touch NUMA
        // placement are deterministic for the whole process.
        ApplyCpuPinning(cfg);
        return Run(cfg);
    } catch (const std::exception& ex) {
        Cerr << "failed: " << ex.what() << Endl;
        return 1;
    }
}
