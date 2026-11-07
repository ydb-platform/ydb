#include <ydb/library/schlab/schine/scheduler.h>
#include <util/system/datetime.h>

#include <algorithm>
#include <charconv>
#include <chrono>
#include <iomanip>
#include <iostream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <vector>

using namespace NKikimr::NSchLab;

namespace {

struct TOptions {
    ui64 Owners = 8;
    ui64 Gates = 10;
    ui64 Active = 0; // All registered queues.
    ui64 Depth = 4;
    ui64 Jobs = 200'000;
    ui64 Warmup = 10'000;
    ui64 Repeats = 5;
    std::string Mode = "both";
};

ui64 ParseNumber(std::string_view value) {
    ui64 result = 0;
    const auto [end, error] = std::from_chars(value.data(), value.data() + value.size(), result);
    if (error != std::errc{} || end != value.data() + value.size()) {
        throw std::runtime_error("Invalid unsigned integer: " + std::string(value));
    }
    return result;
}

TOptions ParseOptions(int argc, char** argv) {
    TOptions options;
    for (int i = 1; i < argc; ++i) {
        const std::string_view name = argv[i];
        if (++i == argc) {
            throw std::runtime_error("Missing value for " + std::string(name));
        }
        const std::string_view value = argv[i];
        if (name == "--mode") {
            options.Mode = value;
        } else if (name == "--owners") {
            options.Owners = ParseNumber(value);
        } else if (name == "--gates") {
            options.Gates = ParseNumber(value);
        } else if (name == "--active") {
            options.Active = ParseNumber(value);
        } else if (name == "--depth") {
            options.Depth = ParseNumber(value);
        } else if (name == "--jobs") {
            options.Jobs = ParseNumber(value);
        } else if (name == "--warmup") {
            options.Warmup = ParseNumber(value);
        } else if (name == "--repeats") {
            options.Repeats = ParseNumber(value);
        } else {
            throw std::runtime_error("Unknown option: " + std::string(name));
        }
    }
    if (!options.Owners || options.Owners > 255 || !options.Gates || options.Gates > 255) {
        throw std::runtime_error("--owners and --gates must be in [1, 255]");
    }
    const ui64 queues = options.Owners * options.Gates;
    if (!options.Active) {
        options.Active = queues;
    }
    if (options.Active > queues || !options.Depth || options.Depth > 1'000'000 / options.Active) {
        throw std::runtime_error("Require active <= owners*gates and 1 <= active*depth <= 1000000");
    }
    if (!options.Jobs || options.Jobs > 1'000'000'000 || options.Warmup > 1'000'000'000
            || !options.Repeats || options.Repeats > 1000) {
        throw std::runtime_error("Require jobs in [1, 1000000000], warmup <= 1000000000, repeats in [1, 1000]");
    }
    if (options.Mode != "steady" && options.Mode != "burst" && options.Mode != "both") {
        throw std::runtime_error("--mode must be steady, burst or both");
    }
    return options;
}

class TWorkload {
    TScheduler Scheduler;
    std::vector<TCbs*> Active;
    std::vector<TCbs*> ByIndex;
    ui64 TimeNs = 1'000'000'000;
    ui64 Submitted = 0;
    ui64 Completed = 0;
    ui64 SubmittedIds = 0;
    ui64 CompletedIds = 0;
    const ui64 Depth;

    void Add(TCbs* cbs) {
        auto job = Scheduler.CreateJob();
        // Deterministic mixture of short and long operations, expressed in virtual ns.
        job->Cost = 10'000 * (1 + Submitted % 8);
        Scheduler.AddJob(cbs, job, cbs->OwnerIdx, cbs->GateIdx, ++TimeNs);
        SubmittedIds += job->Id;
        ++Submitted;
    }

    TCbs* Complete() {
        auto job = Scheduler.SelectJob(TimeNs);
        Y_ABORT_UNLESS(job, "Forseti returned no job while work was pending");
        auto* cbs = ByIndex[job->CbsIdx];
        TimeNs += job->Cost + 1; // Same virtual-time progression as PDisk.
        const ui64 completedId = job->Id;
        Scheduler.CompleteJob(TimeNs, job);
        CompletedIds += completedId;
        ++Completed;
        return cbs;
    }

public:
    explicit TWorkload(const TOptions& options)
        : Depth(options.Depth)
    {
        Scheduler.SetIsBinLogEnabled(false); // PDisk's normal configuration.
        const ui64 queues = options.Owners * options.Gates;
        for (ui64 owner = 0; owner < options.Owners; ++owner) {
            for (ui64 gate = 0; gate < options.Gates; ++gate) {
                TCbs cbs;
                cbs.Weight = 1 + gate;
                cbs.MaxBudget = gate == 0 ? 2'000'000 : 0;
                Scheduler.AddCbs(owner, gate, std::move(cbs), ++TimeNs);
            }
        }
        // AddCbs can reallocate its vector. Cache pointers only after registration is complete.
        ByIndex.resize(queues);
        for (ui64 i = 0; i < queues; ++i) {
            auto* cbs = Scheduler.GetCbs(i / options.Gates, i % options.Gates);
            ByIndex[cbs->CbsIdx] = cbs;
        }
        for (ui64 i = 0; i < options.Active; ++i) {
            Active.push_back(ByIndex[i * queues / options.Active]);
        }
    }

    void Fill() {
        for (ui64 depth = 0; depth < Depth; ++depth) {
            for (auto* cbs : Active) {
                Add(cbs);
            }
        }
    }

    void Steady(ui64 jobs) {
        for (ui64 i = 0; i < jobs; ++i) {
            Add(Complete()); // Keep the selected queue at a constant depth.
        }
    }

    void Burst(ui64 jobs) {
        while (jobs) {
            const ui64 count = std::min<ui64>(jobs, Active.size() * Depth);
            for (ui64 i = 0; i < count; ++i) {
                Add(Active[i % Active.size()]);
            }
            for (ui64 i = 0; i < count; ++i) {
                Complete();
            }
            jobs -= count;
        }
    }

    ui64 VerifyAndDrain() {
        while (Completed < Submitted) {
            Complete();
        }
        Y_ABORT_UNLESS(Scheduler.IsEmpty() && !Scheduler.SelectJob(TimeNs));
        Y_ABORT_UNLESS(Completed == Submitted && CompletedIds == SubmittedIds);
        return CompletedIds;
    }
};

void Run(const TOptions& options, std::string_view mode, ui64 repeat) {
    TWorkload workload(options);
    if (mode == "steady") {
        workload.Fill();
        workload.Steady(options.Warmup);
    } else {
        workload.Burst(options.Warmup);
    }
    const auto start = std::chrono::steady_clock::now();
    const ui64 cpuStartUs = ThreadCPUTime();
    if (mode == "steady") {
        workload.Steady(options.Jobs);
    } else {
        workload.Burst(options.Jobs);
    }
    const ui64 cpuFinishUs = ThreadCPUTime();
    const auto finish = std::chrono::steady_clock::now();
    const double ns = std::chrono::duration<double, std::nano>(finish - start).count();
    const ui64 checksum = workload.VerifyAndDrain();
    std::cout << mode << '\t' << options.Owners << '\t' << options.Gates << '\t'
        << options.Active << '\t' << options.Depth << '\t' << options.Jobs << '\t'
        << options.Warmup << '\t' << repeat << '\t' << ns / 1e6 << '\t'
        << ns / options.Jobs << '\t' << options.Jobs * 1e9 / ns << '\t'
        << (cpuFinishUs - cpuStartUs) * 1000.0 / options.Jobs << '\t' << checksum << '\n';
}

} // anonymous namespace

int main(int argc, char** argv) {
    if (argc == 2 && std::string_view(argv[1]) == "--help") {
        std::cout << "Forseti scheduler benchmark (no disk I/O). Outputs TSV.\n"
            "Options: --owners 8 --gates 10 --active 0 (all) --depth 4\n"
            "         --jobs 200000 --warmup 10000 --repeats 5 --mode both|steady|burst\n";
        return 0;
    }
    try {
        const auto options = ParseOptions(argc, argv);
        std::cout << "mode\towners\tgates\tactive\tdepth\tjobs\twarmup\trepeat\tms\tns_per_job\tjobs_per_second\tcpu_ns_per_job\tchecksum\n"
            << std::fixed << std::setprecision(3);
        for (ui64 repeat = 1; repeat <= options.Repeats; ++repeat) {
            for (const auto mode : {"steady", "burst"}) {
                if (options.Mode == "both" || options.Mode == mode) {
                    Run(options, mode, repeat);
                }
            }
        }
    } catch (const std::exception& error) {
        std::cerr << error.what() << '\n';
        return 2;
    }
}
