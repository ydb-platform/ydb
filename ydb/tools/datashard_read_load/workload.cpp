#include "workload.h"

#include <library/cpp/getopt/last_getopt.h>
#include <util/datetime/base.h>

#include <algorithm>
#include <charconv>
#include <limits>
#include <stdexcept>
#include <string_view>

namespace NDataShardReadLoad {
namespace {

constexpr int64_t MaxDurationUs = 24LL * 60 * 60 * 1000000;
constexpr uint64_t MaxRate = 1000000;

uint64_t Number(std::string_view text) {
    uint64_t result = 0;
    auto [end, error] = std::from_chars(text.data(), text.data() + text.size(), result);
    if (error != std::errc() || end != text.data() + text.size() || text.empty()) {
        throw std::runtime_error("expected an unsigned integer: " + std::string(text));
    }
    return result;
}

int64_t Duration(const std::string& text) {
    const auto duration = TDuration::Parse(text);
    if (!duration || duration.MicroSeconds() > MaxDurationUs) {
        throw std::runtime_error("duration must be positive and at most 24h: " + text);
    }
    return duration.MicroSeconds();
}

} // namespace

TOptions ParseOptions(int argc, char** argv) {
    TOptions result;
    NLastGetopt::TOpts opts = NLastGetopt::TOpts::Default();
    opts.SetTitle("SDK-only DataShard read/cancel load. Commands: init, run.");
    opts.SetFreeArgsNum(1);
    opts.AddLongOption('e', "endpoint", "YDB endpoint").Required().RequiredArgument("HOST:PORT").StoreResult(&result.Endpoint);
    opts.AddLongOption('d', "database", "Database path").Required().RequiredArgument("PATH").StoreResult(&result.Database);
    opts.AddLongOption("table", "Dedicated load table path").Required().RequiredArgument("PATH").StoreResult(&result.Table);
    opts.AddLongOption("rows", "Rows populated by init; must match when running").RequiredArgument("N").StoreResult(&result.Rows);
    opts.AddLongOption("payload-bytes", "Payload bytes per row during init").RequiredArgument("N").StoreResult(&result.PayloadBytes);
    opts.AddLongOption("rows-per-read", "Rows returned by each narrow range read").RequiredArgument("N").StoreResult(&result.RowsPerRead);
    opts.AddLongOption("seed", "Random seed").RequiredArgument("N").StoreResult(&result.Seed);
    opts.AddLongOption("threads", "Dispatch threads; rates and in-flight caps remain process-wide totals; default 1")
        .RequiredArgument("N").StoreResult(&result.Threads);
    opts.AddLongOption("dispatch-batch", "Maximum arrivals dispatched per stream per thread per wakeup; default 64")
        .RequiredArgument("N").StoreResult(&result.DispatchBatch);
    opts.AddLongOption("long-inflight", "Reserved long-request sessions (required for run)").RequiredArgument("N").StoreResult(&result.Caps[Long]);
    opts.AddLongOption("short-inflight", "Reserved short-request sessions (required for run)").RequiredArgument("N").StoreResult(&result.Caps[Short]);
    opts.AddLongOption("probe-inflight", "Reserved probe sessions (required for run)").RequiredArgument("N").StoreResult(&result.Caps[Probe]);
    opts.AddLongOption("probe-rps", "Probe arrivals per second; default 1").RequiredArgument("N").StoreResult(&result.ProbeRate);
    std::string cancelAfter = "100ms", longTimeout = "60s", rpcGrace = "5s", drainTimeout = "70s";
    opts.AddLongOption("cancel-after", "Short-request server cancellation deadline; default 100ms").RequiredArgument("DURATION").StoreResult(&cancelAfter);
    opts.AddLongOption("long-timeout", "Operation timeout for all requests; default 60s").RequiredArgument("DURATION").StoreResult(&longTimeout);
    opts.AddLongOption("rpc-grace", "Extra RPC timeout beyond operation timeout; default 5s").RequiredArgument("DURATION").StoreResult(&rpcGrace);
    opts.AddLongOption("drain-timeout", "Maximum final drain time; default 70s").RequiredArgument("DURATION").StoreResult(&drainTimeout);
    std::vector<std::string> phases;
    opts.AddLongOption("phase", "Repeat in execution order: NAME:DURATION:LONG_RPS:SHORT_RPS").RequiredArgument("SPEC").AppendTo(&phases);
    NLastGetopt::TOptsParseResult parsed(&opts, argc, argv);
    result.Command = parsed.GetFreeArgs().at(0);
    if (result.Command != "init" && result.Command != "run") {
        throw std::runtime_error("command must be init or run");
    }
    if (result.Table.empty() || result.Table.front() != '/' || result.Table.find_first_of("`\\\n\r") != std::string::npos) {
        throw std::runtime_error("table must be an absolute path without backticks, backslashes or newlines");
    }
    if (result.Rows == 0 || result.Rows > 1000000000 || result.RowsPerRead == 0 || result.RowsPerRead > result.Rows || result.PayloadBytes > 1048576) {
        throw std::runtime_error("require 1 <= rows-per-read <= rows <= 1000000000 and payload-bytes <= 1048576");
    }
    result.CancelAfterUs = Duration(cancelAfter);
    result.LongTimeoutUs = Duration(longTimeout);
    result.RpcGraceUs = Duration(rpcGrace);
    result.DrainTimeoutUs = Duration(drainTimeout);
    if (result.CancelAfterUs >= result.LongTimeoutUs || result.LongTimeoutUs + result.RpcGraceUs > MaxDurationUs) {
        throw std::runtime_error("require cancel-after < long-timeout and long-timeout + rpc-grace <= 24h");
    }
    if (result.ProbeRate > MaxRate) {
        throw std::runtime_error("probe-rps must be <= 1000000");
    }
    if (!result.DispatchBatch || result.DispatchBatch > 1024) {
        throw std::runtime_error("dispatch-batch must be between 1 and 1024");
    }
    if (!result.Threads || result.Threads > 64) {
        throw std::runtime_error("threads must be between 1 and 64");
    }
    int64_t totalDuration = 0;
    for (const auto& spec : phases) {
        std::vector<std::string> parts;
        size_t start = 0;
        for (;;) {
            auto end = spec.find(':', start);
            parts.emplace_back(spec.substr(start, end - start));
            if (end == std::string::npos) {
                break;
            }
            start = end + 1;
        }
        if (parts.size() != 4 || parts[0].empty() || parts[0].find_first_not_of("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-") != std::string::npos) {
            throw std::runtime_error("phase must be NAME:DURATION:LONG_RPS:SHORT_RPS: " + spec);
        }
        TPhase phase{parts[0], Duration(parts[1]), {Number(parts[2]), Number(parts[3]), result.ProbeRate}};
        for (const auto& previous : result.Phases) {
            if (previous.Name == phase.Name) {
                throw std::runtime_error("phase names must be unique");
            }
        }
        if (phase.Rates[Long] > MaxRate || phase.Rates[Short] > MaxRate) {
            throw std::runtime_error("request rates must be <= 1000000");
        }
        totalDuration += phase.DurationUs;
        result.Phases.push_back(phase);
    }
    if (totalDuration > MaxDurationUs) {
        throw std::runtime_error("total phase duration must be <= 24h");
    }
    if (result.Command == "run") {
        if (phases.empty() || !parsed.Has("long-inflight") || !parsed.Has("short-inflight") || !parsed.Has("probe-inflight")) {
            throw std::runtime_error("run requires --phase, --long-inflight, --short-inflight and --probe-inflight");
        }
        uint64_t total = 0;
        for (size_t p = 0; p < PopulationCount; ++p) {
            if (result.Caps[p] > 100000) {
                throw std::runtime_error("each in-flight cap must be <= 100000");
            }
            total += result.Caps[p];
            for (const auto& phase : result.Phases) {
                if (phase.Rates[p] && !result.Caps[p]) {
                    throw std::runtime_error("positive request rates require positive corresponding in-flight caps");
                }
            }
        }
        if (!total) {
            throw std::runtime_error("at least one in-flight cap must be positive");
        }
    }
    return result;
}

std::array<uint64_t, PopulationCount> WorkerCaps(const TOptions& options, uint64_t worker) {
    if (!options.Threads || worker >= options.Threads) {
        throw std::invalid_argument("invalid worker index or thread count");
    }
    std::array<uint64_t, PopulationCount> caps;
    for (size_t p = 0; p < PopulationCount; ++p) {
        caps[p] = options.Caps[p] / options.Threads + (worker < options.Caps[p] % options.Threads);
    }
    return caps;
}

int64_t TReportClock::NextUs() const {
    return LastUs_ + 1000000;
}

int64_t TReportClock::Record(int64_t collectedUs) {
    const int64_t intervalUs = collectedUs - LastUs_;
    LastUs_ = collectedUs;
    return intervalUs;
}

TArrivalClock::TArrivalClock(uint64_t rate, int64_t startUs, int64_t endUs,
    uint64_t partition, uint64_t partitions)
    : Rate_(rate), StartUs_(startUs), EndUs_(endUs), Partition_(partition), Partitions_(partitions)
{
    if (rate > MaxRate || startUs < 0 || endUs <= startUs || endUs > MaxDurationUs
        || !partitions || partition >= partitions) {
        throw std::invalid_argument("invalid arrival clock bounds or rate");
    }
}

uint64_t TArrivalClock::Poll(int64_t nowUs) {
    if (!Rate_ || nowUs < StartUs_) {
        return 0;
    }
    const uint64_t elapsed = std::min(nowUs, EndUs_ - 1) - StartUs_;
    const uint64_t totalDue = elapsed * Rate_ / 1000000 + 1;
    const uint64_t due = totalDue > Partition_ ? (totalDue - Partition_ - 1) / Partitions_ + 1 : 0;
    const uint64_t result = due - Consumed_;
    Consumed_ = due;
    return result;
}

TArrivalBatch TArrivalClock::PollBatch(int64_t nowUs, uint64_t batchLimit) {
    const uint64_t due = Poll(nowUs);
    const uint64_t dispatch = nowUs < EndUs_ ? std::min(due, batchLimit) : 0;
    return {due, dispatch, Consumed_ - dispatch};
}

int64_t TArrivalClock::DueUs(uint64_t index) const {
    if (!Rate_ || index >= Consumed_) {
        throw std::out_of_range("arrival has not been scheduled");
    }
    return StartUs_ + ((Partition_ + index * Partitions_) * 1000000 + Rate_ - 1) / Rate_;
}

int64_t TArrivalClock::NextUs() const {
    return Rate_ ? std::min(EndUs_, StartUs_ + static_cast<int64_t>(
        ((Partition_ + Consumed_ * Partitions_) * 1000000 + Rate_ - 1) / Rate_)) : EndUs_;
}

int64_t TArrivalClock::LastDueUs() const {
    return Consumed_ && Rate_ ? DueUs(Consumed_ - 1) : StartUs_;
}

TCapacity::TCapacity(std::array<uint64_t, PopulationCount> caps)
    : Caps_(caps)
{}

bool TCapacity::Acquire(size_t population) {
    if (Outstanding_.at(population) == Caps_.at(population)) {
        return false;
    }
    ++Outstanding_[population];
    return true;
}

void TCapacity::Release(size_t population) {
    if (!Outstanding_.at(population)) {
        throw std::logic_error("releasing an idle population");
    }
    --Outstanding_[population];
}

uint64_t TCapacity::Outstanding(size_t population) const {
    return Outstanding_.at(population);
}

} // namespace NDataShardReadLoad
