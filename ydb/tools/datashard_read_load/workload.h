#pragma once

#include <array>
#include <cstdint>
#include <string>
#include <vector>

namespace NDataShardReadLoad {

constexpr size_t PopulationCount = 3;
enum EPopulation : size_t { Long, Short, Probe };
constexpr std::array<const char*, PopulationCount> PopulationNames = {"long", "short", "probe"};

struct TPhase {
    std::string Name;
    int64_t DurationUs = 0;
    std::array<uint64_t, PopulationCount> Rates = {};
};

struct TOptions {
    std::string Command;
    std::string Endpoint;
    std::string Database;
    std::string Table;
    uint64_t Rows = 4096;
    uint64_t PayloadBytes = 64;
    uint64_t RowsPerRead = 1;
    uint64_t Seed = 1;
    uint64_t Threads = 1;
    uint64_t DispatchBatch = 64;
    std::array<uint64_t, PopulationCount> Caps = {};
    uint64_t ProbeRate = 1;
    int64_t CancelAfterUs = 100000;
    int64_t LongTimeoutUs = 60000000;
    int64_t RpcGraceUs = 5000000;
    int64_t DrainTimeoutUs = 70000000;
    std::vector<TPhase> Phases;
};

TOptions ParseOptions(int argc, char** argv);

// Rates and caps are process-wide. Sessions are divided among workers;
// their clocks interleave arrivals from the original population schedule.
std::array<uint64_t, PopulationCount> WorkerCaps(const TOptions& options, uint64_t worker);

// Record interval boundaries after statistics collection, using the same
// timestamp as elapsed_s. Collection delays must not overlap later intervals.
class TReportClock {
public:
    int64_t NextUs() const;
    int64_t Record(int64_t collectedUs);

private:
    int64_t LastUs_ = 0;
};

struct TArrivalBatch {
    uint64_t Scheduled = 0;
    uint64_t Dispatch = 0;
    uint64_t FirstIndex = 0;
};

// A deterministic open-loop clock. Elapsed arrivals are consumed immediately;
// at most batchLimit are eligible for dispatch, without retaining a backlog.
// Partition i owns global arrivals i, i + partitions, i + 2 * partitions, ...;
// indices returned by PollBatch are local to that partition.
class TArrivalClock {
public:
    TArrivalClock(uint64_t rate, int64_t startUs, int64_t endUs,
        uint64_t partition = 0, uint64_t partitions = 1);
    uint64_t Poll(int64_t nowUs);
    TArrivalBatch PollBatch(int64_t nowUs, uint64_t batchLimit);
    int64_t DueUs(uint64_t index) const;
    int64_t NextUs() const;
    int64_t LastDueUs() const;

private:
    uint64_t Rate_;
    int64_t StartUs_;
    int64_t EndUs_;
    uint64_t Partition_;
    uint64_t Partitions_;
    uint64_t Consumed_ = 0;
};

// Capacity belongs to a population, not a phase. Changing phases must not
// release capacity or cancel outstanding requests.
class TCapacity {
public:
    explicit TCapacity(std::array<uint64_t, PopulationCount> caps);
    bool Acquire(size_t population);
    void Release(size_t population);
    uint64_t Outstanding(size_t population) const;

private:
    std::array<uint64_t, PopulationCount> Caps_;
    std::array<uint64_t, PopulationCount> Outstanding_ = {};
};

} // namespace NDataShardReadLoad
