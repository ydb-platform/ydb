#pragma once

#include <ydb/library/workload/abstract/workload_query_generator.h>

#include <atomic>
#include <mutex>
#include <random>
#include <sstream>

namespace NYdbWorkload {

enum SplitMergeWorkloadConstants : ui64 {
    TABLES_CNT = 1,
    INITIAL_PARTITIONS = 4,
    MIN_PARTITIONS = 1,
    MAX_PARTITIONS = 256,
    PARTITION_SIZE_MB = 1,
    PARTITION_SIZE_HIGH_MB = 64,
    AUTO_PARTITION = 0,
    CPU_THRESHOLD = 1,
    ROWS_CNT = 1000,
    STRING_LEN = 1024,
    TARGET_SHARE_PCT = 1, // percent of the target shard's key range
    PHASE_TIME_SEC = 60,
    CYCLES_CNT = 3,
    MAX_ROWS = Max<ui64>(),
};

class TSplitMergeWorkloadParams : public TWorkloadParams {
public:
    void ConfigureOpts(NLastGetopt::TOpts& opts, const ECommandType commandType, int workloadType) override;
    THolder<IWorkloadQueryGenerator> CreateGenerator() const override;
    TString GetWorkloadName() const override;

    // Table set
    std::string TableName = "splitmerge";
    ui64 TablesCnt = SplitMergeWorkloadConstants::TABLES_CNT;
    TVector<ui64> InitialPartitions; // per-table UNIFORM_PARTITIONS; empty -> default for all

    // Table policy (per-table lists; single value applies to all tables)
    TVector<ui64> MinPartitions = {SplitMergeWorkloadConstants::MIN_PARTITIONS};
    TVector<ui64> MaxPartitions = {SplitMergeWorkloadConstants::MAX_PARTITIONS};
    ui64 PartitionSizeMb = SplitMergeWorkloadConstants::PARTITION_SIZE_MB;
    ui64 PartitionSizeHighMb = SplitMergeWorkloadConstants::PARTITION_SIZE_HIGH_MB;
    TVector<ui64> AutoPartition = {SplitMergeWorkloadConstants::AUTO_PARTITION};
    ui64 CpuThreshold = SplitMergeWorkloadConstants::CPU_THRESHOLD;

    // Traffic shape
    ui64 RowsCnt = SplitMergeWorkloadConstants::ROWS_CNT;
    ui64 StringLen = SplitMergeWorkloadConstants::STRING_LEN;
    TString KeyDistribution = "sequential"; // sequential | targeted | striped
    TVector<ui64> TargetShards;               // empty -> all (sequential/striped) or shard 0 (targeted)
    ui64 TargetSharePct = SplitMergeWorkloadConstants::TARGET_SHARE_PCT;

    // Orchestration
    bool ConcurrentPhases = false;
    ui64 PhaseTimeSec = SplitMergeWorkloadConstants::PHASE_TIME_SEC;
    ui64 CyclesCnt = SplitMergeWorkloadConstants::CYCLES_CNT;
    ui64 MaxRows = SplitMergeWorkloadConstants::MAX_ROWS;

    // External-table mode (run-only): --table paths override the <path>N
    // naming; init/clean do not apply. The tool writes data and issues ALTERs
    // on these tables.
    TVector<TString> ExternalTables;
    TString KeyColumn = "k";
    TString PayloadColumn = "payload";
    TString PayloadType = "string"; // string | utf8 | uint64 | int64
    ui64 StartKey = 0;              // sequential-mode first key (external tables)
};

class TSplitMergeWorkloadGenerator final: public TWorkloadQueryGeneratorBase<TSplitMergeWorkloadParams> {
public:
    using TBase = TWorkloadQueryGeneratorBase<TSplitMergeWorkloadParams>;

    TSplitMergeWorkloadGenerator(const TSplitMergeWorkloadParams* params);

    std::string GetDDLQueries() const override;
    TQueryInfoList GetInitialData() override;
    TVector<std::string> GetCleanPaths() const override;
    TQueryInfoList GetWorkload(int type) override;
    TVector<TWorkloadType> GetSupportedWorkloadTypes() const override;

    enum class EType {
        SplitBySize = 0,
        SplitByLoad,
        MergeBySize,
        MergeByLoad,
        SplitBurst,
        MergeBurst,
        MultiTableSplit,
        SmallTableStarvation,
        SplitVsMergeRace,
        Flap,
        Status,
    };

private:
    // Primitives
    TQueryInfoList UpsertSequential(size_t tableIdx = 0); // fresh rows, monotonically increasing keys
    TQueryInfoList UpsertStriped();          // fresh rows, round-robin across shard ranges
    TQueryInfoList UpdateHotKeys();          // in-place UPSERTs over the fixed hot key set
    // merge/split phase ALTER. recordInitialPartitions: the ALTER's table
    // operation first describes the table and records its pre-ALTER partition
    // count as the cascade's "initial" value (used by the merge modes so the
    // done-line's "initial" is the true pre-merge count, not the first poll's).
    // failureLabel: when non-empty, a failed ALTER prints a
    // "<label>\terror\t..." line so a one-shot ALTER that is never re-issued
    // (flap phases, split-vs-merge-race) fails observably instead of only in
    // the driver's --verbose output.
    TQueryInfoList AlterPartitionSize(ui64 sizeMb, ui64 minPartitions, size_t tableIdx = 0, bool recordInitialPartitions = false, const TString& failureLabel = TString());
    TQueryInfoList StatusPoll();             // DescribeTable partition-count report
    TQueryInfoList WriteStep();              // routes write modes through --key-distribution
    TQueryInfoList PhaseWaitQuery(const TString& name); // no-op keepalive for wait phases

    // Mode state machines (state lives in the generator; the run driver calls GetWorkload repeatedly)
    TQueryInfoList FlapStep();
    // Shared "poll the merge cascade until drained" step for merge-by-size,
    // merge-burst and merge-by-load; modeName names the queries and output
    // lines (e.g. "merge-by-size", "merge-by-load").
    TQueryInfoList MergeCascadePollStep(const TString& modeName);
    TQueryInfoList SplitByLoadStep();
    // One-shot warning when a by-load mode runs against a table whose
    // AUTO_PARTITIONING_BY_LOAD is disabled by the init defaults.
    void WarnIfByLoadDisabled();
    TQueryInfoList ValidateExternalTables(); // one-shot schema validation (external mode)

    // Number of partitions used for hot-key-range and stride math. In external
    // mode this is the actual partition count of the given table observed at
    // validation time (the --initial-partitions default does not describe a
    // user table); otherwise the init-time UNIFORM_PARTITIONS value.
    ui64 HotKeySpacePartitions(size_t tableIdx = 0) const;

    // Atomically reserve up to rowsToWrite rows from the --max-rows budget;
    // returns the number actually reserved (0 when the budget is exhausted).
    // Callers must emit at most the returned count of VALUES rows so
    // --max-rows is a true hard stop even on a partial reservation. The
    // budget counts attempted rows: it is reserved before the query runs and
    // is not refunded if the query fails.
    ui64 TryReserveRows(ui64 rowsToWrite);

    TString FullTablePath(size_t tableIdx) const;
    size_t ExternalTableCount() const;
    bool ExternalMode() const;
    ui64 InitialPartitionsFor(size_t tableIdx) const;
    ui64 MinPartitionsFor(size_t tableIdx) const;
    ui64 MaxPartitionsFor(size_t tableIdx) const;
    ui64 AutoPartitionFor(size_t tableIdx) const;
    // Payload value/param emission for the configured --payload-type.
    void AddPayloadParam(NYdb::TParamsBuilder& builder, const TString& name) const;
    TString PayloadYqlType() const;

    // External-table mode: one-shot schema validation state. The first worker
    // to arrive claims the validation; the others wait it out. The actual
    // per-table partition counts observed during validation feed the
    // hot-key-range math. A schema-validation failure latches: the run stops
    // instead of degrading into keepalive traffic against a table the tool
    // cannot write correctly. Transient (transport/session) failures do not
    // latch: the claim is released and the next GetWorkload() call
    // re-attempts the validation.
    std::atomic<bool> ExternalValidated = false;
    std::atomic<bool> ExternalValidationFailed = false;
    // A generation prevents stale claimants from releasing a newer claim.
    // ValidationMutex serializes claim stealing and publishing so a claimant
    // cannot publish after losing ownership; in-flight validations keep their
    // counts private until that publication point.
    std::mutex ValidationMutex;
    std::atomic<i64> ValidationClaimGeneration = 0;
    // When the current validation claim was taken (microseconds). Waiters
    // whose wait exceeds ValidationClaimTimeoutSec steal the claim and
    // re-attempt, so a dead claimant (worker lost to exhausted retries)
    // cannot leave the run spinning on keepalives forever.
    std::atomic<i64> ValidationClaimTimeUs = 0;
    // Monotonic ticket source for claim generations.
    std::atomic<i64> ValidationClaimTicket = 0;
    static constexpr i64 ValidationClaimTimeoutSec = 30;
    // Per-table partition counts observed during validation (index = table).
    // Published once under ValidationMutex before ExternalValidated is
    // release-stored. Readers acquire ExternalValidated before accessing it.
    TVector<ui64> ExternalPartitionsCounts;

    TString BigString;
    std::atomic<ui64> NextFirstKey = 0;      // sequential fresh-key counter (upsert-seq pattern)
    std::atomic<ui64> RowsWritten = 0;
    std::atomic<ui64> TableRotator = 0;      // multi-table-split round-robin
    std::atomic<ui64> TargetShardRotator = 0; // targeted-mode shard round-robin

    // Flap state machine. The run driver calls GetWorkload() from multiple
    // worker threads concurrently, so the whole machine is guarded by a mutex:
    // phase transitions and the one-ALTER-per-phase flag must be atomic
    // check-and-set, or two workers can both observe "ALTER not issued" and
    // double-issue it.
    enum class EFlapPhase {
        Grow,
        Merge,
        Split,
    };
    std::mutex FlapMutex;
    EFlapPhase FlapPhase = EFlapPhase::Grow;
    ui64 FlapCycle = 0;
    TInstant FlapPhaseStart;
    bool FlapAlterIssued = false;            // one ALTER per phase, not per iteration

    // Merge ALTER state: issue once per run, then wait out the cascade.
    // Atomic exchange so exactly one worker issues the ALTER even under the
    // multi-threaded run driver.
    std::atomic<bool> MergeAlterIssued = false;
    // True for modes whose cascade is driven by a one-shot ALTER
    // (merge-by-size, merge-burst). MergeCascadePollStep must not poll until
    // that ALTER has actually executed (the driver's rate limiter may delay
    // it): polling in that window could report a "done" plateau for a
    // cascade that has not started. MergeAlterDone is set by the ALTER's
    // table operation on success.
    std::atomic<bool> MergeAlterExpected = false;
    std::atomic<bool> MergeAlterDone = false;

    // Merge-cascade drain tracking, shared by merge-by-size, merge-burst and
    // merge-by-load. The run driver calls GetWorkload() from multiple worker
    // threads concurrently, so all drain state is kept in atomics: the single
    // poller thread updates them, and they are safe to read from any worker.
    // A value of 0 in MergeCascadeInitialPartitions means "not recorded yet"
    // (for the merge modes it is recorded by the ALTER's table operation,
    // before any merge can fire).
    std::atomic<ui64> MergeCascadeInitialPartitions = 0; // partition count before the cascade started
    std::atomic<ui64> MergeCascadeLastPartitions = 0;    // partition count on the previous poll
    std::atomic<ui64> MergeCascadeStablePolls = 0;       // consecutive polls with an unchanged count
    // Partition count reported in the last printed "done" summary. 0 means
    // "no summary printed yet" (a table always has at least one partition).
    // The detector re-arms: a new "done" line is printed for every plateau
    // whose count differs from the previously reported one, so a delayed
    // split/merge tail after the first plateau is still reported.
    std::atomic<ui64> MergeCascadeLastDonePartitions = 0;

    // Single-poller rate limiting for the status and merge-cascade poll
    // loops: at most one poll per second across all worker threads. A lease
    // CAS on the last-poll timestamp picks a single winner per tick without
    // thread-local or function-local static state (which would leak across
    // generator instances). 0 means "no poll yet". The winner rolls the lease
    // back if its poll operation fails, so a failing poll does not silence
    // the timeline for a second.
    std::atomic<i64> StatusLastPollUs = 0;
    std::atomic<i64> MergeCascadeLastPollUs = 0;

    // One-shot warning for by-load modes against a table whose
    // AUTO_PARTITIONING_BY_LOAD is disabled by the init defaults: without
    // the flag no by-load splits/merges ever fire and the mode tests nothing.
    std::atomic<bool> ByLoadPrereqWarned = false;
};

} // namespace NYdbWorkload
