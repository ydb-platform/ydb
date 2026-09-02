#pragma once

#include <util/datetime/base.h>

#include <memory>
#include <optional>
#include <string>
#include <vector>

namespace Ydb::TableStats {
    class CompilationStats;
    class OperationStats;
    class QueryPhaseStats;
    class QueryStats;
    class TableAccessStats;
}

namespace NYdb::inline Dev {
    class TProtoAccessor;
}

namespace NYdb::inline Dev::NQuery {

//! Row and byte counts for one kind of table operation.
class TOperationStats {
public:
    //! Constructs operation statistics from their wire representation.
    explicit TOperationStats(const Ydb::TableStats::OperationStats& proto);

    //! Returns the number of affected rows.
    uint64_t GetRows() const;
    //! Returns the number of affected bytes.
    uint64_t GetBytes() const;

private:
    uint64_t Rows_ = 0;
    uint64_t Bytes_ = 0;
};

//! Statistics for all operations performed on one table.
class TTableAccessStats {
public:
    //! Constructs table access statistics from their wire representation.
    explicit TTableAccessStats(const Ydb::TableStats::TableAccessStats& proto);

    //! Returns the table name.
    const std::string& GetName() const;
    //! Returns read operation statistics.
    const TOperationStats& GetReads() const;
    //! Returns update, insert, upsert, and replace operation statistics.
    const TOperationStats& GetUpdates() const;
    //! Returns delete operation statistics.
    const TOperationStats& GetDeletes() const;
    //! Returns the number of accessed table partitions.
    uint64_t GetPartitionsCount() const;

private:
    std::string Name_;
    TOperationStats Reads_;
    TOperationStats Updates_;
    TOperationStats Deletes_;
    uint64_t PartitionsCount_ = 0;
};

//! Statistics for one query execution phase.
class TQueryPhaseStats {
public:
    //! Constructs query phase statistics from their wire representation.
    explicit TQueryPhaseStats(const Ydb::TableStats::QueryPhaseStats& proto);

    //! Returns the phase wall-clock duration in microseconds.
    uint64_t GetDurationUs() const;
    //! Returns the phase wall-clock duration.
    TDuration GetDuration() const;
    //! Returns the phase CPU time in microseconds.
    uint64_t GetCpuTimeUs() const;
    //! Returns the phase CPU time.
    TDuration GetCpuTime() const;
    //! Returns the number of shards affected by the phase.
    uint64_t GetAffectedShards() const;
    //! Returns whether this was a literal execution phase.
    bool IsLiteralPhase() const;
    //! Returns per-table access statistics for this phase.
    const std::vector<TTableAccessStats>& GetTableAccess() const;

private:
    uint64_t DurationUs_ = 0;
    uint64_t CpuTimeUs_ = 0;
    uint64_t AffectedShards_ = 0;
    bool LiteralPhase_ = false;
    std::vector<TTableAccessStats> TableAccess_;
};

//! Query compilation statistics.
class TCompilationStats {
public:
    //! Constructs compilation statistics from their wire representation.
    explicit TCompilationStats(const Ydb::TableStats::CompilationStats& proto);

    //! Returns whether the compiled query was taken from cache.
    bool IsFromCache() const;
    //! Returns the compilation wall-clock duration in microseconds.
    uint64_t GetDurationUs() const;
    //! Returns the compilation wall-clock duration.
    TDuration GetDuration() const;
    //! Returns compilation CPU time in microseconds.
    uint64_t GetCpuTimeUs() const;
    //! Returns compilation CPU time.
    TDuration GetCpuTime() const;

private:
    bool FromCache_ = false;
    uint64_t DurationUs_ = 0;
    uint64_t CpuTimeUs_ = 0;
};

//! Execution statistics returned for a query or script.
class TExecStats {
    friend class NYdb::TProtoAccessor;

public:
    //! Constructs an uninitialized statistics holder used for response metadata.
    //! Accessors require the holder to be populated by the SDK.
    TExecStats() = default;

    //! Constructs execution statistics by taking ownership of their wire representation.
    explicit TExecStats(Ydb::TableStats::QueryStats&& proto);
    //! Constructs execution statistics by copying their wire representation.
    explicit TExecStats(const Ydb::TableStats::QueryStats& proto);

    //! Returns the protobuf text representation, optionally including plan, AST, and query metadata.
    std::string ToString(bool withPlan = false) const;

    //! Returns CPU time spent by the query process in microseconds.
    uint64_t GetProcessCpuTimeUs() const;
    //! Returns total query duration in microseconds.
    uint64_t GetTotalDurationUs() const;
    //! Returns total query CPU time in microseconds.
    uint64_t GetTotalCpuTimeUs() const;
    //! Returns the query plan when it was collected.
    std::optional<std::string> GetPlan() const;
    //! Returns the query abstract syntax tree when it was collected.
    std::optional<std::string> GetAst() const;
    //! Returns additional query compilation metadata when it was collected.
    std::optional<std::string> GetMeta() const;

    //! Returns CPU time spent by the query process.
    TDuration GetProcessCpuTime() const;
    //! Returns total query duration.
    TDuration GetTotalDuration() const;
    //! Returns total query CPU time.
    TDuration GetTotalCpuTime() const;

    //! Returns statistics for every query execution phase.
    std::vector<TQueryPhaseStats> GetQueryPhases() const;
    //! Returns compilation statistics when they were collected.
    std::optional<TCompilationStats> GetCompilation() const;

private:
    const Ydb::TableStats::QueryStats& GetProto() const;

private:
    class TImpl;
    std::shared_ptr<TImpl> Impl_;
};

} // namespace NYdb::NQuery
