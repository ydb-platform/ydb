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

class TOperationStats {
public:
    explicit TOperationStats(const Ydb::TableStats::OperationStats& proto);

    uint64_t GetRows() const;
    uint64_t GetBytes() const;

private:
    uint64_t Rows_ = 0;
    uint64_t Bytes_ = 0;
};

class TTableAccessStats {
public:
    explicit TTableAccessStats(const Ydb::TableStats::TableAccessStats& proto);

    const std::string& GetName() const;
    const TOperationStats& GetReads() const;
    const TOperationStats& GetUpdates() const;
    const TOperationStats& GetDeletes() const;
    uint64_t GetPartitionsCount() const;

private:
    std::string Name_;
    TOperationStats Reads_;
    TOperationStats Updates_;
    TOperationStats Deletes_;
    uint64_t PartitionsCount_ = 0;
};

class TQueryPhaseStats {
public:
    explicit TQueryPhaseStats(const Ydb::TableStats::QueryPhaseStats& proto);

    uint64_t GetDurationUs() const;
    TDuration GetDuration() const;
    uint64_t GetCpuTimeUs() const;
    TDuration GetCpuTime() const;
    uint64_t GetAffectedShards() const;
    bool IsLiteralPhase() const;
    const std::vector<TTableAccessStats>& GetTableAccess() const;

private:
    uint64_t DurationUs_ = 0;
    uint64_t CpuTimeUs_ = 0;
    uint64_t AffectedShards_ = 0;
    bool LiteralPhase_ = false;
    std::vector<TTableAccessStats> TableAccess_;
};

class TCompilationStats {
public:
    explicit TCompilationStats(const Ydb::TableStats::CompilationStats& proto);

    bool IsFromCache() const;
    uint64_t GetDurationUs() const;
    TDuration GetDuration() const;
    uint64_t GetCpuTimeUs() const;
    TDuration GetCpuTime() const;

private:
    bool FromCache_ = false;
    uint64_t DurationUs_ = 0;
    uint64_t CpuTimeUs_ = 0;
};

class TExecStats {
    friend class NYdb::TProtoAccessor;

public:
    TExecStats() = default;

    explicit TExecStats(Ydb::TableStats::QueryStats&& proto);
    explicit TExecStats(const Ydb::TableStats::QueryStats& proto);

    std::string ToString(bool withPlan = false) const;

    uint64_t GetProcessCpuTimeUs() const;
    uint64_t GetTotalDurationUs() const;
    uint64_t GetTotalCpuTimeUs() const;
    std::optional<std::string> GetPlan() const;
    std::optional<std::string> GetAst() const;
    std::optional<std::string> GetMeta() const;

    TDuration GetProcessCpuTime() const;
    TDuration GetTotalDuration() const;
    TDuration GetTotalCpuTime() const;

    std::vector<TQueryPhaseStats> GetQueryPhases() const;
    std::optional<TCompilationStats> GetCompilation() const;

private:
    const Ydb::TableStats::QueryStats& GetProto() const;

private:
    class TImpl;
    std::shared_ptr<TImpl> Impl_;
};

} // namespace NYdb::NQuery
