#pragma once

#include <util/datetime/base.h>

#include <memory>
#include <optional>
#include <string>

namespace Ydb::TableStats {
    class QueryStats;
}

namespace NYdb::inline Dev {
    class TProtoAccessor;
}

namespace NYdb::inline Dev::NQuery {

class TExecStats {
    friend class NYdb::TProtoAccessor;

public:
    TExecStats() = default;

    explicit TExecStats(Ydb::TableStats::QueryStats&& proto);
    explicit TExecStats(const Ydb::TableStats::QueryStats& proto);

    std::string ToString(bool withPlan = false) const;

    std::optional<std::string> GetPlan() const;
    std::optional<std::string> GetAst() const;
    std::optional<std::string> GetMeta() const;

    TDuration GetTotalDuration() const;
    TDuration GetTotalCpuTime() const;

private:
    const Ydb::TableStats::QueryStats& GetProto() const;

private:
    class TImpl;
    std::shared_ptr<TImpl> Impl_;
};

} // namespace NYdb::NQuery
