#pragma once

#include <ydb-cpp-sdk/type_switcher.h>

#include <util/datetime/base.h>

#include <memory>
#include <optional>
#include <string>

YDB_PROTOS_NAMESPACE {
namespace TableStats {
    class QueryStats;
}
}

namespace NYdb::inline V3 {
    class TProtoAccessor;
}

namespace NYdb::inline V3::NQuery {

class TExecStats {
    friend class NYdb::V3::TProtoAccessor;

public:
    TExecStats() = default;

    explicit TExecStats(NYdbProtos::TableStats::QueryStats&& proto);
    explicit TExecStats(const NYdbProtos::TableStats::QueryStats& proto);

    std::string ToString(bool withPlan = false) const;

    std::optional<std::string> GetPlan() const;
    std::optional<std::string> GetAst() const;
    std::optional<std::string> GetMeta() const;

    TDuration GetTotalDuration() const;
    TDuration GetTotalCpuTime() const;

private:
    const NYdbProtos::TableStats::QueryStats& GetProto() const;

private:
    class TImpl;
    std::shared_ptr<TImpl> Impl_;
};

} // namespace NYdb::V3::NQuery
