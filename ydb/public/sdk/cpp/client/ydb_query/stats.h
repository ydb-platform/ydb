#pragma once

#include "fwd.h"

#include <util/datetime/base.h>
#include <util/generic/maybe.h>
#include <util/generic/string.h>

#include <memory>

class TDuration;

namespace Ydb::TableStats {
    class QueryStats;
}

namespace NYdb::inline V2 {
    class TProtoAccessor;
}

namespace NYdb::inline V2::NQuery {

class TExecStats {
    friend class NYdb::V2::TProtoAccessor;

public:
    TExecStats() = default;

    explicit TExecStats(Ydb::TableStats::QueryStats&& proto);
    explicit TExecStats(const Ydb::TableStats::QueryStats& proto);

    TString ToString(bool withPlan = false) const;

    TMaybe<TString> GetPlan() const;
    TMaybe<TString> GetAst() const;

    TDuration GetTotalDuration() const;
    TDuration GetTotalCpuTime() const;

private:
    const Ydb::TableStats::QueryStats& GetProto() const;

private:
    class TImpl;
    std::shared_ptr<TImpl> Impl_;
};

} // namespace NYdb::NQuery
