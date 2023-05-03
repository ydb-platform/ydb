#pragma once

#include <util/generic/maybe.h>
#include <util/generic/string.h>

#include <memory>

class TDuration;

namespace Ydb {
    namespace TableStats {
        class QueryStats;
    }

    namespace Table {
        class QueryStatsCollection;
    }
}

namespace NYdb {

class TProtoAccessor;

namespace NScripting {

class TScriptingClient;
class TYqlResultPartIterator;

} // namespace NScripting

namespace NTable {

enum class ECollectQueryStatsMode {
    None = 0,  // Stats collection is disabled
    Basic = 1, // Aggregated stats of reads, updates and deletes per table
    Full = 2,   // Add per-stage execution profile and query plan on top of Basic mode
    Profile = 3   // Detailed execution stats including stats for individual tasks and channels
};

class TQueryStats {
    friend class TTableClient;
    friend class NYdb::TProtoAccessor;
    friend class NYdb::NScripting::TScriptingClient;
    friend class NYdb::NScripting::TYqlResultPartIterator;
    friend class TScanQueryPartIterator;

public:
    TQueryStats(Ydb::TableStats::QueryStats&& proto);
    TString ToString(bool withPlan = false) const;
    TMaybe<TString> GetPlan() const;
    TDuration GetTotalDuration() const;
    TDuration GetTotalCpuTime() const;

private:
    explicit TQueryStats(const Ydb::TableStats::QueryStats& proto);
    const Ydb::TableStats::QueryStats& GetProto() const;

private:
    class TImpl;
    std::shared_ptr<TImpl> Impl_;
};

} // namespace NTable
} // namespace NYdb
