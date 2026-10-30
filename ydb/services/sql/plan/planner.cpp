#include "plan.h"

#include <algorithm>

namespace NKikimr::NSql {

namespace {

TPlanResult Fail(const char* sqlState, TString message) {
    TPlanResult result;
    result.SqlState = sqlState;
    result.Message = std::move(message);
    return result;
}

TCost MakeCost(const TPlannerConfig& config, ui32 calls, double rows, ui64 bytesPerRow) {
    TCost cost;
    cost.Calls = calls;
    cost.Rows = rows;
    cost.Startup = calls * config.CallCost;
    cost.Total = cost.Startup + rows * config.RowCost + rows * bytesPerRow * config.ByteCost;
    return cost;
}

class TReadPath : public IPath {
public:
    TReadPath(TVector<TReadTarget> reads, double rows, ui64 bytesPerRow)
        : Targets(std::move(reads))
        , Rows(rows)
        , BytesPerRow(bytesPerRow)
    {
    }

    TCost Cost(const TPlannerConfig& config) const override {
        return MakeCost(config, static_cast<ui32>(Targets.size()), Rows, BytesPerRow);
    }

    TVector<TReadTarget> Reads() const override {
        return Targets;
    }

private:
    TVector<TReadTarget> Targets;
    double Rows = 0;
    ui64 BytesPerRow = 0;
};

bool Covers(const TPartitionBound& partition, const TString& key) {
    if (key < partition.From) {
        return false;
    }
    return partition.ToUnbounded || key < partition.To;
}

bool Overlaps(const TPartitionBound& partition, const TString& from, bool fromSet, const TString& to, bool toSet) {
    if (fromSet && !partition.ToUnbounded && partition.To <= from) {
        return false;
    }
    if (toSet && !partition.From.empty() && to <= partition.From && !(fromSet && from == partition.From)) {
        if (to <= partition.From) {
            return false;
        }
    }
    return true;
}

std::optional<TString> EqualKey(const TTableShape& table, const TVector<TKeyCompare>& compares) {
    TString key;
    for (const TString& column : table.KeyColumns) {
        const TKeyCompare* found = nullptr;
        for (const TKeyCompare& compare : compares) {
            if (compare.Column == column && compare.Equal) {
                found = &compare;
                break;
            }
        }
        if (!found) {
            return std::nullopt;
        }
        key.append(found->Value);
    }
    return key;
}

} // namespace

TPlanResult ChoosePlan(const TTableShape& table, const TVector<TKeyCompare>& compares, const TPlannerConfig& config) {
    if (table.ColumnTable) {
        return Fail("0A000", "not supported");
    }
    if (table.Partitions.empty() || table.KeyColumns.empty()) {
        return Fail("0A000", "not supported");
    }

    TVector<TReadPath> paths;
    if (const auto key = EqualKey(table, compares)) {
        for (const TPartitionBound& partition : table.Partitions) {
            if (Covers(partition, *key)) {
                paths.emplace_back(TVector<TReadTarget>{{partition.TabletId, EReadShape::Keys}}, 1, table.BytesPerRow);
                break;
            }
        }
    }

    TString lower;
    TString upper;
    bool lowerSet = false;
    bool upperSet = false;
    bool prefix = !compares.empty();
    for (size_t i = 0; i < table.KeyColumns.size() && prefix; ++i) {
        const TKeyCompare* found = nullptr;
        for (const TKeyCompare& compare : compares) {
            if (compare.Column == table.KeyColumns[i]) {
                found = &compare;
                break;
            }
        }
        if (!found) {
            prefix = i > 0;
            break;
        }
        if (found->Equal || found->Greater) {
            lower = found->Value;
            lowerSet = true;
        }
        if (found->Equal || found->Less) {
            if (found->Equal) {
                upper = found->Value + '\xff';
            } else {
                upper = found->Upper.empty() ? found->Value : found->Upper;
            }
            upperSet = true;
        }
        if (!found->Equal) {
            break;
        }
    }
    if (lowerSet || upperSet) {
        TVector<TReadTarget> reads;
        for (const TPartitionBound& partition : table.Partitions) {
            if (Overlaps(partition, lower, lowerSet, upper, upperSet)) {
                reads.push_back({partition.TabletId, EReadShape::Ranges});
            }
        }
        if (!reads.empty()) {
            paths.emplace_back(std::move(reads), reads.size() * table.RowsPerPartition, table.BytesPerRow);
        }
    }

    if (config.EnableFullScan) {
        TVector<TReadTarget> reads;
        for (const TPartitionBound& partition : table.Partitions) {
            reads.push_back({partition.TabletId, EReadShape::Ranges});
        }
        paths.emplace_back(std::move(reads), table.Partitions.size() * table.RowsPerPartition * 10, table.BytesPerRow);
    }

    if (paths.empty()) {
        return Fail("0A000", "not supported");
    }
    const TReadPath* best = &paths[0];
    for (const TReadPath& path : paths) {
        if (path.Cost(config).Total < best->Cost(config).Total) {
            best = &path;
        }
    }
    TPlanResult result;
    result.Ok = true;
    result.Plan = TPlan{best->Cost(config), best->Reads(), table.SchemaVersion};
    return result;
}

TPlanCache::TPlanCache(TPlannerConfig config)
    : Config(std::move(config))
{
}

std::optional<TPlan> TPlanCache::Lookup(const TString& database, const TString& text, const TVector<ui16>& types, ui64 schemaVersion, ui64 nowMs) {
    const auto it = Entries.find(TKey{database, text, types});
    if (it == Entries.end()) {
        return std::nullopt;
    }
    if (it->second.Plan.SchemaVersion != schemaVersion || nowMs - it->second.StoredAtMs > Config.CacheTtlMs) {
        Entries.erase(it);
        return std::nullopt;
    }
    return it->second.Plan;
}

void TPlanCache::Store(const TString& database, const TString& text, const TVector<ui16>& types, const TPlan& plan, ui64 nowMs) {
    if (Config.CacheEntries == 0) {
        return;
    }
    if (Entries.size() >= Config.CacheEntries) {
        Entries.clear();
    }
    Entries[TKey{database, text, types}] = TEntry{plan, nowMs};
}

void TPlanCache::ForgetSchema(const TString& database, const TString& text) {
    TVector<TKey> doomed;
    for (const auto& item : Entries) {
        if (item.first.Database == database && item.first.Text == text) {
            doomed.push_back(item.first);
        }
    }
    for (const TKey& key : doomed) {
        Entries.erase(key);
    }
}

} // namespace NKikimr::NSql
