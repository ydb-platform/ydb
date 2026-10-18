#pragma once

#include <util/generic/hash.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>

#include <optional>

namespace NKikimr::NSql {

enum class EReadShape {
    Keys,
    Ranges,
};

struct TReadTarget {
    ui64 TabletId = 0;
    EReadShape Shape = EReadShape::Keys;
};

struct TCost {
    double Startup = 0;
    double Total = 0;
    double Rows = 0;
    ui32 Calls = 0;
};

struct TPartitionBound {
    ui64 TabletId = 0;
    TString From;
    TString To;
    bool ToUnbounded = false;
};

struct TKeyCompare {
    TString Column;
    bool Equal = false;
    bool Less = false;
    bool Greater = false;
    TString Value;
    TString Upper;
};

struct TTableShape {
    bool ColumnTable = false;
    TVector<TString> KeyColumns;
    TVector<TPartitionBound> Partitions;
    ui64 SchemaVersion = 0;
    double RowsPerPartition = 1;
    ui64 BytesPerRow = 1;
};

struct TPlannerConfig {
    bool EnableFullScan = false;
    double CallCost = 10000;
    double ByteCost = 1;
    double RowCost = 1;
    ui32 CacheEntries = 128;
    ui64 CacheTtlMs = 60000;
};

struct TPlan {
    TCost Cost;
    TVector<TReadTarget> Reads;
    ui64 SchemaVersion = 0;
};

struct TPlanResult {
    bool Ok = false;
    std::optional<TPlan> Plan;
    TString SqlState;
    TString Message;
};

class IPath {
public:
    virtual ~IPath() = default;
    virtual TCost Cost(const TPlannerConfig& config) const = 0;
    virtual TVector<TReadTarget> Reads() const = 0;
};

TPlanResult ChoosePlan(const TTableShape& table, const TVector<TKeyCompare>& compares, const TPlannerConfig& config);

class TPlanCache {
public:
    explicit TPlanCache(TPlannerConfig config);

    std::optional<TPlan> Lookup(const TString& database, const TString& text, const TVector<ui16>& types, ui64 schemaVersion, ui64 nowMs);
    void Store(const TString& database, const TString& text, const TVector<ui16>& types, const TPlan& plan, ui64 nowMs);
    void ForgetSchema(const TString& database, const TString& text);

private:
    struct TKey {
        TString Database;
        TString Text;
        TVector<ui16> Types;

        bool operator==(const TKey& other) const {
            return Database == other.Database && Text == other.Text && Types == other.Types;
        }
    };

    struct THash {
        size_t operator()(const TKey& key) const {
            return ComputeHash(key.Database) ^ ComputeHash(key.Text) ^ key.Types.size();
        }
    };

    struct TEntry {
        TPlan Plan;
        ui64 StoredAtMs = 0;
    };

    TPlannerConfig Config;
    THashMap<TKey, TEntry, THash> Entries;
};

} // namespace NKikimr::NSql
