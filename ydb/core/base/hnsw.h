#pragma once

#include <ydb/core/scheme/scheme_tablecell.h>

#include <util/generic/hash_set.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>

#include <functional>
#include <optional>
#include <queue>

namespace Ydb::Table { class HnswSettings; }

namespace NKikimr::NTableIndex::NHnsw {

inline constexpr const char* HnswTable = "indexImplHnswTable";
inline constexpr const char* BuildTable = "indexImplPostingTablehnswbuild";
inline constexpr const char* RecordTypeColumn = "__ydb_record_type";
inline constexpr const char* NodeIdColumn = "__ydb_node_id";
inline constexpr const char* KeyColumn = "__ydb_key";
inline constexpr const char* SourceKeyColumn = "__ydb_source_key";
inline constexpr const char* NeighborsColumn = "__ydb_neighbors";
inline constexpr const char* NodeLevelColumn = "__ydb_node_level";
inline constexpr const char* EntryIdColumn = "__ydb_entry_id";
inline constexpr const char* MaxLevelColumn = "__ydb_max_level";
inline constexpr const char* BaseCountColumn = "__ydb_base_count";
inline constexpr const char* FormatVersionColumn = "__ydb_format_version";
inline constexpr ui8 MetaRecord = 0;
inline constexpr ui8 NodeRecord = 1;
inline constexpr ui32 FormatVersion = 1;
inline constexpr ui32 MaxLevel = 32;
inline constexpr int LevelTablePosition = 0;
inline constexpr int HnswTablePosition = 1;
inline constexpr int PrefixTablePosition = 2;

struct TSettings {
    ui32 M = 16;
    ui32 EfConstruction = 200;
    ui32 EfSearch = 64;
    ui64 Seed = 0;
    ui64 MaxNodes = 1'000'000;
    ui64 MaxBytes = 256 * 1024 * 1024;

    void Validate() const;
};

TSettings GetSettings(const Ydb::Table::HnswSettings& settings);
bool ValidateSettings(const Ydb::Table::HnswSettings& settings, TString& error);
bool FillSetting(Ydb::Table::HnswSettings& settings, const TString& name, const TString& value, TString& error);

using TNeighbors = TVector<TVector<ui64>>;
using TDistance = std::function<double(TStringBuf, TStringBuf)>;

// Version 1 fixes TSerializedCellVec's byte layout as the source-key codec.
TString EncodeSourceKey(TConstArrayRef<TCell> cells);
TString EncodeNeighbors(const TNeighbors& neighbors);
TNeighbors DecodeNeighbors(TStringBuf data, ui32 level, ui32 m, ui64 nodeCount);
ui32 ChooseLevel(ui64 seed, ui64 parent, TStringBuf sourceKey, ui32 m);

struct TNode {
    TString Embedding;
    TNeighbors Neighbors;
};

struct TMeta {
    ui64 EntryId = 0;
    ui32 Level = 0;
    ui64 Count = 0;
};

struct TCandidate {
    double Distance = 0;
    ui64 Id = 0;

    bool operator<(const TCandidate& rhs) const {
        return Distance < rhs.Distance || (Distance == rhs.Distance && Id < rhs.Id);
    }
    bool operator>(const TCandidate& rhs) const { return rhs < *this; }
};

// The reader returns nullopt on a page fault. Nodes are owned by this invocation;
// no storage pointers or transaction objects survive Step().
class TSearch {
public:
    using TReadNode = std::function<std::optional<TNode>(ui64)>;
    enum class EStatus { Done, NeedData, NeedContinue };

    TSearch(TMeta meta, TString target, ui32 ef, ui64 maxVisited = 1'000'000);
    EStatus Step(const TReadNode& read, const TDistance& distance, ui32 budget);
    TVector<TCandidate> GetResult() const;

private:
    TMeta Meta;
    TString Target;
    ui32 Ef;
    ui64 MaxVisited;
    ui32 Layer;
    TCandidate Entry;
    bool Initialized = false;
    bool LayerStarted = false;
    bool Done = false;
    ui64 Expanding = 0;
    TVector<ui64> Neighbors;
    size_t NextNeighbor = 0;
    THashSet<ui64> Visited;
    std::priority_queue<TCandidate, TVector<TCandidate>, std::greater<TCandidate>> Frontier;
    std::priority_queue<TCandidate, TVector<TCandidate>> Best;
};

// One bounded leaf at a time, in source primary-key order. The caller owns the
// typed source columns and uploads META/NODE rows after the graph is complete.
class TBuilder {
public:
    TBuilder(TSettings settings, ui64 parent, TDistance distance);
    void Add(TString sourceKey, TString embedding);
    const TVector<TNode>& GetNodes() const { return Nodes; }
    const TMeta& GetMeta() const { return Meta; }

private:
    TVector<ui64> SelectNeighbors(TVector<TCandidate> candidates, ui32 count) const;
    void Prune(ui64 id, ui32 layer, ui32 count);

    TSettings Settings;
    ui64 Parent;
    TDistance Distance;
    TMeta Meta;
    TVector<TNode> Nodes;
    ui64 Bytes = 0;
};

}
