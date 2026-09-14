#include "hnsw.h"
#include <ydb/public/api/protos/ydb_table.pb.h>

#include <util/generic/algorithm.h>
#include <util/generic/yexception.h>
#include <util/string/cast.h>

#include <cmath>

namespace NKikimr::NTableIndex::NHnsw {
namespace {

void PutVarint(TString& data, ui64 value) {
    while (value >= 128) {
        data.push_back(char((value & 127) | 128));
        value >>= 7;
    }
    data.push_back(char(value));
}

ui64 GetVarint(TStringBuf& data) {
    ui64 value = 0;
    for (ui32 shift = 0; shift < 64; shift += 7) {
        Y_ENSURE(!data.empty(), "Truncated HNSW adjacency");
        const ui8 byte = data.front();
        data.Skip(1);
        Y_ENSURE(shift != 63 || byte <= 1, "Overflow in HNSW adjacency");
        value |= ui64(byte & 127) << shift;
        if (!(byte & 128)) {
            Y_ENSURE(!shift || byte, "Noncanonical HNSW varint");
            return value;
        }
    }
    ythrow yexception() << "Overflow in HNSW adjacency";
}

ui64 Mix(ui64 value) {
    value = (value ^ (value >> 30)) * 0xbf58476d1ce4e5b9ULL;
    value = (value ^ (value >> 27)) * 0x94d049bb133111ebULL;
    return value ^ (value >> 31);
}

double CheckedDistance(const TDistance& distance, TStringBuf a, TStringBuf b) {
    const double value = distance(a, b);
    Y_ENSURE(std::isfinite(value), "Nonfinite HNSW distance");
    return value;
}

}

void TSettings::Validate() const {
    Y_ENSURE(M >= 2 && M <= 128, "hnsw_m must be between 2 and 128");
    Y_ENSURE(EfConstruction >= M && EfConstruction <= 4096,
        "hnsw_ef_construction must be between hnsw_m and 4096");
    Y_ENSURE(EfSearch >= 1 && EfSearch <= 4096, "hnsw_ef_search must be between 1 and 4096");
    Y_ENSURE(MaxNodes && MaxBytes, "HNSW resource limits must be positive");
}

TSettings GetSettings(const Ydb::Table::HnswSettings& settings) {
    TSettings result;
    if (settings.has_m()) result.M = settings.m();
    if (settings.has_ef_construction()) result.EfConstruction = settings.ef_construction();
    if (settings.has_ef_search()) result.EfSearch = settings.ef_search();
    if (settings.has_seed()) result.Seed = settings.seed();
    return result;
}

bool ValidateSettings(const Ydb::Table::HnswSettings& settings, TString& error) {
    try {
        GetSettings(settings).Validate();
        return true;
    } catch (const std::exception& e) {
        error = e.what();
        return false;
    }
}

bool FillSetting(Ydb::Table::HnswSettings& settings, const TString& name, const TString& value, TString& error) {
    ui32 number = 0;
    ui64 seed = 0;
    if (name == "hnsw_seed" && TryFromString(value, seed)) {
        settings.set_seed(seed);
        return true;
    }
    if (TryFromString(value, number)) {
        if (name == "hnsw_m") { settings.set_m(number); return true; }
        if (name == "hnsw_ef_construction") { settings.set_ef_construction(number); return true; }
        if (name == "hnsw_ef_search") { settings.set_ef_search(number); return true; }
    }
    error = "Invalid HNSW setting: " + name + " = " + value;
    return false;
}

TString EncodeSourceKey(TConstArrayRef<TCell> cells) {
    return TSerializedCellVec::Serialize(cells);
}

TString EncodeNeighbors(const TNeighbors& neighbors) {
    Y_ENSURE(!neighbors.empty() && neighbors.size() <= MaxLevel + 1);
    TString data;
    PutVarint(data, FormatVersion);
    PutVarint(data, neighbors.size());
    for (auto layer : neighbors) {
        Sort(layer);
        Y_ENSURE(std::adjacent_find(layer.begin(), layer.end()) == layer.end());
        PutVarint(data, layer.size());
        for (ui64 id : layer) {
            Y_ENSURE(id);
            PutVarint(data, id);
        }
    }
    return data;
}

TNeighbors DecodeNeighbors(TStringBuf data, ui32 level, ui32 m, ui64 nodeCount) {
    Y_ENSURE(level <= MaxLevel && m >= 2 && m <= 128, "Invalid HNSW metadata");
    Y_ENSURE(GetVarint(data) == FormatVersion, "Unsupported HNSW adjacency version");
    Y_ENSURE(GetVarint(data) == ui64(level) + 1, "Invalid HNSW layer count");
    TNeighbors result(level + 1);
    for (ui32 i = 0; i <= level; ++i) {
        const ui64 count = GetVarint(data);
        Y_ENSURE(count <= (i ? m : 2 * m), "Invalid HNSW degree");
        ui64 previous = 0;
        for (ui64 j = 0; j < count; ++j) {
            const ui64 id = GetVarint(data);
            Y_ENSURE(id > previous && id <= nodeCount, "Invalid HNSW neighbor id");
            result[i].push_back(id);
            previous = id;
        }
    }
    Y_ENSURE(data.empty(), "Trailing HNSW adjacency bytes");
    return result;
}

ui32 ChooseLevel(ui64 seed, ui64 parent, TStringBuf sourceKey, ui32 m) {
    Y_ENSURE(m >= 2);
    // Fixed byte-wise hash and integer geometric sampling are independent of
    // std::hash, floating-point logarithms, architecture and process RNG state.
    ui64 hash = Mix(seed ^ Mix(parent));
    for (ui8 byte : sourceKey) {
        hash = Mix(hash ^ byte);
    }
    ui32 level = 0;
    while (level < MaxLevel && hash % m == 0) {
        ++level;
        hash = Mix(hash + 0x9e3779b97f4a7c15ULL);
    }
    return level;
}

TSearch::TSearch(TMeta meta, TString target, ui32 ef, ui64 maxVisited)
    : Meta(meta)
    , Target(std::move(target))
    , Ef(ef)
    , MaxVisited(maxVisited)
    , Layer(meta.Level)
    , Entry{0, meta.EntryId}
{
    Y_ENSURE(Ef && MaxVisited && Meta.Level <= MaxLevel, "Invalid HNSW search settings");
    Y_ENSURE(Meta.Count ? Meta.EntryId && Meta.EntryId <= Meta.Count : !Meta.EntryId,
        "Invalid HNSW entry point");
    Done = !Meta.Count;
}

TSearch::EStatus TSearch::Step(const TReadNode& read, const TDistance& distance, ui32 budget) {
    while (!Done) {
        if (!budget) {
            return EStatus::NeedContinue;
        }
        if (!Initialized) {
            auto node = read(Entry.Id);
            if (!node) {
                return EStatus::NeedData;
            }
            --budget;
            Entry.Distance = CheckedDistance(distance, node->Embedding, Target);
            Initialized = true;
            if (!budget) {
                return EStatus::NeedContinue;
            }
        }
        if (!LayerStarted) {
            Visited.clear();
            Frontier = {};
            Best = {};
            Visited.insert(Entry.Id);
            Frontier.push(Entry);
            Best.push(Entry);
            LayerStarted = true;
        }
        const ui32 width = Layer ? 1 : Ef;
        if (!Expanding) {
            if (Frontier.empty() || (Best.size() >= width && Best.top().Distance < Frontier.top().Distance)) {
                if (!Layer) {
                    Done = true;
                    break;
                }
                Entry = Best.top();
                --Layer;
                LayerStarted = false;
                continue;
            }
            const ui64 id = Frontier.top().Id;
            auto node = read(id);
            if (!node) {
                return EStatus::NeedData;
            }
            --budget;
            Y_ENSURE(node->Neighbors.size() > Layer, "Missing HNSW node layer");
            Neighbors = std::move(node->Neighbors[Layer]);
            NextNeighbor = 0;
            Expanding = id;
            Frontier.pop();
        }
        while (NextNeighbor < Neighbors.size()) {
            const ui64 id = Neighbors[NextNeighbor];
            Y_ENSURE(id && id <= Meta.Count, "Invalid HNSW node id");
            if (Visited.contains(id)) {
                ++NextNeighbor;
                continue;
            }
            if (!budget) {
                return EStatus::NeedContinue;
            }
            Y_ENSURE(Visited.size() < MaxVisited, "HNSW search visited-node limit exceeded");
            auto node = read(id);
            if (!node) {
                return EStatus::NeedData;
            }
            --budget;
            Y_ENSURE(node->Neighbors.size() > Layer, "Missing HNSW neighbor layer");
            TCandidate candidate{CheckedDistance(distance, node->Embedding, Target), id};
            Visited.insert(id);
            ++NextNeighbor;
            if (Best.size() < width || candidate < Best.top()) {
                Frontier.push(candidate);
                Best.push(candidate);
                if (Best.size() > width) {
                    Best.pop();
                }
            }
        }
        Expanding = 0;
        Neighbors.clear();
    }
    return EStatus::Done;
}

TVector<TCandidate> TSearch::GetResult() const {
    Y_ENSURE(Done);
    auto heap = Best;
    TVector<TCandidate> result(heap.size());
    for (size_t i = result.size(); i; --i) {
        result[i - 1] = heap.top();
        heap.pop();
    }
    return result;
}

TBuilder::TBuilder(TSettings settings, ui64 parent, TDistance distance)
    : Settings(settings)
    , Parent(parent)
    , Distance(std::move(distance))
{
    Settings.Validate();
}

TVector<ui64> TBuilder::SelectNeighbors(TVector<TCandidate> candidates, ui32 count) const {
    Sort(candidates);
    TVector<ui64> selected;
    for (const auto& candidate : candidates) {
        bool diverse = true;
        for (ui64 id : selected) {
            if (CheckedDistance(Distance, Nodes[candidate.Id - 1].Embedding, Nodes[id - 1].Embedding) < candidate.Distance) {
                diverse = false;
                break;
            }
        }
        if (diverse) {
            selected.push_back(candidate.Id);
            if (selected.size() == count) {
                break;
            }
        }
    }
    return selected;
}

void TBuilder::Prune(ui64 id, ui32 layer, ui32 count) {
    auto& node = Nodes[id - 1];
    auto& neighbors = node.Neighbors[layer];
    if (neighbors.size() <= count) {
        return;
    }
    TVector<TCandidate> candidates;
    TVector<ui64> consecutive;
    for (ui64 other : neighbors) {
        if (!layer && (other + 1 == id || other == id + 1)) {
            consecutive.push_back(other);
        } else {
            candidates.push_back({CheckedDistance(Distance, node.Embedding, Nodes[other - 1].Embedding), other});
        }
    }
    neighbors = SelectNeighbors(std::move(candidates), count - consecutive.size());
    neighbors.insert(neighbors.end(), consecutive.begin(), consecutive.end());
}

void TBuilder::Add(TString sourceKey, TString embedding) {
    const ui32 level = ChooseLevel(Settings.Seed, Parent, sourceKey, Settings.M);
    const ui64 bytes = embedding.size() + sourceKey.size() + sizeof(TNode)
        + (level + 1) * sizeof(TVector<ui64>) + ui64(level + 2) * Settings.M * sizeof(ui64) * 2;
    Y_ENSURE(Nodes.size() < Settings.MaxNodes && bytes <= Settings.MaxBytes - Bytes,
        "HNSW leaf exceeds its build limit; increase K-means clusters or levels");
    Bytes += bytes;
    const ui64 id = Nodes.size() + 1;
    TNode node{std::move(embedding), TNeighbors(level + 1)};
    if (Meta.Count) {
        // Build each layer against the already inserted prefix. Search uses the
        // same tie-breaking and storage-independent traversal as the reader.
        for (ui32 layer = 0; layer <= Min(level, Meta.Level); ++layer) {
            TMeta meta = Meta;
            meta.Level -= layer;
            auto read = [&](ui64 other) -> std::optional<TNode> {
                auto copy = Nodes.at(other - 1);
                Y_ENSURE(copy.Neighbors.size() > layer);
                copy.Neighbors.erase(copy.Neighbors.begin(), copy.Neighbors.begin() + layer);
                return copy;
            };
            TSearch search(meta, node.Embedding, Settings.EfConstruction, Settings.MaxNodes);
            while (search.Step(read, Distance, 1024) != TSearch::EStatus::Done) {}
            node.Neighbors[layer] = SelectNeighbors(search.GetResult(), Settings.M);
        }
    }
    // Keep a reciprocal chain within layer 0. Without it, deterministic pruning
    // of equal-distance neighbors can make later duplicate vectors unreachable.
    // The chain occupies at most two of the existing 2*M neighbor slots.
    if (id > 1 && std::find(node.Neighbors[0].begin(), node.Neighbors[0].end(), id - 1) == node.Neighbors[0].end()) {
        node.Neighbors[0].push_back(id - 1);
    }
    Nodes.push_back(std::move(node));
    for (ui32 layer = 0; layer <= level; ++layer) {
        for (ui64 other : Nodes.back().Neighbors[layer]) {
            Nodes[other - 1].Neighbors[layer].push_back(id);
            Prune(other, layer, layer ? Settings.M : 2 * Settings.M);
        }
    }
    if (!Meta.Count || level > Meta.Level) {
        Meta.EntryId = id;
        Meta.Level = level;
    }
    Meta.Count = id;
}

}
