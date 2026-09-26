#include "hnsw_index.h"

#include <ydb/core/base/kmeans_clusters.h>
#include <ydb/library/yql/udfs/common/knn/knn-defines.h>

#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wsign-compare"
#pragma clang diagnostic ignored "-Wunused-parameter"
#pragma clang diagnostic ignored "-Wunused-but-set-variable"
#include <knnquery.h>
#include <knnqueue.h>
#include <method/hnsw.h>
#include <space/space_lp.h>
#include <space/space_scalar.h>
#pragma clang diagnostic pop

#include <util/generic/hash.h>
#include <util/generic/hash_set.h>
#include <util/generic/algorithm.h>
#include <util/generic/yexception.h>
#include <util/string/builder.h>

#include <tuple>

namespace NKikimr::NDataShard {

namespace {

using Ydb::Table::VectorIndexSettings;

// Rough fixed per-node overhead of the HNSW graph, excluding friend ids.
constexpr size_t EstimatedBytesPerNodeOverhead = 256;
// HNSW construction runs in a build actor registered in the actor system's
// batch pool. Keep NMSLIB single-threaded so it does not create unmanaged
// std::threads outside that pool.
constexpr unsigned BuildThreadsPerIndex = 1;
constexpr ui32 DefaultHnswConnectivity = 16;
constexpr ui32 DefaultHnswConstructionCandidates = 200;
constexpr ui32 DefaultHnswSearchCandidates = 15;
constexpr ui64 DefaultHnswMinRows = 10000;
constexpr ui32 DefaultHnswRebuildThresholdPercent = 10;

std::unique_ptr<similarity::Space<float>> CreateSpace(VectorIndexSettings::Metric metric, TString& error) {
    switch (metric) {
        case VectorIndexSettings::DISTANCE_COSINE:
        case VectorIndexSettings::SIMILARITY_COSINE:
            return std::make_unique<similarity::SpaceCosineSimilarity<float>>();
        case VectorIndexSettings::DISTANCE_EUCLIDEAN:
            return std::make_unique<similarity::SpaceLp<float>>(2);
        case VectorIndexSettings::DISTANCE_MANHATTAN:
            return std::make_unique<similarity::SpaceLp<float>>(1);
        case VectorIndexSettings::SIMILARITY_INNER_PRODUCT:
            return std::make_unique<similarity::SpaceNegativeScalarProduct<float>>();
        default:
            error = TStringBuilder() << "Unsupported metric for HNSW: " << static_cast<int>(metric);
            return nullptr;
    }
}

// View of a wire-format float vector: raw float data plus its dimension.
// Returns an invalid view (Data == nullptr) if the bytes are not a
// well-formed FloatVector.
struct TFloatVectorView {
    const void* Data = nullptr;
    size_t Dimension = 0;

    static TFloatVectorView FromSerialized(TStringBuf serialized) {
        TFloatVectorView result;
        if (serialized.size() <= HeaderLen) {
            return result;
        }

        const ui8 formatByte = static_cast<ui8>(serialized.back());
        if (formatByte != EFormat::FloatVector) {
            return result;
        }

        const size_t dataSize = serialized.size() - HeaderLen;
        if (dataSize % sizeof(float) != 0 || dataSize == 0) {
            return result;
        }

        // similarity::Object copies raw bytes into its own aligned storage;
        // keep this as an untyped pointer because TStringBuf itself does not
        // guarantee float alignment.
        result.Data = serialized.data();
        result.Dimension = dataSize / sizeof(float);
        return result;
    }

    bool IsValid() const {
        return Data != nullptr && Dimension > 0;
    }
};

} // namespace

ui64 GetHnswMinRows(const VectorIndexSettings& settings) {
    return settings.has_hnsw_min_rows() ? settings.hnsw_min_rows() : DefaultHnswMinRows;
}

ui32 GetHnswRebuildThresholdPercent(const VectorIndexSettings& settings) {
    return settings.has_hnsw_rebuild_threshold_percent()
        ? settings.hnsw_rebuild_threshold_percent() : DefaultHnswRebuildThresholdPercent;
}

bool AreHnswIndexSettingsCompatible(
        const VectorIndexSettings& cached,
        const VectorIndexSettings& requested) {
    const auto connectivity = [](const auto& settings) {
        return settings.has_hnsw_connectivity()
            ? settings.hnsw_connectivity() : DefaultHnswConnectivity;
    };
    const auto constructionCandidates = [](const auto& settings) {
        return settings.has_hnsw_construction_candidates()
            ? settings.hnsw_construction_candidates() : DefaultHnswConstructionCandidates;
    };
    const auto searchCandidates = [](const auto& settings) {
        return settings.has_hnsw_search_candidates()
            ? settings.hnsw_search_candidates() : DefaultHnswSearchCandidates;
    };
    return cached.metric() == requested.metric()
        && cached.vector_type() == requested.vector_type()
        && cached.vector_dimension() == requested.vector_dimension()
        && connectivity(cached) == connectivity(requested)
        && constructionCandidates(cached) == constructionCandidates(requested)
        && searchCandidates(cached) == searchCandidates(requested);
}

void THnswIndexChanges::Set(TString key, TRowVersion version,
        std::optional<TString> vector, std::shared_ptr<void> reservation) {
    auto& history = Rows[key];
    for (auto& [base, count] : ChangedRowCounts) {
        if (version > base && (history.empty() || history.back().Version <= base)) {
            ++count;
        }
    }
    auto it = std::lower_bound(history.begin(), history.end(), version,
        [](const TVersion& item, TRowVersion value) { return item.Version < value; });
    TVersion item{version, std::move(vector), std::move(reservation)};
    const size_t bytes = EstimateBytes(key, item.Vector ? item.Vector->size() : 0);
    if (it != history.end() && it->Version == version) {
        EstimatedBytes -= EstimateBytes(key, it->Vector ? it->Vector->size() : 0);
        *it = std::move(item);
    } else {
        history.insert(it, std::move(item));
        ++VersionCount;
    }
    EstimatedBytes += bytes;
}

const THnswIndexChanges::TVersion* THnswIndexChanges::Find(
        const THistory& history, TRowVersion version, TRowVersion base) {
    const auto it = std::upper_bound(history.begin(), history.end(), version,
        [](TRowVersion value, const TVersion& item) { return value < item.Version; });
    if (it == history.begin() || std::prev(it)->Version <= base) {
        return nullptr;
    }
    return &*std::prev(it);
}

const THnswIndexChanges::TVersion* THnswIndexChanges::Find(
        TStringBuf key, TRowVersion version, TRowVersion base) const {
    const auto it = Rows.find(key);
    return it == Rows.end() ? nullptr : Find(it->second, version, base);
}

size_t THnswIndexChanges::CountAfter(TRowVersion base) const {
    if (auto it = ChangedRowCounts.find(base); it != ChangedRowCounts.end()) {
        return it->second;
    }
    size_t count = 0;
    for (const auto& [_, history] : Rows) {
        count += !history.empty() && history.back().Version > base;
    }
    ChangedRowCounts.emplace(base, count);
    return count;
}

void THnswIndexChanges::PruneThrough(TRowVersion base) {
    ChangedRowCounts.erase(ChangedRowCounts.begin(), ChangedRowCounts.lower_bound(base));
    for (auto it = Rows.begin(); it != Rows.end();) {
        auto& history = it->second;
        auto end = std::upper_bound(history.begin(), history.end(), base,
            [](TRowVersion value, const TVersion& item) { return value < item.Version; });
        if (end == history.begin()) {
            ++it;
            continue;
        }
        VersionCount -= end - history.begin();
        for (auto removed = history.begin(); removed != end; ++removed) {
            EstimatedBytes -= EstimateBytes(it->first, removed->Vector ? removed->Vector->size() : 0);
        }
        history.erase(history.begin(), end);
        if (history.empty()) {
            Rows.erase(it++);
        } else {
            // Release capacity as well as payload/reservations after pruning.
            THistory(history).swap(history);
            ++it;
        }
    }
}

size_t THnswIndexChanges::EstimateBytes(TStringBuf key, size_t vectorBytes) {
    return 2 * key.size() + vectorBytes + 2 * sizeof(TVersion) + 128;
}

class THnswIndex::TImpl {
public:
    TImpl(std::unique_ptr<similarity::Space<float>> space, size_t dimension)
        : Space(std::move(space))
        , Dimension(dimension)
    {}

    ~TImpl() {
        // Hnsw keeps pointers to Objects, so destroy it before their owners.
        Index.reset();
    }

    void AddVector(TString key, const void* data) {
        const auto id = static_cast<similarity::IdType>(Keys.size());
        auto obj = std::make_unique<similarity::Object>(
            id, /* label */ -1, Dimension * sizeof(float), data);
        KeyBytes += key.size();
        Keys.push_back(std::move(key));
        KeyToIndex.emplace(Keys.back(), static_cast<size_t>(id));
        Objects.push_back(obj.get());
        OwnedObjects.push_back(std::move(obj));
    }

    bool Build(const VectorIndexSettings& settings) {
        if (Objects.empty()) {
            return true; // Empty rebuilt generations still have a searchable journal.
        }

        Index = std::make_unique<similarity::Hnsw<float>>(/* PrintProgress */ false, *Space, Objects);

        Connectivity = settings.has_hnsw_connectivity()
            ? settings.hnsw_connectivity() : DefaultHnswConnectivity;
        similarity::AnyParams buildParams(std::vector<std::string>{
            "M=" + std::to_string(Connectivity),
            "efConstruction=" + std::to_string(settings.has_hnsw_construction_candidates()
                ? settings.hnsw_construction_candidates() : DefaultHnswConstructionCandidates),
            "indexThreadQty=" + std::to_string(BuildThreadsPerIndex),
        });
        Index->CreateIndex(buildParams);
        for (size_t i = 0; i < Objects.size(); ++i) {
            if (Objects[i]->id() != static_cast<similarity::IdType>(i)) {
                Index.reset();
                return false;
            }
        }
        Index->SetQueryTimeParams(similarity::AnyParams({
            "efSearch=" + std::to_string(settings.has_hnsw_search_candidates()
                ? settings.hnsw_search_candidates() : DefaultHnswSearchCandidates)}));
        return true;
    }

    THnswSearchResult Search(TStringBuf targetVector, size_t k, TRowVersion readVersion) const {
        THnswSearchResult result;
        if (!CanRead(readVersion)) {
            result.Covered = false;
            return result;
        }
        if (k == 0) {
            return result;
        }

        auto view = TFloatVectorView::FromSerialized(targetVector);
        if (!view.IsValid() || view.Dimension != Dimension) {
            return result;
        }

        std::unique_ptr<const similarity::Object> queryObj(
            new similarity::Object(-1, -1, Dimension * sizeof(float), view.Data));

        const size_t graphK = Min(k, Keys.size()) + Min(ChangeCount(), Keys.size() - Min(k, Keys.size()));
        similarity::KNNQuery<float> query(*Space, queryObj.get(), static_cast<unsigned>(Max<size_t>(graphK, 1)));
        if (Index) {
            Index->Search(&query, -1);
        }

        const similarity::KNNQueue<float>* queue = query.Result();
        std::vector<std::pair<TString, float>> reversed;
        reversed.reserve(queue->Size());
        // KNNQueue is a max-heap over distance; popping yields farthest-first.
        // Clone it since Result() is owned by the query and Pop() is destructive.
        std::unique_ptr<similarity::KNNQueue<float>> mutableQueue(queue->Clone());
        while (!mutableQueue->Empty()) {
            const float distance = mutableQueue->TopDistance();
            const similarity::Object* obj = mutableQueue->Pop();
            const size_t idx = static_cast<size_t>(obj->id());
            if (idx < Keys.size()) {
                reversed.emplace_back(Keys[idx], distance);
            }
        }

        THashMap<TString, float> merged;
        for (auto it = reversed.rbegin(); it != reversed.rend(); ++it) {
            if (!Changes->Find(it->first, readVersion, BaseVersion)) {
                merged.emplace(it->first, it->second);
            }
        }

        for (const auto& [key, history] : Changes->Rows) {
            const auto* visible = THnswIndexChanges::Find(history, readVersion, BaseVersion);
            if (!visible || !visible->Vector) {
                continue;
            }
            auto deltaView = TFloatVectorView::FromSerialized(*visible->Vector);
            if (!deltaView.IsValid() || deltaView.Dimension != Dimension) {
                continue;
            }
            similarity::Object deltaObj(-1, -1, Dimension * sizeof(float), deltaView.Data);
            merged[key] = query.DistanceObjLeft(&deltaObj);
        }

        result.Results.assign(merged.begin(), merged.end());
        Sort(result.Results, [](const auto& lhs, const auto& rhs) {
            return std::tie(lhs.second, lhs.first) < std::tie(rhs.second, rhs.first);
        });
        if (result.Results.size() > k) {
            result.Results.resize(k);
        }
        return result;
    }

    size_t Size() const {
        return Keys.size();
    }

    size_t Dim() const {
        return Dimension;
    }

    ui32 GetConnectivity() const {
        return Connectivity;
    }

    size_t GetKeyBytes() const {
        return KeyBytes;
    }

    bool GetVector(TStringBuf key, TString& result, TRowVersion readVersion) const {
        if (!CanRead(readVersion)) {
            return false;
        }
        if (const auto* visible = Changes->Find(key, readVersion, BaseVersion)) {
            if (!visible->Vector) {
                return false;
            }
            result = *visible->Vector;
            return true;
        }
        auto it = KeyToIndex.find(key);
        if (it == KeyToIndex.end()) {
            return false;
        }
        const auto* object = OwnedObjects[it->second].get();
        result.assign(object->data(), object->datalength());
        result.push_back(static_cast<char>(Format<float>));
        return true;
    }

    bool Upsert(TString key, TString vector, TRowVersion version) {
        auto view = TFloatVectorView::FromSerialized(vector);
        if (!view.IsValid() || view.Dimension != Dimension) {
            return false;
        }
        Changes->Set(std::move(key), version, std::move(vector));
        return true;
    }

    void Erase(TStringBuf key, TRowVersion version) {
        Changes->Set(TString(key), version, std::nullopt);
    }

    bool HasDelta(TStringBuf key) const {
        return Changes->Find(key, TRowVersion::Max(), BaseVersion) != nullptr;
    }

    size_t ChangeCount() const {
        return Changes->CountAfter(BaseVersion);
    }

    bool CanRead(TRowVersion version) const {
        return Changes->Valid && BaseVersion <= version && version <= UpperVersion;
    }

    TRowVersion BaseVersion = TRowVersion::Min();
    TRowVersion UpperVersion = TRowVersion::Max();
    std::shared_ptr<THnswIndexChanges> Changes = std::make_shared<THnswIndexChanges>();

private:
    std::unique_ptr<similarity::Space<float>> Space;
    size_t Dimension = 0;
    ui32 Connectivity = DefaultHnswConnectivity;
    size_t KeyBytes = 0;
    std::vector<const similarity::Object*> Objects;
    std::vector<std::unique_ptr<similarity::Object>> OwnedObjects;
    std::vector<TString> Keys; // Object::id() -> serialized primary key
    THashMap<TString, size_t> KeyToIndex;
    std::unique_ptr<similarity::Hnsw<float>> Index;
};

THnswIndex::THnswIndex(std::unique_ptr<TImpl> impl)
    : Impl(std::move(impl))
{}

THnswIndex::~THnswIndex() = default;

size_t THnswIndex::EstimateMemoryBytes(size_t rowCount, size_t dimension, ui32 connectivity,
        size_t serializedKeyBytes) {
    // NMSLIB reserves up to 2*M friend ids on level zero. Higher levels and
    // container allocations are covered by the deliberately conservative
    // fixed overhead. Saturate on overflow so an attacker cannot wrap the
    // estimate and pass the cache budget check.
    const size_t friendBytes = static_cast<size_t>(connectivity) * 2 * sizeof(similarity::IdType);
    // NMSLIB retains the source Object and copies its raw vector into the
    // contiguous optimized-search index. We no longer retain a third copy in
    // YDB wire format; GetVector() reconstructs its trailing format byte.
    const size_t bytesPerRow = 2 * dimension * sizeof(float)
        + EstimatedBytesPerNodeOverhead + friendBytes;
    if (rowCount != 0 && bytesPerRow > (Max<size_t>() - serializedKeyBytes) / rowCount) {
        return Max<size_t>();
    }
    return rowCount * bytesPerRow + serializedKeyBytes;
}

std::unique_ptr<THnswIndex> THnswIndex::Build(
    const Ydb::Table::VectorIndexSettings& settings,
    const std::vector<std::pair<TString, TString>>& keysAndVectors,
    ui64 maxMemoryBytes,
    TString& error, bool allowEmpty)
{
    if (settings.vector_type() != VectorIndexSettings::VECTOR_TYPE_FLOAT) {
        error = "HNSW index is only supported for float vectors";
        return nullptr;
    }

    TString settingsError;
    if (!NKMeans::ValidateSettingsPartial(settings, settingsError)) {
        error = TStringBuilder() << "Invalid HNSW settings: " << settingsError;
        return nullptr;
    }

    if (keysAndVectors.empty() && !allowEmpty) {
        error = "No vectors to build HNSW index from";
        return nullptr;
    }

    size_t dimension = settings.vector_dimension();
    if (dimension == 0) {
        // Auto-detect from the first well-formed vector.
        for (const auto& [key, vec] : keysAndVectors) {
            auto view = TFloatVectorView::FromSerialized(vec);
            if (view.IsValid()) {
                dimension = view.Dimension;
                break;
            }
        }
    }
    if (dimension == 0) {
        error = "Could not determine vector dimension";
        return nullptr;
    }

    if (maxMemoryBytes != 0) {
        const ui32 connectivity = settings.has_hnsw_connectivity()
            ? settings.hnsw_connectivity() : DefaultHnswConnectivity;
        size_t keyBytes = 0;
        for (const auto& [key, _] : keysAndVectors) {
            if (key.size() > Max<size_t>() - keyBytes) {
                keyBytes = Max<size_t>();
                break;
            }
            keyBytes += key.size();
        }
        const size_t estimated = EstimateMemoryBytes(
            keysAndVectors.size(), dimension, connectivity, keyBytes);
        if (estimated > maxMemoryBytes) {
            error = TStringBuilder() << "Estimated HNSW memory usage " << estimated
                << " exceeds budget " << maxMemoryBytes;
            return nullptr;
        }
    }

    auto space = CreateSpace(settings.metric(), error);
    if (!space) {
        return nullptr;
    }

    auto impl = std::make_unique<TImpl>(std::move(space), dimension);

    for (const auto& [key, vec] : keysAndVectors) {
        auto view = TFloatVectorView::FromSerialized(vec);
        if (!view.IsValid() || view.Dimension != dimension) {
            continue; // Skip malformed/mismatched rows; do not fail the whole build.
        }
        impl->AddVector(key, view.Data);
    }

    if (impl->Size() == 0 && !allowEmpty) {
        error = "No valid vectors of the expected dimension were found";
        return nullptr;
    }

    if (!impl->Build(settings)) {
        error = "Failed to build HNSW index";
        return nullptr;
    }

    return std::unique_ptr<THnswIndex>(new THnswIndex(std::move(impl)));
}

THnswSearchResult THnswIndex::Search(TStringBuf targetVector, size_t k, TRowVersion version) const {
    return Impl->Search(targetVector, k, version);
}

bool THnswIndex::GetVector(TStringBuf key, TString& result, TRowVersion version) const {
    return Impl->GetVector(key, result, version);
}

bool THnswIndex::Upsert(TString key, TString vector, TRowVersion version) {
    return Impl->Upsert(std::move(key), std::move(vector), version);
}

void THnswIndex::Erase(TStringBuf key, TRowVersion version) {
    Impl->Erase(key, version);
}

bool THnswIndex::HasDelta(TStringBuf key) const {
    return Impl->HasDelta(key);
}

bool THnswIndex::HasChanges() const {
    return Impl->ChangeCount() != 0;
}

size_t THnswIndex::ChangeCount() const {
    return Impl->ChangeCount();
}

void THnswIndex::SetSnapshot(TRowVersion base, std::shared_ptr<THnswIndexChanges> changes,
        TRowVersion upper) {
    Impl->BaseVersion = base;
    Impl->UpperVersion = upper;
    Impl->Changes = std::move(changes);
}

TRowVersion THnswIndex::GetBaseVersion() const {
    return Impl->BaseVersion;
}

bool THnswIndex::CanRead(TRowVersion version) const {
    return Impl->CanRead(version);
}

bool THnswIndex::NeedsRebuild(ui32 thresholdPercent) const {
    return static_cast<long double>(ChangeCount()) * 100
        > static_cast<long double>(Max<size_t>(Size(), 1)) * thresholdPercent;
}

bool THnswIndex::IsValidVector(TStringBuf vector, size_t dimension) {
    const auto view = TFloatVectorView::FromSerialized(vector);
    return view.IsValid() && view.Dimension == dimension;
}

size_t THnswIndex::Size() const {
    return Impl->Size();
}

size_t THnswIndex::Dimension() const {
    return Impl->Dim();
}

size_t THnswIndex::EstimatedMemoryBytes() const {
    return EstimateMemoryBytes(
        Impl->Size(), Impl->Dim(), Impl->GetConnectivity(), Impl->GetKeyBytes());
}

} // namespace NKikimr::NDataShard
