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

    void AddVector(TString key, TString vector, const void* data) {
        const auto id = static_cast<similarity::IdType>(Keys.size());
        auto obj = std::make_unique<similarity::Object>(
            id, /* label */ -1, Dimension * sizeof(float), data);
        KeyBytes += key.size();
        Keys.push_back(std::move(key));
        Vectors.push_back(std::move(vector));
        KeyToIndex.emplace(Keys.back(), static_cast<size_t>(id));
        Objects.push_back(obj.get());
        OwnedObjects.push_back(std::move(obj));
    }

    bool Build(const VectorIndexSettings& settings) {
        if (Objects.empty()) {
            return false;
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
        Index->SetQueryTimeParams(similarity::AnyParams({
            "efSearch=" + std::to_string(settings.has_hnsw_search_candidates()
                ? settings.hnsw_search_candidates() : DefaultHnswSearchCandidates)}));
        return true;
    }

    THnswSearchResult Search(TStringBuf targetVector, size_t k) const {
        THnswSearchResult result;
        if (!Index || k == 0) {
            return result;
        }

        auto view = TFloatVectorView::FromSerialized(targetVector);
        if (!view.IsValid() || view.Dimension != Dimension) {
            return result;
        }

        std::unique_ptr<const similarity::Object> queryObj(
            new similarity::Object(-1, -1, Dimension * sizeof(float), view.Data));

        const size_t graphK = DeltaVectors.empty() && ErasedKeys.empty() ? k : Keys.size();
        similarity::KNNQuery<float> query(*Space, queryObj.get(), static_cast<unsigned>(graphK));
        Index->Search(&query, -1);

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
            merged.emplace(it->first, it->second);
        }

        for (const auto& [key, vector] : DeltaVectors) {
            auto deltaView = TFloatVectorView::FromSerialized(vector);
            if (!deltaView.IsValid() || deltaView.Dimension != Dimension) {
                continue;
            }
            similarity::Object deltaObj(-1, -1, Dimension * sizeof(float), deltaView.Data);
            merged[key] = query.DistanceObjLeft(&deltaObj);
        }

        result.Results.assign(merged.begin(), merged.end());
        SortBy(result.Results, [](const auto& item) { return item.second; });
        if (DeltaVectors.empty() && ErasedKeys.empty() && result.Results.size() > k) {
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

    bool GetVector(TStringBuf key, TString& result) const {
        if (auto it = DeltaVectors.find(key); it != DeltaVectors.end()) {
            result = it->second;
            return true;
        }
        if (ErasedKeys.contains(key)) {
            return false;
        }
        auto it = KeyToIndex.find(key);
        if (it == KeyToIndex.end()) {
            return false;
        }
        result = Vectors[it->second];
        return true;
    }

    bool Upsert(TString key, TString vector) {
        auto view = TFloatVectorView::FromSerialized(vector);
        if (!view.IsValid() || view.Dimension != Dimension) {
            return false;
        }
        ErasedKeys.erase(key);
        DeltaVectors[std::move(key)] = std::move(vector);
        return true;
    }

    void Erase(TStringBuf key) {
        DeltaVectors.erase(key);
        ErasedKeys.emplace(key);
    }

    bool HasDelta(TStringBuf key) const {
        return DeltaVectors.contains(key) || ErasedKeys.contains(key);
    }

    bool HasChanges() const {
        return !DeltaVectors.empty() || !ErasedKeys.empty();
    }

private:
    std::unique_ptr<similarity::Space<float>> Space;
    size_t Dimension = 0;
    ui32 Connectivity = DefaultHnswConnectivity;
    size_t KeyBytes = 0;
    std::vector<const similarity::Object*> Objects;
    std::vector<std::unique_ptr<similarity::Object>> OwnedObjects;
    std::vector<TString> Keys; // Object::id() -> serialized primary key
    std::vector<TString> Vectors; // Object::id() -> wire-format vector
    THashMap<TString, size_t> KeyToIndex;
    THashMap<TString, TString> DeltaVectors;
    THashSet<TString> ErasedKeys;
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
    const size_t bytesPerRow = 2 * dimension * sizeof(float) + HeaderLen
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
    TString& error)
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

    if (keysAndVectors.empty()) {
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
        impl->AddVector(key, vec, view.Data);
    }

    if (impl->Size() == 0) {
        error = "No valid vectors of the expected dimension were found";
        return nullptr;
    }

    if (!impl->Build(settings)) {
        error = "Failed to build HNSW index";
        return nullptr;
    }

    return std::unique_ptr<THnswIndex>(new THnswIndex(std::move(impl)));
}

THnswSearchResult THnswIndex::Search(TStringBuf targetVector, size_t k) const {
    return Impl->Search(targetVector, k);
}

bool THnswIndex::GetVector(TStringBuf key, TString& result) const {
    return Impl->GetVector(key, result);
}

bool THnswIndex::Upsert(TString key, TString vector) {
    return Impl->Upsert(std::move(key), std::move(vector));
}

void THnswIndex::Erase(TStringBuf key) {
    Impl->Erase(key);
}

bool THnswIndex::HasDelta(TStringBuf key) const {
    return Impl->HasDelta(key);
}

bool THnswIndex::HasChanges() const {
    return Impl->HasChanges();
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
