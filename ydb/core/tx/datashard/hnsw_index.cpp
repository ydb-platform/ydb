#include "hnsw_index.h"

#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wunused-parameter"
#include <usearch/index_dense.hpp>
#pragma clang diagnostic pop

#include <util/generic/yexception.h>
#include <ydb/library/actors/core/log.h>

#include <cstring>
#include <vector>
#include <thread>

namespace NKikimr::NDataShard {

using namespace unum::usearch;

namespace {
    // Format byte values (from knn-defines.h)
    enum EFormat : ui8 {
        FloatVector = 1,
        Uint8Vector = 2,
        Int8Vector = 3,
        BitVector = 10,
    };

    // Unified helper: zero-copy for FloatVector, deserialized vector for other formats
    struct TFloatVectorView {
        const float* Data = nullptr;
        size_t Dimension = 0;
        std::vector<float> OwnedData;  // Only used for non-FloatVector formats

        static TFloatVectorView FromSerialized(const TString& serialized) {
            TFloatVectorView result;
            if (serialized.size() < 2) {
                return result;
            }

            ui8 formatByte = static_cast<ui8>(serialized.back());
            size_t dataSize = serialized.size() - 1;  // Exclude format byte

            if (formatByte == FloatVector) {
                // Zero-copy path for FloatVector
                if (dataSize % sizeof(float) == 0) {
                    result.Dimension = dataSize / sizeof(float);
                    result.Data = reinterpret_cast<const float*>(serialized.data());
                }
            } else {
                // Deserialize other formats
                switch (formatByte) {
                    case Uint8Vector: {
                        result.OwnedData.reserve(dataSize);
                        for (size_t i = 0; i < dataSize; ++i) {
                            result.OwnedData.push_back(static_cast<float>(static_cast<ui8>(serialized[i])));
                        }
                        break;
                    }
                    case Int8Vector: {
                        result.OwnedData.reserve(dataSize);
                        for (size_t i = 0; i < dataSize; ++i) {
                            result.OwnedData.push_back(static_cast<float>(static_cast<i8>(serialized[i])));
                        }
                        break;
                    }
                    default:
                        return result;  // Unsupported format
                }
                if (!result.OwnedData.empty()) {
                    result.Dimension = result.OwnedData.size();
                    result.Data = result.OwnedData.data();
                }
            }

            return result;
        }

        bool IsValid() const {
            return Data != nullptr && Dimension > 0;
        }
    };
} // anonymous namespace

// Implementation class using usearch
class THnswIndexImpl {
public:
    using IndexType = index_dense_gt<>;

    THnswIndexImpl() = default;

    bool Build(const std::vector<std::pair<ui64, TString>>& vectors) {
        if (vectors.empty()) {
            return false;
        }

        // Determine dimension from first vector
        size_t floatDimension = 0;
        for (const auto& [key, vectorData] : vectors) {
            auto view = TFloatVectorView::FromSerialized(vectorData);
            if (view.IsValid()) {
                floatDimension = view.Dimension;
                break;
            }
        }

        if (floatDimension == 0) {
            return false;
        }

        Dimension = floatDimension;

        // Create metric for cosine distance with float vectors
        metric_punned_t metric(floatDimension, metric_kind_t::cos_k, scalar_kind_t::f32_k);

        // Configure index
        index_dense_config_t config;
        config.connectivity = 24;  // M parameter in HNSW
        config.expansion_add = 200;
        config.expansion_search = 15;

        // Create index
        auto result = IndexType::make(metric, config);
        if (!result) {
            return false;
        }
        Index = std::move(result.index);

        // Setup multi-threaded executor using all available cores
        std::size_t executor_threads = std::thread::hardware_concurrency();
        if (executor_threads == 0) {
            executor_threads = 1;  // Fallback if hardware_concurrency() returns 0
        }
        executor_default_t executor(executor_threads);

        // Reserve capacity with executor size for parallel operations
        index_limits_t limits{vectors.size(), executor.size()};
        if (!Index.try_reserve(limits)) {
            return false;
        }

        // Add all vectors in parallel (zero-copy for FloatVector, deserialized for others)
        executor.fixed(vectors.size(), [&](std::size_t thread, std::size_t task) {
            const auto& [key, vectorData] = vectors[task];
            auto view = TFloatVectorView::FromSerialized(vectorData);
            if (!view.IsValid() || view.Dimension != floatDimension) {
                return;  // Skip invalid vectors
            }
            auto addResult = Index.add(key, view.Data, thread);
            if (!addResult) {
                // Log error but continue
            }
        });

        Ready = true;
        return true;
    }

    THnswSearchResult Search(const TString& targetVector, size_t k) const {
        THnswSearchResult result;

        if (!Ready) {
            return result;
        }

        auto view = TFloatVectorView::FromSerialized(targetVector);
        if (!view.IsValid() || view.Dimension != Dimension) {
            return result;
        }

        auto searchResult = Index.search(view.Data, k);
        if (!searchResult) {
            return result;
        }

        result.Results.reserve(searchResult.size());
        for (size_t i = 0; i < searchResult.size(); ++i) {
            auto match = searchResult[i];
            result.Results.emplace_back(match.member.key, match.distance);
        }

        return result;
    }

    bool IsReady() const {
        return Ready;
    }

    size_t Size() const {
        return Ready ? Index.size() : 0;
    }

private:
    mutable IndexType Index;
    size_t Dimension = 0;
    bool Ready = false;
};

// THnswIndex implementation

THnswIndex::THnswIndex()
    : Impl(std::make_unique<THnswIndexImpl>())
{}

THnswIndex::~THnswIndex() = default;

THnswIndex::THnswIndex(THnswIndex&&) noexcept = default;
THnswIndex& THnswIndex::operator=(THnswIndex&&) noexcept = default;

bool THnswIndex::Build(const std::vector<std::pair<ui64, TString>>& vectors) {
    return Impl->Build(vectors);
}

THnswSearchResult THnswIndex::Search(const TString& targetVector, size_t k) const {
    return Impl->Search(targetVector, k);
}

bool THnswIndex::IsReady() const {
    return Impl->IsReady();
}

size_t THnswIndex::Size() const {
    return Impl->Size();
}

// TNodeHnswIndexCache implementation (singleton)

TNodeHnswIndexCache& TNodeHnswIndexCache::Instance() {
    static TNodeHnswIndexCache instance;
    return instance;
}

std::shared_ptr<THnswIndex> TNodeHnswIndexCache::RegisterIndex(
    ui64 tabletId, ui64 tableId, const TString& columnName,
    std::unique_ptr<THnswIndex> index) {

    std::lock_guard<std::mutex> lock(Mutex);
    TNodeHnswIndexKey key{tabletId, tableId, columnName};

    auto sharedIndex = std::shared_ptr<THnswIndex>(std::move(index));
    size_t indexSize = sharedIndex->Size();
    Indexes[key] = sharedIndex;

    LOG_INFO_S(*NActors::TlsActivationContext, NKikimrServices::TX_DATASHARD,
               "HNSW: registered index tabletId=" << tabletId << " tableId=" << tableId
               << " column=" << columnName << " size=" << indexSize);

    return sharedIndex;
}

std::shared_ptr<THnswIndex> TNodeHnswIndexCache::GetIndex(
    ui64 tabletId, ui64 tableId, const TString& columnName, size_t expectedSize) const {

    std::lock_guard<std::mutex> lock(Mutex);
    TNodeHnswIndexKey key{tabletId, tableId, columnName};

    auto it = Indexes.find(key);
    if (it != Indexes.end() && it->second && it->second->IsReady()) {
        size_t cachedSize = it->second->Size();
        // If expectedSize is provided (non-zero), validate the index size matches
        // This detects stale indexes after data changes
        if (expectedSize > 0 && cachedSize != expectedSize) {
            LOG_INFO_S(*NActors::TlsActivationContext, NKikimrServices::TX_DATASHARD,
                       "HNSW: cache size mismatch tabletId=" << tabletId << " tableId=" << tableId
                       << " column=" << columnName << " cached=" << cachedSize << " expected=" << expectedSize);
            return nullptr;  // Size mismatch, index is stale
        }
        LOG_INFO_S(*NActors::TlsActivationContext, NKikimrServices::TX_DATASHARD,
                   "HNSW: cache hit tabletId=" << tabletId << " tableId=" << tableId
                   << " column=" << columnName << " size=" << cachedSize);
        return it->second;
    }
    LOG_INFO_S(*NActors::TlsActivationContext, NKikimrServices::TX_DATASHARD,
               "HNSW: cache miss tabletId=" << tabletId << " tableId=" << tableId
               << " column=" << columnName);
    return nullptr;
}

bool TNodeHnswIndexCache::HasIndex(ui64 tabletId, ui64 tableId, const TString& columnName, size_t expectedSize) const {
    return GetIndex(tabletId, tableId, columnName, expectedSize) != nullptr;
}

void TNodeHnswIndexCache::RemoveIndex(ui64 tabletId, ui64 tableId, const TString& columnName) {
    std::lock_guard<std::mutex> lock(Mutex);
    TNodeHnswIndexKey key{tabletId, tableId, columnName};
    Indexes.erase(key);
}

void TNodeHnswIndexCache::RemoveTabletIndexes(ui64 tabletId) {
    std::lock_guard<std::mutex> lock(Mutex);
    // Collect keys to remove (THashMap::erase returns void)
    std::vector<TNodeHnswIndexKey> keysToRemove;
    for (const auto& [key, _] : Indexes) {
        if (key.TabletId == tabletId) {
            keysToRemove.push_back(key);
        }
    }
    for (const auto& key : keysToRemove) {
        Indexes.erase(key);
    }
}

ui64 TNodeHnswIndexCache::GetCacheHits() const {
    return GlobalCacheHits.load(std::memory_order_relaxed);
}

ui64 TNodeHnswIndexCache::GetCacheMisses() const {
    return GlobalCacheMisses.load(std::memory_order_relaxed);
}

void TNodeHnswIndexCache::IncrementCacheHit() {
    GlobalCacheHits.fetch_add(1, std::memory_order_relaxed);
}

void TNodeHnswIndexCache::IncrementCacheMiss() {
    GlobalCacheMisses.fetch_add(1, std::memory_order_relaxed);
}

bool TNodeHnswIndexCache::IsBuildInProgress(ui64 tabletId, ui64 tableId, const TString& columnName) const {
    std::lock_guard lock(Mutex);
    TNodeHnswIndexKey key{tabletId, tableId, columnName};
    return BuildingIndexes.contains(key);
}

bool TNodeHnswIndexCache::TryStartBuild(ui64 tabletId, ui64 tableId, const TString& columnName) {
    std::lock_guard lock(Mutex);
    TNodeHnswIndexKey key{tabletId, tableId, columnName};

    // Check if already exists in cache - no need to build
    if (Indexes.contains(key)) {
        return false;
    }

    // Check if another builder is already working on this index
    if (BuildingIndexes.contains(key)) {
        return false;  // Another build in progress, caller should wait
    }

    // Mark as building and return true (caller should build)
    BuildingIndexes.insert(key);
    return true;
}

void TNodeHnswIndexCache::FinishBuild(ui64 tabletId, ui64 tableId, const TString& columnName) {
    std::lock_guard lock(Mutex);
    TNodeHnswIndexKey key{tabletId, tableId, columnName};
    BuildingIndexes.erase(key);
}

// THnswIndexManager implementation

bool THnswIndexManager::IsVectorColumn(const TString& columnName) {
    return columnName == "emb" || columnName == "embedding" || columnName == "vector";
}

bool THnswIndexManager::BuildIndex(ui64 tableId, const TString& columnName,
                                    const std::vector<std::pair<ui64, TString>>& vectors) {
    THnswIndexKey key{tableId, columnName};

    auto index = std::make_unique<THnswIndex>();
    if (!index->Build(vectors)) {
        return false;
    }

    // Register in node cache for sharing with followers, and store locally
    if (TabletId != 0) {
        Indexes[key] = TNodeHnswIndexCache::Instance().RegisterIndex(
            TabletId, tableId, columnName, std::move(index));
    } else {
        // Fallback: no tablet ID set, store locally only
        Indexes[key] = std::shared_ptr<THnswIndex>(std::move(index));
    }
    return true;
}

bool THnswIndexManager::TryGetFromCache(ui64 tableId, const TString& columnName, size_t expectedSize) {
    if (TabletId == 0) {
        ++CacheMisses;
        TNodeHnswIndexCache::Instance().IncrementCacheMiss();
        return false;
    }

    auto cachedIndex = TNodeHnswIndexCache::Instance().GetIndex(TabletId, tableId, columnName, expectedSize);
    if (cachedIndex) {
        THnswIndexKey key{tableId, columnName};
        Indexes[key] = cachedIndex;
        ++CacheHits;
        TNodeHnswIndexCache::Instance().IncrementCacheHit();
        return true;
    }
    ++CacheMisses;
    TNodeHnswIndexCache::Instance().IncrementCacheMiss();
    return false;
}

const THnswIndex* THnswIndexManager::GetIndex(ui64 tableId, const TString& columnName) const {
    THnswIndexKey key{tableId, columnName};
    auto it = Indexes.find(key);
    if (it != Indexes.end() && it->second && it->second->IsReady()) {
        return it->second.get();
    }
    return nullptr;
}

bool THnswIndexManager::HasIndex(ui64 tableId, const TString& columnName) const {
    return GetIndex(tableId, columnName) != nullptr;
}

void THnswIndexManager::Clear() {
    Indexes.clear();
}

void THnswIndexManager::ClearAndRemoveFromCache() {
    if (TabletId != 0) {
        TNodeHnswIndexCache::Instance().RemoveTabletIndexes(TabletId);
    }
    Indexes.clear();
}

std::vector<THnswIndexManager::TIndexInfo> THnswIndexManager::GetAllIndexesInfo() const {
    std::vector<TIndexInfo> result;
    result.reserve(Indexes.size());
    for (const auto& [key, index] : Indexes) {
        TIndexInfo info;
        info.TableId = key.TableId;
        info.ColumnName = key.ColumnName;
        info.IndexSize = index ? index->Size() : 0;
        info.IsReady = index ? index->IsReady() : false;
        result.push_back(info);
    }
    return result;
}

} // namespace NKikimr::NDataShard

