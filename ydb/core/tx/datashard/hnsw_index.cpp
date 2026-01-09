#include "hnsw_index.h"

#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wunused-parameter"
#include <usearch/index_dense.hpp>
#pragma clang diagnostic pop

#include <util/generic/yexception.h>

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

    // Convert serialized vector (with format byte at end) to float vector for HNSW
    std::vector<float> DeserializeToFloatVector(const TString& serialized) {
        if (serialized.size() < 2) {
            return {};
        }

        ui8 formatByte = static_cast<ui8>(serialized.back());
        size_t dataSize = serialized.size() - 1;  // Exclude format byte

        std::vector<float> result;

        switch (formatByte) {
            case FloatVector: {
                if (dataSize % sizeof(float) != 0) {
                    return {};
                }
                size_t dim = dataSize / sizeof(float);
                result.resize(dim);
                std::memcpy(result.data(), serialized.data(), dataSize);
                break;
            }
            case Uint8Vector: {
                result.reserve(dataSize);
                for (size_t i = 0; i < dataSize; ++i) {
                    result.push_back(static_cast<float>(static_cast<ui8>(serialized[i])));
                }
                break;
            }
            case Int8Vector: {
                result.reserve(dataSize);
                for (size_t i = 0; i < dataSize; ++i) {
                    result.push_back(static_cast<float>(static_cast<i8>(serialized[i])));
                }
                break;
            }
            default:
                // Unsupported format (e.g., BitVector)
                return {};
        }

        return result;
    }
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

        // Determine dimension from first vector after deserialization
        size_t floatDimension = 0;
        for (const auto& [key, vectorData] : vectors) {
            auto floatVec = DeserializeToFloatVector(vectorData);
            if (!floatVec.empty()) {
                floatDimension = floatVec.size();
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

        // Add all vectors in parallel (deserialized to float)
        executor.fixed(vectors.size(), [&](std::size_t thread, std::size_t task) {
            const auto& [key, vectorData] = vectors[task];
            auto floatVec = DeserializeToFloatVector(vectorData);
            if (floatVec.size() != floatDimension) {
                return;  // Skip invalid vectors
            }
            // Pass thread ID directly as third parameter
            auto addResult = Index.add(key, floatVec.data(), thread);
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

        // Deserialize target vector
        auto floatVec = DeserializeToFloatVector(targetVector);
        if (floatVec.size() != Dimension) {
            return result;
        }

        auto searchResult = Index.search(floatVec.data(), k);
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

// THnswIndexManager implementation

bool THnswIndexManager::IsVectorColumn(const TString& columnName) {
    return columnName == "emb" || columnName == "embedding" || columnName == "vector";
}

bool THnswIndexManager::BuildIndex(ui64 tableId, const TString& columnName,
                                    const std::vector<std::pair<ui64, TString>>& vectors) {
    THnswIndexKey key{tableId, columnName};

    THnswIndex index;
    if (!index.Build(vectors)) {
        return false;
    }

    Indexes[key] = std::move(index);
    return true;
}

const THnswIndex* THnswIndexManager::GetIndex(ui64 tableId, const TString& columnName) const {
    THnswIndexKey key{tableId, columnName};
    auto it = Indexes.find(key);
    if (it != Indexes.end()) {
        if (it->second.IsReady()) {
            return &it->second;
        }
    }
    return nullptr;
}

bool THnswIndexManager::HasIndex(ui64 tableId, const TString& columnName) const {
    return GetIndex(tableId, columnName) != nullptr;
}

void THnswIndexManager::Clear() {
    Indexes.clear();
}

} // namespace NKikimr::NDataShard

