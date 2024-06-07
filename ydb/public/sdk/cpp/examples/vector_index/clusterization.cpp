#include "clusterization.h"
#include "util/system/yassert.h"

#define USE_CURRENT_UDF_ABI_VERSION
#include "ydb/library/yql/udfs/common/knn/knn-distance.h"

static constexpr ui64 kStep = 1 << 7;

TClusterizer::TClusters TClusterizer::Run(const TOptions& options) {
    Y_ASSERT(!options.normalize); // normalize not supported
    const auto rows = It.Rows();
    if (rows < kStep * kStep) {
        return {};
    }
    // const auto minClustersCount = size / options.maxClusterSize;
    // const auto maxClustersCount = size / options.minClusterSize;
    Init(rows / kStep);
    for (size_t i = 0; i < options.maxIterations; ++i) {
        if (!Step(1.1)) {
            break;
        }
    }
    Finalize();
    return std::move(Clusters);
}

void TClusterizer::Init(ui64 k) {
    Clusters.Coords.clear();
    Clusters.Ids.clear();
    Clusters.Ids.resize(k);
    Clusters.Coords.reserve(k);
    It.RandomK(k, [&](TRawEmbedding embedding) {
        Clusters.Coords.emplace_back(embedding);
    });
    Y_ASSERT(Clusters.Coords.size() == k)
}

bool TClusterizer::Step(float neededDiff) {
    It.Iterate([&](TRawEmbedding rawEmbedding) {
        std::span embedding = TKnnVectorSerializer<float>::GetArray(rawEmbedding);
        float minDistance = std::numeric_limits<float>::max();
        size_t minPos = 0;
        Y_ASSERT(!Clusters.Coords.empty());
        for (size_t pos = 0; auto& cluster : Clusters.Coords) {
            auto distance = Distance(cluster, embedding);
            if (distance < minDistance) {
                minDistance = distance;
                minPos = pos;
            }
            ++pos;
        }
        Y_ASSERT(Clusters.Coords.size() == NewClusters.size());
        NewClusters[minPos].Distance += minDistance;
        for (size_t pos = 0; auto& coord : NewClusters[minPos].Coords) {
            coord += embedding[pos++];
        }
        NewClusters[minPos].Count++;
    });
    float newMean = 0;
    for (auto& cluster : NewClusters) {
        auto count = static_cast<float>(cluster.Count);
        for (auto& coord : cluster.Coords) {
            coord /= count;
        }
        cluster.Distance /= count;
        newMean += cluster.Distance;
    }
    if (newMean > OldMean) {
        return false;
    }

    for (size_t pos = 0; auto& cluster : NewClusters) {
        cluster.Coords.swap(Clusters.Coords[pos++]);
    }
    return newMean * neededDiff < OldMean;
}

void TClusterizer::Finalize() {
    for (size_t pos = 0; auto& cluster : NewClusters) {
        Clusters.Ids[pos++].reserve(cluster.Count);
    }
    It.Iterate([&](TId id, TRawEmbedding rawEmbedding) {
        std::span embedding = TKnnVectorSerializer<float>::GetArray(rawEmbedding);

        float minDistance = std::numeric_limits<float>::max();
        size_t minPos = 0;
        Y_ASSERT(!Clusters.Coords.empty());
        for (size_t pos = 0; auto& cluster : Clusters.Coords) {
            auto distance = Distance(cluster, embedding);
            if (distance < minDistance) {
                minDistance = distance;
                minPos = pos;
            }
            ++pos;
        }
        Clusters.Ids[minPos].push_back(id);
    });
}