#include "clusterizer.h"
#include "util/stream/output.h"
#include "util/system/yassert.h"

static constexpr ui64 kStep = 1 << 7;

template <typename T>
static std::span<const T> GetArray(std::string_view str) {
    const char* buf = str.data();
    const size_t len = str.size() - 1;
    if (Y_UNLIKELY(len % sizeof(T) != 0))
        return {};

    const auto count = len / sizeof(T);
    return {reinterpret_cast<const T*>(buf), count};
}

TClusterizer::TClusterizer(TDatasetIterator& it, TDistance distance)
    : It{it}
    , Distance{std::move(distance)}
{
}

TClusterizer::TClusters TClusterizer::Run(const TOptions& options) {
    Y_ASSERT(!options.normalize); // normalize not supported
    const auto rows = It.Rows();
    if (rows < kStep * kStep) {
        return {};
    }
    // const auto minClustersCount = size / options.maxClusterSize;
    // const auto maxClustersCount = size / options.minClusterSize;
    Init(std::min<ui64>(rows / kStep, options.maxK));
    for (size_t i = 0; i < options.maxIterations; ++i) {
        Cout << "Start step: " << i << " / " << options.maxIterations << Endl;
        if (!Step(1.25)) {
            break;
        }
    }
    Finalize();
    return std::move(Clusters);
}

void TClusterizer::Init(ui64 k) {
    Cout << "Start init" << Endl;
    Y_ASSERT(k > 0);
    Clusters.Coords.clear();
    Clusters.Ids.clear();
    Clusters.Ids.resize(k);
    Clusters.Coords.reserve(k);
    It.RandomK(k, [&](TRawEmbedding rawEmbedding) {
        auto embedding = GetArray<float>(rawEmbedding);
        Clusters.Coords.emplace_back(embedding.begin(), embedding.end());
    });
    Cout << k << " " << Clusters.Coords.size() << Endl;
    Y_ASSERT(Clusters.Coords.size() == k);
    auto dims = Clusters.Coords.front().size();
    NewClusters.resize(k);
    for (auto& cluster : NewClusters) {
        cluster.Coords.resize(dims, 0.f);
    }
}

bool TClusterizer::Step(float neededDiff) {
    Progress.Reset(It.Rows());
    It.Iterate([&](TRawEmbedding rawEmbedding) {
        Progress.Report();
        auto embedding = GetArray<float>(rawEmbedding);
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
    Cout << "old mean: " << OldMean / NewClusters.size()
         << " new mean: " << newMean / NewClusters.size() << Endl;
    if (newMean > OldMean) {
        return false;
    }

    for (size_t pos = 0; auto& cluster : NewClusters) {
        cluster.Coords.swap(Clusters.Coords[pos++]);
    }
    bool stop = newMean * neededDiff >= OldMean;
    OldMean = newMean;
    return !stop;
}

void TClusterizer::Finalize() {
    Cout << "Start finalize" << Endl;
    for (size_t pos = 0; auto& cluster : NewClusters) {
        Clusters.Ids[pos++].reserve(cluster.Count);
    }
    Progress.Reset(It.Rows());
    It.Iterate([&](TId id, TRawEmbedding rawEmbedding) {
        Progress.Report();
        auto embedding = GetArray<float>(rawEmbedding);
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

void TClusterizer::TProgress::Reset(ui64 rows) {
    Cout << "Start reading: " << rows << Endl;
    Curr = 0;
    Rows = rows;
    Count = 0;
    Last = std::chrono::steady_clock::now();
}

void TClusterizer::TProgress::Report() {
    if (auto now = std::chrono::steady_clock::now(); (now - Last) >= std::chrono::seconds{1}) {
        Cout << "Already read:\t" << Count << "\t" << static_cast<ui64>(Curr / Rows * 100.0) << "%" << Endl;
        ++Count;
        Last = now;
    }
    ++Curr;
}
