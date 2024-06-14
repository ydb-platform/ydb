#include "clusterizer.h"
#include "util/stream/output.h"
#include "util/system/yassert.h"

static constexpr ui32 kBatchSize = 500;
static ui64 gId = 1;

void TClusterizer::TProgress::Reset(ui64 rows) {
    Cout << "Start reading: " << rows << Endl;
    Curr = 0;
    Rows = rows;
    Last = std::chrono::steady_clock::now();
}

void TClusterizer::TProgress::Report() {
    if (auto now = std::chrono::steady_clock::now(); (now - Last) >= std::chrono::seconds{1}) {
        Cout << "Already read\t" << static_cast<ui64>(Curr / Rows * 100.0)
             << "% rows, time spent:\t" << std::chrono::duration<double>{now - Last}.count() << " sec" << Endl;
        Last = now;
    }
    ++Curr;
}

template <typename T>
static std::span<const T> GetArray(std::string_view str) {
    const char* buf = str.data();
    const size_t len = str.size() - 1;
    if (Y_UNLIKELY(len % sizeof(T) != 0))
        return {};

    const auto count = len / sizeof(T);
    return {reinterpret_cast<const T*>(buf), count};
}

void TClusterizer::TBatch::Swap(TBatch& other) {
    IdData.swap(other.IdData);
    RawData.swap(other.RawData);
    Min.swap(other.Min);
}
void TClusterizer::TBatch::Clear() {
    IdData.clear();
    RawData.clear();
    Min.clear();
}
bool TClusterizer::TBatch::Empty() const {
    return RawData.empty();
}

TClusterizer::TClusterizer(TDatasetIterator& it, TDistance distance, TCreateParentChild create)
    : It{it}
    , Distance{std::move(distance)}
    , Create{std::move(create)}
{
    ui64 n = std::clamp<ui64>(std::thread::hardware_concurrency(), 1, std::numeric_limits<ui64>::max());
    Cout << "kmeans will use " << n << " threads" << Endl;
    Threads.reserve(n);
    for (ui64 i = 0; i != n; ++i) {
        Threads.emplace_back([this, i] {
            std::unique_lock lock{M};
            while (true) {
                while ((Work & (ui64{1} << i)) == 0) {
                    WaitIdle.wait(lock);
                }
                if (Stop) {
                    return;
                }
                lock.unlock();
                auto size = Batch.RawData.size();
                auto start = i * kBatchSize;
                auto len = start < size ? std::min<ui32>(size - start, kBatchSize) : 0;
                std::span batch{Batch.RawData.data() + start, len};
                for (const auto& rawEmbedding : batch) {
                    auto embedding = GetArray<float>(rawEmbedding);
                    auto min = Compute(embedding);
                    Batch.Min[start++] = min;
                }
                lock.lock();
                Work &= ~(ui64{1} << i);
                if (Work == 0) {
                    WaitWork.notify_one();
                }
            }
        });
    }
}

TClusterizer::TMin TClusterizer::Compute(TEmbedding embedding) {
    float minDistance = std::numeric_limits<float>::max();
    ui32 minPos = 0;
    Y_ASSERT(!Clusters.Coords.empty());
    for (ui32 pos = 0; auto& cluster : Clusters.Coords) {
        auto distance = Distance(cluster, embedding);
        if (distance < minDistance) {
            minDistance = distance;
            minPos = pos;
        }
        ++pos;
    }
    return {minDistance, minPos};
}

template <typename Func>
void TClusterizer::ComputeBatch(Func&& func) {
    if (Batch.Empty()) {
        return;
    }

    std::unique_lock lock{M};
    Y_ASSERT(Work == 0);
    Work = (ui64{1} << Threads.size()) - 1;
    WaitIdle.notify_all();
    while (Work != 0) {
        WaitWork.wait(lock);
    }
    lock.unlock();

    func();
    Batch.Clear();
}

void TClusterizer::Update(TMin min, TEmbedding embedding) {
    Y_ASSERT(Clusters.Coords.size() == NewClusters.size());
    NewClusters[min.Pos].Distance += min.Distance;
    for (size_t pos = 0; auto& coord : NewClusters[min.Pos].Coords) {
        coord += embedding[pos++];
    }
    NewClusters[min.Pos].Count++;
}

TClusterizer::~TClusterizer() {
    std::unique_lock lock{M};
    Work = (ui64{1} << Threads.size()) - 1;
    Stop = true;
    lock.unlock();
    WaitIdle.notify_all();
    for (auto& thread : Threads) {
        thread.join();
    }
}

TClusterizer::TClusters TClusterizer::Run(const TOptions& options) {
    Y_ASSERT(!options.normalize); // normalize not supported
    const auto rows = It.Rows();
    const ui64 clusters = std::sqrt(rows);
    if (clusters < 128) {
        return {};
    }
    // const auto minClustersCount = size / options.maxClusterSize;
    // const auto maxClustersCount = size / options.minClusterSize;
    Init(std::min<ui64>(clusters, options.maxK));
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
    Clusters.Ids.resize(k, 0);
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

    auto update = [&] {
        for (size_t i = 0; i != Batch.RawData.size(); ++i) {
            auto embedding = GetArray<float>(Batch.RawData[i]);
            Update(Batch.Min[i], embedding);
        }
    };
    It.Iterate([&](TRawEmbedding rawEmbedding) {
        Progress.Report();
        Batch.RawData.emplace_back(std::move(rawEmbedding));
        Batch.Min.emplace_back();
        if (Batch.RawData.size() == Threads.size() * kBatchSize) {
            ComputeBatch(update);
        }
    });
    ComputeBatch(update); // compute tail

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
    Progress.Reset(It.Rows());

    auto update = [&] {
        for (size_t i = 0; i != Batch.RawData.size(); ++i) {
            auto& parentId = Clusters.Ids[Batch.Min[i].Pos];
            auto id = Batch.IdData[i];
            if (Y_UNLIKELY(!parentId))
                parentId = ++gId;
            Create(parentId, id, std::move(Batch.RawData[i]));
        }
    };
    It.Iterate([&](TId id, TRawEmbedding rawEmbedding) {
        Progress.Report();
        Batch.IdData.emplace_back(id);
        Batch.RawData.emplace_back(std::move(rawEmbedding));
        Batch.Min.emplace_back();
        if (Batch.RawData.size() == Threads.size() * kBatchSize) {
            ComputeBatch(update);
        }
    });
    ComputeBatch(update); // compute tail
}
