#include "clusterizer.h"
#include "util/stream/output.h"
#include "util/system/yassert.h"

static ui64 gId = 1;

void TClusterizer::TProgress::Reset(ui64 rows) {
    Cout << "Start reading: " << rows << Endl;
    Curr = 0;
    Rows = rows;
    Last = std::chrono::steady_clock::now();
}

void TClusterizer::TProgress::ForceReport() {
    auto now = std::chrono::steady_clock::now();
    Cout << "Already read\t" << static_cast<ui64>(Curr / Rows * 100.0)
         << "% rows, time spent:\t" << std::chrono::duration<double>{now - Last}.count()
         << " sec, " << Curr << " / " << Rows << " rows" << Endl;
    Last = now;
}

void TClusterizer::TProgress::Report(ui64 read) {
    if (auto now = std::chrono::steady_clock::now(); (now - Last) >= std::chrono::seconds{1}) {
        ForceReport();
    }
    Curr += read;
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
                auto size = ToCompute.RawData.size();
                auto threads = Threads.size();
                auto batchSize = (size + threads - 1) / threads;
                auto start = i * batchSize;
                auto len = start < size ? std::min<ui32>(size - start, batchSize) : 0;
                std::span batch{ToCompute.RawData.data() + start, len};
                for (const auto& rawEmbedding : batch) {
                    auto embedding = GetArray<float>(rawEmbedding);
                    auto min = Compute(embedding);
                    ToCompute.Min[start++] = min;
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
    std::unique_lock lock{M};
    while (Work != 0) {
        WaitWork.wait(lock);
    }
    ToCompute.Swap(ToFill);
    if (!ToCompute.Empty()) {
        Work = (ui64{1} << Threads.size()) - 1;
        WaitIdle.notify_all();
    }
    lock.unlock();
    Progress.Report(0);
    if (!ToFill.Empty()) {
        func();
        ToFill.Clear();
        Progress.Report(0);
    }
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
    for (size_t i = 0; i < options.maxIterations;) {
        Cout << "Start step: " << ++i << " / " << options.maxIterations << Endl;
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
        for (size_t i = 0; i != ToFill.RawData.size(); ++i) {
            auto embedding = GetArray<float>(ToFill.RawData[i]);
            Update(ToFill.Min[i], embedding);
        }
    };
    It.Iterate([&]([[maybe_unused]] ui32 rows, TRawEmbedding rawEmbedding) {
        Progress.Report(1);
        ToFill.RawData.emplace_back(std::move(rawEmbedding));
        ToFill.Min.emplace_back();
        if (ToFill.RawData.size() == rows) {
            ComputeBatch(update);
        }
    });
    ComputeBatch(update); // wait last and compute tail
    ComputeBatch(update); // wait tail

    float newMean = 0;
    ui64 zeroCount = 0;
    float maxDistance = std::numeric_limits<float>::min();
    for (auto& cluster : NewClusters) {
        if (cluster.Count == 0) {
            ++zeroCount;
            continue;
        }
        auto count = static_cast<float>(cluster.Count);
        for (auto& coord : cluster.Coords) {
            coord /= count;
        }
        cluster.Distance /= count;
        if (cluster.Distance > maxDistance) {
            maxDistance = cluster.Distance;
        }
        newMean += cluster.Distance;
    }
    auto it = NewClusters.begin();
    It.RandomK(zeroCount, [&](TRawEmbedding rawEmbedding) {
        for (; it != NewClusters.end(); ++it) {
            if (it->Count == 0) {
                auto embedding = GetArray<float>(rawEmbedding);
                it->Coords.assign(embedding.begin(), embedding.end());
            }
        }
    });
    newMean += zeroCount * maxDistance;
    Progress.ForceReport();
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
        for (size_t i = 0; i != ToFill.RawData.size(); ++i) {
            auto& parentId = Clusters.Ids[ToFill.Min[i].Pos];
            auto id = ToFill.IdData[i];
            if (Y_UNLIKELY(!parentId))
                parentId = ++gId;
            Create(parentId, id, std::move(ToFill.RawData[i]));
        }
    };
    It.Iterate([&]([[maybe_unused]] ui32 rows, TId id, TRawEmbedding rawEmbedding) {
        Progress.Report(1);
        ToFill.IdData.emplace_back(id);
        ToFill.RawData.emplace_back(std::move(rawEmbedding));
        ToFill.Min.emplace_back();
        if (ToFill.RawData.size() == rows) {
            ComputeBatch(update);
        }
    });
    ComputeBatch(update); // wait last and compute tail
    ComputeBatch(update); // wait tail
    Progress.ForceReport();
}
