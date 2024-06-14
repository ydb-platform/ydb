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

TClusterizer::TClusterizer(TDatasetIterator& it, TDistance distance, TCreateParentChild create)
    : It{it}
    , Distance{std::move(distance)}
    , Create{std::move(create)}
{
    ui64 n = std::clamp<ui64>(std::thread::hardware_concurrency(), 1, std::numeric_limits<ui64>::max());
    Threads.reserve(n);
    for (ui64 i = 0; i != n; ++i) {
        Threads.emplace_back([this, i] {
            std::unique_lock lock{M};
            while (true) {
                while ((Work & (1 << i)) == 0) {
                    WaitIdle.wait(lock);
                }
                if (Stop) {
                    return;
                }
                lock.unlock();
                auto start = i * 100;
                auto len = i * 100 >= Batch.Data.size() ? 0 : std::min<ui32>(Batch.Data.size() - i * 100, 100);
                std::span batch{Batch.Data.data() + i * 100, len};
                for (const auto& e : batch) {
                    auto min = Compute(e);
                    Batch.Min[start++] = min;
                }
                lock.lock();
                Work &= ~(1 << i);
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

void TClusterizer::ComputeBatch() {
    std::unique_lock lock{M};
    Y_ASSERT(Work == 0);
    Work = (ui64{1} << Threads.size()) - 1;
    WaitIdle.notify_all();
    while (Work != 0) {
        WaitWork.wait(lock);
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

void TClusterizer::UpdateBatch() {
    ComputeBatch();
    for (size_t i = 0; i != Batch.Data.size(); ++i) {
        Update(Batch.Min[i], Batch.Data[i]);
    }
    Batch.Data.clear();
    Batch.Min.clear();
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
        Batch.Data.emplace_back(embedding.begin(), embedding.end());
        Batch.Min.emplace_back();
        if (Batch.Data.size() == Threads.size() * 100) {
            UpdateBatch();
        }
    });
    if (!Batch.Data.empty()) {
        UpdateBatch();
    }
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
        ComputeBatch();
        for (size_t i = 0; i != Batch.Data.size(); ++i) {
            auto& parentId = Clusters.Ids[Batch.Min[i].Pos];
            auto id = Batch.IdData[i];
            if (Y_UNLIKELY(!parentId))
                parentId = id;
            Create(*parentId, id, Batch.RawData[i]);
        }
        Batch.IdData.clear();
        Batch.RawData.clear();
        Batch.Data.clear();
        Batch.Min.clear();
    };
    It.Iterate([&](TId id, TRawEmbedding rawEmbedding) {
        Progress.Report();
        auto embedding = GetArray<float>(rawEmbedding);
        Batch.IdData.emplace_back(id);
        Batch.RawData.emplace_back(rawEmbedding);
        Batch.Data.emplace_back(embedding.begin(), embedding.end());
        Batch.Min.emplace_back();
        if (Batch.Data.size() == Threads.size() * 100) {
            update();
        }
    });
    if (!Batch.Data.empty()) {
        update();
    }
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
