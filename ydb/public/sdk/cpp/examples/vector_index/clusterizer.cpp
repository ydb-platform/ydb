#include "clusterizer.h"

#include "util/stream/output.h"
#include "util/system/yassert.h"

#include <format>

static constexpr ui64 kMinClusters = 4;
static ui64 gId = 1;

void TClusterizer::TProgress::Reset(std::string_view operation, ui64 rows) {
    // Y_UNUSED(rows, Rows, Curr, Last);
    Cout << "Start " << operation << ": " << rows << Endl;
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
    std::swap(RawData, other.RawData);
    IdData.swap(other.IdData);
    RawDataStorage.swap(other.RawDataStorage);
    Min.swap(other.Min);
}
void TClusterizer::TBatch::Clear() {
    RawData = {};
    IdData.clear();
    RawDataStorage.clear();
    Min.clear();
}
bool TClusterizer::TBatch::Empty() const {
    return RawData.empty();
}

TClusterizer::TClusterizer(TDatasetIterator& it, TDistance distance, TCreateParentChild create, NVectorIndex::TThreadPool* tp)
    : It{it}
    , Distance{std::move(distance)}
    , Create{std::move(create)}
    , ThreadPool{tp}
{
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
    if (ThreadPool) {
        auto threads = ThreadPool->Size();
        std::unique_lock lock{M};
        while (Work != 0) {
            WaitWork.wait(lock);
        }
        ToCompute.Swap(ToFill);
        if (!ToCompute.Empty()) {
            Work = threads;
            lock.unlock();
            for (ui32 i = 0; i != threads; ++i) {
                ThreadPool->Submit([this, i, threads] {
                    auto size = ToCompute.RawData.size();
                    auto batchSize = (size + threads - 1) / threads;
                    auto start = i * batchSize;
                    auto len = start < size ? std::min<ui32>(size - start, batchSize) : 0;
                    auto batch = ToCompute.RawData.subspan(start, len);
                    for (const auto& rawEmbedding : batch) {
                        auto embedding = GetArray<float>(rawEmbedding);
                        auto min = Compute(embedding);
                        ToCompute.Min[start++] = min;
                    }
                    std::lock_guard lock{M};
                    if (--Work == 0) {
                        WaitWork.notify_one();
                    }
                });
            }
        }
        Progress.Report(0);
    } else {
        for (size_t start = 0; const auto& rawEmbedding : ToFill.RawData) {
            auto embedding = GetArray<float>(rawEmbedding);
            auto min = Compute(embedding);
            ToFill.Min[start++] = min;
        }
    }
    if (!ToFill.Empty()) {
        func();
        ToFill.Clear();
        if (ThreadPool) {
            Progress.Report(0);
        }
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

TClusterizer::TClusters TClusterizer::Run(const TOptions& options) {
    Y_ASSERT(!options.normalize); // normalize not supported
    const ui64 clusters = std::min<ui64>(options.maxK, 1000);
    if (Init(clusters)) {
        for (ui32 i = 1; i <= options.maxIterations; ++i) {
            if (!Step(i, options.maxIterations, 1.1)) {
                break;
            }
        }
        Finalize();
    } else {
        auto rows = It.Rows();
        Cout << "Bad dataset (" << rows << ") for such clusterization ( iterations: " << options.maxIterations << ", k: " << options.maxK << " )" << Endl;
#if 0
        Progress.Reset("read", rows);
        auto parentId = ++gId;
        // It.Iterate([&](ui32, TId id, TRawEmbedding rawEmbedding) {
        //     Progress.Report(1);
        //     Create(parentId, id, std::move(rawEmbedding));
        // });
        Progress.ForceReport();
        Clusters.Ids.emplace_back(parentId);
        Clusters.Count.emplace_back(rows);
        Clusters.Coords.emplace_back();
#endif
    }
    return std::move(Clusters);
}

bool TClusterizer::Init(ui64 k) {
    // Cout << "Start init" << Endl;
    // TODO kmeans++, kmeans||?
    if (k < kMinClusters || k * kMinClusterSize >= It.Rows()) {
        return false;
    }
    Clusters.Coords.clear();
    Clusters.Ids.clear();
    Clusters.Ids.resize(k, 0);
    Clusters.Count.resize(k, 0);
    Clusters.Coords.reserve(k);
    It.RandomK(k, [&](TRawEmbedding rawEmbedding) {
        auto embedding = GetArray<float>(rawEmbedding);
        Clusters.Coords.emplace_back(embedding.begin(), embedding.end());
    });
    if (Clusters.Coords.size() < k) {
        return false;
    }
    // TODO check distance between vectors in initial set
    auto dims = Clusters.Coords.front().size();
    NewClusters.resize(k);
    for (auto& cluster : NewClusters) {
        cluster.Coords.resize(dims, 0.f);
    }
    OldMean = std::numeric_limits<float>::max();
    BatchSize = (ui64{900'000} * ui64{ThreadPool ? ThreadPool->Size() : 1}) / (ui64{Clusters.Coords.size()} * ui64{Clusters.Coords.front().size()});
    return true;
}

void TClusterizer::StepUpdate() {
    for (size_t i = 0; i != ToFill.RawData.size(); ++i) {
        auto embedding = GetArray<float>(ToFill.RawData[i]);
        Update(ToFill.Min[i], embedding);
    }
}

void TClusterizer::EmbeddingsTrigger() {
    ToFill.RawData = ToFill.RawDataStorage;
    ComputeBatch([this] { StepUpdate(); }); // wait last and compute tail
    ComputeBatch([this] { StepUpdate(); }); // wait tail
}

void TClusterizer::Handle(std::span<const TString> embeddings) {
    if (ThreadPool) {
        Progress.Report(embeddings.size());
    }
    ToFill.RawData = embeddings;
    ToFill.Min.resize(embeddings.size());
    ComputeBatch([this] { StepUpdate(); });
}

void TClusterizer::Handle(TRawEmbedding rawEmbedding) {
    if (ThreadPool) {
        Progress.Report(1);
    }
    ToFill.RawDataStorage.emplace_back(std::move(rawEmbedding));
    ToFill.Min.emplace_back();
    if (ToFill.RawDataStorage.size() >= BatchSize) {
        ToFill.RawData = ToFill.RawDataStorage;
        ComputeBatch([this] { StepUpdate(); });
    }
}

bool TClusterizer::Step(ui32 iteration, ui32 maxIterations, float neededDiff) {
    if (ThreadPool) {
        Progress.Reset(std::format("step {} / {}", iteration, maxIterations), It.Rows());
    }

    It.IterateEmbedding(*this);
    EmbeddingsTrigger();

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
    if (ThreadPool) {
        Progress.ForceReport();
        Cout << "old mean: " << OldMean / NewClusters.size()
             << " new mean: " << newMean / NewClusters.size() << Endl;
    }
    if (newMean > OldMean) {
        return false;
    }

    for (size_t pos = 0; auto& cluster : NewClusters) {
        cluster.Coords.swap(Clusters.Coords[pos++]);
        std::fill(cluster.Coords.begin(), cluster.Coords.end(), 0);
        cluster.Count = 0;
    }
    bool stop = newMean * neededDiff >= OldMean;
    OldMean = newMean;
    return !stop;
}

void TClusterizer::FinalizeUpdate() {
    for (size_t i = 0; i != ToFill.RawDataStorage.size(); ++i) {
        auto& parentId = Clusters.Ids[ToFill.Min[i].Pos];
        Clusters.Count[ToFill.Min[i].Pos]++;
        auto id = ToFill.IdData[i];
        if (Y_UNLIKELY(!parentId))
            parentId = ++gId;
        Create(parentId, id, std::move(ToFill.RawDataStorage[i]));
    }
}

void TClusterizer::IdsTrigger() {
    ToFill.RawData = ToFill.RawDataStorage;
    ComputeBatch([this] { FinalizeUpdate(); }); // wait last and compute tail
    ComputeBatch([this] { FinalizeUpdate(); }); // wait tail
}

void TClusterizer::Handle(TId id, TRawEmbedding rawEmbedding) {
    if (ThreadPool) {
        Progress.Report(1);
    }
    ToFill.IdData.emplace_back(id);
    ToFill.RawDataStorage.emplace_back(std::move(rawEmbedding));
    ToFill.Min.emplace_back();
    if (ToFill.RawDataStorage.size() >= BatchSize) {
        ToFill.RawData = ToFill.RawDataStorage;
        ComputeBatch([this] { FinalizeUpdate(); });
    }
}

void TClusterizer::Finalize() {
    if (ThreadPool) {
        Progress.Reset("finalize", It.Rows());
    }

    It.IterateId(*this);
    IdsTrigger();
    if (ThreadPool) {
        Progress.ForceReport();
    }
}
