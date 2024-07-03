#include "clusterizer.h"

#include "util/stream/output.h"
#include "util/system/yassert.h"

#include <format>

static constexpr ui64 kMinClusters = 4;
static TId gId = 1;

template <typename T>
static std::span<const T> GetArray(std::string_view str) {
    const char* buf = str.data();
    const size_t len = str.size() - 1;
    if (Y_UNLIKELY(len % sizeof(T) != 0))
        return {};

    const auto count = len / sizeof(T);
    return {reinterpret_cast<const T*>(buf), count};
}

template <typename T>
void TClusterizer<T>::TProgress::Reset(std::string_view operation, ui64 rows) {
    // Y_UNUSED(rows, Rows, Curr, Last);
    Cout << "Start " << operation << ": " << rows << Endl;
    Curr = 0;
    Rows = rows;
    Last = std::chrono::steady_clock::now();
}

template <typename T>
void TClusterizer<T>::TProgress::ForceReport() {
    auto now = std::chrono::steady_clock::now();
    Cout << "Already read\t" << static_cast<ui64>(Curr / Rows * 100.0)
         << "% rows, time spent:\t" << std::chrono::duration<double>{now - Last}.count()
         << " sec, " << Curr << " / " << Rows << " rows" << Endl;
    Last = now;
}

template <typename T>
void TClusterizer<T>::TProgress::Report(ui64 read) {
    if (auto now = std::chrono::steady_clock::now(); (now - Last) >= std::chrono::seconds{1}) {
        ForceReport();
    }
    Curr += read;
}

template <typename T>
void TClusterizer<T>::TBatch::Swap(TBatch& other) {
    std::swap(RawData, other.RawData);
    IdData.swap(other.IdData);
    RawDataStorage.swap(other.RawDataStorage);
    Min.swap(other.Min);
}

template <typename T>
void TClusterizer<T>::TBatch::Clear() {
    RawData = {};
    IdData.clear();
    RawDataStorage.clear();
    Min.clear();
}

template <typename T>
bool TClusterizer<T>::TBatch::Empty() const {
    return RawData.empty();
}

template <typename T>
TClusterizer<T>::TClusterizer(TDatasetIterator& it, TDistance distance, TCreateParentChild create, NVectorIndex::TThreadPool* tp)
    : It{it}
    , Distance{std::move(distance)}
    , Create{std::move(create)}
    , ThreadPool{tp}
{
}

template <typename T>
auto TClusterizer<T>::Compute(TEmbedding embedding) -> TMin {
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

template <typename T>
template <typename Func>
void TClusterizer<T>::ComputeBatch(Func&& func) {
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
                        auto embedding = GetArray<T>(rawEmbedding);
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
            auto embedding = GetArray<T>(rawEmbedding);
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

template <typename T>
void TClusterizer<T>::Update(TMin min, TEmbedding embedding) {
    Y_ASSERT(Clusters.Coords.size() == AggregatedClusters.size());
    auto& cluster = AggregatedClusters[min.Pos];
    cluster.Distance += min.Distance;
    for (size_t pos = 0; auto& coord : cluster.Coords) {
        coord += embedding[pos++];
    }
    cluster.Count++;
}

template <typename T>
auto TClusterizer<T>::Run(const TOptions& options) -> TClusters {
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
        BadCluster(options);
    }
    return std::move(Clusters);
}

template <typename T>
void TClusterizer<T>::BadCluster(const TOptions& options) {
    auto rows = It.Rows();
    Cout << "Bad dataset {" << rows << "} for such clusterization { iterations: " << options.maxIterations << ", k: " << options.maxK << " }" << Endl;
    if (ThreadPool) {
        Progress.Reset("finalize bad dataset", It.Rows());
    }
    Clusters.Ids.emplace_back(options.parentId);
    Clusters.Count.emplace_back(rows);

    It.IterateId([&](TId id, TRawEmbedding rawEmbedding) {
        auto embedding = GetArray<T>(rawEmbedding);
        if (AggregatedClusters.empty()) {
            auto& aggregated = AggregatedClusters.emplace_back();
            aggregated.Coords.resize(embedding.size(), 0);
        }
        Update({}, embedding);
        Create(options.parentId, id, std::move(rawEmbedding));
    });
    if (!AggregatedClusters.empty()) {
        auto& aggregated = AggregatedClusters[0];
        auto& coords = Clusters.Coords.emplace_back(aggregated.Coords.size());
        auto coordsCount = static_cast<TSum>(aggregated.Count);
        for (size_t j = 0; auto& coord : aggregated.Coords) {
            coords[j++] = static_cast<T>(coord / coordsCount);
            coord = 0;
        }
        aggregated.Count = 0;
    }
    if (ThreadPool) {
        Progress.ForceReport();
    }
}

template <typename T>
bool TClusterizer<T>::Init(ui64 k) {
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
        if (rawEmbedding.empty()) {
            Clusters.Coords.clear();
            return;
        }
        auto embedding = GetArray<float>(rawEmbedding);
        Clusters.Coords.emplace_back(embedding.begin(), embedding.end());
    });
    if (Clusters.Coords.size() < k) {
        return false;
    }
    // TODO check distance between vectors in initial set
    auto d = Clusters.Coords.front().size();
    AggregatedClusters.resize(k);
    for (auto& cluster : AggregatedClusters) {
        cluster.Coords.resize(d, 0);
    }
    OldMean = std::numeric_limits<float>::max();
    BatchSize = (ui64{900'000} * ui64{ThreadPool ? ThreadPool->Size() : 1}) / (ui64{Clusters.Coords.size()} * ui64{Clusters.Coords.front().size()});
    return true;
}

template <typename T>
void TClusterizer<T>::StepUpdate() {
    for (size_t i = 0; i != ToFill.RawData.size(); ++i) {
        auto embedding = GetArray<T>(ToFill.RawData[i]);
        Update(ToFill.Min[i], embedding);
    }
}

template <typename T>
void TClusterizer<T>::TriggerEmbeddings() {
    ToFill.RawData = ToFill.RawDataStorage;
    ComputeBatch([this] { StepUpdate(); }); // wait last and compute tail
    ComputeBatch([this] { StepUpdate(); }); // wait tail
}

template <typename T>
void TClusterizer<T>::Handle(std::span<const TString> embeddings) {
    if (ThreadPool) {
        Progress.Report(embeddings.size());
    }
    ToFill.RawData = embeddings;
    ToFill.Min.resize(embeddings.size());
    ComputeBatch([this] { StepUpdate(); });
}

template <typename T>
void TClusterizer<T>::Handle(TRawEmbedding rawEmbedding) {
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

template <typename T>
bool TClusterizer<T>::Step(ui32 iteration, ui32 maxIterations, float neededDiff) {
    if (ThreadPool) {
        Progress.Reset(std::format("step {} / {}", iteration, maxIterations), It.Rows());
    }

    It.IterateEmbedding(*this);
    TriggerEmbeddings();

    ui64 zeroCount = 0;
    float maxDistance = std::numeric_limits<float>::min();
    float newMean = 0;
    for (auto& cluster : AggregatedClusters) {
        if (Y_UNLIKELY(cluster.Count == 0)) {
            ++zeroCount;
            continue;
        }
        cluster.Distance /= cluster.Count;
        if (maxDistance < cluster.Distance) {
            maxDistance = cluster.Distance;
        }
        newMean += cluster.Distance;
    }
    auto it = AggregatedClusters.begin();
    It.RandomK(zeroCount, [&](TRawEmbedding rawEmbedding) {
        if (rawEmbedding.empty()) {
            it = AggregatedClusters.begin();
            return;
        }
        for (; it != AggregatedClusters.end(); ++it) {
            if (it->Count == 0) {
                auto embedding = GetArray<T>(rawEmbedding);
                it->Coords.assign(embedding.begin(), embedding.end());
                it->Count = 1;
            }
        }
    });
    newMean += zeroCount * maxDistance;
    if (ThreadPool) {
        Progress.ForceReport();
        Cout << "old mean: " << OldMean / AggregatedClusters.size()
             << " new mean: " << newMean / AggregatedClusters.size() << Endl;
    }
    if (newMean > OldMean) {
        return false;
    }

    for (size_t i = 0; auto& coords : Clusters.Coords) {
        auto& aggregated = AggregatedClusters[i++];
        auto coordsCount = static_cast<TSum>(aggregated.Count);
        for (size_t j = 0; auto& coord : aggregated.Coords) {
            coords[j++] = static_cast<T>(coord / coordsCount);
            coord = 0;
        }
        aggregated.Count = 0;
    }
    bool stop = newMean * neededDiff >= OldMean;
    OldMean = newMean;
    return !stop;
}

template <typename T>
void TClusterizer<T>::FinalizeUpdate() {
    for (size_t i = 0; i != ToFill.RawDataStorage.size(); ++i) {
        const auto minPos = ToFill.Min[i].Pos;
        Clusters.Count[minPos]++;
        auto& parentId = Clusters.Ids[minPos];
        if (Y_UNLIKELY(!parentId))
            parentId = gId++;
        Create(parentId, ToFill.IdData[i], std::move(ToFill.RawDataStorage[i]));
    }
}

template <typename T>
void TClusterizer<T>::TriggerIds() {
    ToFill.RawData = ToFill.RawDataStorage;
    ComputeBatch([this] { FinalizeUpdate(); }); // wait last and compute tail
    ComputeBatch([this] { FinalizeUpdate(); }); // wait tail
}

template <typename T>
void TClusterizer<T>::Handle(TId id, TRawEmbedding rawEmbedding) {
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

template <typename T>
void TClusterizer<T>::Finalize() {
    if (ThreadPool) {
        Progress.Reset("finalize", It.Rows());
    }

    It.IterateId(*this);
    TriggerIds();
    if (ThreadPool) {
        Progress.ForceReport();
    }
}

template class TClusterizer<float>;
template class TClusterizer<i8>;
