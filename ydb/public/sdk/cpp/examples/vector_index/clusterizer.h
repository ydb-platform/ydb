#include "thread_pool.h"

#include "util/generic/fwd.h"
#include "util/generic/string.h"
#include "util/system/types.h"

#include <vector>
#include <span>
#include <functional>
#include <chrono>

inline constexpr ui64 kMinClusterSize = 8;

using TId = uint64_t;
using TRawEmbedding = TString&&;
using TEmbedding = std::span<const float>;

class TClusterizer;

class TDatasetIterator {
public:
    virtual ui64 Rows() const = 0;
    virtual void RandomK(ui64 k, std::function<void(TRawEmbedding)>) = 0;
    virtual void IterateEmbedding(TClusterizer& clusterizer) = 0;
    virtual void IterateId(TClusterizer& clusterizer) = 0;
};

using TDistance = std::function<float(TEmbedding, TEmbedding)>;
using TCreateParentChild = std::function<void(TId, TId, TRawEmbedding)>;

class TClusterizer {
public:
    TClusterizer(TDatasetIterator& it, TDistance distance, TCreateParentChild create, NVectorIndex::TThreadPool* tp = nullptr);

    struct TOptions {
        ui32 maxIterations = 10;
        ui32 maxK = 10;
        bool normalize = false;
    };

    struct TClusters {
        std::vector<TId> Ids;
        std::vector<ui64> Count;
        std::vector<std::vector<float>> Coords;
    };

    TClusters Run(const TOptions& options);

    void Handle(std::span<const TString> embeddings);
    void Handle(TRawEmbedding embedding);
    void Handle(TId id, TRawEmbedding embedding);

    void EmbeddingsTrigger();
    void IdsTrigger();

private:
    bool Init(ui64 k);
    void StepUpdate();
    bool Step(ui32 iteration, ui32 maxIterations, float neededDiff);

    void FinalizeUpdate();
    void Finalize();

    struct TMin {
        float Distance = 0;
        ui32 Pos = 0;
    };

    TMin Compute(TEmbedding embedding);
    void Update(TMin min, TEmbedding embedding);

    template <typename Func>
    void ComputeBatch(Func&& func);

    TClusters Clusters;

    TDatasetIterator& It;
    TDistance Distance;
    TCreateParentChild Create;

    struct TAggregatedCluster {
        std::vector<float> Coords;
        float Distance = 0;
        ui64 Count = 0;
    };

    std::vector<TAggregatedCluster> NewClusters;
    float OldMean = std::numeric_limits<float>::max();

    struct TProgress {
        void Reset(std::string_view operation, ui64 rows);
        void Report(ui64 read);
        void ForceReport();

    private:
        double Curr = 0;
        double Rows = 0;
        std::chrono::steady_clock::time_point Last{};
    };

    TProgress Progress;

    struct TBatch {
        std::span<const TString> RawData;
        std::vector<TId> IdData;
        std::vector<TString> RawDataStorage;
        std::vector<TMin> Min;

        void Swap(TBatch& other);
        void Clear();
        bool Empty() const;
    };

    NVectorIndex::TThreadPool* ThreadPool = nullptr;
    TBatch ToCompute;
    TBatch ToFill;
    std::mutex M;
    std::condition_variable WaitWork;
    ui64 Work = 0;
    ui64 BatchSize = 0;
};
