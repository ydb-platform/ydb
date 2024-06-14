#include "util/generic/fwd.h"
#include "util/system/types.h"
#include <vector>
#include <span>
#include <functional>
#include <chrono>
#include <thread>

using TId = uint64_t;
using TRawEmbedding = const TString&;
using TEmbedding = std::span<const float>;

class TDatasetIterator {
public:
    virtual ui64 Rows() const = 0;
    virtual void RandomK(ui64 k, std::function<void(TRawEmbedding)>) = 0;
    virtual void Iterate(std::function<void(TRawEmbedding)>) = 0;
    virtual void Iterate(std::function<void(TId, TRawEmbedding)>) = 0;
};

using TDistance = std::function<float(TEmbedding, TEmbedding)>;
using TCreateParentChild = std::function<void(TId, TId, TRawEmbedding)>;

class TClusterizer {
public:
    TClusterizer(TDatasetIterator& it, TDistance distance, TCreateParentChild create);
    ~TClusterizer();

    struct TOptions {
        ui32 maxIterations = 10;
        ui32 maxK = 1000;
        ui32 minClusterSize = 1;
        ui32 maxClusterSize = 1 << 13;
        bool normalize = false;
    };

    struct TClusters {
        std::vector<std::optional<TId>> Ids;
        std::vector<std::vector<float>> Coords;
    };

    TClusters Run(const TOptions& options);

private:
    void Init(ui64 k);

    bool Step(float neededDiff);

    void Finalize();

    struct TMin {
        float Distance = 0;
        ui32 Pos = 0;
    };

    TMin Compute(TEmbedding embedding);
    void ComputeBatch();
    void Update(TMin min, TEmbedding embedding);
    void UpdateBatch();

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
        void Reset(ui64 rows);
        void Report();

    private:
        double Curr = 0;
        double Rows = 0;
        ui64 Count = 0;
        std::chrono::steady_clock::time_point Last{};
    };
    TProgress Progress;
    struct TBatch {
        std::vector<TId> IdData;
        std::vector<TString> RawData;

        std::vector<std::vector<float>> Data;
        std::vector<TMin> Min;
    };
    TBatch Batch;
    std::vector<std::thread> Threads;
    std::mutex M;
    std::condition_variable WaitWork;
    std::condition_variable WaitIdle;
    bool Stop = false;
    ui64 Work = 0;
};
