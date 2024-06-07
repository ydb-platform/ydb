#include "util/system/types.h"
#include <vector>
#include <span>
#include <functional>
#include <chrono>

// It's prototype so it's not really optimized

// ~~Idea of ​​how not to keep everything in memory is taken from here~~
// https://github.com/google-research/google-research/blob/master/scann/scann/utils/gmm_utils.h

// A = available memory
// E = expected embedding size
// X = A / E = available rows
// Y = dataset rows
// P = parts
// X / P -- needed count of clusters in distributed case in each part
// when go each 2^7

using TId = uint64_t;
using TEmbedding = std::span<const float>;

class TDatasetIterator {
public:
    virtual ui64 Rows() const = 0;
    virtual void RandomK(ui64 k, std::function<void(TEmbedding)>) = 0;
    virtual void Iterate(std::function<void(TEmbedding)>) = 0;
    virtual void Iterate(std::function<void(TId, TEmbedding)>) = 0;
};

class TClusterizer {
public:
    using TDistance = std::function<float(TEmbedding, TEmbedding)>;

    TClusterizer(TDatasetIterator& it, TDistance distance);

    struct TOptions {
        ui32 size = 0;
        ui32 maxIterations = 10;
        ui32 minClusterSize = 1;
        ui32 maxClusterSize = 1 << 13;
        bool normalize = false;
    };

    struct TClusters {
        std::vector<std::vector<TId>> Ids;
        std::vector<std::vector<float>> Coords;
    };

    TClusters Run(const TOptions& options);

private:
    void Init(ui64 k);

    bool Step(float neededDiff);

    void Finalize();

    TClusters Clusters;

    TDatasetIterator& It;
    TDistance Distance;

    struct AggregatedCluster {
        std::vector<float> Coords;
        float Distance;
        ui64 Count;
    };
    std::vector<AggregatedCluster> NewClusters;
    float OldMean = std::numeric_limits<float>::max();
};
