#include "thread_pool.h"

#include "util/generic/fwd.h"
#include "util/generic/string.h"
#include "util/system/types.h"

#include <vector>
#include <span>
#include <functional>
#include <chrono>

inline constexpr ui64 kMinClusterSize = 8;

using TId = ui32;
using TRawEmbedding = TString&&;

class TReadCallback {
public:
    virtual void Handle(std::span<const TString> embeddings) = 0;
    virtual void Handle(TRawEmbedding embedding) = 0;
    virtual void Handle(TId id, TRawEmbedding embedding) = 0;

    virtual void TriggerEmbeddings() = 0;
    virtual void TriggerIds() = 0;
};

class TDatasetIterator {
public:
    virtual ui64 Rows() const = 0;
    virtual void RandomK(ui64 k, std::function<void(TRawEmbedding)> cb) = 0;
    virtual void IterateEmbedding(TReadCallback& cb) = 0;
    virtual void IterateId(TReadCallback& cb) = 0;
    virtual void IterateId(std::function<void(TId, TRawEmbedding)> cb) = 0;
};

using TCreateParentChild = std::function<void(TId, TId, TRawEmbedding)>;

template <typename T>
class TClusterizer final: TReadCallback {
public:
    using TEmbedding = std::span<const T>;
    using TDistance = std::function<float(TEmbedding, TEmbedding)>;

    TClusterizer(TDatasetIterator& it, TDistance distance, TCreateParentChild create, NVectorIndex::TThreadPool* tp = nullptr);

    struct TOptions {
        TId parentId = 0;
        ui32 maxIterations = 10;
        ui32 maxK = 10;
        bool normalize = false;
    };

    struct TClusters {
        std::vector<ui64> Count;
        std::vector<TId> Ids;
        std::vector<std::vector<T>> Coords;
    };

    TClusters Run(const TOptions& options);

private:
    void Handle(std::span<const TString> embeddings) final;
    void Handle(TRawEmbedding embedding) final;
    void Handle(TId id, TRawEmbedding embedding) final;

    void TriggerEmbeddings() final;
    void TriggerIds() final;

    void BadCluster(const TOptions& options);
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

    using TSum = std::conditional_t<std::is_integral_v<T>, int64_t, T>;
    struct TAggregatedCluster {
        float Distance = 0;
        std::vector<TSum> Coords;
        i64 Count = 0;
    };

    std::vector<TAggregatedCluster> AggregatedClusters;
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
