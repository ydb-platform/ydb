#include "kmeans_clusters.h"

#include <library/cpp/dot_product/dot_product.h>
#include <library/cpp/l1_distance/l1_distance.h>
#include <library/cpp/l2_distance/l2_distance.h>

#include <span>

namespace NKikimr::NKMeans {

template <typename TRes>
Y_PURE_FUNCTION TTriWayDotProduct<TRes> CosineImpl(const float* lhs, const float* rhs, size_t length)
{
    auto r = TriWayDotProduct(lhs, rhs, length);
    return {static_cast<TRes>(r.LL), static_cast<TRes>(r.LR), static_cast<TRes>(r.RR)};
}

template <typename TRes>
Y_PURE_FUNCTION TTriWayDotProduct<TRes> CosineImpl(const i8* lhs, const i8* rhs, size_t length)
{
    const auto ll = DotProduct(lhs, lhs, length);
    const auto lr = DotProduct(lhs, rhs, length);
    const auto rr = DotProduct(rhs, rhs, length);
    return {static_cast<TRes>(ll), static_cast<TRes>(lr), static_cast<TRes>(rr)};
}

template <typename TRes>
Y_PURE_FUNCTION TTriWayDotProduct<TRes> CosineImpl(const ui8* lhs, const ui8* rhs, size_t length)
{
    const auto ll = DotProduct(lhs, lhs, length);
    const auto lr = DotProduct(lhs, rhs, length);
    const auto rr = DotProduct(rhs, rhs, length);
    return {static_cast<TRes>(ll), static_cast<TRes>(lr), static_cast<TRes>(rr)};
}

// TODO(mbkkt) maybe compute floating sum in double? Needs benchmark
template <typename TCoord>
struct TMetric {
    using TCoord_ = TCoord;
    using TSum = std::conditional_t<std::is_floating_point_v<TCoord>, TCoord, i64>;
};

template <typename TCoord>
struct TCosineSimilarity : TMetric<TCoord> {
    using TSum = typename TMetric<TCoord>::TSum;
    // double used to avoid precision issues
    using TRes = double;

    static TRes Init()
    {
        return std::numeric_limits<TRes>::max();
    }

    static auto Distance(const char* cluster, const char* embedding, ui32 dimensions)
    {
        const auto r = CosineImpl<TRes>(reinterpret_cast<const TCoord*>(cluster),
                                        reinterpret_cast<const TCoord*>(embedding), dimensions);
        // sqrt(ll) * sqrt(rr) computed instead of sqrt(ll * rr) to avoid precision issues
        const auto norm = std::sqrt(r.LL) * std::sqrt(r.RR);
        const TRes similarity = norm != 0 ? static_cast<TRes>(r.LR) / static_cast<TRes>(norm) : 0;
        return -similarity;
    }
};

template <typename TCoord>
struct TL1Distance : TMetric<TCoord> {
    using TSum = typename TMetric<TCoord>::TSum;
    using TRes = std::conditional_t<std::is_floating_point_v<TCoord>, TCoord, ui64>;

    static TRes Init()
    {
        return std::numeric_limits<TRes>::max();
    }

    static auto Distance(const char* cluster, const char* embedding, ui32 dimensions)
    {
        const auto distance = L1Distance(reinterpret_cast<const TCoord*>(cluster),
                                         reinterpret_cast<const TCoord*>(embedding), dimensions);
        return distance;
    }
};

template <typename TCoord>
struct TL2Distance : TMetric<TCoord> {
    using TSum = typename TMetric<TCoord>::TSum;
    using TRes = std::conditional_t<std::is_floating_point_v<TCoord>, TCoord, ui64>;

    static TRes Init()
    {
        return std::numeric_limits<TRes>::max();
    }

    static auto Distance(const char* cluster, const char* embedding, ui32 dimensions)
    {
        const auto distance = L2SqrDistance(reinterpret_cast<const TCoord*>(cluster),
                                            reinterpret_cast<const TCoord*>(embedding), dimensions);
        return distance;
    }
};

template <typename TCoord>
struct TMaxInnerProductSimilarity : TMetric<TCoord> {
    using TSum = typename TMetric<TCoord>::TSum;
    using TRes = std::conditional_t<std::is_floating_point_v<TCoord>, TCoord, i64>;

    static TRes Init()
    {
        return std::numeric_limits<TRes>::max();
    }

    static auto Distance(const char* cluster, const char* embedding, ui32 dimensions)
    {
        const TRes similarity = DotProduct(reinterpret_cast<const TCoord*>(cluster),
                                           reinterpret_cast<const TCoord*>(embedding), dimensions);
        return -similarity;
    }
};

template <typename TMetric>
class TClusters: public IClusters {
    // If less than 1% of vectors are reassigned to new clusters we want to stop
    static constexpr double MinVectorsNeedsReassigned = 0.01;

    using TCoord = TMetric::TCoord_;
    using TSum = TMetric::TSum;
    using TEmbedding = TVector<TSum>;

    ui32 InitK = 0;
    ui32 K = 0;
    const ui32 Dimensions = 0;
    const ui8 TypeByte = 0;

    TVector<TString> Clusters;
    TVector<ui64> ClusterSizes;
    TVector<TEmbedding> AggregatedClusters;
    TVector<ui64> NewClusterSizes;

    ui32 Round = 0;
    ui32 MaxRounds = 0;

public:
    TClusters(ui32 dimensions, ui8 typeByte)
        : Dimensions(dimensions)
        , TypeByte(typeByte)
    {
    }

    void Init(ui32 k, ui32 maxRounds) override {
        InitK = k;
        K = k;
        MaxRounds = maxRounds;
    }

    void SetRound(ui32 round) override {
        Round = round;
    }

    ui32 GetK() const override {
        return K;
    }

    TString Debug() const override {
        if (!MaxRounds) {
            return TStringBuilder() << "K: " << K;
        }
        return TStringBuilder() << "K: " << K << " Round: " << Round << " / " << MaxRounds;
    }

    const TVector<TString>& GetClusters() const override {
        return Clusters;
    }

    const TVector<ui64>& GetClusterSizes() const override {
        return ClusterSizes;
    }

    const TVector<ui64>& GetNewClusterSizes() const override {
        return NewClusterSizes;
    }

    virtual void SetOldClusterSize(ui32 num, ui64 size) override {
        if (num < K) {
            ClusterSizes[num] = size;
        }
    }

    void Clear() override {
        K = InitK;
        Clusters.clear();
        ClusterSizes.clear();
        AggregatedClusters.clear();
        Round = 0;
    }

    bool SetClusters(TVector<TString> && newClusters) override {
        if (newClusters.size() == 0) {
            return false;
        }
        for (const auto& cluster: newClusters) {
            if (!IsExpectedSize(cluster)) {
                return false;
            }
        }
        Clusters = newClusters;
        K = newClusters.size();
        return true;
    }

    void InitAggregatedClusters() override {
        if (AggregatedClusters.size() == K) {
            return;
        }
        AggregatedClusters.resize(K);
        NewClusterSizes.resize(K, 0);
        ClusterSizes.resize(K, 0);
        for (auto& aggregate : AggregatedClusters) {
            aggregate.resize(Dimensions, 0);
        }
        Round = 1;
    }

    void ResetAggregatedClusters() override {
        AggregatedClusters.clear();
        NewClusterSizes.clear();
        InitAggregatedClusters();
    }

    void RecomputeNoClear() override {
        Y_ENSURE(K >= 1);
        Y_ENSURE(AggregatedClusters.size() == K && NewClusterSizes.size() == K);
        if (Clusters.size() != K) {
            Clusters.resize(K);
            size_t expectedSize = 1 + sizeof(TCoord) * Dimensions;
            for (auto& cluster : Clusters) {
                cluster.resize(expectedSize, 0);
                cluster.MutRef().data()[expectedSize-1] = TypeByte;
            }
        }
        for (size_t i = 0; auto& aggregate : AggregatedClusters) {
            if (NewClusterSizes[i] != 0) {
                auto aggData = aggregate.data();
                auto data = GetData(Clusters[i].MutRef().data());
                for (auto& coord : data) {
                    coord = *aggData / NewClusterSizes[i];
                }
            }
            ++i;
        }
    }

    bool RecomputeClusters() override {
        Y_ENSURE(K >= 1);
        ui64 vectorCount = 0;
        ui64 reassignedCount = 0;
        for (size_t i = 0; auto& aggregate : AggregatedClusters) {
            auto& newSize = NewClusterSizes[i];
            vectorCount += newSize;

            auto& clusterSize = ClusterSizes[i];
            reassignedCount += clusterSize < newSize ? newSize - clusterSize : 0;
            clusterSize = newSize;

            if (newSize != 0) {
                this->Fill(Clusters[i], aggregate.data(), newSize);
                Y_ENSURE(newSize == 0);
            }
            ++i;
        }
        Y_ENSURE(vectorCount >= K);
        Y_ENSURE(reassignedCount <= vectorCount);
        if (K == 1) {
            return true;
        }

        bool last = Round >= MaxRounds;
        if (!last && Round > 1) {
            const auto changes = static_cast<double>(reassignedCount) / static_cast<double>(vectorCount);
            last = changes < MinVectorsNeedsReassigned;
        }
        if (!last) {
            ++Round;
            return false;
        }
        return true;
    }

    void RemoveEmptyClusters() override {
        size_t w = 0;
        for (size_t r = 0; r < ClusterSizes.size(); ++r) {
            if (ClusterSizes[r] != 0) {
                ClusterSizes[w] = ClusterSizes[r];
                Clusters[w] = std::move(Clusters[r]);
                ++w;
            }
        }
        ClusterSizes.erase(ClusterSizes.begin() + w, ClusterSizes.end());
        Clusters.erase(Clusters.begin() + w, Clusters.end());
    }

    std::optional<ui32> FindCluster(TArrayRef<const TCell> row, ui32 embeddingPos) override {
        Y_ENSURE(embeddingPos < row.size());
        const auto embedding = row.at(embeddingPos).AsRef();
        if (!IsExpectedSize(embedding)) {
            return {};
        }

        auto min = TMetric::Init();
        std::optional<ui32> closest = {};
        for (size_t i = 0; const auto& cluster : Clusters) {
            auto distance = TMetric::Distance(cluster.data(), embedding.data(), Dimensions);
            if (distance < min) {
                min = distance;
                closest = i;
            }
            ++i;
        }
        return closest;
    }

    void AggregateToCluster(ui32 pos, const char* embedding) override {
        auto& aggregate = AggregatedClusters[pos];
        auto* coords = aggregate.data();
        for (auto coord : this->GetCoords(embedding)) {
            *coords++ += coord;
        }
        ++NewClusterSizes[pos];
    }

    void AddAggregatedCluster(ui32 pos, const TString& embedding, ui64 size) override {
        auto& aggregate = AggregatedClusters.at(pos);
        auto* coords = aggregate.data();
        Y_ENSURE(IsExpectedSize(embedding));
        for (auto coord : this->GetCoords(embedding.data())) {
            *coords++ += (TSum)coord * size;
        }
        NewClusterSizes.at(pos) += size;
    }

    bool IsExpectedSize(TArrayRef<const char> data) override {
        return data.size() == 1 + sizeof(TCoord) * Dimensions;
    }

private:
    auto GetCoords(const char* coords) {
        return std::span{reinterpret_cast<const TCoord*>(coords), Dimensions};
    }

    auto GetData(char* data) {
        return std::span{reinterpret_cast<TCoord*>(data), Dimensions};
    }

    void Fill(TString& d, TSum* embedding, ui64& c) {
        Y_ENSURE(c > 0);
        const auto count = static_cast<TSum>(std::exchange(c, 0));
        auto data = GetData(d.MutRef().data());
        for (auto& coord : data) {
            coord = *embedding / count;
            *embedding++ = 0;
        }
    }
};

std::unique_ptr<IClusters> CreateClusters(const Ydb::Table::VectorIndexSettings& settings, TString& error) {
    if (settings.vector_dimension() < 1) {
        error = "Dimension of vector should be at least one";
        return nullptr;
    }

    const ui8 typeVal = (ui8)settings.vector_type();
    const ui32 dim = settings.vector_dimension();

    auto handleMetric = [&]<typename T>() -> std::unique_ptr<IClusters> {
        switch (settings.metric()) {
            case Ydb::Table::VectorIndexSettings::SIMILARITY_INNER_PRODUCT:
                return std::make_unique<TClusters<TMaxInnerProductSimilarity<T>>>(dim, typeVal);
            case Ydb::Table::VectorIndexSettings::SIMILARITY_COSINE:
            case Ydb::Table::VectorIndexSettings::DISTANCE_COSINE:
                // We don't need to have separate implementation for distance,
                // because clusters will be same as for similarity
                return std::make_unique<TClusters<TCosineSimilarity<T>>>(dim, typeVal);
            case Ydb::Table::VectorIndexSettings::DISTANCE_MANHATTAN:
                return std::make_unique<TClusters<TL1Distance<T>>>(dim, typeVal);
            case Ydb::Table::VectorIndexSettings::DISTANCE_EUCLIDEAN:
                return std::make_unique<TClusters<TL2Distance<T>>>(dim, typeVal);
            default:
                error = "Wrong similarity";
                break;
        }
        return nullptr;
    };

    switch (settings.vector_type()) {
        case Ydb::Table::VectorIndexSettings::VECTOR_TYPE_FLOAT:
            return handleMetric.template operator()<float>();
        case Ydb::Table::VectorIndexSettings::VECTOR_TYPE_UINT8:
            return handleMetric.template operator()<ui8>();
        case Ydb::Table::VectorIndexSettings::VECTOR_TYPE_INT8:
            return handleMetric.template operator()<i8>();
        case Ydb::Table::VectorIndexSettings::VECTOR_TYPE_BIT:
            error = "TODO(mbkkt) bit vector type is not supported";
            break;
        default:
            error = "Wrong vector type";
            break;
    }

    return nullptr;
}

}
