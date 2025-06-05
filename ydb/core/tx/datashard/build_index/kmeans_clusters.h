#pragma once

#include <library/cpp/dot_product/dot_product.h>
#include <library/cpp/l1_distance/l1_distance.h>
#include <library/cpp/l2_distance/l2_distance.h>

#include <span>

namespace NKikimr::NDataShard::NKMeans {

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

template <typename TCoord>
bool IsExpectedSize(TArrayRef<const char> data, ui32 dimensions)
{
    return data.size() == 1 + sizeof(TCoord) * dimensions;
}

// TODO(mbkkt) separate implementation for bit
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
class TClusters {
    // If less than 1% of vectors are reassigned to new clusters we want to stop
    static constexpr double MinVectorsNeedsReassigned = 0.01;

    using TCoord = TMetric::TCoord_;
    using TSum = TMetric::TSum;
    using TEmbedding = TVector<TSum>;

    const ui32 InitK = 0;
    ui32 K = 0;
    const ui32 Dimensions = 0;

    TVector<TString> Clusters;
    TVector<ui64> ClusterSizes;

    struct TAggregatedCluster {
        TEmbedding Cluster;
        ui64 Size = 0;
    };
    TVector<TAggregatedCluster> AggregatedClusters;

    ui32 Round = 0;
    const ui32 MaxRounds = 0;

public:
    TClusters(ui32 k, ui32 dimensions, ui32 maxRounds)
        : InitK(k)
        , K(k)
        , Dimensions(dimensions)
        , MaxRounds(maxRounds)
    {}

    TClusters(TVector<TString>&& clusters, ui32 dimensions)
        : InitK(clusters.size())
        , K(clusters.size())
        , Dimensions(dimensions)
        , Clusters(clusters)
    {}

    ui32 GetK() const {
        return K;
    }

    TString Debug() const {
        if (!MaxRounds) {
            return TStringBuilder() << "K: " << K;
        }
        return TStringBuilder() << "K: " << K << " Round: " << Round << " / " << MaxRounds;
    }

    const TVector<TString>& GetClusters() const {
        return Clusters;
    }

    void Clear() {
        K = InitK;
        Clusters.clear();
        ClusterSizes.clear();
        AggregatedClusters.clear();
        Round = 0;
    }

    bool SetClusters(TVector<TString> && newClusters) {
        Clusters = newClusters;
        if (Clusters.size() == 0) {
            return false;
        }
        if (Clusters.size() < K) {
            // if this datashard have less than K valid embeddings for this parent
            // lets make single centroid for it
            K = 1;
            Clusters.resize(K);
        }
        Y_ENSURE(Clusters.size() == K);
        return true;
    }

    void InitAggregatedClusters() {
        AggregatedClusters.resize(K);
        ClusterSizes.resize(K, 0);
        for (auto& aggregate : AggregatedClusters) {
            aggregate.Cluster.resize(Dimensions, 0);
        }
        Round = 1;
    }

    bool RecomputeClusters() {
        Y_ENSURE(K >= 1);
        ui64 vectorCount = 0;
        ui64 reassignedCount = 0;
        for (size_t i = 0; auto& aggregate : AggregatedClusters) {
            vectorCount += aggregate.Size;

            auto& clusterSize = ClusterSizes[i];
            reassignedCount += clusterSize < aggregate.Size ? aggregate.Size - clusterSize : 0;
            clusterSize = aggregate.Size;

            if (aggregate.Size != 0) {
                this->Fill(Clusters[i], aggregate.Cluster.data(), aggregate.Size);
                Y_ENSURE(aggregate.Size == 0);
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
        return true;
    }

    std::optional<ui32> FindCluster(TArrayRef<const TCell> row, NTable::TPos embeddingPos)
    {
        Y_ENSURE(embeddingPos < row.size());
        const auto embedding = row.at(embeddingPos).AsRef();
        if (!IsExpectedSize<TCoord>(embedding, Dimensions)) {
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

    void AggregateToCluster(ui32 pos, const char* embedding)
    {
        auto& aggregate = AggregatedClusters[pos];
        auto* coords = aggregate.Cluster.data();
        for (auto coord : this->GetCoords(embedding)) {
            *coords++ += coord;
        }
        ++aggregate.Size;
    }

private:
    auto GetCoords(const char* coords)
    {
        return std::span{reinterpret_cast<const TCoord*>(coords), Dimensions};
    }

    auto GetData(char* data)
    {
        return std::span{reinterpret_cast<TCoord*>(data), Dimensions};
    }

    void Fill(TString& d, TSum* embedding, ui64& c)
    {
        Y_ENSURE(c > 0);
        const auto count = static_cast<TSum>(std::exchange(c, 0));
        auto data = GetData(d.MutRef().data());
        for (auto& coord : data) {
            coord = *embedding / count;
            *embedding++ = 0;
        }
    }
};

}
