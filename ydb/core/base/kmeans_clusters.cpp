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
    using TSum = std::conditional_t<std::is_floating_point_v<TCoord>, double, i64>;
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

    const ui32 Dimensions = 0;
    const ui32 MaxRounds = 0;
    const ui8 TypeByte = 0;

    TVector<TString> Clusters;
    TVector<ui64> ClusterSizes;
    TVector<TEmbedding> NextClusters;
    TVector<ui64> NextClusterSizes;

    ui32 Round = 0;

public:
    TClusters(ui32 dimensions, ui32 maxRounds, ui8 typeByte)
        : Dimensions(dimensions)
        , MaxRounds(maxRounds)
        , TypeByte(typeByte)
    {
    }

    void SetRound(ui32 round) override {
        Round = round;
    }

    TString Debug() const override {
        auto sb = TStringBuilder() << "K: " << Clusters.size();
        if (MaxRounds) {
            sb << " Round: " << Round << " / " << MaxRounds;
        }
        return sb;
    }

    const TVector<TString>& GetClusters() const override {
        return Clusters;
    }

    const TVector<ui64>& GetClusterSizes() const override {
        return ClusterSizes;
    }

    const TVector<ui64>& GetNextClusterSizes() const override {
        return NextClusterSizes;
    }

    virtual void SetClusterSize(ui32 num, ui64 size) override {
        ClusterSizes.at(num) = size;
    }

    void Clear() override {
        Clusters.clear();
        ClusterSizes.clear();
        NextClusterSizes.clear();
        NextClusters.clear();
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
        Clusters = std::move(newClusters);
        ClusterSizes.clear();
        ClusterSizes.resize(Clusters.size());
        NextClusterSizes.clear();
        NextClusterSizes.resize(Clusters.size());
        NextClusters.clear();
        NextClusters.resize(Clusters.size());
        for (auto& aggregate : NextClusters) {
            aggregate.resize(Dimensions, 0);
        }
        return true;
    }

    bool RecomputeClusters() override {
        ui64 vectorCount = 0;
        ui64 reassignedCount = 0;
        for (size_t i = 0; auto& aggregate : NextClusters) {
            auto newSize = NextClusterSizes[i];
            vectorCount += newSize;

            auto clusterSize = ClusterSizes[i];
            reassignedCount += clusterSize < newSize ? newSize - clusterSize : 0;

            if (newSize != 0) {
                this->Fill(Clusters[i], aggregate.data(), newSize);
            }
            ++i;
        }

        Y_ENSURE(reassignedCount <= vectorCount);
        if (Clusters.size() == 1) {
            return true;
        }

        bool last = Round >= MaxRounds;
        if (!last && Round > 1) {
            const auto changes = static_cast<double>(reassignedCount) / static_cast<double>(vectorCount);
            last = changes < MinVectorsNeedsReassigned;
        }
        if (!last) {
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

    bool NextRound() override {
        bool isLast = RecomputeClusters();
        ClusterSizes = std::move(NextClusterSizes);
        RemoveEmptyClusters();
        if (isLast) {
            NextClusters.clear();
            return true;
        }
        ++Round;
        NextClusterSizes.clear();
        NextClusterSizes.resize(Clusters.size());
        NextClusters.clear();
        NextClusters.resize(Clusters.size());
        for (auto& aggregate : NextClusters) {
            aggregate.resize(Dimensions, 0);
        }
        return false;
    }

    std::optional<ui32> FindCluster(TArrayRef<const char> embedding) override {
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

    std::optional<ui32> FindCluster(TArrayRef<const TCell> row, ui32 embeddingPos) override {
        Y_ENSURE(embeddingPos < row.size());
        return FindCluster(row.at(embeddingPos).AsRef());
    }

    void AggregateToCluster(ui32 pos, const TArrayRef<const char>& embedding, ui64 weight) override {
        auto& aggregate = NextClusters.at(pos);
        auto* coords = aggregate.data();
        Y_ENSURE(IsExpectedSize(embedding));
        for (auto coord : this->GetCoords(embedding.data())) {
            *coords++ += (TSum)coord * weight;
        }
        NextClusterSizes.at(pos) += weight;
    }

    bool IsExpectedSize(const TArrayRef<const char>& data) override {
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
        const auto count = static_cast<TSum>(c);
        auto data = GetData(d.MutRef().data());
        for (auto& coord : data) {
            coord = *embedding / count;
            embedding++;
        }
    }
};

std::unique_ptr<IClusters> CreateClusters(const Ydb::Table::VectorIndexSettings& settings, ui32 maxRounds, TString& error) {
    if (settings.vector_dimension() < 1) {
        error = "Dimension of vector should be at least one";
        return nullptr;
    }

    const ui8 typeVal = (ui8)settings.vector_type();
    const ui32 dim = settings.vector_dimension();

    auto handleMetric = [&]<typename T>() -> std::unique_ptr<IClusters> {
        switch (settings.metric()) {
            case Ydb::Table::VectorIndexSettings::SIMILARITY_INNER_PRODUCT:
                return std::make_unique<TClusters<TMaxInnerProductSimilarity<T>>>(dim, maxRounds, typeVal);
            case Ydb::Table::VectorIndexSettings::SIMILARITY_COSINE:
            case Ydb::Table::VectorIndexSettings::DISTANCE_COSINE:
                // We don't need to have separate implementation for distance,
                // because clusters will be same as for similarity
                return std::make_unique<TClusters<TCosineSimilarity<T>>>(dim, maxRounds, typeVal);
            case Ydb::Table::VectorIndexSettings::DISTANCE_MANHATTAN:
                return std::make_unique<TClusters<TL1Distance<T>>>(dim, maxRounds, typeVal);
            case Ydb::Table::VectorIndexSettings::DISTANCE_EUCLIDEAN:
                return std::make_unique<TClusters<TL2Distance<T>>>(dim, maxRounds, typeVal);
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
