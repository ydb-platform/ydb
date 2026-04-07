#include "kmeans_clusters.h"

#include <ydb/public/api/protos/ydb_table.pb.h>

#include <library/cpp/dot_product/dot_product.h>
#include <library/cpp/l1_distance/l1_distance.h>
#include <library/cpp/l2_distance/l2_distance.h>
#include <ydb/library/yql/udfs/common/knn/knn-defines.h>
#include <ydb/library/yql/udfs/common/knn/knn-distance.h>
#include <ydb/library/yql/udfs/common/knn/knn-serializer-shared.h>

#include <span>

namespace NKikimr::NKMeans {

namespace {
    constexpr ui64 MinVectorDimension = 1;
    constexpr ui64 MaxVectorDimension = 16384;
    constexpr ui64 MinLevels = 1;
    constexpr ui64 MaxLevels = 16;
    constexpr ui64 MinClusters = 2;
    constexpr ui64 MaxClusters = 2048;
    constexpr ui64 MaxClustersPowLevels = ui64(1) << 30;
    constexpr ui64 MaxVectorDimensionMultiplyClusters = ui64(4) << 20; // 4 bytes per dimension for float vector type ~= 16 MB

    bool ValidateSettingInRange(const TString& name, std::optional<ui64> value, ui64 minValue, ui64 maxValue, TString& error) {
        if (!value.has_value()) {
            error = TStringBuilder() << name << " should be set";
            return false;
        }

        if (minValue <= *value && *value <= maxValue) {
            return true;
        }

        error = TStringBuilder() << "Invalid " << name << ": " << *value << " should be between " << minValue << " and " << maxValue;
        return false;
    };

    Ydb::Table::VectorIndexSettings_Metric ParseDistance(const TString& distance_, TString& error) {
        const TString distance = to_lower(distance_);
        if (distance == "cosine")
            return Ydb::Table::VectorIndexSettings::DISTANCE_COSINE;
        else if (distance == "manhattan")
            return Ydb::Table::VectorIndexSettings::DISTANCE_MANHATTAN;
        else if (distance == "euclidean")
            return Ydb::Table::VectorIndexSettings::DISTANCE_EUCLIDEAN;
        else {
            error = TStringBuilder() << "Invalid distance: " << distance_;
            return Ydb::Table::VectorIndexSettings::METRIC_UNSPECIFIED;
        }
    };

    Ydb::Table::VectorIndexSettings_Metric ParseSimilarity(const TString& similarity_, TString& error) {
        const TString similarity = to_lower(similarity_);
        if (similarity == "cosine")
            return Ydb::Table::VectorIndexSettings::SIMILARITY_COSINE;
        else if (similarity == "inner_product")
            return Ydb::Table::VectorIndexSettings::SIMILARITY_INNER_PRODUCT;
        else {
            error = TStringBuilder() << "Invalid similarity: " << similarity_;
            return Ydb::Table::VectorIndexSettings::METRIC_UNSPECIFIED;
        }
    };

    Ydb::Table::VectorIndexSettings_VectorType ParseVectorType(const TString& vectorType_, TString& error) {
        const TString vectorType = to_lower(vectorType_);
        if (vectorType == "float")
            return Ydb::Table::VectorIndexSettings::VECTOR_TYPE_FLOAT;
        else if (vectorType == "uint8")
            return Ydb::Table::VectorIndexSettings::VECTOR_TYPE_UINT8;
        else if (vectorType == "int8")
            return Ydb::Table::VectorIndexSettings::VECTOR_TYPE_INT8;
        else if (vectorType == "bit")
            return Ydb::Table::VectorIndexSettings::VECTOR_TYPE_BIT;
        else {
            error = TStringBuilder() << "Invalid vector_type: " << vectorType_;
            return Ydb::Table::VectorIndexSettings::VECTOR_TYPE_UNSPECIFIED;
        }
    };

    ui32 ParseUInt32(const TString& name, const TString& value, ui64 minValue, ui64 maxValue, TString& error) {
        ui32 result = 0;
        if (!TryFromString(value, result)) {
            error = TStringBuilder() << "Invalid " << name << ": " << value;
            return result;
        }
        ValidateSettingInRange(name, result, minValue, maxValue, error);
        return result;
    }

    double ParseDouble(const TString& name, const TString& value, TString& error) {
        double result = 0;
        if (!TryFromString(value, result)) {
            error = TStringBuilder() << "Invalid " << name << ": " << value;
        }
        return result;
    }
}

// TODO(mbkkt) maybe compute floating sum in double? Needs benchmark
template <typename TCoord>
struct TMetric {
    using TCoord_ = TCoord;
    using TSum = std::conditional_t<std::is_floating_point_v<TCoord>, double, i64>;
};

template <typename TCoord>
struct TCosineDistance : TMetric<TCoord> {
    using TSum = typename TMetric<TCoord>::TSum;
    // double used to avoid precision issues
    using TRes = double;

    static TRes Init()
    {
        return std::numeric_limits<TRes>::max();
    }

    static auto Distance(const TStringBuf cluster, const TStringBuf embedding)
    {
        const TRes similarity = KnnDistance<TRes>::CosineSimilarity(cluster, embedding).value();
        return 1 - similarity;
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

    static auto Distance(const TStringBuf cluster, const TStringBuf embedding)
    {
        const auto distance = KnnDistance<TRes>::ManhattanDistance(cluster, embedding).value();
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

    static auto Distance(const TStringBuf cluster, const TStringBuf embedding)
    {
        const auto distance = KnnDistance<TRes>::EuclideanDistance(cluster, embedding).value();
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

    static auto Distance(const TStringBuf cluster, const TStringBuf embedding)
    {
        const TRes similarity = KnnDistance<TRes>::DotProduct(cluster, embedding).value();
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
    const ui8 FormatByte = 0;

    TVector<TString> Clusters;
    TVector<ui64> ClusterSizes;
    TVector<TEmbedding> NextClusters;
    TVector<ui64> NextClusterSizes;

    ui32 Round = 0;

public:
    TClusters(ui32 dimensions, ui32 maxRounds, ui8 formatByte)
        : Dimensions(dimensions)
        , MaxRounds(maxRounds)
        , FormatByte(formatByte)
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
            if (!IsExpectedFormat(cluster)) {
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

    void FindClusters(const TStringBuf embedding, std::vector<std::pair<ui32, double>>& clusters, size_t n, double skipRatio) override {
        if (!IsExpectedFormat(embedding)) {
            return;
        }
        clusters.clear();
        for (ui32 i = 0; const auto& cluster : Clusters) {
            auto cl = std::make_pair(i, (double)TMetric::Distance(cluster, embedding));
            auto it = std::lower_bound(clusters.begin(), clusters.end(), cl, [](const std::pair<ui32, double>& a, const std::pair<ui32, double>& b) {
                return a.second < b.second;
            });
            if (clusters.size() < n) {
                clusters.insert(it, cl);
            } else if (it != clusters.end()) {
                clusters.insert(it, cl);
                clusters.pop_back();
            }
            ++i;
        }
        if (skipRatio > 0 && clusters.size() > 1) {
            double thresh = (clusters[0].second < 0 ? clusters[0].second/skipRatio : clusters[0].second*skipRatio);
            for (ui32 i = 1; i < clusters.size(); i++) {
                if (clusters[i].second > thresh) {
                    clusters.resize(i);
                    break;
                }
            }
        }
    }

    std::optional<ui32> FindCluster(const TStringBuf embedding) override {
        if (!IsExpectedFormat(embedding)) {
            return {};
        }
        auto min = TMetric::Init();
        std::optional<ui32> closest = {};
        for (size_t i = 0; const auto& cluster : Clusters) {
            auto distance = TMetric::Distance(cluster, embedding);
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
        return FindCluster(row.at(embeddingPos).AsBuf());
    }

    double CalcDistance(const TStringBuf a, const TStringBuf b) override {
        return TMetric::Distance(a, b);
    }

    void AggregateToCluster(ui32 pos, const TArrayRef<const char>& embedding, ui64 weight) override {
        auto& aggregate = NextClusters.at(pos);
        auto* coords = aggregate.data();
        Y_ENSURE(IsExpectedFormat(embedding));

        if (IsBitQuantized()) {
            const ui8* data = reinterpret_cast<const ui8*>(embedding.data());
            for (size_t i = 0; i < Dimensions; ++i) {
                const bool coord = data[i / 8] & (1 << (i % 8));
                *coords++ += (TSum)coord * weight;
            }
        } else {
            for (const auto coord : this->GetCoords(embedding.data())) {
                *coords++ += (TSum)coord * weight;
            }
        }
        NextClusterSizes.at(pos) += weight;
    }

    bool IsExpectedFormat(const TArrayRef<const char>& data) override {
        if (!data.size() || FormatByte != data.back()) {
            return false;
        }

        if (IsBitQuantized()) {
            return data.size() >= 2 && Dimensions == (data.size() - 2) * 8 - data[data.size() - 2];
        }

        return data.size() == 1 + sizeof(TCoord) * Dimensions;
    }

    TString GetEmptyRow() const override {
        TString str;
        const size_t bufferSize = NKnnVectorSerialization::GetBufferSize<TCoord>(Dimensions);
        str.resize(bufferSize);
        str[bufferSize - HeaderLen] = FormatByte;
        if (IsBitQuantized()) {
            str[bufferSize - HeaderLen - 1] = 8 - Dimensions % 8;
        }
        return str;
    }

private:
    static constexpr bool IsBitQuantized() {
        return std::is_same_v<TCoord, bool>;
    }

    auto GetCoords(const char* coords) {
        return std::span{reinterpret_cast<const TCoord*>(coords), Dimensions};
    }

    auto GetData(char* data) {
        return std::span{reinterpret_cast<TCoord*>(data), Dimensions};
    }

    void Fill(TString& d, TSum* embedding, ui64& c) {
        Y_ENSURE(c > 0);
        const auto count = static_cast<TSum>(c);

        if (IsBitQuantized()) {
            ui8* const data = reinterpret_cast<ui8*>(d.MutRef().data());
            for (size_t i = 0; i < Dimensions; ++i) {
                if (i % 8 == 0) {
                    data[i / 8] = 0;
                }
                const bool bitValue = embedding[i] >= (count + 1) / 2;
                if (bitValue) {
                    data[i / 8] |= (1 << (i % 8));
                }
            }
        } else {
            auto data = GetData(d.MutRef().data());
            for (auto& coord : data) {
                coord = *embedding / count;
                embedding++;
            }
        }
    }
};

std::unique_ptr<IClusters> CreateClusters(const Ydb::Table::VectorIndexSettings& settings, ui32 maxRounds, TString& error) {
    if (!ValidateSettings(settings, error)) {
        return nullptr;
    }

    const ui32 dim = settings.vector_dimension();

    auto handleMetric = [&]<typename T>() -> std::unique_ptr<IClusters> {
        constexpr ui8 formatByte = Format<T>;
        switch (settings.metric()) {
            case Ydb::Table::VectorIndexSettings::SIMILARITY_INNER_PRODUCT:
                return std::make_unique<TClusters<TMaxInnerProductSimilarity<T>>>(dim, maxRounds, formatByte);
            case Ydb::Table::VectorIndexSettings::SIMILARITY_COSINE:
            case Ydb::Table::VectorIndexSettings::DISTANCE_COSINE:
                // We don't need to have separate implementation for distance,
                // because clusters will be same as for similarity
                return std::make_unique<TClusters<TCosineDistance<T>>>(dim, maxRounds, formatByte);
            case Ydb::Table::VectorIndexSettings::DISTANCE_MANHATTAN:
                return std::make_unique<TClusters<TL1Distance<T>>>(dim, maxRounds, formatByte);
            case Ydb::Table::VectorIndexSettings::DISTANCE_EUCLIDEAN:
                return std::make_unique<TClusters<TL2Distance<T>>>(dim, maxRounds, formatByte);
            default:
                error = TStringBuilder() << "Invalid metric: " << static_cast<int>(settings.metric());
                return nullptr;
        }
    };

    switch (settings.vector_type()) {
        case Ydb::Table::VectorIndexSettings::VECTOR_TYPE_FLOAT:
            return handleMetric.template operator()<float>();
        case Ydb::Table::VectorIndexSettings::VECTOR_TYPE_UINT8:
            return handleMetric.template operator()<ui8>();
        case Ydb::Table::VectorIndexSettings::VECTOR_TYPE_INT8:
            return handleMetric.template operator()<i8>();
        case Ydb::Table::VectorIndexSettings::VECTOR_TYPE_BIT:
            return handleMetric.template operator()<bool>();
        default:
            error = TStringBuilder() << "Invalid vector_type: " << static_cast<int>(settings.vector_type());
            return nullptr;
    }
}

std::unique_ptr<IClusters> CreateClustersAutoDetect(Ydb::Table::VectorIndexSettings settings, const TStringBuf& targetVector, ui32 maxRounds, TString& error) {
    if (targetVector.empty()) {
        error = "Target vector is empty";
        return nullptr;
    }

    const auto setLinearType = [&](Ydb::Table::VectorIndexSettings::VectorType type, size_t elementSize, TStringBuf typeName) -> bool {
        if (targetVector.size() < HeaderLen + elementSize) {
            error = TStringBuilder() << "Target vector too short for " << typeName << " type";
            return false;
        }
        settings.set_vector_type(type);
        settings.set_vector_dimension((targetVector.size() - HeaderLen) / elementSize);
        return true;
    };

    const ui8 formatByte = static_cast<ui8>(targetVector.back());
    switch (formatByte) {
        case EFormat::FloatVector:
            if (!setLinearType(Ydb::Table::VectorIndexSettings::VECTOR_TYPE_FLOAT, sizeof(float), "float")) {
                return nullptr;
            }
            break;
        case EFormat::Uint8Vector:
            if (!setLinearType(Ydb::Table::VectorIndexSettings::VECTOR_TYPE_UINT8, sizeof(ui8), "uint8")) {
                return nullptr;
            }
            break;
        case EFormat::Int8Vector:
            if (!setLinearType(Ydb::Table::VectorIndexSettings::VECTOR_TYPE_INT8, sizeof(i8), "int8")) {
                return nullptr;
            }
            break;
        case EFormat::BitVector: {
            if (targetVector.size() < HeaderLen + 2) {
                error = "Target vector too short for bit type";
                return nullptr;
            }
            const ui8 paddingBits = static_cast<ui8>(targetVector[targetVector.size() - 2]);
            const size_t payloadBits = (targetVector.size() - HeaderLen - 1) * 8;
            if (payloadBits < paddingBits) {
                error = "Invalid bit vector padding";
                return nullptr;
            }
            settings.set_vector_type(Ydb::Table::VectorIndexSettings::VECTOR_TYPE_BIT);
            settings.set_vector_dimension(payloadBits - paddingBits);
            break;
        }
        default:
            error = TStringBuilder() << "Unknown vector format byte: " << static_cast<int>(formatByte);
            return nullptr;
    }

    return CreateClusters(settings, maxRounds, error);
}

bool ValidateSettings(const Ydb::Table::KMeansTreeSettings& settings, TString& error) {
    error = "";

    if (auto unknownCount = settings.GetReflection()->GetUnknownFields(settings).field_count(); unknownCount > 0) {
        error = TStringBuilder() << "vector index settings contain " << unknownCount << " unsupported parameter(s)";
        return false;
    }

    if (!settings.has_settings()) {
        error = TStringBuilder() << "vector index settings should be set";
        return false;
    }

    if (!ValidateSettings(settings.settings(), error)) {
        return false;
    }

    if (!ValidateSettingInRange("levels",
        settings.has_levels() ? std::optional<ui64>(settings.levels()) : std::nullopt,
        MinLevels, MaxLevels,
        error))
    {
        return false;
    }

    if (!ValidateSettingInRange("clusters",
        settings.has_clusters() ? std::optional<ui64>(settings.clusters()) : std::nullopt,
        MinClusters, MaxClusters,
        error))
    {
        return false;
    }

    if (settings.has_overlap_clusters() &&
        settings.overlap_clusters() > settings.clusters()) {
        error = TStringBuilder() << "overlap_clusters should be less than or equal to clusters";
        return false;
    }

    if (settings.has_overlap_ratio() &&
        settings.overlap_ratio() < 0) {
        error = TStringBuilder() << "overlap_ratio should be >= 0";
        return false;
    }

    ui64 clustersPowLevels = 1;
    for (ui64 i = 0; i < settings.levels(); ++i) {
        clustersPowLevels *= settings.clusters();
        if (clustersPowLevels > MaxClustersPowLevels) {
            error = TStringBuilder() << "Invalid clusters^levels: " << settings.clusters() << "^" << settings.levels() << " should be less than " << MaxClustersPowLevels;
            return false;
        }
    }

    if (settings.settings().vector_dimension() * settings.clusters() > MaxVectorDimensionMultiplyClusters) {
        error = TStringBuilder() << "Invalid vector_dimension*clusters: " << settings.settings().vector_dimension() << "*" << settings.clusters()
            << " should be less than " << MaxVectorDimensionMultiplyClusters;
        return false;
    }

    error = "";
    return true;
}

bool ValidateSettings(const Ydb::Table::VectorIndexSettings& settings, TString& error) {
    if (auto unknownCount = settings.GetReflection()->GetUnknownFields(settings).field_count(); unknownCount > 0) {
        error = TStringBuilder() << "vector index settings contain " << unknownCount << " unsupported parameter(s)";
        return false;
    }

    if (!settings.has_metric() || settings.metric() == Ydb::Table::VectorIndexSettings::METRIC_UNSPECIFIED) {
        error = TStringBuilder() << "either distance or similarity should be set";
        return false;
    }
    if (!Ydb::Table::VectorIndexSettings::Metric_IsValid(settings.metric())) {
        error = TStringBuilder() << "Invalid metric: " << static_cast<int>(settings.metric());
        return false;
    }

    if (!settings.has_vector_type() || settings.vector_type() == Ydb::Table::VectorIndexSettings::VECTOR_TYPE_UNSPECIFIED) {
        error = TStringBuilder() << "vector_type should be set";
        return false;
    }
    if (!Ydb::Table::VectorIndexSettings::VectorType_IsValid(settings.vector_type())) {
        error = TStringBuilder() << "Invalid vector_type: " << static_cast<int>(settings.vector_type());
        return false;
    }

    if (!ValidateSettingInRange("vector_dimension",
        settings.has_vector_dimension() ? std::optional<ui64>(settings.vector_dimension()) : std::nullopt,
        MinVectorDimension, MaxVectorDimension,
        error))
    {
        Y_ASSERT(error);
        return false;
    }

    error = "";
    return true;
}

bool FillSetting(Ydb::Table::KMeansTreeSettings& settings, const TString& name, const TString& value, TString& error) {
    error = "";

    const TString nameLower = to_lower(name);
    if (nameLower == "distance") {
        if (settings.mutable_settings()->has_metric()) {
            error = "only one of distance or similarity should be set, not both";
            return false;
        }
        settings.mutable_settings()->set_metric(ParseDistance(value, error));
    } else if (nameLower == "similarity") {
        if (settings.mutable_settings()->has_metric()) {
            error = "only one of distance or similarity should be set, not both";
            return false;
        }
        settings.mutable_settings()->set_metric(ParseSimilarity(value, error));
    } else if (nameLower =="vector_type") {
        settings.mutable_settings()->set_vector_type(ParseVectorType(value, error));
    } else if (nameLower =="vector_dimension") {
        settings.mutable_settings()->set_vector_dimension(ParseUInt32(name, value, MinVectorDimension, MaxVectorDimension, error));
    } else if (nameLower =="clusters") {
        settings.set_clusters(ParseUInt32(name, value, MinClusters, MaxClusters, error));
    } else if (nameLower =="levels") {
        settings.set_levels(ParseUInt32(name, value, MinLevels, MaxLevels, error));
    } else if (nameLower == "overlap_clusters") {
        settings.set_overlap_clusters(ParseUInt32(name, value, MinClusters, MaxClusters, error));
    } else if (nameLower == "overlap_ratio") {
        settings.set_overlap_ratio(ParseDouble(name, value, error));
    } else {
        error = TStringBuilder() << "Unknown index setting: " << name;
        return false;
    }

    return !error;
}

void FilterOverlapRows(TVector<TSerializedCellVec>& rows, size_t distancePos, ui32 overlapClusters, double overlapRatio) {
    if (rows.size() <= 1) {
        return;
    }
    std::sort(rows.begin(), rows.end(), [&](const TSerializedCellVec& a, const TSerializedCellVec& b) {
        auto da = a.GetCells().at(distancePos).AsValue<double>();
        auto db = b.GetCells().at(distancePos).AsValue<double>();
        return da < db;
    });
    if (rows.size() > overlapClusters) {
        rows.resize(overlapClusters);
    }
    if (overlapRatio > 0) {
        auto thresh = rows[0].GetCells().at(distancePos).AsValue<double>();
        thresh = (thresh < 0 ? thresh/overlapRatio : thresh*overlapRatio);
        for (size_t i = 1; i < rows.size(); i++) {
            auto d = rows[i].GetCells().at(distancePos).AsValue<double>();
            if (d > thresh) {
                rows.resize(i);
                break;
            }
        }
    }
}

void FilterOverlapRows(TVector<std::pair<NTableIndex::NKMeans::TClusterId, double>>& rowClusters, ui32 overlapClusters, double overlapRatio) {
    if (rowClusters.size() <= 1) {
        return;
    }
    std::sort(rowClusters.begin(), rowClusters.end(),
        [&](const std::pair<NTableIndex::NKMeans::TClusterId, double>& a,
            const std::pair<NTableIndex::NKMeans::TClusterId, double>& b) {
            return a.second < b.second;
        });
    if (rowClusters.size() > overlapClusters) {
        rowClusters.resize(overlapClusters);
    }
    if (overlapRatio > 0) {
        double thresh = (rowClusters[0].second < 0 ? rowClusters[0].second/overlapRatio : rowClusters[0].second*overlapRatio);
        for (size_t i = 1; i < rowClusters.size(); i++) {
            if (rowClusters[i].second > thresh) {
                rowClusters.resize(i);
                break;
            }
        }
    }
}

}
