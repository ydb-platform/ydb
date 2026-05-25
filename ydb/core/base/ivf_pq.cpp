#include "ivf_pq.h"
#include "kmeans_clusters.h"

#include <util/string/builder.h>
#include <util/string/cast.h>

#include <optional>

namespace NKikimr::NIvfPq {

namespace {

    constexpr ui64 MinVectorDimension = 1;
    constexpr ui64 MaxVectorDimension = 16384;
    constexpr ui64 MinLevels = 1;
    constexpr ui64 MaxLevels = 16;
    constexpr ui64 MinClusters = 2;
    constexpr ui64 MaxClusters = 2048;
    constexpr ui64 MaxClustersPowLevels = ui64(1) << 30;
    constexpr ui64 MaxVectorDimensionMultiplyClusters = ui64(4) << 20;
    constexpr ui64 MinSubspaces = 1;
    constexpr ui64 MaxSubspaces = 1024;
    constexpr ui64 MinSubspaceBits = 1;
    constexpr ui64 MaxSubspaceBits = 12;
    constexpr ui64 MaxCodebookEntries = ui64(1) << 20;
    
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
    }

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

} // namespace

bool FillSetting(Ydb::Table::IvfPqSettings& settings, const TString& name, const TString& value, TString& error) {
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
    } else if (nameLower == "vector_type") {
        settings.mutable_settings()->set_vector_type(ParseVectorType(value, error));
    } else if (nameLower == "vector_dimension") {
        settings.mutable_settings()->set_vector_dimension(ParseUInt32(name, value, MinVectorDimension, MaxVectorDimension, error));
    } else if (nameLower == "kmeans_tree_clusters") {
        settings.mutable_kmeans_tree_settings()->set_clusters(ParseUInt32(name, value, MinClusters, MaxClusters, error));
    } else if (nameLower =="kmeans_tree_levels") {
        settings.mutable_kmeans_tree_settings()->set_levels(ParseUInt32(name, value, MinLevels, MaxLevels, error));
    } else if (nameLower == "kmeans_tree_overlap_clusters") {
        settings.mutable_kmeans_tree_settings()->set_overlap_clusters(ParseUInt32(name, value, MinClusters, MaxClusters, error));
    } else if (nameLower == "kmeans_tree_overlap_ratio") {
        settings.mutable_kmeans_tree_settings()->set_overlap_ratio(ParseDouble(name, value, error));
    } else if (nameLower == "subspaces") {
        settings.set_subspaces(ParseUInt32(name, value, MinSubspaces, MaxSubspaces, error));
    } else if (nameLower == "subspace_bits") {
        settings.set_subspace_bits(ParseUInt32(name, value, MinSubspaceBits, MaxSubspaceBits, error));
    } else{
        error = TStringBuilder() << "Unknown index setting: " << name;
        return false;
    }
    
    return !error;
}

bool ValidateSettings(const Ydb::Table::IvfPqSettings& settings, TString& error) {
    error = "";

    if (auto unknownCount = settings.GetReflection()->GetUnknownFields(settings).field_count(); unknownCount > 0) {
        error = TStringBuilder() << "ivf_pq index settings contain " << unknownCount << " unsupported parameter(s)";
        return false;
    }

    if (!settings.has_settings()) {
        error = "vector index settings should be set";
        return false;
    }
    if (!NKMeans::ValidateSettings(settings.settings(), error)) {
        return false;
    }

    if (settings.settings().vector_type() != Ydb::Table::VectorIndexSettings::VECTOR_TYPE_FLOAT) {
        error = "ivf_pq index supports only vector_type=float";
        return false;
    }

    if (!ValidateSettingInRange("subspaces",
        settings.has_subspaces() ? std::optional<ui64>(settings.subspaces()) : std::nullopt,
        MinSubspaces, MaxSubspaces, error))
    {
        return false;
    }

    if (!ValidateSettingInRange("subspace_bits",
        settings.has_subspace_bits() ? std::optional<ui64>(settings.subspace_bits()) : std::nullopt,
        MinSubspaceBits, MaxSubspaceBits, error))
    {
        return false;
    }

    if (settings.settings().vector_dimension() % settings.subspaces() != 0) {
        error = TStringBuilder() << "vector_dimension (" << settings.settings().vector_dimension()
            << ") must be a multiple of subspaces (" << settings.subspaces() << ")";
        return false;
    }

    const ui64 codebookEntries = ui64(settings.subspaces()) << settings.subspace_bits();
    if (codebookEntries > MaxCodebookEntries) {
        error = TStringBuilder() << "subspaces * 2^subspace_bits (" << settings.subspaces()
            << " * 2^" << settings.subspace_bits() << " = " << codebookEntries
            << ") should be less than or equal to " << MaxCodebookEntries;
        return false;
    }

    if (settings.ivf_type_case() != Ydb::Table::IvfPqSettings::kKmeansTreeSettings) {
        error = "ivf_pq requires kmeans_tree_settings to be set";
        return false;
    }
    const auto& kmeans = settings.kmeans_tree_settings();
    if (!ValidateSettingInRange("kmeans_tree_levels",
        kmeans.has_levels() ? std::optional<ui64>(kmeans.levels()) : std::nullopt,
        MinLevels, MaxLevels, error))
    {
        return false;
    }
    if (!ValidateSettingInRange("kmeans_tree_clusters",
        kmeans.has_clusters() ? std::optional<ui64>(kmeans.clusters()) : std::nullopt,
        MinClusters, MaxClusters, error))
    {
        return false;
    }
    if (kmeans.has_overlap_clusters() && kmeans.overlap_clusters() > kmeans.clusters()) {
        error = "kmeans_tree_overlap_clusters should be less than or equal to kmeans_tree_clusters";
        return false;
    }
    if (kmeans.has_overlap_ratio() && kmeans.overlap_ratio() < 0) {
        error = "kmeans_tree_overlap_ratio should be >= 0";
        return false;
    }

    ui64 clustersPowLevels = 1;
    for (ui64 i = 0; i < kmeans.levels(); ++i) {
        clustersPowLevels *= kmeans.clusters();
        if (clustersPowLevels > MaxClustersPowLevels) {
            error = TStringBuilder() << "Invalid kmeans_tree_clusters^kmeans_tree_levels: "
                << kmeans.clusters() << "^" << kmeans.levels()
                << " should be less than " << MaxClustersPowLevels;
            return false;
        }
    }
    if (ui64(settings.settings().vector_dimension()) * kmeans.clusters() > MaxVectorDimensionMultiplyClusters) {
        error = TStringBuilder() << "Invalid vector_dimension*kmeans_tree_clusters: "
            << settings.settings().vector_dimension() << "*" << kmeans.clusters()
            << " should be less than " << MaxVectorDimensionMultiplyClusters;
        return false;
    }

    return true;
}

}