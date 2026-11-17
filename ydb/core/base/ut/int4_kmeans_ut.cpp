#include <ydb/core/base/kmeans_clusters.h>
#include <ydb/public/api/protos/ydb_table.pb.h>
#include <ydb/library/testlib/helpers.h>
#include <ydb/library/yql/udfs/common/knn/knn-serializer-shared.h>

#include <library/cpp/testing/unittest/registar.h>
#include <util/stream/str.h>
#include <cmath>

namespace NKikimr::NKMeans {
namespace {
    template <typename T>
    TString EncodeInt4(const TVector<int>& values) {
        TString bytes;
        TStringOutput output(bytes);
        NKnnVectorSerialization::TSerializer<T> serializer(&output);
        for (int value : values) {
            serializer.HandleElement(value);
        }
        serializer.Finish();
        return bytes;
    }
}
Y_UNIT_TEST_SUITE(NKMeansInt4) {
    Y_UNIT_TEST_TWIN(SettingsAndDetection, Signed) {
        using T = NVectorDistance::NPrivate::TInt4<Signed>;
        using TSettings = Ydb::Table::VectorIndexSettings;
        TString error;
        Ydb::Table::KMeansTreeSettings tree;
        UNIT_ASSERT_C(FillSetting(tree, "vector_type", Signed ? "Int4" : "Uint4", error), error);
        const auto type = Signed ? TSettings::VECTOR_TYPE_INT4 : TSettings::VECTOR_TYPE_UINT4;
        UNIT_ASSERT_VALUES_EQUAL(tree.settings().vector_type(), type);
        for (size_t count : {1u, 2u, 3u, 16u, 17u}) {
            TSettings settings;
            settings.set_metric(TSettings::DISTANCE_EUCLIDEAN);
            settings.set_vector_type(type);
            settings.set_vector_dimension(count);
            auto clusters = CreateClusters(settings, 1, error);
            UNIT_ASSERT_C(clusters, error);
            const auto data = EncodeInt4<T>(TVector<int>(count, T::MaxValue));
            UNIT_ASSERT(clusters->IsExpectedFormat(data));
            const auto empty = clusters->GetEmptyRow();
            UNIT_ASSERT_VALUES_EQUAL(empty.size(), (count + 1) / 2 + 2);
            UNIT_ASSERT(clusters->IsExpectedFormat(empty));
            NKnnVectorSerialization::TDeserializer<T> zeroDecoder(empty);
            zeroDecoder.DoDeserialize([](const T& value) { UNIT_ASSERT_VALUES_EQUAL(static_cast<int>(value), 0); });
            settings.clear_vector_type();
            settings.clear_vector_dimension();
            UNIT_ASSERT(AutoSelectVectorSettings(settings, data));
            UNIT_ASSERT_VALUES_EQUAL(settings.vector_type(), type);
            UNIT_ASSERT_VALUES_EQUAL(settings.vector_dimension(), count);
            auto detected = CreateClustersAutoDetect(settings, data, 1, error);
            UNIT_ASSERT_C(detected, error);
            UNIT_ASSERT(detected->IsExpectedFormat(data));
            auto invalid = data;
            invalid[invalid.size() - 2] = 2;
            UNIT_ASSERT(!clusters->IsExpectedFormat(invalid));
            UNIT_ASSERT(!CreateClustersAutoDetect(settings, invalid, 1, error));
            settings.clear_vector_dimension();
            UNIT_ASSERT(!AutoSelectVectorSettings(settings, invalid));
            UNIT_ASSERT_STRING_CONTAINS(clusters->FormatError(invalid), "padding");
        }
    }

    Y_UNIT_TEST_TWIN(DistancesAndCentroids, Signed) {
        using T = NVectorDistance::NPrivate::TInt4<Signed>;
        using TRef = std::conditional_t<Signed, i8, ui8>;
        using TSettings = Ydb::Table::VectorIndexSettings;
        const TVector<int> a = Signed ? TVector<int>{-8, -1, 7} : TVector<int>{1, 2, 3};
        const TVector<int> b = Signed ? TVector<int>{4, 3, -5} : TVector<int>{5, 2, 1};
        const auto first = EncodeInt4<T>(a), second = EncodeInt4<T>(b);
        for (auto metric : {TSettings::DISTANCE_EUCLIDEAN, TSettings::DISTANCE_MANHATTAN,
                TSettings::SIMILARITY_INNER_PRODUCT, TSettings::DISTANCE_COSINE, TSettings::SIMILARITY_COSINE}) {
            TSettings settings;
            settings.set_metric(metric);
            settings.set_vector_type(Signed ? TSettings::VECTOR_TYPE_INT4 : TSettings::VECTOR_TYPE_UINT4);
            settings.set_vector_dimension(3);
            TString error;
            auto clusters = CreateClusters(settings, 1, error);
            UNIT_ASSERT_C(clusters, error);
            settings.set_vector_type(Signed ? TSettings::VECTOR_TYPE_INT8 : TSettings::VECTOR_TYPE_UINT8);
            auto reference = CreateClusters(settings, 1, error);
            UNIT_ASSERT_C(reference, error);
            UNIT_ASSERT_DOUBLES_EQUAL(clusters->CalcDistance(first, second),
                reference->CalcDistance(EncodeInt4<TRef>(a), EncodeInt4<TRef>(b)), 1e-6);
            UNIT_ASSERT(clusters->SetClusters({first, second}));
            UNIT_ASSERT_VALUES_EQUAL(clusters->FindCluster(second).value(), 1);
            UNIT_ASSERT(clusters->SetClusters({clusters->GetEmptyRow()}));
            clusters->AggregateToCluster(0, first, 300000);
            clusters->AggregateToCluster(0, second, 100000);
            UNIT_ASSERT(clusters->NextRound());
            UNIT_ASSERT_VALUES_EQUAL(clusters->GetClusterSizes().front(), 400000);
            NKnnVectorSerialization::TDeserializer<T> decoder(clusters->GetClusters().front());
            TVector<int> centroid;
            decoder.DoDeserialize([&](const T& value) { centroid.push_back(value); });
            UNIT_ASSERT_VALUES_EQUAL(centroid.size(), 3);
            if (metric == TSettings::SIMILARITY_COSINE || metric == TSettings::DISTANCE_COSINE) {
                double an = 0, bn = 0;
                for (size_t i = 0; i < 3; ++i) { an += a[i] * a[i]; bn += b[i] * b[i]; }
                an = std::sqrt(an); bn = std::sqrt(bn);
                double sums[3], maxAbs = 0;
                for (size_t i = 0; i < 3; ++i) {
                    sums[i] = 3.0 * a[i] / an + b[i] / bn;
                    maxAbs = std::max(maxAbs, std::abs(sums[i]));
                }
                for (size_t i = 0; i < 3; ++i) {
                    UNIT_ASSERT_VALUES_EQUAL(centroid[i], static_cast<int>(std::round(sums[i] / maxAbs * T::MaxValue)));
                }
            } else {
                for (size_t i = 0; i < 3; ++i) {
                    UNIT_ASSERT_VALUES_EQUAL(centroid[i], (3 * a[i] + b[i]) / 4);
                }
            }
            const auto zero = EncodeInt4<T>({0, 0, 0});
            UNIT_ASSERT(clusters->SetClusters({zero}));
            clusters->AggregateToCluster(0, zero);
            UNIT_ASSERT(clusters->NextRound());
            UNIT_ASSERT_VALUES_EQUAL(clusters->GetClusters().front(), zero);
        }
    }
}
}
