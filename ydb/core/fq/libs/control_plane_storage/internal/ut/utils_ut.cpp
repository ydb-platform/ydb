#include <library/cpp/testing/unittest/registar.h>
#include <ydb/core/fq/libs/control_plane_storage/internal/utils.h>
#include <ydb/core/fq/libs/control_plane_storage/proto/yq_internal.pb.h>

namespace NFq {

namespace {
void ValidateStats(std::string_view statisticsStr, const std::unordered_map<std::string_view, i64>& expected) {
    FederatedQuery::Internal::QueryInternal internal;
    auto statisticsPtr = internal.mutable_statistics();
    PackStatisticsToProtobuf(*statisticsPtr, statisticsStr);

    for (const auto& statsElement : *statisticsPtr) {
        const auto& name = statsElement.name();
        auto value = statsElement.value();

        auto it = expected.find(name);
        UNIT_ASSERT(it != expected.end());
        UNIT_ASSERT_EQUAL(value, it->second);
    }
    UNIT_ASSERT_EQUAL(expected.size(), static_cast<size_t>(statisticsPtr->size()));
}
}

Y_UNIT_TEST_SUITE(ParseStats) {

    Y_UNIT_TEST(ParseWithSources) {
        auto v1S3Source = NResource::Find("v1_s3source.json");
        auto v2S3Source = NResource::Find("v2_s3source.json");

        std::unordered_map<std::string_view, i64> expectedS3Source{
            {"IngressBytes", 53},
            {"InputBytes", 30},
            {"OutputBytes", 60},
            {"S3Source", 53}};

        ValidateStats(v1S3Source, expectedS3Source);
        ValidateStats(v2S3Source, expectedS3Source);
    }

    Y_UNIT_TEST(ParseJustOutput) {
        auto v1Output = NResource::Find("v1_output.json");
        auto v2Output = NResource::Find("v2_output.json");

        std::unordered_map<std::string_view, i64> expectedOutput{{"OutputBytes", 3}};

        ValidateStats(v1Output, expectedOutput);
        ValidateStats(v2Output, expectedOutput);
    }

    Y_UNIT_TEST(ParseMultipleGraphsV1) {
        auto v1TwoResults = NResource::Find("v1_two_results.json");
        std::unordered_map<std::string_view, i64> expectedOutput{
            {"OutputBytes", 129},
            {"InputBytes", 76},
            {"IngressBytes", 106},
            {"S3Source", 106}
        };
        ValidateStats(v1TwoResults, expectedOutput);
    }

    Y_UNIT_TEST(ParseMultipleGraphsV2) {
        auto v2TwoResults = NResource::Find("v2_two_results.json");
        std::unordered_map<std::string_view, i64> expectedOutput{
            {"OutputBytes", 106},
            {"InputBytes", 53},
            {"IngressBytes", 106},
            {"S3Source", 106}
        };
        ValidateStats(v2TwoResults, expectedOutput);
    }
}
}