#include <gtest/gtest.h>
#include <gmock/gmock.h>

#include <google/protobuf/text_format.h>

#include <yt/yt/library/profiling/solomon/encoder.h>

#include <library/cpp/monlib/metrics/metric_registry.h>

namespace NYT::NProfiling {
namespace {

////////////////////////////////////////////////////////////////////////////////

TEST(TSensorEncoder, Encode)
{
    // Fill metric registry.
    NMonitoring::TMetricRegistry registry{{{"common", "label"}}};

    auto* g = registry.Gauge({{"sensor", "gauge"}, {"gauge", "1"}});
    g->Set(50.);

    auto* r = registry.Rate({{"sensor", "rate"}, {"rate", "2"}});
    r->Add(500);

    auto* hist = registry.HistogramCounter(
        {{"sensor", "hist"}, {"hist", "3"}, {"a", "b"}},
        NMonitoring::ExplicitHistogram({10, 20, 30}));
    hist->Record(5.);
    hist->Record(15., 3);

    auto* histRate = registry.HistogramRate(
        {{"sensor", "histRate"}, {"histRate", "4"}},
        NMonitoring::ExplicitHistogram({100, 1000}));
    histRate->Record(1, 1);
    histRate->Record(500, 2);
    histRate->Record(10000, 3);

    // Encode.
    TSensorEncoder encoder("sensor", "/");
    registry.Accept(TInstant::Zero(), &encoder);
    encoder.Close();

    auto sensorDump = encoder.BuildSensorDump();

    TString sensorDumpText;
    EXPECT_TRUE(NProtoBuf::TextFormat::PrintToString(sensorDump, &sensorDumpText));
    Cerr << "Sensor dump:\n" << sensorDumpText << Endl;

    // Check result.
    const TTag dummyTag;
    const i64 mcsFactor = 1'000'000;

    std::vector<TTag> tagById(sensorDump.tags_size());
    for (int i = 0; i < sensorDump.tags_size(); ++i) {
        const auto& tagProto = sensorDump.tags().Get(i);
        tagById[i] = {tagProto.key(), tagProto.value()};

        if (i == 0) {
            EXPECT_EQ(dummyTag, tagById[0]) << "dummy tag must be first";
        }
    }

    // Includes dummy tag for consistency between tagId and tag index.
    EXPECT_THAT(tagById, testing::UnorderedElementsAre(
        dummyTag, TTag{"common", "label"}, TTag{"gauge", "1"},
        TTag{"rate", "2"}, TTag{"hist", "3"}, TTag{"histRate", "4"}, TTag{"a", "b"}
    ));

    auto getProjectionTags = [&tagById] (const NProto::TProjection& projection) {
        std::vector<TTag> tags;
        tags.reserve(projection.tag_ids_size());
        for (int i = 0; i < projection.tag_ids_size(); ++i) {
            auto tagId = projection.tag_ids().Get(i);
            tags.push_back(tagById.at(tagId));
        }
        return tags;
    };

    EXPECT_EQ(4, sensorDump.cubes_size());
    for (int i = 0; i < sensorDump.cubes_size(); ++i) {
        const auto& cube = sensorDump.cubes().Get(i);
        const auto& sensorName = cube.name();

        EXPECT_EQ(1, cube.projections_size()) << "Unexpected projections count for sensor " << sensorName;
        if (cube.projections_size() != 1) {
            continue;
        }

        const auto& projection = cube.projections().Get(0);
        EXPECT_TRUE(projection.has_value()) << "Projection for sensor " << sensorName << " does not have value";

        auto expectTags = [&] (std::initializer_list<TTag> expectedTags) {
            EXPECT_THAT(getProjectionTags(projection), testing::UnorderedElementsAreArray(expectedTags))
                << "Tags do not match for sensor " << sensorName;
        };

        if (sensorName == "/gauge") {
            expectTags({TTag{"common", "label"}, TTag{"gauge", "1"}});

            if (!projection.has_gauge()) {
                ADD_FAILURE() << "Sensor " << sensorName << "does not have gauge value";
                continue;
            }

            EXPECT_NEAR(50, projection.gauge(), 1e-9)
                << "Gauge value does not match for sensor " << sensorName;
        } else if (sensorName == "/rate") {
            expectTags({TTag{"common", "label"}, TTag{"rate", "2"}});

            if (!projection.has_counter()) {
                ADD_FAILURE() << "Sensor " << sensorName << "does not have counter value";
                continue;
            }

            EXPECT_EQ(500, projection.counter()) << "Counter value does not match for sensor " << sensorName;
        } else if (sensorName == "/hist") {
            expectTags({TTag{"common", "label"}, TTag{"hist", "3"}, TTag{"a", "b"}});

            if (!projection.has_gauge_histogram()) {
                ADD_FAILURE() << "Sensor " << sensorName << "does not have gauge histogram value";
                continue;
            }

            auto& hist = projection.gauge_histogram();

            EXPECT_THAT(hist.values(), testing::ElementsAre(1, 3, 0, 0))
                << "Gauge histogram values do not match for sensor " << sensorName;

            EXPECT_THAT(hist.times(), testing::ElementsAre(10 * mcsFactor, 20 * mcsFactor, 30 * mcsFactor))
                << "Gauge histogram times do not match for sensor " << sensorName;
        } else if (sensorName == "/histRate") {
            expectTags({TTag{"common", "label"}, {"histRate", "4"}});

            if (!projection.has_rate_histogram()) {
                ADD_FAILURE() << "Sensor " << sensorName << "does not have rate histogram value";
                continue;
            }

            auto& hist = projection.rate_histogram();

            EXPECT_THAT(hist.values(), testing::ElementsAre(1, 2, 3))
                << "Rate histogram values do not match for sensor " << sensorName;

            EXPECT_THAT(hist.times(), testing::ElementsAre(100 * mcsFactor, 1000 * mcsFactor))
                << "Rate histogram times do not match for sensor " << sensorName;
        } else {
            ADD_FAILURE() << "Unexpected sensor name: " << sensorName;
        }
    }
}

TEST(TSensorEncoder, EncodeSameSensorWithDifferentTypes)
{
    // Fill metric registry.
    NMonitoring::TMetricRegistry registry{{{"common", "label"}}};

    auto* g = registry.Gauge({{"sensor", "bad"}, {"gauge", "1"}});
    g->Set(50.);

    auto* r = registry.Rate({{"sensor", "bad"}, {"rate", "2"}});
    r->Add(500);

    // Check encoder.
    EXPECT_THAT(
        [&] {
            TSensorEncoder encoder("sensor");
            registry.Accept(TInstant::Zero(), &encoder);
        },
        testing::Throws<std::exception>()
    );
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NProfiling
