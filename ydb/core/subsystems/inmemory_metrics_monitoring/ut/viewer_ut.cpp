#include <ydb/core/subsystems/inmemory_metrics_monitoring/subsystem.h>
#include <ydb/core/subsystems/inmemory_metrics_monitoring/viewer.h>
#include <ydb/library/actors/core/subsystems/inmemory_metrics.h>
#include <ydb/library/actors/metrics/inmemory_backend.h>
#include <ydb/library/actors/metrics/lines/group_line_frontend.h>
#include <ydb/library/actors/metrics/lines/on_change_line_frontend.h>
#include <ydb/library/actors/testlib/test_runtime.h>
#include <library/cpp/json/json_reader.h>
#include <library/cpp/testing/unittest/registar.h>
#include <limits>

namespace NKikimr::NInMemoryMetricsMonitoring {
namespace {
using namespace NActors;

struct TTestFields {
    template<class TValue>
    struct TField {
        using TValueType = TValue;
        inline static constexpr std::array<TLineLabelView, 0> Labels = {};
    };
    struct TValue : TField<double> { static constexpr TStringBuf Name = "test.value"; };
    struct TCount : TField<ui64> { static constexpr TStringBuf Name = "test.count"; };
    using TFields = std::tuple<TValue, TCount>;
};
using TTestFrontend = TGroupLineFrontend<TTestFields>;

void Pump(TInMemoryMetricsBackend* backend) {
    backend->BeginMaintenance();
    backend->ProcessMaintenance();
}

NJson::TJsonValue Json(const TInMemorySnapshot& snapshot, const TInMemoryMetricsBackend& backend, bool history = true) {
    NJson::TJsonValue json;
    UNIT_ASSERT(NJson::ReadJsonTree(SerializeSnapshot(snapshot, backend.GetStats(), backend.GetConfig(),
        TInstant::Now(), TDuration::Minutes(5), history), &json, true));
    return json;
}

const NJson::TJsonValue& Line(const NJson::TJsonValue& json, TStringBuf name) {
    for (const auto& line : json["lines"].GetArray()) {
        if (line["name"].GetString() == name) {
            return line;
        }
    }
    UNIT_FAIL("Missing metric line");
    Y_UNREACHABLE();
}
} // namespace

Y_UNIT_TEST_SUITE(TInMemoryMetricsViewerTest) {
    Y_UNIT_TEST(ScalarTypesKeepValuesAndLabels) {
        TInMemoryMetricsBackend backend({.MemoryBytes = 64 * 1024, .MaxLines = 8});
        const std::array<TLabel, 1> labels = {{{"name", "<unsafe>"}}};
        auto integer = backend.CreateLine<TRawLineFrontend<ui64>>("integer", labels);
        auto signedInteger = backend.CreateLine<TRawLineFrontend<i64>>("signed", {});
        auto boolean = backend.CreateLine<TOnChangeLineFrontend<bool>>("boolean", {});
        auto number = backend.CreateLine<TRawLineFrontend<float>>("float", {});
        Pump(&backend);
        UNIT_ASSERT(integer.Append(std::numeric_limits<ui64>::max()));
        UNIT_ASSERT(signedInteger.Append(-42));
        UNIT_ASSERT(boolean.Append(true));
        UNIT_ASSERT(number.Append(1.5f));
        const auto snapshot = backend.CaptureSnapshot();
        const auto json = Json(snapshot, backend);
        UNIT_ASSERT_VALUES_EQUAL(Line(json, "integer")["points"][0]["values"][0].GetString(), "18446744073709551615");
        UNIT_ASSERT_VALUES_EQUAL(Line(json, "signed")["points"][0]["values"][0].GetString(), "-42");
        UNIT_ASSERT_VALUES_EQUAL(Line(json, "boolean")["points"][0]["values"][0].GetString(), "1");
        UNIT_ASSERT_VALUES_EQUAL(Line(json, "float")["points"][0]["values"][0].GetString(), "1.5");
        UNIT_ASSERT_VALUES_EQUAL(Line(json, "integer")["labels"][0]["value"].GetString(), "<unsafe>");
        const auto metadata = Json(snapshot, backend, false);
        UNIT_ASSERT(!Line(metadata, "integer").Has("points"));
        UNIT_ASSERT_VALUES_EQUAL(Line(metadata, "integer")["chunks"].GetUInteger(), 1);
    }

    Y_UNIT_TEST(GroupFieldsAndSelectedSnapshot) {
        TInMemoryMetricsBackend backend({.MemoryBytes = 64 * 1024, .MaxLines = 4});
        auto group = backend.CreateLine<TTestFrontend>("test.group", {});
        auto unrelated = backend.CreateLine("other", {});
        Pump(&backend);
        UNIT_ASSERT(group.Append({
            TTestFrontend::Value<TTestFields::TValue>(1.25),
            TTestFrontend::Value<TTestFields::TCount>(42),
        }));
        UNIT_ASSERT(unrelated.Append(10));
        const auto json = Json(backend.CaptureSnapshot(group.GetLineId()), backend);
        UNIT_ASSERT_VALUES_EQUAL(json["lines"].GetArray().size(), 1);
        const auto& line = Line(json, "test.group");
        UNIT_ASSERT_VALUES_EQUAL(line["fields"].GetArray().size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(line["fields"][0]["name"].GetString(), "test.value");
        UNIT_ASSERT_VALUES_EQUAL(line["points"][0]["values"][0].GetString(), "1.25");
        UNIT_ASSERT_VALUES_EQUAL(line["points"][0]["values"][1].GetString(), "42");
    }

    Y_UNIT_TEST(HistoryIsBoundedAndNonfiniteIsNull) {
        TInMemoryMetricsBackend backend({.MemoryBytes = 4 * 1024 * 1024, .ChunkSizeBytes = 64 * 1024, .MaxLines = 2});
        auto line = backend.CreateLine<TRawLineFrontend<double>>("number", {});
        Pump(&backend);
        for (size_t i = 0; i < MaxHistoryPoints + 10; ++i) {
            if (!line.Append(static_cast<double>(i))) {
                Pump(&backend);
                UNIT_ASSERT(line.Append(static_cast<double>(i)));
            }
        }
        UNIT_ASSERT(line.Append(std::numeric_limits<double>::quiet_NaN()));
        const auto json = Json(backend.CaptureSnapshot(), backend);
        const auto& output = Line(json, "number");
        UNIT_ASSERT(output["truncated"].GetBoolean());
        UNIT_ASSERT_VALUES_EQUAL(output["points"].GetArray().size(), MaxHistoryPoints);
        UNIT_ASSERT(output["points"].GetArray().back()["values"][0].IsNull());
        UNIT_ASSERT_VALUES_EQUAL(output["points"][0]["values"][0].GetString(), "11");
    }

    Y_UNIT_TEST(MissingLineProducesEmptyList) {
        TInMemoryMetricsBackend backend({.MemoryBytes = 4096, .MaxLines = 2});
        const auto json = Json(backend.CaptureSnapshot(123), backend);
        UNIT_ASSERT(json["lines"].GetArray().empty());
    }

    Y_UNIT_TEST(SubsystemStartsAndStopsWithRuntime) {
        for (ui32 i = 0; i < 2; ++i) {
            TActorId endpoint;
            TTestActorRuntimeBase runtime(1, true);
            runtime.SetupNodeSubSystems = [&](ui32, TActorSystemSetup* setup) {
                setup->RegisterSubSystem(MakeInMemoryMetricsRegistry({.MemoryBytes = 64 * 1024, .MaxLines = 8}));
                TConfig config;
                config.RegisterPage = [&](TActorSystem&, const TActorId& actor) { endpoint = actor; };
                setup->RegisterSubSystem(MakeInMemoryMetricsMonitoring(std::move(config)));
            };
            runtime.Initialize();
            UNIT_ASSERT(endpoint);
            UNIT_ASSERT(runtime.GetActorSystem(0)->GetSubSystem<TInMemoryMetricsMonitoring>());
        }
    }
}
} // namespace NKikimr::NInMemoryMetricsMonitoring
