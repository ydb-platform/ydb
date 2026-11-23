#include "actor_benchmark_helper.h"
#include "subsystems/metric_system.h"
#include "harmonizer/harmonizer.h"
#include "harmonizer/harmonizer_metrics.h"

#include <library/cpp/testing/unittest/registar.h>
#include <array>
#include <cstring>

using namespace NActors;
using namespace NActors::NTests;

namespace {
    // Deliberately independent of TInMemoryMetricsBackend and its writer state.
    class TRecordingLine final : public IMetricLine {
    public:
        bool Accept = true;
        ui32 CloseCount = 0;
        TVector<ui64> Values;
        struct TSample {
            std::array<char, 128> Payload;
            ui32 UsedBytes;
        };
        TVector<TSample> Samples;

        bool IsValid() const noexcept override { return true; }
        void Close() noexcept override { ++CloseCount; }
        ui32 GetLineId() const noexcept override { return CloseCount ? 0 : 42; }
        NHPTimer::STime CurrentTimestampTs() const noexcept override { return 123; }
        bool AccessChunkMemory(void* opaque, TAccessChunkMemoryFn access) noexcept override {
            if (!Accept || CloseCount) {
                return false;
            }
            // A fresh small chunk for each sample is sufficient for this mock.
            alignas(ui64) std::array<char, 128> payload{};
            TWritableChunkMemory memory{.Payload = payload};
            if (!access(opaque, memory)) {
                return false;
            }
            Samples.push_back({payload, memory.UsedPayloadBytes});
            return true;
        }
        std::optional<ui64> GetLastMaterializedValue() const noexcept override {
            return CloseCount || Values.empty() ? std::nullopt : std::optional<ui64>(Values.back());
        }
        void MarkMaterialized(ui64 value) noexcept override { Values.push_back(value); }
    };

    class TRecordingMetricSystem final : public IMetricSystem {
    public:
        struct TEntry {
            TLineKey Key;
            TLineMeta Meta;
            std::shared_ptr<TRecordingLine> Line;
        };
        TVector<TEntry> Entries;
        TVector<TLabel> CommonLabels;
        bool Reject = false;

        bool SetCommonLabels(std::span<const TLabel> labels) override {
            CommonLabels.assign(labels.begin(), labels.end());
            return true;
        }

    private:
        std::shared_ptr<IMetricLine> CreateLineWithMeta(TStringBuf name, std::span<const TLabel> labels, const TLineMeta& meta) override {
            if (Reject) {
                return {};
            }
            auto line = std::make_shared<TRecordingLine>();
            Entries.push_back({MakeLineKey(name, labels), meta, line});
            return line;
        }
    };
}

Y_UNIT_TEST_SUITE(MetricSystem) {
    Y_UNIT_TEST(IndependentImplementationAndLineOwnership) {
        TRecordingMetricSystem recording;
        IMetricSystem& system = recording;
        const TVector<TLabel> labels{{"pool", "test"}};
        UNIT_ASSERT(system.SetCommonLabels(labels));
        UNIT_ASSERT_VALUES_EQUAL(recording.CommonLabels[0].Value, "test");
        {
            auto raw = system.CreateLine<TRawLineFrontend<float>>("raw", labels);
            auto changed = system.CreateLine<TOnChangeLineFrontend<bool>>("changed", {});
            UNIT_ASSERT(raw && changed);
            UNIT_ASSERT_VALUES_EQUAL(raw.GetLineId(), 42);
            UNIT_ASSERT_VALUES_EQUAL(recording.Entries[0].Key.Name, "raw");
            UNIT_ASSERT_VALUES_EQUAL(recording.Entries[0].Meta.FrontendName(), "raw");
            UNIT_ASSERT_VALUES_EQUAL(recording.Entries[1].Meta.FrontendName(), "on_change");
            UNIT_ASSERT_VALUES_EQUAL(recording.Entries[0].Key.Labels[0].Value, "test");
            UNIT_ASSERT(raw.Append(1.5f));
            UNIT_ASSERT(raw.Append(1.5f));
            UNIT_ASSERT_VALUES_EQUAL(recording.Entries[0].Line->Values.size(), 2);
            UNIT_ASSERT_VALUES_EQUAL(TRawLineFrontend<float>::DecodeValue(recording.Entries[0].Line->Values[0]), 1.5f);
            UNIT_ASSERT(changed.Append(true));
            UNIT_ASSERT(changed.Append(true));
            UNIT_ASSERT_VALUES_EQUAL(recording.Entries[1].Line->Values.size(), 1);
            recording.Entries[1].Line->Accept = false;
            UNIT_ASSERT(!changed.Append(false));
            UNIT_ASSERT_VALUES_EQUAL(recording.Entries[1].Line->Values.size(), 1);
            recording.Entries[1].Line->Accept = true;
            UNIT_ASSERT(changed.Append(false));
            UNIT_ASSERT_VALUES_EQUAL(recording.Entries[1].Line->Values.size(), 2);

            auto moved = std::move(raw);
            UNIT_ASSERT(!raw);
            UNIT_ASSERT(!raw.Append(2.0f));
            UNIT_ASSERT(moved.Append(2.0f));
            auto replacement = system.CreateLine<TRawLineFrontend<float>>("replacement", {});
            replacement = std::move(moved);
            UNIT_ASSERT_VALUES_EQUAL(recording.Entries[2].Line->CloseCount, 1);
            replacement.Close();
            replacement.Close();
            UNIT_ASSERT(!replacement.Append(3.0f));
            UNIT_ASSERT_VALUES_EQUAL(replacement.GetLineId(), 0);
        }
        for (const auto& entry : recording.Entries) {
            UNIT_ASSERT_VALUES_EQUAL(entry.Line->CloseCount, 1);
        }
        recording.Reject = true;
        auto rejected = system.CreateLine("rejected", {});
        UNIT_ASSERT(!rejected);
        UNIT_ASSERT(!rejected.Append(1));
    }

    Y_UNIT_TEST(HarmonizerUsesReplacementAndClosesBeforeStop) {
        TTestActorRuntimeBase runtime;
        TRecordingMetricSystem* recording = nullptr;
        runtime.SetupNodeSubSystems = [&](ui32, TActorSystemSetup* setup) {
            auto mock = std::make_unique<TRecordingMetricSystem>();
            recording = mock.get();
            setup->RegisterSubSystem<IMetricSystem>(std::move(mock));
        };
        runtime.Initialize();
        auto* actorSystem = runtime.GetActorSystem(0);
        UNIT_ASSERT_VALUES_EQUAL(GetMetricSystem(*actorSystem), recording);
        UNIT_ASSERT_VALUES_EQUAL(GetMetricSystem(static_cast<const TActorSystem&>(*actorSystem)), recording);
        UNIT_ASSERT_EQUAL(GetMetricSystem(), nullptr);
        auto harmonizer = MakeHarmonizer(Us2Ts(1'000'000));
        harmonizer->SetActorSystem(actorSystem);
        harmonizer->Harmonize(Us2Ts(1'000'000));
        using namespace NHarmonizerMetrics;
        UNIT_ASSERT_VALUES_EQUAL(recording->Entries.size(), 1);
        const auto& entry = recording->Entries.front();
        UNIT_ASSERT_VALUES_EQUAL(entry.Key.Name, TGlobal::Name);
        UNIT_ASSERT(entry.Key.Labels.empty());
        UNIT_ASSERT_EQUAL(entry.Meta.Frontend, &TGlobalFrontend::Descriptor());
        UNIT_ASSERT_VALUES_EQUAL(entry.Line->Samples.size(), 1);
        const auto& sample = entry.Line->Samples.front();
        using TStorage = NHarmonizerMetrics::TDecimalStorage;
        UNIT_ASSERT_VALUES_EQUAL(sample.UsedBytes,
            sizeof(TStorage::THeader<4>) + 8 + 4 * 9);
        const char* cursor = sample.Payload.data() + sizeof(TStorage::THeader<4>);
        const char* end = sample.Payload.data() + sample.UsedBytes;
        ui64 timestamp;
        ui8 tag;
        UNIT_ASSERT(TStorage::Unpack(&cursor, end, &timestamp, &tag));
        UNIT_ASSERT_VALUES_EQUAL(timestamp, 0); // 123 cycles rounds down to the 100 ms grid.
        UNIT_ASSERT_VALUES_EQUAL(tag, 0);
        std::array<ui64, 4> encoded;
        UNIT_ASSERT(TStorage::UnpackValue<0>(&cursor, end, 0, &encoded[0]));
        UNIT_ASSERT(TStorage::UnpackValue<1>(&cursor, end, 0, &encoded[1]));
        UNIT_ASSERT(TStorage::UnpackValue<2>(&cursor, end, 0, &encoded[2]));
        UNIT_ASSERT(TStorage::UnpackValue<3>(&cursor, end, 0, &encoded[3]));
        UNIT_ASSERT_EQUAL(cursor, end);
        const auto values = TGlobalFrontend::DecodeValue(encoded,
            std::make_index_sequence<TGlobalFrontend::FieldCount>{});
        THarmonizerStats stats;
        harmonizer->GetStats(stats);
        UNIT_ASSERT_DOUBLES_EQUAL(values.Get<TGlobal::TAvgAwakeningTimeUs>(), stats.AvgAwakeningTimeUs, 0.0051);
        UNIT_ASSERT_DOUBLES_EQUAL(values.Get<TGlobal::TAvgWakingUpTimeUs>(), stats.AvgWakingUpTimeUs, 0.0051);
        UNIT_ASSERT_DOUBLES_EQUAL(values.Get<TGlobal::TBudget>(), stats.Budget, 0.0051);
        UNIT_ASSERT_DOUBLES_EQUAL(values.Get<TGlobal::TSharedFreeCpu>(), stats.SharedFreeCpu, 0.0051);
        actorSystem->Stop();
        harmonizer->Harmonize(Us2Ts(2'000'000));
        UNIT_ASSERT_VALUES_EQUAL(entry.Line->Samples.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(entry.Line->CloseCount, 1);
    }
}
