#include "actor_benchmark_helper.h"
#include "subsystems/metric_system.h"
#include "harmonizer/harmonizer.h"

#include <library/cpp/testing/unittest/registar.h>
#include <array>

using namespace NActors;
using namespace NActors::NTests;

namespace {
    // Deliberately independent of TInMemoryMetricsBackend and its writer state.
    class TRecordingLine final : public IMetricLine {
    public:
        bool Accept = true;
        ui32 CloseCount = 0;
        TVector<ui64> Values;

        bool IsValid() const noexcept override { return true; }
        void Close() noexcept override { ++CloseCount; }
        ui32 GetLineId() const noexcept override { return CloseCount ? 0 : 42; }
        NHPTimer::STime CurrentTimestampTs() const noexcept override { return 123; }
        bool AccessChunkMemory(void* opaque, TAccessChunkMemoryFn access) noexcept override {
            if (!Accept || CloseCount) {
                return false;
            }
            // A fresh small chunk for each sample is sufficient for this mock.
            alignas(ui64) std::array<char, 64> payload{};
            TWritableChunkMemory memory{.Payload = payload};
            return access(opaque, memory);
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
        UNIT_ASSERT_VALUES_EQUAL(recording->Entries.size(), 4);
        for (const auto& entry : recording->Entries) {
            UNIT_ASSERT(entry.Key.Name.StartsWith("harmonizer."));
            UNIT_ASSERT_VALUES_EQUAL(entry.Line->Values.size(), 1);
        }
        actorSystem->Stop();
        harmonizer->Harmonize(Us2Ts(2'000'000));
        for (const auto& entry : recording->Entries) {
            UNIT_ASSERT_VALUES_EQUAL(entry.Line->Values.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(entry.Line->CloseCount, 1);
        }
    }
}
