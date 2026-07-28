#include <benchmark/benchmark.h>

#include <ydb/core/blobstorage/vdisk/hulldb/base/hullds_ut.h>
#include <ydb/core/blobstorage/vdisk/hulldb/compstrat/hulldb_compstrat_ratio.h>
#include <ydb/core/blobstorage/vdisk/hulldb/hull_ds_all.h>

#include <library/cpp/time_provider/time_provider.h>

#include <optional>
#include <utility>

namespace NKikimr {
namespace {

    using TLogoSst = TLevelSegment<TKeyLogoBlob, TMemRecLogoBlob>;
    using TLogoSstPtr = TIntrusivePtr<TLogoSst>;
    using TStorageRatioStrategy = NHullComp::TStrategyStorageRatio<TKeyLogoBlob, TMemRecLogoBlob>;

    enum class EDueLayout : ui32 {
        Prefix,
        Uniform,
        Edges,
        Random,
        Count,
    };

    class TMutableTimeProvider : public ITimeProvider {
    public:
        explicit TMutableTimeProvider(TInstant now)
            : Current(now)
        {}

        TInstant Now() override {
            return Current;
        }

        void Advance(TDuration duration) {
            Current += duration;
        }

    private:
        TInstant Current;
    };

    class TBenchmarkData {
    public:
        TTestContexts Context{
            135249920,
            2ul << 20ul,
            true,
            TDuration::Minutes(5),
            TDuration::Hours(1),
        };
        std::shared_ptr<TRopeArena> Arena =
            std::make_shared<TRopeArena>(&TRopeArenaBackend::Allocate);
        TIntrusivePtr<THullDs> Ds = MakeIntrusive<THullDs>(Context.GetHullCtx());
        TVector<TLogoSstPtr> Ssts;
        ui64 TotalRecords = 0;
        ui64 DueRecords = 0;

        TBenchmarkData() {
            const TLevelIndexSettings& settings = Context.GetLevelIndexSettings();
            Ds->LogoBlobs = MakeIntrusive<TLogoBlobsDs>(settings, Arena);
            Ds->Blocks = MakeIntrusive<TBlocksDs>(settings, Arena);
            Ds->Barriers = MakeIntrusive<TBarriersDs>(settings, Arena);
        }

        void Prepare(
                ui64 requestedRecords,
                ui32 numSsts,
                ui32 overlap,
                ui64 freshRecords,
                ui32 keyStride,
                ui32 duePercent,
                bool allLevel0,
                EDueLayout dueLayout)
        {
            Y_ABORT_UNLESS(numSsts);
            Y_ABORT_UNLESS(overlap && overlap <= numSsts);
            Y_ABORT_UNLESS(keyStride);
            Y_ABORT_UNLESS(duePercent <= 100);
            Y_ABORT_UNLESS(dueLayout < EDueLayout::Count);

            const ui64 recordsPerSst = Max<ui64>(1, requestedRecords / numSsts);
            for (ui32 sstIndex = 0; sstIndex < numSsts; ++sstIndex) {
                const ui32 overlapGroup = sstIndex / overlap;
                const ui32 overlapIndex = sstIndex % overlap;
                const ui32 level = allLevel0 ? 0 : overlapIndex + 1;
                const ui64 firstStep =
                    1 + static_cast<ui64>(overlapGroup) * recordsPerSst * keyStride;
                AddSst(level, firstStep, recordsPerSst, keyStride, sstIndex);
            }

            Ds->LogoBlobs->LoadCompleted();
            Ds->Blocks->LoadCompleted();
            Ds->Barriers->LoadCompleted();

            for (ui64 index = 0; index < freshRecords; ++index) {
                const ui64 step = 1 + index * keyStride;
                Ds->LogoBlobs->PutToFresh(
                    1 + index,
                    MakeKey(step),
                    TMemRecLogoBlob());
            }

            const size_t dueSsts = (Ssts.size() * duePercent + 99) / 100;
            const TVector<bool> due = SelectDueSsts(Ssts.size(), dueSsts, dueLayout);
            for (size_t index = 0; index < Ssts.size(); ++index) {
                const TInstant calculationTime = due[index]
                    ? TInstant::Zero()
                    : TInstant::Seconds(10'000'000'000ULL);
                NHullComp::TSstRatioPtr ratio =
                    MakeIntrusive<NHullComp::TSstRatio>(calculationTime);
                ratio->IndexItemsTotal = Ssts[index]->Elements();
                ratio->IndexItemsKeep = Ssts[index]->Elements();
                Ssts[index]->StorageRatio.Set(ratio, calculationTime);

                if (due[index]) {
                    DueRecords += Ssts[index]->Elements();
                }
            }
        }

    private:
        static TVector<bool> SelectDueSsts(
                size_t numSsts,
                size_t dueSsts,
                EDueLayout layout)
        {
            Y_ABORT_UNLESS(dueSsts <= numSsts);
            TVector<bool> due(numSsts, false);

            switch (layout) {
                case EDueLayout::Prefix:
                    for (size_t i = 0; i < dueSsts; ++i) {
                        due[i] = true;
                    }
                    break;

                case EDueLayout::Uniform:
                    for (size_t i = 0; i < dueSsts; ++i) {
                        due[i * numSsts / dueSsts] = true;
                    }
                    break;

                case EDueLayout::Edges:
                    for (size_t i = 0; i < dueSsts; ++i) {
                        const size_t index = i % 2
                            ? numSsts - 1 - i / 2
                            : i / 2;
                        due[index] = true;
                    }
                    break;

                case EDueLayout::Random: {
                    TVector<size_t> indices;
                    indices.reserve(numSsts);
                    for (size_t i = 0; i < numSsts; ++i) {
                        indices.push_back(i);
                    }

                    ui64 state = 0x6a09e667f3bcc909ULL;
                    for (size_t size = indices.size(); size > 1; --size) {
                        state = state * 6364136223846793005ULL + 1442695040888963407ULL;
                        std::swap(indices[size - 1], indices[state % size]);
                    }
                    for (size_t i = 0; i < dueSsts; ++i) {
                        due[indices[i]] = true;
                    }
                    break;
                }

                case EDueLayout::Count:
                    Y_ABORT("Unexpected due SST layout");
            }

            return due;
        }

        static TKeyLogoBlob MakeKey(ui64 step) {
            Y_ABORT_UNLESS(step <= Max<ui32>());
            return TKeyLogoBlob(TLogoBlobID(
                1,
                1,
                static_cast<ui32>(step),
                0,
                32,
                0));
        }

        void AddSst(
                ui32 level,
                ui64 firstStep,
                ui64 numRecords,
                ui32 keyStride,
                ui32 sstIndex)
        {
            TTrackableVector<TLogoSst::TRec> index(
                TMemoryConsumer(Context.GetVCtx()->SstIndex));
            index.reserve(numRecords);

            for (ui64 record = 0; record < numRecords; ++record) {
                Y_ABORT_UNLESS(record <= Max<ui32>() / 32);
                TMemRecLogoBlob memRec;
                memRec.SetDiskBlob(TDiskPart(
                    100 + sstIndex,
                    static_cast<ui32>(record * 32),
                    32));
                index.emplace_back(
                    MakeKey(firstStep + record * keyStride),
                    memRec);
            }

            TLogoSstPtr sst = MakeIntrusive<TLogoSst>(Context.GetVCtx());
            sst->LoadLinearIndex(index);
            sst->Info.Items = numRecords;
            sst->Info.FirstLsn = Ssts.size() + 1;
            sst->Info.LastLsn = Ssts.size() + 1;
            sst->Info.CTime = TInstant::Zero();
            sst->AssignedSstId = Ssts.size() + 1;

            if (level == 0) {
                Ds->LogoBlobs->CurSlice->Level0.Put(sst);
            } else {
                while (Ds->LogoBlobs->CurSlice->SortedLevels.size() < level) {
                    Ds->LogoBlobs->CurSlice->SortedLevels.emplace_back(TKeyLogoBlob());
                }
                Ds->LogoBlobs->CurSlice->SortedLevels[level - 1].Put(sst);
            }

            TotalRecords += numRecords;
            Ssts.push_back(std::move(sst));
        }
    };

    class TStorageRatioFixture : public benchmark::Fixture {
    public:
        void SetUp(benchmark::State& state) override {
            OriginalTimeProvider = TAppData::TimeProvider;
            TimeProvider = MakeIntrusive<TMutableTimeProvider>(TInstant::Seconds(1000));
            TAppData::TimeProvider = TimeProvider;

            Data = std::make_unique<TBenchmarkData>();
            Data->Prepare(
                state.range(0),
                state.range(1),
                state.range(2),
                state.range(3),
                state.range(4),
                state.range(5),
                state.range(6),
                static_cast<EDueLayout>(state.range(7)));
            Snapshot.emplace(Data->Ds->GetIndexSnapshot());

            state.counters["SSTs"] = Data->Ssts.size();
            state.counters["Records"] = Data->TotalRecords;
            state.counters["DueRecords"] = Data->DueRecords;
            state.counters["FreshRecords"] = state.range(3);
            state.counters["Overlap"] = state.range(2);
            state.counters["DuePercent"] = state.range(5);
            state.counters["DueLayout"] = state.range(7);
        }

        void TearDown(benchmark::State&) override {
            Snapshot.reset();
            Data.reset();
            TAppData::TimeProvider = std::move(OriginalTimeProvider);
            TimeProvider.Reset();
        }

        void AdvanceCalculationTime() {
            TimeProvider->Advance(TDuration::Minutes(5));
        }

        void SetOptimizationEnabled(bool enabled) {
            Snapshot->HullCtx->VCfg->FeatureFlags.SetEnableHullCompStorageRatioOptimization(
                enabled);
        }

        void Calculate() {
            auto barriers = Snapshot->BarriersSnap.CreateEssence(Snapshot->HullCtx);
            TStorageRatioStrategy(
                Snapshot->HullCtx,
                Snapshot->LogoBlobsSnap,
                std::move(barriers),
                true).Work();
        }

        void SetItemsProcessed(benchmark::State& state) const {
            state.SetItemsProcessed(state.iterations() * Data->DueRecords);
        }

    private:
        TIntrusivePtr<ITimeProvider> OriginalTimeProvider;
        TIntrusivePtr<TMutableTimeProvider> TimeProvider;
        std::unique_ptr<TBenchmarkData> Data;
        std::optional<THullDsSnap> Snapshot;
    };

    BENCHMARK_DEFINE_F(TStorageRatioFixture, PerSst)(benchmark::State& state) {
        for (auto _ : state) {
            Y_UNUSED(_);
            state.PauseTiming();
            SetOptimizationEnabled(false);
            AdvanceCalculationTime();
            state.ResumeTiming();

            Calculate();
            benchmark::ClobberMemory();
        }
        SetItemsProcessed(state);
    }

    BENCHMARK_DEFINE_F(TStorageRatioFixture, Batch)(benchmark::State& state) {
        for (auto _ : state) {
            Y_UNUSED(_);
            state.PauseTiming();
            SetOptimizationEnabled(true);
            AdvanceCalculationTime();
            state.ResumeTiming();

            Calculate();
            benchmark::ClobberMemory();
        }
        SetItemsProcessed(state);
    }

    void AddStorageRatioScenarios(benchmark::Benchmark* benchmark)
    {
        benchmark
            ->ArgNames({
                "Records",
                "SSTs",
                "Overlap",
                "Fresh",
                "Stride",
                "DuePercent",
                "AllL0",
                "DueLayout",
            })
            // records, SSTs, overlap, Fresh, stride, due %, all L0, due layout
            // due layout: 0 prefix, 1 uniform, 2 edges, 3 random
            ->Args({100'000, 8, 1, 0, 1, 100, 0, 0})
            ->Args({100'000, 8, 2, 0, 1, 100, 0, 0})
            ->Args({100'000, 8, 4, 0, 1, 100, 0, 0})
            ->Args({100'000, 8, 8, 0, 1, 100, 0, 0})
            ->Args({100'000, 8, 8, 0, 1, 100, 1, 0})
            ->Args({100'000, 8, 1, 100'000, 1, 100, 0, 0})
            ->Args({100'000, 8, 4, 0, 16, 100, 0, 0})
            ->Args({100'000, 100, 4, 0, 1, 0, 0, 0})
            ->Args({100'000, 100, 4, 0, 1, 1, 0, 0})
            ->Args({100'000, 100, 4, 0, 1, 10, 0, 0})
            ->Args({100'000, 100, 4, 0, 1, 100, 0, 0})
            ->Args({1'000'000, 100, 4, 0, 1, 0, 0, 0})
            ->Args({1'000'000, 100, 4, 0, 1, 1, 0, 0})
            ->Args({1'000'000, 100, 4, 0, 1, 10, 0, 0})
            ->Args({1'000'000, 100, 4, 0, 1, 100, 0, 0})
            ->Args({100'000, 100, 4, 0, 1, 10, 0, 1})
            ->Args({100'000, 100, 4, 0, 1, 2, 0, 2})
            ->Args({100'000, 100, 4, 0, 1, 10, 0, 2})
            ->Args({100'000, 100, 4, 0, 1, 10, 0, 3})
            ->Args({1'000'000, 100, 4, 0, 1, 10, 0, 1})
            ->Unit(benchmark::kMillisecond);
    }

    BENCHMARK_REGISTER_F(TStorageRatioFixture, PerSst)
        ->Apply(AddStorageRatioScenarios);
    BENCHMARK_REGISTER_F(TStorageRatioFixture, Batch)
        ->Apply(AddStorageRatioScenarios);

} // namespace
} // namespace NKikimr
