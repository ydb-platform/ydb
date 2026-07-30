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

    enum class EDataLayout : ui32 {
        UniformOverlap,
        ProductionLike,
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
                bool allLevel0,
                EDataLayout dataLayout)
        {
            Y_ABORT_UNLESS(numSsts);
            Y_ABORT_UNLESS(keyStride);
            Y_ABORT_UNLESS(dataLayout < EDataLayout::Count);

            switch (dataLayout) {
                case EDataLayout::UniformOverlap:
                    PrepareUniformOverlap(
                        requestedRecords,
                        numSsts,
                        overlap,
                        keyStride,
                        allLevel0);
                    break;

                case EDataLayout::ProductionLike:
                    PrepareProductionLike(requestedRecords, numSsts);
                    break;

                case EDataLayout::Count:
                    Y_ABORT("Unexpected data layout");
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
        }

        void MakeAllSstsDue(TInstant now) {
            const TInstant calculationTime =
                now - Context.GetHullCtx()->HullCompStorageRatioCalcPeriod;
            for (const TLogoSstPtr& sst : Ssts) {
                sst->StorageRatio.SetCalculationTime(calculationTime);
            }
        }

    private:
        static constexpr ui32 ProductionLikeLevel0Ssts = 13;
        static constexpr ui32 ProductionLikeMiddleLevels = 12;
        static constexpr ui32 ProductionLikeLevel17Ssts = 3;
        static constexpr ui32 ProductionLikeWideSsts =
            ProductionLikeLevel0Ssts +
            ProductionLikeMiddleLevels +
            ProductionLikeLevel17Ssts;

        void PrepareUniformOverlap(
                ui64 requestedRecords,
                ui32 numSsts,
                ui32 overlap,
                ui32 keyStride,
                bool allLevel0)
        {
            Y_ABORT_UNLESS(overlap && overlap <= numSsts);

            const ui64 recordsPerSst = Max<ui64>(1, requestedRecords / numSsts);
            for (ui32 sstIndex = 0; sstIndex < numSsts; ++sstIndex) {
                const ui32 overlapGroup = sstIndex / overlap;
                const ui32 overlapIndex = sstIndex % overlap;
                const ui32 level = allLevel0 ? 0 : overlapIndex + 1;
                const ui64 firstStep =
                    1 + static_cast<ui64>(overlapGroup) * recordsPerSst * keyStride;
                AddSst(level, firstStep, recordsPerSst, keyStride, sstIndex);
            }
        }

        static ui64 GetPartSize(ui64 total, ui32 index, ui32 parts) {
            return total / parts + (index < total % parts);
        }

        void AddSparseSst(
                ui32 level,
                ui64 firstStep,
                ui64 lastStep,
                ui64 numRecords)
        {
            Y_ABORT_UNLESS(firstStep <= lastStep);
            Y_ABORT_UNLESS(numRecords);

            const ui64 keyStride = numRecords > 1
                ? Max<ui64>(1, (lastStep - firstStep) / (numRecords - 1))
                : 1;
            Y_ABORT_UNLESS(keyStride <= Max<ui32>());
            Y_ABORT_UNLESS(Ssts.size() <= Max<ui32>());
            AddSst(
                level,
                firstStep,
                numRecords,
                static_cast<ui32>(keyStride),
                static_cast<ui32>(Ssts.size()));
        }

        void PrepareProductionLike(ui64 requestedRecords, ui32 numSsts) {
            // Mirrors w.html: 13 L0 SSTs, one SST on levels 1..12,
            // three SSTs on level 17 and the remaining non-overlapping
            // leaf SSTs on level 18.
            Y_ABORT_UNLESS(numSsts > ProductionLikeWideSsts);
            Y_ABORT_UNLESS(requestedRecords >= numSsts);

            const ui32 leafSsts = numSsts - ProductionLikeWideSsts;

            // Record shares from w.html, rounded to tenths of a percent:
            // L0 0.4%, levels 1..12 0.8%, L17 1.2%, L18 97.6%.
            const ui64 level0Records = requestedRecords * 4 / 1000;
            const ui64 middleLevelRecords = requestedRecords * 8 / 1000;
            const ui64 level17Records = requestedRecords * 12 / 1000;
            const ui64 leafRecords =
                requestedRecords -
                level0Records -
                middleLevelRecords -
                level17Records;
            Y_ABORT_UNLESS(
                level0Records >= ProductionLikeLevel0Ssts &&
                middleLevelRecords >= ProductionLikeMiddleLevels &&
                level17Records >= ProductionLikeLevel17Ssts &&
                leafRecords >= leafSsts);

            const ui64 firstKey = 1;
            const ui64 lastKey = leafRecords;

            for (ui32 i = 0; i < ProductionLikeLevel0Ssts; ++i) {
                AddSparseSst(
                    0,
                    firstKey,
                    lastKey,
                    GetPartSize(
                        level0Records,
                        i,
                        ProductionLikeLevel0Ssts));
            }

            for (ui32 i = 0; i < ProductionLikeMiddleLevels; ++i) {
                AddSparseSst(
                    i + 1,
                    firstKey,
                    lastKey,
                    GetPartSize(
                        middleLevelRecords,
                        i,
                        ProductionLikeMiddleLevels));
            }

            ui64 level17FirstKey = firstKey;
            for (ui32 i = 0; i < ProductionLikeLevel17Ssts; ++i) {
                const ui64 level17LastKey =
                    (i + 1) * leafRecords / ProductionLikeLevel17Ssts;
                AddSparseSst(
                    17,
                    level17FirstKey,
                    level17LastKey,
                    GetPartSize(
                        level17Records,
                        i,
                        ProductionLikeLevel17Ssts));
                level17FirstKey = level17LastKey + 1;
            }

            ui64 leafFirstKey = firstKey;
            for (ui32 i = 0; i < leafSsts; ++i) {
                const ui64 records = GetPartSize(leafRecords, i, leafSsts);
                Y_ABORT_UNLESS(Ssts.size() <= Max<ui32>());
                AddSst(
                    18,
                    leafFirstKey,
                    records,
                    1,
                    static_cast<ui32>(Ssts.size()));
                leafFirstKey += records;
            }
            Y_ABORT_UNLESS(leafFirstKey == lastKey + 1);
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
                static_cast<EDataLayout>(state.range(6)));
            Snapshot.emplace(Data->Ds->GetIndexSnapshot());

            state.counters["SSTs"] = Data->Ssts.size();
            state.counters["Records"] = Data->TotalRecords;
            state.counters["FreshRecords"] = state.range(3);
            state.counters["Overlap"] = state.range(2);
            state.counters["KeyStride"] = state.range(4);
            state.counters["AllL0"] = state.range(5);
            state.counters["DataLayout"] = state.range(6);
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

        void MakeAllSstsDue() {
            Data->MakeAllSstsDue(TimeProvider->Now());
        }

        void ScheduleFullBatchNow() {
            Snapshot->HullCtx->StorageRatioFullBatchNextCalculationTime =
                TimeProvider->Now();
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

        void SetItemsProcessed(benchmark::State& state) const
        {
            state.counters["CalculatedRecords"] = Data->TotalRecords;
            state.counters["CalculatedSSTs"] = Data->Ssts.size();
            state.SetItemsProcessed(state.iterations() * Data->TotalRecords);
        }

    private:
        TIntrusivePtr<ITimeProvider> OriginalTimeProvider;
        TIntrusivePtr<TMutableTimeProvider> TimeProvider;
        std::unique_ptr<TBenchmarkData> Data;
        std::optional<THullDsSnap> Snapshot;
    };

    BENCHMARK_DEFINE_F(TStorageRatioFixture, Legacy)(benchmark::State& state) {
        for (auto _ : state) {
            Y_UNUSED(_);
            state.PauseTiming();
            SetOptimizationEnabled(false);
            AdvanceCalculationTime();
            MakeAllSstsDue();
            state.ResumeTiming();

            // Represents the total legacy work over one recalculation period:
            // every SST is calculated once, without measuring the idle time
            // between individually scheduled SSTs.
            Calculate();
            benchmark::ClobberMemory();
        }
        SetItemsProcessed(state);
    }

    BENCHMARK_DEFINE_F(TStorageRatioFixture, FullBatch)(benchmark::State& state) {
        for (auto _ : state) {
            Y_UNUSED(_);
            state.PauseTiming();
            SetOptimizationEnabled(true);
            AdvanceCalculationTime();
            ScheduleFullBatchNow();
            state.ResumeTiming();

            // Measures one scheduled production FullBatch over the complete
            // snapshot.
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
                "AllL0",
                "DataLayout",
            })
            // records, SSTs, overlap, Fresh, stride, all L0, data layout
            // data layout: 0 uniform overlap, 1 production-like
            // Controlled overlap.
            ->Args({100'000, 100, 1, 0, 1, 0, 0})
            ->Args({100'000, 100, 4, 0, 1, 0, 0})
            ->Args({100'000, 100, 8, 0, 1, 0, 0})
            ->Args({1'000'000, 100, 1, 0, 1, 0, 0})
            ->Args({1'000'000, 100, 4, 0, 1, 0, 0})
            ->Args({1'000'000, 100, 8, 0, 1, 0, 0})
            // L0, Fresh and sparse-key variants.
            ->Args({100'000, 100, 8, 0, 1, 1, 0})
            ->Args({100'000, 100, 4, 100'000, 1, 0, 0})
            ->Args({100'000, 100, 4, 0, 16, 0, 0})
            // Topology and record distribution derived from w.html. The
            // variants with Fresh use its observed ~7% record count.
            ->Args({100'000, 352, 1, 0, 1, 0, 1})
            ->Args({100'000, 352, 1, 7'000, 1, 0, 1})
            ->Args({1'000'000, 352, 1, 0, 1, 0, 1})
            ->Args({1'000'000, 352, 1, 70'000, 1, 0, 1})
            ->Args({5'439'294, 352, 1, 0, 1, 0, 1})
            ->Args({5'439'294, 352, 1, 367'052, 1, 0, 1})
            ->Unit(benchmark::kMillisecond);
    }

    BENCHMARK_REGISTER_F(TStorageRatioFixture, Legacy)
        ->Apply(AddStorageRatioScenarios);
    BENCHMARK_REGISTER_F(TStorageRatioFixture, FullBatch)
        ->Apply(AddStorageRatioScenarios);

} // namespace
} // namespace NKikimr
