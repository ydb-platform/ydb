#include <benchmark/benchmark.h>
#include <random>

#include <ydb/core/tablet_flat/flat_row_celled.h>
#include <ydb/core/tablet_flat/flat_part_charge.h>
#include <ydb/core/tablet_flat/test/libs/rows/cook.h>
#include <ydb/core/tablet_flat/test/libs/rows/tool.h>
#include <ydb/core/tablet_flat/test/libs/table/model/large.h>
#include <ydb/core/tablet_flat/test/libs/table/test_writer.h>
#include <ydb/core/tablet_flat/test/libs/table/test_envs.h>
#include <ydb/core/tablet_flat/test/libs/table/wrap_part.h>
#include <ydb/core/tablet_flat/test/libs/table/test_steps.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr {
namespace NTable {

namespace {
    const NTest::TMass Mass(new NTest::TModelStd(true), 2*1000);

    struct TTouchEnv : public NTest::TTestEnv {
        TTouchEnv(bool fail) : Fail(fail) { }

        const TSharedData* TryGetPage(const TPart *part, TPageId id, TGroupId groupId) override
        {
            TouchedCount++;
            return Fail ? nullptr : NTest::TTestEnv::TryGetPage(part, id, groupId);
        }

        const bool Fail = false;
        ui64 TouchedCount = 0;
    };

    struct TCooker {
        TCooker(const TRowScheme &scheme)
            : Tool(scheme)
            , Writer(new TPartScheme(scheme.Cols), { }, NPage::TGroupId(0))
        {

        }

        TCooker& Add(const NTest::TRow &row, ui64 offset, ui32 page)
        {
            const TCelled key(Tool.LookupKey(row), *Tool.Scheme.Keys, false);

            return Writer.Add(key, offset, page), *this;
        }

        TSharedData Flush()
        {
            return Writer.Flush();
        }

    private:
        const NTest::TRowTool Tool;
        NPage::TIndexWriter Writer;
    };

    struct TModel : public benchmark::Fixture {
        using TGroupId = NPage::TGroupId;

        TModel()
            : Tool(*Mass.Model->Scheme)
        {
            auto pages = Eggs.At(0)->Index->End() - Eggs.At(0)->Index->Begin();

            Y_VERIFY(pages > 120);
        }

        static NTest::TPartEggs MakeEggs() noexcept
        {
            NPage::TConf conf{ true, 8192 };

            auto groups = Mass.Model->Scheme->Families.size();
            for (size_t group : xrange(groups)) {
                conf.Group(group).PageRows = 10;
            }
            conf.Group(1).PageRows = 5;
            conf.Group(2).PageRows = 2;

            NTest::TPartCook cook(Mass.Model->Scheme, conf);

            for (auto seq: xrange(Mass.Saved.Size())) {
                // fill with random keys
                if (seq % 3 != 0) cook.Add(Mass.Saved[seq]);
            }

            return cook.Finish();
        }

        void SetUp(const ::benchmark::State& state) 
        {
            bool fail = state.range(1);
            ui32 groups = state.range(2);

            Env = MakeHolder<TTouchEnv>(fail);

            const auto &keyDefaults = *Tool.Scheme.Keys;
            
            Run = MakeHolder<TRun>(keyDefaults);

            auto part = Eggs.Lone();
            for (auto& slice : *part->Slices) {
                Run->Insert(part, slice);
            }

            Tags = TVector<TTag>();
            for (auto c : Mass.Model->Scheme->Cols) {
                if (c.Group <= groups) {
                    Tags.push_back(c.Tag);
                }
            }
        }

        void TearDown(const ::benchmark::State& state) {
            (void)state;
            Run.Reset();
            Env.Reset();
        }

        const NTest::TRowTool Tool;
        const NTest::TPartEggs Eggs = MakeEggs();
        THolder<TTouchEnv> Env;
        THolder<TRun> Run;
        TVector<TTag> Tags;
    };
}

BENCHMARK_DEFINE_F(TModel, PrechargeByKeys)(benchmark::State& state) {
    ui64 items = state.range(0);

    const auto &keyDefaults = *Tool.Scheme.Keys;

    ui64 it = 0;
    for (auto _ : state) {
        ui32 lower = ++it % 50;
        ui32 upper = lower + items;

        const auto from = Tool.KeyCells(Mass.Saved[lower]);
        const auto to = Tool.KeyCells(Mass.Saved[upper]);

        TCharge::Range(Env.Get(), from, to, *Run.Get(), keyDefaults, Tags, items, Max<ui64>());
    }

    state.counters["Touched"] = benchmark::Counter(Env->TouchedCount, benchmark::Counter::kAvgIterations);
}

BENCHMARK_DEFINE_F(TModel, PrechargeByRows)(benchmark::State& state) {
    ui64 items = state.range(0);

    const auto &keyDefaults = *Tool.Scheme.Keys;

    ui64 it = 0;
    for (auto _ : state) {
        ui32 lower = ++it % 50;
        ui32 upper = lower + items;

        TCharge(Env.Get(), *(Run.Get())->begin()->Part, Tags, false).Do(lower, upper, keyDefaults, items, Max<ui64>());
    }

    state.counters["Touched"] = Env->TouchedCount / it;
}

BENCHMARK_REGISTER_F(TModel, PrechargeByKeys)
    ->ArgsProduct({/*items:*/ {0, 100, 1000}, /*fail:*/{0, 1}, /*groups:*/ {0, 1, 2}})
    ->Unit(benchmark::kMicrosecond);

BENCHMARK_REGISTER_F(TModel, PrechargeByRows)
    ->ArgsProduct({/*items:*/ {0, 100, 1000}, /*fail:*/{0, 1}, /*groups:*/ {0, 1, 2}})
    ->Unit(benchmark::kMicrosecond);

}
}
