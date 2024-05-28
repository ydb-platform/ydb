#include <benchmark/benchmark.h>

#include <ydb/core/tablet_flat/flat_row_celled.h>
#include <ydb/core/tablet_flat/flat_part_charge_range.h>
#include <ydb/core/tablet_flat/flat_part_charge_create.h>
#include <ydb/core/tablet_flat/test/libs/rows/cook.h>
#include <ydb/core/tablet_flat/test/libs/rows/tool.h>
#include <ydb/core/tablet_flat/test/libs/table/model/large.h>
#include <ydb/core/tablet_flat/test/libs/table/test_make.h>
#include <ydb/core/tablet_flat/test/libs/table/test_mixer.h>
#include "ydb/core/tablet_flat/flat_part_index_iter_bree_index.h"
#include "ydb/core/tablet_flat/flat_stat_table.h"
#include "ydb/core/tablet_flat/test/libs/table/wrap_iter.h"
#include "ydb/core/tx/datashard/datashard.h"
#include <ydb/core/tablet_flat/test/libs/table/test_writer.h>
#include <ydb/core/tablet_flat/test/libs/table/test_envs.h>
#include <ydb/core/tablet_flat/test/libs/table/wrap_part.h>
#include <ydb/core/tablet_flat/test/libs/table/test_steps.h>

#ifndef BENCHMARK_MAKE_LARGE_PART
#define BENCHMARK_MAKE_LARGE_PART 0
#endif

namespace NKikimr::NTable {

namespace {
    using namespace NTest;

    using TCheckIter = TChecker<TWrapIter, TSubset>;
    using TCheckReverseIter = TChecker<TWrapReverseIter, TSubset>;

    NPage::TConf PageConf(size_t groups, bool writeBTreeIndex) noexcept
    {
        NPage::TConf conf;

        conf.Groups.resize(groups);
        
        conf.WriteBTreeIndex = writeBTreeIndex;

        conf.SliceSize = conf.Group(0).PageSize * 4;

        return conf;
    }

    struct TPartFixture : public benchmark::Fixture {
        using TGroupId = NPage::TGroupId;

        void SetUp(::benchmark::State& state) 
        {
            const bool useBTree = state.range(0);
            const bool groups = state.range(1);
            const bool history = state.range(2);

            ui64 rows = history ? 300000 : 1000000;
            if (BENCHMARK_MAKE_LARGE_PART) {
                rows *= 10;
            }
            Mass = new NTest::TMass(new NTest::TModelStd(groups), rows);
            Subset = TMake(*Mass, PageConf(Mass->Model->Scheme->Families.size(), useBTree)).Mixed(0, 1, TMixerOne{ }, history ? 0.7 : 0);
            
            for (const auto& part : Subset->Flatten) { // single part
                state.counters["DataBytes"] = part->Stat.Bytes;
                state.counters["DataPages"] = IndexTools::CountMainPages(*part);
                state.counters["IndexBytes"] = part->IndexesRawSize;
                if (useBTree) {
                    state.counters["Levels{0}"] = part->IndexPages.BTreeGroups[0].LevelCount;
                }
            }

            if (history) {
                Checker = new TCheckIter(*Subset, {new TTestEnv()}, TRowVersion(0, 8));
                CheckerReverse = new TCheckReverseIter(*Subset, {new TTestEnv()}, TRowVersion(0, 8));
            } else {
                Checker = new TCheckIter(*Subset, {new TTestEnv()});
                CheckerReverse = new TCheckReverseIter(*Subset, {new TTestEnv()});
            }

            GroupId = TGroupId(groups, history);
            Part = Subset->Flatten[0].Part.Get();
        }

        TMersenne<ui64> Rnd;
        TAutoPtr<NTest::TMass> Mass;
        TAutoPtr<TSubset> Subset;
        TAutoPtr<TCheckIter> Checker;
        TAutoPtr<TCheckReverseIter> CheckerReverse;
        TTestEnv Env;
        TGroupId GroupId;
        TPart const* Part;
    };
}

BENCHMARK_DEFINE_F(TPartFixture, SeekRowId)(benchmark::State& state) {
    const bool useBTree = state.range(0);

    for (auto _ : state) {
        THolder<IPartGroupIndexIter> iter;

        if (useBTree) {
            iter = MakeHolder<TPartGroupBtreeIndexIter>(Part, &Env, GroupId);
        } else {
            iter = MakeHolder<TPartGroupFlatIndexIter>(Part, &Env, GroupId);
        }

        iter->Seek(RandomNumber<ui32>(Part->Stat.Rows));    
    }
}

BENCHMARK_DEFINE_F(TPartFixture, Next)(benchmark::State& state) {
    const bool useBTree = state.range(0);

    THolder<IPartGroupIndexIter> iter;

    if (useBTree) {
        iter = MakeHolder<TPartGroupBtreeIndexIter>(Part, &Env, GroupId);
    } else {
        iter = MakeHolder<TPartGroupFlatIndexIter>(Part, &Env, GroupId);
    }

    iter->Seek(RandomNumber<ui32>(Part->Stat.Rows));

    for (auto _ : state) {
        if (!iter->IsValid()) {
            iter->Seek(RandomNumber<ui32>(Part->Stat.Rows));
        }
        iter->Next();
    }
}

BENCHMARK_DEFINE_F(TPartFixture, Prev)(benchmark::State& state) {
    const bool useBTree = state.range(0);

    THolder<IPartGroupIndexIter> iter;

    if (useBTree) {
        iter = MakeHolder<TPartGroupBtreeIndexIter>(Part, &Env, GroupId);
    } else {
        iter = MakeHolder<TPartGroupFlatIndexIter>(Part, &Env, GroupId);
    }

    iter->Seek(RandomNumber<ui32>(Part->Stat.Rows));

    for (auto _ : state) {
        if (!iter->IsValid()) {
            iter->Seek(RandomNumber<ui32>(Part->Stat.Rows));
        }
        iter->Prev();
    }
}

BENCHMARK_DEFINE_F(TPartFixture, SeekKey)(benchmark::State& state) {
    const bool useBTree = state.range(0);
    const ESeek seek = ESeek(state.range(3));

    TRowTool rowTool(*Subset->Scheme);
    auto tags = TVector<TTag>();
    for (auto c : Subset->Scheme->Cols) {
        tags.push_back(c.Tag);
    }

    for (auto _ : state) {
        THolder<IPartGroupIndexIter> iter;

        if (useBTree) {
            iter = MakeHolder<TPartGroupBtreeIndexIter>(Part, &Env, GroupId);
        } else {
            iter = MakeHolder<TPartGroupFlatIndexIter>(Part, &Env, GroupId);
        }

        state.PauseTiming();
        auto& row = *Mass->Saved.Any(Rnd);
        auto key_ = rowTool.LookupKey(row);
        const TCelled key(key_, *Subset->Scheme->Keys, false);
        state.ResumeTiming();

        iter->Seek(seek, key, Subset->Scheme->Keys.Get());
    }
}

BENCHMARK_DEFINE_F(TPartFixture, DoReads)(benchmark::State& state) {
    const bool reverse = state.range(3);
    const ESeek seek = static_cast<ESeek>(state.range(4));
    const ui32 items = state.range(5);

    for (auto _ : state) {
        auto it = Mass->Saved.Any(Rnd);

        if (reverse) {
            CheckerReverse->Seek(*it, seek);
            for (ui32 i = 1; CheckerReverse->GetReady() == EReady::Data && i < items; i++) {
                CheckerReverse->Next();
            }
        } else {
            Checker->Seek(*it, seek);
            for (ui32 i = 1; Checker->GetReady() == EReady::Data && i < items; i++) {
                Checker->Next();
            }
        }
    }
}

BENCHMARK_DEFINE_F(TPartFixture, DoCharge)(benchmark::State& state) {
    const bool reverse = state.range(3);
    const ui32 items = state.range(4);

    auto tags = TVector<TTag>();
    for (auto c : Subset->Scheme->Cols) {
        tags.push_back(c.Tag);
    }
    TRun run(*Subset->Scheme->Keys);
    NTest::TRowTool tool(*Subset->Scheme);

    for (auto _ : state) {
        auto row1 = Rnd.Uniform(Mass->Saved.Size());
        auto row2 = Min(row1 + items, Mass->Saved.Size() - 1);
        auto key1 = tool.KeyCells(Mass->Saved[row1]);
        auto key2 = tool.KeyCells(Mass->Saved[row2]);
        if (reverse) {
            ChargeRangeReverse(&Env, key1, key2, run, *Subset->Scheme->Keys, tags, items, 0, true);
        } else {
            ChargeRange(&Env, key1, key2, run, *Subset->Scheme->Keys, tags, items, 0, true);
        }
    }
}

BENCHMARK_DEFINE_F(TPartFixture, BuildStats)(benchmark::State& state) {
    for (auto _ : state) {
        TStats stats;
        BuildStats(*Subset, stats, NDataShard::gDbStatsRowCountResolution, NDataShard::gDbStatsDataSizeResolution, &Env, [](){});
    }
}

BENCHMARK_REGISTER_F(TPartFixture, SeekRowId)
    ->ArgsProduct({
        /* b-tree */ {0, 1},
        /* groups: */ {0, 1},
        /* history: */ {0}})
    ->Unit(benchmark::kMicrosecond);

BENCHMARK_REGISTER_F(TPartFixture, Next)
    ->ArgsProduct({
        /* b-tree */ {0, 1},
        /* groups: */ {0, 1},
        /* history: */ {0}})
    ->Unit(benchmark::kMicrosecond);

BENCHMARK_REGISTER_F(TPartFixture, Prev)
    ->ArgsProduct({
        /* b-tree */ {0, 1},
        /* groups: */ {0, 1},
        /* history: */ {0}})
    ->Unit(benchmark::kMicrosecond);

BENCHMARK_REGISTER_F(TPartFixture, SeekKey)
    ->ArgsProduct({
        /* b-tree */ {0, 1},
        /* groups: */ {0, 1},
        /* history: */ {0},
        /* ESeek: */ {1}})
    ->Unit(benchmark::kMicrosecond);

BENCHMARK_REGISTER_F(TPartFixture, DoReads)
    ->ArgsProduct({
        /* b-tree */ {0, 1},
        /* groups: */ {1},
        /* history: */ {1},
        /* reverse: */ {0},
        /* ESeek: */ {1},
        /* items */ {1, 50, 1000}})
    ->Unit(benchmark::kMicrosecond);

BENCHMARK_REGISTER_F(TPartFixture, DoCharge)
    ->ArgsProduct({
        /* b-tree */ {0, 1},
        /* groups: */ {1},
        /* history: */ {1},
        /* reverse: */ {0},
        /* items */ {1, 50, 1000}})
    ->Unit(benchmark::kMicrosecond);

BENCHMARK_REGISTER_F(TPartFixture, BuildStats)
    ->ArgsProduct({
        /* b-tree */ {0, 1},
        /* groups: */ {0, 1},
        /* history: */ {0, 1}})
    ->Unit(benchmark::kMicrosecond);

}
