#include <benchmark/benchmark.h>

#include <ydb/core/tablet_flat/flat_row_celled.h>
#include <ydb/core/tablet_flat/flat_part_charge_range.h>
#include <ydb/core/tablet_flat/flat_part_charge_create.h>
#include <ydb/core/tablet_flat/test/libs/rows/cook.h>
#include <ydb/core/tablet_flat/test/libs/rows/tool.h>
#include <ydb/core/tablet_flat/test/libs/table/model/large.h>
#include <ydb/core/tablet_flat/test/libs/table/test_make.h>
#include <ydb/core/tablet_flat/test/libs/table/test_mixer.h>
#include "ydb/core/tablet_flat/flat_part_btree_index_iter.h"
#include "ydb/core/tablet_flat/test/libs/table/wrap_iter.h"
#include <ydb/core/tablet_flat/test/libs/table/test_writer.h>
#include <ydb/core/tablet_flat/test/libs/table/test_envs.h>
#include <ydb/core/tablet_flat/test/libs/table/wrap_part.h>
#include <ydb/core/tablet_flat/test/libs/table/test_steps.h>

namespace NKikimr {
namespace NTable {

namespace {
    using namespace NTest;

    using TCheckIt = TChecker<TWrapIter, TSubset>;
    using TCheckReverseIt = TChecker<TWrapReverseIter, TSubset>;

    NPage::TConf PageConf(size_t groups, bool writeBTreeIndex) noexcept
    {
        NPage::TConf conf{ true, 1024 };

        conf.Groups.resize(groups);
        for (size_t group : xrange(groups)) {
            conf.Group(group).PageSize = 1024;
            conf.Group(group).BTreeIndexNodeTargetSize = 1024;
        }

        conf.WriteBTreeIndex = writeBTreeIndex;

        conf.SliceSize = conf.Group(0).PageSize * 4;

        return conf;
    }

    struct TPartIndexSeekFixture : public benchmark::Fixture {
        using TGroupId = NPage::TGroupId;

        void SetUp(const ::benchmark::State& state) 
        {
            const bool groups = state.range(1);

            TLayoutCook lay;

            lay
                .Col(0, 0,  NScheme::NTypeIds::Uint32)
                .Col(0, 1,  NScheme::NTypeIds::Uint32)
                .Col(0, 2,  NScheme::NTypeIds::Uint32)
                .Col(0, 3,  NScheme::NTypeIds::Uint32)
                .Col(groups ? 1 : 0, 4,  NScheme::NTypeIds::Uint32)
                .Key({0, 1, 2});

            TPartCook cook(lay, PageConf(groups ? 2 : 1, true));
            
            for (ui32 i = 0; (groups ? cook.GetDataBytes(0) + cook.GetDataBytes(1) : cook.GetDataBytes(0)) < 100ull*1024*1024; i++) {
                cook.Add(*TSchemedCookRow(*lay).Col(i / 10000, i / 100 % 100, i % 100, i, i));
            }

            Eggs = cook.Finish();

            const auto part = Eggs.Lone();

            Cerr << "DataBytes = " << part->Stat.Bytes << " DataPages = " << IndexTools::CountMainPages(*part) << Endl;
            Cerr << "FlatIndexBytes = " << part->GetPageSize(part->IndexPages.Groups[groups ? 1 : 0], {}) << " BTreeIndexBytes = " << part->IndexPages.BTreeGroups[groups ? 1 : 0].IndexSize << Endl;
            Cerr << "Levels = " << part->IndexPages.BTreeGroups[groups ? 1 : 0].LevelCount << Endl;

            // 100 MB
            UNIT_ASSERT_GE(part->Stat.Bytes, 100ull*1024*1024);
            UNIT_ASSERT_LE(part->Stat.Bytes, 100ull*1024*1024 + 10ull*1024*1024);

            GroupId = TGroupId(groups ? 1 : 0);
        }

        TPartEggs Eggs;
        TTestEnv Env;
        TGroupId GroupId;
    };

    struct TPartIndexIteratorFixture : public benchmark::Fixture {
        using TGroupId = NPage::TGroupId;

        void SetUp(const ::benchmark::State& state) 
        {
            const bool useBTree = state.range(0);
            const bool groups = state.range(1);
            const bool history = state.range(2);

            Mass = new NTest::TMass(new NTest::TModelStd(groups), history ? 1000000 : 300000);
            Subset = TMake(*Mass, PageConf(Mass->Model->Scheme->Families.size(), useBTree)).Mixed(0, 1, TMixerOne{ }, history ? 0.7 : 0);
            
            if (history) {
                Checker = new TCheckIt(*Subset, {new TTestEnv()}, TRowVersion(0, 8));
                CheckerReverse = new TCheckReverseIt(*Subset, {new TTestEnv()}, TRowVersion(0, 8));
            } else {
                Checker = new TCheckIt(*Subset, {new TTestEnv()});
                CheckerReverse = new TCheckReverseIt(*Subset, {new TTestEnv()});
            }
        }

        TMersenne<ui64> Rnd;
        TAutoPtr<NTest::TMass> Mass;
        TAutoPtr<TSubset> Subset;
        TAutoPtr<TCheckIt> Checker;
        TAutoPtr<TCheckReverseIt> CheckerReverse;
    };
}

BENCHMARK_DEFINE_F(TPartIndexSeekFixture, SeekRowId)(benchmark::State& state) {
    const bool useBTree = state.range(0);

    for (auto _ : state) {
        THolder<IIndexIter> iter;

        if (useBTree) {
            iter = MakeHolder<TPartBtreeIndexIt>(Eggs.Lone().Get(), &Env, GroupId);
        } else {
            iter = MakeHolder<TPartIndexIt>(Eggs.Lone().Get(), &Env, GroupId);
        }

        iter->Seek(RandomNumber<ui32>(Eggs.Lone()->Stat.Rows));    
    }
}

BENCHMARK_DEFINE_F(TPartIndexSeekFixture, Next)(benchmark::State& state) {
    const bool useBTree = state.range(0);

    THolder<IIndexIter> iter;

    if (useBTree) {
        iter = MakeHolder<TPartBtreeIndexIt>(Eggs.Lone().Get(), &Env, GroupId);
    } else {
        iter = MakeHolder<TPartIndexIt>(Eggs.Lone().Get(), &Env, GroupId);
    }

    iter->Seek(RandomNumber<ui32>(Eggs.Lone()->Stat.Rows));

    for (auto _ : state) {
        if (!iter->IsValid()) {
            iter->Seek(RandomNumber<ui32>(Eggs.Lone()->Stat.Rows));
        }
        iter->Next();
    }
}

BENCHMARK_DEFINE_F(TPartIndexSeekFixture, Prev)(benchmark::State& state) {
    const bool useBTree = state.range(0);

    THolder<IIndexIter> iter;

    if (useBTree) {
        iter = MakeHolder<TPartBtreeIndexIt>(Eggs.Lone().Get(), &Env, GroupId);
    } else {
        iter = MakeHolder<TPartIndexIt>(Eggs.Lone().Get(), &Env, GroupId);
    }

    iter->Seek(RandomNumber<ui32>(Eggs.Lone()->Stat.Rows));

    for (auto _ : state) {
        if (!iter->IsValid()) {
            iter->Seek(RandomNumber<ui32>(Eggs.Lone()->Stat.Rows));
        }
        iter->Prev();
    }
}

BENCHMARK_DEFINE_F(TPartIndexSeekFixture, SeekKey)(benchmark::State& state) {
    const bool useBTree = state.range(0);
    const ESeek seek = ESeek(state.range(2));

    for (auto _ : state) {
        THolder<IIndexIter> iter;

        if (useBTree) {
            iter = MakeHolder<TPartBtreeIndexIt>(Eggs.Lone().Get(), &Env, GroupId);
        } else {
            iter = MakeHolder<TPartIndexIt>(Eggs.Lone().Get(), &Env, GroupId);
        }

        ui32 rowId = RandomNumber<ui32>(Eggs.Lone()->Stat.Rows);
        TVector<TCell> key{TCell::Make(rowId / 10000), TCell::Make(rowId / 100 % 100), TCell::Make(rowId % 100)};
        iter->Seek(seek, key, Eggs.Scheme->Keys.Get());
    }
}

BENCHMARK_DEFINE_F(TPartIndexIteratorFixture, DoReads)(benchmark::State& state) {
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

BENCHMARK_REGISTER_F(TPartIndexSeekFixture, SeekRowId)
    ->ArgsProduct({
        /* b-tree */ {0, 1},
        /* groups: */ {0, 1}})
    ->Unit(benchmark::kMicrosecond);

BENCHMARK_REGISTER_F(TPartIndexSeekFixture, Next)
    ->ArgsProduct({
        /* b-tree */ {0, 1},
        /* groups: */ {0, 1}})
    ->Unit(benchmark::kMicrosecond);

BENCHMARK_REGISTER_F(TPartIndexSeekFixture, Prev)
    ->ArgsProduct({
        /* b-tree */ {0, 1},
        /* groups: */ {0, 1}})
    ->Unit(benchmark::kMicrosecond);

BENCHMARK_REGISTER_F(TPartIndexSeekFixture, SeekKey)
    ->ArgsProduct({
        /* b-tree */ {0, 1},
        /* groups: */ {0, 1},
        /* ESeek: */ {0, 1, 2}})
    ->Unit(benchmark::kMicrosecond);

BENCHMARK_REGISTER_F(TPartIndexIteratorFixture, DoReads)
    ->ArgsProduct({
        /* b-tree */ {0, 1},
        /* groups: */ {1},
        /* history: */ {1},
        /* reverse: */ {0},
        /* ESeek: */ {1},
        /* items */ {1, 10, 100}})
    ->Unit(benchmark::kMicrosecond);

}
}
