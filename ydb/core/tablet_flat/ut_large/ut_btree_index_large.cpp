#include "flat_page_btree_index.h"
#include "flat_page_btree_index_writer.h"
#include "test/libs/table/test_writer.h"
#include <ydb/core/tablet_flat/test/libs/rows/layout.h>
#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NTable::NPage {

namespace {
    using namespace NTest;
}

// Note: this test requires too much memory when sanitizers are enabled
#if !defined(_tsan_enabled_) && !defined(_msan_enabled_) && !defined(_asan_enabled_)
Y_UNIT_TEST_SUITE(TBtreeIndexTPartLarge) {

    Y_UNIT_TEST(SmallKeys1GB) {
        TLayoutCook lay;

        lay
            .Col(0, 0,  NScheme::NTypeIds::Uint32)
            .Col(0, 1,  NScheme::NTypeIds::String)
            .Key({0});

        NPage::TConf conf{ true, 7 * 1024 };
        conf.WriteBTreeIndex = true;

        TPartCook cook(lay, conf);
        
        for (ui32 i = 0; cook.GetDataBytes(0) < 1ull*1024*1024*1024; i++) {
            cook.Add(*TSchemedCookRow(*lay).Col(i, TString(std::to_string(i))));
        }

        TPartEggs eggs = cook.Finish();

        const auto part = eggs.Lone();

        Cerr << "DataBytes = " << part->Stat.Bytes << " DataPages " << IndexTools::CountMainPages(*part) << Endl;
        UNIT_ASSERT_GE(part->Stat.Bytes, 1ull*1024*1024*1024);
        UNIT_ASSERT_LE(part->Stat.Bytes, 1ull*1024*1024*1024 + 100*1024*1024);

        UNIT_ASSERT_VALUES_EQUAL(part->IndexPages.BTreeGroups[0].LevelsCount, 3);
    }

    Y_UNIT_TEST(MiddleKeys1GB) {
        TLayoutCook lay;

        lay
            .Col(0, 0,  NScheme::NTypeIds::Uint32)
            .Col(0, 1,  NScheme::NTypeIds::String)
            .Key({0, 1});

        NPage::TConf conf{ true, 7 * 1024 };
        conf.WriteBTreeIndex = true;

        TPartCook cook(lay, conf);
        
        for (ui32 i = 0; cook.GetDataBytes(0) < 1ull*1024*1024*1024; i++) {
            cook.Add(*TSchemedCookRow(*lay).Col(0u, TString(120, 'x') + std::to_string(i)));
        }

        TPartEggs eggs = cook.Finish();

        const auto part = eggs.Lone();

        Cerr << "DataBytes = " << part->Stat.Bytes << " DataPages " << IndexTools::CountMainPages(*part) << Endl;
        UNIT_ASSERT_GE(part->Stat.Bytes, 1ull*1024*1024*1024);
        UNIT_ASSERT_LE(part->Stat.Bytes, 1ull*1024*1024*1024 + 100*1024*1024);

        UNIT_ASSERT_VALUES_EQUAL(part->IndexPages.BTreeGroups[0].LevelsCount, 3);
    }

    Y_UNIT_TEST(BigKeys1GB) {
        TLayoutCook lay;

        lay
            .Col(0, 0,  NScheme::NTypeIds::Uint32)
            .Col(0, 1,  NScheme::NTypeIds::String)
            .Key({0, 1});

        NPage::TConf conf{ true, 7 * 1024 };
        conf.WriteBTreeIndex = true;

        TPartCook cook(lay, conf);
        
        for (ui32 i = 0; cook.GetDataBytes(0) < 1ull*1024*1024*1024; i++) {
            cook.Add(*TSchemedCookRow(*lay).Col(0u, TString(7*1024, 'x') + std::to_string(i)));
        }

        TPartEggs eggs = cook.Finish();

        const auto part = eggs.Lone();

        Cerr << "DataBytes = " << part->Stat.Bytes << " DataPages " << IndexTools::CountMainPages(*part) << Endl;
        UNIT_ASSERT_GE(part->Stat.Bytes, 1ull*1024*1024*1024);
        UNIT_ASSERT_LE(part->Stat.Bytes, 1ull*1024*1024*1024 + 100*1024*1024);

        UNIT_ASSERT_VALUES_EQUAL(part->IndexPages.BTreeGroups[0].LevelsCount, 6);
    }
}
#endif

}
