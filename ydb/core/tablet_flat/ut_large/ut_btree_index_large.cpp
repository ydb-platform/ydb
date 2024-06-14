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
        conf.WriteFlatIndex = true;

        TPartCook cook(lay, conf);
        
        for (ui32 i = 0; cook.GetDataBytes(0) < 1ull*1024*1024*1024; i++) {
            cook.Add(*TSchemedCookRow(*lay).Col(i, TString(std::to_string(i))));
        }

        TPartEggs eggs = cook.Finish();

        const auto part = eggs.Lone();

        Cerr << "DataBytes = " << part->Stat.Bytes << " DataPages = " << IndexTools::CountMainPages(*part) << Endl;
        Cerr << "FlatIndexBytes = " << part->GetPageSize(part->IndexPages.FlatGroups[0], {}) << " BTreeIndexBytes = " << part->IndexPages.BTreeGroups[0].IndexSize << Endl;

        UNIT_ASSERT_GE(part->Stat.Bytes, 1ull*1024*1024*1024);
        UNIT_ASSERT_LE(part->Stat.Bytes, 1ull*1024*1024*1024 + 100*1024*1024);

        UNIT_ASSERT_VALUES_EQUAL(part->IndexPages.BTreeGroups[0].LevelCount, 3);
    }

    Y_UNIT_TEST(MiddleKeys1GB) {
        TLayoutCook lay;

        lay
            .Col(0, 0,  NScheme::NTypeIds::Uint32)
            .Col(0, 1,  NScheme::NTypeIds::String)
            .Key({0, 1});

        NPage::TConf conf{ true, 7 * 1024 };
        conf.WriteBTreeIndex = true;
        conf.WriteFlatIndex = true;

        TPartCook cook(lay, conf);
        
        for (ui32 i = 0; cook.GetDataBytes(0) < 1ull*1024*1024*1024; i++) {
            cook.Add(*TSchemedCookRow(*lay).Col(0u, TString(110, 'x') + std::to_string(i)));
        }

        TPartEggs eggs = cook.Finish();

        const auto part = eggs.Lone();

        Cerr << "DataBytes = " << part->Stat.Bytes << " DataPages = " << IndexTools::CountMainPages(*part) << Endl;
        Cerr << "FlatIndexBytes = " << part->GetPageSize(part->IndexPages.FlatGroups[0], {}) << " BTreeIndexBytes = " << part->IndexPages.BTreeGroups[0].IndexSize << Endl;

        UNIT_ASSERT_GE(part->Stat.Bytes, 1ull*1024*1024*1024);
        UNIT_ASSERT_LE(part->Stat.Bytes, 1ull*1024*1024*1024 + 100*1024*1024);

        UNIT_ASSERT_VALUES_EQUAL(part->IndexPages.BTreeGroups[0].LevelCount, 3);
    }

    Y_UNIT_TEST(BigKeys1GB) {
        TLayoutCook lay;

        lay
            .Col(0, 0,  NScheme::NTypeIds::Uint32)
            .Col(0, 1,  NScheme::NTypeIds::String)
            .Key({0, 1});

        NPage::TConf conf{ true, 7 * 1024 };
        conf.WriteBTreeIndex = true;
        conf.WriteFlatIndex = true;

        TPartCook cook(lay, conf);
        
        for (ui32 i = 0; cook.GetDataBytes(0) < 1ull*1024*1024*1024; i++) {
            cook.Add(*TSchemedCookRow(*lay).Col(0u, TString(7*1024, 'x') + std::to_string(i)));
        }

        TPartEggs eggs = cook.Finish();

        const auto part = eggs.Lone();

        Cerr << "DataBytes = " << part->Stat.Bytes << " DataPages = " << IndexTools::CountMainPages(*part) << Endl;
        Cerr << "FlatIndexBytes = " << part->GetPageSize(part->IndexPages.FlatGroups[0], {}) << " BTreeIndexBytes = " << part->IndexPages.BTreeGroups[0].IndexSize << Endl;

        UNIT_ASSERT_GE(part->Stat.Bytes, 1ull*1024*1024*1024);
        UNIT_ASSERT_LE(part->Stat.Bytes, 1ull*1024*1024*1024 + 100*1024*1024);

        UNIT_ASSERT_VALUES_EQUAL(part->IndexPages.BTreeGroups[0].LevelCount, 6);
    }

    Y_UNIT_TEST(CutKeys) {
        TLayoutCook lay;

        lay
            .Col(0, 0,  NScheme::NTypeIds::Uint64)
            .Col(0, 1,  NScheme::NTypeIds::Uint64)
            .Col(0, 2,  NScheme::NTypeIds::Uint64)
            .Col(0, 3,  NScheme::NTypeIds::String)
            .Key({0, 1, 2});

        NPage::TConf conf{ true, 7 * 1024 };
        conf.WriteBTreeIndex = true;
        conf.WriteFlatIndex = true;

        TPartCook cook(lay, conf);
        
        for (ui64 i = 0; cook.GetDataBytes(0) < 1ull*1024*1024*1024; i++) {
            cook.Add(*TSchemedCookRow(*lay).Col(i, i, i, TString(std::to_string(i))));
        }

        TPartEggs eggs = cook.Finish();

        const auto part = eggs.Lone();

        Cerr << "DataBytes = " << part->Stat.Bytes << " DataPages = " << IndexTools::CountMainPages(*part) << Endl;
        Cerr << "FlatIndexBytes = " << part->GetPageSize(part->IndexPages.FlatGroups[0], {}) << " BTreeIndexBytes = " << part->IndexPages.BTreeGroups[0].IndexSize << Endl;

        UNIT_ASSERT_GE(part->Stat.Bytes, 1ull*1024*1024*1024);
        UNIT_ASSERT_LE(part->Stat.Bytes, 1ull*1024*1024*1024 + 100*1024*1024);

        UNIT_ASSERT_VALUES_EQUAL(part->IndexPages.BTreeGroups[0].LevelCount, 3);
    }

    Y_UNIT_TEST(Group) {
        TLayoutCook lay;

        lay
            .Col(0, 0,  NScheme::NTypeIds::Uint32)
            .Col(1, 1,  NScheme::NTypeIds::String)
            .Key({0});

        NPage::TConf conf{ true, 7 * 1024 };
        conf.Group(1).PageSize = 7 * 1024;
        conf.WriteBTreeIndex = true;
        conf.WriteFlatIndex = true;

        TPartCook cook(lay, conf);
        
        for (ui32 i = 0; cook.GetDataBytes(0) + cook.GetDataBytes(1) < 1ull*1024*1024*1024; i++) {
            cook.Add(*TSchemedCookRow(*lay).Col(i, TString(std::to_string(i))));
        }

        TPartEggs eggs = cook.Finish();

        const auto part = eggs.Lone();

        Cerr << "DataBytes = " << part->Stat.Bytes << " DataPages = " << IndexTools::CountMainPages(*part) << Endl;
        Cerr << "FlatIndexBytes = " << part->GetPageSize(part->IndexPages.FlatGroups[1], {}) << " BTreeIndexBytes = " << part->IndexPages.BTreeGroups[1].IndexSize << Endl;

        UNIT_ASSERT_GE(part->Stat.Bytes, 1ull*1024*1024*1024);
        UNIT_ASSERT_LE(part->Stat.Bytes, 1ull*1024*1024*1024 + 100*1024*1024);

        UNIT_ASSERT_VALUES_EQUAL(part->IndexPages.BTreeGroups[1].LevelCount, 2);
    }

    Y_UNIT_TEST(History) {
        TLayoutCook lay;

        lay
            .Col(0, 0,  NScheme::NTypeIds::Uint32)
            .Col(0, 1,  NScheme::NTypeIds::String)
            .Key({0});

        NPage::TConf conf{ true, 7 * 1024 };
        conf.WriteBTreeIndex = true;
        conf.WriteFlatIndex = true;

        TPartCook cook(lay, conf);
        
        for (ui32 i = 0; cook.GetDataBytes(0) < 1ull*1024*1024*1024; i++) {
            cook.Ver({0, 2}).Add(*TSchemedCookRow(*lay).Col(i, TString(std::to_string(i))));
            cook.Ver({0, 1}).Add(*TSchemedCookRow(*lay).Col(i, TString(std::to_string(i + 1))));
        }

        TPartEggs eggs = cook.Finish();

        const auto part = eggs.Lone();

        Cerr << "DataBytes = " << part->Stat.Bytes << " DataPages = " << IndexTools::CountMainPages(*part) << Endl;
        Cerr << "FlatIndexBytes = " << part->GetPageSize(part->IndexPages.FlatHistoric[0], {}) << " BTreeIndexBytes = " << part->IndexPages.BTreeHistoric[0].IndexSize << Endl;

        UNIT_ASSERT_GE(part->Stat.Bytes, 1ull*1024*1024*1024);
        UNIT_ASSERT_LE(part->Stat.Bytes, 1ull*1024*1024*1024 + 100*1024*1024);

        UNIT_ASSERT_VALUES_EQUAL(part->IndexPages.BTreeHistoric[0].LevelCount, 3);
    }

}
#endif

}
