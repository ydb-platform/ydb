#include "flat_part_charge_range.h"
#include "flat_stat_table.h"
#include "flat_stat_table_mixed_index.h"
#include "flat_stat_table_btree_index.h"
#include <test/libs/table/wrap_iter.h>
#include <test/libs/table/wrap_part.h>
#include <ydb/core/tablet_flat/test/libs/table/model/large.h>
#include <ydb/core/tablet_flat/test/libs/table/test_make.h>
#include <ydb/core/tablet_flat/test/libs/table/test_mixer.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NTable {

namespace {
    using namespace NTest;

    struct TTouchEnv : public NTest::TTestEnv {
        const TSharedData* TryGetPage(const TPart *part, TPageId pageId, TGroupId groupId) override
        {
            auto page = NTest::TTestEnv::TryGetPage(part, pageId, groupId);

            bool newTouch = Touched[{part, groupId}].insert(pageId).second;

            if (newTouch) {
                auto type = part->GetPageType(pageId, groupId);
                if (type == EPage::DataPage) {
                    auto dataPage = NPage::TDataPage(page);
                    
                    TouchedBytes += page->size();
                    if (groupId.IsMain()) {
                        TouchedRows += dataPage->Count;
                    }
                }
                if (type == EPage::FlatIndex || type == EPage::BTreeIndex) {
                    TouchedIndexPages++;
                    TouchedIndexBytes += page->size();
                }
            }

            return newTouch && Faulty
                ? nullptr
                : page;
        }

        TMap<std::pair<const TPart*, TGroupId>, TSet<TPageId>> Touched;
        bool Faulty = true;
        ui64 TouchedBytes = 0, TouchedRows = 0, TouchedIndexBytes = 0, TouchedIndexPages = 0;
    };

    NPage::TConf PageConf(size_t groups, bool writeBTreeIndex, bool lowResolution = false) noexcept
    {
        NPage::TConf conf{ true, 2 * 1024 };

        conf.Groups.resize(groups);
        for (size_t group : xrange(groups)) {
            conf.Group(group).IndexMin = 1024; /* Should cover index buffer grow code */
            conf.Group(group).BTreeIndexNodeTargetSize = 512; /* Should cover up/down moves */
            if (lowResolution) {
                // make more levels
                conf.Group(group).BTreeIndexNodeKeysMin = conf.Group(group).BTreeIndexNodeKeysMax = 2;
            }
        }
        conf.SmallEdge = 19;  /* Packed to page collection large cell values */
        conf.LargeEdge = 29;  /* Large values placed to single blobs */

        conf.CutIndexKeys = false;
        conf.WriteBTreeIndex = writeBTreeIndex;

        return conf;
    }

    const NTest::TMass Mass0(new NTest::TModelStd(false), 24000);
    const NTest::TMass Mass1(new NTest::TModelStd(true), 24000);
    const NTest::TMass Mass2(new NTest::TModelStd(false), 240000);
    const NTest::TMass Mass3(new NTest::TModelStd(false), 500);

    void CheckMixedIndex(const TSubset& subset, ui64 expectedRows, ui64 expectedData, ui64 expectedIndex, ui64 rowCountResolution = 531, ui64 dataSizeResolution = 53105) {
        TStats stats;
        TTouchEnv env;

        const ui32 attempts = 10;
        for (ui32 attempt : xrange(attempts)) {
            if (NTable::BuildStatsMixedIndex(subset, stats, rowCountResolution, dataSizeResolution, &env, [](){})) {
                break;
            }
            UNIT_ASSERT_C(attempt + 1 < attempts, "Too many attempts");
        }

        Cerr << "Got     : " << stats.RowCount << " " << stats.DataSize.Size << " " << stats.IndexSize.Size << " " << stats.DataSizeHistogram.size() << " " << stats.RowCountHistogram.size() << Endl;
        Cerr << "Expected: " << expectedRows << " " << expectedData << " " << expectedIndex << " " << stats.DataSizeHistogram.size() << " " << stats.RowCountHistogram.size() << Endl;
        UNIT_ASSERT_VALUES_EQUAL(stats.RowCount, expectedRows);
        UNIT_ASSERT_VALUES_EQUAL(stats.DataSize.Size, expectedData);
        UNIT_ASSERT_VALUES_EQUAL(stats.IndexSize.Size, expectedIndex);
    }

    void CheckBTreeIndex(const TSubset& subset, ui64 expectedRows, ui64 expectedData, ui64 expectedIndex, ui32 histogramBucketsCount = 10) {
        TStats stats;
        TTouchEnv env;

        const ui32 attempts = 25;
        for (ui32 attempt : xrange(attempts)) {
            if (NTable::BuildStatsBTreeIndex(subset, stats, histogramBucketsCount, &env, [](){})) {
                break;
            }
            UNIT_ASSERT_C(attempt + 1 < attempts, "Too many attempts");
        }

        if (subset.Flatten.begin()->Slices->size() == 1) {
            ui64 byIndexBytes = 0;
            for (const auto& part : subset.Flatten) {
                auto &root = part->IndexPages.GetBTree({});
                byIndexBytes += root.GetDataSize() + root.GetGroupDataSize();
            }
            UNIT_ASSERT_VALUES_EQUAL(byIndexBytes, expectedData);
        }

        Cerr << "Got     : " << stats.RowCount << " " << stats.DataSize.Size << " " << stats.IndexSize.Size << " " << stats.DataSizeHistogram.size() << " " << stats.RowCountHistogram.size() << Endl;
        Cerr << "Expected: " << expectedRows << " " << expectedData << " " << expectedIndex << " " << stats.DataSizeHistogram.size() << " " << stats.RowCountHistogram.size() << Endl;
        UNIT_ASSERT_VALUES_EQUAL(stats.RowCount, expectedRows);
        UNIT_ASSERT_VALUES_EQUAL(stats.DataSize.Size, expectedData);
        UNIT_ASSERT_VALUES_EQUAL(stats.IndexSize.Size, expectedIndex);
    }
}

Y_UNIT_TEST_SUITE(BuildStatsFlatIndex) {
    using namespace NTest;
    const bool WriteBTreeIndex = false;

    Y_UNIT_TEST(Single)
    {
        auto subset = TMake(Mass0, PageConf(Mass0.Model->Scheme->Families.size(), WriteBTreeIndex)).Mixed(0, 1, TMixerOne{ });   
        CheckMixedIndex(*subset, 24000, 2106439, 25272);
    }

    Y_UNIT_TEST(Single_Slices)
    {
        auto subset = TMake(Mass0, PageConf(Mass0.Model->Scheme->Families.size(), WriteBTreeIndex)).Mixed(0, 1, TMixerOne{ }, 0, 13);   
        subset->Flatten.begin()->Slices->Describe(Cerr); Cerr << Endl;
        CheckMixedIndex(*subset, 12816, 1121048, 25272);
    }

    Y_UNIT_TEST(Single_History)
    {
        auto subset = TMake(Mass0, PageConf(Mass0.Model->Scheme->Families.size(), WriteBTreeIndex)).Mixed(0, 1, TMixerOne{ }, 0.3);   
        CheckMixedIndex(*subset, 24000, 3547100, 49916);
    }

    Y_UNIT_TEST(Single_History_Slices)
    {
        auto subset = TMake(Mass0, PageConf(Mass0.Model->Scheme->Families.size(), WriteBTreeIndex)).Mixed(0, 1, TMixerOne{ }, 0.3, 13);   
        subset->Flatten.begin()->Slices->Describe(Cerr); Cerr << Endl;
        CheckMixedIndex(*subset, 9582, 1425198, 49916);
    }

    Y_UNIT_TEST(Single_Groups)
    {
        auto subset = TMake(Mass1, PageConf(Mass1.Model->Scheme->Families.size(), WriteBTreeIndex)).Mixed(0, 1, TMixerOne{ });   
        CheckMixedIndex(*subset, 24000, 2460139, 13170);
    }

    Y_UNIT_TEST(Single_Groups_Slices)
    {
        auto subset = TMake(Mass1, PageConf(Mass1.Model->Scheme->Families.size(), WriteBTreeIndex)).Mixed(0, 1, TMixerOne{ }, 0, 13);   
        subset->Flatten.begin()->Slices->Describe(Cerr); Cerr << Endl;
        CheckMixedIndex(*subset, 10440, 1060798, 13170);
    }

    Y_UNIT_TEST(Single_Groups_History)
    {
        auto subset = TMake(Mass1, PageConf(Mass1.Model->Scheme->Families.size(), WriteBTreeIndex)).Mixed(0, 1, TMixerOne{ }, 0.3);   
        CheckMixedIndex(*subset, 24000, 4054050, 29361);
    }

    Y_UNIT_TEST(Single_Groups_History_Slices)
    {
        auto subset = TMake(Mass1, PageConf(Mass1.Model->Scheme->Families.size(), WriteBTreeIndex)).Mixed(0, 1, TMixerOne{ }, 0.3, 13);   
        subset->Flatten.begin()->Slices->Describe(Cerr); Cerr << Endl;
        CheckMixedIndex(*subset, 13570, 2277890, 29361);
    }

    Y_UNIT_TEST(Mixed)
    {
        auto subset = TMake(Mass0, PageConf(Mass0.Model->Scheme->Families.size(), WriteBTreeIndex)).Mixed(0, 4, TMixerRnd(4));
        CheckMixedIndex(*subset, 24000, 2106459, 25428);
    }

    Y_UNIT_TEST(Mixed_Groups)
    {
        auto subset = TMake(Mass1, PageConf(Mass1.Model->Scheme->Families.size(), WriteBTreeIndex)).Mixed(0, 4, TMixerRnd(4));
        CheckMixedIndex(*subset, 24000, 2460219, 13482);
    }

    Y_UNIT_TEST(Mixed_Groups_History)
    {
        auto subset = TMake(Mass1, PageConf(Mass1.Model->Scheme->Families.size(), WriteBTreeIndex)).Mixed(0, 4, TMixerRnd(4), 0.3);
        CheckMixedIndex(*subset, 24000, 4054270, 29970);
    }

    Y_UNIT_TEST(Serial)
    {
        TMixerSeq mixer(4, Mass0.Saved.Size());
        auto subset = TMake(Mass0, PageConf(Mass0.Model->Scheme->Families.size(), WriteBTreeIndex)).Mixed(0, 4, mixer);
        CheckMixedIndex(*subset, 24000, 2106479, 25458);
    }

    Y_UNIT_TEST(Serial_Groups)
    {
        TMixerSeq mixer(4, Mass1.Saved.Size());
        auto subset = TMake(Mass1, PageConf(Mass1.Model->Scheme->Families.size(), WriteBTreeIndex)).Mixed(0, 4, mixer);
        CheckMixedIndex(*subset, 24000, 2460259, 13528);
    }

    Y_UNIT_TEST(Serial_Groups_History)
    {
        TMixerSeq mixer(4, Mass1.Saved.Size());
        auto subset = TMake(Mass1, PageConf(Mass1.Model->Scheme->Families.size(), WriteBTreeIndex)).Mixed(0, 4, mixer, 0.3);
        CheckMixedIndex(*subset, 24000, 4054290, 30013);
    }
}

Y_UNIT_TEST_SUITE(BuildStatsMixedIndex) {
    using namespace NTest;
    const bool WriteBTreeIndex = true;

    Y_UNIT_TEST(Single)
    {
        auto subset = TMake(Mass0, PageConf(Mass0.Model->Scheme->Families.size(), WriteBTreeIndex)).Mixed(0, 1, TMixerOne{ });   
        CheckMixedIndex(*subset, 24000, 2106439, 49449);
    }

    Y_UNIT_TEST(Single_Slices)
    {
        auto subset = TMake(Mass0, PageConf(Mass0.Model->Scheme->Families.size(), WriteBTreeIndex)).Mixed(0, 1, TMixerOne{ }, 0, 13);   
        subset->Flatten.begin()->Slices->Describe(Cerr); Cerr << Endl;
        CheckMixedIndex(*subset, 12816, 1121048, 49449);
    }

    Y_UNIT_TEST(Single_History)
    {
        auto subset = TMake(Mass0, PageConf(Mass0.Model->Scheme->Families.size(), WriteBTreeIndex)).Mixed(0, 1, TMixerOne{ }, 0.3);
        CheckMixedIndex(*subset, 24000, 3547100, 81694);
    }

    Y_UNIT_TEST(Single_History_Slices)
    {
        auto subset = TMake(Mass0, PageConf(Mass0.Model->Scheme->Families.size(), WriteBTreeIndex)).Mixed(0, 1, TMixerOne{ }, 0.3, 13);   
        subset->Flatten.begin()->Slices->Describe(Cerr); Cerr << Endl;
        CheckMixedIndex(*subset, 9582, 1425198, 81694);
    }

    Y_UNIT_TEST(Single_Groups)
    {
        auto subset = TMake(Mass1, PageConf(Mass1.Model->Scheme->Families.size(), WriteBTreeIndex)).Mixed(0, 1, TMixerOne{ });   
        CheckMixedIndex(*subset, 24000, 2460139, 23760);
    }

    Y_UNIT_TEST(Single_Groups_Slices)
    {
        auto subset = TMake(Mass1, PageConf(Mass1.Model->Scheme->Families.size(), WriteBTreeIndex)).Mixed(0, 1, TMixerOne{ }, 0, 13);   
        subset->Flatten.begin()->Slices->Describe(Cerr); Cerr << Endl;
        CheckMixedIndex(*subset, 10440, 1060798, 23760);
    }

    Y_UNIT_TEST(Single_Groups_History)
    {
        auto subset = TMake(Mass1, PageConf(Mass1.Model->Scheme->Families.size(), WriteBTreeIndex)).Mixed(0, 1, TMixerOne{ }, 0.3);   
        CheckMixedIndex(*subset, 24000, 4054050, 46562);
    }

    Y_UNIT_TEST(Single_Groups_History_Slices)
    {
        auto subset = TMake(Mass1, PageConf(Mass1.Model->Scheme->Families.size(), WriteBTreeIndex)).Mixed(0, 1, TMixerOne{ }, 0.3, 13);   
        subset->Flatten.begin()->Slices->Describe(Cerr); Cerr << Endl;
        CheckMixedIndex(*subset, 13570, 2277890, 46562);
    }

    Y_UNIT_TEST(Mixed)
    {
        auto subset = TMake(Mass0, PageConf(Mass0.Model->Scheme->Families.size(), WriteBTreeIndex)).Mixed(0, 4, TMixerRnd(4));
        CheckMixedIndex(*subset, 24000, 2106459, 49449);
    }

    Y_UNIT_TEST(Mixed_Groups)
    {
        auto subset = TMake(Mass1, PageConf(Mass1.Model->Scheme->Families.size(), WriteBTreeIndex)).Mixed(0, 4, TMixerRnd(4));
        CheckMixedIndex(*subset, 24000, 2460219, 23555);
    }

    Y_UNIT_TEST(Mixed_Groups_History)
    {
        auto subset = TMake(Mass1, PageConf(Mass1.Model->Scheme->Families.size(), WriteBTreeIndex)).Mixed(0, 4, TMixerRnd(4), 0.3);
        CheckMixedIndex(*subset, 24000, 4054270, 46543);
    }

    Y_UNIT_TEST(Serial)
    {
        TMixerSeq mixer(4, Mass0.Saved.Size());
        auto subset = TMake(Mass0, PageConf(Mass0.Model->Scheme->Families.size(), WriteBTreeIndex)).Mixed(0, 4, mixer);
        CheckMixedIndex(*subset, 24000, 2106479, 49555);
    }

    Y_UNIT_TEST(Serial_Groups)
    {
        TMixerSeq mixer(4, Mass1.Saved.Size());
        auto subset = TMake(Mass1, PageConf(Mass1.Model->Scheme->Families.size(), WriteBTreeIndex)).Mixed(0, 4, mixer);
        CheckMixedIndex(*subset, 24000, 2460259, 23628);
    }

    Y_UNIT_TEST(Serial_Groups_History)
    {
        TMixerSeq mixer(4, Mass1.Saved.Size());
        auto subset = TMake(Mass1, PageConf(Mass1.Model->Scheme->Families.size(), WriteBTreeIndex)).Mixed(0, 4, mixer, 0.3);
        CheckMixedIndex(*subset, 24000, 4054290, 46640);
    }

    Y_UNIT_TEST(Single_LowResolution)
    {
        auto subset = TMake(Mass0, PageConf(Mass0.Model->Scheme->Families.size(), true, WriteBTreeIndex)).Mixed(0, 1, TMixerOne{ });   
        CheckMixedIndex(*subset, 24000, 2106439, 66674, 5310, 531050);
    }

    Y_UNIT_TEST(Single_Slices_LowResolution)
    {
        auto subset = TMake(Mass0, PageConf(Mass0.Model->Scheme->Families.size(), true, WriteBTreeIndex)).Mixed(0, 1, TMixerOne{ }, 0, 13);   
        subset->Flatten.begin()->Slices->Describe(Cerr); Cerr << Endl;
        CheckMixedIndex(*subset, 12816, 1121048, 66674, 5310, 531050);
    }

    Y_UNIT_TEST(Single_Groups_LowResolution)
    {
        auto subset = TMake(Mass1, PageConf(Mass1.Model->Scheme->Families.size(), true, WriteBTreeIndex)).Mixed(0, 1, TMixerOne{ });   
        CheckMixedIndex(*subset, 24000, 2460139, 33541, 5310, 531050);
    }

    Y_UNIT_TEST(Single_Groups_Slices_LowResolution)
    {
        auto subset = TMake(Mass1, PageConf(Mass1.Model->Scheme->Families.size(), true, WriteBTreeIndex)).Mixed(0, 1, TMixerOne{ }, 0, 13);   
        subset->Flatten.begin()->Slices->Describe(Cerr); Cerr << Endl;
        CheckMixedIndex(*subset, 10440, 1060798, 33541, 5310, 531050);
    }

    Y_UNIT_TEST(Single_Groups_History_LowResolution)
    {
        auto subset = TMake(Mass1, PageConf(Mass1.Model->Scheme->Families.size(), true, WriteBTreeIndex)).Mixed(0, 1, TMixerOne{ }, 0.3);   
        CheckMixedIndex(*subset, 24000, 4054050, 64742, 5310, 531050);
    }

    Y_UNIT_TEST(Single_Groups_History_Slices_LowResolution)
    {
        auto subset = TMake(Mass1, PageConf(Mass1.Model->Scheme->Families.size(), true, WriteBTreeIndex)).Mixed(0, 1, TMixerOne{ }, 0.3, 13);   
        subset->Flatten.begin()->Slices->Describe(Cerr); Cerr << Endl;
        CheckMixedIndex(*subset, 13570, 2234982 /* ~2277890 */, 64742, 5310, 531050);
    }
}

Y_UNIT_TEST_SUITE(BuildStatsBTreeIndex) {
    using namespace NTest;
    const bool WriteBTreeIndex = true;

    Y_UNIT_TEST(Single)
    {
        auto subset = TMake(Mass0, PageConf(Mass0.Model->Scheme->Families.size(), WriteBTreeIndex)).Mixed(0, 1, TMixerOne{ });   
        CheckBTreeIndex(*subset, 24000, 2106439, 49449);
    }

    Y_UNIT_TEST(Single_Slices)
    {
        auto subset = TMake(Mass0, PageConf(Mass0.Model->Scheme->Families.size(), WriteBTreeIndex)).Mixed(0, 1, TMixerOne{ }, 0, 13);   
        subset->Flatten.begin()->Slices->Describe(Cerr); Cerr << Endl;
        CheckBTreeIndex(*subset, 12816, 1121048, 49449);
    }

    Y_UNIT_TEST(Single_History)
    {
        auto subset = TMake(Mass0, PageConf(Mass0.Model->Scheme->Families.size(), WriteBTreeIndex)).Mixed(0, 1, TMixerOne{ }, 0.3);
        CheckBTreeIndex(*subset, 24000, 3547100, 81694);
    }

    Y_UNIT_TEST(Single_History_Slices)
    {
        auto subset = TMake(Mass0, PageConf(Mass0.Model->Scheme->Families.size(), WriteBTreeIndex)).Mixed(0, 1, TMixerOne{ }, 0.3, 13);   
        subset->Flatten.begin()->Slices->Describe(Cerr); Cerr << Endl;
        CheckBTreeIndex(*subset, 9582, 1425282, 81694);
    }

    Y_UNIT_TEST(Single_Groups)
    {
        auto subset = TMake(Mass1, PageConf(Mass1.Model->Scheme->Families.size(), WriteBTreeIndex)).Mixed(0, 1, TMixerOne{ });   
        CheckBTreeIndex(*subset, 24000, 2460139, 23760);
    }

    Y_UNIT_TEST(Single_Groups_Slices)
    {
        auto subset = TMake(Mass1, PageConf(Mass1.Model->Scheme->Families.size(), WriteBTreeIndex)).Mixed(0, 1, TMixerOne{ }, 0, 13);   
        subset->Flatten.begin()->Slices->Describe(Cerr); Cerr << Endl;
        CheckBTreeIndex(*subset, 10440, 1060767, 23760);
    }

    Y_UNIT_TEST(Single_Groups_History)
    {
        auto subset = TMake(Mass1, PageConf(Mass1.Model->Scheme->Families.size(), WriteBTreeIndex)).Mixed(0, 1, TMixerOne{ }, 0.3);   
        CheckBTreeIndex(*subset, 24000, 4054050, 46562);
    }

    Y_UNIT_TEST(Single_Groups_History_Slices)
    {
        auto subset = TMake(Mass1, PageConf(Mass1.Model->Scheme->Families.size(), WriteBTreeIndex)).Mixed(0, 1, TMixerOne{ }, 0.3, 13);   
        subset->Flatten.begin()->Slices->Describe(Cerr); Cerr << Endl;
        CheckBTreeIndex(*subset, 13570, 2273213, 46562);
    }

    Y_UNIT_TEST(Mixed)
    {
        auto subset = TMake(Mass0, PageConf(Mass0.Model->Scheme->Families.size(), WriteBTreeIndex)).Mixed(0, 4, TMixerRnd(4));
        CheckBTreeIndex(*subset, 24000, 2106459, 49449);
    }

    Y_UNIT_TEST(Mixed_Groups)
    {
        auto subset = TMake(Mass1, PageConf(Mass1.Model->Scheme->Families.size(), WriteBTreeIndex)).Mixed(0, 4, TMixerRnd(4));
        CheckBTreeIndex(*subset, 24000, 2460219, 23555);
    }

    Y_UNIT_TEST(Mixed_Groups_History)
    {
        auto subset = TMake(Mass1, PageConf(Mass1.Model->Scheme->Families.size(), WriteBTreeIndex)).Mixed(0, 4, TMixerRnd(4), 0.3);
        CheckBTreeIndex(*subset, 24000, 4054270, 46543);
    }
}

Y_UNIT_TEST_SUITE(BuildStatsHistogram) {
    using namespace NTest;

    enum TMode {
        BTreeIndex,
        FlatIndex,
        MixedIndex,
    };

    NPage::TConf PageConf(size_t groups, TMode mode) noexcept
    {
        NPage::TConf conf{ true, 2 * 1024 };

        conf.Groups.resize(groups);
        for (size_t group : xrange(groups)) {
            conf.Group(group).BTreeIndexNodeKeysMin = conf.Group(group).BTreeIndexNodeKeysMax = 3;
        }

        conf.CutIndexKeys = false;
        conf.WriteBTreeIndex = (mode == FlatIndex ? false : true);

        return conf;
    }

    void Dump(const TSubset& subset)
    {
        const ui32 samples = 5;

        Cerr << subset.Flatten.size() << " parts:" << Endl;
        for (auto &part : subset.Flatten) {
            TTestEnv env;
            auto index = CreateIndexIter(part.Part.Get(), &env, {});
            Cerr << "  " << index->GetEndRowId() << " rows, " 
                << IndexTools::CountMainPages(*part.Part) << " pages, "
                << (part->IndexPages.HasBTree() ? part->IndexPages.GetBTree({}).LevelCount : -1) << " levels: ";
            for (ui32 sample : xrange(1u, samples + 1)) {
                TRowId rowId((index->GetEndRowId() - 1) * sample / samples);
                Y_ABORT_UNLESS(index->Seek(rowId) == EReady::Data);
                TSmallVec<TCell> keyCells;
                index->GetKeyCells(keyCells);
                Cerr << "(";
                for (auto off : xrange(keyCells.size())) {
                    TString str;
                    DbgPrintValue(str, keyCells[off], subset.Scheme->Cols[off].TypeInfo);
                    Cerr << (off ? ", " : "") << str;
                }
                Cerr << ") ";
            }
            // Cerr << DumpPart(*part.As<TPartStore>(), 2) << Endl;
            Cerr << Endl;
        }
    }

    TString FormatPercent(double value, ui64 total) {
        if (total == 0) {
            return "0%";
        }
        return TStringBuilder() << static_cast<int>(100.0 * value / total) << "%";
    }

    void VerifyPercent(double value, ui64 total, int allowed) {
        auto percent = static_cast<int>(100.0 * value / total);
        UNIT_ASSERT_LE(std::abs(percent), allowed);
    }

    void CalcDataBeforeIterate(const TSubset& subset, TSerializedCellVec key, ui64& bytes, ui64& rows) {
        NTest::TChecker<NTest::TWrapIter, TSubset> wrap(subset, { new TTouchEnv });
        auto env = wrap.GetEnv<TTouchEnv>();
        env->Faulty = false;
        
        bytes = 0;
        rows = 0;
        wrap.Seek({}, ESeek::Lower);

        while (wrap.GetReady() == EReady::Data) {
            ui64 prevBytes = env->TouchedBytes;

            wrap.Next();

            if (wrap.GetReady() == EReady::Data && key.GetCells()) {
                auto cmp = CompareTypedCellVectors(key.GetCells().data(), wrap->GetKey().Cells().data(), subset.Scheme->Keys->Types.data(), Min(key.GetCells().size(), wrap->GetKey().Cells().size()));
                if (cmp < 0) {
                    break;
                }
            }

            rows++;
            bytes = prevBytes;
        }
    }

    void CalcDataBeforePrecharge(const TSubset& subset, TSerializedCellVec key, ui64& bytes, ui64& rows) {
        TTouchEnv env;
        env.Faulty = false;

        for (auto& part : subset.Flatten) {
            const auto& keyDefaults = *subset.Scheme->Keys.Get();
            TRun run(keyDefaults);
            for (auto& slice : *part.Slices) {
                run.Insert(part.Part, slice);
            }
            auto tags = TVector<TTag>();
            for (auto c : subset.Scheme->Cols) {
                tags.push_back(c.Tag);
            }
            Y_ABORT_UNLESS(ChargeRange(&env, {}, key.GetCells(), run, keyDefaults, tags, 0, 0, true));
        }

        bytes = env.TouchedBytes;
        rows = env.TouchedRows;
    }

    void CalcDataBefore(const TSubset& subset, TSerializedCellVec key, ui64& bytes, ui64& rows) {
        bool groups = false;
        rows = 0;
        for (const auto& part : subset.Flatten) {
            TTestEnv env;
            auto index = CreateIndexIter(part.Part.Get(), &env, {});
            rows += index->GetEndRowId();
            groups |= part->GroupsCount > 1 || part->HistoricGroupsCount > 0;
        }

        if (groups || rows > 10000) {
            CalcDataBeforePrecharge(subset, key, bytes, rows);
        } else {
            CalcDataBeforeIterate(subset, key, bytes, rows);
        }
    }

    void CheckHistogram(const TSubset& subset, THistogram histogram, bool isBytes, ui64 total, bool verifyPercents) {
        Cerr << "  " << (isBytes ? "DataSizeHistogram:" : "RowCountHistogram:") << Endl;

        ui64 prevValue = 0, prevActualValue = 0;

        for (const auto& bucket : histogram) {
            TSerializedCellVec key(bucket.EndKey);
            ui64 actualBytes, actualRows;
            CalcDataBefore(subset, key, actualBytes, actualRows);
            ui64 actualValue = isBytes ? actualBytes : actualRows;

            ui64 delta = bucket.Value - prevValue, actualDelta = actualValue - prevActualValue;
            Cerr << "    " << FormatPercent(delta, total) << " (actual " << FormatPercent(actualDelta, total) << ")" << Endl;
            if (verifyPercents) VerifyPercent(delta, total, 20);

            Cerr << "    key = (";
            for (auto off : xrange(key.GetCells().size())) {
                TString str;
                DbgPrintValue(str, key.GetCells()[off], subset.Scheme->Cols[off].TypeInfo);
                Cerr << (off ? ", " : "") << str;
            }
            Cerr << ") ";

            Cerr << "value = " << bucket.Value << " (actual " << actualValue << " - ";
            i64 bucketError = static_cast<i64>(bucket.Value) - static_cast<i64>(actualValue);
            Cerr << FormatPercent(bucketError, total) << " error)" << Endl;
            if (verifyPercents) VerifyPercent(bucketError, total, 10);

            UNIT_ASSERT_GT(bucket.Value, prevValue);
            prevValue = bucket.Value;
            prevActualValue = actualValue;
        }

        {
            ui64 delta = total - prevValue, actualDelta = total - prevActualValue;
            Cerr << "    " << FormatPercent(delta, total) << " (actual " << FormatPercent(actualDelta, total) << ")" << Endl;
            if (verifyPercents) VerifyPercent(delta, total, 20);
            UNIT_ASSERT_GE(total, prevValue);
        }
    }

    void Check(const TSubset& subset, TMode mode, ui32 histogramBucketsCount = 10, bool verifyPercents = true, bool faulty = true) {
        if (mode == 0) {
            Dump(subset);
        }

        Cerr << "Checking " << (mode == FlatIndex ? "Flat" : (mode == MixedIndex ? "Mixed" : "BTree")) << ":" << Endl;

        ui64 totalRows = 0, totalBytes = 0;
        {
            TVector<TCell> emptyKey;
            CalcDataBefore(subset, TSerializedCellVec(emptyKey), totalBytes, totalRows);
        }

        ui64 rowCountResolution = totalRows / histogramBucketsCount;
        ui64 dataSizeResolution = totalBytes / histogramBucketsCount;

        TTouchEnv env;
        env.Faulty = faulty;
        // env.Faulty = false; // uncomment for debug
        TStats stats;
        auto buildStats = [&]() {
            if (mode == BTreeIndex) {
                return NTable::BuildStatsBTreeIndex(subset, stats, histogramBucketsCount, &env, [](){});
            } else {
                return NTable::BuildStatsMixedIndex(subset, stats, rowCountResolution, dataSizeResolution, &env, [](){});
            }
        };

        const ui32 attempts = 100;
        for (ui32 attempt : xrange(attempts)) {
            if (buildStats()) {
                break;
            }
            UNIT_ASSERT_C(attempt + 1 < attempts, "Too many attempts");
        }

        Cerr << " Touched " << FormatPercent(env.TouchedIndexBytes, stats.IndexSize.Size) << " bytes, " << env.TouchedIndexPages << " pages" << Endl;

        CheckHistogram(subset, stats.RowCountHistogram, false, totalRows, verifyPercents);
        CheckHistogram(subset, stats.DataSizeHistogram, true, totalBytes, verifyPercents);

        if (mode == BTreeIndex && verifyPercents && histogramBucketsCount != 1000) {
            UNIT_ASSERT_VALUES_EQUAL(stats.RowCountHistogram.size(), histogramBucketsCount - 1);
            UNIT_ASSERT_VALUES_EQUAL(stats.DataSizeHistogram.size(), histogramBucketsCount - 1);
        }
    }

    Y_UNIT_TEST(Single)
    {
        for (auto mode : {BTreeIndex, FlatIndex, MixedIndex}) {
            auto subset = TMake(Mass2, PageConf(Mass2.Model->Scheme->Families.size(), mode)).Mixed(0, 1, TMixerOne{ });   
            Check(*subset, mode);
        }
    }

    Y_UNIT_TEST(Single_Slices)
    {
        for (auto mode : {BTreeIndex, FlatIndex, MixedIndex}) {
            auto subset = TMake(Mass2, PageConf(Mass2.Model->Scheme->Families.size(), mode)).Mixed(0, 1, TMixerOne{ }, 0, 13);   
            subset->Flatten.begin()->Slices->Describe(Cerr); Cerr << Endl;
            Check(*subset, mode);
        }
    }

    Y_UNIT_TEST(Single_History)
    {
        for (auto mode : {BTreeIndex, FlatIndex, MixedIndex}) {
            auto subset = TMake(Mass2, PageConf(Mass2.Model->Scheme->Families.size(), mode)).Mixed(0, 1, TMixerOne{ }, 0.3);
            Check(*subset, mode);
        }
    }

    Y_UNIT_TEST(Single_History_Slices)
    {
        for (auto mode : {BTreeIndex, FlatIndex, MixedIndex}) {
            auto subset = TMake(Mass2, PageConf(Mass2.Model->Scheme->Families.size(), mode)).Mixed(0, 1, TMixerOne{ }, 0.3, 13);   
            subset->Flatten.begin()->Slices->Describe(Cerr); Cerr << Endl;
            Check(*subset, mode);
        }
    }

    Y_UNIT_TEST(Ten_Mixed)
    {
        for (auto mode : {BTreeIndex, FlatIndex, MixedIndex}) {
            auto subset = TMake(Mass2, PageConf(Mass2.Model->Scheme->Families.size(), mode)).Mixed(0, 10, TMixerRnd(10));
            Check(*subset, mode);
        }
    }

    Y_UNIT_TEST(Ten_Serial)
    {
        for (auto mode : {BTreeIndex, FlatIndex, MixedIndex}) {
            auto subset = TMake(Mass2, PageConf(Mass2.Model->Scheme->Families.size(), mode)).Mixed(0, 10, TMixerSeq(10, Mass2.Saved.Size()));
            Check(*subset, mode);
        }
    }

    Y_UNIT_TEST(Ten_Crossed)
    {
        struct TMixer {
            TMixer(ui32 buckets, ui64 rows)
                : Buckets(buckets)
                , RowsPerBucket(rows / buckets)
            {
            }

            ui32 operator()(const TRow&) noexcept
            {
                while (CurrentSize >= RowsPerBucket) {
                    CurrentSize = NextSize;
                    NextSize = 0;
                    CurrentBucket++;
                }

                if (CurrentSize > RowsPerBucket / 2 && Random.Uniform(2)) {
                    NextSize++;
                    return Min(CurrentBucket + 1, Buckets - 1);
                } else {
                    CurrentSize++;
                    return Min(CurrentBucket, Buckets - 1);
                }
            }

        private:
            const ui32 Buckets;
            ui64 RowsPerBucket;
            ui32 CurrentBucket = 0;
            ui64 CurrentSize = 0, NextSize = 0;
            TMersenne<ui64> Random;
        };

        for (auto mode : {BTreeIndex, FlatIndex, MixedIndex}) {
            auto subset = TMake(Mass2, PageConf(Mass2.Model->Scheme->Families.size(), mode)).Mixed(0, 10, TMixer(10, Mass2.Saved.Size()));
            Check(*subset, mode);
        }
    }

    Y_UNIT_TEST(Ten_Mixed_Log)
    {
        struct TMixer {
            TMixer(ui32 buckets) 
                : Buckets(buckets) 
            {
            }

            ui32 operator()(const TRow&) noexcept
            {
                auto x = Random.Uniform(1 << Buckets);
                return Min(ui32(log2(x)), Buckets - 1);
            }

        private:
            const ui32 Buckets = 1;
            TMersenne<ui64> Random;
        };

        for (auto mode : {BTreeIndex, FlatIndex, MixedIndex}) {
            auto subset = TMake(Mass2, PageConf(Mass2.Model->Scheme->Families.size(), mode)).Mixed(0, 10, TMixer(10));
            Check(*subset, mode);
        }
    }

    Y_UNIT_TEST(Ten_Serial_Log)
    {
        struct TMixer {
            TMixer(ui32 buckets, ui64 rows)
                : Buckets(buckets)
                , RowsPerBucket(rows / 2)
            {
            }

            ui32 operator()(const TRow&) noexcept
            {
                while (CurrentSize >= RowsPerBucket && RowsPerBucket) {
                    CurrentSize = 0;
                    CurrentBucket++;
                    RowsPerBucket /= 2;
                }

                CurrentSize++;

                return Min(CurrentBucket, Buckets - 1);
            }

        private:
            const ui32 Buckets;
            ui64 RowsPerBucket;
            ui32 CurrentBucket = 0;
            ui64 CurrentSize = 0;
            TMersenne<ui64> Random;
        };

        for (auto mode : {BTreeIndex, FlatIndex, MixedIndex}) {
            auto subset = TMake(Mass2, PageConf(Mass2.Model->Scheme->Families.size(), mode)).Mixed(0, 10, TMixer(10, Mass2.Saved.Size()));
            Check(*subset, mode);
        }
    }

    Y_UNIT_TEST(Ten_Crossed_Log)
    {
        struct TMixer {
            TMixer(ui32 buckets, ui64 rows)
                : Buckets(buckets)
                , RowsPerBucket(rows / 2)
            {
            }

            ui32 operator()(const TRow&) noexcept
            {
                while (CurrentSize >= RowsPerBucket && RowsPerBucket) {
                    CurrentSize = NextSize;
                    NextSize = 0;
                    CurrentBucket++;
                    RowsPerBucket /= 2;
                }

                if (CurrentSize > RowsPerBucket / 2 && Random.Uniform(3) == 0) {
                    NextSize++;
                    return Min(CurrentBucket + 1, Buckets - 1);
                } else {
                    CurrentSize++;
                    return Min(CurrentBucket, Buckets - 1);
                }
            }

        private:
            const ui32 Buckets;
            ui64 RowsPerBucket;
            ui32 CurrentBucket = 0;
            ui64 CurrentSize = 0, NextSize = 0;
            TMersenne<ui64> Random;
        };

        for (auto mode : {BTreeIndex, FlatIndex, MixedIndex}) {
            auto subset = TMake(Mass2, PageConf(Mass2.Model->Scheme->Families.size(), mode)).Mixed(0, 10, TMixer(10, Mass2.Saved.Size()));
            Check(*subset, mode);
        }
    }

    Y_UNIT_TEST(Five_Five_Mixed)
    {
        struct TMixer {
            TMixer(ui32 buckets) 
                : Buckets(buckets) 
            {
            }

            ui32 operator()(const TRow&) noexcept
            {
                if (Random.Uniform(20) == 0) {
                    return Random.Uniform(Buckets / 2);
                } else {
                    return Buckets / 2 + Random.Uniform(Buckets / 2);
                }
            }

        private:
            const ui32 Buckets = 1;
            TMersenne<ui64> Random;
        };

        for (auto mode : {BTreeIndex, FlatIndex, MixedIndex}) {
            auto subset = TMake(Mass2, PageConf(Mass2.Model->Scheme->Families.size(), mode)).Mixed(0, 10, TMixer(10));
            Check(*subset, mode);
        }
    }

    Y_UNIT_TEST(Five_Five_Serial)
    {
        struct TMixer {
            TMixer(ui32 buckets, ui64 rows)
                : Buckets(buckets)
                , RowsPerBigBucket(rows / Buckets / 10 * 19)
                , RowsPerSmallBucket(rows / Buckets / 10 * 1)
                , RowsPerBucket(RowsPerBigBucket)
            {
            }

            ui32 operator()(const TRow&) noexcept
            {
                while (CurrentSize >= RowsPerBucket) {
                    CurrentSize = 0;
                    CurrentBucket++;
                    RowsPerBucket = RowsPerBigBucket + RowsPerSmallBucket - RowsPerBucket;
                }

                CurrentSize++;

                return Min(CurrentBucket, Buckets - 1);
            }

        private:
            const ui32 Buckets;
            ui64 RowsPerBigBucket, RowsPerSmallBucket, RowsPerBucket;
            ui32 CurrentBucket = 0;
            ui64 CurrentSize = 0;
            TMersenne<ui64> Random;
        };

        for (auto mode : {BTreeIndex, FlatIndex, MixedIndex}) {
            TMixer mixer(10, Mass2.Saved.Size());
            auto subset = TMake(Mass2, PageConf(Mass2.Model->Scheme->Families.size(), mode)).Mixed(0, 10, mixer);
            Check(*subset, mode);
        }
    }

    Y_UNIT_TEST(Five_Five_Crossed)
    {
        struct TMixer {
            TMixer(ui32 buckets, ui64 rows)
                : Buckets(buckets)
                , RowsPerBigBucket(rows / Buckets / 10 * 19)
                , RowsPerSmallBucket(rows / Buckets / 10 * 1)
                , RowsPerBucket(RowsPerBigBucket)
            {
            }

            ui32 operator()(const TRow&) noexcept
            {
                while (CurrentSize >= RowsPerBucket && RowsPerBucket) {
                    CurrentSize = NextSize;
                    NextSize = 0;
                    CurrentBucket++;
                    RowsPerBucket = RowsPerBigBucket + RowsPerSmallBucket - RowsPerBucket;
                }

                if (CurrentSize > RowsPerBucket / 2 && (RowsPerBucket == RowsPerBigBucket) == (Random.Uniform(20) == 0)) {
                    NextSize++;
                    return Min(CurrentBucket + 1, Buckets - 1);
                } else {
                    CurrentSize++;
                    return Min(CurrentBucket, Buckets - 1);
                }
            }

        private:
            const ui32 Buckets;
            ui64 RowsPerBigBucket, RowsPerSmallBucket, RowsPerBucket;
            ui32 CurrentBucket = 0;
            ui64 CurrentSize = 0, NextSize = 0;
            TMersenne<ui64> Random;
        };

        for (auto mode : {BTreeIndex, FlatIndex, MixedIndex}) {
            TMixer mixer(10, Mass2.Saved.Size());
            auto subset = TMake(Mass2, PageConf(Mass2.Model->Scheme->Families.size(), mode)).Mixed(0, 10, mixer);
            Check(*subset, mode);
        }
    }

    Y_UNIT_TEST(Single_Small_2_Levels)
    {
        for (auto mode : {BTreeIndex, FlatIndex, MixedIndex}) {
            auto subset = TMake(Mass3, PageConf(Mass3.Model->Scheme->Families.size(), mode)).Mixed(0, 1, TMixerOne{ });   
            Check(*subset, mode, 1000);
        }
    }

    Y_UNIT_TEST(Single_Small_2_Levels_3_Buckets)
    {
        for (auto mode : {BTreeIndex, FlatIndex, MixedIndex}) {
            auto subset = TMake(Mass3, PageConf(Mass3.Model->Scheme->Families.size(), mode)).Mixed(0, 1, TMixerOne{ });   
            Check(*subset, mode, 5, false);
        }
    }

    Y_UNIT_TEST(Single_Small_1_Level)
    {
        for (auto mode : {BTreeIndex, FlatIndex, MixedIndex}) {
            auto conf = PageConf(Mass3.Model->Scheme->Families.size(), mode);
            for (auto& group : conf.Groups) {
                group.BTreeIndexNodeKeysMin = group.BTreeIndexNodeKeysMax = Max<ui32>();
            }
            auto subset = TMake(Mass3, conf).Mixed(0, 1, TMixerOne{ });   
            Check(*subset, mode, 1000);
        }
    }

    Y_UNIT_TEST(Single_Small_0_Levels)
    {
        for (auto mode : {BTreeIndex, FlatIndex, MixedIndex}) {
            auto conf = PageConf(Mass3.Model->Scheme->Families.size(), mode);
            for (auto& group : conf.Groups) {
                group.PageSize = group.PageRows = Max<ui32>();
            }
            auto subset = TMake(Mass3, conf).Mixed(0, 1, TMixerOne{ });   
            Check(*subset, mode, 1000, false);
        }
    }

    Y_UNIT_TEST(Three_Mixed_Small_2_Levels)
    {
        for (auto mode : {BTreeIndex, FlatIndex, MixedIndex}) {
            auto subset = TMake(Mass3, PageConf(Mass3.Model->Scheme->Families.size(), mode)).Mixed(0, 3, TMixerRnd(3));   
            Check(*subset, mode, 1000, false);
        }
    }

    Y_UNIT_TEST(Three_Mixed_Small_2_Levels_3_Buckets)
    {
        for (auto mode : {BTreeIndex, FlatIndex, MixedIndex}) {
            auto subset = TMake(Mass3, PageConf(Mass3.Model->Scheme->Families.size(), mode)).Mixed(0, 3, TMixerRnd(3));   
            Check(*subset, mode, 5, false);
        }
    }

    Y_UNIT_TEST(Three_Mixed_Small_1_Level)
    {
        for (auto mode : {BTreeIndex, FlatIndex, MixedIndex}) {
            auto conf = PageConf(Mass3.Model->Scheme->Families.size(), mode);
            for (auto& group : conf.Groups) {
                group.BTreeIndexNodeKeysMin = group.BTreeIndexNodeKeysMax = Max<ui32>();
            }
            auto subset = TMake(Mass3, conf).Mixed(0, 3, TMixerRnd(3));
            Check(*subset, mode, 1000, false);
        }
    }

    Y_UNIT_TEST(Three_Mixed_Small_0_Levels)
    {
        for (auto mode : {BTreeIndex, FlatIndex, MixedIndex}) {
            auto conf = PageConf(Mass3.Model->Scheme->Families.size(), mode);
            for (auto& group : conf.Groups) {
                group.PageSize = group.PageRows = Max<ui32>();
            }
            auto subset = TMake(Mass3, conf).Mixed(0, 3, TMixerRnd(3));
            Check(*subset, mode, 1000, false);
        }
    }

    Y_UNIT_TEST(Three_Serial_Small_2_Levels)
    {
        for (auto mode : {BTreeIndex, FlatIndex, MixedIndex}) {
            auto subset = TMake(Mass3, PageConf(Mass3.Model->Scheme->Families.size(), mode)).Mixed(0, 3, TMixerSeq(3, Mass3.Saved.Size()));   
            Check(*subset, mode, 1000, false);
        }
    }

    Y_UNIT_TEST(Three_Serial_Small_2_Levels_3_Buckets)
    {
        for (auto mode : {BTreeIndex, FlatIndex, MixedIndex}) {
            auto subset = TMake(Mass3, PageConf(Mass3.Model->Scheme->Families.size(), mode)).Mixed(0, 3, TMixerSeq(3, Mass3.Saved.Size()));   
            Check(*subset, mode, 5, false);
        }
    }

    Y_UNIT_TEST(Three_Serial_Small_1_Level)
    {
        for (auto mode : {BTreeIndex, FlatIndex, MixedIndex}) {
            auto conf = PageConf(Mass3.Model->Scheme->Families.size(), mode);
            for (auto& group : conf.Groups) {
                group.BTreeIndexNodeKeysMin = group.BTreeIndexNodeKeysMax = Max<ui32>();
            }
            auto subset = TMake(Mass3, conf).Mixed(0, 3, TMixerSeq(3, Mass3.Saved.Size()));
            Check(*subset, mode, 1000, false);
        }
    }

    Y_UNIT_TEST(Three_Serial_Small_0_Levels)
    {
        for (auto mode : {BTreeIndex, FlatIndex, MixedIndex}) {
            auto conf = PageConf(Mass3.Model->Scheme->Families.size(), mode);
            for (auto& group : conf.Groups) {
                group.PageSize = group.PageRows = Max<ui32>();
            }
            auto subset = TMake(Mass3, conf).Mixed(0, 3, TMixerSeq(3, Mass3.Saved.Size()));
            Check(*subset, mode, 1000, false);
        }
    }

    Y_UNIT_TEST(Mixed_Groups_History)
    {
        for (auto mode : {BTreeIndex, FlatIndex, MixedIndex}) {
            auto subset = TMake(Mass1, PageConf(Mass1.Model->Scheme->Families.size(), mode)).Mixed(0, 4, TMixerRnd(4), 0.3);
            Check(*subset, mode);
        }
    }

    Y_UNIT_TEST(Serial_Groups_History)
    {
        for (auto mode : {BTreeIndex, FlatIndex, MixedIndex}) {
            TMixerSeq mixer(4, Mass1.Saved.Size());
            auto subset = TMake(Mass1, PageConf(Mass1.Model->Scheme->Families.size(), mode)).Mixed(0, 4, mixer, 0.3);
            Check(*subset, mode);
        }
    }

    // this test uses same data as benchmark::TPartFixture/BuildStats/*, but may be debugged and prints result histograms
    Y_UNIT_TEST(Benchmark)
    {
        const ui32 partsCount = 4;
        const bool groups = false;
        const bool history = false;
        ui64 rowsCount = history ? 300000 : 1000000;

        rowsCount /= 100; // to be faster

        TAutoPtr<TMass> mass = new NTest::TMass(new NTest::TModelStd(groups), rowsCount);

        for (auto mode : {BTreeIndex, FlatIndex, MixedIndex}) {
            NPage::TConf conf;
            conf.Groups.resize(mass->Model->Scheme->Families.size());
            conf.WriteBTreeIndex = (mode == FlatIndex ? false : true);

            TAutoPtr<TSubset> subset = TMake(*mass, conf).Mixed(0, partsCount, TMixerRnd(partsCount), history ? 0.7 : 0);
            
            Check(*subset, mode, 10, false);
        }
    }

    Y_UNIT_TEST(Many_Mixed)
    {
        const ui32 partsCount = 1000;
        const ui64 rowsCount = 100000;

        TAutoPtr<TMass> mass = new NTest::TMass(new NTest::TModelStd(false), rowsCount);

        for (auto mode : {BTreeIndex, FlatIndex, MixedIndex}) {
            NPage::TConf conf;
            conf.Groups.resize(mass->Model->Scheme->Families.size());
            conf.Group(0).PageRows = 1; // we don't care about pages actual size
            conf.Group(0).BTreeIndexNodeKeysMin = conf.Group(0).BTreeIndexNodeKeysMax = 2;
            conf.WriteBTreeIndex = (mode == FlatIndex ? false : true);

            TAutoPtr<TSubset> subset = TMake(*mass, conf).Mixed(0, partsCount, TMixerRnd(partsCount));
            
            Check(*subset, mode, 10, false, false);
        }
    }

    Y_UNIT_TEST(Many_Serial)
    {
        const ui32 partsCount = 1000;
        const ui64 rowsCount = 100000;

        TAutoPtr<TMass> mass = new NTest::TMass(new NTest::TModelStd(false), rowsCount);

        for (auto mode : {BTreeIndex, FlatIndex, MixedIndex}) {
            NPage::TConf conf;
            conf.Groups.resize(mass->Model->Scheme->Families.size());
            conf.Group(0).PageRows = 1; // we don't care about pages actual size
            conf.Group(0).BTreeIndexNodeKeysMin = conf.Group(0).BTreeIndexNodeKeysMax = 2;
            conf.WriteBTreeIndex = (mode == FlatIndex ? false : true);

            TAutoPtr<TSubset> subset = TMake(*mass, conf).Mixed(0, partsCount, TMixerSeq(partsCount, mass->Saved.Size()));
            
            Check(*subset, mode, 10, false, false);
        }
    }
}

}
