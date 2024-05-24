#include "flat_part_charge_range.h"
#include "flat_stat_table.h"
#include "flat_stat_table_mixed_index.h"
#include "flat_stat_table_btree_index.h"
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
                if (part->GetPageType(pageId, groupId) == EPage::DataPage) {
                    auto dataPage = NPage::TDataPage(page);
                    
                    TouchedBytes += page->size();
                    TouchedRows += dataPage->Count;
                }
            }

            return newTouch && Faulty
                ? nullptr
                : page;
        }

        TMap<std::pair<const TPart*, TGroupId>, TSet<TPageId>> Touched;
        bool Faulty = true;
        ui64 TouchedBytes = 0, TouchedRows = 0;
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

    void CheckBTreeIndex(const TSubset& subset, ui64 expectedRows, ui64 expectedData, ui64 expectedIndex, ui64 rowCountResolution = 531, ui64 dataSizeResolution = 53105) {
        TStats stats;
        TTouchEnv env;

        const ui32 attempts = 10;
        for (ui32 attempt : xrange(attempts)) {
            if (NTable::BuildStatsBTreeIndex(subset, stats, rowCountResolution, dataSizeResolution, &env)) {
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
        CheckMixedIndex(*subset, 24000, 3547100, 31242);
    }

    Y_UNIT_TEST(Single_History_Slices)
    {
        auto subset = TMake(Mass0, PageConf(Mass0.Model->Scheme->Families.size(), WriteBTreeIndex)).Mixed(0, 1, TMixerOne{ }, 0.3, 13);   
        subset->Flatten.begin()->Slices->Describe(Cerr); Cerr << Endl;
        CheckMixedIndex(*subset, 9582, 1425198, 31242);
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
        CheckMixedIndex(*subset, 24000, 4054050, 18810);
    }

    Y_UNIT_TEST(Single_Groups_History_Slices)
    {
        auto subset = TMake(Mass1, PageConf(Mass1.Model->Scheme->Families.size(), WriteBTreeIndex)).Mixed(0, 1, TMixerOne{ }, 0.3, 13);   
        subset->Flatten.begin()->Slices->Describe(Cerr); Cerr << Endl;
        CheckMixedIndex(*subset, 13570, 2277890, 18810);
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
        CheckMixedIndex(*subset, 24000, 4054270, 19152);
    }

    Y_UNIT_TEST(Serial)
    {
        TMixerSeq mixer(4, Mass0.Saved.Size());
        auto subset = TMake(Mass0, PageConf(Mass0.Model->Scheme->Families.size(), WriteBTreeIndex)).Mixed(0, 4, mixer);
        CheckMixedIndex(*subset, 24000, 2106459, 25428);
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
        CheckMixedIndex(*subset, 24000, 4054290, 19168);
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
        CheckMixedIndex(*subset, 24000, 3547100, 61162);
    }

    Y_UNIT_TEST(Single_History_Slices)
    {
        auto subset = TMake(Mass0, PageConf(Mass0.Model->Scheme->Families.size(), WriteBTreeIndex)).Mixed(0, 1, TMixerOne{ }, 0.3, 13);   
        subset->Flatten.begin()->Slices->Describe(Cerr); Cerr << Endl;
        CheckMixedIndex(*subset, 9582, 1425198, 61162);
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
        CheckMixedIndex(*subset, 24000, 4054050, 34837);
    }

    Y_UNIT_TEST(Single_Groups_History_Slices)
    {
        auto subset = TMake(Mass1, PageConf(Mass1.Model->Scheme->Families.size(), WriteBTreeIndex)).Mixed(0, 1, TMixerOne{ }, 0.3, 13);   
        subset->Flatten.begin()->Slices->Describe(Cerr); Cerr << Endl;
        CheckMixedIndex(*subset, 13570, 2277890, 34837);
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
        CheckMixedIndex(*subset, 24000, 4054270, 34579);
    }

    Y_UNIT_TEST(Serial)
    {
        TMixerSeq mixer(4, Mass0.Saved.Size());
        auto subset = TMake(Mass0, PageConf(Mass0.Model->Scheme->Families.size(), WriteBTreeIndex)).Mixed(0, 4, mixer);
        CheckMixedIndex(*subset, 24000, 2106459, 49502);
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
        CheckMixedIndex(*subset, 24000, 4054290, 34652);
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
        CheckMixedIndex(*subset, 24000, 4054050, 48540, 5310, 531050);
    }

    Y_UNIT_TEST(Single_Groups_History_Slices_LowResolution)
    {
        auto subset = TMake(Mass1, PageConf(Mass1.Model->Scheme->Families.size(), true, WriteBTreeIndex)).Mixed(0, 1, TMixerOne{ }, 0.3, 13);   
        subset->Flatten.begin()->Slices->Describe(Cerr); Cerr << Endl;
        CheckMixedIndex(*subset, 13570, 2114857 /* ~2277890 */, 48540, 5310, 531050);
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
        CheckBTreeIndex(*subset, 24000, 3547100, 61162);
    }

    Y_UNIT_TEST(Single_History_Slices)
    {
        auto subset = TMake(Mass0, PageConf(Mass0.Model->Scheme->Families.size(), WriteBTreeIndex)).Mixed(0, 1, TMixerOne{ }, 0.3, 13);   
        subset->Flatten.begin()->Slices->Describe(Cerr); Cerr << Endl;
        CheckBTreeIndex(*subset, 9582, 1425282, 61162);
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
        CheckBTreeIndex(*subset, 24000, 4054050, 34837);
    }

    Y_UNIT_TEST(Single_Groups_History_Slices)
    {
        auto subset = TMake(Mass1, PageConf(Mass1.Model->Scheme->Families.size(), WriteBTreeIndex)).Mixed(0, 1, TMixerOne{ }, 0.3, 13);   
        subset->Flatten.begin()->Slices->Describe(Cerr); Cerr << Endl;
        CheckBTreeIndex(*subset, 13570, 2273213, 34837);
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
        CheckBTreeIndex(*subset, 24000, 4054270, 34579);
    }

    Y_UNIT_TEST(Serial)
    {
        TMixerSeq mixer(4, Mass0.Saved.Size());
        auto subset = TMake(Mass0, PageConf(Mass0.Model->Scheme->Families.size(), WriteBTreeIndex)).Mixed(0, 4, mixer);
        CheckBTreeIndex(*subset, 24000, 2106459, 49502);
    }

    Y_UNIT_TEST(Serial_Groups)
    {
        TMixerSeq mixer(4, Mass1.Saved.Size());
        auto subset = TMake(Mass1, PageConf(Mass1.Model->Scheme->Families.size(), WriteBTreeIndex)).Mixed(0, 4, mixer);
        CheckBTreeIndex(*subset, 24000, 2460259, 23628);
    }

    Y_UNIT_TEST(Serial_Groups_History)
    {
        TMixerSeq mixer(4, Mass1.Saved.Size());
        auto subset = TMake(Mass1, PageConf(Mass1.Model->Scheme->Families.size(), WriteBTreeIndex)).Mixed(0, 4, mixer, 0.3);
        CheckBTreeIndex(*subset, 24000, 4054290, 34652);
    }
}

Y_UNIT_TEST_SUITE(BuildStatsHistogram) {
    using namespace NTest;

    enum TMode {
        FlatIndex,
        MixedIndex,
        BTreeIndex
    };

    NPage::TConf PageConf(size_t groups, TMode mode) noexcept
    {
        NPage::TConf conf{ true, 2 * 1024 };

        conf.Groups.resize(groups);
        for (size_t group : xrange(groups)) {
            conf.Group(group).BTreeIndexNodeKeysMin = conf.Group(group).BTreeIndexNodeKeysMax = 2;
        }

        conf.CutIndexKeys = false;
        conf.WriteBTreeIndex = mode == FlatIndex ? false : true;

        return conf;
    }

    void PrintPercent(double value, ui64 total) {
        Cerr << static_cast<int>(100 * value / total) << "%";
    }

    void CalcDataBefore(const TSubset& subset, TSerializedCellVec key, ui64& bytes, ui64& rows) {
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
            Y_ABORT_UNLESS(ChargeRange(&env, {}, key.GetCells(), run, keyDefaults, tags, 0, 0));
        }

        bytes = env.TouchedBytes;
        rows = env.TouchedRows;
    }

    void CheckHistogram(const TSubset& subset, THistogram histogram, bool isBytes, ui64 total) {
        Cerr << "  " << (isBytes ? "DataSizeHistogram:" : "RowCountHistogram:") << Endl;

        ui64 prevValue = 0;

        for (const auto& bucket : histogram) {
            TSerializedCellVec key(bucket.EndKey);
            ui64 actualBytes, actualRows;
            CalcDataBefore(subset, key, actualBytes, actualRows);
            ui64 actualValue = isBytes ? actualBytes : actualRows;

            UNIT_ASSERT_GT(bucket.Value, prevValue);
            ui64 delta = bucket.Value - prevValue;
            Cerr << "    ";
            PrintPercent(delta, total);
            Cerr << Endl;

            Cerr << "    ";
            
            Cerr << "key = (";
            for (auto off : xrange(key.GetCells().size())) {
                TString str;
                DbgPrintValue(str, key.GetCells()[off], subset.Scheme->Cols[off].TypeInfo);
                Cerr << (off ? ", " : "") << str;
            }
            Cerr << ") ";

            Cerr << "value = " << bucket.Value << " (" << actualValue << " - ";
            PrintPercent(static_cast<i64>(bucket.Value) - static_cast<i64>(actualValue), total);
            Cerr << " error)" << Endl;

            prevValue = bucket.Value;
        }

        {
            Cerr << "    ";
            UNIT_ASSERT_GT(total, prevValue);
            ui64 delta = total - prevValue;
            PrintPercent(delta, total);
            Cerr << Endl;
        }
    }

    void Check(const TSubset& subset, TMode mode) {
        Cerr << "Checking " << (mode == FlatIndex ? "Flat" : (mode == MixedIndex ? "Mixed" : "BTree")) << Endl;

        TStats stats;
        TTouchEnv env;

        ui64 rowCountResolution = 1, dataSizeResolution = 1;

        auto buildStats = [&]() {
            if (mode == BTreeIndex) {
                return NTable::BuildStatsBTreeIndex(subset, stats, rowCountResolution, dataSizeResolution, &env);
            } else {
                return NTable::BuildStatsMixedIndex(subset, stats, rowCountResolution, dataSizeResolution, &env, [](){});
            }
        };

        env.Faulty = false;
        buildStats();
        ui64 totalRows = stats.RowCount, totalBytes = stats.DataSize.Size;
        env.Faulty = true;
        rowCountResolution = totalRows / 10;
        dataSizeResolution = totalBytes / 10;

        const ui32 attempts = 10;
        for (ui32 attempt : xrange(attempts)) {

            if (buildStats()) {
                break;
            }
            UNIT_ASSERT_C(attempt + 1 < attempts, "Too many attempts");
        }

        CheckHistogram(subset, stats.RowCountHistogram, false, totalRows);
        CheckHistogram(subset, stats.DataSizeHistogram, true, totalBytes);
    }

    Y_UNIT_TEST(Single)
    {
        for (auto mode : {FlatIndex, MixedIndex, BTreeIndex}) {
            auto subset = TMake(Mass0, PageConf(Mass0.Model->Scheme->Families.size(), mode)).Mixed(0, 1, TMixerOne{ });   
            Check(*subset, mode);
        }
    }

    // Y_UNIT_TEST(Single_Slices)
    // {
    //     auto subset = TMake(Mass0, PageConf(Mass0.Model->Scheme->Families.size(), WriteBTreeIndex)).Mixed(0, 1, TMixerOne{ }, 0, 13);   
    //     subset->Flatten.begin()->Slices->Describe(Cerr); Cerr << Endl;
    //     CheckMixedIndex(*subset, 12816, 1121048, 49449);
    // }

    // Y_UNIT_TEST(Single_History)
    // {
    //     auto subset = TMake(Mass0, PageConf(Mass0.Model->Scheme->Families.size(), WriteBTreeIndex)).Mixed(0, 1, TMixerOne{ }, 0.3);
    //     CheckMixedIndex(*subset, 24000, 3547100, 61162);
    // }

    // Y_UNIT_TEST(Single_History_Slices)
    // {
    //     auto subset = TMake(Mass0, PageConf(Mass0.Model->Scheme->Families.size(), WriteBTreeIndex)).Mixed(0, 1, TMixerOne{ }, 0.3, 13);   
    //     subset->Flatten.begin()->Slices->Describe(Cerr); Cerr << Endl;
    //     CheckMixedIndex(*subset, 9582, 1425198, 61162);
    // }

    // Y_UNIT_TEST(Single_Groups)
    // {
    //     auto subset = TMake(Mass1, PageConf(Mass1.Model->Scheme->Families.size(), WriteBTreeIndex)).Mixed(0, 1, TMixerOne{ });   
    //     CheckMixedIndex(*subset, 24000, 2460139, 23760);
    // }

    // Y_UNIT_TEST(Single_Groups_Slices)
    // {
    //     auto subset = TMake(Mass1, PageConf(Mass1.Model->Scheme->Families.size(), WriteBTreeIndex)).Mixed(0, 1, TMixerOne{ }, 0, 13);   
    //     subset->Flatten.begin()->Slices->Describe(Cerr); Cerr << Endl;
    //     CheckMixedIndex(*subset, 10440, 1060798, 23760);
    // }

    // Y_UNIT_TEST(Single_Groups_History)
    // {
    //     auto subset = TMake(Mass1, PageConf(Mass1.Model->Scheme->Families.size(), WriteBTreeIndex)).Mixed(0, 1, TMixerOne{ }, 0.3);   
    //     CheckMixedIndex(*subset, 24000, 4054050, 34837);
    // }

    // Y_UNIT_TEST(Single_Groups_History_Slices)
    // {
    //     auto subset = TMake(Mass1, PageConf(Mass1.Model->Scheme->Families.size(), WriteBTreeIndex)).Mixed(0, 1, TMixerOne{ }, 0.3, 13);   
    //     subset->Flatten.begin()->Slices->Describe(Cerr); Cerr << Endl;
    //     CheckMixedIndex(*subset, 13570, 2277890, 34837);
    // }

    // Y_UNIT_TEST(Mixed)
    // {
    //     auto subset = TMake(Mass0, PageConf(Mass0.Model->Scheme->Families.size(), WriteBTreeIndex)).Mixed(0, 4, TMixerRnd(4));
    //     CheckMixedIndex(*subset, 24000, 2106459, 49449);
    // }

    // Y_UNIT_TEST(Mixed_Groups)
    // {
    //     auto subset = TMake(Mass1, PageConf(Mass1.Model->Scheme->Families.size(), WriteBTreeIndex)).Mixed(0, 4, TMixerRnd(4));
    //     CheckMixedIndex(*subset, 24000, 2460219, 23555);
    // }

    // Y_UNIT_TEST(Mixed_Groups_History)
    // {
    //     auto subset = TMake(Mass1, PageConf(Mass1.Model->Scheme->Families.size(), WriteBTreeIndex)).Mixed(0, 4, TMixerRnd(4), 0.3);
    //     CheckMixedIndex(*subset, 24000, 4054270, 34579);
    // }

    // Y_UNIT_TEST(Serial)
    // {
    //     TMixerSeq mixer(4, Mass0.Saved.Size());
    //     auto subset = TMake(Mass0, PageConf(Mass0.Model->Scheme->Families.size(), WriteBTreeIndex)).Mixed(0, 4, mixer);
    //     CheckMixedIndex(*subset, 24000, 2106459, 49502);
    // }

    // Y_UNIT_TEST(Serial_Groups)
    // {
    //     TMixerSeq mixer(4, Mass1.Saved.Size());
    //     auto subset = TMake(Mass1, PageConf(Mass1.Model->Scheme->Families.size(), WriteBTreeIndex)).Mixed(0, 4, mixer);
    //     CheckMixedIndex(*subset, 24000, 2460259, 23628);
    // }

    // Y_UNIT_TEST(Serial_Groups_History)
    // {
    //     TMixerSeq mixer(4, Mass1.Saved.Size());
    //     auto subset = TMake(Mass1, PageConf(Mass1.Model->Scheme->Families.size(), WriteBTreeIndex)).Mixed(0, 4, mixer, 0.3);
    //     CheckMixedIndex(*subset, 24000, 4054290, 34652);
    // }

    // Y_UNIT_TEST(Single_LowResolution)
    // {
    //     auto subset = TMake(Mass0, PageConf(Mass0.Model->Scheme->Families.size(), true, WriteBTreeIndex)).Mixed(0, 1, TMixerOne{ });   
    //     CheckMixedIndex(*subset, 24000, 2106439, 66674, 5310, 531050);
    // }

    // Y_UNIT_TEST(Single_Slices_LowResolution)
    // {
    //     auto subset = TMake(Mass0, PageConf(Mass0.Model->Scheme->Families.size(), true, WriteBTreeIndex)).Mixed(0, 1, TMixerOne{ }, 0, 13);   
    //     subset->Flatten.begin()->Slices->Describe(Cerr); Cerr << Endl;
    //     CheckMixedIndex(*subset, 12816, 1121048, 66674, 5310, 531050);
    // }

    // Y_UNIT_TEST(Single_Groups_LowResolution)
    // {
    //     auto subset = TMake(Mass1, PageConf(Mass1.Model->Scheme->Families.size(), true, WriteBTreeIndex)).Mixed(0, 1, TMixerOne{ });   
    //     CheckMixedIndex(*subset, 24000, 2460139, 33541, 5310, 531050);
    // }

    // Y_UNIT_TEST(Single_Groups_Slices_LowResolution)
    // {
    //     auto subset = TMake(Mass1, PageConf(Mass1.Model->Scheme->Families.size(), true, WriteBTreeIndex)).Mixed(0, 1, TMixerOne{ }, 0, 13);   
    //     subset->Flatten.begin()->Slices->Describe(Cerr); Cerr << Endl;
    //     CheckMixedIndex(*subset, 10440, 1060798, 33541, 5310, 531050);
    // }

    // Y_UNIT_TEST(Single_Groups_History_LowResolution)
    // {
    //     auto subset = TMake(Mass1, PageConf(Mass1.Model->Scheme->Families.size(), true, WriteBTreeIndex)).Mixed(0, 1, TMixerOne{ }, 0.3);   
    //     CheckMixedIndex(*subset, 24000, 4054050, 48540, 5310, 531050);
    // }

    // Y_UNIT_TEST(Single_Groups_History_Slices_LowResolution)
    // {
    //     auto subset = TMake(Mass1, PageConf(Mass1.Model->Scheme->Families.size(), true, WriteBTreeIndex)).Mixed(0, 1, TMixerOne{ }, 0.3, 13);   
    //     subset->Flatten.begin()->Slices->Describe(Cerr); Cerr << Endl;
    //     CheckMixedIndex(*subset, 13570, 2114857 /* ~2277890 */, 48540, 5310, 531050);
    // }
}

}
