#include "flat_stat_table.h"
#include <ydb/core/tablet_flat/test/libs/table/model/large.h>
#include <ydb/core/tablet_flat/test/libs/table/test_make.h>
#include <ydb/core/tablet_flat/test/libs/table/test_mixer.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NTable {

namespace {
    using namespace NTest;

    struct TTouchEnv : public NTest::TTestEnv {
        const TSharedData* TryGetPage(const TPart *part, TPageId id, TGroupId groupId) override
        {
            UNIT_ASSERT_C(part->GetPageType(id) == EPage::Index || part->GetPageType(id) == EPage::BTreeIndex, "Shouldn't request non-index pages");
            if (!Touched[groupId].insert(id).second) {
                return NTest::TTestEnv::TryGetPage(part, id, groupId);
            }
            return nullptr;
        }

        TMap<TGroupId, TSet<TPageId>> Touched;
    };

    NPage::TConf PageConf(size_t groups, bool writeBTreeIndex) noexcept
    {
        NPage::TConf conf{ true, 2 * 1024 };

        conf.Groups.resize(groups);
        for (size_t group : xrange(groups)) {
            conf.Group(group).IndexMin = 1024; /* Should cover index buffer grow code */
            conf.Group(group).BTreeIndexNodeTargetSize = 512; /* Should cover up/down moves */
        }
        conf.SmallEdge = 19;  /* Packed to page collection large cell values */
        conf.LargeEdge = 29;  /* Large values placed to single blobs */
        conf.SliceSize = conf.Group(0).PageSize * 4;
        conf.CutIndexKeys = false;
        conf.WriteBTreeIndex = writeBTreeIndex;

        return conf;
    }

    const NTest::TMass Mass0(new NTest::TModelStd(false), 24000);
    const NTest::TMass Mass1(new NTest::TModelStd(true), 24000);

    void Dump(const TSubset& subset, THistogram histogram) {
        for (const auto& bucket : histogram) {
            Cerr << bucket.Value << " (";
            TSerializedCellVec key(bucket.EndKey);
            for (auto off : xrange(key.GetCells().size())) {
                TString str;
                DbgPrintValue(str, key.GetCells()[off], subset.Scheme->Cols[off].TypeInfo);
                Cerr << (off ? ", " : "") << str;
            }
            Cerr << ")" << Endl;
        }
    }

    template<typename TEnv>
    void Check(const TSubset& subset, ui64 expectedRows, ui64 expectedData, ui64 expectedIndex) {
        TStats stats;
        TEnv env;

        const ui32 attempts = 10;
        for (ui32 attempt : xrange(attempts)) {
            if (NTable::BuildStats(subset, stats, 531, 53105, &env)) {
                break;
            }
            UNIT_ASSERT_C(attempt + 1 < attempts, "Too many attempts");
        }

        Cerr << "Stats: " << stats.RowCount << " " << stats.DataSize.Size << " " << stats.IndexSize.Size << " " << stats.DataSizeHistogram.size() << " " << stats.RowCountHistogram.size() << Endl;
        UNIT_ASSERT_VALUES_EQUAL(stats.RowCount, expectedRows);
        UNIT_ASSERT_VALUES_EQUAL(stats.DataSize.Size, expectedData);
        UNIT_ASSERT_VALUES_EQUAL(stats.IndexSize.Size, expectedIndex);

        Cerr << "RowCountHistogram:" << Endl;
        Dump(subset, stats.RowCountHistogram);
        Cerr << "DataSizeHistogram:" << Endl;
        Dump(subset, stats.DataSizeHistogram);
    }
    
    
    void Check(const TSubset& subset, ui64 expectedRows, ui64 expectedData, ui64 expectedIndex) {
        Check<TTestEnv>(subset, expectedRows, expectedData, expectedIndex);
        Check<TTouchEnv>(subset, expectedRows, expectedData, expectedIndex);
    }
}

Y_UNIT_TEST_SUITE(BuildStats) {
    using namespace NTest;

    Y_UNIT_TEST(Single)
    {
        auto subset = TMake(Mass0, PageConf(Mass0.Model->Scheme->Families.size(), false)).Mixed(0, 1, TMixerOne{ });   
        Check(*subset, 24000, 2106439, 25272);
    }

    Y_UNIT_TEST(Single_Slices)
    {
        auto subset = TMake(Mass0, PageConf(Mass0.Model->Scheme->Families.size(), false)).Mixed(0, 1, TMixerOne{ }, 0, 13);   
        subset->Flatten.begin()->Slices->Describe(Cerr); Cerr << Endl;
        Check(*subset, 12816, 1121048, 25272);
    }

    Y_UNIT_TEST(Single_Groups)
    {
        auto subset = TMake(Mass1, PageConf(Mass1.Model->Scheme->Families.size(), false)).Mixed(0, 1, TMixerOne{ });   
        Check(*subset, 24000, 2460139, 13170);
    }

    Y_UNIT_TEST(Single_Groups_Slices)
    {
        auto subset = TMake(Mass1, PageConf(Mass1.Model->Scheme->Families.size(), false)).Mixed(0, 1, TMixerOne{ }, 0, 13);   
        subset->Flatten.begin()->Slices->Describe(Cerr); Cerr << Endl;
        Check(*subset, 10440, 1060798, 13170);
    }

    Y_UNIT_TEST(Single_Groups_History)
    {
        auto subset = TMake(Mass1, PageConf(Mass1.Model->Scheme->Families.size(), false)).Mixed(0, 1, TMixerOne{ }, 0.3);   
        Check(*subset, 24000, 4054050, 18810);
    }

    Y_UNIT_TEST(Single_Groups_History_Slices)
    {
        auto subset = TMake(Mass1, PageConf(Mass1.Model->Scheme->Families.size(), false)).Mixed(0, 1, TMixerOne{ }, 0.3, 13);   
        subset->Flatten.begin()->Slices->Describe(Cerr); Cerr << Endl;
        Check(*subset, 13570, 2277890, 18810);
    }

    Y_UNIT_TEST(Mixed)
    {
        auto subset = TMake(Mass0, PageConf(Mass0.Model->Scheme->Families.size(), false)).Mixed(0, 4, TMixerRnd(4));
        Check(*subset, 24000, 2106459, 25428);
    }

    Y_UNIT_TEST(Mixed_Groups)
    {
        auto subset = TMake(Mass1, PageConf(Mass1.Model->Scheme->Families.size(), false)).Mixed(0, 4, TMixerRnd(4));
        Check(*subset, 24000, 2460219, 13482);
    }

    Y_UNIT_TEST(Mixed_Groups_History)
    {
        auto subset = TMake(Mass1, PageConf(Mass1.Model->Scheme->Families.size(), false)).Mixed(0, 4, TMixerRnd(4), 0.3);
        Check(*subset, 24000, 4054270, 19152);
    }

    Y_UNIT_TEST(Serial)
    {
        TMixerSeq mixer(4, Mass0.Saved.Size());
        auto subset = TMake(Mass0, PageConf(Mass0.Model->Scheme->Families.size(), false)).Mixed(0, 4, mixer);
        Check(*subset, 24000, 2106459, 25428);
    }

    Y_UNIT_TEST(Serial_Groups)
    {
        TMixerSeq mixer(4, Mass1.Saved.Size());
        auto subset = TMake(Mass1, PageConf(Mass1.Model->Scheme->Families.size(), false)).Mixed(0, 4, mixer);
        Check(*subset, 24000, 2460259, 13528);
    }

    Y_UNIT_TEST(Serial_Groups_History)
    {
        TMixerSeq mixer(4, Mass1.Saved.Size());
        auto subset = TMake(Mass1, PageConf(Mass1.Model->Scheme->Families.size(), false)).Mixed(0, 4, mixer, 0.3);
        Check(*subset, 24000, 4054290, 19168);
    }

    Y_UNIT_TEST(Single_BTreeIndex)
    {
        auto subset = TMake(Mass0, PageConf(Mass0.Model->Scheme->Families.size(), true)).Mixed(0, 1, TMixerOne{ });   
        Check(*subset, 24000, 2106439, 41220);
    }

    Y_UNIT_TEST(Single_Slices_BTreeIndex)
    {
        auto subset = TMake(Mass0, PageConf(Mass0.Model->Scheme->Families.size(), true)).Mixed(0, 1, TMixerOne{ }, 0, 13);   
        subset->Flatten.begin()->Slices->Describe(Cerr); Cerr << Endl;
        Check(*subset, 12816, 1121048, 41220);
    }

    Y_UNIT_TEST(Single_Groups_BTreeIndex)
    {
        auto subset = TMake(Mass1, PageConf(Mass1.Model->Scheme->Families.size(), true)).Mixed(0, 1, TMixerOne{ });   
        Check(*subset, 24000, 2460139, 20485);
    }

    Y_UNIT_TEST(Single_Groups_Slices_BTreeIndex)
    {
        auto subset = TMake(Mass1, PageConf(Mass1.Model->Scheme->Families.size(), true)).Mixed(0, 1, TMixerOne{ }, 0, 13);   
        subset->Flatten.begin()->Slices->Describe(Cerr); Cerr << Endl;
        Check(*subset, 10440, 1060798, 20485);
    }

    Y_UNIT_TEST(Single_Groups_History_BTreeIndex)
    {
        auto subset = TMake(Mass1, PageConf(Mass1.Model->Scheme->Families.size(), true)).Mixed(0, 1, TMixerOne{ }, 0.3);   
        Check(*subset, 24000, 4054050, 29710);
    }

    Y_UNIT_TEST(Single_Groups_History_Slices_BTreeIndex)
    {
        auto subset = TMake(Mass1, PageConf(Mass1.Model->Scheme->Families.size(), true)).Mixed(0, 1, TMixerOne{ }, 0.3, 13);   
        subset->Flatten.begin()->Slices->Describe(Cerr); Cerr << Endl;
        Check(*subset, 13570, 2277890, 29710);
    }

    Y_UNIT_TEST(Mixed_BTreeIndex)
    {
        auto subset = TMake(Mass0, PageConf(Mass0.Model->Scheme->Families.size(), true)).Mixed(0, 4, TMixerRnd(4));
        Check(*subset, 24000, 2106459, 40950);
    }

    Y_UNIT_TEST(Mixed_Groups_BTreeIndex)
    {
        auto subset = TMake(Mass1, PageConf(Mass1.Model->Scheme->Families.size(), true)).Mixed(0, 4, TMixerRnd(4));
        Check(*subset, 24000, 2460219, 20394);
    }

    Y_UNIT_TEST(Mixed_Groups_History_BTreeIndex)
    {
        auto subset = TMake(Mass1, PageConf(Mass1.Model->Scheme->Families.size(), true)).Mixed(0, 4, TMixerRnd(4), 0.3);
        Check(*subset, 24000, 4054270, 29619);
    }

    Y_UNIT_TEST(Serial_BTreeIndex)
    {
        TMixerSeq mixer(4, Mass0.Saved.Size());
        auto subset = TMake(Mass0, PageConf(Mass0.Model->Scheme->Families.size(), true)).Mixed(0, 4, mixer);
        Check(*subset, 24000, 2106459, 40950);
    }

    Y_UNIT_TEST(Serial_Groups_BTreeIndex)
    {
        TMixerSeq mixer(4, Mass1.Saved.Size());
        auto subset = TMake(Mass1, PageConf(Mass1.Model->Scheme->Families.size(), false)).Mixed(0, 4, mixer);
        Check(*subset, 24000, 2460259, 13528);
    }

    Y_UNIT_TEST(Serial_Groups_History_BTreeIndex)
    {
        TMixerSeq mixer(4, Mass1.Saved.Size());
        auto subset = TMake(Mass1, PageConf(Mass1.Model->Scheme->Families.size(), false)).Mixed(0, 4, mixer, 0.3);
        Check(*subset, 24000, 4054290, 19168);
    }
}

}
