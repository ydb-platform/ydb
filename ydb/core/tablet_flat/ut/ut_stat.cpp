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
            UNIT_ASSERT_C(part->GetPageType(id) == EPage::Index, "Shouldn't request non-index pages");
            if (!Touched[groupId].insert(id).second) {
                return NTest::TTestEnv::TryGetPage(part, id, groupId);
            }
            return nullptr;
        }

        TMap<TGroupId, TSet<TPageId>> Touched;
    };

    NPage::TConf PageConf(size_t groups = 1) noexcept
    {
        NPage::TConf conf{ true, 2 * 1024 };

        conf.Groups.resize(groups);
        for (size_t group : xrange(groups)) {
            conf.Group(group).IndexMin = 1024; /* Should cover index buffer grow code */
        }
        conf.SmallEdge = 19;  /* Packed to page collection large cell values */
        conf.LargeEdge = 29;  /* Large values placed to single blobs */
        conf.SliceSize = conf.Group(0).PageSize * 4;

        return conf;
    }

    const NTest::TMass Mass0(new NTest::TModelStd(false), 24000);
    const NTest::TMass Mass1(new NTest::TModelStd(true), 24000);

    template<typename TEnv>
    void Check(const TSubset& subset, ui64 expectedRows, ui64 expectedData, ui64 expectedIndex) {
        TStats stats;
        TEnv env;

        const ui32 attempts = 10;
        for (ui32 attempt : xrange(attempts)) {
            if (NTable::BuildStats(subset, stats, 310, 3105, &env)) {
                break;
            }
            UNIT_ASSERT_C(attempt + 1 < attempts, "Too many attempts");
        }

        Cerr << "Stats: " << stats.RowCount << " " << stats.DataSize.Size << " " << stats.IndexSize.Size << " " << stats.DataSizeHistogram.size() << " " << stats.RowCountHistogram.size() << Endl;
        UNIT_ASSERT_VALUES_EQUAL(stats.RowCount, expectedRows);
        UNIT_ASSERT_VALUES_EQUAL(stats.DataSize.Size, expectedData);
        UNIT_ASSERT_VALUES_EQUAL(stats.IndexSize.Size, expectedIndex);
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
        auto subset = TMake(Mass0, PageConf(Mass0.Model->Scheme->Families.size())).Mixed(0, 1, TMixerOne{ });   
        Check(*subset, 24000, 2106439, 25272);
    }

    Y_UNIT_TEST(Single_Groups)
    {
        auto subset = TMake(Mass1, PageConf(Mass1.Model->Scheme->Families.size())).Mixed(0, 1, TMixerOne{ });   
        Check(*subset, 24000, 2460139, 13170);
    }

    Y_UNIT_TEST(Single_Groups_History)
    {
        auto subset = TMake(Mass1, PageConf(Mass1.Model->Scheme->Families.size())).Mixed(0, 1, TMixerOne{ }, 0.3);   
        Check(*subset, 24000, 4054050, 18810);
    }

    Y_UNIT_TEST(Mixed)
    {
        auto subset = TMake(Mass0, PageConf(Mass0.Model->Scheme->Families.size())).Mixed(0, 4, TMixerRnd(4));
        Check(*subset, 24000, 2106459, 25428);
    }

    Y_UNIT_TEST(Mixed_Groups)
    {
        auto subset = TMake(Mass1, PageConf(Mass1.Model->Scheme->Families.size())).Mixed(0, 4, TMixerRnd(4));
        Check(*subset, 24000, 2460219, 13482);
    }

    Y_UNIT_TEST(Mixed_Groups_History)
    {
        auto subset = TMake(Mass1, PageConf(Mass1.Model->Scheme->Families.size())).Mixed(0, 4, TMixerRnd(4), 0.3);
        Check(*subset, 24000, 4054270, 19152);
    }

    Y_UNIT_TEST(Serial)
    {
        TMixerSeq mixer(4, Mass0.Saved.Size());
        auto subset = TMake(Mass0, PageConf(Mass0.Model->Scheme->Families.size())).Mixed(0, 4, mixer);
        Check(*subset, 24000, 2106459, 25428);
    }

    Y_UNIT_TEST(Serial_Groups)
    {
        TMixerSeq mixer(4, Mass1.Saved.Size());
        auto subset = TMake(Mass1, PageConf(Mass1.Model->Scheme->Families.size())).Mixed(0, 4, mixer);
        Check(*subset, 24000, 2460259, 13528);
    }

    Y_UNIT_TEST(Serial_Groups_History)
    {
        TMixerSeq mixer(4, Mass1.Saved.Size());
        auto subset = TMake(Mass1, PageConf(Mass1.Model->Scheme->Families.size())).Mixed(0, 4, mixer, 0.3);
        Check(*subset, 24000, 4054290, 19168);
    }
}

}
