#include <ydb/core/tablet_flat/test/libs/rows/cook.h>
#include <ydb/core/tablet_flat/test/libs/table/model/large.h>
#include <ydb/core/tablet_flat/test/libs/table/test_part.h>
#include <ydb/core/tablet_flat/test/libs/table/wrap_part.h>
#include <ydb/core/tablet_flat/test/libs/table/test_writer.h>
#include <ydb/core/tablet_flat/test/libs/table/test_wreck.h>

#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/resource/resource.h>
#include <util/generic/xrange.h>
#include <util/stream/file.h>
#include <util/stream/str.h>
#include <util/string/join.h>

namespace NKikimr {
namespace NTable {

namespace {
    /* Sample rows set for old pages compatability tests, should be
        updated with assosiated raw page collections (see data/ directory).
     */

    const NTest::TMass MassZ(new NTest::TModelStd(false), 128);
}

Y_UNIT_TEST_SUITE(NPage) {

    Y_UNIT_TEST(Encoded)
    {
        using namespace NTable::NTest;

        TLayoutCook lay;

        lay
            .Col(0, 0,  NScheme::NTypeIds::Uint32)
            .Col(0, 1,  NScheme::NTypeIds::String)
            .Key({ 0 });

        const TRow foo = *TSchemedCookRow(*lay).Col(555_u32, "foo");
        const TRow bar = *TSchemedCookRow(*lay).Col(777_u32, "bar");

        NPage::TConf conf{ true, 2 * 1024 };

        conf.Group(0).Codec = NPage::ECodec::LZ4;
        conf.Group(0).ForceCompression = true; /* required for this UT only */

        TCheckIter wrap(TPartCook(lay, conf).Add(foo).Finish(), { });

        wrap.To(1).Has(foo).To(2).NoKey(bar);

        auto &part = dynamic_cast<const NTest::TPartStore&>(*(*wrap).Eggs.Lone());

        for (auto page: xrange(part.Store->PageCollectionPagesCount(0))) {
            auto *raw = part.Store->GetPage(0, page);
            auto got = NPage::TLabelWrapper().Read(*raw, NPage::EPage::Undef);

            if (got.Type != NPage::EPage::DataPage) {
                /* Have to check for compression only rows page */
            } else if (got.Codec == NPage::ECodec::Plain) {
                UNIT_FAIL("Test has failed to cook compressed pages");
            }
        }
    }

    Y_UNIT_TEST(ABI_002)
    {
        const auto raw = NResource::Find("abi/002_full_part.pages");

        TStringInput input(raw);

        {
            using namespace NTest;

            const TLogoBlobID label(1,2,3);

            auto part = NTest::TLoader(TStore::Restore(input), { }).Load(label);

            TPartEggs eggs{ nullptr, MassZ.Model->Scheme, { std::move(part) } };

            TWreck<TCheckIter, TPartEggs>(MassZ, 666).Do(EWreck::Cached, eggs);
        }
    }

    struct TDeltaInfo {
        ui64 TxId;
        ui32 SavepointSeqNum;
        ELockMode LockMode;

        bool operator==(const TDeltaInfo&) const = default;

        friend IOutputStream& operator<<(IOutputStream& out, const TDeltaInfo& info) {
            return out << "{" << info.TxId << ", " << info.SavepointSeqNum << ", " << ui32(info.LockMode) << "}";
        }
    };

    // Reads delta records of the main group straight from data pages
    TVector<TDeltaInfo> CollectDeltas(const NTest::TPartEggs& eggs, TSet<ui16>& dataPageVersions) {
        auto& part = dynamic_cast<const NTest::TPartStore&>(*eggs.Lone());
        const auto& group = part.Scheme->Groups[0];

        TVector<TDeltaInfo> deltas;
        for (auto pageId : xrange(part.Store->PageCollectionPagesCount(0))) {
            auto* raw = part.Store->GetPage(0, pageId);
            auto label = NPage::TLabelWrapper().Read(*raw, NPage::EPage::Undef);
            if (label.Type != NPage::EPage::DataPage) {
                continue;
            }
            dataPageVersions.insert(label.Version);

            NPage::TDataPage page(raw);
            for (auto it = page->Begin(); it != page->End(); ++it) {
                for (size_t index = 0; const auto* record = it->GetAltRecord(index); ++index) {
                    if (!record->IsDelta()) {
                        break;
                    }
                    auto lockMode = record->IsLocked() ? std::get<0>(record->GetLockInfo(group)) : ELockMode::None;
                    deltas.push_back({ record->GetDeltaTxId(group), record->GetDeltaSavepointSeqNum(group), lockMode });
                }
            }
        }
        return deltas;
    }

    Y_UNIT_TEST(DeltaSavepointSeqNum)
    {
        using namespace NTable::NTest;

        TLayoutCook lay;

        lay
            .Col(0, 0,  NScheme::NTypeIds::Uint32)
            .Col(0, 1,  NScheme::NTypeIds::String)
            .Key({ 0 });

        NPage::TConf conf{ true, 2 * 1024 };

        {
            // Without savepoint seq nums pages keep the old format
            auto eggs = TPartCook(lay, conf)
                .Delta(123).AddN(1_u32, "a")
                .Delta(234).AddN(1_u32, "b")
                .Ver().AddN(1_u32, "c")
                .Delta(123).AddN(2_u32, "d")
                .Finish();

            TSet<ui16> versions;
            auto deltas = CollectDeltas(eggs, versions);
            UNIT_ASSERT_VALUES_EQUAL(JoinSeq(",", versions), "1");
            UNIT_ASSERT_VALUES_EQUAL(deltas, (TVector<TDeltaInfo>{
                { 123, 0, ELockMode::None },
                { 234, 0, ELockMode::None },
                { 123, 0, ELockMode::None },
            }));
        }

        {
            // A delta with a savepoint seq num makes its page version 2
            auto eggs = TPartCook(lay, conf)
                .Delta(123, 5).AddN(1_u32, "a")
                .Delta(123).AddN(1_u32, "b")
                .Ver().AddN(1_u32, "c")
                .Lock(ELockMode::Exclusive, 234).Delta(234, 7).AddN(2_u32, "d")
                .Finish();

            TSet<ui16> versions;
            auto deltas = CollectDeltas(eggs, versions);
            UNIT_ASSERT_VALUES_EQUAL(JoinSeq(",", versions), "2");
            UNIT_ASSERT_VALUES_EQUAL(deltas, (TVector<TDeltaInfo>{
                { 123, 5, ELockMode::None },
                { 123, 0, ELockMode::None },
                { 234, 7, ELockMode::Exclusive },
            }));

            // Rows are still readable through the regular iterator
            TCheckIter wrap(eggs, { });
            wrap.To(1).Has(*TSchemedCookRow(*lay).Col(1_u32, "c"));
        }
    }

    Y_UNIT_TEST(GroupIdEncoding) {
        NPage::TGroupId main;
        UNIT_ASSERT_VALUES_EQUAL(main.Raw(), 0u);
        NPage::TGroupId alt(1);
        UNIT_ASSERT_VALUES_EQUAL(alt.Raw(), 1u);
        NPage::TGroupId mainHist(0, true);
        UNIT_ASSERT_VALUES_EQUAL(mainHist.Raw(), 0x80000000u);
        NPage::TGroupId altHist(1, true);
        UNIT_ASSERT_VALUES_EQUAL(altHist.Raw(), 0x80000001u);
        UNIT_ASSERT(main == main);
        UNIT_ASSERT(main < alt);
        UNIT_ASSERT(main < mainHist);
        UNIT_ASSERT(alt < mainHist);
        UNIT_ASSERT(alt < altHist);
        UNIT_ASSERT(mainHist < altHist);
    }

}

}
}
