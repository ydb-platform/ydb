#include <ydb/core/tablet_flat/test/libs/rows/cook.h>
#include <ydb/core/tablet_flat/test/libs/table/model/large.h>
#include <ydb/core/tablet_flat/test/libs/table/test_part.h>
#include <ydb/core/tablet_flat/test/libs/table/wrap_part.h>
#include <ydb/core/tablet_flat/test/libs/table/test_writer.h>
#include <ydb/core/tablet_flat/test/libs/table/test_wreck.h>

#include <contrib/libs/zstd/include/zstd.h>

#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/resource/resource.h>
#include <util/generic/xrange.h>
#include <util/stream/file.h>
#include <util/stream/str.h>

namespace NKikimr {
namespace NTable {

namespace {
    /* Sample rows set for old pages compatability tests, should be
        updated with assosiated raw page collections (see data/ directory).
     */

    const NTest::TMass MassZ(new NTest::TModelStd(false), 128);
}

namespace {
    void CheckEncoded(NPage::ECodec codec, ui32 codecLevel = 1)
    {
        using namespace NTable::NTest;

        TLayoutCook lay;

        lay
            .Col(0, 0,  NScheme::NTypeIds::Uint32)
            .Col(0, 1,  NScheme::NTypeIds::String)
            .Key({ 0 });

        TString value;
        for (ui32 i : xrange(128u)) {
            value += ToString(i % 17) + "prefix" + ToString(i * i) + TString(i % 11, 'a');
        }
        const TRow foo = *TSchemedCookRow(*lay).Col(555_u32, value);
        const TRow bar = *TSchemedCookRow(*lay).Col(777_u32, "bar");

        NPage::TConf conf{ true, 2 * 1024 };

        conf.Group(0).Codec = codec;
        conf.Group(0).CodecLevel = codecLevel;
        conf.Group(0).ForceCompression = true; /* required for this UT only */

        TCheckIter wrap(TPartCook(lay, conf).Add(foo).Finish(), { });

        wrap.To(1).Has(foo).To(2).NoKey(bar);

        auto &part = dynamic_cast<const NTest::TPartStore&>(*(*wrap).Eggs.Lone());

        for (auto page: xrange(part.Store->PageCollectionPagesCount(0))) {
            auto *raw = part.Store->GetPage(0, page);
            auto got = NPage::TLabelWrapper().Read(*raw, NPage::EPage::Undef);

            if (got.Type != NPage::EPage::DataPage) {
                continue;
            }
            UNIT_ASSERT(got.Codec == codec);
            if (codec == NPage::ECodec::ZSTD) {
                const NPage::TDataPage decoded(raw);
                const TStringBuf plain(decoded.GetData().data() + 16, decoded.GetData().size() - 16);
                const TString expected = NBlockCodecs::Codec(
                    "zstd_" + ToString(codecLevel ? codecLevel : ZSTD_defaultCLevel()))->Encode(plain);
                UNIT_ASSERT_VALUES_EQUAL(TStringBuf(got.Page.data(), got.Page.size()), expected);
            }
        }

        NProto::TRoot root;
        TMemoryInput stream(part.Store->GetMeta()->data(), part.Store->GetMeta()->size());
        UNIT_ASSERT(root.ParseFromArcadiaStream(&stream));
        if (codec == NPage::ECodec::ZSTD) {
            UNIT_ASSERT_VALUES_EQUAL(root.GetEvol().GetTail(), 32);
        } else {
            UNIT_ASSERT_LT(root.GetEvol().GetTail(), 32);
        }
    }
}

Y_UNIT_TEST_SUITE(NPage) {

    Y_UNIT_TEST(Encoded)
    {
        CheckEncoded(NPage::ECodec::LZ4);
    }

    Y_UNIT_TEST(EncodedZstd)
    {
        for (ui32 level : {0u, 1u, 2u, 3u, 10u, static_cast<ui32>(ZSTD_maxCLevel())}) {
            CheckEncoded(NPage::ECodec::ZSTD, level);
        }
    }

    Y_UNIT_TEST(ZstdCodecReuse)
    {
        NPage::TZstdPageCodec encoder;
        const auto* decoder = NPage::GetBlockCodec(NPage::ECodec::ZSTD);
        for (ui32 level : {0u, static_cast<ui32>(ZSTD_maxCLevel()), 1u, 7u}) {
            encoder.SetCompressionLevel(level);
            const auto* originalCodec = NBlockCodecs::Codec(
                "zstd_" + ToString(level ? level : ZSTD_defaultCLevel()));
            for (size_t size : {0u, 32u, 4096u, 65536u, 128u}) {
                TString value(size, 'a');
                for (size_t i = 0; i < size; ++i) {
                    value[i] = 'a' + (i * i % 23);
                }
                const auto encoded = encoder.Encode(value);
                UNIT_ASSERT_VALUES_EQUAL(encoded, originalCodec->Encode(value));
                UNIT_ASSERT_VALUES_EQUAL(originalCodec->Decode(encoded), value);
                UNIT_ASSERT_VALUES_EQUAL(decoder->Decode(encoded), value);
            }
        }

        const TString value(128, 'a');
        const auto encoded = encoder.Encode(value);
        auto corrupt = encoded;
        corrupt[sizeof(ui64)] = 0; // Damage the frame magic, preserving the length prefix.
        UNIT_ASSERT_EXCEPTION(decoder->Decode(corrupt), yexception);
        UNIT_ASSERT_VALUES_EQUAL(decoder->Decode(encoded), value);

        auto badLength = encoded;
        WriteUnaligned<ui64>(badLength.Detach(), value.size() + 1);
        UNIT_ASSERT_EXCEPTION(decoder->Decode(badLength), NBlockCodecs::TDecompressError);
        UNIT_ASSERT_VALUES_EQUAL(decoder->Decode(encoded), value);
    }

    Y_UNIT_TEST(MixedCompressionGroups)
    {
        using namespace NTable::NTest;

        TLayoutCook lay;
        lay
            .Col(0, 0, NScheme::NTypeIds::Uint32)
            .Col(0, 1, NScheme::NTypeIds::String)
            .Col(1, 2, NScheme::NTypeIds::String)
            .Col(2, 3, NScheme::NTypeIds::String)
            .Col(3, 4, NScheme::NTypeIds::String)
            .Col(4, 5, NScheme::NTypeIds::String)
            .Key({ 0 });

        const TString value(512, 'a');
        const TRow first = *TSchemedCookRow(*lay).Col(111_u32, value, value, value, value, value);
        const TRow second = *TSchemedCookRow(*lay).Col(222_u32, value, value, value, value, value);

        const NPage::ECodec codecs[] = {
            NPage::ECodec::LZ4, NPage::ECodec::ZSTD, NPage::ECodec::Plain, NPage::ECodec::LZ4, NPage::ECodec::ZSTD,
        };
        NPage::TConf conf{ true, 2 * 1024 };
        for (ui32 group : xrange(5u)) {
            conf.Group(group).Codec = codecs[group];
            conf.Group(group).PageRows = 1;
        }
        conf.Group(1).CodecLevel = 0;
        conf.Group(4).CodecLevel = ZSTD_maxCLevel();

        TCheckIter wrap(TPartCook(lay, conf).Add(first).Add(second).Finish(), { });
        wrap.To(1).Has(first).Next().Has(second).Next().Is(EReady::Gone);

        const auto& part = dynamic_cast<const NTest::TPartStore&>(*(*wrap).Eggs.Lone());
        NPage::TDataPage decoder;
        for (ui32 group : { 0u, 1u, 2u, 3u, 4u, 1u, 0u }) {
            ui32 dataPages = 0;
            for (ui32 page : xrange(part.Store->PageCollectionPagesCount(group))) {
                const auto* raw = part.Store->GetPage(group, page);
                const auto label = NPage::TLabelWrapper().Read(*raw, NPage::EPage::Undef);
                if (label.Type != NPage::EPage::DataPage) {
                    continue;
                }
                UNIT_ASSERT(label.Codec == codecs[group]);
                decoder.Set(raw);
                UNIT_ASSERT_VALUES_EQUAL(decoder->Count, 1);
                UNIT_ASSERT_VALUES_EQUAL(decoder.BaseRow(), dataPages++);
                if (label.Codec != NPage::ECodec::Plain) {
                    UNIT_ASSERT_LT(raw->size(), decoder.GetData().size());
                }
            }
            UNIT_ASSERT_VALUES_EQUAL(dataPages, 2);
        }
    }

    Y_UNIT_TEST(ZstdCompressionFallback)
    {
        using namespace NTable::NTest;

        TLayoutCook lay;
        lay.Col(0, 0, NScheme::NTypeIds::Uint32).Key({ 0 });
        const TRow row = *TSchemedCookRow(*lay).Col(111_u32);

        NPage::TConf conf;
        conf.Group(0).Codec = NPage::ECodec::ZSTD;
        TCheckIter wrap(TPartCook(lay, conf).Add(row).Finish(), { });
        wrap.To(1).Has(row);

        const auto& part = dynamic_cast<const NTest::TPartStore&>(*(*wrap).Eggs.Lone());
        ui32 dataPages = 0;
        for (ui32 page : xrange(part.Store->PageCollectionPagesCount(0))) {
            const auto label = NPage::TLabelWrapper().Read(*part.Store->GetPage(0, page), NPage::EPage::Undef);
            if (label.Type == NPage::EPage::DataPage) {
                UNIT_ASSERT(label.Codec == NPage::ECodec::Plain);
                ++dataPages;
            }
        }
        UNIT_ASSERT_VALUES_EQUAL(dataPages, 1);

        NProto::TRoot root;
        TMemoryInput stream(part.Store->GetMeta()->data(), part.Store->GetMeta()->size());
        UNIT_ASSERT(root.ParseFromArcadiaStream(&stream));
        UNIT_ASSERT_LT(root.GetEvol().GetTail(), 32);
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
