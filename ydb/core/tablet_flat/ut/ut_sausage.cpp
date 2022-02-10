#include <ydb/core/tablet_flat/flat_sausage_align.h>
#include <ydb/core/tablet_flat/flat_sausage_meta.h>
#include <ydb/core/tablet_flat/flat_sausage_writer.h>
#include <ydb/core/tablet_flat/flat_sausage_flow.h>
#include <ydb/core/tablet_flat/flat_sausage_solid.h>
#include <ydb/core/tablet_flat/flat_sausage_chop.h>
#include <ydb/core/tablet_flat/flat_sausage_grind.h>
#include <ydb/core/tablet_flat/util_fmt_desc.h>

#include <library/cpp/testing/unittest/registar.h>
#include <util/generic/xrange.h>
#include <array>

namespace NKikimr {
namespace NPageCollection {

namespace {
    struct TMyPageCollection {
        using TArrayRef = TArrayRef<const TGlobId>;

        TMyPageCollection(TArrayRef globs) : Globs(globs) { }

        inline TBorder Bounds(ui32 page) const noexcept
        {
            const auto size = Glob(page).Logo.BlobSize();

            return { size, { page, 0 }, { page, size } };
        }

        inline TGlobId Glob(ui32 at) const noexcept
        {
            return at < Globs.size() ? Globs[at] : TGlobId{ };
        }

        const TArrayRef Globs;
    };
}

Y_UNIT_TEST_SUITE(NPageCollection) {
    using TGlow = TPagesToBlobsConverter<TMeta>;

    static const std::array<TLogoBlobID, 7> Blobs = {{
        TLogoBlobID(10, 20, 30, 1, 100, 0),
        TLogoBlobID(10, 20, 30, 1, 120, 1),
        TLogoBlobID(10, 20, 30, 1,  40, 2),
        TLogoBlobID(10, 20, 30, 1,  60, 3),
        TLogoBlobID(10, 20, 30, 1, 140, 4),
        TLogoBlobID(10, 20, 30, 1, 170, 5),
        TLogoBlobID(10, 20, 30, 1, 150, 6),
    }};

    const std::array<ui32, 9> Pages= {{ 50, 30, 80, 60, 40, 60, 70, 380, 10 }};

    TSharedData MakeMeta()
    {
        NPageCollection::TRecord meta(0);

        meta.Push(Blobs);

        for (auto size: Pages) {
            TString lumber(size, '9');
            meta.Push(0, lumber);
        }

        return meta.Finish();
    }

    Y_UNIT_TEST(Align)
    {
        const std::array<ui64, 3> steps = {{ 200, 500, 1000 }};

        const TAlign align(steps);

        const auto lookup1 = align.Lookup(0, 1000);
        const auto lookup2 = align.Lookup(100, 800);
        const auto lookup3 = align.Lookup(300, 200);
        const auto lookup4 = align.Lookup(300, 250);
        const auto lookup5 = align.Lookup(200, 20);

        UNIT_ASSERT(lookup1.Lo.Blob == 0 && lookup1.Lo.Skip == 0
                    && lookup1.Up.Blob == 2 && lookup1.Up.Skip == 500);
        UNIT_ASSERT(lookup2.Lo.Blob == 0 && lookup2.Lo.Skip == 100
                    && lookup2.Up.Blob == 2 && lookup2.Up.Skip == 400);
        UNIT_ASSERT(lookup3.Lo.Blob == 1 && lookup3.Lo.Skip == 100
                    && lookup3.Up.Blob == 1 && lookup3.Up.Skip == 300);
        UNIT_ASSERT(lookup4.Lo.Blob == 1 && lookup4.Lo.Skip == 100
                    && lookup4.Up.Blob == 2 && lookup4.Up.Skip == 50);
        UNIT_ASSERT(lookup5.Lo.Blob == 1 && lookup5.Lo.Skip == 0
                    && lookup5.Up.Blob == 1 && lookup5.Up.Skip == 20);
    }

    Y_UNIT_TEST(Meta)
    {
        TString chunk1(5000000, '1');
        TString chunk2(15000000, '2');
        TString chunk3(6000000, '3');

        const TGlobId glob{ TLogoBlobID(10, 20, 30, 1, 0, 0), 777 };

        auto checkGlobs = [&](TVector<TGlob> blobs) {
            for (const auto &one: blobs) {
                bool pln = TGroupBlobsByCookie::IsInPlane(one.GId.Logo, glob.Logo);
                bool grp = (one.GId.Group == glob.Group);
                bool cnl = (one.GId.Logo.Channel() == glob.Logo.Channel());

                UNIT_ASSERT(pln && grp && cnl);
            }

            return blobs.size();
        };

        TCookieAllocator cookieAllocator(10, (ui64(20) << 32) | 30, { 0,  999 }, {{ 1, 777 }});

        TWriter writer(cookieAllocator, 1 /* channel */, 8192 * 1024);

        const auto r1 = writer.AddPage(chunk1, 1);
        writer.AddInplace(r1, TStringBuf("chunk 1"));

        UNIT_ASSERT(r1 == 0 && checkGlobs(writer.Grab()) == 0);

        const auto r2 = writer.AddPage(chunk2, 2);
        writer.AddInplace(r2, TStringBuf("chunk 2"));

        UNIT_ASSERT(r2 == 1 && checkGlobs(writer.Grab()) == 2);

        const auto r3 = writer.AddPage(chunk3, 3);
        writer.AddInplace(r3, TStringBuf("chunk 3"));

        UNIT_ASSERT(r3 == 2 && checkGlobs(writer.Grab()) == 1);

        const auto blob = writer.Finish(true);

        UNIT_ASSERT(checkGlobs(writer.Grab()) == 1);

        const TMeta meta(blob, 0 /* group, unused */);

        UNIT_ASSERT(meta.TotalPages() == 3);
        UNIT_ASSERT(meta.GetPageType(0) == 1);
        UNIT_ASSERT(meta.GetPageType(1) == 2);
        UNIT_ASSERT(meta.GetPageType(2) == 3);

        UNIT_ASSERT(meta.GetPageInplaceData(0) == "chunk 1");
        UNIT_ASSERT(meta.GetPageInplaceData(1) == "chunk 2");
        UNIT_ASSERT(meta.GetPageInplaceData(2) == "chunk 3");

        auto l1 = meta.Bounds(0);
        auto l2 = meta.Bounds(1);
        auto l3 = meta.Bounds(2);

        UNIT_ASSERT(l1.Lo.Blob == 0 && l1.Up.Blob == 0);
        UNIT_ASSERT(l2.Lo.Blob == 0 && l2.Up.Blob == 2);
        UNIT_ASSERT(l3.Lo.Blob == 2 && l3.Up.Blob == 3);
    }

    Y_UNIT_TEST(PagesToBlobsConverter)
    {
        const TMeta meta(MakeMeta(), 0);

        auto grow = [&meta](const TVector<ui32> pages) {
            TGlow flow(meta, pages);
            flow.Grow(Max<ui64>());
            return std::move(flow.Queue);
        };

        { /*_ { 1 } lies away of the bounds of one blob */
            const auto ln = grow({ 1 });

            UNIT_ASSERT(ln.size() == 1);
            UNIT_ASSERT((ln[0] == TGlow::TReadPortion{ 0, 30, 0, 50 }));
        }

        { /*_ { 0, 6 } snaps to the start of the single blob */
            const auto ln = grow({ 0, 6 });

            UNIT_ASSERT(ln.size() == 2);
            UNIT_ASSERT((ln[0] == TGlow::TReadPortion{ 0, 50, 0, 0 }));
            UNIT_ASSERT((ln[1] == TGlow::TReadPortion{ 1, 70, 4, 0 }));
        }

        { /*_ { 3, 8 } snaps to the end of the single blob */
            const auto ln = grow({ 3, 8 });

            UNIT_ASSERT(ln.size() == 2);
            UNIT_ASSERT((ln[0] == TGlow::TReadPortion{ 0, 60, 1,  60 }));
            UNIT_ASSERT((ln[1] == TGlow::TReadPortion{ 1, 10, 6, 140 }));
        }

        { /*_ { 2 } spans over the bounds of the two blobs */
            const auto ln = grow({ 2 });


            UNIT_ASSERT(ln.size() == 2);
            UNIT_ASSERT((ln[0] == TGlow::TReadPortion{ 0, 20, 0, 80 }));
            UNIT_ASSERT((ln[1] == TGlow::TReadPortion{ 0, 60, 1, 0 }));
        }

        { /*_ { 4, 5 } each occupies entire single blob */
            const auto ln = grow({ 4, 5 });

            UNIT_ASSERT(ln.size() == 2);
            UNIT_ASSERT((ln[0] == TGlow::TReadPortion{ 0, 40, 2, 0 }));
            UNIT_ASSERT((ln[1] == TGlow::TReadPortion{ 1, 60, 3, 0 }));
        }

        { /*_ { 7 } spans over a serveral subsequent blobs */
            const auto ln = grow({ 7 });

            UNIT_ASSERT(ln.size() == 3);
            UNIT_ASSERT((ln[0] == TGlow::TReadPortion{ 0,  70, 4, 70 }));
            UNIT_ASSERT((ln[1] == TGlow::TReadPortion{ 0, 170, 5, 0 }));
            UNIT_ASSERT((ln[2] == TGlow::TReadPortion{ 0, 140, 6, 0 }));
        }
    }

    Y_UNIT_TEST(Grow)
    {
        const TMeta meta(MakeMeta(), 0);
        const std::array<ui32, 9> nums = {{ 0, 1, 2, 3, 4, 5, 6, 7, 8 }};

        TGlow flow(meta, nums);

        auto grow = [&flow](ui64 bytes) {
            ui64 used = 0;

            if (const auto more = flow.Grow(bytes))
                for (const auto on : xrange(+more))
                    used += flow.Queue[more.From + on].Size;

            return used;
        };

        UNIT_ASSERT((grow(200) == 160));
        UNIT_ASSERT((grow(200) == 160));
        UNIT_ASSERT((grow(200) == 70));
        UNIT_ASSERT((grow(200) == 380));
        UNIT_ASSERT((grow(200) == 10));
    }

    Y_UNIT_TEST(Groups)
    {
        const std::array<TGlobId, 6> globs = {{
            { TLogoBlobID(1, 2, 3, 1, 10, 0), 7 },
            { TLogoBlobID(1, 2, 3, 1, 12, 1), 7 },
            { TLogoBlobID(1, 2, 3, 1, 14, 2), 7 },
            { TLogoBlobID(1, 2, 3, 1, 16, 3), 4 },
            { TLogoBlobID(1, 2, 3, 1, 18, 4), 7 },
            { TLogoBlobID(1, 2, 3, 1, 20, 5), 7 },
        }};

        const TMyPageCollection myPageCollection(globs);

        auto check = [&globs](const TPagesToBlobsConverter<TMyPageCollection> &flow) {
            for (auto num : xrange(flow.Queue.size())) {
                const auto &brick = flow.Queue.at(num);

                UNIT_ASSERT(brick.Slot == num && brick.Blob == num);
                UNIT_ASSERT(brick.Size == globs[num].Logo.BlobSize());
                UNIT_ASSERT(brick.Skip == 0);
            }
        };

        { /*_ Take the only blob page in middle */
            const ui32 slice1[] = { 3 };
            const ui32 slice2[] = { 4 };
            auto one = TPagesToBlobsConverter<TMyPageCollection>(myPageCollection, slice1).Grow(7500);
            auto two = TPagesToBlobsConverter<TMyPageCollection>(myPageCollection, slice2).Grow(7500);

            UNIT_ASSERT(one && one.From == 0 && one.To == 1);
            UNIT_ASSERT(two && two.From == 0 && two.To == 1);
        }

        { /*_ Read all pages in blobs page collection */
            const ui32 slice[] = { 0, 1, 2, 3, 4, 5 };
            TPagesToBlobsConverter<TMyPageCollection> flow(myPageCollection, slice);

            auto one = flow.Grow(7500);
            auto two = flow.Grow(7500);
            auto thr = flow.Grow(7500);

            UNIT_ASSERT(one);
            UNIT_ASSERT_VALUES_EQUAL(one.From, 0);
            UNIT_ASSERT_VALUES_EQUAL(one.To, 3);
            UNIT_ASSERT(one && one.From == 0 && one.To == 3);
            UNIT_ASSERT(two && two.From == 3 && two.To == 4);
            UNIT_ASSERT(thr && thr.From == 4 && thr.To == 6);

            check(flow);
        }

        { /*_ Read with limits on each package */
            const ui32 slice[] = { 0, 1, 2, 3, 4, 5 };
            TPagesToBlobsConverter<TMyPageCollection> flow(myPageCollection, slice);

            auto one = flow.Grow(40);
            auto two = flow.Grow(40);
            auto thr = flow.Grow(40);

            UNIT_ASSERT(one && one.From == 0 && one.To == 3);
            UNIT_ASSERT(two && two.From == 3 && two.To == 4);
            UNIT_ASSERT(thr && thr.From == 4 && thr.To == 6);

            check(flow);
        }
    }

    Y_UNIT_TEST(Chop)
    {
        static const std::array<TLogoBlobID, 6> dash = {{
            TLogoBlobID(10, 20, 30, 1, 25, 0),
            TLogoBlobID(10, 20, 30, 1, 25, 1),
            TLogoBlobID(10, 20, 30, 1, 10, 2),
            TLogoBlobID(10, 20, 30, 1, 15, 5),
            TLogoBlobID(10, 20, 30, 1, 15, 6),
            TLogoBlobID(10, 20, 30, 1, 10, 9),
        }};

        { /* array of logos with full span */
            const auto span = TGroupBlobsByCookie(Blobs).Do();

            UNIT_ASSERT(span.size() == 7 && span[0] == Blobs[0]);
        }

        { /* tirvial array of zero lengh */
            const auto span = TGroupBlobsByCookie({ }).Do();

            UNIT_ASSERT(span.size() == 0);
        }

        { /* dashed aray and largeGlobId parser */
            TGroupBlobsByCookie chop(dash);

            TVector<TGroupBlobsByCookie::TArray> span;

            for (size_t it = 0; it < 4; it++)
                span.emplace_back(chop.Do());

            UNIT_ASSERT(span[0].size() == 3 && span[0][0] == dash[0]);
            UNIT_ASSERT(span[1].size() == 2 && span[1][0] == dash[3]);
            UNIT_ASSERT(span[2].size() == 1 && span[2][0] == dash[5]);
            UNIT_ASSERT(span[3].size() == 0);

            const auto largeGlobId = TGroupBlobsByCookie::ToLargeGlobId(span[0]);

            UNIT_ASSERT(largeGlobId.Bytes == 60 && largeGlobId.Lead == dash[0]);
            UNIT_ASSERT(TGroupBlobsByCookie::ToLargeGlobId(span[2]).Bytes == 10);
            UNIT_ASSERT(TGroupBlobsByCookie::ToLargeGlobId({ }).Bytes == 0);
        }
    }

    Y_UNIT_TEST(CookieAllocator)
    {
        TSteppedCookieAllocator cookieAllocator(1, (ui64(2) << 32) | 1, { 10, 32 }, {{ 3, 999 }});

        cookieAllocator.Switch(3, true /* require step switch */);

        { /*_ LargeGlobId spanned over several blobs */
            auto largeGlobId = cookieAllocator.Do(3, 25, 10);
            auto itBlobs = largeGlobId.Blobs().begin();

            UNIT_ASSERT(largeGlobId.Group == 999 && largeGlobId.BlobCount() == 3);
            UNIT_ASSERT(*itBlobs++ == TLogoBlobID(1, 2, 3, 3, 10, 10));
            UNIT_ASSERT(*itBlobs++ == TLogoBlobID(1, 2, 3, 3, 10, 11));
            UNIT_ASSERT(*itBlobs++ == TLogoBlobID(1, 2, 3, 3,  5, 12));
            UNIT_ASSERT(itBlobs == largeGlobId.Blobs().end());
        }

        { /*_ Trivial largeGlobId occupying one blob */
            auto largeGlobId = cookieAllocator.Do(3, 9, 10);
            auto itBlobs = largeGlobId.Blobs().begin();

            UNIT_ASSERT(largeGlobId.Group == 999 && largeGlobId.BlobCount() == 1);
            UNIT_ASSERT(*itBlobs++ == TLogoBlobID(1, 2, 3, 3,  9, 14));
            UNIT_ASSERT(itBlobs == largeGlobId.Blobs().end());
        }

        { /*_ Single blob just after placed largeGlobId */
            auto glob = cookieAllocator.Do(3, 88);

            UNIT_ASSERT(glob == TGlobId(TLogoBlobID(1, 2, 3, 3, 88, 16), 999));
        }

        { /*_ Continue single blob series, no holes */
            auto glob = cookieAllocator.Do(3, 26);

            UNIT_ASSERT(glob == TGlobId(TLogoBlobID(1, 2, 3, 3, 26, 17), 999));
        }

        { /*_ Trivial largeGlobId with exact blob size */
            auto largeGlobId = cookieAllocator.Do(3, 10, 10);
            auto itBlobs = largeGlobId.Blobs().begin();

            UNIT_ASSERT(largeGlobId.Group == 999 && largeGlobId.BlobCount() == 1);
            UNIT_ASSERT(*itBlobs++ == TLogoBlobID(1, 2, 3, 3, 10, 19));
            UNIT_ASSERT(itBlobs == largeGlobId.Blobs().end());
        }

        cookieAllocator.Switch(6, true /* require step switch*/);

        { /*_ After step switch should reset state */
            auto glob = cookieAllocator.Do(3, 19);

            UNIT_ASSERT(glob == TGlobId(TLogoBlobID(1, 2, 6, 3, 19, 10), 999));
        }

        cookieAllocator.Switch(6, false /* should allow noop */);

        { /*_ On NOOP cookieRange state should not be altered */
            auto glob = cookieAllocator.Do(3, 77);

            UNIT_ASSERT(glob == TGlobId(TLogoBlobID(1, 2, 6, 3, 77, 11), 999));
        }
    }
}

}
}
