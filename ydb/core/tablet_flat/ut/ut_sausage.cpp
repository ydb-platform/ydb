#include <ydb/core/tablet_flat/flat_sausage_align.h>
#include <ydb/core/tablet_flat/flat_sausage_meta.h>
#include <ydb/core/tablet_flat/flat_sausage_writer.h>
#include <ydb/core/tablet_flat/flat_sausage_packet.h>
#include <ydb/core/tablet_flat/flat_sausage_flow.h>
#include <ydb/core/tablet_flat/flat_sausage_solid.h>
#include <ydb/core/tablet_flat/flat_sausage_chop.h>
#include <ydb/core/tablet_flat/flat_sausage_grind.h>
#include <ydb/core/tablet_flat/flat_page_blobs.h>
#include <ydb/core/tablet_flat/flat_part_iface.h>
#include <ydb/core/tablet_flat/flat_page_other.h>
#include <ydb/core/tablet_flat/util_fmt_desc.h>

#include <library/cpp/digest/crc32c/crc32c.h>
#include <library/cpp/testing/unittest/registar.h>
#include <util/generic/xrange.h>
#include <util/generic/hash.h>
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

        inline TBorder Bounds(NTable::NPage::TPageLocation location) const noexcept
        {
            return Bounds(location.Offset.AsPageIndex());
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

        auto grow = [&meta](TVector<TPageLocation> pages) {
            TGlow flow(meta, pages);
            flow.Grow(Max<ui64>());
            return std::move(flow.Queue);
        };

        { /*_ { 1 } lies away of the bounds of one blob */
            const auto ln = grow({ meta.GetLocation(1) });

            UNIT_ASSERT(ln.size() == 1);
            UNIT_ASSERT((ln[0] == TGlow::TReadPortion{ 0, 30, 0, 50 }));
        }

        { /*_ { 0, 6 } snaps to the start of the single blob */
            const auto ln = grow({ meta.GetLocation(0), meta.GetLocation(6) });

            UNIT_ASSERT(ln.size() == 2);
            UNIT_ASSERT((ln[0] == TGlow::TReadPortion{ 0, 50, 0, 0 }));
            UNIT_ASSERT((ln[1] == TGlow::TReadPortion{ 1, 70, 4, 0 }));
        }

        { /*_ { 3, 8 } snaps to the end of the single blob */
            const auto ln = grow({ meta.GetLocation(3), meta.GetLocation(8) });

            UNIT_ASSERT(ln.size() == 2);
            UNIT_ASSERT((ln[0] == TGlow::TReadPortion{ 0, 60, 1,  60 }));
            UNIT_ASSERT((ln[1] == TGlow::TReadPortion{ 1, 10, 6, 140 }));
        }

        { /*_ { 2 } spans over the bounds of the two blobs */
            const auto ln = grow({ meta.GetLocation(2) });


            UNIT_ASSERT(ln.size() == 2);
            UNIT_ASSERT((ln[0] == TGlow::TReadPortion{ 0, 20, 0, 80 }));
            UNIT_ASSERT((ln[1] == TGlow::TReadPortion{ 0, 60, 1, 0 }));
        }

        { /*_ { 4, 5 } each occupies entire single blob */
            const auto ln = grow({ meta.GetLocation(4), meta.GetLocation(5) });

            UNIT_ASSERT(ln.size() == 2);
            UNIT_ASSERT((ln[0] == TGlow::TReadPortion{ 0, 40, 2, 0 }));
            UNIT_ASSERT((ln[1] == TGlow::TReadPortion{ 1, 60, 3, 0 }));
        }

        { /*_ { 7 } spans over a serveral subsequent blobs */
            const auto ln = grow({ meta.GetLocation(7) });

            UNIT_ASSERT(ln.size() == 3);
            UNIT_ASSERT((ln[0] == TGlow::TReadPortion{ 0,  70, 4, 70 }));
            UNIT_ASSERT((ln[1] == TGlow::TReadPortion{ 0, 170, 5, 0 }));
            UNIT_ASSERT((ln[2] == TGlow::TReadPortion{ 0, 140, 6, 0 }));
        }
    }

    Y_UNIT_TEST(Grow)
    {
        const TMeta meta(MakeMeta(), 0);
        TVector<TPageLocation> pages = {
            meta.GetLocation(0), meta.GetLocation(1), meta.GetLocation(2),
            meta.GetLocation(3), meta.GetLocation(4), meta.GetLocation(5),
            meta.GetLocation(6), meta.GetLocation(7), meta.GetLocation(8),
        };

        TGlow flow(meta, pages);

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
            const TPageLocation slice1[] = { TPageLocation::FromPageIndex(3, globs[3].Logo.BlobSize()) };
            const TPageLocation slice2[] = { TPageLocation::FromPageIndex(4, globs[4].Logo.BlobSize()) };
            auto one = TPagesToBlobsConverter<TMyPageCollection>(myPageCollection, slice1).Grow(7500);
            auto two = TPagesToBlobsConverter<TMyPageCollection>(myPageCollection, slice2).Grow(7500);

            UNIT_ASSERT(one && one.From == 0 && one.To == 1);
            UNIT_ASSERT(two && two.From == 0 && two.To == 1);
        }

        { /*_ Read all pages in blobs page collection */
            const TPageLocation slice[] = {
                TPageLocation::FromPageIndex(0, globs[0].Logo.BlobSize()),
                TPageLocation::FromPageIndex(1, globs[1].Logo.BlobSize()),
                TPageLocation::FromPageIndex(2, globs[2].Logo.BlobSize()),
                TPageLocation::FromPageIndex(3, globs[3].Logo.BlobSize()),
                TPageLocation::FromPageIndex(4, globs[4].Logo.BlobSize()),
                TPageLocation::FromPageIndex(5, globs[5].Logo.BlobSize()),
            };
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
            const TPageLocation slice[] = {
                TPageLocation::FromPageIndex(0, globs[0].Logo.BlobSize()),
                TPageLocation::FromPageIndex(1, globs[1].Logo.BlobSize()),
                TPageLocation::FromPageIndex(2, globs[2].Logo.BlobSize()),
                TPageLocation::FromPageIndex(3, globs[3].Logo.BlobSize()),
                TPageLocation::FromPageIndex(4, globs[4].Logo.BlobSize()),
                TPageLocation::FromPageIndex(5, globs[5].Logo.BlobSize()),
            };
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

    Y_UNIT_TEST(TPageLocation_Basics)
    {
        using NTable::NPage::TPageLocation;
        using NTable::NPage::TPageOffset;

        // Default construction yields Max
        TPageLocation def;
        UNIT_ASSERT(!def);
        UNIT_ASSERT(def.Offset == TPageOffset::Max());

        // Parameterized construction
        auto loc = TPageLocation::FromByteOffset(42, 1024);
        UNIT_ASSERT(bool(loc));
        UNIT_ASSERT(loc.Offset == TPageOffset::FromByteOffset(42));
        UNIT_ASSERT(loc.Size == 1024);

        // Equality (compares offset, verifies size)
        auto same = TPageLocation::FromByteOffset(42, 1024);
        auto diff = TPageLocation::FromByteOffset(99, 1024);
        UNIT_ASSERT(loc == same);
        UNIT_ASSERT(loc != diff);

        // Max factory
        auto maxed = TPageLocation::Max();
        UNIT_ASSERT(!maxed);
        UNIT_ASSERT(maxed.Offset.IsMax());
    }

    Y_UNIT_TEST(GetLocation)
    {
        const TMeta meta(MakeMeta(), 0);
        // Pages: { 50, 30, 80, 60, 40, 60, 70, 380, 10 }
        //
        // Expected offsets:
        //   0:   0,  1:  50,  2:  80,  3: 160,  4: 220,
        //   5: 260,  6: 320,  7: 390,  8: 770

        const std::pair<ui64, ui32> expected[9] = {
            {   0, 50 },
            {  50, 30 },
            {  80, 80 },
            { 160, 60 },
            { 220, 40 },
            { 260, 60 },
            { 320, 70 },
            { 390, 380 },
            { 770, 10 },
        };

        for (ui32 i = 0; i < 9; i++) {
            auto loc = meta.GetLocation(i);
            UNIT_ASSERT(loc);
            UNIT_ASSERT_VALUES_EQUAL(loc.GetByteOffset(), expected[i].first);
            UNIT_ASSERT_VALUES_EQUAL(loc.Size,   expected[i].second);
        }
    }

    Y_UNIT_TEST(Bounds_via_Location)
    {
        const TMeta meta(MakeMeta(), 0);

        for (ui32 i = 0; i < 9; i++) {
            auto loc = meta.GetLocation(i);

            // Bounds(TPageLocation) must match Bounds(ui32)
            auto boundsViaId   = meta.Bounds(i);
            auto boundsViaLoc  = meta.Bounds(loc);

            UNIT_ASSERT(boundsViaLoc.Lo.Blob == boundsViaId.Lo.Blob);
            UNIT_ASSERT(boundsViaLoc.Lo.Skip == boundsViaId.Lo.Skip);
            UNIT_ASSERT(boundsViaLoc.Up.Blob == boundsViaId.Up.Blob);
            UNIT_ASSERT(boundsViaLoc.Up.Skip == boundsViaId.Up.Skip);
            UNIT_ASSERT(boundsViaLoc.Bytes == boundsViaId.Bytes);
        }
    }

    Y_UNIT_TEST(Verify_via_Location)
    {
        auto metaBlob = MakeMeta();
        TLargeGlobId largeGlobId(0, TLogoBlobID(10, 20, 30, 1, metaBlob.size(), 0), metaBlob.size());
        const TPageCollection pageCollection(largeGlobId, metaBlob);
        const TMeta meta(metaBlob, 0);

        for (ui32 i = 0; i < 9; i++) {
            auto loc = meta.GetLocation(i);

            // Correct data: verify through IPageCollection::Verify
            TString data(loc.Size, '9');
            UNIT_ASSERT(pageCollection.Verify(loc, TArrayRef<const char>(data.data(), data.size())));

            // Wrong size → false
            TString wrongSize(loc.Size + 1, '9');
            UNIT_ASSERT(!pageCollection.Verify(loc, TArrayRef<const char>(wrongSize.data(), wrongSize.size())));

            // Wrong data → false
            if (loc.Size > 0) {
                TString wrongData(loc.Size, 'X');
                UNIT_ASSERT(!pageCollection.Verify(loc, TArrayRef<const char>(wrongData.data(), wrongData.size())));
            }
        }
    }

    Y_UNIT_TEST(TPageCollection_GetLocation)
    {
        auto metaBlob = MakeMeta();
        // Build a TLargeGlobId with Bytes matching the blob size
        TLargeGlobId largeGlobId(0, TLogoBlobID(10, 20, 30, 1, metaBlob.size(), 0), metaBlob.size());
        TPageCollection pageCollection(largeGlobId, metaBlob);

        UNIT_ASSERT_VALUES_EQUAL(pageCollection.Total(), 9);

        // TPageCollection::GetLocation must delegate to TMeta::GetLocation
        TMeta rawMeta(MakeMeta(), 0);
        for (ui32 i = 0; i < 9; i++) {
            auto viaPC = pageCollection.GetLocation(i);
            auto viaRaw = rawMeta.GetLocation(i);
            UNIT_ASSERT(viaPC == viaRaw);
            UNIT_ASSERT_VALUES_EQUAL(viaPC.Offset, viaRaw.Offset);
            UNIT_ASSERT_VALUES_EQUAL(viaPC.Size,   viaRaw.Size);
        }
    }

    Y_UNIT_TEST(TExtBlobs_GetLocation)
    {
        // Build 5 globs with varying sizes
        const std::array<NPageCollection::TGlobId, 5> globs = {{
            { TLogoBlobID(1, 2, 3,  1, 100, 0), 7 },
            { TLogoBlobID(1, 2, 3,  7,  50, 1), 7 },
            { TLogoBlobID(1, 2, 3, 13, 200, 2), 7 },
            { TLogoBlobID(1, 2, 3, 33,  80, 3), 4 },
            { TLogoBlobID(1, 2, 3, 57, 120, 4), 7 },
        }};

        NTable::NPage::TExtBlobsWriter writer;
        for (auto &g : globs) {
            writer.Put(g);
        }

        auto blob = writer.Make();
        NTable::NPage::TExtBlobs extBlobs(blob, TLogoBlobID(1, 2, 3, 1, blob.size(), 0));

        // For independently addressed blobs, Offset equals pageId (index in Array)
        //   0:   0,  1:   1,  2:   2,  3:   3,  4:   4
        const ui64 expectedIndex[5] = { 0, 1, 2, 3, 4 };
        const ui32 expectedSize[5]   = { 100, 50, 200, 80, 120 };

        UNIT_ASSERT_VALUES_EQUAL(extBlobs.Total(), 5);

        for (ui32 i = 0; i < 5; i++) {
            auto loc = extBlobs.GetLocation(i);
            UNIT_ASSERT(bool(loc));
            UNIT_ASSERT_VALUES_EQUAL(loc.GetPageIndex(), expectedIndex[i]);
            UNIT_ASSERT_VALUES_EQUAL(loc.Size,   expectedSize[i]);
        }
    }

    Y_UNIT_TEST(IPageCollection_GetLocation_Polymorphic)
    {
        // Verify pure virtual dispatch works for both TPageCollection and TExtBlobs

        // --- TPageCollection via IPageCollection& ---
        auto metaBlob = MakeMeta();
        TLargeGlobId largeGlobId(0, TLogoBlobID(10, 20, 30, 1, metaBlob.size(), 0), metaBlob.size());
        TPageCollection pageCollection(largeGlobId, metaBlob);

        const IPageCollection& pcRef = pageCollection;
        TMeta rawMeta(MakeMeta(), 0);
        for (ui32 i = 0; i < 9; i++) {
            auto loc = pcRef.GetLocation(i);
            auto expected = rawMeta.GetLocation(i);
            UNIT_ASSERT(loc == expected);
        }

        // --- TExtBlobs via IPageCollection& ---
        const std::array<NPageCollection::TGlobId, 3> globs = {{
            { TLogoBlobID(1, 2, 3,  1, 60, 0), 7 },
            { TLogoBlobID(1, 2, 3,  7, 40, 1), 7 },
            { TLogoBlobID(1, 2, 3, 13, 90, 2), 7 },
        }};

        NTable::NPage::TExtBlobsWriter writer;
        for (auto &g : globs) writer.Put(g);
        auto blob = writer.Make();
        NTable::NPage::TExtBlobs extBlobs(blob, TLogoBlobID(1, 2, 3, 1, blob.size(), 0));

        const IPageCollection& ebRef = extBlobs;
        for (ui32 i = 0; i < 3; i++) {
            auto loc = ebRef.GetLocation(i);
            UNIT_ASSERT(bool(loc));
            // For independently addressed blobs, Offset equals pageId (index in Array)
            UNIT_ASSERT_VALUES_EQUAL(loc.GetPageIndex(), i);
            UNIT_ASSERT_VALUES_EQUAL(loc.Size, globs[i].Bytes());
        }
    }

    Y_UNIT_TEST(ByteOffset_TryGetPage)
    {
        using NTable::IPages;
        using NTable::NPage::TPageLocation;
        using NTable::NPage::TPageOffset;
        using NTable::ELargeObj;

        // A minimal IPages mock that stores data keyed by byte-offset TPageOffset
        // — no page-index conversion, no CRC32 check (delegated to Verify)
        struct TByteOffsetStore : public IPages {
            THashMap<TPageOffset, TSharedData> Map;

            void Add(TPageLocation loc, TSharedData data)
            {
                Map[loc.Offset] = std::move(data);
            }

            const TSharedData* TryGetPage(const TPart*, TPageLocation location, TGroupId) override
            {
                auto it = Map.find(location.Offset);
                return it != Map.end() ? &it->second : nullptr;
            }

            TResult Locate(const TMemTable*, ui64, ui32) override
            {
                return {false, nullptr};
            }

            TResult Locate(const TPart*, ui64, ELargeObj) override
            {
                return {false, nullptr};
            }
        };

        const TMeta meta(MakeMeta(), 0);

        // Build TPageCollection — production path for IPageCollection::Verify
        auto metaBlob = MakeMeta();
        TLargeGlobId largeGlobId(0, TLogoBlobID(10, 20, 30, 1, metaBlob.size(), 0), metaBlob.size());
        const TPageCollection pageCollection(largeGlobId, metaBlob);

        TByteOffsetStore store;

        // Phase 1: fill with same '9'-filled data as MakeMeta — CRC32 matches TMeta
        for (ui32 i = 0; i < 9; i++) {
            auto loc = meta.GetLocation(i);
            UNIT_ASSERT(loc.Offset.IsByteOffset());   // real byte offset, not page index
            UNIT_ASSERT(loc.Crc32 != 0);              // real CRC32 from TMeta

            TString data(loc.Size, '9');              // same fill as MakeMeta()
            // Confirm our data produces the same CRC32 as what TMeta recorded
            UNIT_ASSERT_VALUES_EQUAL(Crc32c(data.data(), data.size()), loc.Crc32);

            store.Add(loc, TSharedData::Copy(data.data(), data.size()));
        }

        // Phase 2: retrieve each page and verify CRC32 via IPageCollection::Verify
        // — the same production path as flat_bio_actor.cpp
        for (ui32 i = 0; i < 9; i++) {
            auto loc = meta.GetLocation(i);
            const TSharedData* page = store.TryGetPage(nullptr, loc, {});
            UNIT_ASSERT(page);
            UNIT_ASSERT_VALUES_EQUAL(page->size(), loc.Size);
            UNIT_ASSERT(pageCollection.Verify(loc,
                TArrayRef<const char>(page->data(), page->size())));
        }

        // Phase 3: unknown byte-offset location → nullptr
        auto badLoc = TPageLocation::FromByteOffset(999999, 10);
        UNIT_ASSERT(!store.TryGetPage(nullptr, badLoc, {}));

        // Phase 4: corrupt data at a known offset — TryGetPage returns it,
        // but IPageCollection::Verify rejects it (CRC32 mismatch)
        auto loc0 = meta.GetLocation(0);
        TString badData(loc0.Size, 'X');               // different content → different CRC32
        UNIT_ASSERT(Crc32c(badData.data(), badData.size()) != loc0.Crc32);
        store.Add(loc0, TSharedData::Copy(badData.data(), badData.size()));
        {
            const TSharedData* page = store.TryGetPage(nullptr, loc0, {});
            UNIT_ASSERT(page);                          // lookup succeeds (no CRC32 inside TryGetPage)
            UNIT_ASSERT(!pageCollection.Verify(loc0,    // but Verify catches the mismatch
                TArrayRef<const char>(page->data(), page->size())));
        }
    }
}

}
}
