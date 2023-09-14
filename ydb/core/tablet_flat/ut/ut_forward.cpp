#include <ydb/core/tablet_flat/util_basics.h>
#include <ydb/core/tablet_flat/flat_page_other.h>
#include <ydb/core/tablet_flat/flat_page_frames.h>
#include <ydb/core/tablet_flat/flat_page_blobs.h>
#include <ydb/core/tablet_flat/flat_fwd_blobs.h>
#include <ydb/core/tablet_flat/flat_fwd_sieve.h>
#include <ydb/core/tablet_flat/test/libs/table/test_steps.h>
#include <library/cpp/testing/unittest/registar.h>
#include <util/random/shuffle.h>
#include <util/random/mersenne.h>

namespace NKikimr {
namespace NTable {

namespace {

    struct TWrap : public NTest::TSteps<TWrap>, protected NFwd::IPageLoadingQueue {
        using TFrames = NPage::TFrames;

        TWrap(TIntrusiveConstPtr<TFrames> frames, TIntrusiveConstPtr<TSlices> run, ui32 edge, ui64 aLo = 999, ui64 aHi = 999)
            : Large(std::move(frames))
            , Run(std::move(run))
            , Edge(edge)
            , AheadLo(aLo)
            , AheadHi(aHi)
        {
            TVector<ui32> edges(Large->Stats().Tags.size(), edge);

            Cache = new NFwd::TBlobs(Large, Run, edges, true);
        }

        TWrap(TIntrusiveConstPtr<TFrames> frames, ui32 edge, ui64 aLo = 999, ui64 aHi = 999)
            : TWrap(std::move(frames), TSlices::All(), edge, aLo, aHi)
        {
        }

        ui64 AddToQueue(ui32 page, EPage) noexcept override
        {
            Pages.push_back(page);

            return Large->Relation(page).Size;
        }

        TDeque<TScreen::THole> Trace() noexcept
        {
            return dynamic_cast<NFwd::TBlobs&>(*Cache).Traced();
        }

        TWrap& Get(ui32 page, bool has, bool grow, bool need)
        {
            auto got = Cache->Handle(this, page, AheadLo);

            if (has != bool(got.Page) || grow != got.Grow || need != got.Need){
                Log()
                    << "Page " << page << " lookup got"
                    << " data="  << bool(got.Page) << "(" << has <<")"
                    << ", grow=" << got.Grow << "(" << grow << ")"
                    << ", need=" << got.Need << "(" << need << ")"
                    << Endl;

                UNIT_ASSERT(false);
            }

            Grow = Grow || got.Grow;

            return *this;
        }

        TWrap& Fill(ui32 least, ui32 most, std::initializer_list<ui16> tags)
        {
            if (std::exchange(Grow, false))
                Cache->Forward(this, AheadHi);

            TVector<NPageCollection::TLoadedPage> load;

            for (auto page: std::exchange(Pages, TDeque<ui32>{ })) {
                const auto &rel = Large->Relation(page);

                if (rel.Size >= Edge) {
                    Log()
                        << "Queued page " << page << ", " << rel.Size << "b"
                        << " above the edge " << Edge << "b" << Endl;

                    UNIT_ASSERT(false);
                }

                if (std::count(tags.begin(), tags.end(), rel.Tag) == 0) {
                    Log()
                        << "Page " << page << " has tag " << rel.Tag
                        << " out of allowed set" << Endl;

                    UNIT_ASSERT(false);
                }

                load.emplace_back(page, TSharedData::Copy(TString(rel.Size, 'x')));
            }

            if (load.size() < least || load.size() >= most) {
                Log()
                    << "Unexpected queued page count " << load.size()
                    << ", should be [" << least << ", " << most << ")"
                    << Endl;

                UNIT_ASSERT(false);
            }

            Shuffle(load.begin(), load.end(), Rnd);

            Cache->Apply(load);

            UNIT_ASSERT(Cache->Stat.Saved == Cache->Stat.Fetch);

            return *this;
        }

    public:
        const TIntrusiveConstPtr<TFrames> Large;
        const TIntrusiveConstPtr<TSlices> Run;
        const ui32 Edge = Max<ui32>();
        const ui64 AheadLo = 0;
        const ui64 AheadHi = Max<ui64>();

    private:
        bool Grow = false;
        TAutoPtr<NFwd::IPageLoadingLogic> Cache;
        TDeque<ui32> Pages;
        TMersenne<ui64> Rnd;
    };
}

Y_UNIT_TEST_SUITE(NFwd) {

    static TIntrusiveConstPtr<NPage::TFrames> Cook()
    {
        NPage::TFrameWriter writer(3);

        writer.Put(10, 0, 10);  /* 0: */
        writer.Put(10, 2, 20);
        writer.Put(13, 1, 10);
        writer.Put(15, 0, 50);
        writer.Put(15, 1, 55);
        writer.Put(15, 2, 61);  /* 5: */
        writer.Put(17, 2, 13);
        writer.Put(18, 2, 10);
        writer.Put(19, 2, 17);
        writer.Put(22, 2, 15);

        return new NPage::TFrames(writer.Make());
    }

    Y_UNIT_TEST(MemTableTest)
    {
        { /*_ Trivial test, should not produce empty frames */
            UNIT_ASSERT(!NPage::TFrameWriter().Make());
        }

        const auto frames = Cook();

        { /*_ Basic tests on aggregate values */
            const auto stat = frames->Stats();

            UNIT_ASSERT(stat.Items == 10);
            UNIT_ASSERT(stat.Rows == 7);
            UNIT_ASSERT(stat.Size == 261);
            UNIT_ASSERT(stat.Tags.at(0) == 2);
            UNIT_ASSERT(stat.Tags.at(1) == 2);
            UNIT_ASSERT(stat.Tags.at(2) == 6);
        }

        { /*_ Test on frame relation offsets */
            UNIT_ASSERT(frames->Relation(3).Refer == -3);
            UNIT_ASSERT(frames->Relation(4).Refer == 1);
            UNIT_ASSERT(frames->Relation(5).Refer == 2);
        }

        { /*_ Test row assigment in entries */
            UNIT_ASSERT(frames->Relation(3).Row == 15);
            UNIT_ASSERT(frames->Relation(4).Row == 15);
            UNIT_ASSERT(frames->Relation(5).Row == 15);
        }
    }

    Y_UNIT_TEST(Lower)
    {
        const auto frames = Cook();

        UNIT_ASSERT(frames->Lower(8, 0, Max<ui32>()) == 0);
        UNIT_ASSERT(frames->Lower(10, 0, Max<ui32>()) == 0);
        UNIT_ASSERT(frames->Lower(13, 0, Max<ui32>()) == 2);
        UNIT_ASSERT(frames->Lower(15, 0, Max<ui32>()) == 3);
        UNIT_ASSERT(frames->Lower(17, 0, Max<ui32>()) == 6);
        UNIT_ASSERT(frames->Lower(18, 0, Max<ui32>()) == 7);
        UNIT_ASSERT(frames->Lower(19, 0, Max<ui32>()) == 8);
        UNIT_ASSERT(frames->Lower(10, 1, Max<ui32>()) == 1);
        UNIT_ASSERT(frames->Lower(15, 1, 2) == 2);
        UNIT_ASSERT(frames->Lower(15, 0, 0) == 0);
        UNIT_ASSERT(frames->Lower(15, Max<ui32>(), Max<ui32>()) == 10);
    }

    Y_UNIT_TEST(Sieve)
    {
        NPage::TExtBlobsWriter out;

        std::array<NPageCollection::TGlobId, 6> globs = {{
            { TLogoBlobID(1, 2, 3,  1, 10, 0), 7 },
            { TLogoBlobID(1, 2, 3,  7, 12, 1), 7 },
            { TLogoBlobID(1, 2, 3, 13, 14, 2), 7 },
            { TLogoBlobID(1, 2, 3, 33, 16, 3), 4 },
            { TLogoBlobID(1, 2, 3, 57, 18, 4), 7 },
            { TLogoBlobID(1, 2, 3, 99, 20, 5), 7 },
        }};

        for (auto &one: globs) out.Put(one);

        const auto blobs = new NPage::TExtBlobs(out.Make(), { });

        NFwd::TSieve sieve{ blobs, nullptr, nullptr, {{ 1, 3}}};

        TVector<TLogoBlobID> logo;

        sieve.MaterializeTo(logo);

        UNIT_ASSERT(logo.size() == 4);
        UNIT_ASSERT(logo[0] == globs[0].Logo);
        UNIT_ASSERT(logo[1] == globs[3].Logo);
        UNIT_ASSERT(logo[3] == globs[5].Logo);
    }

    Y_UNIT_TEST(SieveFiltered)
    {
        NPage::TExtBlobsWriter out;

        std::array<NPageCollection::TGlobId, 6> globs = {{
            { TLogoBlobID(1, 2, 3,  1, 10, 0), 7 },
            { TLogoBlobID(1, 2, 3,  7, 12, 1), 7 },
            { TLogoBlobID(1, 2, 3, 13, 14, 2), 7 },
            { TLogoBlobID(1, 2, 3, 33, 16, 3), 4 },
            { TLogoBlobID(1, 2, 3, 57, 18, 4), 7 },
            { TLogoBlobID(1, 2, 3, 99, 20, 5), 7 },
        }};

        for (auto &one: globs) out.Put(one);

        const auto blobs = new NPage::TExtBlobs(out.Make(), { });

        TIntrusiveConstPtr<NPage::TFrames> frames;

        {
            NPage::TFrameWriter writer(3);
            writer.Put(10, 1, 10);
            writer.Put(15, 1, 12);
            writer.Put(20, 1, 14);
            writer.Put(25, 1, 16);
            writer.Put(30, 1, 18);
            writer.Put(35, 1, 20);
            frames = new NPage::TFrames(writer.Make());
        }

        // Construct a run with [13,27] and [33,35] ranges
        TIntrusivePtr<TSlices> run = new TSlices;
        {
            run->emplace_back(
                TSerializedCellVec(), // key not important
                TSerializedCellVec(), // key not important
                13,
                27,
                true,
                true);
            run->emplace_back(
                TSerializedCellVec(), // key not important
                TSerializedCellVec(), // key not important
                33,
                35,
                true,
                true);
        }

        NFwd::TSieve sieve{ blobs, frames, run, {{ 1, 3}}};

        TVector<TLogoBlobID> logo;

        sieve.MaterializeTo(logo);

        UNIT_ASSERT(logo.size() == 2);
        UNIT_ASSERT(logo[0] == globs[3].Logo);
        UNIT_ASSERT(logo[1] == globs[5].Logo);
    }

    Y_UNIT_TEST(Basics)
    {
        /*_ Check unorderd access over the same frame */
        TWrap(Cook(), 61).Get(1, false, true, true).Get(0, false, true, true);
        TWrap(Cook(), 61).Get(4, false, true, true).Get(3, false, true, true);

        /*_ Should not load page with size above the dge */
        TWrap(Cook(), 61).Get(5, false, true, false).Fill(4, 5, { 2 });

        /*_ Jump between frames with full cache flush   */
        TWrap(Cook(), 61, 1, 10)
            .To(30).Get(1, false, true, true).Fill(1, 2, { 2 })
            .To(31).Get(6, false, true, true).Fill(1, 2, { 2 });

        /*_ Long jump, from begin of incomplete frame */
        TWrap(Cook(), 61).Get(3, false, true, true).Get(7, false, true, true);
    }

    Y_UNIT_TEST(Simple)
    {
        TWrap(Cook(), 61, 999, 999)
        /*_ Get, load and reread the same blob  */
            .To(10).Get(1, false, true, true)
            .To(11).Fill(5, 6, { 2 })
            .To(12).Get(1, true, false, true)
        /*_ Try blob above the materialize edge */
            .To(13).Get(5, false, false, false)
        /*_ Try next blob with the same tag     */
            .To(14).Get(6, true, false, true);
    }

    Y_UNIT_TEST(Shuffle)
    {
        TWrap(Cook(), 61, 999, 999)
        /*_ Touch two columns on the same frame */
            .To(10).Get(1, false, true, true)
            .To(11).Get(0, false, true, true)
            .To(12).Fill(7, 8, { 0, 2 })
            .To(13).Get(1, true, false, true)
            .To(14).Get(0, true, false, true)
            .To(15).Fill(0, 1, { })
        /*_ Then touch another tag on next frame*/
            .To(20).Get(2, false, true, true)
            .To(21).Fill(2, 3, { 1 });
    }

    Y_UNIT_TEST(Grow)
    {
        TWrap(Cook(), 55, 15, 30)
            .To(10).Get(1, false, true, true).Fill(2, 3, { 2 })
            .To(12).Get(6, true, true, true).Fill(2, 3, { 2 })
            .To(14).Get(8, true, false, true).Fill(0, 1, { 2 })
            .To(16).Get(9, false, true, true).Fill(1, 2, { 2 })
            .To(17).Get(9, true, false, true).Fill(0, 1, { 2 });
    }

    Y_UNIT_TEST(Trace)
    {
        TWrap wrap(Cook(), 15, 999, 999);

        wrap
            .To(1).Get(1, false, true, false)
            .To(2).Get(2, false, true, true)
            .To(3).Get(4, false, true, false)
            .To(4).Get(5, false, true, false)
            .To(5).Get(7, false, true, true)
            .To(6).Get(9, false, true, false)
            .Fill(2, 3, { 1, 2 });

        const auto trace = wrap.To(8).Trace();

        UNIT_ASSERT(trace.size() == 3);
        UNIT_ASSERT(trace[0] == TScreen::THole(1, 2));
        UNIT_ASSERT(trace[1] == TScreen::THole(4, 6));
        UNIT_ASSERT(trace[2] == TScreen::THole(9, 10));
    }

    Y_UNIT_TEST(Filtered)
    {
        TIntrusivePtr<TSlices> run = new TSlices;
        run->emplace_back(TSlice({ }, { }, 0, 15, true, false));
        run->emplace_back(TSlice({ }, { }, 18, 22, true, true));
        TWrap wrap(Cook(), run, Max<ui32>(), 999, 999);

        wrap
            .To(1).Get(1, false, true, true).Fill(4, 5, { 2 })
            .To(2).Get(2, false, true, true).Fill(1, 2, { 1 })
            .To(3).Get(9, true, false, true).Fill(0, 1, { });

        const auto trace = wrap.To(4).Trace();

        UNIT_ASSERT(trace.size() == 0);
    }

}

}
}
