#include "flat_fwd_cache.h"
#include "flat_page_conf.h"
#include "test/libs/rows/layout.h"
#include "test/libs/table/test_part.h"
#include "test/libs/table/test_writer.h"
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
    using namespace NTest;

    struct TFramesWrap : public NTest::TSteps<TFramesWrap>, protected NFwd::IPageLoadingQueue {
        using TFrames = NPage::TFrames;

        TFramesWrap(TIntrusiveConstPtr<TFrames> frames, TIntrusiveConstPtr<TSlices> run, ui32 edge, ui64 aLo = 999, ui64 aHi = 999)
            : Large(std::move(frames))
            , Run(std::move(run))
            , Edge(edge)
            , AheadLo(aLo)
            , AheadHi(aHi)
        {
            TVector<ui32> edges(Large->Stats().Tags.size(), edge);

            Cache = new NFwd::TBlobs(Large, Run, edges, true);
        }

        TFramesWrap(TIntrusiveConstPtr<TFrames> frames, ui32 edge, ui64 aLo = 999, ui64 aHi = 999)
            : TFramesWrap(std::move(frames), TSlices::All(), edge, aLo, aHi)
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

        TFramesWrap& Get(ui32 page, bool has, bool grow, bool need)
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

        TFramesWrap& Fill(ui32 least, ui32 most, std::initializer_list<ui16> tags)
        {
            if (std::exchange(Grow, false)) {
                Cache->Forward(this, AheadHi);
            }

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

    struct TPagesWrap : public NTest::TSteps<TPagesWrap>, protected NFwd::IPageLoadingQueue {
        using TFrames = NPage::TFrames;

        struct TTouchEnv : public NTest::TTestEnv {
            const TSharedData* TryGetPage(const TPart *part, TPageId pageId, TGroupId groupId) override
            {
                Y_ABORT_UNLESS(part->GetPageType(pageId, groupId) != EPage::DataPage, "Only index pages are allowed");
                Y_ABORT_UNLESS(groupId.IsMain());

                Touched.insert(pageId);
                if (!Faulty || Loaded.contains(pageId)) {
                    return NTest::TTestEnv::TryGetPage(part, pageId, groupId);
                }
                return nullptr;
            }

            bool Faulty = false;
            THashSet<TPageId> Loaded;
            TSet<TPageId> Touched;
        };

        TPagesWrap(const TIntrusiveConstPtr<TPartStore> part, TIntrusiveConstPtr<TSlices> run, ui64 aLo, ui64 aHi)
            : Part(std::move(part))
            , Run(std::move(run))
            , AheadLo(aLo)
            , AheadHi(aHi)
        {
            Cache = new NFwd::TCache(Part.Get(), &Env, { }, Run);
        }

        TPagesWrap(TIntrusiveConstPtr<TPartStore> part, ui64 aLo, ui64 aHi)
            : TPagesWrap(std::move(part), nullptr, aLo, aHi)
        {
        }

        ui64 AddToQueue(ui32 pageId, EPage) noexcept override
        {
            Queue.push_back(pageId);

            return Part->GetPageSize(pageId, { });
        }

        TPagesWrap& Get(ui32 pageIndex, bool has, bool grow, bool need, NFwd::TStat stat)
        {
            auto pageId = IndexTools::GetPageId(*Part, pageIndex);
            auto got = Cache->Handle(this, pageId, AheadLo);

            if (has != bool(got.Page) || grow != got.Grow || need != got.Need){
                Log()
                    << "Page " << pageId << " lookup got"
                    << " data="  << bool(got.Page) << "(" << has <<")"
                    << ", grow=" << got.Grow << "(" << grow << ")"
                    << ", need=" << got.Need << "(" << need << ")"
                    << Endl;

                UNIT_ASSERT(false);
            }

            Grow = Grow || got.Grow;

            UNIT_ASSERT_VALUES_EQUAL_C(RescaleStat(Cache->Stat), stat, CurrentStepStr());

            return *this;
        }

        TPagesWrap& Fill(const TVector<ui32>& pageIndexes, NFwd::TStat stat)
        {
            if (std::exchange(Grow, false)) {
                Cache->Forward(this, AheadHi);
            }

            TVector<TPageId> pageIds(::Reserve(pageIndexes.size()));
            for (auto pageIndex : pageIndexes) {
                pageIds.push_back(IndexTools::GetPageId(*Part, pageIndex));
            }
            UNIT_ASSERT_VALUES_EQUAL_C(TVector<TPageId>(Queue.begin(), Queue.end()), pageIds, CurrentStepStr());

            TVector<NPageCollection::TLoadedPage> load;
            NTest::TTestEnv testEnv;
            for (auto pageId: std::exchange(Queue, TDeque<ui32>{ })) {
                load.emplace_back(pageId, *testEnv.TryGetPage(Part.Get(), pageId, { }));
            }

            Shuffle(load.begin(), load.end(), Rnd);

            Cache->Apply(load);

            UNIT_ASSERT_VALUES_EQUAL_C(RescaleStat(Cache->Stat), stat, CurrentStepStr());

            return *this;
        }

        TPagesWrap& Forward(const TVector<ui32>& pageIndexes, NFwd::TStat stat)
        {
            if (std::exchange(Grow, false)) {
                Cache->Forward(this, AheadHi);
            }

            TVector<TPageId> pageIds(::Reserve(pageIndexes.size()));
            for (auto pageIndex : pageIndexes) {
                pageIds.push_back(IndexTools::GetPageId(*Part, pageIndex));
            }
            UNIT_ASSERT_VALUES_EQUAL_C(TVector<TPageId>(Queue.begin(), Queue.end()), pageIds, CurrentStepStr());
            
            UNIT_ASSERT_VALUES_EQUAL_C(RescaleStat(Cache->Stat), stat, CurrentStepStr());

            return *this;
        }

        void UseFaultyEnv(bool value = true) 
        {
            Env.Faulty = value;
        }

        void LoadTouched()
        {
            Env.Loaded.insert(Env.Touched.begin(), Env.Touched.end());
            Env.Touched.clear();
        }

    public:
        const TIntrusiveConstPtr<TPartStore> Part;
        TTouchEnv Env;
        const TIntrusiveConstPtr<TSlices> Run;
        const ui64 AheadLo;
        const ui64 AheadHi;

    private:
        NFwd::TStat RescaleStat(NFwd::TStat stat) {
            // 50 bytes per page
            stat.Fetch /= 50;
            stat.Saved /= 50;
            stat.Usage /= 50;
            stat.After /= 50;
            stat.Before /= 50;
            return stat;
        }

        bool Grow = false;
        TAutoPtr<NFwd::IPageLoadingLogic> Cache;
        TDeque<TPageId> Queue;
        TMersenne<ui64> Rnd;
    };
}

Y_UNIT_TEST_SUITE(NFwd) {
    using namespace NFwd;
    using namespace NTest;

    static TIntrusiveConstPtr<NPage::TFrames> CookFrames()
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

    TPartEggs CookPart() {
        NPage::TConf conf;

        conf.WriteBTreeIndex = true;
        conf.Group(0).PageRows = 2;
        conf.Group(0).BTreeIndexNodeKeysMin = conf.Group(0).BTreeIndexNodeKeysMax = 2;

        TLayoutCook lay;

        lay
            .Col(0, 0,  NScheme::NTypeIds::Uint32)
            .Col(0, 1,  NScheme::NTypeIds::Uint32)
            .Key({0});
        
        TPartCook cook(lay, conf);

        for (ui32 i : xrange<ui32>(0, 40)) {
            cook.Add(*TSchemedCookRow(*lay).Col(i, i * 100));
        }
        
        TPartEggs eggs = cook.Finish();

        Cerr << DumpPart(*eggs.Lone(), 3) << Endl;

        return eggs;
    }

    Y_UNIT_TEST(MemTableTest)
    {
        { /*_ Trivial test, should not produce empty frames */
            UNIT_ASSERT(!NPage::TFrameWriter().Make());
        }

        const auto frames = CookFrames();

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
        const auto frames = CookFrames();

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

    Y_UNIT_TEST(Frames_Basics)
    {
        /*_ Check unordered access over the same frame */
        TFramesWrap(CookFrames(), 61).Get(1, false, true, true).Get(0, false, true, true);
        TFramesWrap(CookFrames(), 61).Get(4, false, true, true).Get(3, false, true, true);

        /*_ Should not load page with size above the edge */
        TFramesWrap(CookFrames(), 61).Get(5, false, true, false).Fill(4, 5, { 2 });

        /*_ Jump between frames with full cache flush   */
        TFramesWrap(CookFrames(), 61, 1, 10)
            .To(30).Get(1, false, true, true).Fill(1, 2, { 2 })
            .To(31).Get(6, false, true, true).Fill(1, 2, { 2 });

        /*_ Long jump, from begin of incomplete frame */
        TFramesWrap(CookFrames(), 61).Get(3, false, true, true).Get(7, false, true, true);
    }

    Y_UNIT_TEST(Frames_Simple)
    {
        TFramesWrap(CookFrames(), 61, 999, 999)
        /*_ Get, load and reread the same blob  */
            .To(10).Get(1, false, true, true)
            .To(11).Fill(5, 6, { 2 })
            .To(12).Get(1, true, false, true)
        /*_ Try blob above the materialize edge */
            .To(13).Get(5, false, false, false)
        /*_ Try next blob with the same tag     */
            .To(14).Get(6, true, false, true);
    }

    Y_UNIT_TEST(Frames_Shuffle)
    {
        TFramesWrap(CookFrames(), 61, 999, 999)
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

    Y_UNIT_TEST(Frames_Grow)
    {
        TFramesWrap(CookFrames(), 55, 15, 30)
            .To(10).Get(1, false, true, true).Fill(2, 3, { 2 })
            .To(12).Get(6, true, true, true).Fill(2, 3, { 2 })
            .To(14).Get(8, true, false, true).Fill(0, 1, { 2 })
            .To(16).Get(9, false, true, true).Fill(1, 2, { 2 })
            .To(17).Get(9, true, false, true).Fill(0, 1, { 2 });
    }

    Y_UNIT_TEST(Frames_Trace)
    {
        TFramesWrap wrap(CookFrames(), 15, 999, 999);

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

    Y_UNIT_TEST(Frames_Filtered)
    {
        TIntrusivePtr<TSlices> run = new TSlices;
        run->emplace_back(TSlice({ }, { }, 0, 15, true, false));
        run->emplace_back(TSlice({ }, { }, 18, 22, true, true));
        TFramesWrap wrap(CookFrames(), run, Max<ui32>(), 999, 999);

        wrap
            .To(1).Get(1, false, true, true).Fill(4, 5, { 2 })
            .To(2).Get(2, false, true, true).Fill(1, 2, { 1 })
            .To(3).Get(9, true, false, true).Fill(0, 1, { });

        const auto trace = wrap.To(4).Trace();

        UNIT_ASSERT(trace.size() == 0);
    }

    Y_UNIT_TEST(Pages_Basics)
    {
        // 20 pages, 50 bytes each
        const auto eggs = CookPart();

        TPagesWrap wrap(eggs.Lone(), 200, 350);
        
        wrap.To(0).Get(0, false, true, true, 
            {1, 0, 1, 0, 0});
        wrap.To(1).Fill({0, 1, 2, 3, 4, 5, 6}, 
            {7, 7, 1, 0, 0});
        wrap.To(2).Get(0, true, false, true, 
            {7, 7, 1, 0, 0});
        
        // page 0 goes to trace:
        wrap.To(3).Get(1, true, false, true, 
            {7, 7, 2, 0, 0});

        // page 1 goes to trace, pages 2, 3 dropped as unused:
        wrap.To(4).Get(4, true, false, true, 
            {7, 7, 3, 2, 0});
        
        // have less than 4 pages, grow:
        wrap.To(5).Get(5, true, true, true, 
            {7, 7, 4, 2, 0});
        wrap.To(6).Fill({7, 8, 9}, 
            {10, 10, 4, 2, 0});
    }

    Y_UNIT_TEST(Pages_Twice)
    {
        // 20 pages, 50 bytes each
        const auto eggs = CookPart();

        TPagesWrap wrap(eggs.Lone(), 200, 350);
        
        wrap.To(0).Get(5, false, true, true, 
            {1, 0, 1, 0, 0});
        wrap.To(1).Get(5, false, true, true, 
            {1, 0, 1, 0, 0});
        wrap.To(2).Fill({5, 6, 7, 8, 9, 10, 11}, 
            {7, 7, 1, 0, 0});
        
        wrap.To(3).Get(5, true, false, true, 
            {7, 7, 1, 0, 0});
        wrap.To(4).Get(5, true, false, true, 
            {7, 7, 1, 0, 0});

        wrap.To(5).Get(6, true, false, true, 
            {7, 7, 2, 0, 0});
        wrap.To(6).Get(6, true, false, true, 
            {7, 7, 2, 0, 0});
    }

    Y_UNIT_TEST(Pages_Jump_Done)
    {
        // 20 pages, 50 bytes each
        const auto eggs = CookPart();

        TPagesWrap wrap(eggs.Lone(), 200, 350);
        
        wrap.To(0).Get(0, false, true, true, 
            {1, 0, 1, 0, 0});
        wrap.To(1).Fill({0, 1, 2, 3, 4, 5, 6}, 
            {7, 7, 1, 0, 0});

        wrap.To(2).Get(10, false, true, true, 
            {8, 7, 2, 6, 0});
        wrap.To(3).Fill({10, 11, 12, 13, 14, 15}, 
            {13, 13, 2, 6, 0});
    }

    Y_UNIT_TEST(Pages_Jump_Keep)
    {
        // 20 pages, 50 bytes each
        const auto eggs = CookPart();

        TPagesWrap wrap(eggs.Lone(), 200, 350);
        
        wrap.To(0).Get(0, false, true, true, 
            {1, 0, 1, 0, 0});

        // page 0 drops keep
        wrap.To(1).Get(10, false, true, true, 
            {2, 0, 2, 0, 1});

        wrap.To(2).Fill({0, 10, 11, 12, 13, 14, 15}, 
            {7, 7, 2, 0, 1});
    }

    Y_UNIT_TEST(Pages_Jump_Wait)
    {
        // 20 pages, 50 bytes each
        const auto eggs = CookPart();

        TPagesWrap wrap(eggs.Lone(), 200, 350);
        
        wrap.To(0).Get(0, false, true, true, 
            {1, 0, 1, 0, 0});
        wrap.To(1).Forward({0, 1, 2, 3, 4, 5, 6}, 
            {7, 0, 1, 0, 0});

        // page 0 drops keep, pages 1 - 6 drops wait
        wrap.To(2).Get(10, false, false, true, 
            {8, 0, 2, 0, 7});

        wrap.To(3).Fill({0, 1, 2, 3, 4, 5, 6, 10}, 
            {8, 8, 2, 0, 7});

        // ready to grow again:
        wrap.To(4).Get(10, true, true, true, 
            {8, 8, 2, 0, 7});
        wrap.To(5).Fill({11, 12, 13, 14, 15, 16}, 
            {14, 14, 2, 0, 7});
    }

    Y_UNIT_TEST(Pages_Trace)
    {
        // 20 pages, 50 bytes each
        const auto eggs = CookPart();

        TPagesWrap wrap(eggs.Lone(), 200, 350);
        
        wrap.To(0).Get(0, false, true, true, 
            {1, 0, 1, 0, 0});
        wrap.To(1).Fill({0, 1, 2, 3, 4, 5, 6}, 
            {7, 7, 1, 0, 0});
        
        // page 0 goes to trace:
        wrap.To(2).Get(2, true, false, true, 
            {7, 7, 2, 1, 0});

        // page 2 goes to trace, page 1 drops:
        wrap.To(3).Get(3, true, false, true, 
            {7, 7, 3, 1, 0});
        
        // trace: page 0, page 2:
        wrap.To(4).Get(0, true, false, true, 
            {7, 7, 3, 1, 0});
        wrap.To(5).Get(2, true, false, true, 
            {7, 7, 3, 1, 0});

        // page 3 goes to trace:
        wrap.To(6).Get(4, true, false, true, 
            {7, 7, 4, 1, 0});

        // trace: page 2, page 3:
        wrap.To(7).Get(2, true, false, true, 
            {7, 7, 4, 1, 0});
        wrap.To(8).Get(3, true, false, true, 
            {7, 7, 4, 1, 0});
    }

    Y_UNIT_TEST(Pages_End)
    {
        // 20 pages, 50 bytes each
        const auto eggs = CookPart();

        TPagesWrap wrap(eggs.Lone(), 200, 350);
        
        wrap.To(0).Get(17, false, true, true, 
            {1, 0, 1, 0, 0});
        wrap.To(1).Fill({17, 18, 19}, 
            {3, 3, 1, 0, 0});
        wrap.To(2).Get(17, true, false, true, 
            {3, 3, 1, 0, 0});
        wrap.To(3).Get(18, true, false, true, 
            {3, 3, 2, 0, 0});
        wrap.To(4).Get(19, true, false, true, 
            {3, 3, 3, 0, 0});
    }

    Y_UNIT_TEST(Pages_Slices)
    {
        // 20 pages, 50 bytes each
        const auto eggs = CookPart();

        TIntrusivePtr<TSlices> slices = new TSlices;
        // pages 5 - 7
        slices->emplace_back(TSlice({ }, { }, 10, 16, true, false));
        // pages 10 - 11
        slices->emplace_back(TSlice({ }, { }, 20, 23, true, true));

        TPagesWrap wrap(eggs.Lone(), slices, 1000, 1000);
        
        wrap.To(0).Get(5, false, true, true, 
            {1, 0, 1, 0, 0});
        wrap.To(1).Fill({5, 6, 7, 8, 9, 10, 11}, 
            {7, 7, 1, 0, 0});
        wrap.To(2).Get(10, true, false, true, 
            {7, 7, 2, 4, 0});
        wrap.To(3).Get(11, true, false, true, 
            {7, 7, 3, 4, 0});
    }

    /* B-Tree index:
        {
            {
                {0, 1, 2},
                {3, 4, 5},
                {6, 7, 8}
            },
            {
                {9, 10, 11},
                {12, 13, 14},
                {15, 16, 17, 18, 19}
            }
        }
    */

    Y_UNIT_TEST(Pages_PageFaults_SyncIndex_Start)
    {
        // 20 pages, 50 bytes each
        const auto eggs = CookPart();

        TPagesWrap wrap(eggs.Lone(), 200, 350);
        wrap.UseFaultyEnv();
        
        for (ui32 attempt : xrange(3)) { // 3-leveled B-Tree
            wrap.To(attempt).Get(1, false, false, true, 
                {0, 0, 0, 0, 0});
            wrap.LoadTouched();
        }
        
        wrap.To(3).Get(1, false, true, true, 
            {1, 0, 1, 0, 0});
    }

    Y_UNIT_TEST(Pages_PageFaults_SyncIndex_Next)
    {
        // 20 pages, 50 bytes each
        const auto eggs = CookPart();

        TPagesWrap wrap(eggs.Lone(), 200, 200);
        wrap.UseFaultyEnv();
        
        // Seek(0):
        for (ui32 attempt : xrange(3)) { // 3-leveled B-Tree
            wrap.To(attempt).Get(6, false, false, true, 
                {0, 0, 0, 0, 0});
            wrap.LoadTouched();
        }

        // Next(4):
        wrap.To(3).Get(4, false, false, true, 
            {0, 0, 0, 0, 0});
        wrap.LoadTouched();
        wrap.To(4).Get(4, false, true, true, 
            {1, 0, 1, 0, 0});
    
        wrap.To(6).Fill({4, 5},
            {2, 2, 1, 0, 0});

        // set Grow = true:
        wrap.To(7).Get(4, true, true, true, 
            {2, 2, 1, 0, 0});

        // load {6, 7, 8} node:
        wrap.To(8).Fill({},
            {2, 2, 1, 0, 0});
        wrap.LoadTouched();

        // set Grow = true:
        wrap.To(9).Get(4, true, true, true, 
            {2, 2, 1, 0, 0});

        wrap.To(10).Fill({6, 7},
            {4, 4, 1, 0, 0});
    }

    Y_UNIT_TEST(Pages_PageFaults_SyncIndex_NextNext)
    {
        // 20 pages, 50 bytes each
        const auto eggs = CookPart();

        TPagesWrap wrap(eggs.Lone(), 200, 350);
        wrap.UseFaultyEnv();
        
        // Seek(0):
        for (ui32 attempt : xrange(3)) { // 3-leveled B-Tree
            wrap.To(attempt).Get(5, false, false, true, 
                {0, 0, 0, 0, 0});
            wrap.LoadTouched();
        }

        // Next(5):
        wrap.To(3).Get(5, false, false, true, 
            {0, 0, 0, 0, 0});
        wrap.LoadTouched();
        wrap.To(4).Get(5, false, true, true, 
            {1, 0, 1, 0, 0});
    
        wrap.To(6).Fill({5},
            {1, 1, 1, 0, 0});

        // set Grow = true:
        wrap.To(7).Get(5, true, true, true, 
            {1, 1, 1, 0, 0});

        // load {6, 7, 8} node:
        wrap.To(8).Fill({},
            {1, 1, 1, 0, 0});
        wrap.LoadTouched();

        // set Grow = true:
        wrap.To(9).Get(5, true, true, true, 
            {1, 1, 1, 0, 0});

        wrap.To(10).Fill({6, 7, 8},
            {4, 4, 1, 0, 0});
    }

    Y_UNIT_TEST(Pages_PageFaults_SyncIndex_Forward)
    {
        // 20 pages, 50 bytes each
        const auto eggs = CookPart();

        TPagesWrap wrap(eggs.Lone(), 1000, 1000);
        wrap.UseFaultyEnv();
        
        // Seek(0):
        for (ui32 attempt : xrange(3)) { // 3-leveled B-Tree
            wrap.To(attempt).Get(0, false, false, true, 
                {0, 0, 0, 0, 0});
            wrap.LoadTouched();
        }

        wrap.To(3).Get(0, false, true, true, 
            {1, 0, 1, 0, 0});
        wrap.To(4).Fill({0, 1, 2},
            {3, 3, 1, 0, 0});

        // ContinueNext = true but request further page
        for (ui32 attempt : xrange(4)) {
            wrap.To(5 + attempt).Get(9, false, false, true, 
                {3, 3, 1, 2, 0});
            wrap.LoadTouched();
        }
        wrap.To(9).Get(9, false, true, true, 
            {4, 3, 2, 2, 0});

        wrap.To(10).Fill({9, 10, 11},
            {6, 6, 2, 2, 0});
    }

    Y_UNIT_TEST(Pages_PageFaults_Fill)
    {
        // 20 pages, 50 bytes each
        const auto eggs = CookPart();

        TPagesWrap wrap(eggs.Lone(), 500, 500);
        wrap.UseFaultyEnv();
        
        // Seek(0):
        for (ui32 attempt : xrange(3)) { // 3-leveled B-Tree
            wrap.To(attempt).Get(0, false, false, true, 
                {0, 0, 0, 0, 0});
            wrap.LoadTouched();
        }

        wrap.To(3).Get(0, false, true, true, 
            {1, 0, 1, 0, 0});
        wrap.To(4).Fill({0, 1, 2},
            {3, 3, 1, 0, 0});

        // set Grow = true:
        wrap.To(5).Get(0, true, true, true, 
            {3, 3, 1, 0, 0});

        // load {3, 4, 5} node:
        wrap.To(6).Fill({},
            {3, 3, 1, 0, 0});
        wrap.LoadTouched();

        // set Grow = true:
        wrap.To(7).Get(0, true, true, true, 
            {3, 3, 1, 0, 0});

        wrap.To(8).Fill({3, 4, 5},
            {6, 6, 1, 0, 0});
    }

    Y_UNIT_TEST(TLoadedPagesCircularBuffer) {
        auto buffer = TLoadedPagesCircularBuffer<5>();

        for (ui32 pageId = 0; pageId < 42; pageId++) {
            // doesn't have current
            UNIT_ASSERT_VALUES_EQUAL(buffer.Get(pageId), nullptr);\

            auto page = TPage(pageId * 1, pageId * 10 + 1, pageId * 100, pageId * 1000);
            page.Data =  TSharedData::Copy(TString(page.Size, 'x'));

            UNIT_ASSERT_VALUES_EQUAL(buffer.Emplace(page), pageId >= 5 ? (pageId - 5) * 10 + 1 : 0);

            // has trace
            for (ui32 i = 0; i < Min(5u, pageId); i++) {
                auto got = buffer.Get(pageId - i);
                UNIT_ASSERT_VALUES_UNEQUAL(got, nullptr);
                UNIT_ASSERT_VALUES_EQUAL(got->size(), (pageId - i) * 10 + 1);
            }
            // doesn't have next
            UNIT_ASSERT_VALUES_EQUAL(buffer.Get(pageId + 1), nullptr);
        }
    }

}

}
}
