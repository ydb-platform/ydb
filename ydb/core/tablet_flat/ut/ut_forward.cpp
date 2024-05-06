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

    struct TBlobsWrap : public NTest::TSteps<TBlobsWrap>, protected NFwd::IPageLoadingQueue {
        using TFrames = NPage::TFrames;

        TBlobsWrap(TIntrusiveConstPtr<TFrames> frames, TIntrusiveConstPtr<TSlices> run, ui32 edge, ui64 aLo = 999, ui64 aHi = 999)
            : Large(std::move(frames))
            , Run(std::move(run))
            , Edge(edge)
            , AheadLo(aLo)
            , AheadHi(aHi)
        {
            TVector<ui32> edges(Large->Stats().Tags.size(), edge);

            Cache = new NFwd::TBlobs(Large, Run, edges, true);
        }

        TBlobsWrap(TIntrusiveConstPtr<TFrames> frames, ui32 edge, ui64 aLo = 999, ui64 aHi = 999)
            : TBlobsWrap(std::move(frames), TSlices::All(), edge, aLo, aHi)
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

        TBlobsWrap& Get(ui32 page, bool has, bool grow, bool need)
        {
            auto got = Cache->Get(this, page, EPage::Opaque, AheadLo);

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

        TBlobsWrap& Fill(ui32 least, ui32 most, std::initializer_list<ui16> tags)
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

            for (auto &page : load) {
                Cache->Fill(page, EPage::Opaque);
            }

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

    struct TCacheWrap : public NTest::TSteps<TCacheWrap>, protected NFwd::IPageLoadingQueue {
        using TFrames = NPage::TFrames;
        using TPartStore = NTable::NTest::TPartStore;

        TCacheWrap(const TIntrusiveConstPtr<TPartStore> part, TIntrusiveConstPtr<TSlices> slices, ui64 aLo, ui64 aHi)
            : Part(std::move(part))
            , Cache(NFwd::CreateCache(Part.Get(), IndexPageLocator, {}, slices))
            , AheadLo(aLo)
            , AheadHi(aHi)
        {
        }

        ui64 AddToQueue(TPageId pageId, EPage type) noexcept override
        {
            Y_ABORT_UNLESS(type == Part->GetPageType(pageId, { }));

            Queue.push_back(pageId);

            return Part->GetPageSize(pageId, { });
        }

        TCacheWrap& Get(TPageId pageId, bool has, bool grow, bool need, NFwd::TStat stat)
        {
            auto got = Cache->Get(this, pageId, Part->GetPageType(pageId, { }), AheadLo);

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

            UNIT_ASSERT_VALUES_EQUAL_C(Cache->Stat, stat, CurrentStepStr());

            return *this;
        }

        TCacheWrap& Fill(const TVector<TPageId>& pageIds, NFwd::TStat stat)
        {
            if (std::exchange(Grow, false)) {
                Cache->Forward(this, AheadHi);
            }

            UNIT_ASSERT_VALUES_EQUAL_C(TVector<TPageId>(Queue.begin(), Queue.end()), pageIds, CurrentStepStr());

            TVector<NPageCollection::TLoadedPage> load;
            NTest::TTestEnv testEnv;
            for (auto pageId : std::exchange(Queue, TDeque<ui32>{ })) {
                load.emplace_back(pageId, *testEnv.TryGetPage(Part.Get(), pageId, { }));
            }

            Shuffle(load.begin(), load.end(), Rnd);

            for (auto &page : load) {
                Cache->Fill(page, Part->GetPageType(page.PageId, {}));
            }

            UNIT_ASSERT_VALUES_EQUAL_C(Cache->Stat, stat, CurrentStepStr());

            return *this;
        }

        TCacheWrap& Forward(const TVector<TPageId>& pageIds, NFwd::TStat stat)
        {
            if (std::exchange(Grow, false)) {
                Cache->Forward(this, AheadHi);
            }

            UNIT_ASSERT_VALUES_EQUAL_C(TVector<TPageId>(Queue.begin(), Queue.end()), pageIds, CurrentStepStr());
        
            UNIT_ASSERT_VALUES_EQUAL_C(Cache->Stat, stat, CurrentStepStr());

            return *this;
        }

        TCacheWrap& Apply(const TVector<TPageId>& pageIds, NFwd::TStat stat)
        {
            TVector<NPageCollection::TLoadedPage> load;
            NTest::TTestEnv testEnv;
            for (auto pageId : pageIds) {
                bool found = false;
                for (auto it = Queue.begin(); it != Queue.end(); it++) {
                    if (*it == pageId) {
                        found = true;
                        Queue.erase(it);
                        break;
                    }
                }
                UNIT_ASSERT_C(found, CurrentStepStr());
                load.emplace_back(pageId, *testEnv.TryGetPage(Part.Get(), pageId, { }));
            }

            Shuffle(load.begin(), load.end(), Rnd);

            for (auto &page : load) {
                Cache->Fill(page, Part->GetPageType(page.PageId, {}));
            }

            UNIT_ASSERT_VALUES_EQUAL_C(Cache->Stat, stat, CurrentStepStr());

            return *this;
        }

        TCacheWrap& CheckLocator(TVector<TPageId> pageIds)
        {
            TVector<TPageId> actual;
            for (const auto& it : IndexPageLocator.GetMap()) {
                actual.push_back(it.first);
            }

            std::sort(pageIds.begin(), pageIds.end());

            UNIT_ASSERT_VALUES_EQUAL_C(actual, pageIds, CurrentStepStr());

            return *this;
        }

    public:
        const TIntrusiveConstPtr<TPartStore> Part;
        NFwd::TIndexPageLocator IndexPageLocator;
        TAutoPtr<NFwd::IPageLoadingLogic> Cache;
        const ui64 AheadLo;
        const ui64 AheadHi;
        bool Grow = false;

    private:
        TDeque<TPageId> Queue;
        TMersenne<ui64> Rnd;
    };
}

Y_UNIT_TEST_SUITE(NFwd_TBlobs) {
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

    Y_UNIT_TEST(Basics)
    {
        /*_ Check unordered access over the same frame */
        TBlobsWrap(CookFrames(), 61).Get(1, false, true, true).Get(0, false, true, true);
        TBlobsWrap(CookFrames(), 61).Get(4, false, true, true).Get(3, false, true, true);

        /*_ Should not load page with size above the edge */
        TBlobsWrap(CookFrames(), 61).Get(5, false, true, false).Fill(4, 5, { 2 });

        /*_ Jump between frames with full cache flush   */
        TBlobsWrap(CookFrames(), 61, 1, 10)
            .To(30).Get(1, false, true, true).Fill(1, 2, { 2 })
            .To(31).Get(6, false, true, true).Fill(1, 2, { 2 });

        /*_ Long jump, from begin of incomplete frame */
        TBlobsWrap(CookFrames(), 61).Get(3, false, true, true).Get(7, false, true, true);
    }

    Y_UNIT_TEST(Simple)
    {
        TBlobsWrap(CookFrames(), 61, 999, 999)
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
        TBlobsWrap(CookFrames(), 61, 999, 999)
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
        TBlobsWrap(CookFrames(), 55, 15, 30)
            .To(10).Get(1, false, true, true).Fill(2, 3, { 2 })
            .To(12).Get(6, true, true, true).Fill(2, 3, { 2 })
            .To(14).Get(8, true, false, true).Fill(0, 1, { 2 })
            .To(16).Get(9, false, true, true).Fill(1, 2, { 2 })
            .To(17).Get(9, true, false, true).Fill(0, 1, { 2 });
    }

    Y_UNIT_TEST(Trace)
    {
        TBlobsWrap wrap(CookFrames(), 15, 999, 999);

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
        TBlobsWrap wrap(CookFrames(), run, Max<ui32>(), 999, 999);

        wrap
            .To(1).Get(1, false, true, true).Fill(4, 5, { 2 })
            .To(2).Get(2, false, true, true).Fill(1, 2, { 1 })
            .To(3).Get(9, true, false, true).Fill(0, 1, { });

        const auto trace = wrap.To(4).Trace();

        UNIT_ASSERT(trace.size() == 0);
    }
}

Y_UNIT_TEST_SUITE(NFwd_TLoadedPagesCircularBuffer){
    Y_UNIT_TEST(Basics) {
        auto buffer = NFwd::TLoadedPagesCircularBuffer<5>();

        for (ui32 pageId = 0; pageId < 42; pageId++) {
            // doesn't have current
            UNIT_ASSERT_VALUES_EQUAL(buffer.Get(pageId), nullptr);\

            auto page = NFwd::TPage(pageId * 1, pageId * 10 + 1, pageId * 100, pageId * 1000);
            page.Data =  TSharedData::Copy(TString(page.Size, 'x'));

            auto result = buffer.Emplace(page);
            UNIT_ASSERT_VALUES_EQUAL(result, pageId >= 5 ? (pageId - 5) * 10 + 1 : 0);

            // has trace
            ui64 totalSize = 0;
            for (ui32 i = 0; i < Min(5u, pageId + 1); i++) {
                auto got = buffer.Get(pageId - i);
                UNIT_ASSERT_VALUES_UNEQUAL(got, nullptr);
                UNIT_ASSERT_VALUES_EQUAL(got->size(), (pageId - i) * 10 + 1);
                totalSize += got->size();
            }
            UNIT_ASSERT_VALUES_EQUAL(totalSize, buffer.GetDataSize());

            // doesn't have next
            UNIT_ASSERT_VALUES_EQUAL(buffer.Get(pageId + 1), nullptr);
        }
    }
}

Y_UNIT_TEST_SUITE(NFwd_TFlatIndexCache) {
    using namespace NFwd;

    // 20 pages, 50 bytes each
    TPartEggs CookPart() {
        NPage::TConf conf;

        conf.WriteBTreeIndex = false;
        conf.WriteFlatIndex = true;
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

    Y_UNIT_TEST(Basics)
    {
        const auto eggs = CookPart();

        TCacheWrap wrap(eggs.Lone(), nullptr, 200, 350);
    
        // provide index page:
        wrap.To(0).Get(20, false, false, true,
            {453, 0, 453, 0, 0});
        wrap.To(1).Fill({20},
            {453, 453, 453, 0, 0});
        wrap.To(2).Get(20, true, false, true,
            {453, 453, 453, 0, 0});

        wrap.To(3).Get(0, false, true, true,
            {503, 453, 503, 0, 0});
        wrap.To(4).Fill({0, 1, 2, 3, 4, 5, 6},
            {803, 803, 503, 0, 0});
        wrap.To(5).Get(0, true, false, true,
            {803, 803, 503, 0, 0});

        wrap.To(6).Get(1, true, false, true,
            {803, 803, 553, 0, 0});
        wrap.To(7).Get(2, true, false, true,
            {803, 803, 603, 0, 0});
        wrap.To(8).Get(3, true, false, true,
            {803, 803, 653, 0, 0});
        wrap.To(9).Get(4, true, false, true,
            {803, 803, 703, 0, 0});
    
        wrap.To(10).Get(5, true, true, true,
            {803, 803, 753, 0, 0});
        wrap.To(11).Fill({7, 8, 9},
            {953, 953, 753, 0, 0});
    }

    Y_UNIT_TEST(IndexPagesLocator)
    {
        const auto eggs = CookPart();

        TCacheWrap wrap(eggs.Lone(), nullptr, 200, 350);
    
        wrap.To(0).CheckLocator({20});

        // provide index page:
        wrap.To(1).Get(20, false, false, true,
            {453, 0, 453, 0, 0});
        wrap.To(2).Fill({20},
            {453, 453, 453, 0, 0});

        wrap.To(3).Get(0, false, true, true,
            {503, 453, 503, 0, 0});
        wrap.To(4).Fill({0, 1, 2, 3, 4, 5, 6},
            {803, 803, 503, 0, 0});

        wrap.To(5).CheckLocator({20});
    }

    Y_UNIT_TEST(GetTwice)
    {
        const auto eggs = CookPart();

        TCacheWrap wrap(eggs.Lone(), nullptr, 200, 350);
    
        // provide index page:
        wrap.To(0).Get(20, false, false, true,
            {453, 0, 453, 0, 0});
        wrap.To(1).Get(20, false, false, true,
            {453, 0, 453, 0, 0});
        wrap.To(2).Fill({20},
            {453, 453, 453, 0, 0});
        wrap.To(3).Get(20, true, false, true,
            {453, 453, 453, 0, 0});
        wrap.To(4).Get(20, true, false, true,
            {453, 453, 453, 0, 0});

        wrap.To(5).Get(5, false, true, true,
            {503, 453, 503, 0, 0});
        wrap.To(6).Get(5, false, true, true,
            {503, 453, 503, 0, 0});
        wrap.To(7).Fill({5, 6, 7, 8, 9, 10, 11},
            {803, 803, 503, 0, 0});
        wrap.To(8).Get(5, true, false, true,
            {803, 803, 503, 0, 0});
        wrap.To(9).Get(5, true, false, true,
            {803, 803, 503, 0, 0});
    
        wrap.To(10).Get(6, true, false, true,
            {803, 803, 553, 0, 0});
        wrap.To(11).Get(6, true, false, true,
            {803, 803, 553, 0, 0});
    }

    Y_UNIT_TEST(ForwardTwice)
    {
        const auto eggs = CookPart();

        TCacheWrap wrap(eggs.Lone(), nullptr, 200, 350);
    
        // provide index page:
        wrap.To(0).Get(20, false, false, true,
            {453, 0, 453, 0, 0});
        wrap.To(1).Fill({20},
            {453, 453, 453, 0, 0});
        
        wrap.To(2).Get(5, false, true, true,
            {503, 453, 503, 0, 0});
        wrap.To(3).Fill({5, 6, 7, 8, 9, 10, 11},
            {803, 803, 503, 0, 0});
        wrap.Grow = true;
        wrap.To(4).Fill({},
            {803, 803, 503, 0, 0});
    }

    Y_UNIT_TEST(Skip_Done)
    {
        const auto eggs = CookPart();

        TCacheWrap wrap(eggs.Lone(), nullptr, 200, 350);
    
        // provide index page:
        wrap.To(0).Get(20, false, false, true,
            {453, 0, 453, 0, 0});
        wrap.To(1).Fill({20},
            {453, 453, 453, 0, 0});

        wrap.To(2).Get(0, false, true, true,
            {503, 453, 503, 0, 0});
        wrap.To(3).Fill({0, 1, 2, 3, 4, 5, 6},
            {803, 803, 503, 0, 0});

        wrap.To(4).Get(5, true, true, true,
            {803, 803, 553, 200, 0});
        wrap.To(5).Fill({7, 8, 9, 10},
            {1003, 1003, 553, 200, 0});
    }

    Y_UNIT_TEST(Skip_Done_None)
    {
        const auto eggs = CookPart();

        TCacheWrap wrap(eggs.Lone(), nullptr, 200, 350);
    
        // provide index page:
        wrap.To(0).Get(20, false, false, true,
            {453, 0, 453, 0, 0});
        wrap.To(1).Fill({20},
            {453, 453, 453, 0, 0});

        wrap.To(2).Get(0, false, true, true,
            {503, 453, 503, 0, 0});
        wrap.To(3).Fill({0, 1, 2, 3, 4, 5, 6},
            {803, 803, 503, 0, 0});

        wrap.To(4).Get(10, false, true, true,
            {853, 803, 553, 300, 0});
        wrap.To(5).Fill({10, 11, 12, 13, 14, 15},
            {1103, 1103, 553, 300, 0});
    }

    Y_UNIT_TEST(Skip_Keep)
    {
        const auto eggs = CookPart();

        TCacheWrap wrap(eggs.Lone(), nullptr, 200, 350);
    
        // provide index page:
        wrap.To(0).Get(20, false, false, true,
            {453, 0, 453, 0, 0});
        wrap.To(1).Fill({20},
            {453, 453, 453, 0, 0});

        wrap.To(2).Get(0, false, true, true,
            {503, 453, 503, 0, 0});

        wrap.To(3).Get(5, false, true, true,
            {553, 453, 553, 0, 50});
        wrap.To(4).Fill({0, 5, 6, 7, 8, 9, 10},
            {803, 803, 553, 0, 50});
    }

    Y_UNIT_TEST(Skip_Wait)
    {
        const auto eggs = CookPart();

        TCacheWrap wrap(eggs.Lone(), nullptr, 200, 350);
    
        // provide index page:
        wrap.To(0).Get(20, false, false, true,
            {453, 0, 453, 0, 0});
        wrap.To(1).Fill({20},
            {453, 453, 453, 0, 0});

        wrap.To(2).Get(0, false, true, true,
            {503, 453, 503, 0, 0});
        wrap.To(3).Forward({0, 1, 2, 3, 4, 5, 6},
            {803, 453, 503, 0, 0});

        // page 0 drops keep, pages 1 - 6 drops wait
        wrap.To(4).Get(10, false, false, true,
            {853, 453, 553, 0, 350});
        wrap.To(5).Fill({0, 1, 2, 3, 4, 5, 6, 10},
            {853, 853, 553, 0, 350});
    
        // ready to grow again:
        wrap.To(6).Get(10, true, true, true,
            {853, 853, 553, 0, 350});
        wrap.To(7).Fill({11, 12, 13, 14, 15, 16},
            {1153, 1153, 553, 0, 350});
    }

    Y_UNIT_TEST(Trace)
    {
        const auto eggs = CookPart();

        TCacheWrap wrap(eggs.Lone(), nullptr, 200, 350);
    
        // provide index page:
        wrap.To(0).Get(20, false, false, true,
            {453, 0, 453, 0, 0});
        wrap.To(1).Fill({20},
            {453, 453, 453, 0, 0});

        wrap.To(2).Get(0, false, true, true,
            {503, 453, 503, 0, 0});
        wrap.To(3).Fill({0, 1, 2, 3, 4, 5, 6},
            {803, 803, 503, 0, 0});
    
        // page 0 goes to trace:
        wrap.To(4).Get(2, true, false, true,
            {803, 803, 553, 50, 0});

        // page 2 goes to trace, page 1 drops:
        wrap.To(5).Get(3, true, false, true,
            {803, 803, 603, 50, 0});
    
        // trace: page 0, page 2:
        wrap.To(6).Get(0, true, false, true,
            {803, 803, 603, 50, 0});
        wrap.To(7).Get(2, true, false, true,
            {803, 803, 603, 50, 0});
        wrap.To(8).Get(3, true, false, true,
            {803, 803, 603, 50, 0});

        // page 3 goes to trace:
        wrap.To(9).Get(4, true, false, true,
            {803, 803, 653, 50, 0});

        // trace: page 2, page 3:
        wrap.To(10).Get(2, true, false, true,
            {803, 803, 653, 50, 0});
        wrap.To(11).Get(3, true, false, true,
            {803, 803, 653, 50, 0});
        wrap.To(12).Get(4, true, false, true,
            {803, 803, 653, 50, 0});
    }

    Y_UNIT_TEST(End)
    {
        const auto eggs = CookPart();

        TCacheWrap wrap(eggs.Lone(), nullptr, 200, 350);
    
        // provide index page:
        wrap.To(0).Get(20, false, false, true,
            {453, 0, 453, 0, 0});
        wrap.To(1).Fill({20},
            {453, 453, 453, 0, 0});

        wrap.To(2).Get(17, false, true, true,
            {503, 453, 503, 0, 0});
        wrap.To(3).Fill({17, 18, 19},
            {603, 603, 503, 0, 0});
        wrap.To(4).Get(17, true, false, true,
            {603, 603, 503, 0, 0});
        wrap.To(5).Get(18, true, false, true,
            {603, 603, 553, 0, 0});
        wrap.To(6).Get(19, true, false, true,
            {603, 603, 603, 0, 0});
    }

    Y_UNIT_TEST(Slices)
    {
        const auto eggs = CookPart();

        TIntrusivePtr<TSlices> slices = new TSlices;
        // pages 5 - 7
        slices->emplace_back(TSlice({ }, { }, 10, 16, true, false));
        // pages 10 - 11
        slices->emplace_back(TSlice({ }, { }, 20, 23, true, true));

        TCacheWrap wrap(eggs.Lone(), slices, 1000, 1000);
    
        // provide index page:
        wrap.To(0).Get(20, false, false, true,
            {453, 0, 453, 0, 0});
        wrap.To(1).Fill({20},
            {453, 453, 453, 0, 0});

        wrap.To(2).Get(5, false, true, true,
            {503, 453, 503, 0, 0});
        wrap.To(3).Fill({5, 6, 7, 8, 9, 10, 11},
            {803, 803, 503, 0, 0});
        wrap.To(4).Get(10, true, false, true,
            {803, 803, 553, 200, 0});
        wrap.To(5).Get(11, true, false, true,
            {803, 803, 603, 200, 0});
    }
}

Y_UNIT_TEST_SUITE(NFwd_TBTreeIndexCache) {
    using namespace NFwd;

    /**
     20 pages, 50 bytes each
     B-Tree index:
        [28] {
            [23] {
                [6] {0, 1, 2},
                [10] {3, 4, 5},
                [14] {6, 7, 8}
            },
            [27] {
                [18] {9, 10, 11},
                [22] {12, 13, 14},
                [26] {15, 16, 17, 18, 19}
            }
        }
    */
    TPartEggs CookPart() {
        NPage::TConf conf;

        conf.WriteBTreeIndex = true;
        conf.WriteFlatIndex = false;
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

    Y_UNIT_TEST(Basics)
    {
        const auto eggs = CookPart();

        TCacheWrap wrap(eggs.Lone(), nullptr, 200, 350);
    
        // level 0:
        wrap.To(0).Get(28, false, false, true,
            {98, 0, 98, 0, 0});
        wrap.To(1).Fill({28},
            {98, 98, 98, 0, 0});
        wrap.To(2).Get(28, true, false, true,
            {98, 98, 98, 0, 0});

        // level 1:
        wrap.To(3).Get(23, false, true, true,
            {241, 98, 241, 0, 0});
        wrap.To(4).Fill({23, 27},
            {384, 384, 241, 0, 0});
        wrap.To(5).Get(23, true, false, true,
            {384, 384, 241, 0, 0});

        // level 2:
        wrap.To(6).Get(6, false, true, true,
            {527, 384, 384, 0, 0});
        wrap.To(7).Fill({6, 10, 14, 18},
            {956, 956, 384, 0, 0});
        wrap.To(8).Get(6, true, false, true,
            {956, 956, 384, 0, 0});

        // data pages:
        wrap.To(9).Get(0, false, true, true,
            {1006, 956, 434, 0, 0});
        wrap.To(10).Fill({0, 1, 2, 3, 4, 5, 7},
            {1306, 1306, 434, 0, 0});
        wrap.To(11).Get(0, true, false, true,
            {1306, 1306, 434, 0, 0});

        wrap.To(12).Get(1, true, false, true,
            {1306, 1306, 484, 0, 0});
        wrap.To(13).Get(2, true, false, true,
            {1306, 1306, 534, 0, 0});

        wrap.To(14).Get(10, true, false, true,
            {1306, 1306, 677, 0, 0});
        wrap.To(15).Get(3, true, false, true,
            {1306, 1306, 727, 0, 0});
        wrap.To(16).Get(4, true, false, true,
            {1306, 1306, 777, 0, 0});
    
        wrap.To(17).Get(5, true, true, true,
            {1306, 1306, 827, 0, 0});
        wrap.To(18).Fill({22, 8, 9, 11},
            {1599, 1599, 827, 0, 0});
    }

    Y_UNIT_TEST(IndexPagesLocator)
    {
        const auto eggs = CookPart();

        TCacheWrap wrap(eggs.Lone(), nullptr, 200, 350);

        wrap.To(0).CheckLocator({28});
    
        // level 0:
        wrap.To(1).Get(28, false, false, true,
            {98, 0, 98, 0, 0});
        wrap.To(2).Fill({28},
            {98, 98, 98, 0, 0});
        wrap.To(3).CheckLocator({28, 23, 27});

        // level 1:
        wrap.To(4).Get(23, false, true, true,
            {241, 98, 241, 0, 0});
        wrap.To(5).Forward({23, 27},
            {384, 98, 241, 0, 0});
        wrap.To(6).Apply({27},
            {384, 241, 241, 0, 0});
        wrap.To(7).CheckLocator({28, 23, 27, 18, 22, 26});
        wrap.To(8).Apply({23},
            {384, 384, 241, 0, 0});
        wrap.To(9).CheckLocator({28, 23, 27, 6, 10, 14, 18, 22, 26});

        // level 2:
        wrap.To(10).Get(6, false, true, true,
            {527, 384, 384, 0, 0});
        wrap.To(11).Fill({6, 10, 14, 18},
            {956, 956, 384, 0, 0});
        wrap.To(12).CheckLocator({28, 23, 27, 6, 10, 14, 18, 22, 26});

        // iterating:
        wrap.To(13).Get(6, true, false, true,
            {956, 956, 384, 0, 0});
        wrap.To(14).CheckLocator({28, 23, 27, 6, 10, 14, 18, 22, 26});
        wrap.To(15).Get(10, true, false, true,
            {956, 956, 527, 0, 0});
        wrap.To(16).CheckLocator({28, 23, 27, 6, 10, 14, 18, 22, 26});

        // data pages:
        wrap.To(1000).Get(0, false, true, true,
            {1006, 956, 577, 0, 0});
        wrap.To(1001).Fill({0, 22, 1, 2, 3, 4, 5, 7},
            {1449, 1449, 577, 0, 0});
        wrap.To(1002).CheckLocator({28, 23, 27, 6, 10, 14, 18, 22, 26});
    }

    Y_UNIT_TEST(GetTwice)
    {
        const auto eggs = CookPart();

        TCacheWrap wrap(eggs.Lone(), nullptr, 200, 350);
    
        // level 0:
        wrap.To(0).Get(28, false, false, true,
            {98, 0, 98, 0, 0});
        wrap.To(1).Get(28, false, false, true,
            {98, 0, 98, 0, 0});
        wrap.To(2).Fill({28},
            {98, 98, 98, 0, 0});
        wrap.To(3).Get(28, true, false, true,
            {98, 98, 98, 0, 0});
        wrap.To(4).Get(28, true, false, true,
            {98, 98, 98, 0, 0});

        // level 1:
        wrap.To(5).Get(23, false, true, true,
            {241, 98, 241, 0, 0});
        wrap.To(6).Get(23, false, true, true,
            {241, 98, 241, 0, 0});
        wrap.To(7).Fill({23, 27},
            {384, 384, 241, 0, 0});
        wrap.To(8).Get(23, true, false, true,
            {384, 384, 241, 0, 0});
        wrap.To(9).Get(23, true, false, true,
            {384, 384, 241, 0, 0});
    }

    Y_UNIT_TEST(ForwardTwice)
    {
        const auto eggs = CookPart();

        TCacheWrap wrap(eggs.Lone(), nullptr, 200, 350);
    
        // level 0:
        wrap.To(0).Get(28, false, false, true,
            {98, 0, 98, 0, 0});
        wrap.To(2).Fill({28},
            {98, 98, 98, 0, 0});
        wrap.Grow = true;
        wrap.To(2).Fill({},
            {98, 98, 98, 0, 0});

        // level 1:
        wrap.To(5).Get(23, false, true, true,
            {241, 98, 241, 0, 0});
        wrap.To(7).Fill({23, 27},
            {384, 384, 241, 0, 0});
        wrap.Grow = true;
        wrap.To(7).Fill({},
            {384, 384, 241, 0, 0});
    }

    Y_UNIT_TEST(Forward_OnlyUsed)
    {
        const auto eggs = CookPart();

        TCacheWrap wrap(eggs.Lone(), nullptr, 200, 350);
    
        // level 0:
        wrap.To(0).Get(28, false, false, true,
            {98, 0, 98, 0, 0});
        wrap.To(1).Fill({28},
            {98, 98, 98, 0, 0});

        // level 1:
        wrap.To(2).Get(23, false, true, true,
            {241, 98, 241, 0, 0});
        wrap.To(3).Fill({23, 27},
            {384, 384, 241, 0, 0});

        // level 2:
        wrap.To(4).Get(6, false, true, true,
            {527, 384, 384, 0, 0});
        wrap.To(5).Fill({6, 10, 14, 18},
            {956, 956, 384, 0, 0});

        wrap.To(6).Get(10, true, false, true,
            {956, 956, 527, 0, 0});
        wrap.To(7).Get(14, true, true, true,
            {956, 956, 670, 0, 0});
        wrap.To(8).Fill({22, 26},
            {1332, 1332, 670, 0, 0});

        // data pages:
        wrap.To(9).Get(4, false, true, true,
            {1382, 1332, 720, 0, 0});
        wrap.To(10).Fill({4, 5, 7, 8, 9, 11, 12},
            {1682, 1682, 720, 0, 0});
    }

    Y_UNIT_TEST(Skip_Done)
    {
        const auto eggs = CookPart();

        TCacheWrap wrap(eggs.Lone(), nullptr, 200, 350);
    
        // level 0:
        wrap.To(0).Get(28, false, false, true,
            {98, 0, 98, 0, 0});
        wrap.To(1).Fill({28},
            {98, 98, 98, 0, 0});

        // level 1:
        wrap.To(2).Get(23, false, true, true,
            {241, 98, 241, 0, 0});
        wrap.To(3).Fill({23, 27},
            {384, 384, 241, 0, 0});

        // level 2:
        wrap.To(4).Get(6, false, true, true,
            {527, 384, 384, 0, 0});
        wrap.To(5).Fill({6, 10, 14, 18},
            {956, 956, 384, 0, 0});

        wrap.To(6).Get(14, true, true, true,
            {956, 956, 527, 143, 0});
        wrap.To(7).Fill({22, 26},
            {1332, 1332, 527, 143, 0});
    }

    Y_UNIT_TEST(Skip_Done_None)
    {
        const auto eggs = CookPart();

        TCacheWrap wrap(eggs.Lone(), nullptr, 200, 350);
    
        // level 0:
        wrap.To(0).Get(28, false, false, true,
            {98, 0, 98, 0, 0});
        wrap.To(1).Fill({28},
            {98, 98, 98, 0, 0});

        // level 1:
        wrap.To(2).Get(23, false, true, true,
            {241, 98, 241, 0, 0});
        wrap.To(3).Fill({23, 27},
            {384, 384, 241, 0, 0});

        // level 2:
        wrap.To(4).Get(6, false, true, true,
            {527, 384, 384, 0, 0});
        wrap.To(5).Fill({6, 10, 14, 18},
            {956, 956, 384, 0, 0});

        wrap.To(6).Get(26, false, false, true,
            {1189, 956, 617, 429, 0});
        wrap.To(7).Fill({26},
            {1189, 1189, 617, 429, 0});
    }

    Y_UNIT_TEST(Skip_Keep)
    {
        const auto eggs = CookPart();

        TCacheWrap wrap(eggs.Lone(), nullptr, 200, 350);
    
        // level 0:
        wrap.To(0).Get(28, false, false, true,
            {98, 0, 98, 0, 0});
        wrap.To(1).Fill({28},
            {98, 98, 98, 0, 0});

        // level 1:
        wrap.To(2).Get(23, false, true, true,
            {241, 98, 241, 0, 0});
        wrap.To(3).Fill({23, 27},
            {384, 384, 241, 0, 0});

        // level 2:
        wrap.To(4).Get(6, false, true, true,
            {527, 384, 384, 0, 0});

        wrap.To(6).Get(22, false, true, true,
            {670, 384, 527, 0, 143});
        wrap.To(7).Fill({6, 22},
            {670, 670, 527, 0, 143});

        wrap.To(8).Get(22, true, true, true,
            {670, 670, 527, 0, 143});
        wrap.To(9).Fill({26},
            {903, 903, 527, 0, 143});
    }

    Y_UNIT_TEST(Skip_Wait)
    {
        const auto eggs = CookPart();

        TCacheWrap wrap(eggs.Lone(), nullptr, 200, 350);
    
        // level 0:
        wrap.To(0).Get(28, false, false, true,
            {98, 0, 98, 0, 0});
        wrap.To(1).Fill({28},
            {98, 98, 98, 0, 0});

        // level 1:
        wrap.To(2).Get(23, false, true, true,
            {241, 98, 241, 0, 0});
        wrap.To(3).Fill({23, 27},
            {384, 384, 241, 0, 0});

        // level 2:
        wrap.To(4).Get(6, false, true, true,
            {527, 384, 384, 0, 0});
        wrap.To(5).Forward({6, 10, 14, 18},
            {956, 384, 384, 0, 0});

        wrap.To(6).Get(26, false, false, true,
            {1189, 384, 617, 0, 572});
        wrap.To(7).Fill({6, 10, 14, 18, 26},
            {1189, 1189, 617, 0, 572});
    }

    Y_UNIT_TEST(Trace_BTree)
    {
        const auto eggs = CookPart();

        TCacheWrap wrap(eggs.Lone(), nullptr, 200, 350);
    
        // level 0:
        wrap.To(0).Get(28, false, false, true,
            {98, 0, 98, 0, 0});
        wrap.To(1).Fill({28},
            {98, 98, 98, 0, 0});
        wrap.To(2).Get(28, true, false, true,
            {98, 98, 98, 0, 0});

        // level 1:
        wrap.To(3).Get(23, false, true, true,
            {241, 98, 241, 0, 0});
        wrap.To(4).Fill({23, 27},
            {384, 384, 241, 0, 0});
        wrap.To(5).Get(23, true, false, true,
            {384, 384, 241, 0, 0});

        // level 2:
        wrap.To(6).Get(6, false, true, true,
            {527, 384, 384, 0, 0});
        wrap.To(7).Fill({6, 10, 14, 18},
            {956, 956, 384, 0, 0});
        wrap.To(8).Get(6, true, false, true,
            {956, 956, 384, 0, 0});
        wrap.To(9).Get(14, true, true, true,
            {956, 956, 527, 143, 0});
        wrap.To(10).Get(18, true, true, true,
            {956, 956, 670, 143, 0});
        wrap.To(11).Get(6, true, false, true,
            {956, 956, 670, 143, 0});
        wrap.To(12).Get(14, true, false, true,
            {956, 956, 670, 143, 0});
        wrap.To(13).Get(18, true, true, true,
            {956, 956, 670, 143, 0});

        wrap.To(14).Fill({22, 26},
            {1332, 1332, 670, 143, 0});
        wrap.To(15).Get(22, true, false, true,
            {1332, 1332, 813, 143, 0});
        wrap.To(16).Get(14, true, false, true,
            {1332, 1332, 813, 143, 0});
        wrap.To(17).Get(18, true, false, true,
            {1332, 1332, 813, 143, 0});
    }

    Y_UNIT_TEST(Trace_Data)
    {
        const auto eggs = CookPart();

        TCacheWrap wrap(eggs.Lone(), nullptr, 200, 350);
    
        // level 0:
        wrap.To(0).Get(28, false, false, true,
            {98, 0, 98, 0, 0});
        wrap.To(1).Fill({28},
            {98, 98, 98, 0, 0});

        // level 1:
        wrap.To(2).Get(23, false, true, true,
            {241, 98, 241, 0, 0});
        wrap.To(3).Fill({23, 27},
            {384, 384, 241, 0, 0});

        // level 2:
        wrap.To(4).Get(6, false, true, true,
            {527, 384, 384, 0, 0});
        wrap.To(5).Fill({6, 10, 14, 18},
            {956, 956, 384, 0, 0});

        // data pages:
        wrap.To(6).Get(0, false, true, true,
            {1006, 956, 434, 0, 0});
        wrap.To(7).Fill({0, 1, 2, 3, 4, 5, 7},
            {1306, 1306, 434, 0, 0});
    
        // page 0 goes to trace:
        wrap.To(8).Get(2, true, false, true,
            {1306, 1306, 484, 50, 0});

        // page 2 goes to trace, page 1 drops:
        wrap.To(9).Get(3, true, false, true,
            {1306, 1306, 534, 50, 0});
    
        // trace: page 0, page 2:
        wrap.To(10).Get(0, true, false, true,
            {1306, 1306, 534, 50, 0});
        wrap.To(11).Get(2, true, false, true,
            {1306, 1306, 534, 50, 0});
        wrap.To(12).Get(3, true, false, true,
            {1306, 1306, 534, 50, 0});

        // page 3 goes to trace:
        wrap.To(13).Get(4, true, false, true,
            {1306, 1306, 584, 50, 0});

        // trace: page 2, page 3:
        wrap.To(14).Get(2, true, false, true,
            {1306, 1306, 584, 50, 0});
        wrap.To(15).Get(3, true, false, true,
            {1306, 1306, 584, 50, 0});
        wrap.To(16).Get(4, true, false, true,
            {1306, 1306, 584, 50, 0});
    }

    Y_UNIT_TEST(End)
    {
        const auto eggs = CookPart();

        TCacheWrap wrap(eggs.Lone(), nullptr, 200, 350);
    
        // level 0:
        wrap.To(0).Get(28, false, false, true,
            {98, 0, 98, 0, 0});
        wrap.To(1).Fill({28},
            {98, 98, 98, 0, 0});
        wrap.To(2).Get(28, true, false, true,
            {98, 98, 98, 0, 0});

        // level 1:
        wrap.To(3).Get(23, false, true, true,
            {241, 98, 241, 0, 0});
        wrap.To(4).Fill({23, 27},
            {384, 384, 241, 0, 0});

        // level 2:
        wrap.To(6).Get(22, false, true, true,
            {527, 384, 384, 0, 0});
        wrap.To(7).Fill({22, 26},
            {760, 760, 384, 0, 0});
        wrap.To(8).Get(26, true, false, true,
            {760, 760, 617, 0, 0});

        // data pages:
        wrap.To(9).Get(24, false, true, true,
            {810, 760, 667, 0, 0});
        wrap.To(10).Fill({24, 25},
            {860, 860, 667, 0, 0});
        wrap.To(11).Get(25, true, false, true,
            {860, 860, 717, 0, 0});
    }

    Y_UNIT_TEST(Slices)
    {
        const auto eggs = CookPart();

        TIntrusivePtr<TSlices> slices = new TSlices;
        // pages 5 - 8
        slices->emplace_back(TSlice({ }, { }, 10, 16, true, false));
        // pages 12 - 13
        slices->emplace_back(TSlice({ }, { }, 20, 23, true, true));

        TCacheWrap wrap(eggs.Lone(), slices, 1000, 1000);
    
        // level 0:
        wrap.To(0).Get(28, false, false, true,
            {98, 0, 98, 0, 0});
        wrap.To(1).Fill({28},
            {98, 98, 98, 0, 0});
        wrap.To(2).Get(28, true, false, true,
            {98, 98, 98, 0, 0});

        // level 1:
        wrap.To(3).Get(23, false, true, true,
            {241, 98, 241, 0, 0});
        wrap.To(4).Fill({23, 27},
            {384, 384, 241, 0, 0});

        // level 2:
        wrap.To(6).Get(10, false, true, true,
            {527, 384, 384, 0, 0});
        wrap.To(7).Fill({10, 14, 18},
            {813, 813, 384, 0, 0});

        wrap.To(8).Get(5, false, true, true,
            {863, 813, 434, 0, 0});
        wrap.To(9).Fill({5, 7, 8, 9, 11, 12, 13},
            {1163, 1163, 434, 0, 0});
        wrap.To(10).Get(12, true, false, true,
            {1163, 1163, 484, 200, 0});
        wrap.To(11).Get(13, true, false, true,
            {1163, 1163, 534, 200, 0});
    }

    Y_UNIT_TEST(ManyApplies)
    {
        const auto eggs = CookPart();

        TCacheWrap wrap(eggs.Lone(), nullptr, 1000, 1000);
    
        // level 0:
        wrap.To(0).Get(28, false, false, true,
            {98, 0, 98, 0, 0});
        wrap.To(1).Fill({28},
            {98, 98, 98, 0, 0});

        // level 1:
        wrap.To(2).Get(23, false, true, true,
            {241, 98, 241, 0, 0});
        wrap.To(3).Fill({23, 27},
            {384, 384, 241, 0, 0});

        // level 2:
        wrap.To(4).Get(6, false, true, true,
            {527, 384, 384, 0, 0});
        wrap.To(5).Forward({6, 10, 14, 18, 22, 26},
            {1332, 384, 384, 0, 0});

        wrap.To(6).Apply({6},
            {1332, 527, 384, 0, 0});

        // skip page 10:
        wrap.To(7).Get(14, false, false, true,
            {1332, 527, 527, 0, 143});
        wrap.To(8).Apply({18},
            {1332, 670, 527, 0, 143});
        wrap.To(9).Apply({10},
            {1332, 813, 527, 0, 143});
        wrap.To(10).Apply({14},
            {1332, 956, 527, 0, 143});

        // data pages:
        wrap.To(11).Get(0, false, true, true,
            {1382, 956, 577, 0, 143});
        wrap.To(12).Fill({22, 26, 0, 1, 2, 7, 8, 9, 11, 12, 13},
            {1782, 1782, 577, 0, 143});
    }
}

}
}
