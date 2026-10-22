#pragma once
#include "defs.h"
#include "flat_part_store.h"
#include "flat_part_walker.h"
#include "flat_sausagecache.h"
#include "shared_cache_events.h"
#include "util_fmt_abort.h"
#include <ydb/core/tablet_flat/protos/flat_table_part.pb.h>
#include <ydb/core/util/pb.h>
#include <util/generic/hash.h>
#include <util/generic/hash_set.h>
#include <util/stream/mem.h>

namespace NKikimr {
namespace NTable {

    class TLoader {
    public:
        enum class EStage : ui8 {
            Meta,
            PartView,
            Slice,
            Deltas,
            PreloadData,
            Result,
        };

        using TPageCollection = NTabletFlatExecutor::TPrivatePageCache::TPageCollection;

        struct TFetch : TMoveOnly {
            TIntrusiveConstPtr<NPageCollection::IPageCollection> PageCollection;
            TVector<TPageLocation> Pages;

            explicit operator bool() const {
                return bool(Pages);
            }
        };

        struct TLoaderEnv : public IPages {
            TLoaderEnv(TVector<TIntrusivePtr<TPageCollection>> pageCollections, TPartSlots slots)
                : PageCollections(std::move(pageCollections))
                , Slots(slots)
                , SavedPages(Slots.SlotsCount())
                , NeedPages(Slots.SlotsCount())
            {
                Y_ENSURE(PageCollections.size() >= Slots.SlotsCount());
            }

            TResult Locate(const TMemTable*, ui64, ui32) override
            {
                Y_TABLET_ERROR("IPages::Locate(TMemTable*, ...) shouldn't be used here");
            }

            TResult Locate(const TPart*, ui64, ELargeObj) override
            {
                Y_TABLET_ERROR("IPages::Locate(TPart*, ...) shouldn't be used here");
            }

            void ProvidePart(const TPart* part)
            {
                Y_ENSURE(!Part);
                Part = part;
            }

            const TSharedData* TryGetPage(const TPart* part, const TPageLocation& location, TGroupId groupId) override
            {
                Y_ENSURE(part == Part, "Unsupported part");
                return TryGetSlotPage(Slots.DataSlot(groupId), location);
            }

            const TSharedData* TryGetIndexPage(const TPart* part, const TPageLocation& location, TGroupId groupId) override
            {
                Y_ENSURE(part == Part, "Unsupported part");
                return TryGetSlotPage(Slots.IndexSlot(groupId), location);
            }

            /**
             * Loads a page of the page collection, the outer page collection is not supported
             */
            const TSharedData* TryGetSlotPage(ui32 slot, const TPageLocation& location)
            {
                Y_ENSURE(slot < Slots.SlotsCount(), "Unsupported page collection " << slot);

                auto& savedPages = SavedPages[slot];
                auto savedPage = savedPages.find(location.Offset);

                if (savedPage == savedPages.end()) {
                    if (auto cachedPage = PageCollections[slot]->FindPage(location.Offset); cachedPage) {
                        if (auto sharedPageRef = cachedPage->SharedBody; sharedPageRef && sharedPageRef.Use()) {
                            // Save page in case it's evicted on the next iteration
                            AddSavedPage(slot, location.Offset, std::move(sharedPageRef));
                            savedPage = savedPages.find(location.Offset);
                        }
                    }
                }

                if (savedPage != savedPages.end()) {
                    return &savedPage->second;
                } else {
                    auto [it, inserted] = NeedPages[slot].emplace(location);
                    Y_DEBUG_ABORT_UNLESS(inserted || *it == location);
                    return nullptr;
                }
            }

            void EnsureNoNeedPages() const
            {
                for (const auto& needPages : NeedPages) {
                    Y_ENSURE(!needPages);
                }
            }

            /**
             * Returns missing pages of a single page collection, they are expected to be saved before the next fetch
             */
            TFetch GetFetch()
            {
                for (ui32 slot : xrange(NeedPages.size())) {
                    if (const auto& needPages = NeedPages[slot]) {
                        TVector<TPageLocation> pages(needPages.begin(), needPages.end());
                        std::sort(pages.begin(), pages.end());
                        FetchSlot = slot;
                        return {
                            .PageCollection = PageCollections[slot]->PageCollection,
                            .Pages = std::move(pages)
                        };
                    }
                }
                return {};
            }

            void Save(NSharedCache::TEvResult::TLoaded&& loaded)
            {
                auto& needPages = NeedPages.at(FetchSlot);
                auto it = needPages.find(TPageLocation(loaded.Offset));
                Y_ENSURE(it != needPages.end(), "Got unknown page at " << loaded.Offset << " of page collection " << FetchSlot);
                EPage pageType = it->Type;
                needPages.erase(it);

                bool sticky = NeedIn(pageType) || pageType == EPage::FlatIndex;
                AddSavedPage(FetchSlot, loaded.Offset, loaded.Page);
                if (sticky) {
                    PageCollections[FetchSlot]->AddStickyPage(loaded.Offset, loaded.Size, std::move(loaded.Page));
                } else {
                    PageCollections[FetchSlot]->AddPage(loaded.Offset, loaded.Size, std::move(loaded.Page));
                }
            }

        private:
            void AddSavedPage(ui32 slot, TPageOffset offset, NSharedCache::TSharedPageRef page)
            {
                SavedPages[slot][offset] = NSharedCache::TPinnedPageRef(page).GetData();
                SavedPagesRefs.emplace_back(std::move(page));
            }

            struct TPageLocationByOffsetEq {
                bool operator()(const TPageLocation& a, const TPageLocation& b) const noexcept {
                    return a.Offset == b.Offset;
                }
            };

            const TPart* Part = nullptr;
            const TVector<TIntrusivePtr<TPageCollection>> PageCollections;
            const TPartSlots Slots;
            TVector<THashMap<TPageOffset, TSharedData>> SavedPages;
            TVector<NSharedCache::TSharedPageRef> SavedPagesRefs;
            // Dedup by offset — keep Size/Type for the fetch request
            TVector<THashSet<TPageLocation, NPage::TPageLocationByOffsetHash, TPageLocationByOffsetEq>> NeedPages;
            ui32 FetchSlot = Max<ui32>(); // page collection of the last fetch
        };

        struct TRunOptions {
            // Marks that optional index pages should be loaded
            //
            // Effects only b-tree index as flat index is kept as sticky
            bool PreloadIndex = true;

            // Marks that all data pages from the main group should be loaded
            bool PreloadData = false;
        };

        TLoader(TPartComponents components, TVector<TIntrusivePtr<TPageCollection>> prebuiltPageCollections = {});
        ~TLoader();

        TFetch Run(TRunOptions options)
        {
            while (Stage < EStage::Result) {
                TFetch fetch;

                switch (Stage) {
                    case EStage::Meta:
                        StageParseMeta();
                        break;
                    case EStage::PartView:
                        fetch = StageCreatePartView(options.PreloadIndex);
                        break;
                    case EStage::Slice:
                        fetch = StageSliceBounds();
                        break;
                    case EStage::Deltas:
                        StageDeltas();
                        break;
                    case EStage::PreloadData:
                        if (options.PreloadData) {
                            fetch = StagePreloadData();
                        }
                        break;
                    default:
                        break;
                }

                if (fetch) {
                    return fetch;
                }

                Stage = EStage(ui8(Stage) + 1);
            }

            return { };
        }

        void Save(TVector<NSharedCache::TEvResult::TLoaded>&& pages);

        constexpr static bool NeedIn(EPage page) noexcept
        {
            return
                page == EPage::Scheme
                || page == EPage::Frames || page == EPage::Globs
                || page == EPage::Schem2 || page == EPage::Bloom
                || page == EPage::GarbageStats
                || page == EPage::TxIdStats;
        }

        TPartView Result()
        {
            Y_ENSURE(Stage == EStage::Result);
            Y_ENSURE(PartView, "Result may only be grabbed once");
            Y_ENSURE(PartView.Slices, "Missing slices in Result stage");
            
            return std::move(PartView);
        }

        static TEpoch GrabEpoch(const TPartComponents &pc)
        {
            Y_ENSURE(pc.PageCollectionComponents, "PartComponents should have at least one pageCollectionComponent");
            Y_ENSURE(pc.PageCollectionComponents[0].RawMeta, "PartComponents should have raw meta data");

            const auto &comp = pc.PageCollectionComponents[0];
            NPageCollection::TMeta meta(TSharedData(comp.RawMeta), comp.LargeGlobId.Group);

            for (ui32 page = meta.TotalPages(); page--;) {
                if (meta.GetPageType(page) == ui32(EPage::Schem2)
                    || meta.GetPageType(page) == ui32(EPage::Scheme))
                {
                    TProtoBox<NProto::TRoot> root(meta.GetPageInplaceData(page));

                    Y_ENSURE(root.HasEpoch());

                    return TEpoch(root.GetEpoch());
                }
            }

            Y_TABLET_ERROR("Cannot locate part metadata in page collections of PartComponents");
        }

        static TLogoBlobID BlobsLabelFor(const TLogoBlobID &base) noexcept
        {
            /* By convention IPageCollection blobs label for page collection has the same logo
                as the meta logo but Cookie + 1. Blocks writer always make a
                gap between two subsequent meta TLargeGlobIds thus this value does
                not overlap any real TLargeGlobId leader blob.
             */

            return
                TLogoBlobID(
                    base.TabletID(), base.Generation(), base.Step(),
                    base.Channel(), 0 /* size */, base.Cookie() + 1);
        }

    private:
        TPartSlots GetSlots() const noexcept
        {
            return TPartSlots(Max(BTreeGroupIndexes.size(), FlatGroupIndexes.size()), PartLayout);
        }

        bool HasBasics() const noexcept
        {
            return SchemeId != Max<TPageId>() && 
                (FlatGroupIndexes || BTreeGroupIndexes);
        }

        void ParseMeta(TArrayRef<const char> plain)
        {
            TMemoryInput stream(plain.data(), plain.size());
            bool parsed = Root.ParseFromArcadiaStream(&stream);
            Y_ENSURE(parsed && stream.Skip(1) == 0, "Cannot parse TPart meta");
            Y_ENSURE(Root.HasEpoch(), "TPart meta has no epoch info");
        }

        void StageParseMeta();
        TFetch StageCreatePartView(bool preloadIndex);
        TFetch StageSliceBounds();
        void StageDeltas();
        TFetch StagePreloadData();

    private:
        TVector<TIntrusivePtr<TPageCollection>> PageCollections;
        TPartComponents Components;
        const TString Legacy;
        const TString Opaque;
        const TVector<TString> Deltas;
        const TEpoch Epoch;
        EStage Stage = EStage::Meta;
        bool Rooted = false; /* Has full topology metablob */
        TPageId SchemeId = Max<TPageId>();
        TPageId GlobsId = Max<TPageId>();
        TPageId LargeId = Max<TPageId>();
        TPageId SmallId = Max<TPageId>();
        TPageId ByKeyId = Max<TPageId>();
        struct TByKeyPrefixMeta {
            TPageId PageId = Max<TPageId>();
            ui32 PrefixColumns = 0;
        };
        TVector<TByKeyPrefixMeta> ByKeyPrefixMetas;
        TPageId GarbageStatsId = Max<TPageId>();
        TPageId TxIdStatsId = Max<TPageId>();
        TVector<TPageId> FlatGroupIndexes;
        TVector<TPageId> FlatHistoricIndexes;
        TVector<NPage::TBtreeIndexMeta> BTreeGroupIndexes;
        TVector<NPage::TBtreeIndexMeta> BTreeHistoricIndexes;
        TRowVersion MinRowVersion;
        TRowVersion MaxRowVersion;
        TPartLayout PartLayout;
        TVector<NPage::TGroupId> StrippedV1Shadows; // groups with a stripped V1 shadow of the V2 b-tree index
        NProto::TRoot Root;
        TPartView PartView;
        THolder<TLoaderEnv> LoaderEnv;
        struct TPreloadBTreeWalker {
            THolder<TBTreePartWalker> Walker;
            NPage::TGroupId GroupId;
            bool SkipDataPages = false;
        };
        TVector<TPreloadBTreeWalker> PreloadBTreeWalkers;
    };
}}
