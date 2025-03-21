#pragma once
#include "defs.h"
#include "flat_part_store.h"
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

        using TCache = NTabletFlatExecutor::TPrivatePageCache::TInfo;

        struct TLoaderEnv : public IPages {
            TLoaderEnv(TIntrusivePtr<TCache> cache)
                : Cache(std::move(cache))
            {
            }

            TResult Locate(const TMemTable*, ui64, ui32) override
            {
                Y_ABORT("IPages::Locate(TMemTable*, ...) shouldn't be used here");
            }

            TResult Locate(const TPart*, ui64, ELargeObj) override
            {
                Y_ABORT("IPages::Locate(TPart*, ...) shouldn't be used here");
            }

            void ProvidePart(const TPart* part)
            {
                Y_ABORT_IF(Part);
                Part = part;
            }

            const TSharedData* TryGetPage(const TPart* part, TPageId pageId, TGroupId groupId) override
            {
                Y_ABORT_UNLESS(part == Part, "Unsupported part");
                Y_ABORT_UNLESS(groupId.IsMain(), "Unsupported column group");

                auto savedPage = SavedPages.find(pageId);
                
                if (savedPage == SavedPages.end()) {
                    if (auto cachedPage = Cache->GetPage(pageId); cachedPage) {
                        if (auto sharedPageRef = cachedPage->SharedBody; sharedPageRef && sharedPageRef.Use()) {
                            // Save page in case it's evicted on the next iteration
                            AddSavedPage(pageId, std::move(sharedPageRef));
                            savedPage = SavedPages.find(pageId);
                        }
                    }
                }

                if (savedPage != SavedPages.end()) {
                    return &savedPage->second;
                } else {
                    NeedPages.insert(pageId);
                    return nullptr;
                }
            }

            void EnsureNoNeedPages() const
            {
                Y_ABORT_UNLESS(!NeedPages);
            }

            TAutoPtr<NPageCollection::TFetch> GetFetch()
            {
                if (NeedPages) {
                    TVector<TPageId> pages(NeedPages.begin(), NeedPages.end());
                    std::sort(pages.begin(), pages.end());
                    return new NPageCollection::TFetch{ 0, Cache->PageCollection, std::move(pages) };
                } else {
                    return nullptr;
                }
            }

            void Save(ui32 cookie, NSharedCache::TEvResult::TLoaded&& loaded)
            {
                if (cookie == 0 && NeedPages.erase(loaded.PageId)) {
                    auto pageType = Cache->GetPageType(loaded.PageId);
                    bool sticky = NeedIn(pageType) || pageType == EPage::FlatIndex;
                    AddSavedPage(loaded.PageId, loaded.Page);
                    Cache->Fill(loaded.PageId, std::move(loaded.Page), sticky);
                }
            }

        private:
            void AddSavedPage(TPageId pageId, NSharedCache::TSharedPageRef page)
            {
                SavedPages[pageId] = NSharedCache::TPinnedPageRef(page).GetData();
                SavedPagesRefs.emplace_back(std::move(page));
            }

            const TPart* Part = nullptr;
            TIntrusivePtr<TCache> Cache;
            THashMap<TPageId, TSharedData> SavedPages;
            TVector<NSharedCache::TSharedPageRef> SavedPagesRefs;
            THashSet<TPageId> NeedPages;
        };

        struct TRunOptions {
            // Marks that optional index pages should be loaded
            //
            // Effects only b-tree index as flat index is kept as sticky
            bool PreloadIndex = true;

            // Marks that all data pages from the main group should be loaded
            bool PreloadData = false;
        };

        TLoader(TPartComponents ou)
            : TLoader(TPartStore::Construct(std::move(ou.PageCollectionComponents)),
                    std::move(ou.Legacy),
                    std::move(ou.Opaque),
                    /* no deltas */ { },
                    ou.Epoch)
        {

        }

        TLoader(TVector<TIntrusivePtr<TCache>>, TString legacy, TString opaque,
                TVector<TString> deltas = { },
                TEpoch epoch = NTable::TEpoch::Max());
        ~TLoader();

        TVector<TAutoPtr<NPageCollection::TFetch>> Run(TRunOptions options)
        {
            while (Stage < EStage::Result) {
                TAutoPtr<NPageCollection::TFetch> fetch;

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
                    if (!fetch->Pages) {
                        Y_Fail("TLoader is trying to fetch 0 pages");
                    }
                    return { fetch };
                }

                Stage = EStage(ui8(Stage) + 1);
            }

            return { };
        }

        void Save(ui64 cookie, TArrayRef<NSharedCache::TEvResult::TLoaded>);

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
            Y_ABORT_UNLESS(Stage == EStage::Result);
            Y_ABORT_UNLESS(PartView, "Result may only be grabbed once");
            Y_ABORT_UNLESS(PartView.Slices, "Missing slices in Result stage");
            
            return std::move(PartView);
        }

        static TEpoch GrabEpoch(const TPartComponents &pc)
        {
            Y_ABORT_UNLESS(pc.PageCollectionComponents, "PartComponents should have at least one pageCollectionComponent");
            Y_ABORT_UNLESS(pc.PageCollectionComponents[0].Packet, "PartComponents should have a parsed meta pageCollectionComponent");

            const auto &meta = pc.PageCollectionComponents[0].Packet->Meta;

            for (ui32 page = meta.TotalPages(); page--;) {
                if (meta.GetPageType(page) == ui32(EPage::Schem2)
                    || meta.GetPageType(page) == ui32(EPage::Scheme))
                {
                    TProtoBox<NProto::TRoot> root(meta.GetPageInplaceData(page));

                    Y_ABORT_UNLESS(root.HasEpoch());

                    return TEpoch(root.GetEpoch());
                }
            }

            Y_ABORT("Cannot locate part metadata in page collections of PartComponents");
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
        bool HasBasics() const noexcept
        {
            return SchemeId != Max<TPageId>() && 
                (FlatGroupIndexes || BTreeGroupIndexes);
        }

        void ParseMeta(TArrayRef<const char> plain)
        {
            TMemoryInput stream(plain.data(), plain.size());
            bool parsed = Root.ParseFromArcadiaStream(&stream);
            Y_ABORT_UNLESS(parsed && stream.Skip(1) == 0, "Cannot parse TPart meta");
            Y_ABORT_UNLESS(Root.HasEpoch(), "TPart meta has no epoch info");
        }

        void StageParseMeta();
        TAutoPtr<NPageCollection::TFetch> StageCreatePartView(bool preloadIndex);
        TAutoPtr<NPageCollection::TFetch> StageSliceBounds();
        void StageDeltas();
        TAutoPtr<NPageCollection::TFetch> StagePreloadData();

    private:
        TVector<TIntrusivePtr<TCache>> Packs;
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
        TPageId GarbageStatsId = Max<TPageId>();
        TPageId TxIdStatsId = Max<TPageId>();
        TVector<TPageId> FlatGroupIndexes;
        TVector<TPageId> FlatHistoricIndexes;
        TVector<NPage::TBtreeIndexMeta> BTreeGroupIndexes;
        TVector<NPage::TBtreeIndexMeta> BTreeHistoricIndexes;
        TRowVersion MinRowVersion;
        TRowVersion MaxRowVersion;
        NProto::TRoot Root;
        TPartView PartView;
        THolder<TLoaderEnv> LoaderEnv;
    };
}}
