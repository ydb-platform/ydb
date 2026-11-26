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
#include <ydb/library/actors/core/actorid.h>

namespace NKikimr {
namespace NTable {

    class TLoader;

    enum class EMetadataLoadResult {
        Ignored,
        Pending,
        Ready,
        Error,
    };

    // Hosts and callbacks are borrowed for this call; only address data may be retained
    class TMetadataLoadContext {
    public:
        template <class THost>
        TMetadataLoadContext(THost& host, NActors::TActorSystem* system, NActors::TActorId replyTo, ui64 cookie,
            NTabletFlatExecutor::NBlockIO::EPriority priority,
            NTabletFlatExecutor::IPageMetadataLoadGroup* group = nullptr) noexcept
            : Host_(&host)
            , LoadRaw_(&InvokeLoadRaw<THost>)
            , SendRequest_(&InvokeSendRequest<THost>)
            , SendUpdate_(&InvokeSendUpdate<THost>)
            , System(system)
            , ReplyTo(replyTo)
            , Cookie(cookie)
            , Priority(priority)
            , Group(group)
        {}

        void LoadRaw(ui32 slot, const NPageCollection::TLargeGlobId& id) const {
            LoadRaw_(Host_, slot, id);
        }

        bool SendRequest(ui32 slot, const void* request, THolder<NActors::IEventBase> event) const {
            return SendRequest_(Host_, slot, request, std::move(event));
        }

        void SendUpdate(THolder<NActors::IEventBase> event) const {
            SendUpdate_(Host_, std::move(event));
        }

    private:
        template <class THost>
        static void InvokeLoadRaw(void* context, ui32 slot, const NPageCollection::TLargeGlobId& id) {
            static_cast<THost*>(context)->LoadRawMetadata(slot, id);
        }

        template <class THost>
        static bool InvokeSendRequest(
            void* context, ui32 slot, const void* request, THolder<NActors::IEventBase> event) {
            return static_cast<THost*>(context)->SendMetadataRequest(slot, request, std::move(event));
        }

        template <class THost>
        static void InvokeSendUpdate(void* context, THolder<NActors::IEventBase> event) {
            static_cast<THost*>(context)->SendMetadataUpdate(std::move(event));
        }

        void* Host_;
        void (*LoadRaw_)(void*, ui32, const NPageCollection::TLargeGlobId&);
        bool (*SendRequest_)(void*, ui32, const void*, THolder<NActors::IEventBase>);
        void (*SendUpdate_)(void*, THolder<NActors::IEventBase>);

    public:
        NActors::TActorSystem* const System;
        const NActors::TActorId ReplyTo;
        const ui64 Cookie;
        const NTabletFlatExecutor::NBlockIO::EPriority Priority;
        // The environment retains the group if it needs it beyond this call
        NTabletFlatExecutor::IPageMetadataLoadGroup* const Group;
    };

    struct TLoaderFetch : TMoveOnly {
        TIntrusiveConstPtr<NPageCollection::IPageCollection> PageCollection;
        TVector<TPageLocation> Pages;

        explicit operator bool() const {
            return bool(Pages);
        }
    };

    struct IPartLoaderEnv : public IPages {
        // The root callback publishes the frozen selection for all available slots
        // Invoke it after each arriving collection once slot 0 is available; do not retain it
        virtual void BeginMetadata(TArrayRef<TPageCollectionComponents> components, const TMetadataLoadContext& context,
            TFunctionRef<bool()> prepareRoot) = 0;
        virtual EMetadataLoadResult SaveMetadata(
            TArrayRef<TPageCollectionComponents> components, ui32 slot, TSharedData data) = 0;
        virtual EMetadataLoadResult AcceptMetadata(ui32 slot,
            const NTabletFlatExecutor::TPageCacheMetadataResult& result, NActors::IEventBase& event,
            const TMetadataLoadContext& context, TFunctionRef<bool()> prepareRoot) = 0;
        virtual bool MetadataReady() const noexcept = 0;
        virtual void CancelMetadata() noexcept = 0;
        virtual const NTabletFlatExecutor::IPageCacheProtocol& Protocol() const noexcept = 0;
        virtual THolder<NActors::IEventBase> MakeRequest(
            NTabletFlatExecutor::NBlockIO::EPriority priority, TLoaderFetch fetch, ui64 cookie) const = 0;
        virtual ui32 GetCollectionCount() const noexcept = 0;
        virtual void ResizeCollections(ui32 count) = 0;
        virtual bool HasCollection(ui32 slot) const noexcept = 0;
        virtual const TIntrusiveConstPtr<NPageCollection::IPageCollection>& GetCollection(ui32 slot) const = 0;
        virtual void PrepareCollection(ui32 slot, TPageCollectionComponents& components, bool outer) = 0;
        virtual void Initialize(TPartSlots slots) = 0;
        virtual TPartStore* CreatePart(TPart::TParams params, TPart::TStat stat) = 0;
        virtual void ProvidePart(const TPart* part) = 0;
        virtual const TSharedData* TryGetSlotPage(ui32 slot, const TPageLocation& location) = 0;
        virtual void EnsureNoNeedPages() const = 0;
        virtual TLoaderFetch GetFetch() = 0;
        virtual void Save(NActors::IEventBase& event) = 0;
    };

    class TLoader {
    public:
        enum class EStage : ui8 {
            Root,
            Meta,
            PartView,
            Slice,
            Deltas,
            PreloadData,
            Result,
        };

        using TPageCollection = NTabletFlatExecutor::TPrivatePageCache::TPageCollection;

        using TFetch = TLoaderFetch;
        using IEnv = IPartLoaderEnv;

        // Legacy cache environment retained as the default implementation
        struct TLoaderEnv : public IEnv {
            enum class EMetadataState : ui8 {
                Idle,
                Loading,
                Cancelled,
            };

            void BeginMetadata(TArrayRef<TPageCollectionComponents> components, const TMetadataLoadContext& context,
                TFunctionRef<bool()> prepareRoot) override;
            EMetadataLoadResult SaveMetadata(
                TArrayRef<TPageCollectionComponents> components, ui32 slot, TSharedData data) override;

            EMetadataLoadResult AcceptMetadata(ui32, const NTabletFlatExecutor::TPageCacheMetadataResult&,
                NActors::IEventBase&, const TMetadataLoadContext&, TFunctionRef<bool()>) override {
                return EMetadataLoadResult::Ignored;
            }

            bool MetadataReady() const noexcept override {
                return MetadataState == EMetadataState::Loading && MetadataRemaining == 0;
            }

            void CancelMetadata() noexcept override {
                MetadataState = EMetadataState::Cancelled;
            }

            const NTabletFlatExecutor::IPageCacheProtocol& Protocol() const noexcept override {
                return NTabletFlatExecutor::GetPrivatePageCacheProtocol();
            }

            THolder<NActors::IEventBase> MakeRequest(
                NTabletFlatExecutor::NBlockIO::EPriority priority, TFetch fetch, ui64 cookie) const override {
                return Protocol().MakePageRequest(
                    priority, std::move(fetch.PageCollection), std::move(fetch.Pages), cookie);
            }

            explicit TLoaderEnv(TVector<TIntrusivePtr<TPageCollection>> pageCollections)
                : PageCollections(std::move(pageCollections))
                , Slots(0)
            {}

            ui32 GetCollectionCount() const noexcept override {
                return Collections().size();
            }

            void ResizeCollections(ui32 count) override {
                PageCollections.resize(count);
            }

            bool HasCollection(ui32 slot) const noexcept override {
                return bool(Collections()[slot]);
            }

            const TIntrusiveConstPtr<NPageCollection::IPageCollection>& GetCollection(ui32 slot) const override {
                return Collections()[slot]->PageCollection;
            }

            void PrepareCollection(ui32 slot, TPageCollectionComponents& components, bool outer) override;
            void Initialize(TPartSlots slots) override;
            TPartStore* CreatePart(TPart::TParams params, TPart::TStat stat) override;
            void Save(NActors::IEventBase& event) override;

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

            void ProvidePart(const TPart* part) override
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
            const TSharedData* TryGetSlotPage(ui32 slot, const TPageLocation& location) override
            {
                Y_ENSURE(slot < Slots.SlotsCount(), "Unsupported page collection " << slot);

                auto& savedPages = SavedPages[slot];
                auto savedPage = savedPages.find(location.Offset);

                if (savedPage == savedPages.end()) {
                    if (auto cachedPage = Collections()[slot]->FindPage(location.Offset); cachedPage) {
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

            void EnsureNoNeedPages() const override
            {
                for (const auto& needPages : NeedPages) {
                    Y_ENSURE(!needPages);
                }
            }

            /**
             * Returns missing pages of a single page collection: only one fetch may be outstanding and its pages
             * are saved before the next fetch. Pages of other page collections are requested by the next fetches
             * when the stage is run again, so stages must be safe to re-run
             */
            TFetch GetFetch() override
            {
                for (ui32 slot : xrange(NeedPages.size())) {
                    if (const auto& needPages = NeedPages[slot]) {
                        TVector<TPageLocation> pages(needPages.begin(), needPages.end());
                        std::sort(pages.begin(), pages.end());
                        FetchSlot = slot;
                        return {
                            .PageCollection = Collections()[slot]->PageCollection,
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
                    Collections()[FetchSlot]->AddStickyPage(loaded.Offset, loaded.Size, std::move(loaded.Page));
                } else {
                    Collections()[FetchSlot]->AddPage(loaded.Offset, loaded.Size, std::move(loaded.Page));
                }
            }

        private:
            const TVector<TIntrusivePtr<TPageCollection>>& Collections() const noexcept {
                return PageCollections.empty() ? RetainedCollections : PageCollections;
            }

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

            EMetadataState MetadataState = EMetadataState::Idle;
            size_t MetadataRemaining = 0;
            const TPart* Part = nullptr;
            TVector<TIntrusivePtr<TPageCollection>> PageCollections;
            TVector<TIntrusivePtr<TPageCollection>> RetainedCollections;
            TPartSlots Slots;
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
        TLoader(THolder<IEnv> environment, TPartComponents components);
        ~TLoader();

        void BeginMetadata(const TMetadataLoadContext& context);
        EMetadataLoadResult SaveMetadata(ui32 slot, TSharedData data);
        EMetadataLoadResult AcceptMetadata(ui32 slot, const NTabletFlatExecutor::TPageCacheMetadataResult& result,
            NActors::IEventBase& event, const TMetadataLoadContext& context);

        bool MetadataReady() const noexcept {
            return LoaderEnv->MetadataReady();
        }

        void CancelMetadata() noexcept {
            LoaderEnv->CancelMetadata();
        }

        // Prepare the layout from slot 0 before other collection metadata arrives
        // Descriptor index flags are applied when all collection metadata is available
        void PrepareRoot();

        bool HasOuterCollection() const {
            Y_DEBUG_ABORT_UNLESS(Stage != EStage::Root);
            return SmallId != Max<TPageId>();
        }

        THolder<NActors::IEventBase> MakeRequest(
            NTabletFlatExecutor::NBlockIO::EPriority priority, TFetch fetch, ui64 cookie = 0) const {
            return LoaderEnv->MakeRequest(priority, std::move(fetch), cookie);
        }

        TFetch Run(TRunOptions options)
        {
            while (Stage < EStage::Result) {
                TFetch fetch;

                switch (Stage) {
                    case EStage::Root:
                        PrepareRoot();
                        continue;
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

        void Save(NActors::IEventBase& event);

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

        bool PrepareMetadataRoot();
        void StageParseMeta();
        TFetch StageCreatePartView(bool preloadIndex);
        TFetch StageSliceBounds();
        void StageDeltas();
        TFetch StagePreloadData();

    private:
        TPartComponents Components;
        const TString Legacy;
        const TString Opaque;
        const TVector<TString> Deltas;
        const TEpoch Epoch;
        EStage Stage = EStage::Root;
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
        THolder<IEnv> LoaderEnv;
        struct TPreloadBTreeWalker {
            THolder<TBTreePartWalker> Walker;
            NPage::TGroupId GroupId;
            bool SkipDataPages = false;
        };
        TVector<TPreloadBTreeWalker> PreloadBTreeWalkers;
    };
}}
