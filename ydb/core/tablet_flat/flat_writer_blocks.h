#pragma once

#include "flat_sausagecache.h"
#include "flat_sausage_packet.h"
#include "flat_sausage_writer.h"
#include "flat_sausage_solid.h"
#include "flat_part_loader.h"
#include "flat_writer_banks.h"

namespace NKikimr {
namespace NTabletFlatExecutor {
namespace NWriter {

    class TBlocks {
    public:
        using ECache = NTable::NPage::ECache;
        using EPage = NTable::NPage::EPage;
        using TPageId = NTable::NPage::TPageId;
        using TCache = TPrivatePageCache::TInfo;

        TBlocks(ICone *cone, ui8 channel, ECache cache, ui32 block, bool stickyFlatIndex)
            : Cone(cone)
            , Channel(channel)
            , Cache(cache)
            , StickyFlatIndex(stickyFlatIndex)
            , Writer(Cone->CookieRange(1), Channel, block)
        {

        }

        ~TBlocks()
        {
            Y_ABORT_UNLESS(!Writer.Grab(), "Block writer still has some blobs");
        }

        explicit operator bool() const noexcept
        {
            return Writer || Regular || Sticky;
        }

        TIntrusivePtr<TCache> Finish() noexcept
        {
            TIntrusivePtr<TCache> pageCollection;

            if (auto meta = Writer.Finish(false /* omit empty page collection */)) {
                for (auto &glob : Writer.Grab())
                    Cone->Put(std::move(glob));

                pageCollection = MakePageCollection(std::move(meta));
            }

            Y_ABORT_UNLESS(!Writer, "Block writer is not empty after Finish");
            Y_ABORT_UNLESS(!Regular && !Sticky, "Unexpected non-empty page lists");

            return pageCollection;
        }

        TPageId Write(TSharedData raw, EPage type)
        {
            auto pageId = Writer.AddPage(raw, (ui32)type);

            for (auto &glob : Writer.Grab())
                Cone->Put(std::move(glob));

            if (NTable::TLoader::NeedIn(type) || StickyFlatIndex && type == EPage::FlatIndex) {
                // Note: we mark flat index pages sticky after we load them
                Sticky.emplace_back(pageId, std::move(raw));
            } else if (bool(Cache) && type == EPage::DataPage || type == EPage::BTreeIndex) {
                // Note: we save b-tree index pages to shared cache regardless of a cache mode  
                Regular.emplace_back(pageId, std::move(raw));
            }

            return pageId;
        }

        void WriteInplace(TPageId page, TArrayRef<const char> body)
        {
            Writer.AddInplace(page, body);
        }

    private:
        TIntrusivePtr<TCache> MakePageCollection(TSharedData body) noexcept
        {
            auto largeGlobId = CutToChunks(body);

            auto *pack = new NPageCollection::TPageCollection(largeGlobId, std::move(body));

            TIntrusivePtr<TCache> cache = new TCache(pack);

            const bool sticky = (Cache == ECache::Ever);

            for (auto &paged : Sticky) cache->Fill(paged, true);
            for (auto &paged : Regular) cache->Fill(paged, sticky);

            Sticky.clear();
            Regular.clear();

            return cache;
        }

        NPageCollection::TLargeGlobId CutToChunks(TArrayRef<const char> body)
        {
            return Cone->Put(0, Channel, body, Writer.MaxBlobSize);
        }

    private:
        ICone * const Cone = nullptr;
        const ui8 Channel = Max<ui8>();
        const ECache Cache = ECache::None;
        const bool StickyFlatIndex;

        NPageCollection::TWriter Writer;
        TVector<NPageCollection::TLoadedPage> Regular;
        TVector<NPageCollection::TLoadedPage> Sticky;
    };
}
}
}
