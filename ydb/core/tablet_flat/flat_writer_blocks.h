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
        using TPageCollection = TPrivatePageCache::TPageCollection;

        struct TResult : TMoveOnly {
            TIntrusiveConstPtr<NPageCollection::IPageCollection> PageCollection;
            TVector<NPageCollection::TLoadedPage> RegularPages;
            TVector<NPageCollection::TLoadedPage> StickyPages;
        };

        TBlocks(ICone *cone, ui8 channel, ECache cache, ui32 block, bool stickyFlatIndex)
            : Cone(cone)
            , Channel(channel)
            , Cache(cache)
            , StickyFlatIndex(stickyFlatIndex)
            , Writer(Cone->CookieRange(1), Channel, block)
        {
        }

        explicit operator bool() const noexcept
        {
            return Writer || Result.RegularPages || Result.StickyPages;
        }

        TResult Finish()
        {
            if (auto meta = Writer.Finish(false /* omit empty page collection */)) {
                for (auto &glob : Writer.Grab()) {
                    Cone->Put(std::move(glob));
                }

                auto largeGlobId = CutToChunks(meta);
                Result.PageCollection = MakeIntrusiveConst<NPageCollection::TPageCollection>(largeGlobId, std::move(meta));
            }

            Y_ENSURE(!Writer, "Block writer is not empty after Finish");

            return std::exchange(Result, {});
        }

        TPageId Write(TSharedData raw, EPage type)
        {
            auto pageId = Writer.AddPage(raw, (ui32)type);

            for (auto &glob : Writer.Grab()) {
                Cone->Put(std::move(glob));
            }

            if (NTable::TLoader::NeedIn(type) || Cache == ECache::Ever || StickyFlatIndex && type == EPage::FlatIndex) {
                Result.StickyPages.emplace_back(pageId, std::move(raw));
            } else if (bool(Cache) && type == EPage::DataPage || type == EPage::BTreeIndex) {
                // Note: save b-tree index pages to shared cache regardless of a cache mode
                Result.RegularPages.emplace_back(pageId, std::move(raw));
            }

            return pageId;
        }

        void WriteInplace(TPageId page, TArrayRef<const char> body)
        {
            Writer.AddInplace(page, body);
        }

    private:
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
        TResult Result;
    };
}
}
}
