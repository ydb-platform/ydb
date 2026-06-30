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
        using ECacheMode = NTable::NPage::ECacheMode;
        using EPage = NTable::NPage::EPage;
        using TPageId = NTable::NPage::TPageId;
        using TPageOffset = NTable::NPage::TPageOffset;
        using TPageCollection = TPrivatePageCache::TPageCollection;

        struct TResult : TMoveOnly {
            TIntrusiveConstPtr<NPageCollection::IPageCollection> PageCollection;
            TVector<NPageCollection::TLoadedPage> RegularPages;
            TVector<NPageCollection::TLoadedPage> StickyPages;
        };

        TBlocks(ICone *cone, ui8 channel, ECache cache, ECacheMode cacheMode, ui32 block, bool stickyFlatIndex, bool isOuter = false)
            : Cone(cone)
            , Channel(channel)
            , Cache(cache)
            , CacheMode(cacheMode)
            , StickyFlatIndex(stickyFlatIndex)
            , IsOuter(isOuter)
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

                if (IsOuter) {
                    Result.PageCollection = MakeIntrusiveConst<NPageCollection::TOuterPageCollection>(largeGlobId, std::move(meta));
                } else {
                    Result.PageCollection = MakeIntrusiveConst<NPageCollection::TPageCollection>(largeGlobId, std::move(meta));
                }
            }

            Y_ENSURE(!Writer, "Block writer is not empty after Finish");

            Offset = 0;
            WrittenPageCount = 0;
            return std::exchange(Result, {});
        }

        TPageOffset Write(TSharedData raw, EPage type)
        {
            ui32 crc32 = 0;

            Writer.AddPage(raw, (ui32)type, &crc32);

            for (auto &glob : Writer.Grab()) {
                Cone->Put(std::move(glob));
            }

            TPageLocation location;
            if (IsOuter) {
                location = TPageLocation::FromPageIndex(WrittenPageCount, raw.size(), type, crc32);
            }
            else {
                location = TPageLocation::FromByteOffset(Offset, raw.size(), type, crc32);
            }
            Offset += raw.size();
            WrittenPageCount++;

            if (NTable::TLoader::NeedIn(type) || Cache == ECache::Ever || StickyFlatIndex && type == EPage::FlatIndex) {
                Result.StickyPages.emplace_back(location, std::move(raw));
            } else if (bool(Cache) && type == EPage::DataPage || type == EPage::BTreeIndex || CacheMode == ECacheMode::TryKeepInMemory) {
                // TODO: take into account memory limits for TryKeepInMemory mode
                // Note: save b-tree index pages to shared cache regardless of a cache mode
                Result.RegularPages.emplace_back(location, std::move(raw));
            }

            return location.Offset;
        }

        ui32 GetWrittenPageId(ui32 /*group*/) const noexcept
        {
            return WrittenPageCount - 1;
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
        const ECacheMode CacheMode = ECacheMode::Regular;
        const bool StickyFlatIndex;
        const bool IsOuter;

        NPageCollection::TWriter Writer;
        TResult Result;
        ui64 Offset = 0;
        ui32 WrittenPageCount = 0;
    };
}
}
}
