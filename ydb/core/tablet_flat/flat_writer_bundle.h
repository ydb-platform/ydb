#pragma once

#include "flat_part_iface.h"
#include "flat_writer_conf.h"
#include "flat_writer_banks.h"
#include "flat_writer_blocks.h"
#include "util_basics.h"
#include "util_channel.h"
#include "util_fmt_abort.h"

namespace NKikimr {
namespace NTabletFlatExecutor {
namespace NWriter {

    class TBundle : public NTable::IPageWriter, protected ICone {
    public:
        struct TResult {
            TVector<TBlocks::TResult> PageCollections;
            TDeque<NTable::TScreen::THole> Growth;
            TString Overlay;
        };

        TBundle(const TLogoBlobID &base, const TConf &conf)
            : Groups(conf.Groups)
            , BlobsChannels(conf.BlobsChannels)
            , ExtraChannel(conf.ExtraChannel)
            , ChannelsShares(conf.ChannelsShares)
            , Banks(base, conf.Slots)
        {
            Y_ENSURE(Groups.size() >= 1, "There must be at least one page collection group");

            const auto none = NTable::NPage::ECache::None;

            Blocks.resize(Groups.size() + 1);
            for (size_t group : xrange(Groups.size())) {
                Blocks[group].Reset(
                    new TBlocks(this, Groups[group].Channel, Groups[group].Cache, Groups[group].MaxBlobSize, conf.StickyFlatIndex));
            }
            Blocks[Groups.size()].Reset(new TBlocks(this, conf.OuterChannel, none, Groups[0].MaxBlobSize, conf.StickyFlatIndex));

            Growth = new NTable::TScreen::TCook;
        }

        TVector<NPageCollection::TGlob> GetBlobsToSave() noexcept
        {
            return std::exchange(Blobs, { });
        }

        TVector<TResult> Results()
        {
            for (auto &blocks : Blocks) {
                Y_ENSURE(!*blocks, "Bundle writer has unflushed data");
            }

            return std::move(Results_);
        }

        NPageCollection::TLargeGlobId WriteExtra(TArrayRef<const char> body)
        {
            return Put(/* data cookieRange */ 1, ExtraChannel, body, Groups[0].MaxBlobSize);
        }

    private:
        TPageId Write(TSharedData page, EPage type, ui32 group) override
        {
            return Blocks.at(group)->Write(std::move(page), type);
        }

        TPageId WriteOuter(TSharedData page) override
        {
            return
                Blocks.back()->Write(std::move(page), EPage::Opaque);
        }

        void WriteInplace(TPageId page, TArrayRef<const char> body) override
        {
            Blocks[0]->WriteInplace(page, body);
        }

        NPageCollection::TGlobId WriteLarge(TString blob, ui64 ref) override
        {
            ui8 bestChannel = ChannelsShares.Select(BlobsChannels);
            
            auto glob = Banks.Data.Do(bestChannel, blob.size());

            Blobs.emplace_back(glob, std::move(blob));
            Growth->Pass(ref);

            return glob;
        }

        void Finish(TString overlay) override
        {
            auto &result = Results_.emplace_back();

            for (auto num : xrange(Blocks.size())) {
                if (auto written = Blocks[num]->Finish(); written.PageCollection) {
                    result.PageCollections.emplace_back(std::move(written));
                } else if (num < Blocks.size() - 1) {
                    Y_TABLET_ERROR("Finish produced an empty main page collection");
                }

                Y_ENSURE(!*Blocks[num], "Block writer has unexpected data");
            }

            Y_ENSURE(result.PageCollections, "Finish produced no page collections");

            result.Growth = Growth->Unwrap();
            result.Overlay = overlay;
        }

        NPageCollection::TCookieAllocator& CookieRange(ui32 cookieRange) override
        {
            Y_ENSURE(cookieRange == 0 || cookieRange == 1, "Invalid cookieRange requested");

            return cookieRange == 0 ? Banks.Meta : Banks.Data;
        }

        void Put(NPageCollection::TGlob&& glob) override
        {
            Blobs.emplace_back(std::move(glob));
        }

        NPageCollection::TLargeGlobId Put(ui32 cookieRange, ui8 channel, TArrayRef<const char> body, ui32 block) override
        {
            const auto largeGlobId = CookieRange(cookieRange).Do(channel, body.size(), block);

            size_t offset = 0;
            size_t left = body.size();
            for (const auto& blobId : largeGlobId.Blobs()) {
                const NPageCollection::TGlobId glob(blobId, largeGlobId.Group);
                const auto chunk = glob.Bytes();
                const auto slice = body.Slice(offset, chunk);

                Put({ glob, TString(slice.data(), slice.size()) });

                offset += chunk;
                left -= chunk;

                Y_ENSURE(chunk && (chunk == block || left == 0));
            }

            Y_ENSURE(offset == body.size());
            Y_ENSURE(left == 0);
            return largeGlobId;
        }

    private:
        const TVector<TConf::TGroup> Groups;
        const TVector<ui8> BlobsChannels;
        const ui8 ExtraChannel;
        const NUtil::TChannelsShares& ChannelsShares;
        TBanks Banks;
        TVector<NPageCollection::TGlob> Blobs;
        TVector<THolder<TBlocks>> Blocks;
        TAutoPtr<NTable::TScreen::TCook> Growth;
        TVector<TResult> Results_;
    };

}
}
}
