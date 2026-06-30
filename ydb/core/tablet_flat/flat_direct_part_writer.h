#pragma once

#include "flat_writer_bundle.h"
#include "flat_part_writer.h"
#include "flat_part_loader.h"
#include "flat_row_scheme.h"
#include "flat_row_state.h"
#include "flat_part_laid.h"
#include "flat_part_screen.h"
#include "flat_part_store.h"
#include "flat_sausagecache.h"
#include "flat_sausage_chop.h"
#include "shared_cache_pages.h"
#include "shared_sausagecache.h"
#include "util_fmt_abort.h"

#include <bitset>

#include <ydb/core/base/blobstorage.h>
#include <ydb/core/base/blobstorage_relevance.h>
#include <ydb/core/base/blobstorage_write_source.h>
#include <ydb/core/base/appdata.h>
#include <ydb/core/scheme/scheme_tablecell.h>
#include <ydb/library/actors/core/actor.h>

namespace NKikimr {
namespace NTabletFlatExecutor {

    /**
     * Configuration for a direct part write: how to lay out pages and which
     * channels/groups to write blobs to. Built by the executor from the table
     * scheme, mirroring the writer/layout config used for compaction.
     */
    struct TDirectWriteCfg {
        NTable::NPage::TConf Layout;
        NWriter::TConf Writer;
        NTable::TEpoch Epoch = NTable::TEpoch::Zero();
    };

    /**
     * Product of a finished direct part write, handed to the commit transaction
     * (via IExecuting::AttachPart) to be merged into the table as a bottom layer.
     */
    struct TDirectPartResult : public IDestructable {
        // One or more produced parts (a single part unless split keys are used).
        TVector<NTable::TPartView> Parts;
        // External blob growth holes, parallel to Parts.
        TVector<TDeque<NTable::TScreen::THole>> Growth;
        // Reserved step whose GC barrier protects the written blobs until commit.
        ui32 Step = Max<ui32>();
        // Yellow channels observed while writing blobs.
        TVector<ui32> YellowMoveChannels;
        TVector<ui32> YellowStopChannels;
    };

    /**
     * Builds a sorted SST (Part) directly inside the local database, bypassing the
     * memtable and compaction. Created by the executor (which reserves a step and
     * holds a GC barrier) and handed off to an arbitrary actor that:
     *   - feeds rows one at a time in strictly ascending key order via AddRow(),
     *   - forwards TEvPutResult events to Handle(),
     *   - calls Finalize() once all rows are fed and, after IsComplete() becomes
     *     true, ExtractResult() to obtain the part(s) for the commit transaction.
     *
     * Blob writing happens in the background as pending TEvPut requests, exactly
     * like the compaction scan (TOpsCompact); CanFeed() provides backpressure.
     */
    class TDirectPartWriter {
        using TEvPut = TEvBlobStorage::TEvPut;
        using TEvPutResult = TEvBlobStorage::TEvPutResult;
        using TBundle = NWriter::TBundle;
        using TPartWriter = NTable::TPartWriter;
        using TScheme = NTable::TRowScheme;

    public:
        // 20 MiB of blobs in flight, same budget as compaction.
        constexpr static ui64 MaxFlight = 20ll * (1ll << 20);

        TDirectPartWriter(TLogoBlobID mask, TDirectWriteCfg cfg, TIntrusiveConstPtr<TScheme> scheme)
            : Mask_(mask)
            , Cfg(std::move(cfg))
            , Scheme(std::move(scheme))
        {
            Bundle = new TBundle(Mask_, Cfg.Writer);
            SharedCachePages = AppData()->SharedCachePages.Get();
        }

        const TLogoBlobID& Mask() const noexcept { return Mask_; }
        ui32 Step() const noexcept { return Mask_.Step(); }

        bool HasError() const noexcept { return Failed; }
        const TString& Error() const noexcept { return ErrorText; }

        // Backpressure: false means the owner should wait for in-flight puts to
        // complete (TEvPutResult) before feeding more rows.
        bool CanFeed() const noexcept { return !Failed && !Finishing && Flushing < MaxFlight; }

        // True once all rows have been fed (Finalize called) and all blobs acked.
        bool IsComplete() const noexcept { return Finishing && Flushing == 0; }

        /**
         * Append a single committed row. Keys must arrive in strictly ascending
         * order; otherwise the writer fails and returns false. version is baked
         * into the part as the row's commit version.
         */
        bool AddRow(TArrayRef<const TCell> key, const NTable::TRowState& row, TRowVersion version,
                    const TActorContext& ctx)
        {
            if (Failed) {
                return false;
            }
            if (Finishing) {
                return Fail("AddRow called after Finalize");
            }

            if (!CheckKeyOrder(key)) {
                return false;
            }

            if (!Writer) {
                auto *partScheme = new NTable::TPartScheme(Scheme->Cols);
                Writer = new TPartWriter(partScheme, Scheme->Tags(), *Bundle, Cfg.Layout, Cfg.Epoch);
            }

            Writer->BeginKey(key);
            Writer->AddKeyVersion(row, version);
            Writer->EndKey();

            PrevKey = TOwnedCellVec(key);
            HavePrevKey = true;

            DrainBlobs(ctx);
            return !Failed;
        }

        /**
         * Signal that all rows have been fed. Finalizes pages/index and starts
         * draining the remaining blobs. After this the owner must wait until
         * IsComplete() before calling ExtractResult().
         */
        bool Finalize(const TActorContext& ctx)
        {
            if (Failed) {
                return false;
            }
            if (Finishing) {
                return Fail("Finalize called twice");
            }

            if (Writer) {
                WriteStats = Writer->Finish();
                Results = Bundle->Results();
                Y_ENSURE(WriteStats.Parts == Results.size());
            }

            Finishing = true;
            DrainBlobs(ctx);
            return !Failed;
        }

        void Handle(TEvPutResult &msg, const TActorContext &ctx)
        {
            if (Failed) {
                return;
            }

            if (!NPageCollection::TGroupBlobsByCookie::IsInPlane(msg.Id, Mask_)) {
                Fail("TEvPutResult Id mask differs from the one used");
                return;
            }

            Y_ENSURE(Writing >= msg.Id.BlobSize(), "Writing bytes counter is out of sync");
            Y_ENSURE(Flushing >= msg.Id.BlobSize(), "Flushing bytes counter is out of sync");

            Writing -= msg.Id.BlobSize();
            Flushing -= msg.Id.BlobSize();

            const ui32 channel = msg.Id.Channel();
            if (msg.StatusFlags.Check(NKikimrBlobStorage::StatusDiskSpaceLightYellowMove)) {
                if (channel < 256 && !SeenYellowMoveChannels[channel]) {
                    SeenYellowMoveChannels[channel] = true;
                    YellowMoveChannels.push_back(channel);
                }
            }
            if (msg.StatusFlags.Check(NKikimrBlobStorage::StatusDiskSpaceYellowStop)) {
                if (channel < 256 && !SeenYellowStopChannels[channel]) {
                    SeenYellowStopChannels[channel] = true;
                    YellowStopChannels.push_back(channel);
                }
            }

            Cfg.Writer.ChannelsShares.Update(channel, msg.ApproximateFreeSpaceShare);

            if (msg.Status != NKikimrProto::OK) {
                Fail(TStringBuilder() << "Blob " << msg.Id << " put failed: " << msg.Status);
                return;
            }

            while (!WriteQueue.empty() && Writing < MaxFlight) {
                SendToBs(std::move(WriteQueue.front()), ctx);
                WriteQueue.pop_front();
            }
        }

        /**
         * Build the resulting part(s) from the written page collections. Must be
         * called only once IsComplete() is true. The returned result carries the
         * reserved Step so the commit can release the GC barrier.
         */
        THolder<TDirectPartResult> ExtractResult(const TActorContext &ctx)
        {
            Y_ENSURE(!Failed, "ExtractResult on a failed writer");
            Y_ENSURE(IsComplete(), "ExtractResult before the writer is complete");
            Y_ENSURE(!Consumed, "ExtractResult called twice");
            Consumed = true;

            auto result = MakeHolder<TDirectPartResult>();
            result->Step = Mask_.Step();
            result->YellowMoveChannels = std::move(YellowMoveChannels);
            result->YellowStopChannels = std::move(YellowStopChannels);

            for (auto &res : Results) {
                Y_ENSURE(res.PageCollections, "Direct write produced a part without page collections");

                TVector<TIntrusivePtr<TPrivatePageCache::TPageCollection>> pageCollections;
                for (auto& pageCollection : res.PageCollections) {
                    auto resultingPageCollection = MakeIntrusive<NTable::TLoader::TPageCollection>(pageCollection.PageCollection);
                    auto saveCompactedPages = MakeHolder<NSharedCache::TEvSaveCompactedPages>(pageCollection.PageCollection);
                    auto gcList = SharedCachePages->GCList;
                    auto addPage = [&saveCompactedPages, &pageCollection, &resultingPageCollection, &gcList]
                        (NPageCollection::TLoadedPage& loadedPage, bool sticky) {
                        auto pageId = loadedPage.PageId;
                        auto pageSize = pageCollection.PageCollection->Page(pageId).Size;
                        auto sharedPage = MakeIntrusive<TPage>(pageId, pageSize, nullptr);
                        sharedPage->ProvideBody(std::move(loadedPage.Data));
                        saveCompactedPages->Pages.push_back(sharedPage);
                        if (sticky) {
                            resultingPageCollection->AddStickyPage(pageId, TSharedPageRef::MakeUsed(std::move(sharedPage), gcList));
                        } else {
                            resultingPageCollection->AddPage(pageId, TSharedPageRef::MakeUsed(std::move(sharedPage), gcList));
                        }
                    };
                    for (auto &page : pageCollection.StickyPages) {
                        addPage(page, true);
                    }
                    for (auto &page : pageCollection.RegularPages) {
                        addPage(page, false);
                    }

                    ctx.Send(MakeSharedPageCacheId(), saveCompactedPages.Release());

                    pageCollections.push_back(std::move(resultingPageCollection));
                }

                NTable::TLoader loader(std::move(pageCollections), { }, std::move(res.Overlay));

                auto fetch = loader.Run({.PreloadIndex = false, .PreloadData = false});
                Y_ENSURE(!fetch, "Just written part needs to load page collection pages");

                result->Parts.push_back(loader.Result());
                result->Growth.push_back(std::move(res.Growth));
                Y_ENSURE(result->Parts.back(), "Unexpected result without a part after direct write");
            }

            return result;
        }

    private:
        bool Fail(const TString& text)
        {
            if (!Failed) {
                Failed = true;
                ErrorText = text;
            }
            return false;
        }

        bool CheckKeyOrder(TArrayRef<const TCell> key)
        {
            const auto types = Scheme->Keys->BasicTypes();
            Y_ENSURE(key.size() == types.size(), "Key column count differs from scheme");
            if (HavePrevKey) {
                int cmp = CompareTypedCellVectors(PrevKey.data(), key.data(), types.data(), types.size());
                if (cmp >= 0) {
                    return Fail("Rows fed to a direct part writer must be strictly ascending by key");
                }
            }
            return true;
        }

        void DrainBlobs(const TActorContext &ctx)
        {
            for (NPageCollection::TGlob& one : Bundle->GetBlobsToSave()) {
                FlushToBs(std::move(one), ctx);
            }
        }

        void FlushToBs(NPageCollection::TGlob&& glob, const TActorContext &ctx)
        {
            Y_ENSURE(glob.GId.Logo.BlobSize() == glob.Data.size(), "Written LogoBlob size doesn't match id");

            Flushing += glob.GId.Logo.BlobSize();
            Blobs++;

            if (Writing < MaxFlight && WriteQueue.empty()) {
                SendToBs(std::move(glob), ctx);
            } else {
                WriteQueue.emplace_back(std::move(glob));
            }
        }

        void SendToBs(NPageCollection::TGlob&& glob, const TActorContext &ctx)
        {
            auto id = glob.GId;
            Writing += id.Logo.BlobSize();

            auto *ev = new TEvPut(TEvPut::TParameters{
                .BlobId = id.Logo,
                .Buffer = TRope(std::exchange(glob.Data, TString{ })),
                .Deadline = TInstant::Max(),
                .HandleClass = NKikimrBlobStorage::AsyncBlob,
                .Tactic = TEvBlobStorage::TEvPut::ETactic::TacticMaxThroughput,
                .WriteSource = TWriteSource::FlatCompactionPut,
                .ExternalRelevanceWatcher = RelevanceTracker,
            });

            SendToBSProxy(ctx, id.Group, ev);
        }

    private:
        const TLogoBlobID Mask_;
        TDirectWriteCfg Cfg;
        TIntrusiveConstPtr<TScheme> Scheme;
        TMessageRelevanceOwner RelevanceTracker = std::make_shared<TMessageRelevanceTracker>();

        TAutoPtr<TBundle> Bundle;
        TAutoPtr<TPartWriter> Writer;
        NTable::TWriteStats WriteStats;
        TVector<TBundle::TResult> Results;
        NSharedCache::TSharedCachePages *SharedCachePages = nullptr;

        TOwnedCellVec PrevKey;
        bool HavePrevKey = false;

        bool Finishing = false;
        bool Failed = false;
        bool Consumed = false;
        TString ErrorText;

        ui64 Blobs = 0;
        ui64 Writing = 0;   /* bytes flying to storage  */
        ui64 Flushing = 0;  /* bytes flushing to storage */

        std::bitset<256> SeenYellowMoveChannels;
        std::bitset<256> SeenYellowStopChannels;
        TVector<ui32> YellowMoveChannels;
        TVector<ui32> YellowStopChannels;
        TDeque<NPageCollection::TGlob> WriteQueue;
    };

}
}
