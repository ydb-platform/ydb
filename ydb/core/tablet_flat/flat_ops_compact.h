#pragma once

#include "flat_scan_iface.h"
#include "flat_scan_spent.h"
#include "flat_writer_bundle.h"
#include "flat_sausage_chop.h"
#include "flat_row_misc.h"
#include "flat_part_writer.h"
#include "flat_part_loader.h"
#include "util_fmt_logger.h"
#include "util_fmt_desc.h"
#include "util_basics.h"
#include "flat_comp.h"
#include "flat_executor_misc.h"
#include "flat_bio_stats.h"
#include "shared_cache_pages.h"
#include "shared_sausagecache.h"
#include "util_channel.h"

#include <ydb/core/base/blobstorage.h>
#include <ydb/core/base/appdata.h>
#include <ydb/library/actors/core/actor.h>

#include <bitset>

namespace NKikimr {
namespace NTabletFlatExecutor {

    struct TProdCompact: public IDestructable {
        struct TResult {
            NTable::TPartView Part;
            TDeque<NTable::TScreen::THole> Growth;
        };

        using TResults = TDeque<TResult>;

        TProdCompact(bool ok, ui32 step, THolder<NTable::TCompactionParams> params,
                TVector<ui32>&& yellowMoveChannels,
                TVector<ui32>&& yellowStopChannels)
            : Success(ok)
            , Step(step)
            , Params(std::move(params))
            , YellowMoveChannels(std::move(yellowMoveChannels))
            , YellowStopChannels(std::move(yellowStopChannels))
        {

        }

        bool Success = false;
        ui32 Step = Max<ui32>();
        TResults Results;
        TVector<TIntrusiveConstPtr<NTable::TTxStatusPart>> TxStatus;
        THolder<NTable::TCompactionParams> Params;
        TVector<ui32> YellowMoveChannels;
        TVector<ui32> YellowStopChannels;
    };

    class TOpsCompact: private ::NActors::IActorCallback, public NTable::IVersionScan {
        using TEvPut = TEvBlobStorage::TEvPut;
        using TEvPutResult = TEvBlobStorage::TEvPutResult;
        using TScheme = NTable::TRowScheme;
        using TPartWriter = NTable::TPartWriter;
        using TBundle = NWriter::TBundle;
        using TStorage = TIntrusivePtr<TTabletStorageInfo>;
        using TEventHandlePtr = TAutoPtr<::NActors::IEventHandle>;
        using ELnLev = NUtil::ELnLev;

    public:
        constexpr static ui64 MaxFlight = 20ll * (1ll << 20);

        TOpsCompact(TActorId owner, TLogoBlobID mask, TAutoPtr<TCompactCfg> conf)
            : ::NActors::IActorCallback(static_cast<TReceiveFunc>(&TOpsCompact::Inbox), NKikimrServices::TActivity::OPS_COMPACT_A)
            , Mask(mask)
            , Owner(owner)
            , Conf(std::move(conf))
        {
            Bundle = new TBundle(Mask, Conf->Writer);
        }

        ~TOpsCompact()
        {
            // Y_ABORT_UNLESS(!Driver, "TOpsCompact is still running under scan");
        }

        void Describe(IOutputStream &out) const override
        {
            out
                << "Compact{" << Mask.TabletID()
                << "." << Mask.Generation()
                << "." << Mask.Step()
                << ", eph " << Conf->Epoch
                << "}";
        }

    private:
        void Registered(TActorSystem *sys, const TActorId&) override
        {
            Logger = new NUtil::TLogger(sys, NKikimrServices::OPS_COMPACT);
        }

        TInitialState Prepare(IDriver *driver, TIntrusiveConstPtr<TScheme> scheme) override
        {
            TActivationContext::AsActorContext().RegisterWithSameMailbox(this);

            Spent = new TSpent(TAppData::TimeProvider.Get());
            Registry = AppData()->TypeRegistry;
            SharedCachePages = AppData()->SharedCachePages.Get();
            Scheme = std::move(scheme);
            Driver = driver;

            NTable::IScan::TConf conf;

            conf.NoErased = false; /* emit erase markers */
            conf.LargeEdge = Conf->Layout.LargeEdge;

            return { EScan::Feed, conf };
        }

        EScan Seek(TLead &lead, ui64 seq) override
        {
            if (seq == 0) /* on first Seek() init compaction */ {
                Y_ABORT_UNLESS(!Writer, "Initial IScan::Seek(...) called twice");

                const auto tags = Scheme->Tags();

                lead.To(tags, { }, NTable::ESeek::Lower);

                auto *scheme = new NTable::TPartScheme(Scheme->Cols);

                Writer = new TPartWriter(scheme, tags, *Bundle, Conf->Layout, Conf->Epoch);

                return EScan::Feed;

            } else if (seq == 1) /* after the end(), stop compaction */ {
                if (!Finished) {
                    WriteStats = Writer->Finish();
                    Results = Bundle->Results();
                    Y_ABORT_UNLESS(WriteStats.Parts == Results.size());
                    WriteTxStatus();
                    Finished = true;
                }

                return Flush(true /* final flush, sleep or finish */);
            } else {
                Y_ABORT("Compaction scan op should get only two Seeks()");
            }
        }

        EScan BeginKey(TArrayRef<const TCell> key) override
        {
            Writer->BeginKey(key);

            if (auto logl = Logger->Log(ELnLev::Dbg03)) {
                logl
                    << NFmt::Do(*this) << " begin key { "
                    << NFmt::TCells(key, *Scheme->Keys, Registry)
                    << "}";
            }

            return Flush(false /* intermediate, sleep or feed */);
        }

        EScan BeginDeltas() override
        {
            if (auto logl = Logger->Log(ELnLev::Dbg03)) {
                logl << NFmt::Do(*this) << " begin deltas";
            }

            return Flush(false /* intermediate, sleep or feed */);
        }

        EScan Feed(const TRow &row, ui64 txId) override
        {
            if (auto logl = Logger->Log(ELnLev::Dbg03)) {
                logl << NFmt::Do(*this) << " feed row { ";

                if (row.GetRowState() == NTable::ERowOp::Erase) {
                    logl << "erased";
                } else {
                    logl << NFmt::TCells(*row, *Scheme->RowCellDefaults, Registry);
                }

                logl << " txId " << txId << " }";
            }

            // Note: we assume the number of uncommitted transactions is limited
            auto res = Deltas.try_emplace(txId, row);
            if (res.second) {
                DeltasOrder.emplace_back(txId);
            } else if (!res.first->second.IsFinalized()) {
                res.first->second.Merge(row);
            }

            return Flush(false /* intermediate, sleep or feed */);
        }

        EScan EndDeltas() override
        {
            if (auto logl = Logger->Log(ELnLev::Dbg03)) {
                logl << NFmt::Do(*this) << " end deltas";
            }

            if (!Deltas.empty()) {
                if (auto logl = Logger->Log(ELnLev::Dbg03)) {
                    logl << NFmt::Do(*this) << " flushing " << Deltas.size() << " deltas";
                }

                for (ui64 txId : DeltasOrder) {
                    auto it = Deltas.find(txId);
                    Y_ABORT_UNLESS(it != Deltas.end(), "Unexpected failure to find txId %" PRIu64, txId);
                    Writer->AddKeyDelta(it->second, txId);
                }

                Deltas.clear();
                DeltasOrder.clear();
            }

            return Flush(false /* intermediate, sleep or feed */);
        }

        EScan Feed(const TRow &row, TRowVersion &rowVersion) override
        {
            if (Conf->RemovedRowVersions) {
                // Adjust rowVersion so removed versions become compacted
                rowVersion = Conf->RemovedRowVersions.AdjustDown(rowVersion);
            }

            if (auto logl = Logger->Log(ELnLev::Dbg03)) {
                logl << NFmt::Do(*this) << " feed row { ";

                if (row.GetRowState() == NTable::ERowOp::Erase) {
                    logl << "erased";
                } else {
                    logl << NFmt::TCells(*row, *Scheme->RowCellDefaults, Registry);
                }

                logl << " at " << rowVersion << " }";
            }

            Writer->AddKeyVersion(row, rowVersion);

            return Flush(false /* intermediate, sleep or feed */);
        }

        EScan EndKey() override
        {
            ui32 written = Writer->EndKey();

            if (auto logl = Logger->Log(ELnLev::Dbg03)) {
                logl << NFmt::Do(*this) << " end key { written " << written << " row versions }";
            }

            return Flush(false /* intermediate, sleep or feed */);
        }

        void WriteTxStatus()
        {
            if (!Conf->Frozen && !Conf->TxStatus) {
                // Nothing to compact
            }

            absl::flat_hash_map<ui64, std::optional<TRowVersion>> status;
            auto mergeStatus = [&](ui64 txId, const std::optional<TRowVersion>& version) {
                if (Conf->GarbageTransactions.Contains(txId)) {
                    // We don't write garbage transactions
                    return;
                }
                auto it = status.find(txId);
                if (it == status.end()) {
                    status[txId] = version;
                } else if (version) {
                    if (!it->second) {
                        // commit wins over remove
                        it->second = version;
                    } else if (*version < *it->second) {
                        // lowest commit version wins
                        it->second = version;
                    }
                }
            };

            for (const auto& memTable : Conf->Frozen) {
                for (const auto& pr : memTable->GetCommittedTransactions()) {
                    mergeStatus(pr.first, pr.second);
                }
                for (const ui64 txId : memTable->GetRemovedTransactions()) {
                    mergeStatus(txId, std::nullopt);
                }
            }
            for (const auto& txStatus : Conf->TxStatus) {
                for (const auto& item : txStatus->TxStatusPage->GetCommittedItems()) {
                    mergeStatus(item.GetTxId(), item.GetRowVersion());
                }
                for (const auto& item : txStatus->TxStatusPage->GetRemovedItems()) {
                    mergeStatus(item.GetTxId(), std::nullopt);
                }
            }

            if (status.empty()) {
                // Nothing to write
                return;
            }

            NTable::NPage::TTxStatusBuilder builder;
            for (const auto& pr : status) {
                if (pr.second) {
                    builder.AddCommitted(pr.first, *pr.second);
                } else {
                    builder.AddRemoved(pr.first);
                }
            }

            auto data = builder.Finish();
            if (!data) {
                // Don't write an empty page
                return;
            }

            auto dataId = Bundle->WriteExtra(data);

            if (auto logl = Logger->Log(ELnLev::Dbg03)) {
                logl << NFmt::Do(*this) << " written tx status " << dataId.Lead << " size=" << dataId.Bytes;
            }

            TxStatus.emplace_back(new NTable::TTxStatusPartStore(dataId, Conf->Epoch, data));
        }

        TAutoPtr<IDestructable> Finish(EAbort abort) override
        {
            const auto fail = Failed || !Finished || abort != EAbort::None;

            auto *prod = new TProdCompact(!fail, Mask.Step(), std::move(Conf->Params),
                    std::move(YellowMoveChannels), std::move(YellowStopChannels));

            if (fail) {
                Results.clear(); /* shouldn't sent w/o fixation in bs */
            }

            for (auto &result : Results) {
                Y_ABORT_UNLESS(result.PageCollections, "Compaction produced a part without page collections");
                TVector<TIntrusivePtr<NTable::TLoader::TCache>> pageCollections;
                for (auto& pageCollection : result.PageCollections) {
                    auto cache = MakeIntrusive<NTable::TLoader::TCache>(pageCollection.PageCollection);
                    auto saveCompactedPages = MakeHolder<NSharedCache::TEvSaveCompactedPages>(pageCollection.PageCollection);
                    auto gcList = SharedCachePages->GCList;
                    auto addPage = [&saveCompactedPages, &pageCollection, &cache, &gcList](NPageCollection::TLoadedPage& loadedPage, bool sticky) {
                        auto pageId = loadedPage.PageId;
                        auto pageSize = pageCollection.PageCollection->Page(pageId).Size;
                        auto sharedPage = MakeIntrusive<TPage>(pageId, pageSize, nullptr);
                        sharedPage->Initialize(std::move(loadedPage.Data));
                        saveCompactedPages->Pages.push_back(sharedPage);
                        cache->Fill(pageId, TSharedPageRef::MakeUsed(std::move(sharedPage), gcList), sticky);
                    };
                    for (auto &page : pageCollection.StickyPages) {
                        addPage(page, true);
                    }
                    for (auto &page : pageCollection.RegularPages) {
                        addPage(page, false);
                    }

                    Send(MakeSharedPageCacheId(), saveCompactedPages.Release());

                    pageCollections.push_back(std::move(cache));
                }

                NTable::TLoader loader(
                    std::move(pageCollections),
                    { },
                    std::move(result.Overlay));

                // do not preload index as it may be already offloaded
                auto fetch = loader.Run({.PreloadIndex = false, .PreloadData = false});

                if (Y_UNLIKELY(fetch)) {
                    TStringBuilder error;
                    error << "Just compacted part needs to load pages";
                    for (auto collection : fetch) {
                        error << " " << collection->PageCollection->Label().ToString() << ": [ ";
                        for (auto pageId : collection->Pages) {
                            error << pageId << " " << (NTable::NPage::EPage)collection->PageCollection->Page(pageId).Type << " ";
                        }
                        error << "]";
                    }
                    Y_ABORT_S(error);
                }

                auto& res = prod->Results.emplace_back();
                res.Part = loader.Result();
                res.Growth = std::move(result.Growth);
                Y_ABORT_UNLESS(res.Part, "Unexpected result without a part after compaction");
            }

            prod->TxStatus = std::move(TxStatus);

            if (auto logl = Logger->Log(fail ? ELnLev::Error : ELnLev::Info)) {
                auto raito = WriteStats.Bytes ? (WriteStats.Coded + 0.) / WriteStats.Bytes : 0.;

                logl
                    << NFmt::Do(*this) << " end=" << ui32(abort)
                    << ", " << Blobs << " blobs " << WriteStats.Rows << "r"
                    << " (max " << Conf->Layout.MaxRows << ")"
                    << ", put " << NFmt::If(Spent.Get());

                for (const auto &result : prod->Results) {
                    if (auto *part = result.Part.As<NTable::TPartStore>()) {
                        auto lobs = part->Blobs ? part->Blobs->Total() : 0;
                        auto small = part->Small ? part->Small->Stats().Size : 0;
                        auto large = part->Large ? part->Large->Stats().Size : 0;
                        auto grow = NTable::TScreen::Sum(result.Growth);

                        logl
                            << " Part{ " << part->PageCollections.size() << " pk"
                            << ", lobs " << (lobs - grow) << " +" << grow
                            << ", (" << part->DataSize()
                                << " " << small << " " << large <<")b"
                            << " }";
                    }
                }

                if (prod->Results) {
                    logl << ", ecr=" << Sprintf("%.3f", raito);
                }

                for (const auto &txStatus : prod->TxStatus) {
                    logl << " TxStatus{ " << txStatus->Label << " }";
                }
            }

            if (fail) {
                Y_ABORT_IF(prod->Results); /* shouldn't sent w/o fixation in bs */
            } else if (bool(prod->Results) != bool(WriteStats.Rows > 0)) {
                Y_ABORT("Unexpected rows production result after compaction");
            } else if ((bool(prod->Results) || bool(prod->TxStatus)) != bool(Blobs > 0)) {
                Y_ABORT("Unexpected blobs production result after compaction");
            }

            Driver = nullptr;

            PassAway();

            return prod;
        }

        EScan Flush(bool last)
        {
            for (NPageCollection::TGlob& one : Bundle->GetBlobsToSave())
                FlushToBs(std::move(one));

            EScan scan = EScan::Sleep;

            if (last) {
                scan = (Flushing > 0 ? EScan::Sleep : EScan::Final);
            } else {
                scan = (Flushing >= MaxFlight ? EScan::Sleep : EScan::Feed);
            }

            Spent->Alter(scan != EScan::Sleep);

            return scan;
        }

        void Inbox(TEventHandlePtr &eh)
        {
            if (auto *ev = eh->CastAsLocal<TEvPutResult>()) {
                Handle(*ev);
            } else if (eh->CastAsLocal<TEvents::TEvUndelivered>()) {
                if (auto logl = Logger->Log(ELnLev::Error)) {
                    logl
                        << NFmt::Do(*this) << " cannot send put event to BS";
                }

                if (!std::exchange(Failed, true))
                    Driver->Touch(EScan::Final);
            } else {
                Y_ABORT("Compaction actor got an unexpected event");
            }
        }

        void Handle(TEvPutResult &msg)
        {
            if (!NPageCollection::TGroupBlobsByCookie::IsInPlane(msg.Id, Mask)) {
                Y_ABORT("TEvPutResult Id mask is differ from used");
            } else if (Writing < msg.Id.BlobSize()) {
                Y_ABORT("Compaction writing bytes counter is out of sync");
            } else if (Flushing < msg.Id.BlobSize()) {
                Y_ABORT("Compaction flushing bytes counter is out of sync");
            }

            Writing -= msg.Id.BlobSize();
            Flushing -= msg.Id.BlobSize();

            const ui32 channel = msg.Id.Channel();

            if (msg.StatusFlags.Check(NKikimrBlobStorage::StatusDiskSpaceLightYellowMove)) {
                Y_DEBUG_ABORT_UNLESS(channel < 256);
                if (!SeenYellowMoveChannels[channel]) {
                    SeenYellowMoveChannels[channel] = true;
                    YellowMoveChannels.push_back(channel);
                }
            }
            if (msg.StatusFlags.Check(NKikimrBlobStorage::StatusDiskSpaceYellowStop)) {
                Y_DEBUG_ABORT_UNLESS(channel < 256);
                if (!SeenYellowStopChannels[channel]) {
                    SeenYellowStopChannels[channel] = true;
                    YellowStopChannels.push_back(channel);
                }
            }

            Conf->Writer.ChannelsShares.Update(channel, msg.ApproximateFreeSpaceShare);

            const auto ok = (msg.Status == NKikimrProto::OK);

            if (auto logl = Logger->Log(ok ? ELnLev::Debug: ELnLev::Error)) {
                logl
                    << NFmt::Do(*this)
                    << " put " << msg.Id.ToString()
                    << " result " << msg.Status
                    << " flags " << msg.StatusFlags
                    << " left " << Flushing << "b";
            }

            if (ok) {
                Send(Owner, new NBlockIO::TEvStat(NBlockIO::EDir::Write, NBlockIO::EPriority::Bulk, msg.GroupId, msg.Id));

                while (!WriteQueue.empty() && Writing < MaxFlight) {
                    SendToBs(std::move(WriteQueue.front()));
                    WriteQueue.pop_front();
                }

                Y_DEBUG_ABORT_UNLESS(Flushing == 0 || Writing > 0, "Unexpected: Flushing > 0 and Writing == 0");

                if (Flushing == 0) {
                    Spent->Alter(true /* resource available again */);
                    Driver->Touch(Finished ? EScan::Final : EScan::Feed);
                }
            } else if (!std::exchange(Failed, true)) {
                Driver->Touch(EScan::Final);
            }
        }

        void FlushToBs(NPageCollection::TGlob&& glob)
        {
            Y_ABORT_UNLESS(glob.GId.Logo.BlobSize() == glob.Data.size(),
                "Written LogoBlob size doesn't match id");

            Flushing += glob.GId.Logo.BlobSize();
            Blobs++;

            if (Writing < MaxFlight && WriteQueue.empty()) {
                SendToBs(std::move(glob));
            } else {
                Y_DEBUG_ABORT_UNLESS(Failed || Writing > 0, "Unexpected: enqueued blob when Writing == 0");
                WriteQueue.emplace_back(std::move(glob));
            }
        }

        void SendToBs(NPageCollection::TGlob&& glob)
        {
            auto id = glob.GId;

            Writing += id.Logo.BlobSize();
            Y_DEBUG_ABORT_UNLESS(Writing <= Flushing, "Unexpected: Writing > Flushing");

            if (auto logl = Logger->Log(ELnLev::Debug)) {
                logl
                    << NFmt::Do(*this)
                    << " saving " << id.Logo.ToString()
                    << " left " << Flushing << "b";
            }

            auto flag = NKikimrBlobStorage::AsyncBlob;
            auto *ev = new TEvPut(id.Logo, std::exchange(glob.Data, TString{ }), TInstant::Max(), flag,
                TEvBlobStorage::TEvPut::ETactic::TacticMaxThroughput);
            auto ctx = ActorContext();

            SendToBSProxy(ctx, id.Group, ev);
        }

    private:
        const TLogoBlobID Mask;
        const TActorId Owner;
        TAutoPtr<NUtil::ILogger> Logger;
        IDriver * Driver = nullptr;
        THolder<TCompactCfg> Conf;
        TIntrusiveConstPtr<TScheme> Scheme;
        TAutoPtr<TBundle> Bundle;
        TAutoPtr<TPartWriter> Writer;
        NTable::TWriteStats WriteStats;
        TVector<TBundle::TResult> Results;
        TVector<TIntrusiveConstPtr<NTable::TTxStatusPart>> TxStatus;
        const NScheme::TTypeRegistry * Registry = nullptr;
        NSharedCache::TSharedCachePages * SharedCachePages;

        bool Finished = false;
        bool Failed = false;/* Failed to write blobs    */
        TAutoPtr<TSpent> Spent; /* Blockage on write stats  */
        ui64 Blobs = 0;     /* Blobs produced by writer */
        ui64 Writing = 0;   /* Bytes flying to storage  */
        ui64 Flushing = 0;  /* Bytes flushing to storage */

        std::bitset<256> SeenYellowMoveChannels;
        std::bitset<256> SeenYellowStopChannels;
        TVector<ui32> YellowMoveChannels;
        TVector<ui32> YellowStopChannels;
        TDeque<NPageCollection::TGlob> WriteQueue;

        THashMap<ui64, TRow> Deltas;
        TSmallVec<ui64> DeltasOrder;
    };
}
}
