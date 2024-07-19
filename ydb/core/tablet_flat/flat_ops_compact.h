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
            , Bundle(new TBundle(Mask, Conf->Writer))
        {
        }

        ~TOpsCompact()
        {
            // Y_ABORT_UNLESS(!Driver, "TOpsCompact is still running under scan");
        }

        void Describe(IOutputStream &out) const noexcept override
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

        TInitialState Prepare(IDriver *driver, TIntrusiveConstPtr<TScheme> scheme) noexcept override
        {
            TActivationContext::AsActorContext().RegisterWithSameMailbox(this);

            Spent = new TSpent(TAppData::TimeProvider.Get());
            Registry = AppData()->TypeRegistry;
            Scheme = std::move(scheme);
            Driver = driver;

            NTable::IScan::TConf conf;

            conf.NoErased = false; /* emit erase markers */
            conf.LargeEdge = Conf->Layout.LargeEdge;

            return { EScan::Feed, conf };
        }

        EScan Seek(TLead &lead, ui64 seq) noexcept override
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

        EScan BeginKey(TArrayRef<const TCell> key) noexcept override
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

        EScan BeginDeltas() noexcept override
        {
            if (auto logl = Logger->Log(ELnLev::Dbg03)) {
                logl << NFmt::Do(*this) << " begin deltas";
            }

            return Flush(false /* intermediate, sleep or feed */);
        }

        EScan Feed(const TRow &row, ui64 txId) noexcept override
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

        EScan EndDeltas() noexcept override
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

        EScan Feed(const TRow &row, TRowVersion &rowVersion) noexcept override
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

        EScan EndKey() noexcept override
        {
            ui32 written = Writer->EndKey();

            if (auto logl = Logger->Log(ELnLev::Dbg03)) {
                logl << NFmt::Do(*this) << " end key { written " << written << " row versions }";
            }

            return Flush(false /* intermediate, sleep or feed */);
        }

        void WriteTxStatus() noexcept
        {
            if (!Conf->CommittedTransactions && !Conf->RemovedTransactions) {
                // Nothing to write
                return;
            }

            THashSet<ui64> txFilter;
            for (const auto& memTable : Conf->Frozen) {
                for (const auto& pr : memTable->GetCommittedTransactions()) {
                    txFilter.insert(pr.first);
                }
                for (const ui64 txId : memTable->GetRemovedTransactions()) {
                    txFilter.insert(txId);
                }
            }
            for (const auto& txStatus : Conf->TxStatus) {
                for (const auto& item : txStatus->TxStatusPage->GetCommittedItems()) {
                    txFilter.insert(item.GetTxId());
                }
                for (const auto& item : txStatus->TxStatusPage->GetRemovedItems()) {
                    txFilter.insert(item.GetTxId());
                }
            }

            NTable::NPage::TTxStatusBuilder builder;
            for (const auto& pr : Conf->CommittedTransactions) {
                if (txFilter.contains(pr.first)) {
                    builder.AddCommitted(pr.first, pr.second);
                }
            }
            for (const ui64 txId : Conf->RemovedTransactions) {
                if (txFilter.contains(txId)) {
                    builder.AddRemoved(txId);
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

        TAutoPtr<IDestructable> Finish(EAbort abort) noexcept override
        {
            const auto fail = Failed || !Finished || abort != EAbort::None;

            auto *prod = new TProdCompact(!fail, Mask.Step(), std::move(Conf->Params),
                    std::move(YellowMoveChannels), std::move(YellowStopChannels));

            for (auto &result : Results) {
                Y_ABORT_UNLESS(result.PageCollections, "Compaction produced a part without page collections");

                NTable::TLoader loader(
                    std::move(result.PageCollections),
                    { },
                    std::move(result.Overlay));

                auto fetch = loader.Run(false);

                Y_ABORT_UNLESS(!fetch, "Just compacted part needs to load some pages");

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
                    << ", " << Blobs << "blobs " << WriteStats.Rows << "r"
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
                prod->Results.clear(); /* shouldn't sent w/o fixation in bs */
            } else if (bool(prod->Results) != bool(WriteStats.Rows > 0)) {
                Y_ABORT("Unexpexced rows production result after compaction");
            } else if ((bool(prod->Results) || bool(prod->TxStatus)) != bool(Blobs > 0)) {
                Y_ABORT("Unexpexced blobs production result after compaction");
            }

            Driver = nullptr;

            PassAway();

            return prod;
        }

        EScan Flush(bool last) noexcept
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

        void Handle(TEvPutResult &msg) noexcept
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


            if (msg.StatusFlags.Check(NKikimrBlobStorage::StatusDiskSpaceLightYellowMove)) {
                const ui32 channel = msg.Id.Channel();
                Y_DEBUG_ABORT_UNLESS(channel < 256);
                if (!SeenYellowMoveChannels[channel]) {
                    SeenYellowMoveChannels[channel] = true;
                    YellowMoveChannels.push_back(channel);
                }
            }
            if (msg.StatusFlags.Check(NKikimrBlobStorage::StatusDiskSpaceYellowStop)) {
                const ui32 channel = msg.Id.Channel();
                Y_DEBUG_ABORT_UNLESS(channel < 256);
                if (!SeenYellowStopChannels[channel]) {
                    SeenYellowStopChannels[channel] = true;
                    YellowStopChannels.push_back(channel);
                }
            }

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

        void FlushToBs(NPageCollection::TGlob&& glob) noexcept
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

        void SendToBs(NPageCollection::TGlob&& glob) noexcept
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
