#pragma once

#include "flat_table.h"
#include "flat_table_subset.h"
#include "flat_dbase_misc.h"
#include "flat_dbase_apply.h"
#include "flat_dbase_scheme.h"
#include "flat_redo_writer.h"
#include "flat_redo_player.h"
#include "flat_util_misc.h"
#include "flat_abi_evol.h"
#include "flat_abi_check.h"
#include "util_fmt_flat.h"
#include "util_basics.h"
#include "util_deref.h"

namespace NKikimr {
namespace NTable {

    class TDatabaseImpl  {

        struct TArgs {
            ui32 Table;
            TEpoch Head;
            TTxStamp Edge;
        };

        struct TTableWrapper {
            TTableWrapper() = default;
            TTableWrapper(const TTableWrapper&) = delete;

            TTableWrapper(TArgs args)
                : Table(args.Table)
                , Self(new TTable(args.Head))
                , Edge(args.Edge)
            {

            }

            explicit operator bool() const noexcept
            {
                return bool(Self);
            }

            TTable* operator->() const noexcept
            {
                return Self.Get();
            }

            bool Touch(ui64 edge, ui64 serial) noexcept
            {
                return std::exchange(Serial, serial) <= edge;
            }

            void Aggr(TDbStats &aggr, bool enter) const noexcept
            {
                const auto &stat = Self->Stat();

                if (enter) {
                    aggr.MemTableWaste += Self->GetMemWaste();
                    aggr.MemTableBytes += Self->GetMemSize();
                    aggr.MemTableOps += Self->GetOpsCount();
                    aggr.Parts += stat.Parts;
                    for (const auto& kv : stat.PartsPerTablet) {
                        aggr.PartsPerTablet[kv.first] += kv.second;
                    }
                } else {
                    NUtil::SubSafe(aggr.MemTableWaste, Self->GetMemWaste());
                    NUtil::SubSafe(aggr.MemTableBytes, Self->GetMemSize());
                    NUtil::SubSafe(aggr.MemTableOps, Self->GetOpsCount());
                    for (const auto& kv : stat.PartsPerTablet) {
                        // Note: we don't cleanup old tablets, because
                        // usually there is a very small number of them
                        aggr.PartsPerTablet[kv.first] -= kv.second;
                    }
                    aggr.Parts -= stat.Parts;
                }
            }

            const ui32 Table = Max<ui32>();
            const TIntrusivePtr<TTable> Self;
            const TTxStamp Edge = 0;    /* Stamp of last snapshot       */
            ui64 Serial = 0;
        };

    public:
        using TEdges = THashMap<ui32, TSnapEdge>;
        using TInfo = TScheme::TTableInfo;
        using TOps = TArrayRef<const TUpdateOp>;
        using TModifier = TSchemeModifier;
        using TMemGlob = NPageCollection::TMemGlob;

        TDatabaseImpl(TTxStamp weak, TAutoPtr<TScheme> scheme, const TEdges *edges)
            : Weak(weak)
            , Redo(*this)
            , Scheme(scheme)
        {
            for (auto it : Scheme->Tables) {
                auto *mine = edges ? edges->FindPtr(it.first) : nullptr;

                MakeTable(it.first, mine ? *mine : TSnapEdge{ })->SetScheme(it.second);
            }

            CalculateAnnexEdge(); /* bring bootstrapped redo settings */
        }

        ui64 Serial() const noexcept
        {
            return Serial_;
        }

        TTableWrapper& Get(ui32 table, bool require) noexcept
        {
            auto *wrap = Tables.FindPtr(table);

            Y_VERIFY(wrap || !require, "Cannot find given table");

            return wrap ? *wrap : Dummy;
        }

        ui64 Rewind(ui64 serial) noexcept
        {
            return std::exchange(Serial_, Max(Serial_, serial));
        }

        TDatabaseImpl& Switch(TTxStamp stamp) noexcept
        {
            if (std::exchange(Stamp, stamp) > stamp)
                Y_FAIL("Executor tx stamp cannot go to the past");

            First_ = Max<ui64>(), Begin_ = Serial_;
            Stats.TxCommited++, Affects = { };

            return *this;
        }

        void Assign(TVector<TMemGlob> annex) noexcept
        {
            Y_VERIFY(!Annex, "Annex has been already attached to TDatabaseImpl");

            Annex = std::move(annex);
        }

        void ReplaceSlices(ui32 tid, TBundleSlicesMap slices) noexcept
        {
            auto &wrap = Get(tid, true);

            wrap.Aggr(Stats, false /* leave */);
            wrap->ReplaceSlices(std::move(slices));
            wrap.Aggr(Stats, true /* enter */);
        }

        void Replace(ui32 tid, TArrayRef<const TPartView> partViews, const TSubset &subset) noexcept
        {
            auto &wrap = Get(tid, true);

            wrap.Aggr(Stats, false /* leave */);
            wrap->Replace(partViews, subset);
            wrap.Aggr(Stats, true /* enter */);
        }

        void ReplaceTxStatus(ui32 tid, TArrayRef<const TIntrusiveConstPtr<TTxStatusPart>> txStatus, const TSubset &subset) noexcept
        {
            auto &wrap = Get(tid, true);

            wrap.Aggr(Stats, false /* leave */);
            wrap->ReplaceTxStatus(txStatus, subset);
            wrap.Aggr(Stats, true /* enter */);
        }

        void Merge(ui32 tid, TPartView partView) noexcept
        {
            auto &wrap = Get(tid, true);

            wrap.Aggr(Stats, false /* leave */);
            wrap->Merge(std::move(partView));
            wrap.Aggr(Stats, true /* enter */);
        }

        void Merge(ui32 tid, TIntrusiveConstPtr<TColdPart> part) noexcept
        {
            auto &wrap = Get(tid, true);

            wrap.Aggr(Stats, false /* leave */);
            wrap->Merge(std::move(part));
            wrap.Aggr(Stats, true /* enter */);
        }

        void Merge(ui32 tid, TIntrusiveConstPtr<TTxStatusPart> txStatus) noexcept
        {
            auto &wrap = Get(tid, true);

            wrap.Aggr(Stats, false /* leave */);
            wrap->Merge(std::move(txStatus));
            wrap.Aggr(Stats, true /* enter */);
        }

        bool Apply(const TSchemeChanges &delta, NRedo::TWriter *writer)
        {
            TModifier modifier(*Scheme);

            if (modifier.Apply(delta)) {
                Apply(modifier.Affects, writer);

                return true;
            } else {
                return false;
            }
        }

        TDatabaseImpl& ApplyRedo(TArrayRef<const char> plain) noexcept
        {
            return Redo.Replay(plain), *this;
        }

        TVector<ui32> GrabAffects() noexcept
        {
            return std::move(Affects);
        }

        TVector<TMemGlob> GrabAnnex() noexcept
        {
            return std::move(Annex);
        }

        ui32 AnnexByteLimit() const noexcept
        {
            return Large;
        }

    protected: /*_ Mine private methods */
        void CalculateAnnexEdge() noexcept
        {
            Large = Max<ui32>();

            for (auto table: Scheme->Tables)
                for (auto family: table.second.Families)
                    Large = Min(Large, family.second.Large);

            Large = Max(Large, Scheme->Redo.Annex);
        }

        TTableWrapper& MakeTable(ui32 table, TSnapEdge edge) noexcept
        {
            if (edge.TxStamp == Max<ui64>()) {
                Y_FAIL("Cannot make table on undefined TxStamp edge");
            } else if (edge.Head == TEpoch::Zero()) {
                /* Table written in compatability mode utilizes global
                    TxStamp instead of private TEpoch values. In order
                    to correcly handle legacy tables should rewind epoch
                    above of the last tx.
                 */

                ui64 head = edge.TxStamp + 1;
                Y_VERIFY(head < Max<i64>(), "TxStamp is too large for epoch");

                edge.Head = TEpoch(i64(head));
            }

            TArgs args{ table, edge.Head, edge.TxStamp };

            auto result = Tables.emplace(table, args);

            Y_VERIFY(result.second, "Table alredy exists");

            Stats.Tables += 1;

            return result.first->second;
        }

        void Apply(const THashSet<ui32> &affects, NRedo::TWriter *writer)
        {
            for (ui32 table : affects) {
                auto &wrap = Get(table, false);

                if (auto *info = Scheme->GetTableInfo(table)) {
                    if (wrap && writer) {
                        /* Hack keeps table epoches consistent in regular
                            flow and on db bootstap. Required due to scheme
                            deltas and redo log async rollup on bootstrap.
                         */

                        writer->EvFlush(table, Stamp, wrap->Snapshot());
                    }

                    (wrap ? wrap : MakeTable(table, { }))->SetScheme(*info);

                } else {
                    wrap.Aggr(Stats, false /* leave */);

                    Deleted.emplace_back(table);
                    Garbage.emplace_back(wrap->Unwrap());
                    Tables.erase(table);
                    NUtil::SubSafe(Stats.Tables, ui32(1));
                }
            }

            CalculateAnnexEdge(); /* could be changed in scheme alter */
        }

    public: /*_ Redo log player interface impl. */
        bool NeedIn(ui32 table) noexcept
        {
            /* Scheme deltas are applied before any redo log entries on
                db bootstrap and udate log entries for already deleted
                tables may appear. Weak stamp points to the end of asyncs.
             */

            const auto &wrap = Get(table, Weak <= Stamp);

            return wrap ? Stamp > wrap.Edge : false;
        }

        void DoBegin(ui32 tail, ui32 head, ui64 serial, ui64 stamp) noexcept
        {
            TAbi().Check(tail, head, "redo");

            /* Hack for redo logs affected by bug in KIKIMR-5323 */

            const auto back = Stamp - (head >= 21 ? 0 : Min(Stamp, ui64(2)));

            if (serial == 0) {
                Serial_++;  /* Legacy EvBegin without embedded serial */
            } else if (Serial_ < serial && (stamp == 0 || stamp >= back)) {
                Serial_ = serial;
            } else {
                Y_Fail("EvBegin{" << serial << " " << NFmt::TStamp(stamp)
                    << "} is not fits to db state {" << Serial_ << " "
                    << NFmt::TStamp(Stamp) << "} (redo log was reordered)");
            }

            First_ = Min(First_, Serial_);
        }

        void DoAnnex(TArrayRef<const TStdPad<NPageCollection::TGlobId>> annex) noexcept
        {
            if (Annex) {
                Y_VERIFY(annex.size() == Annex.size());

                for (auto it : xrange(Annex.size()))
                    if (Annex[it].GId != *annex[it]) {
                        Y_FAIL("NRedo EvAnnex isn't match to assigned annex");
                    } 

            } else {
                Annex.reserve(annex.size());

                for (auto &one : annex)
                    Annex.emplace_back(*one, TSharedData{ });
            }
        }

        void DoUpdate(ui32 tid, ERowOp rop, TKeys key, TOps ops, TRowVersion rowVersion) noexcept
        {
            auto &wrap = Touch(tid);

            NUtil::SubSafe(Stats.MemTableWaste, wrap->GetMemWaste());
            NUtil::SubSafe(Stats.MemTableBytes, wrap->GetMemSize());
            wrap->Update(rop, key, ops, Annex, rowVersion);
            Stats.MemTableWaste += wrap->GetMemWaste();
            Stats.MemTableBytes += wrap->GetMemSize();
            Stats.MemTableOps += 1;
        }

        void DoUpdateTx(ui32 tid, ERowOp rop, TKeys key, TOps ops, ui64 txId) noexcept
        {
            auto &wrap = Touch(tid);

            NUtil::SubSafe(Stats.MemTableWaste, wrap->GetMemWaste());
            NUtil::SubSafe(Stats.MemTableBytes, wrap->GetMemSize());
            wrap->UpdateTx(rop, key, ops, Annex, txId);
            Stats.MemTableWaste += wrap->GetMemWaste();
            Stats.MemTableBytes += wrap->GetMemSize();
            Stats.MemTableOps += 1;
        }

        void DoCommitTx(ui32 tid, ui64 txId, TRowVersion rowVersion) noexcept
        {
            auto &wrap = Touch(tid);

            NUtil::SubSafe(Stats.MemTableWaste, wrap->GetMemWaste());
            NUtil::SubSafe(Stats.MemTableBytes, wrap->GetMemSize());
            wrap->CommitTx(txId, rowVersion);
            Stats.MemTableWaste += wrap->GetMemWaste();
            Stats.MemTableBytes += wrap->GetMemSize();
        }

        void DoRemoveTx(ui32 tid, ui64 txId) noexcept
        {
            auto &wrap = Touch(tid);

            NUtil::SubSafe(Stats.MemTableWaste, wrap->GetMemWaste());
            NUtil::SubSafe(Stats.MemTableBytes, wrap->GetMemSize());
            wrap->RemoveTx(txId);
            Stats.MemTableWaste += wrap->GetMemWaste();
            Stats.MemTableBytes += wrap->GetMemSize();
        }

        void DoFlush(ui32 tid, ui64 /* stamp */, TEpoch epoch) noexcept
        {
            auto on = Touch(tid)->Snapshot();

            if (epoch != TEpoch::Zero() && epoch != on) {
                Y_Fail("EvFlush{" << tid << ", " << epoch << "eph} turned"
                        << " table to unexpected epoch " << on);
            }
        }

        TTableWrapper& Touch(ui32 table) noexcept
        {
            auto &wrap = Get(table, true);

            /* Modern redo log starts each logical update with single EvBegin
                allowing to grow db change serial number in a log driven way.
                Legacy log (Evolution < 12) have no EvBegin and progression
                of serial require hack with virtual insertion of EvBegin here.
             */

            if (wrap.Touch(Begin_, Begin_ == Serial_ ? ++Serial_ : Serial_))
                Affects.emplace_back(table);

            return First_ = Min(First_, Serial_), wrap;
        }

    public:
        void EnumerateTxStatusParts(const std::function<void(const TIntrusiveConstPtr<TTxStatusPart>&)>& callback) {
            for (auto &it : Tables) {
                it.second->EnumerateTxStatusParts(callback);
            }
        }

    private:
        const TTxStamp Weak;    /* db bootstrap upper stamp         */
        ui64 Stamp = 0;
        ui64 Serial_ = 1;       /* db global change serial number    */
        ui64 Begin_ = 0;        /* Serial at moment of Switch() call */
        ui32 Large = Max<ui32>();/* The lowest limit for large blobs */
        TTableWrapper Dummy;
        THashMap<ui32, TTableWrapper> Tables;
        NRedo::TPlayer<TDatabaseImpl> Redo;
        TVector<ui32> Affects;
        TVector<TMemGlob> Annex;

    public:
        const TAutoPtr<TScheme> Scheme;
        TGarbage Garbage;       /* Unused full table subsets */
        TVector<ui32> Deleted;
        TDbStats Stats;
        ui64 First_ = Max<ui64>(); /* First used serial after Switch() */
    };
}
}
