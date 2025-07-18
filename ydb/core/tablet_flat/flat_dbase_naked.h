#pragma once

#include "flat_table.h"
#include "flat_table_subset.h"
#include "flat_dbase_misc.h"
#include "flat_dbase_apply.h"
#include "flat_dbase_change.h"
#include "flat_dbase_scheme.h"
#include "flat_redo_writer.h"
#include "flat_redo_player.h"
#include "flat_util_misc.h"
#include "flat_abi_evol.h"
#include "flat_abi_check.h"
#include "util_fmt_abort.h"
#include "util_fmt_flat.h"
#include "util_basics.h"
#include "util_deref.h"

namespace NKikimr {
namespace NTable {

    class TDatabaseImpl final
        : public IAlterSink
    {
        struct TArgs {
            ui32 Table;
            TEpoch Head;
            TTxStamp Edge;
            const TIntrusivePtr<TKeyRangeCacheNeedGCList>& GCList;
        };

        struct TTableWrapper {
            TTableWrapper() = default;
            TTableWrapper(const TTableWrapper&) = delete;

            TTableWrapper(TArgs args)
                : Table(args.Table)
                , Self(new TTable(args.Head, args.GCList))
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
                ui64 prevSerial = std::exchange(Serial, serial);
                if (prevSerial <= edge) {
                    SerialBackup = prevSerial;
                    return true;
                }
                return false;
            }

            void Aggr(TDbStats &aggr, bool enter) const
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

            void BackupMemStats()
            {
                BackupMemTableWaste = Self->GetMemWaste();
                BackupMemTableBytes = Self->GetMemSize();
                BackupMemTableOps = Self->GetOpsCount();
            }

            void RestoreMemStats(TDbStats &aggr) const
            {
                NUtil::SubSafe(aggr.MemTableWaste, BackupMemTableWaste);
                NUtil::SubSafe(aggr.MemTableBytes, BackupMemTableBytes);
                NUtil::SubSafe(aggr.MemTableOps, BackupMemTableOps);
                aggr.MemTableWaste += Self->GetMemWaste();
                aggr.MemTableBytes += Self->GetMemSize();
                aggr.MemTableOps += Self->GetOpsCount();
            }

            /**
             * Returns serial before the transaction (when in transaction),
             * but possibly after schema changes or memtable flushes.
             */
            ui64 StableSerial() const noexcept
            {
                return DataModified && !EpochSnapshot ? SerialBackup : Serial;
            }

            /**
             * Returns epoch before the transaction (when in transaction),
             * but possibly after schema changes or memtable flushes.
             */
            TEpoch StableHead() const noexcept
            {
                return EpochSnapshot ? *EpochSnapshot : Self->Head();
            }

            const ui32 Table = Max<ui32>();
            const TIntrusivePtr<TTable> Self;
            const TTxStamp Edge = 0;    /* Stamp of last snapshot       */
            ui64 Serial = 0;
            ui64 SerialBackup = 0;

            std::optional<TEpoch> EpochSnapshot;
            ui64 BackupMemTableWaste;
            ui64 BackupMemTableBytes;
            ui64 BackupMemTableOps;

            bool Created = false;
            bool Dropped = false;
            bool SchemePending = false;
            bool SchemeModified = false;
            bool DataModified = false;
            bool RollbackPrepared = false;
            bool Truncated = false;
        };

        void PrepareRollback(TTableWrapper& wrap) {
            if (!wrap.Created && !wrap.RollbackPrepared) {
                wrap.BackupMemStats();
                wrap->PrepareRollback();
                wrap.RollbackPrepared = true;
                Prepared.push_back(wrap.Table);
            }
        }

        TEpoch PrepareSnapshot(TTableWrapper& wrap) {
            if (!wrap.EpochSnapshot) {
                wrap.EpochSnapshot.emplace(wrap->Snapshot());
                // When this is an existing table we also simulate
                // EvFlush that is inserted in the redo log.
                if (!wrap.Created) {
                    Flushed.push_back(wrap.Table);
                    if (wrap.Touch(Begin_, Serial_)) {
                        Affects.push_back(wrap.Table);
                    }
                }
            }
            return *wrap.EpochSnapshot;
        }

    public:
        using TEdges = THashMap<ui32, TSnapEdge>;
        using TInfo = TScheme::TTableInfo;
        using TOps = TArrayRef<const TUpdateOp>;
        using TModifier = TSchemeModifier;
        using TMemGlob = NPageCollection::TMemGlob;

        TDatabaseImpl(TTxStamp weak, TAutoPtr<TScheme> scheme, const TEdges *edges)
            : GCList(new TKeyRangeCacheNeedGCList)
            , Weak(weak)
            , Redo(*this)
            , Scheme(scheme)
        {
            for (const auto& it : Scheme->Tables) {
                auto *mine = edges ? edges->FindPtr(it.first) : nullptr;

                MakeTable(it.first, mine ? *mine : TSnapEdge{ })->SetScheme(it.second);
            }

            CalculateAnnexEdge(); /* bring bootstrapped redo settings */
        }

        ui64 Serial() const noexcept
        {
            return Serial_;
        }

        /**
         * Returns serial before the transaction (when in transaction)
         */
        ui64 StableSerial() const noexcept
        {
            return InTransaction ? Begin_ : Serial_;
        }

        TTableWrapper& Get(ui32 table, bool require)
        {
            auto *wrap = Tables.FindPtr(table);

            if (!wrap || wrap->Dropped) {
                Y_ENSURE(!require, "Cannot find table " << table);
                return Dummy;
            }

            if (wrap->SchemePending) {
                Y_ENSURE(InTransaction);

                auto* info = Scheme->GetTableInfo(table);
                Y_ENSURE(info, "No scheme for existing table " << table);

                PrepareRollback(*wrap);

                // We always flush mem table on schema modification,
                // which happens at the "start" of transaction.
                PrepareSnapshot(*wrap);

                (*wrap)->SetScheme(*info);

                wrap->SchemeModified = true;
                wrap->SchemePending = false;
            }

            return *wrap;
        }

        TTableWrapper& GetForUpdate(ui32 table)
        {
            Y_ENSURE(InTransaction);
            TTableWrapper& wrap = Get(table, true);
            PrepareRollback(wrap);
            if (wrap.Touch(Begin_, Serial_)) {
                Affects.push_back(table);
            }
            Y_ENSURE(wrap.Created || wrap.RollbackPrepared);
            wrap.DataModified = true;
            return wrap;
        }

        ui64 Rewind(ui64 serial)
        {
            Y_ENSURE(!InTransaction, "Unexpected rewind inside a transaction");
            return std::exchange(Serial_, Max(Serial_, serial));
        }

        void BeginTransaction()
        {
            Y_ENSURE(!InTransaction);
            InTransaction = true;

            // We pretend as if we just processed Switch and EvBegin with the next serial
            Begin_ = Serial_;
            Affects = { };
            Serial_++;

            // Sanity checks
            Y_DEBUG_ABORT_UNLESS(Annex.empty());
            Y_DEBUG_ABORT_UNLESS(Flushed.empty());
            Y_DEBUG_ABORT_UNLESS(Prepared.empty());
        }

        TEpoch FlushTable(ui32 tid)
        {
            Y_ENSURE(InTransaction);
            auto& wrap = Get(tid, true);
            Y_ENSURE(!wrap.DataModified, "Cannot flush a modified table");
            return PrepareSnapshot(wrap);
        }

        void TruncateTable(ui32 tid)
        {
            Y_ENSURE(InTransaction);
            auto& wrap = Get(tid, true);
            Y_ENSURE(!wrap.Truncated, "Cannot truncate a truncated table");
            Y_ENSURE(!wrap.DataModified, "Cannot truncate a modified table");
            if (wrap.Created) {
                // New tables have nothing to truncate
                return;
            }
            PrepareRollback(wrap);
            PrepareSnapshot(wrap);
            wrap->PrepareTruncate();
            wrap.DataModified = true;
            wrap.Truncated = true;
        }

        void CommitTransaction(TTxStamp stamp, TArrayRef<const TMemGlob> annex, NRedo::TWriter& writer)
        {
            Y_ENSURE(Stamp <= stamp, "Executor tx stamp cannot go to the past");
            Stamp = stamp;

            CommitScheme(annex);

            for (ui32 tid : Prepared) {
                auto it = Tables.find(tid);
                if (it == Tables.end()) {
                    // Table was actually dropped
                    continue;
                }
                auto& wrap = it->second;
                Y_ENSURE(wrap.RollbackPrepared);
                wrap->CommitChanges(annex);
                if (wrap.Truncated) {
                    TAutoPtr<TSubset> subset = wrap->Subset(TEpoch::Max());
                    wrap->Replace(*subset, { }, { });
                    Truncated.push_back({ tid, std::move(subset) });
                    wrap.Truncated = false;
                }
                wrap.RestoreMemStats(Stats);
                wrap.RollbackPrepared = false;
                wrap.DataModified = false;
            }
            Prepared.clear();

            THashSet<ui32> dropped;
            for (ui32 tid : Flushed) {
                auto it = Tables.find(tid);
                if (it == Tables.end()) {
                    // Table was actually dropped
                    dropped.insert(tid);
                    continue;
                }
                auto& wrap = it->second;
                Y_ENSURE(wrap.EpochSnapshot);
                writer.EvFlush(tid, Stamp - 1, *wrap.EpochSnapshot);
                wrap.EpochSnapshot.reset();
            }
            Flushed.clear();

            // Remove dropped tables (if any) from affects
            if (!dropped.empty()) {
                auto end = std::remove_if(
                    Affects.begin(), Affects.end(),
                    [&dropped](ui32 tid) {
                        return dropped.contains(tid);
                    });
                Affects.erase(end, Affects.end());
            }

            // We expect database to drop commits without any side-effects
            // So we rewind serial to match what it would be after a reboot
            if (Affects.empty()) {
                Serial_ = Begin_;
            }

            Stats.TxCommited++;
            InTransaction = false;
        }

        void CommitScheme(TArrayRef<const TMemGlob> annex)
        {
            if (!SchemeRollbackState.Tables.empty() || SchemeRollbackState.Redo) {
                // Table or redo settings have changed
                CalculateAnnexEdge();
            }

            TScheme& scheme = *Scheme;
            for (auto& pr : SchemeRollbackState.Tables) {
                ui32 tid = pr.first;
                auto* info = scheme.GetTableInfo(tid);
                if (!info) {
                    // This table doesn't exist in current schema,
                    // which means it has been dropped.
                    Y_ENSURE(Tables.contains(tid), "Unexpected drop for a table that doesn't exist");
                    auto& wrap = Tables.at(tid);
                    Y_ENSURE(wrap.Dropped);
                    Y_ENSURE(!wrap.DataModified, "Unexpected drop of a modified table");
                    Y_ENSURE(!wrap.Truncated, "Unexpected drop of a truncated table");
                    if (wrap.RollbackPrepared) {
                        wrap->CommitChanges(annex);
                        wrap.RestoreMemStats(Stats);
                        wrap.RollbackPrepared = false;
                    }
                    wrap.Aggr(Stats, false /* leave */);
                    Deleted.emplace_back(tid);
                    Garbage.emplace_back(wrap->Unwrap());
                    Tables.erase(tid);
                    NUtil::SubSafe(Stats.Tables, ui32(1));
                    continue;
                }

                // This call will also apply schema changes
                auto& wrap = Get(tid, true);
                Y_ENSURE(!wrap.Dropped);
                Y_ENSURE(!wrap.SchemePending);
                Y_ENSURE(wrap.SchemeModified);

                if (wrap.Created) {
                    // If the table is both created and modified in the same
                    // transaction, then make sure flags are cleared and the
                    // table stats are accounted for.
                    wrap.Created = false;
                    wrap.DataModified = false;
                    Y_ENSURE(!wrap.RollbackPrepared);
                    Y_ENSURE(!wrap.Truncated);
                    wrap.EpochSnapshot.reset();
                    wrap->CommitNewTable(annex);
                    wrap.Aggr(Stats, true /* enter */);
                }

                wrap.SchemeModified = false;
            }

            SchemeRollbackState.Tables.clear();
            SchemeRollbackState.Executor.reset();
            SchemeRollbackState.Redo.reset();
        }

        void RollbackTransaction()
        {
            for (ui32 tid : Prepared) {
                auto& wrap = Tables.at(tid);
                Y_ENSURE(wrap.RollbackPrepared);
                wrap->RollbackChanges();
                wrap.RestoreMemStats(Stats);
                wrap.RollbackPrepared = false;
                wrap.SchemeModified = false;
                wrap.DataModified = false;
                wrap.Truncated = false;
            }
            Prepared.clear();

            for (ui32 tid : Flushed) {
                auto& wrap = Tables.at(tid);
                Y_ENSURE(wrap.EpochSnapshot);
                wrap.EpochSnapshot.reset();
            }
            Flushed.clear();

            for (ui32 tid : Affects) {
                auto& wrap = Tables.at(tid);
                if (!wrap.Created) {
                    wrap.Serial = wrap.SerialBackup;
                }
            }
            Affects.clear();

            RollbackScheme();
            Serial_ = Begin_;
            InTransaction = false;
        }

        void RollbackScheme()
        {
            // Note: we assume schema rollback is very rare,
            // so it doesn't have to be efficient
            TScheme& scheme = *Scheme;
            if (SchemeRollbackState.Redo) {
                scheme.Redo = *SchemeRollbackState.Redo;
                SchemeRollbackState.Redo.reset();
            }
            if (SchemeRollbackState.Executor) {
                scheme.Executor = *SchemeRollbackState.Executor;
                SchemeRollbackState.Executor.reset();
            }
            // First pass: we remove all modified tables from schema to handle renames
            for (auto& pr : SchemeRollbackState.Tables) {
                auto it = scheme.Tables.find(pr.first);
                if (it != scheme.Tables.end()) {
                    scheme.TableNames.erase(it->second.Name);
                    scheme.Tables.erase(it);
                }
            }
            // Second pass: restore all tables that existed before transaction started
            for (auto& pr : SchemeRollbackState.Tables) {
                if (pr.second) {
                    auto res = scheme.Tables.emplace(pr.first, *pr.second);
                    Y_ENSURE(res.second);
                    scheme.TableNames.emplace(res.first->second.Name, pr.first);
                }
            }
            // Third pass: we check modified tables and rollback their schema changes
            for (auto& pr : SchemeRollbackState.Tables) {
                ui32 tid = pr.first;
                auto& wrap = Tables.at(tid);
                if (wrap.Created) {
                    // This table didn't exist, just forget about it
                    Tables.erase(tid);
                    NUtil::SubSafe(Stats.Tables, ui32(1));
                    continue;
                }
                // By the time schema rollback is called we expect changes to be rolled back already
                Y_ENSURE(!wrap.SchemeModified, "Unexpected schema rollback on a modified table");
                Y_ENSURE(!wrap.EpochSnapshot, "Unexpected schema rollback on a flushed table");
                if (wrap.Dropped) {
                    // This table is no longer dropped
                    wrap.Dropped = false;
                }
                wrap.SchemePending = false;
            }
            SchemeRollbackState.Tables.clear();
        }

        void RunGC() {
            GCList->RunGC();
        }

        TDatabaseImpl& Switch(TTxStamp stamp)
        {
            Y_ENSURE(!InTransaction, "Unexpected switch inside a transaction");
            Y_ENSURE(Stamp <= stamp, "Executor tx stamp cannot go to the past");
            Stamp = stamp;

            First_ = Max<ui64>();
            Begin_ = Serial_;
            Stats.TxCommited++;
            Affects = { };

            return *this;
        }

        void Assign(TVector<TMemGlob> annex)
        {
            Y_ENSURE(!Annex, "Annex has been already attached to TDatabaseImpl");

            Annex = std::move(annex);
        }

        void ReplaceSlices(ui32 tid, TBundleSlicesMap slices)
        {
            auto &wrap = Get(tid, true);

            wrap.Aggr(Stats, false /* leave */);
            wrap->ReplaceSlices(std::move(slices));
            wrap.Aggr(Stats, true /* enter */);
        }

        void Replace(
            ui32 tid,
            const TSubset &subset,
            TArrayRef<const TPartView> newParts,
            TArrayRef<const TIntrusiveConstPtr<TTxStatusPart>> newTxStatus)
        {
            auto &wrap = Get(tid, true);

            wrap.Aggr(Stats, false /* leave */);
            wrap->Replace(subset, newParts, newTxStatus);
            wrap.Aggr(Stats, true /* enter */);
        }

        void Merge(ui32 tid, TPartView partView)
        {
            auto &wrap = Get(tid, true);

            wrap.Aggr(Stats, false /* leave */);
            wrap->Merge(std::move(partView));
            wrap.Aggr(Stats, true /* enter */);
        }

        void Merge(ui32 tid, TIntrusiveConstPtr<TColdPart> part)
        {
            auto &wrap = Get(tid, true);

            wrap.Aggr(Stats, false /* leave */);
            wrap->Merge(std::move(part));
            wrap.Aggr(Stats, true /* enter */);
        }

        void Merge(ui32 tid, TIntrusiveConstPtr<TTxStatusPart> txStatus)
        {
            auto &wrap = Get(tid, true);

            wrap.Aggr(Stats, false /* leave */);
            wrap->Merge(std::move(txStatus));
            wrap.Aggr(Stats, true /* enter */);
        }

        void MergeDone(ui32 tid)
        {
            auto &wrap = Get(tid, true);

            wrap.Aggr(Stats, false /* leave */);
            wrap->MergeDone();
            wrap.Aggr(Stats, true /* enter */);
        }

        void MergeDone()
        {
            for (auto &pr : Tables) {
                MergeDone(pr.first);
            }
        }

        bool ApplySchema(const TSchemeChanges &delta)
        {
            TModifier modifier(*Scheme);

            if (modifier.Apply(delta)) {
                ApplySchema(modifier.Affects);

                return true;
            } else {
                return false;
            }
        }

        TDatabaseImpl& ApplyRedo(TArrayRef<const char> plain)
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

        TTableWrapper& MakeTable(ui32 table, TSnapEdge edge)
        {
            if (edge.TxStamp == Max<ui64>()) {
                Y_TABLET_ERROR("Cannot make table on undefined TxStamp edge");
            } else if (edge.Head == TEpoch::Zero()) {
                /* Table written in compatability mode utilizes global
                    TxStamp instead of private TEpoch values. In order
                    to correcly handle legacy tables should rewind epoch
                    above of the last tx.
                 */

                ui64 head = edge.TxStamp + 1;
                Y_ENSURE(head < Max<i64>(), "TxStamp is too large for epoch");

                edge.Head = TEpoch(i64(head));
            }

            TArgs args{ table, edge.Head, edge.TxStamp, GCList };

            auto result = Tables.emplace(table, args);

            Y_ENSURE(result.second, "Table alredy exists");

            Stats.Tables += 1;

            return result.first->second;
        }

        void ApplySchema(const THashSet<ui32> &affects)
        {
            for (ui32 table : affects) {
                auto &wrap = Get(table, false);

                if (auto *info = Scheme->GetTableInfo(table)) {

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
        bool NeedIn(ui32 table)
        {
            /* Scheme deltas are applied before any redo log entries on
                db bootstrap and udate log entries for already deleted
                tables may appear. Weak stamp points to the end of asyncs.
             */

            const auto &wrap = Get(table, Weak <= Stamp);

            return wrap ? Stamp > wrap.Edge : false;
        }

        void DoBegin(ui32 tail, ui32 head, ui64 serial, ui64 stamp)
        {
            TAbi().Check(tail, head, "redo");

            /* Hack for redo logs affected by bug in KIKIMR-5323 */

            const auto back = Stamp - (head >= 21 ? 0 : Min(Stamp, ui64(2)));

            if (serial == 0) {
                Serial_++;  /* Legacy EvBegin without embedded serial */
            } else if (Serial_ < serial && (stamp == 0 || stamp >= back)) {
                Serial_ = serial;
            } else {
                Y_TABLET_ERROR("EvBegin{" << serial << " " << NFmt::TStamp(stamp)
                    << "} does not match db state {" << Serial_ << " "
                    << NFmt::TStamp(Stamp) << "} (redo log was reordered)");
            }

            First_ = Min(First_, Serial_);
        }

        void DoAnnex(TArrayRef<const TStdPad<NPageCollection::TGlobId>> annex)
        {
            if (Annex) {
                Y_ENSURE(annex.size() == Annex.size());

                for (auto it : xrange(Annex.size()))
                    if (Annex[it].GId != *annex[it]) {
                        Y_TABLET_ERROR("NRedo EvAnnex isn't match to assigned annex");
                    }

            } else {
                Annex.reserve(annex.size());

                for (auto &one : annex)
                    Annex.emplace_back(*one, TSharedData{ });
            }
        }

        void DoUpdate(ui32 tid, ERowOp rop, TKeys key, TOps ops, TRowVersion rowVersion)
        {
            auto &wrap = Touch(tid);

            NUtil::SubSafe(Stats.MemTableWaste, wrap->GetMemWaste());
            NUtil::SubSafe(Stats.MemTableBytes, wrap->GetMemSize());
            wrap->Update(rop, key, ops, Annex, rowVersion);
            Stats.MemTableWaste += wrap->GetMemWaste();
            Stats.MemTableBytes += wrap->GetMemSize();
            Stats.MemTableOps += 1;
        }

        void DoUpdateTx(ui32 tid, ERowOp rop, TKeys key, TOps ops, ui64 txId)
        {
            auto &wrap = Touch(tid);

            NUtil::SubSafe(Stats.MemTableWaste, wrap->GetMemWaste());
            NUtil::SubSafe(Stats.MemTableBytes, wrap->GetMemSize());
            wrap->UpdateTx(rop, key, ops, Annex, txId);
            Stats.MemTableWaste += wrap->GetMemWaste();
            Stats.MemTableBytes += wrap->GetMemSize();
            Stats.MemTableOps += 1;
        }

        void DoCommitTx(ui32 tid, ui64 txId, TRowVersion rowVersion)
        {
            auto &wrap = Touch(tid);

            NUtil::SubSafe(Stats.MemTableWaste, wrap->GetMemWaste());
            NUtil::SubSafe(Stats.MemTableBytes, wrap->GetMemSize());
            wrap->CommitTx(txId, rowVersion);
            Stats.MemTableWaste += wrap->GetMemWaste();
            Stats.MemTableBytes += wrap->GetMemSize();
        }

        void DoRemoveTx(ui32 tid, ui64 txId)
        {
            auto &wrap = Touch(tid);

            NUtil::SubSafe(Stats.MemTableWaste, wrap->GetMemWaste());
            NUtil::SubSafe(Stats.MemTableBytes, wrap->GetMemSize());
            wrap->RemoveTx(txId);
            Stats.MemTableWaste += wrap->GetMemWaste();
            Stats.MemTableBytes += wrap->GetMemSize();
        }

        void DoFlush(ui32 tid, ui64 /* stamp */, TEpoch epoch)
        {
            auto on = Touch(tid)->Snapshot();

            if (epoch != TEpoch::Zero() && epoch != on) {
                Y_TABLET_ERROR("EvFlush{" << tid << ", " << epoch << "eph} turned"
                        << " table to unexpected epoch " << on);
            }
        }

        TTableWrapper& Touch(ui32 table)
        {
            auto &wrap = Get(table, true);

            /* Modern redo log starts each logical update with single EvBegin
                allowing to grow db change serial number in a log driven way.
                Legacy log (Evolution < 12) have no EvBegin and progression
                of serial require hack with virtual insertion of EvBegin here.
             */
            if (Y_UNLIKELY(Serial_ == Begin_)) {
                ++Serial_;
            }

            if (wrap.Touch(Begin_, Serial_))
                Affects.emplace_back(table);

            First_ = Min(First_, Serial_);
            return wrap;
        }

    private:
        bool ApplyAlterRecord(const TAlterRecord& record) override
        {
            Y_ENSURE(InTransaction, "Unexpected ApplyAlterRecord outside of transaction");
            TSchemeModifier modifier(*Scheme, &SchemeRollbackState);
            bool changes = modifier.Apply(record);
            if (changes) {
                // There will be at most one table id
                for (ui32 tid : modifier.Affects) {
                    auto* wrap = Tables.FindPtr(tid);
                    if (!wrap) {
                        wrap = &MakeTable(tid, { });
                        wrap->Created = true;
                    }
                    Y_ENSURE(!wrap->DataModified, "Table " << tid << " cannot be altered after being changed");
                    Y_ENSURE(!wrap->Dropped, "Table " << tid << " cannot be altered after being dropped");
                    if (!Scheme->GetTableInfo(tid)) {
                        wrap->Dropped = true;
                    }
                    wrap->SchemePending = true;
                }
            }
            return changes;
        }

    public:
        void EnumerateTxStatusParts(const std::function<void(const TIntrusiveConstPtr<TTxStatusPart>&)>& callback) {
            for (auto &it : Tables) {
                it.second->EnumerateTxStatusParts(callback);
            }
        }

    public:
        TDbRuntimeStats GetRuntimeStats() const {
            TDbRuntimeStats stats;
            for (auto& pr : Tables) {
                // TODO: use a lazy aggregate to balance many idle tables vs frequent updates
                stats += pr.second->RuntimeStats();
            }
            return stats;
        }

    private:
        const TIntrusivePtr<TKeyRangeCacheNeedGCList> GCList;
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
        TVector<ui32> Flushed;
        TVector<ui32> Prepared;

        bool InTransaction = false;
        TSchemeRollbackState SchemeRollbackState;

    public:
        const TAutoPtr<TScheme> Scheme;
        TGarbage Garbage;       /* Unused full table subsets */
        TVector<ui32> Deleted;
        TVector<TChange::TTruncate> Truncated;
        TDbStats Stats;
        ui64 First_ = Max<ui64>(); /* First used serial after Switch() */
    };
}
}
