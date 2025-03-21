#include "flat_table.h"
#include "flat_row_celled.h"
#include "flat_row_remap.h"
#include "flat_bloom_hash.h"
#include "flat_part_iter.h"
#include "flat_part_laid.h"
#include "flat_part_charge_range.h"
#include "flat_part_charge_create.h"
#include "flat_part_dump.h"
#include "flat_range_cache.h"
#include "flat_util_misc.h"
#include "util_fmt_abort.h"

#include <ydb/library/yverify_stream/yverify_stream.h>
#include <ydb/core/base/appdata_fwd.h>
#include <ydb/core/base/feature_flags.h>

namespace NKikimr {
namespace NTable {

TTable::TTable(TEpoch epoch, const TIntrusivePtr<TKeyRangeCacheNeedGCList>& gcList)
    : Epoch(epoch)
    , EraseCacheGCList(gcList)
{ }

TTable::~TTable() { }

void TTable::PrepareRollback()
{
    Y_ABORT_UNLESS(!RollbackState);
    auto& state = RollbackState.emplace(Epoch);
    state.Annexed = Annexed;
    state.Scheme = Scheme;
    state.EraseCacheEnabled = EraseCacheEnabled;
    state.EraseCacheConfig = EraseCacheConfig;
    state.MutableExisted = bool(Mutable);
    state.MutableUpdated = false;
    state.DisableEraseCache = false;
}

void TTable::RollbackChanges()
{
    Y_ABORT_UNLESS(RollbackState, "PrepareRollback needed to rollback changes");
    auto& state = *RollbackState;

    CommitOps.clear();

    while (!RollbackOps.empty()) {
        struct TApplyRollbackOp {
            TTable* Self;

            void operator()(const TRollbackRemoveTxDataRef& op) const {
                Self->RemoveTxDataRef(op.TxId);
            }

            void operator()(const TRollbackRemoveTxStatusRef& op) const {
                Self->RemoveTxStatusRef(op.TxId);
            }

            void operator()(const TRollbackAddCommittedTx& op) const {
                Self->CommittedTransactions.Add(op.TxId, op.RowVersion);
            }

            void operator()(const TRollbackRemoveCommittedTx& op) const {
                Self->CommittedTransactions.Remove(op.TxId);
            }

            void operator()(const TRollbackAddRemovedTx& op) const {
                Self->RemovedTransactions.Add(op.TxId);
            }

            void operator()(const TRollbackRemoveRemovedTx& op) const {
                Self->RemovedTransactions.Remove(op.TxId);
            }
        };

        std::visit(TApplyRollbackOp{ this }, RollbackOps.back());
        RollbackOps.pop_back();
    }

    if (Epoch != state.Epoch) {
        // We performed a snapshot, roll it back
        Y_ABORT_UNLESS(MutableBackup, "Previous mem table missing");
        Mutable = std::move(MutableBackup);
    } else if (!state.MutableExisted) {
        // New memtable doesn't need rollback
        Mutable = nullptr;
    } else if (state.MutableUpdated) {
        Y_ABORT_UNLESS(Mutable, "Mutable was updated, but it is missing");
        Mutable->RollbackChanges();
    }
    Y_ABORT_UNLESS(!MutableBackup);

    Epoch = state.Epoch;
    Annexed = state.Annexed;
    if (state.Scheme) {
        Levels.Reset();
        Scheme = std::move(state.Scheme);
        EraseCacheEnabled = state.EraseCacheEnabled;
        EraseCacheConfig = state.EraseCacheConfig;
    }
    RollbackState.reset();
}

void TTable::CommitChanges(TArrayRef<const TMemGlob> blobs)
{
    Y_ABORT_UNLESS(RollbackState, "PrepareRollback needed to rollback changes");
    auto& state = *RollbackState;

    for (auto& op : CommitOps) {
        struct TApplyCommitOp {
            TTable* Self;

            void operator()(const TCommitAddDecidedTx& op) const {
                Self->DecidedTransactions.Add(op.TxId);
            }
        };

        std::visit(TApplyCommitOp{ this }, op);
    }

    CommitOps.clear();
    RollbackOps.clear();

    if (Epoch != state.Epoch) {
        if (Mutable && blobs) {
            Mutable->CommitBlobs(blobs);
        }
        // We performed a snapshot, move it to Frozen
        Y_ABORT_UNLESS(MutableBackup, "Mem table snaphot missing");
        Frozen.insert(MutableBackup);
        Stat_.FrozenWaste += MutableBackup->GetWastedMem();
        Stat_.FrozenSize += MutableBackup->GetUsedMem();
        Stat_.FrozenOps += MutableBackup->GetOpsCount();
        Stat_.FrozenRows += MutableBackup->GetRowCount();
        MutableBackup = nullptr;
    } else if (!state.MutableExisted) {
        // Fresh mem table is not prepared for rollback
        if (Mutable && blobs) {
            Mutable->CommitBlobs(blobs);
        }
    } else if (state.MutableUpdated) {
        Y_ABORT_UNLESS(Mutable, "Mutable was updated, but it is missing");
        Mutable->CommitChanges(blobs);
    }
    Y_ABORT_UNLESS(!MutableBackup);

    RollbackState.reset();
}

void TTable::CommitNewTable(TArrayRef<const TMemGlob> blobs)
{
    Y_ABORT_UNLESS(!RollbackState, "CommitBlobs must only be used for new tables without rollback");

    if (Mutable && blobs) {
        Mutable->CommitBlobs(blobs);
    }
}

void TTable::SetScheme(const TScheme::TTableInfo &table)
{
    Snapshot();

    Levels.Reset();
    ErasedKeysCache.Reset();

    Y_ABORT_UNLESS(!Mutable && table.Columns);

    if (RollbackState) {
        // Make sure we don't populate erase cache with keys based on a schema
        // which may end up rolling back.
        RollbackState->DisableEraseCache = true;
    }
    if (RollbackState && !RollbackState->Scheme) {
        RollbackState->Scheme = Scheme;
        RollbackState->EraseCacheEnabled = EraseCacheEnabled;
        RollbackState->EraseCacheConfig = EraseCacheConfig;
    }

    auto to = TRowScheme::Make(table.Columns, NUtil::TSecond());

    if (auto was = std::exchange(Scheme, to))
        was->CheckCompatibility(table.Name, *Scheme);

    /* This restriction is required for external blobs inverted index, for
        details read NPage::TFrames and NFwd blobs cache implementation. */

    Y_ABORT_UNLESS(Scheme->Cols.size() <= ui32(-Min<i16>()), "Too many columns in row");

    EraseCacheEnabled = table.EraseCacheEnabled;
    EraseCacheConfig = { };
    if (table.EraseCacheMinRows) {
        EraseCacheConfig.MinRows = table.EraseCacheMinRows;
    }
    if (table.EraseCacheMaxBytes) {
        EraseCacheConfig.MaxBytes = table.EraseCacheMaxBytes;
    }
}

TIntrusiveConstPtr<TRowScheme> TTable::GetScheme() const noexcept
{
    return Scheme;
}

TAutoPtr<TSubset> TTable::CompactionSubset(TEpoch head, TArrayRef<const TLogoBlobID> bundle)
{
    head = Min(head, Epoch);

    TAutoPtr<TSubset> subset = new TSubset(head, Scheme);

    if (head > TEpoch::Zero()) {
        for (auto &x : Frozen) {
            if (x->Epoch < head) {
                subset->Frozen.emplace_back(x, x->Immediate());
            }
        }
        if (MutableBackup && MutableBackup->Epoch < head) {
            subset->Frozen.emplace_back(MutableBackup, MutableBackup->Immediate());
        }
        for (const auto &pr : TxStatus) {
            if (pr.second->Epoch < head) {
                subset->TxStatus.emplace_back(pr.second);
            }
        }
    }

    subset->Flatten.reserve(bundle.size());
    for (const TLogoBlobID &token : bundle) {
        if (auto* c = ColdParts.FindPtr(token)) {
            subset->ColdParts.push_back(*c);
            continue;
        }
        auto* p = Flatten.FindPtr(token);
        Y_VERIFY_S(p, "Cannot find part " << token);
        subset->Flatten.push_back(*p);
    }

    subset->CommittedTransactions = CommittedTransactions;
    subset->RemovedTransactions = RemovedTransactions;
    if (!ColdParts) {
        subset->GarbageTransactions = GarbageTransactions;
    }

    return subset;
}

TAutoPtr<TSubset> TTable::PartSwitchSubset(TEpoch head, TArrayRef<const TLogoBlobID> bundle, TArrayRef<const TLogoBlobID> txStatus)
{
    head = Min(head, Epoch);

    TAutoPtr<TSubset> subset = new TSubset(head, Scheme);

    if (head > TEpoch::Zero()) {
        for (auto &x : Frozen) {
            if (x->Epoch < head) {
                subset->Frozen.emplace_back(x, x->Immediate());
            }
        }
        if (MutableBackup && MutableBackup->Epoch < head) {
            subset->Frozen.emplace_back(MutableBackup, MutableBackup->Immediate());
        }
    }

    subset->Flatten.reserve(bundle.size());
    for (const TLogoBlobID &token : bundle) {
        if (auto* c = ColdParts.FindPtr(token)) {
            subset->ColdParts.push_back(*c);
            continue;
        }
        auto* p = Flatten.FindPtr(token);
        Y_VERIFY_S(p, "Cannot find part " << token);
        subset->Flatten.push_back(*p);
    }

    subset->TxStatus.reserve(txStatus.size());
    for (const TLogoBlobID &token : txStatus) {
        auto* p = TxStatus.FindPtr(token);
        Y_VERIFY_S(p, "Cannot find tx status " << token);
        subset->TxStatus.push_back(*p);
    }

    subset->CommittedTransactions = CommittedTransactions;
    subset->RemovedTransactions = RemovedTransactions;
    if (!ColdParts) {
        subset->GarbageTransactions = GarbageTransactions;
    }

    return subset;
}

TAutoPtr<TSubset> TTable::Subset(TEpoch head) const
{
    head = Min(head, Epoch);

    TAutoPtr<TSubset> subset = new TSubset(head, Scheme);

    for (const auto &it : TxStatus) {
        if (it.second->Epoch < head) {
            subset->TxStatus.emplace_back(it.second);
        }
    }

    for (auto &it: ColdParts)
        if (it.second->Epoch < head)
            subset->ColdParts.push_back(it.second);

    for (auto &it: Flatten)
        if (it.second->Epoch < head)
            subset->Flatten.push_back(it.second);

    for (auto &it : Frozen)
        if (it->Epoch < head)
            subset->Frozen.emplace_back(it, it->Immediate());

    if (MutableBackup && MutableBackup->Epoch < head) {
        subset->Frozen.emplace_back(MutableBackup, MutableBackup->Immediate());
    }

    // This method is normally used when we want to take some state snapshot
    // However it can still theoretically be used for iteration or compaction
    subset->CommittedTransactions = CommittedTransactions;
    subset->RemovedTransactions = RemovedTransactions;
    if (!ColdParts) {
        subset->GarbageTransactions = GarbageTransactions;
    }

    return subset;
}

bool TTable::HasBorrowed(ui64 selfTabletId) const
{
    for (const auto &it : TxStatus)
        if (it.second->Label.TabletID() != selfTabletId)
            return true;

    for (auto &it: Flatten)
        if (it.second->Label.TabletID() != selfTabletId)
            return true;

    for (auto &it: ColdParts)
        if (it.second->Label.TabletID() != selfTabletId)
            return true;

    return false;
}

TAutoPtr<TSubset> TTable::ScanSnapshot(TRowVersion snapshot)
{
    if (RollbackState) {
        Y_ABORT_UNLESS(Epoch == RollbackState->Epoch &&
            RollbackState->MutableExisted == bool(Mutable) &&
            !RollbackState->MutableUpdated,
            "Cannot take scan snapshot of a modified table");
    }

    TAutoPtr<TSubset> subset = new TSubset(Epoch, Scheme);

    // TODO: we could filter LSM by the provided snapshot version, but it
    // cannot be a simple if condition since row versions may intersect in
    // non-trivial ways. E.g. update x may be on top of y, x < snapshot < y,
    // but we cannot drop y since x is actually visible. Only the guaranteed
    // invisible top layer (as sorted by epoch) may be excluded from subset.

    for (auto& it : ColdParts) {
        subset->ColdParts.push_back(it.second);
    }

    for (auto& it : Flatten) {
        subset->Flatten.push_back(it.second);
    }

    for (auto& it : Frozen) {
        subset->Frozen.emplace_back(it, it->Immediate());
    }

    if (Mutable && Mutable->GetMinRowVersion() <= snapshot) {
        subset->Frozen.emplace_back(Mutable, Mutable->Snapshot());
    }

    subset->CommittedTransactions = CommittedTransactions;

    return subset;
}

TAutoPtr<TSubset> TTable::Unwrap()
{
    Snapshot();

    auto subset = Subset(TEpoch::Max());

    Replace(*subset, { }, { });

    Y_ABORT_UNLESS(!(Flatten || Frozen || Mutable || TxStatus));

    return subset;
}

TBundleSlicesMap TTable::LookupSlices(TArrayRef<const TLogoBlobID> bundles) const
{
    TBundleSlicesMap slices;
    for (const TLogoBlobID &bundle : bundles) {
        auto it = Flatten.find(bundle);
        if (it != Flatten.end()) {
            slices[bundle] = it->second.Slices;
        }
    }
    return slices;
}

void TTable::ReplaceSlices(TBundleSlicesMap slices)
{
    Y_ABORT_UNLESS(!RollbackState, "Cannot perform this in a transaction");

    for (auto &kv : slices) {
        auto it = Flatten.find(kv.first);
        Y_ABORT_UNLESS(it != Flatten.end(), "Got an unknown TPart in ReplaceSlices");
        Y_ABORT_UNLESS(kv.second && *kv.second, "Got an empty TPart in ReplaceSlices");
        it->second.Slices = std::move(kv.second);
        it->second.Screen = it->second.Slices->ToScreen();
    }
    if (slices) {
        Levels.Reset();
        // Note: ReplaceSlices does not introduce any new rows, so we don't
        // have to invalidate current erase cache.
    }
}

void TTable::Replace(
    const TSubset& subset,
    TArrayRef<const TPartView> newParts,
    TArrayRef<const TIntrusiveConstPtr<TTxStatusPart>> newTxStatus)
{
    Y_ABORT_UNLESS(!RollbackState, "Cannot perform this in a transaction");

    for (const auto& partView : newParts) {
        Y_ABORT_UNLESS(partView, "Replace(...) shouldn't get empty parts");
        Y_ABORT_UNLESS(!partView.Screen, "Replace(...) shouldn't get screened parts");
        Y_ABORT_UNLESS(partView.Slices && *partView.Slices, "Got parts without slices");
        if (Flatten.contains(partView->Label) || ColdParts.contains(partView->Label)) {
            Y_Fail("Duplicate bundle " << partView->Label);
        }
    }

    if (subset.Flatten) {
        Levels.Reset();
    }

    bool removingOld = false;
    bool addingNew = false;

    // Note: we remove old parts first and add new ones next
    // Refcount cannot become zero more than once so vectors are unique
    std::vector<ui64> checkTxDataRefs;
    std::vector<ui64> checkTxStatusRefs;

    for (auto& memTable : subset.Frozen) {
        removingOld = true;
        const auto found = Frozen.erase(memTable.MemTable);

        Y_ABORT_UNLESS(found == 1, "Got an unknown TMemTable table in TSubset");

        NUtil::SubSafe(Stat_.FrozenWaste, memTable->GetWastedMem());
        NUtil::SubSafe(Stat_.FrozenSize, memTable->GetUsedMem());
        NUtil::SubSafe(Stat_.FrozenOps,  memTable->GetOpsCount());
        NUtil::SubSafe(Stat_.FrozenRows, memTable->GetRowCount());

        for (const auto &pr : memTable.MemTable->GetTxIdStats()) {
            const ui64 txId = pr.first;
            auto& count = TxDataRefs.at(txId);
            Y_ABORT_UNLESS(count > 0);
            if (0 == --count) {
                checkTxDataRefs.push_back(txId);
            }
        }

        for (const auto &pr : memTable.MemTable->GetCommittedTransactions()) {
            const ui64 txId = pr.first;
            auto& count = TxStatusRefs.at(txId);
            Y_ABORT_UNLESS(count > 0);
            if (0 == --count) {
                checkTxStatusRefs.push_back(txId);
            }
        }

        for (ui64 txId : memTable.MemTable->GetRemovedTransactions()) {
            auto& count = TxStatusRefs.at(txId);
            Y_ABORT_UNLESS(count > 0);
            if (0 == --count) {
                checkTxStatusRefs.push_back(txId);
            }
        }
    }

    for (auto &part : subset.Flatten) {
        removingOld = true;
        Y_ABORT_UNLESS(part.Slices && *part.Slices,
            "Got an empty TPart subset in TSubset");

        auto it = Flatten.find(part->Label);
        Y_ABORT_UNLESS(it != Flatten.end(), "Got an unknown TPart table in TSubset");
        auto& existing = it->second;

        Y_ABORT_UNLESS(existing.Slices && *existing.Slices,
            "Existing table part has an unexpected empty bounds run");

        if (!TSlices::EqualByRowId(existing.Slices, part.Slices)) {
            if (!TSlices::SupersetByRowId(existing.Slices, part.Slices)) {
                Y_Fail("Removing unexpected subset " << NFmt::Do(*part.Slices)
                    << " from existing " << NFmt::Do(*existing.Slices));
            }

            auto left = TSlices::Subtract(existing.Slices, part.Slices);
            if (left->empty()) {
                Y_Fail("Empty result after removing " << NFmt::Do(*part.Slices)
                    << " from existing " << NFmt::Do(*existing.Slices));
            }

            existing.Slices = std::move(left);
            existing.Screen = existing.Slices->ToScreen();
            continue;
        }

        if (existing->TxIdStats) {
            for (const auto& item : existing->TxIdStats->GetItems()) {
                const ui64 txId = item.GetTxId();
                auto& count = TxDataRefs.at(txId);
                Y_ABORT_UNLESS(count > 0);
                if (0 == --count) {
                    checkTxDataRefs.push_back(txId);
                }
            }
        }

        // Remove this part completely
        Flatten.erase(it);

        Stat_.Parts.Remove(part);
        if (!Stat_.PartsPerTablet[part->Label.TabletID()].Remove(part)) {
            Stat_.PartsPerTablet.erase(part->Label.TabletID());
        }
    }

    for (auto &part : subset.ColdParts) {
        removingOld = true;
        auto it = ColdParts.find(part->Label);
        Y_ABORT_UNLESS(it != ColdParts.end(), "Got an unknown TColdPart in TSubset");
        ColdParts.erase(it);
    }

    for (auto& part : subset.TxStatus) {
        removingOld = true;
        Y_ABORT_UNLESS(part, "Unexpected empty TTxStatusPart in TSubset");

        auto it = TxStatus.find(part->Label);
        Y_ABORT_UNLESS(it != TxStatus.end());
        TxStatus.erase(it);

        for (auto& item : part->TxStatusPage->GetCommittedItems()) {
            const ui64 txId = item.GetTxId();
            auto& count = TxStatusRefs.at(txId);
            Y_ABORT_UNLESS(count > 0);
            if (0 == --count) {
                checkTxStatusRefs.push_back(txId);
            }
        }
        for (auto& item : part->TxStatusPage->GetRemovedItems()) {
            const ui64 txId = item.GetTxId();
            auto& count = TxStatusRefs.at(txId);
            Y_ABORT_UNLESS(count > 0);
            if (0 == --count) {
                checkTxStatusRefs.push_back(txId);
            }
        }
    }

    for (const auto &partView : newParts) {
        addingNew = true;
        if (Mutable && partView->Epoch >= Mutable->Epoch) {
            Y_Fail("Replace with " << NFmt::Do(*partView) << " after mutable epoch " << Mutable->Epoch);
        }

        if (Frozen && partView->Epoch >= (*Frozen.begin())->Epoch) {
            Y_Fail("Replace with " << NFmt::Do(*partView) << " after frozen epoch " << (*Frozen.begin())->Epoch);
        }

        Epoch = Max(Epoch, partView->Epoch + 1);

        AddSafe(partView);
    }

    for (const auto& txStatus : newTxStatus) {
        if (Mutable && txStatus->Epoch >= Mutable->Epoch) {
            Y_Fail("Replace with " << NFmt::Do(*txStatus) << " after mutable epoch " << Mutable->Epoch);
        }

        if (Frozen && txStatus->Epoch >= (*Frozen.begin())->Epoch) {
            Y_Fail("Replace with " << NFmt::Do(*txStatus) << " after frozen epoch " << (*Frozen.begin())->Epoch);
        }

        Epoch = Max(Epoch, txStatus->Epoch + 1);

        auto res = TxStatus.emplace(txStatus->Label, txStatus);
        Y_ABORT_UNLESS(res.second, "Unexpected failure to add a new TTxStatusPart");

        for (auto& item : txStatus->TxStatusPage->GetCommittedItems()) {
            const ui64 txId = item.GetTxId();
            AddTxStatusRef(txId);
        }
        for (auto& item : txStatus->TxStatusPage->GetRemovedItems()) {
            const ui64 txId = item.GetTxId();
            AddTxStatusRef(txId);
        }
    }

    for (ui64 txId : checkTxDataRefs) {
        auto it = TxDataRefs.find(txId);
        Y_ABORT_UNLESS(it != TxDataRefs.end());
        if (it->second == 0) {
            // Transaction no longer has any known rows
            TxDataRefs.erase(it);
            OpenTxs.erase(txId);
            if (TxStatusRefs.contains(txId)) {
                DecidedTransactions.Remove(txId);
                GarbageTransactions.Add(txId);
            }
        }
    }

    for (ui64 txId : checkTxStatusRefs) {
        auto it = TxStatusRefs.find(txId);
        Y_ABORT_UNLESS(it != TxStatusRefs.end());
        if (it->second == 0) {
            // This transaction no longer has any known status
            TxStatusRefs.erase(it);
            CommittedTransactions.Remove(txId);
            RemovedTransactions.Remove(txId);
            GarbageTransactions.Remove(txId);
            if (TxDataRefs.contains(txId)) {
                // In the unlikely case it has some data it is now open
                DecidedTransactions.Remove(txId);
                OpenTxs.insert(txId);
            }
        }
    }

    if (!removingOld && addingNew) {
        // Note: we invalidate erase cache when nothing old is removed,
        // because followers always call Replace, even when leader called
        // Merge. When something is removed we can assume it's a compaction
        // and compactions don't add new rows to the table, keeping erase
        // cache valid.
        ErasedKeysCache.Reset();
    }
}

void TTable::Merge(TPartView partView)
{
    Y_ABORT_UNLESS(!RollbackState, "Cannot perform this in a transaction");

    Y_ABORT_UNLESS(partView, "Merge(...) shouldn't get empty part");
    Y_ABORT_UNLESS(partView.Slices, "Merge(...) shouldn't get parts without slices");

    if (Mutable && partView->Epoch >= Mutable->Epoch) {
        Y_Fail("Merge " << NFmt::Do(*partView) << " after mutable epoch " << Mutable->Epoch);
    }

    if (Frozen && partView->Epoch >= (*Frozen.begin())->Epoch) {
        Y_Fail("Merge " << NFmt::Do(*partView) << " after frozen epoch " << (*Frozen.begin())->Epoch);
    }

    auto it = Flatten.find(partView->Label);

    if (it == Flatten.end()) {
        Epoch = Max(Epoch, partView->Epoch + 1);

        AddSafe(std::move(partView));
    } else if (it->second->Epoch != partView->Epoch) {
        Y_ABORT("Got the same labeled parts with different epoch");
    } else {
        Levels.Reset();
        it->second.Screen = TScreen::Join(it->second.Screen, partView.Screen);
        it->second.Slices = TSlices::Merge(it->second.Slices, partView.Slices);
    }

    // Note: Merge is called when borrowing data, which may introduce new rows
    // and invalidate current erase cache.
    ErasedKeysCache.Reset();
}

void TTable::Merge(TIntrusiveConstPtr<TColdPart> part)
{
    Y_ABORT_UNLESS(!RollbackState, "Cannot perform this in a transaction");

    Y_ABORT_UNLESS(part, "Merge(...) shouldn't get empty parts");

    if (Mutable && part->Epoch >= Mutable->Epoch) {
        Y_Fail("Merge " << NFmt::Do(*part) << " after mutable epoch " << Mutable->Epoch);
    }

    if (Frozen && part->Epoch >= (*Frozen.begin())->Epoch) {
        Y_Fail("Merge " << NFmt::Do(*part) << " after frozen epoch " << (*Frozen.begin())->Epoch);
    }

    auto it = Flatten.find(part->Label);
    Y_VERIFY_S(it == Flatten.end(), "Merge " << NFmt::Do(*part) << " when a loaded part already exists");

    auto itCold = ColdParts.find(part->Label);
    Y_VERIFY_S(itCold == ColdParts.end(), "Merge " << NFmt::Do(*part) << " when another cold part already exists");

    const auto label = part->Label;

    Epoch = Max(Epoch, part->Epoch + 1);
    ColdParts.emplace(label, std::move(part));

    Levels.Reset();

    // Note: Merge is called when borrowing data, which may introduce new rows
    // and invalidate current erase cache.
    ErasedKeysCache.Reset();
}

void TTable::Merge(TIntrusiveConstPtr<TTxStatusPart> txStatus)
{
    Y_ABORT_UNLESS(!RollbackState, "Cannot perform this in a transaction");

    Y_ABORT_UNLESS(txStatus, "Unexpected empty TTxStatusPart");

    for (auto& item : txStatus->TxStatusPage->GetCommittedItems()) {
        const ui64 txId = item.GetTxId();
        AddTxStatusRef(txId);
        const auto rowVersion = item.GetRowVersion();
        if (const auto* prev = CommittedTransactions.Find(txId); Y_LIKELY(!prev) || *prev > rowVersion) {
            CommittedTransactions.Add(txId, rowVersion);
            if (!prev) {
                if (RemovedTransactions.Remove(txId)) {
                    // Transaction was in a removed set and now it's committed
                    // This is not an error in some cases, but may be suspicious
                    RemovedCommittedTxs++;
                }
            }
        }
    }
    for (auto& item : txStatus->TxStatusPage->GetRemovedItems()) {
        const ui64 txId = item.GetTxId();
        AddTxStatusRef(txId);
        if (const auto* prev = CommittedTransactions.Find(txId); Y_LIKELY(!prev)) {
            RemovedTransactions.Add(txId);
        } else {
            // Transaction is in a committed set but also removed
            // This is not an error in some cases, but may be suspicious
            RemovedCommittedTxs++;
        }
    }

    if (Mutable && txStatus->Epoch >= Mutable->Epoch) {
        Y_Fail("Merge " << NFmt::Do(*txStatus) << " after mutable epoch " << Mutable->Epoch);
    }

    if (Frozen && txStatus->Epoch >= (*Frozen.begin())->Epoch) {
        Y_Fail("Merge " << NFmt::Do(*txStatus) << " after frozen epoch " << (*Frozen.begin())->Epoch);
    }

    Epoch = Max(Epoch, txStatus->Epoch + 1);

    auto res = TxStatus.emplace(txStatus->Label, txStatus);
    Y_ABORT_UNLESS(res.second, "Unexpected failure to add a new TTxStatusPart");

    // Note: Merge is called when borrowing data, but new tx status may commit
    // or rollback some transactions, and erase cache already accounts for that
    // eventuality, so doesn't need to be invalidated.
}

void TTable::MergeDone()
{
    // nothing
}

const TLevels& TTable::GetLevels() const
{
    if (!Levels) {
        Y_ABORT_UNLESS(ColdParts.empty(), "Cannot construct Levels with cold parts");
        TVector<const TPartView*> parts; // TPartView* avoids expensive atomic ops
        parts.reserve(Flatten.size());
        for (const auto& kv : Flatten) {
            parts.push_back(&kv.second);
        }
        std::sort(parts.begin(), parts.end(),
            [](const TPartView* a, const TPartView* b) {
                if (a->Part->Epoch != b->Part->Epoch) {
                    return a->Part->Epoch < b->Part->Epoch;
                }
                return a->Part->Label < b->Part->Label;
            });
        Levels.Reset(new TLevels(Scheme->Keys));
        for (const TPartView* p : parts) {
            Levels->Add(p->Part, p->Slices);
        }
    }
    return *Levels;
}

ui64 TTable::GetSearchHeight() const
{
    if (!ColdParts.empty())
        return 0;

    ui64 height = GetLevels().size() + Frozen.size();
    if (Mutable)
        ++height;

    return height;
}

TVector<TIntrusiveConstPtr<TMemTable>> TTable::GetMemTables() const
{
    Y_ABORT_UNLESS(!RollbackState, "Cannot perform this in a transaction");

    TVector<TIntrusiveConstPtr<TMemTable>> vec(Frozen.begin(), Frozen.end());

    if (Mutable)
        vec.emplace_back(Mutable);

    return vec;
}

TEpoch TTable::Snapshot()
{
    if (Mutable) {
        Annexed = Mutable->GetBlobs()->Tail();

        if (RollbackState) {
            Y_ABORT_UNLESS(
                RollbackState->Epoch == Mutable->Epoch &&
                RollbackState->MutableExisted &&
                !RollbackState->MutableUpdated,
                "Cannot snapshot a modified table");
            Y_ABORT_UNLESS(!MutableBackup, "Another mutable backup already exists");
            MutableBackup = std::move(Mutable);
        } else {
            Frozen.insert(Mutable);
            Stat_.FrozenWaste += Mutable->GetWastedMem();
            Stat_.FrozenSize += Mutable->GetUsedMem();
            Stat_.FrozenOps += Mutable->GetOpsCount();
            Stat_.FrozenRows += Mutable->GetRowCount();
        }

        Mutable = nullptr; /* have to make new TMemTable on next update */

        if (++Epoch == TEpoch::Max()) {
            Y_ABORT("Table epoch counter has reached infinity value");
        }
    }

    return Epoch;
}

void TTable::AddSafe(TPartView partView)
{
    if (partView) {
        Y_ABORT_UNLESS(partView->Epoch < Epoch, "Cannot add part above head epoch");

        Stat_.Parts.Add(partView);
        Stat_.PartsPerTablet[partView->Label.TabletID()].Add(partView);

        if (partView->TxIdStats) {
            for (const auto& item : partView->TxIdStats->GetItems()) {
                const ui64 txId = item.GetTxId();
                AddTxDataRef(txId);
            }
        }

        if (FlattenEpoch <= partView->Epoch) {
            FlattenEpoch = partView->Epoch;
            if (Levels) {
                // Slices from this part may be added on top
                Levels->Add(partView.Part, partView.Slices);
            }
        } else {
            Levels.Reset();
        }

        bool done = Flatten.emplace(partView->Label, std::move(partView)).second;
        Y_ABORT_UNLESS(done);
    }
}

EReady TTable::Precharge(TRawVals minKey_, TRawVals maxKey_, TTagsRef tags,
                         IPages* env, ui64 flg,
                         ui64 items, ui64 bytes,
                         EDirection direction,
                         TRowVersion snapshot,
                         TSelectStats& stats) const
{
    bool ready = true;
    bool includeHistory = !snapshot.IsMax();

    if (items == Max<ui64>()) {
        items = 0; // disable limits
    }

    if (bytes == Max<ui64>()) {
        bytes = 0; // disable limits
    }

    if (minKey_.size() && minKey_.data() == maxKey_.data()) {
        const TCelled key(minKey_, *Scheme->Keys, false);
        const NBloom::TPrefix prefix(key);

        for (const auto& run : GetLevels()) {
            auto pos = run.Find(key);
            if (pos != run.end()) {
                const auto* part = pos->Part.Get();
                if ((flg & EHint::NoByKey) ||
                    part->MightHaveKey(prefix.Get(part->Scheme->Groups[0].KeyTypes.size())))
                {
                    TRowId row1 = pos->Slice.BeginRowId();
                    TRowId row2 = pos->Slice.EndRowId() - 1;
                    ready &= CreateCharge(env, *pos->Part, tags, includeHistory)
                        ->Do(key, key, row1, row2, *Scheme->Keys, items, bytes)
                        .Ready;
                    ++stats.Sieved;
                } else {
                    ++stats.Weeded;
                }
            }
        }
    } else {
        const TCelled minKey(minKey_, *Scheme->Keys, false);
        const TCelled maxKey(maxKey_, *Scheme->Keys, false);

        for (const auto& run : GetLevels()) {
            switch (direction) {
                case EDirection::Forward:
                    ready &= ChargeRange(env, minKey, maxKey, run, *Scheme->Keys, tags, items, bytes, includeHistory);
                    break;
                case EDirection::Reverse:
                    ready &= ChargeRangeReverse(env, maxKey, minKey, run, *Scheme->Keys, tags, items, bytes, includeHistory);
                    break;
            }
        }
    }

    return ready ? EReady::Data : EReady::Page;
}

void TTable::Update(ERowOp rop, TRawVals key, TOpsRef ops, TArrayRef<const TMemGlob> apart, TRowVersion rowVersion)
{
    Y_ABORT_UNLESS(!(ops && TCellOp::HaveNoOps(rop)), "Given ERowOp can't have ops");

    if (ErasedKeysCache && rop != ERowOp::Erase) {
        const TCelled cells(key, *Scheme->Keys, true);
        auto res = ErasedKeysCache->FindKey(cells);
        if (res.second) {
            ErasedKeysCache->InvalidateKey(res.first, cells);
        }
    }

    MemTable().Update(rop, key, ops, apart, rowVersion, CommittedTransactions);
    if (TableObserver) {
        TableObserver->OnUpdate(rop, key, ops, rowVersion);
    }
}

void TTable::AddTxDataRef(ui64 txId)
{
    auto it = TxDataRefs.find(txId);
    if (it == TxDataRefs.end()) {
        TxDataRefs.emplace(txId, 1);
        if (TxStatusRefs.contains(txId)) {
            GarbageTransactions.Remove(txId);
            if (RollbackState) {
                CommitOps.emplace_back(TCommitAddDecidedTx{ txId });
            } else {
                DecidedTransactions.Add(txId);
            }
        } else {
            OpenTxs.insert(txId);
        }
    } else {
        ++it->second;
    }
    if (RollbackState) {
        RollbackOps.emplace_back(TRollbackRemoveTxDataRef{ txId });
    }
}

void TTable::RemoveTxDataRef(ui64 txId)
{
    auto it = TxDataRefs.find(txId);
    Y_ABORT_UNLESS(it != TxDataRefs.end());
    Y_ABORT_UNLESS(it->second > 0);
    if (0 == --it->second) {
        // This was the last reference
        TxDataRefs.erase(it);
        OpenTxs.erase(txId);
        if (TxStatusRefs.contains(txId)) {
            DecidedTransactions.Remove(txId);
            GarbageTransactions.Add(txId);
        }
    }
}

void TTable::AddTxStatusRef(ui64 txId)
{
    auto it = TxStatusRefs.find(txId);
    if (it == TxStatusRefs.end()) {
        TxStatusRefs.emplace(txId, 1);
        if (TxDataRefs.contains(txId)) {
            OpenTxs.erase(txId);
            if (RollbackState) {
                CommitOps.emplace_back(TCommitAddDecidedTx{ txId });
            } else {
                DecidedTransactions.Add(txId);
            }
        } else {
            GarbageTransactions.Add(txId);
        }
    } else {
        ++it->second;
    }
    if (RollbackState) {
        RollbackOps.emplace_back(TRollbackRemoveTxStatusRef{ txId });
    }
}

void TTable::RemoveTxStatusRef(ui64 txId)
{
    auto it = TxStatusRefs.find(txId);
    Y_ABORT_UNLESS(it != TxStatusRefs.end());
    Y_ABORT_UNLESS(it->second > 0);
    if (0 == --it->second) {
        // This was the last reference
        TxStatusRefs.erase(it);
        // Note: committed/removed are rolled back separately
        GarbageTransactions.Remove(txId);
        if (TxDataRefs.contains(txId)) {
            DecidedTransactions.Remove(txId);
            OpenTxs.insert(txId);
        }
    }
}

void TTable::UpdateTx(ERowOp rop, TRawVals key, TOpsRef ops, TArrayRef<const TMemGlob> apart, ui64 txId)
{
    auto& memTable = MemTable();
    bool hadTxDataRef = memTable.GetTxIdStats().contains(txId);

    if (ErasedKeysCache) {
        const TCelled cells(key, *Scheme->Keys, true);
        auto res = ErasedKeysCache->FindKey(cells);
        if (res.second) {
            ErasedKeysCache->InvalidateKey(res.first, cells);
        }
    }

    // Use a special row version that marks this update as uncommitted
    TRowVersion rowVersion(Max<ui64>(), txId);
    MemTable().Update(rop, key, ops, apart, rowVersion, CommittedTransactions);

    if (!hadTxDataRef) {
        Y_DEBUG_ABORT_UNLESS(memTable.GetTxIdStats().contains(txId));
        AddTxDataRef(txId);
    } else {
        Y_DEBUG_ABORT_UNLESS(TxDataRefs[txId] > 0);
    }

    if (TableObserver) {
        TableObserver->OnUpdateTx(rop, key, ops, txId);
    }
}

void TTable::CommitTx(ui64 txId, TRowVersion rowVersion)
{
    // TODO: track suspicious transactions (not open at commit time)
    if (MemTable().CommitTx(txId, rowVersion)) {
        AddTxStatusRef(txId);
    }

    // Note: it is possible to have multiple CommitTx for the same TxId but at
    // different row versions. The commit with the minimum row version wins.
    if (const auto* prev = CommittedTransactions.Find(txId); Y_LIKELY(!prev) || *prev > rowVersion) {
        if (RollbackState) {
            if (prev) {
                RollbackOps.emplace_back(TRollbackAddCommittedTx{ txId, *prev });
            } else {
                RollbackOps.emplace_back(TRollbackRemoveCommittedTx{ txId });
            }
        }
        CommittedTransactions.Add(txId, rowVersion);
        if (!prev) {
            if (RollbackState && RemovedTransactions.Contains(txId)) {
                RollbackOps.emplace_back(TRollbackAddRemovedTx{ txId });
            }
            if (RemovedTransactions.Remove(txId)) {
                // Transaction was in a removed set and now it's committed
                // This is not an error in some cases, but may be suspicious
                RemovedCommittedTxs++;
            }
        }
    }

    // Note: erase cache accounts for changes that may commit, no need to invalidate
}

void TTable::RemoveTx(ui64 txId)
{
    // TODO: track suspicious transactions (not open at remove time)
    if (MemTable().RemoveTx(txId)) {
        AddTxStatusRef(txId);
    }

    // Note: it is possible to have both CommitTx and RemoveTx for the same TxId
    // due to complicated split/merge shard interactions. The commit actually
    // wins over removes in all cases.
    if (const auto* prev = CommittedTransactions.Find(txId); Y_LIKELY(!prev)) {
        if (RollbackState && !RemovedTransactions.Contains(txId)) {
            RollbackOps.emplace_back(TRollbackRemoveRemovedTx{ txId });
        }
        RemovedTransactions.Add(txId);
    } else {
        // Transaction is in a committed set but also removed
        // This is not an error in some cases, but may be suspicious
        RemovedCommittedTxs++;
    }
}

bool TTable::HasOpenTx(ui64 txId) const
{
    return OpenTxs.contains(txId);
}

bool TTable::HasTxData(ui64 txId) const
{
    return TxDataRefs.contains(txId) || TxStatusRefs.contains(txId);
}

bool TTable::HasCommittedTx(ui64 txId) const
{
    return CommittedTransactions.Find(txId);
}

bool TTable::HasRemovedTx(ui64 txId) const
{
    return RemovedTransactions.Contains(txId);
}

const absl::flat_hash_set<ui64>& TTable::GetOpenTxs() const
{
    return OpenTxs;
}

size_t TTable::GetOpenTxCount() const
{
    return OpenTxs.size();
}

size_t TTable::GetTxsWithDataCount() const
{
    return TxDataRefs.size();
}

size_t TTable::GetCommittedTxCount() const
{
    return CommittedTransactions.Size();
}

size_t TTable::GetRemovedTxCount() const
{
    return RemovedTransactions.Size();
}

TTableRuntimeStats TTable::RuntimeStats() const noexcept
{
    return TTableRuntimeStats{
        .OpenTxCount = OpenTxs.size(),
        .TxsWithDataCount = TxDataRefs.size() + GarbageTransactions.Size(),
        .CommittedTxCount = CommittedTransactions.Size(),
        .RemovedTxCount = RemovedTransactions.Size(),
        .RemovedCommittedTxs = RemovedCommittedTxs,
    };
}

TMemTable& TTable::MemTable()
{
    if (!Mutable) {
        Mutable = new TMemTable(Scheme, Epoch, Annexed);
    }
    if (RollbackState) {
        // MemTable() is only called when we want to apply updates
        // Make sure we don't taint erase cache with changes that may rollback
        RollbackState->DisableEraseCache = true;
    }
    if (RollbackState && Epoch == RollbackState->Epoch && RollbackState->MutableExisted) {
        if (!RollbackState->MutableUpdated) {
            RollbackState->MutableUpdated = true;
            Mutable->PrepareRollback();
        }
    }
    return *Mutable;
}

TAutoPtr<TTableIter> TTable::Iterate(TRawVals key_, TTagsRef tags, IPages* env, ESeek seek,
        TRowVersion snapshot,
        const ITransactionMapPtr& visible,
        const ITransactionObserverPtr& observer) const
{
    Y_ABORT_UNLESS(ColdParts.empty(), "Cannot iterate with cold parts");

    const TCelled key(key_, *Scheme->Keys, false);
    const ui64 limit = seek == ESeek::Exact ? 1 : Max<ui64>();

    TAutoPtr<TTableIter> dbIter(new TTableIter(Scheme.Get(), tags, limit, snapshot,
            TMergedTransactionMap::Create(visible, CommittedTransactions),
            observer));

    if (Mutable) {
        dbIter->Push(TMemIter::Make(*Mutable, Mutable->Snapshot(), key, seek, Scheme->Keys, &dbIter->Remap, env, EDirection::Forward));
    }

    if (MutableBackup) {
        dbIter->Push(TMemIter::Make(*MutableBackup, MutableBackup->Immediate(), key, seek, Scheme->Keys, &dbIter->Remap, env, EDirection::Forward));
    }

    for (auto& fti : Frozen) {
        const TMemTable* memTable = fti.Get();

        dbIter->Push(TMemIter::Make(*memTable, memTable->Immediate(), key, seek, Scheme->Keys, &dbIter->Remap, env, EDirection::Forward));
    }

    if (Flatten) {
        for (const auto& run : GetLevels()) {
            auto iter = MakeHolder<TRunIter>(run, dbIter->Remap.Tags, Scheme->Keys, env);

            if (iter->Seek(key, seek) != EReady::Gone)
                dbIter->Push(std::move(iter));
        }
    }

    if (EraseCacheEnabled && (!RollbackState || !RollbackState->DisableEraseCache)) {
        if (HasAppData() && AppData()->FeatureFlags.GetDisableLocalDBEraseCache()) {
            // Note: it's not very clean adding dependency to appdata here, but
            // we want to allow disabling erase cache at runtime without alters.
            ErasedKeysCache.Reset();
        } else if (!ErasedKeysCache) {
            ErasedKeysCache = new TKeyRangeCache(*Scheme->Keys, EraseCacheConfig, EraseCacheGCList);
        }
        dbIter->ErasedKeysCache = ErasedKeysCache;
        dbIter->DecidedTransactions = DecidedTransactions;
    }

    return dbIter;
}

TAutoPtr<TTableReverseIter> TTable::IterateReverse(TRawVals key_, TTagsRef tags, IPages* env, ESeek seek,
        TRowVersion snapshot,
        const ITransactionMapPtr& visible,
        const ITransactionObserverPtr& observer) const
{
    Y_ABORT_UNLESS(ColdParts.empty(), "Cannot iterate with cold parts");

    const TCelled key(key_, *Scheme->Keys, false);
    const ui64 limit = seek == ESeek::Exact ? 1 : Max<ui64>();

    TAutoPtr<TTableReverseIter> dbIter(new TTableReverseIter(Scheme.Get(), tags, limit, snapshot,
            TMergedTransactionMap::Create(visible, CommittedTransactions),
            observer));

    if (Mutable) {
        dbIter->Push(TMemIter::Make(*Mutable, Mutable->Snapshot(), key, seek, Scheme->Keys, &dbIter->Remap, env, EDirection::Reverse));
    }

    if (MutableBackup) {
        dbIter->Push(TMemIter::Make(*MutableBackup, MutableBackup->Immediate(), key, seek, Scheme->Keys, &dbIter->Remap, env, EDirection::Reverse));
    }

    for (auto& fti : Frozen) {
        const TMemTable* memTable = fti.Get();

        dbIter->Push(TMemIter::Make(*memTable, memTable->Immediate(), key, seek, Scheme->Keys, &dbIter->Remap, env, EDirection::Reverse));
    }

    if (Flatten) {
        for (const auto& run : GetLevels()) {
            auto iter = MakeHolder<TRunIter>(run, dbIter->Remap.Tags, Scheme->Keys, env);

            if (iter->SeekReverse(key, seek) != EReady::Gone)
                dbIter->Push(std::move(iter));
        }
    }

    if (EraseCacheEnabled && (!RollbackState || !RollbackState->DisableEraseCache)) {
        if (HasAppData() && AppData()->FeatureFlags.GetDisableLocalDBEraseCache()) {
            // Note: it's not very clean adding dependency to appdata here, but
            // we want to allow disabling erase cache at runtime without alters.
            ErasedKeysCache.Reset();
        } else if (!ErasedKeysCache) {
            ErasedKeysCache = new TKeyRangeCache(*Scheme->Keys, EraseCacheConfig, EraseCacheGCList);
        }
        dbIter->ErasedKeysCache = ErasedKeysCache;
        dbIter->DecidedTransactions = DecidedTransactions;
    }

    return dbIter;
}

EReady TTable::Select(TRawVals key_, TTagsRef tags, IPages* env, TRowState& row,
                      ui64 flg, TRowVersion snapshot,
                      TDeque<TPartIter>& tempIterators,
                      TSelectStats& stats,
                      const ITransactionMapPtr& visible,
                      const ITransactionObserverPtr& observer) const
{
    Y_ABORT_UNLESS(ColdParts.empty(), "Cannot select with cold parts");
    Y_ABORT_UNLESS(key_.size() == Scheme->Keys->Types.size());

    const TCelled key(key_, *Scheme->Keys, false);

    const TRemap remap(*Scheme, tags);

    row.Reset(remap.CellDefaults());

    for (auto &pin: remap.KeyPins())
        row.Set(pin.Pos, { ECellOp::Set, ELargeObj::Inline }, key[pin.Key]);

    const NBloom::TPrefix prefix(key);

    TEpoch lastEpoch = TEpoch::Max();

    bool snapshotFound = (snapshot == TRowVersion::Max());
    auto committed = TMergedTransactionMap::Create(visible, CommittedTransactions);

    const auto prevInvisibleRowSkips = stats.InvisibleRowSkips;

    // Mutable has the newest data
    if (Mutable) {
        lastEpoch = Mutable->Epoch;
        if (auto it = TMemIter::Make(*Mutable, Mutable->Immediate(), key, ESeek::Exact, Scheme->Keys, &remap, env, EDirection::Forward)) {
            if (it->IsValid() && (snapshotFound || it->SkipToRowVersion(snapshot, stats, committed, observer, DecidedTransactions))) {
                // N.B. stop looking for snapshot after the first hit
                snapshotFound = true;
                it->Apply(row, committed, observer);
            }
        }
    }

    // Mutable data that is transitioning to frozen
    if (MutableBackup && !row.IsFinalized()) {
        lastEpoch = MutableBackup->Epoch;
        if (auto it = TMemIter::Make(*MutableBackup, MutableBackup->Immediate(), key, ESeek::Exact, Scheme->Keys, &remap, env, EDirection::Forward)) {
            if (it->IsValid() && (snapshotFound || it->SkipToRowVersion(snapshot, stats, committed, observer, DecidedTransactions))) {
                // N.B. stop looking for snapshot after the first hit
                snapshotFound = true;
                it->Apply(row, committed, observer);
            }
        }
    }

    // Frozen are sorted by epoch, apply in reverse order
    for (auto pos = Frozen.rbegin(); !row.IsFinalized() && pos != Frozen.rend(); ++pos) {
        const auto& memTable = *pos;
        Y_ABORT_UNLESS(lastEpoch > memTable->Epoch, "Ordering of epochs is incorrect");
        lastEpoch = memTable->Epoch;
        if (auto it = TMemIter::Make(*memTable, memTable->Immediate(), key, ESeek::Exact, Scheme->Keys, &remap, env, EDirection::Forward)) {
            if (it->IsValid() && (snapshotFound || it->SkipToRowVersion(snapshot, stats, committed, observer, DecidedTransactions))) {
                // N.B. stop looking for snapshot after the first hit
                snapshotFound = true;
                it->Apply(row, committed, observer);
            }
        }
    }

    bool ready = true;
    if (!row.IsFinalized() && Flatten) {
        // Levels are ordered from newest to oldest, apply in order
        for (const auto& run : GetLevels()) {
            auto pos = run.Find(key);
            if (pos != run.end()) {
                const auto* part = pos->Part.Get();
                if ((flg & EHint::NoByKey) ||
                    part->MightHaveKey(prefix.Get(part->Scheme->Groups[0].KeyTypes.size())))
                {
                    ++stats.Sieved;
                    TPartIter& it = tempIterators.emplace_back(part, tags, Scheme->Keys, env);
                    it.SetBounds(pos->Slice);
                    auto res = it.Seek(key, ESeek::Exact);
                    if (res == EReady::Data) {
                        Y_ABORT_UNLESS(lastEpoch > part->Epoch, "Ordering of epochs is incorrect");
                        lastEpoch = part->Epoch;
                        if (!snapshotFound) {
                            res = it.SkipToRowVersion(snapshot, stats, committed, observer, DecidedTransactions);
                            if (res == EReady::Data) {
                                // N.B. stop looking for snapshot after the first hit
                                snapshotFound = true;
                            }
                        }
                    }
                    if (ready = ready && bool(res)) {
                        if (res == EReady::Data) {
                            it.Apply(row, committed, observer);
                            if (row.IsFinalized()) {
                                break;
                            }
                        } else {
                            ++stats.NoKey;
                        }
                    }
                } else {
                    ++stats.Weeded;
                }
            }
        }
    }

    Y_DEBUG_ABORT_UNLESS(!snapshot.IsMax() || (stats.InvisibleRowSkips - prevInvisibleRowSkips) == 0);

    if (!ready || row.Need()) {
        return EReady::Page;
    } else if (row == ERowOp::Erase || row == ERowOp::Absent) {
        return EReady::Gone;
    } else {
        return EReady::Data;
    }
}

TSelectRowVersionResult TTable::SelectRowVersion(
        TRawVals key_, IPages* env, ui64 readFlags,
        const ITransactionMapPtr& visible,
        const ITransactionObserverPtr& observer) const
{
    const TCelled key(key_, *Scheme->Keys, true);

    return SelectRowVersion(key, env, readFlags, visible, observer);
}

TSelectRowVersionResult TTable::SelectRowVersion(
        TArrayRef<const TCell> key_, IPages* env, ui64 readFlags,
        const ITransactionMapPtr& visible,
        const ITransactionObserverPtr& observer) const
{
    const TCelled key(key_, *Scheme->Keys, true);

    return SelectRowVersion(key, env, readFlags, visible, observer);
}

TSelectRowVersionResult TTable::SelectRowVersion(
        const TCelled& key, IPages* env, ui64 readFlags,
        const ITransactionMapPtr& visible,
        const ITransactionObserverPtr& observer) const
{
    Y_ABORT_UNLESS(ColdParts.empty(), "Cannot select with cold parts");

    const TRemap remap(*Scheme, { });

    const NBloom::TPrefix prefix(key);

    TEpoch lastEpoch = TEpoch::Max();

    auto committed = TMergedTransactionMap::Create(visible, CommittedTransactions);

    // Mutable has the newest data
    if (Mutable) {
        lastEpoch = Mutable->Epoch;
        if (auto it = TMemIter::Make(*Mutable, Mutable->Immediate(), key, ESeek::Exact, Scheme->Keys, &remap, env, EDirection::Forward)) {
            if (it->IsValid()) {
                if (auto rowVersion = it->SkipToCommitted(committed, observer)) {
                    return *rowVersion;
                }
            }
        }
    }

    // Mutable data that is transitioning to frozen
    if (MutableBackup) {
        lastEpoch = MutableBackup->Epoch;
        if (auto it = TMemIter::Make(*MutableBackup, MutableBackup->Immediate(), key, ESeek::Exact, Scheme->Keys, &remap, env, EDirection::Forward)) {
            if (it->IsValid()) {
                if (auto rowVersion = it->SkipToCommitted(committed, observer)) {
                    return *rowVersion;
                }
            }
        }
    }

    // Frozen are sorted by epoch, apply in reverse order
    for (auto pos = Frozen.rbegin(); pos != Frozen.rend(); ++pos) {
        const auto& memTable = *pos;
        Y_ABORT_UNLESS(lastEpoch > memTable->Epoch, "Ordering of epochs is incorrect");
        lastEpoch = memTable->Epoch;
        if (auto it = TMemIter::Make(*memTable, memTable->Immediate(), key, ESeek::Exact, Scheme->Keys, &remap, env, EDirection::Forward)) {
            if (it->IsValid()) {
                if (auto rowVersion = it->SkipToCommitted(committed, observer)) {
                    return *rowVersion;
                }
            }
        }
    }

    // Levels are ordered from newest to oldest, apply in order
    bool ready = true;
    for (const auto& run : GetLevels()) {
        auto pos = run.Find(key);
        if (pos != run.end()) {
            const auto* part = pos->Part.Get();
            if ((readFlags & EHint::NoByKey) ||
                part->MightHaveKey(prefix.Get(part->Scheme->Groups[0].KeyTypes.size())))
            {
                TPartIter it(part, { }, Scheme->Keys, env);
                it.SetBounds(pos->Slice);
                auto res = it.Seek(key, ESeek::Exact);
                if (res == EReady::Data && ready) {
                    Y_ABORT_UNLESS(lastEpoch > part->Epoch, "Ordering of epochs is incorrect");
                    lastEpoch = part->Epoch;
                    if (auto rowVersion = it.SkipToCommitted(committed, observer)) {
                        return *rowVersion;
                    }
                }
                ready = ready && bool(res);
            }
        }
    }

    return ready ? EReady::Gone : EReady::Page;
}

void TTable::DebugDump(IOutputStream& str, IPages* env, const NScheme::TTypeRegistry& reg) const
{
    str << "Mutable: " << (Mutable ? Mutable->Epoch : TEpoch::Zero()) << Endl;
    str << "Frozen: [";
    for (const auto& it : Frozen) {
        str << it->Epoch;
    }
    str << "]" << Endl
        << "Parts: ["
        << Endl;
    for (const auto& fpIt: Flatten) {
        str << "    ";
        NFmt::Ln(*fpIt.second);
    }
    str << "]" << Endl;
    if (ColdParts) {
        str << "ColdParts: [" << Endl;
        for (const auto& it : ColdParts) {
            str << "    ";
            NFmt::Ln(*it.second);
        }
        str << "]" << Endl;
    }
    str << "Mutable dump: " << Endl;

    if (Mutable)
        Mutable->DebugDump(str, reg);
    if (MutableBackup)
        MutableBackup->DebugDump(str, reg);
    for (const auto& it : Frozen) {
        str << "Frozen " << it->Epoch << " dump: " << Endl;
        it->DebugDump(str, reg);
    }

    TDump dump(str, env, &reg);

    for (const auto &it: Flatten) dump.Part(*it.second);
}

TKeyRangeCache* TTable::GetErasedKeysCache() const
{
    return ErasedKeysCache.Get();
}

bool TTable::RemoveRowVersions(const TRowVersion& lower, const TRowVersion& upper)
{
    return RemovedRowVersions.Add(lower, upper);
}

TCompactionStats TTable::GetCompactionStats() const
{
    return {
        .PartCount = Flatten.size() + ColdParts.size(),
        .MemRowCount = GetMemRowCount(),
        .MemDataSize = GetMemSize(),
        .MemDataWaste = GetMemWaste(),
    };
}

void TTable::SetTableObserver(TIntrusivePtr<ITableObserver> ptr)
{
    TableObserver = std::move(ptr);
}

void TPartStats::Add(const TPartView& partView)
{
    PartsCount += 1;
    if (partView->IndexPages.HasBTree()) {
        BTreeIndexBytes += partView->IndexesRawSize;
    } else {
        FlatIndexBytes += partView->IndexesRawSize;
    }
    ByKeyBytes += partView->ByKey ? partView->ByKey->Raw.size() : 0;
    PlainBytes += partView->Stat.Bytes;
    CodedBytes += partView->Stat.Coded;
    RowsErase += partView->Stat.Drops;
    RowsTotal += partView->Stat.Rows;
    SmallBytes += partView->Small ? partView->Small->Stats().Size : 0;
    SmallItems += partView->Small ? partView->Small->Stats().Items : 0;
    LargeBytes += partView->Large ? partView->Large->Stats().Size : 0;
    LargeItems += partView->Blobs ? partView->Blobs->Total() : 0;

    OtherBytes += (partView->Small ? partView->Small->Raw.size() : 0);
    OtherBytes += (partView->Large ? partView->Large->Raw.size() : 0);
    OtherBytes += (partView->Blobs ? partView->Blobs->Raw.size() : 0);
}

bool TPartStats::Remove(const TPartView& partView)
{
    NUtil::SubSafe(PartsCount, ui64(1));
    if (partView->IndexPages.HasBTree()) {
        NUtil::SubSafe(BTreeIndexBytes, partView->IndexesRawSize);
    } else {
        NUtil::SubSafe(FlatIndexBytes, partView->IndexesRawSize);
    }
    NUtil::SubSafe(ByKeyBytes, partView->ByKey ? partView->ByKey->Raw.size() : 0);
    NUtil::SubSafe(PlainBytes, partView->Stat.Bytes);
    NUtil::SubSafe(CodedBytes, partView->Stat.Coded);
    NUtil::SubSafe(RowsErase, partView->Stat.Drops);
    NUtil::SubSafe(RowsTotal, partView->Stat.Rows);

    if (auto *small = partView->Small.Get()) {
        NUtil::SubSafe(SmallBytes, small->Stats().Size);
        NUtil::SubSafe(SmallItems, ui64(small->Stats().Items));
        NUtil::SubSafe(OtherBytes, ui64(small->Raw.size()));
    }

    if (auto *large = partView->Large.Get()) {
        NUtil::SubSafe(LargeBytes, large->Stats().Size);
        NUtil::SubSafe(OtherBytes, large->Raw.size());
    }

    if (auto *blobs = partView->Blobs.Get()) {
        NUtil::SubSafe(LargeItems, ui64(blobs->Total()));
        NUtil::SubSafe(OtherBytes, blobs->Raw.size());
    }

    return PartsCount > 0;
}

TPartStats& TPartStats::operator+=(const TPartStats& rhs)
{
    PartsCount += rhs.PartsCount;
    FlatIndexBytes += rhs.FlatIndexBytes;
    BTreeIndexBytes += rhs.BTreeIndexBytes;
    OtherBytes += rhs.OtherBytes;
    ByKeyBytes += rhs.ByKeyBytes;
    PlainBytes += rhs.PlainBytes;
    CodedBytes += rhs.CodedBytes;
    SmallBytes += rhs.SmallBytes;
    SmallItems += rhs.SmallItems;
    LargeBytes += rhs.LargeBytes;
    LargeItems += rhs.LargeItems;
    RowsErase += rhs.RowsErase;
    RowsTotal += rhs.RowsTotal;
    return *this;
}

TPartStats& TPartStats::operator-=(const TPartStats& rhs)
{
    NUtil::SubSafe(PartsCount, rhs.PartsCount);
    NUtil::SubSafe(FlatIndexBytes, rhs.FlatIndexBytes);
    NUtil::SubSafe(BTreeIndexBytes, rhs.BTreeIndexBytes);
    NUtil::SubSafe(OtherBytes, rhs.OtherBytes);
    NUtil::SubSafe(ByKeyBytes, rhs.ByKeyBytes);
    NUtil::SubSafe(PlainBytes, rhs.PlainBytes);
    NUtil::SubSafe(CodedBytes, rhs.CodedBytes);
    NUtil::SubSafe(SmallBytes, rhs.SmallBytes);
    NUtil::SubSafe(SmallItems, rhs.SmallItems);
    NUtil::SubSafe(LargeBytes, rhs.LargeBytes);
    NUtil::SubSafe(LargeItems, rhs.LargeItems);
    NUtil::SubSafe(RowsErase, rhs.RowsErase);
    NUtil::SubSafe(RowsTotal, rhs.RowsTotal);
    return *this;
}

}}
