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

            void operator()(const TRollbackRemoveTxRef& op) const {
                auto it = Self->TxRefs.find(op.TxId);
                Y_ABORT_UNLESS(it != Self->TxRefs.end());
                if (0 == --it->second) {
                    Self->TxRefs.erase(it);
                }
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

            void operator()(const TRollbackAddOpenTx& op) const {
                Self->OpenTxs.insert(op.TxId);
            }

            void operator()(const TRollbackRemoveOpenTx& op) const {
                Self->OpenTxs.erase(op.TxId);
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

TAutoPtr<TSubset> TTable::Subset(TArrayRef<const TLogoBlobID> bundle, TEpoch head)
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

    return subset;
}

TAutoPtr<TSubset> TTable::Subset(TEpoch head) const noexcept
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

    return subset;
}

bool TTable::HasBorrowed(ui64 selfTabletId) const noexcept
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

TAutoPtr<TSubset> TTable::ScanSnapshot(TRowVersion snapshot) noexcept
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

TAutoPtr<TSubset> TTable::Unwrap() noexcept
{
    Snapshot();

    auto subset = Subset(TEpoch::Max());

    Replace({ }, *subset);
    ReplaceTxStatus({ }, *subset);

    Y_ABORT_UNLESS(!(Flatten || Frozen || Mutable || TxStatus));

    return subset;
}

TBundleSlicesMap TTable::LookupSlices(TArrayRef<const TLogoBlobID> bundles) const noexcept
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

void TTable::ReplaceSlices(TBundleSlicesMap slices) noexcept
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

void TTable::Replace(TArrayRef<const TPartView> partViews, const TSubset &subset) noexcept
{
    Y_ABORT_UNLESS(!RollbackState, "Cannot perform this in a transaction");

    for (const auto &partView : partViews) {
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
    THashSet<ui64> checkNewTransactions;

    for (auto &memTable : subset.Frozen) {
        removingOld = true;
        const auto found = Frozen.erase(memTable.MemTable);

        Y_ABORT_UNLESS(found == 1, "Got an unknown TMemTable table in TSubset");

        NUtil::SubSafe(Stat_.FrozenWaste, memTable->GetWastedMem());
        NUtil::SubSafe(Stat_.FrozenSize, memTable->GetUsedMem());
        NUtil::SubSafe(Stat_.FrozenOps,  memTable->GetOpsCount());
        NUtil::SubSafe(Stat_.FrozenRows, memTable->GetRowCount());

        for (const auto &pr : memTable.MemTable->GetTxIdStats()) {
            const ui64 txId = pr.first;
            auto& count = TxRefs.at(txId);
            Y_ABORT_UNLESS(count > 0);
            if (0 == --count) {
                checkNewTransactions.insert(txId);
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
                auto& count = TxRefs.at(txId);
                Y_ABORT_UNLESS(count > 0);
                if (0 == --count) {
                    checkNewTransactions.insert(txId);
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

    for (const auto &partView : partViews) {
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

    for (ui64 txId : checkNewTransactions) {
        auto it = TxRefs.find(txId);
        Y_ABORT_UNLESS(it != TxRefs.end());
        if (it->second == 0) {
            // Transaction no longer needs to be tracked
            if (!ColdParts) {
                CommittedTransactions.Remove(txId);
                RemovedTransactions.Remove(txId);
                DecidedTransactions.Remove(txId);
            } else {
                CheckTransactions.insert(txId);
            }
            TxRefs.erase(it);
            OpenTxs.erase(txId);
        }
    }

    ProcessCheckTransactions();

    if (!removingOld && addingNew) {
        // Note: we invalidate erase cache when nothing old is removed,
        // because followers always call Replace, even when leader called
        // Merge. When something is removed we can assume it's a compaction
        // and compactions don't add new rows to the table, keeping erase
        // cache valid.
        ErasedKeysCache.Reset();
    }
}

void TTable::ReplaceTxStatus(TArrayRef<const TIntrusiveConstPtr<TTxStatusPart>> newTxStatus, const TSubset &subset) noexcept
{
    Y_ABORT_UNLESS(!RollbackState, "Cannot perform this in a transaction");

    for (auto &part : subset.TxStatus) {
        Y_ABORT_UNLESS(part, "Unexpected empty TTxStatusPart in TSubset");

        auto it = TxStatus.find(part->Label);
        Y_ABORT_UNLESS(it != TxStatus.end());
        TxStatus.erase(it);
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
    }
}

void TTable::Merge(TPartView partView) noexcept
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

void TTable::Merge(TIntrusiveConstPtr<TColdPart> part) noexcept
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

void TTable::Merge(TIntrusiveConstPtr<TTxStatusPart> txStatus) noexcept
{
    Y_ABORT_UNLESS(!RollbackState, "Cannot perform this in a transaction");

    Y_ABORT_UNLESS(txStatus, "Unexpected empty TTxStatusPart");

    for (auto& item : txStatus->TxStatusPage->GetCommittedItems()) {
        const ui64 txId = item.GetTxId();
        const auto rowVersion = item.GetRowVersion();
        if (const auto* prev = CommittedTransactions.Find(txId); Y_LIKELY(!prev) || *prev > rowVersion) {
            CommittedTransactions.Add(txId, rowVersion);
            if (!prev) {
                RemovedTransactions.Remove(txId);
            }
        }
        if (!TxRefs.contains(txId)) {
            CheckTransactions.insert(txId);
        }
        DecidedTransactions.Add(txId);
        OpenTxs.erase(txId);
    }
    for (auto& item : txStatus->TxStatusPage->GetRemovedItems()) {
        const ui64 txId = item.GetTxId();
        if (const auto* prev = CommittedTransactions.Find(txId); Y_LIKELY(!prev)) {
            RemovedTransactions.Add(txId);
        }
        if (!TxRefs.contains(txId)) {
            CheckTransactions.insert(txId);
        }
        DecidedTransactions.Add(txId);
        OpenTxs.erase(txId);
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

void TTable::ProcessCheckTransactions() noexcept
{
    if (!ColdParts) {
        for (ui64 txId : CheckTransactions) {
            auto it = TxRefs.find(txId);
            if (it == TxRefs.end()) {
                CommittedTransactions.Remove(txId);
                RemovedTransactions.Remove(txId);
                DecidedTransactions.Remove(txId);
            }
        }
        CheckTransactions.clear();
    }
}

const TLevels& TTable::GetLevels() const noexcept
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

ui64 TTable::GetSearchHeight() const noexcept
{
    if (!ColdParts.empty())
        return 0;

    ui64 height = GetLevels().size() + Frozen.size();
    if (Mutable)
        ++height;

    return height;
}

TVector<TIntrusiveConstPtr<TMemTable>> TTable::GetMemTables() const noexcept
{
    Y_ABORT_UNLESS(!RollbackState, "Cannot perform this in a transaction");

    TVector<TIntrusiveConstPtr<TMemTable>> vec(Frozen.begin(), Frozen.end());

    if (Mutable)
        vec.emplace_back(Mutable);

    return vec;
}

TEpoch TTable::Snapshot() noexcept
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
                const auto newCount = ++TxRefs[txId];
                if (newCount == 1 && !CommittedTransactions.Find(txId) && !RemovedTransactions.Contains(txId)) {
                    OpenTxs.insert(txId);
                }
            }
        }

        using TVal = decltype(Flatten)::value_type;

        if (FlattenEpoch <= partView->Epoch) {
            FlattenEpoch = partView->Epoch;
            if (Levels) {
                // Slices from this part may be added on top
                Levels->Add(partView.Part, partView.Slices);
            }
        } else {
            Levels.Reset();
        }

        bool done = Flatten.insert(TVal(partView->Label, std::move(partView))).second;
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

void TTable::AddTxRef(ui64 txId)
{
    const auto newCount = ++TxRefs[txId];
    const bool addOpenTx = newCount == 1 && !CommittedTransactions.Find(txId) && !RemovedTransactions.Contains(txId);
    if (addOpenTx) {
        auto res = OpenTxs.insert(txId);
        Y_ABORT_UNLESS(res.second);
        Y_DEBUG_ABORT_UNLESS(!DecidedTransactions.Contains(txId),
            "Decided transaction %" PRIu64 " is both open and decided", txId);
    }
    if (RollbackState) {
        RollbackOps.emplace_back(TRollbackRemoveTxRef{ txId });
        if (addOpenTx) {
            RollbackOps.emplace_back(TRollbackRemoveOpenTx{ txId });
        }
    }
}

void TTable::UpdateTx(ERowOp rop, TRawVals key, TOpsRef ops, TArrayRef<const TMemGlob> apart, ui64 txId)
{
    auto& memTable = MemTable();
    bool hadTxRef = memTable.GetTxIdStats().contains(txId);

    if (ErasedKeysCache && rop != ERowOp::Erase) {
        const TCelled cells(key, *Scheme->Keys, true);
        auto res = ErasedKeysCache->FindKey(cells);
        if (res.second) {
            ErasedKeysCache->InvalidateKey(res.first, cells);
        }
    }

    // Use a special row version that marks this update as uncommitted
    TRowVersion rowVersion(Max<ui64>(), txId);
    MemTable().Update(rop, key, ops, apart, rowVersion, CommittedTransactions);

    if (!hadTxRef) {
        Y_DEBUG_ABORT_UNLESS(memTable.GetTxIdStats().contains(txId));
        AddTxRef(txId);
    } else {
        Y_DEBUG_ABORT_UNLESS(TxRefs[txId] > 0);
    }

    if (TableObserver) {
        TableObserver->OnUpdateTx(rop, key, ops, txId);
    }
}

void TTable::CommitTx(ui64 txId, TRowVersion rowVersion)
{
    // TODO: track suspicious transactions (not open at commit time)
    MemTable().CommitTx(txId, rowVersion);

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
            RemovedTransactions.Remove(txId);
        }
        if (auto it = OpenTxs.find(txId); it != OpenTxs.end()) {
            if (RollbackState) {
                RollbackOps.emplace_back(TRollbackAddOpenTx{ txId });
                CommitOps.emplace_back(TCommitAddDecidedTx{ txId });
            } else {
                DecidedTransactions.Add(txId);
            }
            OpenTxs.erase(it);
        }
    }

    // Note: erase cache accounts for changes that may commit, no need to invalidate
}

void TTable::RemoveTx(ui64 txId)
{
    // TODO: track suspicious transactions (not open at remove time)
    MemTable().RemoveTx(txId);

    // Note: it is possible to have both CommitTx and RemoveTx for the same TxId
    // due to complicated split/merge shard interactions. The commit actually
    // wins over removes in all cases.
    if (const auto* prev = CommittedTransactions.Find(txId); Y_LIKELY(!prev)) {
        if (RollbackState && !RemovedTransactions.Contains(txId)) {
            RollbackOps.emplace_back(TRollbackRemoveRemovedTx{ txId });
        }
        RemovedTransactions.Add(txId);
        if (auto it = OpenTxs.find(txId); it != OpenTxs.end()) {
            if (RollbackState) {
                RollbackOps.emplace_back(TRollbackAddOpenTx{ txId });
                CommitOps.emplace_back(TCommitAddDecidedTx{ txId });
            } else {
                DecidedTransactions.Add(txId);
            }
            OpenTxs.erase(it);
        }
    }
}

bool TTable::HasOpenTx(ui64 txId) const
{
    return OpenTxs.contains(txId);
}

bool TTable::HasTxData(ui64 txId) const
{
    return TxRefs.contains(txId);
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
        const ITransactionObserverPtr& observer) const noexcept
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
        if (!ErasedKeysCache) {
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
        const ITransactionObserverPtr& observer) const noexcept
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
        if (!ErasedKeysCache) {
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
                      const ITransactionObserverPtr& observer) const noexcept
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
        const ITransactionObserverPtr& observer) const noexcept
{
    const TCelled key(key_, *Scheme->Keys, true);

    return SelectRowVersion(key, env, readFlags, visible, observer);
}

TSelectRowVersionResult TTable::SelectRowVersion(
        TArrayRef<const TCell> key_, IPages* env, ui64 readFlags,
        const ITransactionMapPtr& visible,
        const ITransactionObserverPtr& observer) const noexcept
{
    const TCelled key(key_, *Scheme->Keys, true);

    return SelectRowVersion(key, env, readFlags, visible, observer);
}

TSelectRowVersionResult TTable::SelectRowVersion(
        const TCelled& key, IPages* env, ui64 readFlags,
        const ITransactionMapPtr& visible,
        const ITransactionObserverPtr& observer) const noexcept
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
    TCompactionStats stats;
    stats.MemRowCount = GetMemRowCount();
    stats.MemDataSize = GetMemSize();
    stats.MemDataWaste = GetMemWaste();
    stats.PartCount = Flatten.size() + ColdParts.size();

    return stats;
}

void TTable::SetTableObserver(TIntrusivePtr<ITableObserver> ptr) noexcept
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
