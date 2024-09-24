#include "defs.h"
#include "flat_abi_evol.h"
#include "flat_database.h"
#include "flat_redo_writer.h"
#include "flat_redo_player.h"
#include "flat_dbase_naked.h"
#include "flat_dbase_apply.h"
#include "flat_dbase_annex.h"
#include "flat_dbase_sz_env.h"
#include "flat_part_shrink.h"
#include "flat_util_misc.h"
#include "flat_sausage_grind.h"
#include <ydb/core/util/pb.h>
#include <ydb/core/scheme_types/scheme_type_registry.h>
#include <util/generic/cast.h>
#include <util/stream/output.h>

namespace NKikimr {
namespace NTable {

bool TDatabase::TChangeCounter::operator<(const TChangeCounter& rhs) const {
    if (Serial && rhs.Serial) {
        // When both counters have serial they can be compared directly
        return Serial < rhs.Serial;
    }

    if (Epoch == rhs.Epoch) {
        // When this counter is (0, epoch) but rhs is (non-zero, epoch), it
        // indicates rhs may have more changes. When serial is zero it means
        // the current memtable is empty, but rhs epoch is the same, so it
        // cannot have fewer changes.
        return Serial < rhs.Serial;
    }

    // The one with the smaller epoch must have fewer changes. In the worst
    // case that change may have been a flush (incrementing epoch and serial)
    // and then compact (possibly resetting serial to zero).
    return Epoch < rhs.Epoch;
}

TDatabase::TDatabase(TDatabaseImpl *databaseImpl) noexcept
    : DatabaseImpl(databaseImpl ? databaseImpl : new TDatabaseImpl(0, new TScheme, nullptr))
    , NoMoreReadsFlag(true)
{

}

TDatabase::~TDatabase() { }

const TScheme& TDatabase::GetScheme() const noexcept
{
    return *DatabaseImpl->Scheme;
}

TIntrusiveConstPtr<TRowScheme> TDatabase::GetRowScheme(ui32 table) const noexcept
{
    return Require(table)->GetScheme();
}

TAutoPtr<TTableIter> TDatabase::Iterate(ui32 table, TRawVals key, TTagsRef tags, ELookup mode) const noexcept
{
    Y_ABORT_UNLESS(!NoMoreReadsFlag, "Trying to read after reads prohibited, table %u", table);

    const auto seekBy = [](TRawVals key, ELookup mode) {
        if (!key && mode != ELookup::ExactMatch) {
            /* Compatability mode with outer iterator interface that yields
             * begin() for non-exact lookups with empty key. Inner iterator
             * yields begin() for ({}, Lower) and end() for ({}, Upper).
             */

            return ESeek::Lower;

        } else {
            switch (mode) {
            case ELookup::ExactMatch:
                return ESeek::Exact;
            case ELookup::GreaterThan:
                return ESeek::Upper;
            case ELookup::GreaterOrEqualThan:
                return ESeek::Lower;
            }
        }

        Y_ABORT("Don't know how to convert ELookup to ESeek mode");
    };

    return Require(table)->Iterate(key, tags, Env, seekBy(key, mode), TRowVersion::Max());
}

TAutoPtr<TTableIter> TDatabase::IterateExact(ui32 table, TRawVals key, TTagsRef tags,
        TRowVersion snapshot,
        const ITransactionMapPtr& visible,
        const ITransactionObserverPtr& observer) const noexcept
{
    Y_ABORT_UNLESS(!NoMoreReadsFlag, "Trying to read after reads prohibited, table %u", table);

    auto iter = Require(table)->Iterate(key, tags, Env, ESeek::Exact, snapshot, visible, observer);

    // N.B. ESeek::Exact produces iterators with limit=1

    return iter;
}

namespace {
    // There are many places that use a non-inclusive -inf/+inf
    // We special case empty keys anyway, so the requirement is temporarily relaxed
    static constexpr bool RelaxEmptyKeys = true;

    const char* IsAmbiguousRangeReason(const TKeyRange& range, ui32 keyColumnsCount) {
        if (!range.MinKey) {
            if (Y_UNLIKELY(!range.MinInclusive) && !RelaxEmptyKeys) {
                return "Ambiguous table range: empty MinKey must be inclusive";
            }
        } else if (range.MinKey.size() < keyColumnsCount) {
            if (Y_UNLIKELY(range.MinInclusive)) {
                return "Ambiguous table range: incomplete MinKey must be non-inclusive (any/+inf is ambiguous otherwise)";
            }
        } else if (Y_UNLIKELY(range.MinKey.size() > keyColumnsCount)) {
            return "Ambiguous table range: MinKey is too large";
        }

        if (!range.MaxKey) {
            if (Y_UNLIKELY(!range.MaxInclusive) && !RelaxEmptyKeys) {
                return "Ambiguous table range: empty MaxKey must be inclusive";
            }
        } else if (range.MaxKey.size() < keyColumnsCount) {
            if (Y_UNLIKELY(!range.MaxInclusive)) {
                return "Ambiguous table range: incomplete MaxKey must be inclusive (any/+inf is ambiguous otherwise)";
            }
        } else if (Y_UNLIKELY(range.MaxKey.size() > keyColumnsCount)) {
            return "Ambiguous table range: MaxKey is too large";
        }

        return nullptr;
    }

    bool IsAmbiguousRange(const TKeyRange& range, ui32 keyColumnsCount) {
        return IsAmbiguousRangeReason(range, keyColumnsCount) != nullptr;
    }
}

TAutoPtr<TTableIter> TDatabase::IterateRange(ui32 table, const TKeyRange& range, TTagsRef tags,
        TRowVersion snapshot,
        const ITransactionMapPtr& visible,
        const ITransactionObserverPtr& observer) const noexcept
{
    Y_ABORT_UNLESS(!NoMoreReadsFlag, "Trying to read after reads prohibited, table %u", table);

    Y_DEBUG_ABORT_UNLESS(!IsAmbiguousRange(range, Require(table)->GetScheme()->Keys->Size()),
        "%s", IsAmbiguousRangeReason(range, Require(table)->GetScheme()->Keys->Size()));

    ESeek seek = !range.MinKey || range.MinInclusive ? ESeek::Lower : ESeek::Upper;

    auto iter = Require(table)->Iterate(range.MinKey, tags, Env, seek, snapshot, visible, observer);

    if (range.MaxKey) {
        TCelled maxKey(range.MaxKey, *iter->Scheme->Keys, false);

        if (range.MaxInclusive) {
            iter->StopAfter(maxKey);
        } else {
            iter->StopBefore(maxKey);
        }
    }

    return iter;
}

TAutoPtr<TTableReverseIter> TDatabase::IterateRangeReverse(ui32 table, const TKeyRange& range, TTagsRef tags,
        TRowVersion snapshot,
        const ITransactionMapPtr& visible,
        const ITransactionObserverPtr& observer) const noexcept
{
    Y_ABORT_UNLESS(!NoMoreReadsFlag, "Trying to read after reads prohibited, table %u", table);

    Y_DEBUG_ABORT_UNLESS(!IsAmbiguousRange(range, Require(table)->GetScheme()->Keys->Size()),
        "%s", IsAmbiguousRangeReason(range, Require(table)->GetScheme()->Keys->Size()));

    ESeek seek = !range.MaxKey || range.MaxInclusive ? ESeek::Lower : ESeek::Upper;

    auto iter = Require(table)->IterateReverse(range.MaxKey, tags, Env, seek, snapshot, visible, observer);

    if (range.MinKey) {
        TCelled minKey(range.MinKey, *iter->Scheme->Keys, false);

        if (range.MinInclusive) {
            iter->StopAfter(minKey);
        } else {
            iter->StopBefore(minKey);
        }
    }

    return iter;
}

template<>
TAutoPtr<TTableIter> TDatabase::IterateRangeGeneric<TTableIter>(ui32 table, const TKeyRange& range, TTagsRef tags,
        TRowVersion snapshot,
        const ITransactionMapPtr& visible,
        const ITransactionObserverPtr& observer) const noexcept
{
    return IterateRange(table, range, tags, snapshot, visible, observer);
}

template<>
TAutoPtr<TTableReverseIter> TDatabase::IterateRangeGeneric<TTableReverseIter>(ui32 table, const TKeyRange& range, TTagsRef tags,
        TRowVersion snapshot,
        const ITransactionMapPtr& visible,
        const ITransactionObserverPtr& observer) const noexcept
{
    return IterateRangeReverse(table, range, tags, snapshot, visible, observer);
}

EReady TDatabase::Select(ui32 table, TRawVals key, TTagsRef tags, TRowState &row, ui64 flg,
        TRowVersion snapshot,
        const ITransactionMapPtr& visible,
        const ITransactionObserverPtr& observer) const noexcept
{
    TSelectStats stats;
    return Select(table, key, tags, row, stats, flg, snapshot, visible, observer);
}

EReady TDatabase::Select(ui32 table, TRawVals key, TTagsRef tags, TRowState &row, TSelectStats& stats, ui64 flg,
        TRowVersion snapshot,
        const ITransactionMapPtr& visible,
        const ITransactionObserverPtr& observer) const noexcept
{
    TempIterators.clear();
    Y_ABORT_UNLESS(!NoMoreReadsFlag, "Trying to read after reads prohibited, table %u", table);

    auto prevSieved = stats.Sieved;
    auto prevWeeded = stats.Weeded;
    auto prevNoKey = stats.NoKey;
    auto prevInvisible = stats.InvisibleRowSkips;

    auto ready = Require(table)->Select(key, tags, Env, row, flg, snapshot, TempIterators, stats, visible, observer);
    Change->Stats.SelectSieved += stats.Sieved - prevSieved;
    Change->Stats.SelectWeeded += stats.Weeded - prevWeeded;
    Change->Stats.SelectNoKey += stats.NoKey - prevNoKey;
    Change->Stats.SelectInvisible += stats.InvisibleRowSkips - prevInvisible;

    return ready;
}

TSelectRowVersionResult TDatabase::SelectRowVersion(
        ui32 table, TRawVals key, ui64 readFlags,
        const ITransactionMapPtr& visible,
        const ITransactionObserverPtr& observer) const noexcept
{
    return Require(table)->SelectRowVersion(key, Env, readFlags, visible, observer);
}

TSelectRowVersionResult TDatabase::SelectRowVersion(
        ui32 table, TArrayRef<const TCell> key, ui64 readFlags,
        const ITransactionMapPtr& visible,
        const ITransactionObserverPtr& observer) const noexcept
{
    return Require(table)->SelectRowVersion(key, Env, readFlags, visible, observer);
}

TSizeEnv TDatabase::CreateSizeEnv()
{
    return TSizeEnv(Env);
}


void TDatabase::CalculateReadSize(TSizeEnv& env, ui32 table, TRawVals minKey, TRawVals maxKey,
                                  TTagsRef tags, ui64 flg, ui64 items, ui64 bytes,
                                  EDirection direction, TRowVersion snapshot)
{
    Y_ABORT_UNLESS(!NoMoreReadsFlag, "Trying to do precharge after reads prohibited, table %u", table);
    TSelectStats stats;
    Require(table)->Precharge(minKey, maxKey, tags, &env, flg, items, bytes, direction, snapshot, stats);
}

bool TDatabase::Precharge(ui32 table, TRawVals minKey, TRawVals maxKey,
                    TTagsRef tags, ui64 flg, ui64 items, ui64 bytes,
                    EDirection direction, TRowVersion snapshot)
{
    Y_ABORT_UNLESS(!NoMoreReadsFlag, "Trying to do precharge after reads prohibited, table %u", table);
    TSelectStats stats;
    auto ready = Require(table)->Precharge(minKey, maxKey, tags, Env, flg, items, bytes, direction, snapshot, stats);
    Change->Stats.ChargeSieved += stats.Sieved;
    Change->Stats.ChargeWeeded += stats.Weeded;
    return ready == EReady::Data;
}

void TDatabase::Update(ui32 table, ERowOp rop, TRawVals key, TArrayRef<const TUpdateOp> ops, TRowVersion rowVersion)
{
    Y_DEBUG_ABORT_UNLESS(rowVersion != TRowVersion::Max(), "Updates cannot have v{max} as row version");

    for (size_t index = 0; index < key.size(); ++index) {
        if (auto error = NScheme::HasUnexpectedValueSize(key[index])) {
            Y_ABORT("Key index %" PRISZT " validation failure: %s", index, error.c_str());
        }
    }

    NRedo::IAnnex* annex = Annex.Get();
    NRedo::IAnnex::TLimit limit = annex->Limit(table);
    limit.MinExternSize = Max(limit.MinExternSize, DatabaseImpl->AnnexByteLimit());

    if (ModifiedRefs.size() < ops.size()) {
        ModifiedRefs.resize(FastClp2(ops.size()));
    }
    ModifiedOps.assign(ops.begin(), ops.end());
    for (size_t index = 0; index < ModifiedOps.size(); ++index) {
        TUpdateOp& op = ModifiedOps[index];
        if (auto error = NScheme::HasUnexpectedValueSize(op.Value)) {
            Y_ABORT("Op index %" PRISZT " tag %" PRIu32 " validation failure: %s", index, op.Tag, error.c_str());
        }

        if (op.Value.IsEmpty()) {
            // Null values lose type id in redo log
            op.Value = {};
            // Null values must change ECellOp::Set to ECellOp::Null in redo log
            op.Op = op.NormalizedCellOp();
        }

        Y_ABORT_UNLESS(op.Op == ELargeObj::Inline, "User provided an invalid ECellOp");

        TArrayRef<const char> raw = op.AsRef();
        if (limit.IsExtern(raw.size())) {
            if (auto got = annex->Place(table, op.Tag, raw)) {
                ModifiedRefs[index] = got.Ref;
                const auto payload = NUtil::NBin::ToRef(ModifiedRefs[index]);
                op.Value = TRawTypeValue(payload, NScheme::TTypeInfo(op.Value.Type()));
                op.Op = ELargeObj::Extern;
            }
        }
    }

    Redo->EvUpdate(table, rop, key, ModifiedOps, rowVersion);
    RequireForUpdate(table)->Update(rop, key, ModifiedOps, Annex->Current(), rowVersion);
}

void TDatabase::UpdateTx(ui32 table, ERowOp rop, TRawVals key, TArrayRef<const TUpdateOp> ops, ui64 txId)
{
    for (size_t index = 0; index < key.size(); ++index) {
        if (auto error = NScheme::HasUnexpectedValueSize(key[index])) {
            Y_ABORT("Key index %" PRISZT " validation failure: %s", index, error.c_str());
        }
    }

    ModifiedOps.assign(ops.begin(), ops.end());
    for (size_t index = 0; index < ModifiedOps.size(); ++index) {
        TUpdateOp& op = ModifiedOps[index];
        if (auto error = NScheme::HasUnexpectedValueSize(op.Value)) {
            Y_ABORT("Op index %" PRISZT " tag %" PRIu32 " validation failure: %s", index, op.Tag, error.c_str());
        }

        if (op.Value.IsEmpty()) {
            // Null values lose type id in redo log
            op.Value = {};
            // Null values must change ECellOp::Set to ECellOp::Null in redo log
            op.Op = op.NormalizedCellOp();
        }

        Y_ABORT_UNLESS(op.Op == ELargeObj::Inline, "User provided an invalid ECellOp");

        // FIXME: we cannot handle blob references during scans, so we
        //        avoid creating large objects when they are in deltas
    }

    Redo->EvUpdateTx(table, rop, key, ModifiedOps, txId);
    RequireForUpdate(table)->UpdateTx(rop, key, ModifiedOps, Annex->Current(), txId);
}

void TDatabase::RemoveTx(ui32 table, ui64 txId)
{
    Redo->EvRemoveTx(table, txId);
    RequireForUpdate(table)->RemoveTx(txId);
}

void TDatabase::CommitTx(ui32 table, ui64 txId, TRowVersion rowVersion)
{
    Redo->EvCommitTx(table, txId, rowVersion);
    RequireForUpdate(table)->CommitTx(txId, rowVersion);
}

bool TDatabase::HasOpenTx(ui32 table, ui64 txId) const
{
    return Require(table)->HasOpenTx(txId);
}

bool TDatabase::HasTxData(ui32 table, ui64 txId) const
{
    return Require(table)->HasTxData(txId);
}

bool TDatabase::HasCommittedTx(ui32 table, ui64 txId) const
{
    return Require(table)->HasCommittedTx(txId);
}

bool TDatabase::HasRemovedTx(ui32 table, ui64 txId) const
{
    return Require(table)->HasRemovedTx(txId);
}

const absl::flat_hash_set<ui64>& TDatabase::GetOpenTxs(ui32 table) const
{
    return Require(table)->GetOpenTxs();
}

size_t TDatabase::GetOpenTxCount(ui32 table) const
{
    return Require(table)->GetOpenTxCount();
}

void TDatabase::RemoveRowVersions(ui32 table, const TRowVersion& lower, const TRowVersion& upper)
{
    if (Y_LIKELY(lower < upper)) {
        Change->RemovedRowVersions[table].push_back({ lower, upper });
    }
}

const TRowVersionRanges& TDatabase::GetRemovedRowVersions(ui32 table) const
{
    if (auto& wrap = DatabaseImpl->Get(table, false)) {
        return wrap->GetRemovedRowVersions();
    }

    static const TRowVersionRanges empty;
    return empty;
}

void TDatabase::NoMoreReadsForTx() {
    NoMoreReadsFlag = true;
}

void TDatabase::Begin(TTxStamp stamp, IPages& env)
{
    Y_ABORT_UNLESS(!Redo, "Transaction already in progress");
    Y_ABORT_UNLESS(!Env);
    Annex = new TAnnex(*DatabaseImpl->Scheme);
    Redo = new NRedo::TWriter;
    DatabaseImpl->BeginTransaction();
    Change = MakeHolder<TChange>(stamp, DatabaseImpl->Serial());
    Env = &env;
    NoMoreReadsFlag = false;
}

void TDatabase::RollbackChanges()
{
    Y_ABORT_UNLESS(Redo, "Transaction is not in progress");
    Y_ABORT_UNLESS(Env);

    TTxStamp stamp = Change->Stamp;
    IPages& env = *Env;

    Commit(stamp, false, nullptr);
    env.OnRollbackChanges();
    Begin(stamp, env);
}

TPartView TDatabase::GetPartView(ui32 tableId, const TLogoBlobID &bundle) const {
    return Require(tableId)->GetPartView(bundle);
}

TVector<TPartView> TDatabase::GetTableParts(ui32 tableId) const {
    return Require(tableId)->GetAllParts();
}

TVector<TIntrusiveConstPtr<TColdPart>> TDatabase::GetTableColdParts(ui32 tableId) const {
    return Require(tableId)->GetColdParts();
}

void TDatabase::EnumerateTableParts(ui32 tableId, const std::function<void(const TPartView&)>& callback) const {
    Require(tableId)->EnumerateParts(std::move(callback));
}

void TDatabase::EnumerateTableColdParts(ui32 tableId, const std::function<void(const TIntrusiveConstPtr<TColdPart>&)>& callback) const {
    Require(tableId)->EnumerateColdParts(callback);
}

void TDatabase::EnumerateTableTxStatusParts(ui32 tableId, const std::function<void(const TIntrusiveConstPtr<TTxStatusPart>&)>& callback) const {
    Require(tableId)->EnumerateTxStatusParts(callback);
}

void TDatabase::EnumerateTxStatusParts(const std::function<void(const TIntrusiveConstPtr<TTxStatusPart>&)>& callback) const {
    DatabaseImpl->EnumerateTxStatusParts(callback);
}

ui64 TDatabase::GetTableMemSize(ui32 tableId, TEpoch epoch) const {
    return Require(tableId)->GetMemSize(epoch);
}

ui64 TDatabase::GetTableMemRowCount(ui32 tableId) const {
    return Require(tableId)->GetMemRowCount();
}

ui64 TDatabase::GetTableMemOpsCount(ui32 tableId) const {
    return Require(tableId)->GetOpsCount();
}

ui64 TDatabase::GetTableIndexSize(ui32 tableId) const {
    const auto& partStats = Require(tableId)->Stat().Parts;
    return partStats.FlatIndexBytes + partStats.BTreeIndexBytes;
}

ui64 TDatabase::GetTableSearchHeight(ui32 tableId) const {
    return Require(tableId)->GetSearchHeight();
}

ui64 TDatabase::EstimateRowSize(ui32 tableId) const {
    return Require(tableId)->EstimateRowSize();
}

const TDbStats& TDatabase::Counters() const noexcept
{
    return DatabaseImpl->Stats;
}

void TDatabase::SetTableObserver(ui32 table, TIntrusivePtr<ITableObserver> ptr) noexcept
{
    Require(table)->SetTableObserver(std::move(ptr));
}

TDatabase::TChangeCounter TDatabase::Head(ui32 table) const noexcept
{
    if (table == Max<ui32>()) {
        return { DatabaseImpl->Serial(), TEpoch::Max() };
    } else {
        auto &wrap = DatabaseImpl->Get(table, true);

        // We return numbers as they have been at the start of transaction,
        // but possibly after schema changes or memtable flushes.
        auto serial = wrap.DataModified && !wrap.EpochSnapshot ? wrap.SerialBackup : wrap.Serial;
        auto head = wrap.EpochSnapshot ? *wrap.EpochSnapshot : wrap->Head();
        return { serial, head };
    }
}

TString TDatabase::SnapshotToLog(ui32 table, TTxStamp stamp)
{
    Y_ABORT_UNLESS(!Redo, "Cannot SnapshotToLog inside a transaction");

    auto scn = DatabaseImpl->Serial() + 1;
    auto epoch = DatabaseImpl->Get(table, true)->Snapshot();

    DatabaseImpl->Rewind(scn);

    NRedo::TWriter writer;
    writer.EvBegin(ui32(ECompatibility::Head), ui32(ECompatibility::Edge), scn, stamp);
    writer.EvFlush(table, stamp, epoch);
    return std::move(writer).Finish();
}

TEpoch TDatabase::TxSnapTable(ui32 table)
{
    Y_ABORT_UNLESS(Redo, "Cannot TxSnapTable outside a transaction");
    ++Change->Snapshots;
    return DatabaseImpl->FlushTable(table);
}

TAutoPtr<TSubset> TDatabase::Subset(ui32 table, TArrayRef<const TLogoBlobID> bundle, TEpoch before) const
{
    return Require(table)->Subset(bundle, before);
}

TAutoPtr<TSubset> TDatabase::Subset(ui32 table, TEpoch before, TRawVals from, TRawVals to) const
{
    auto subset = Require(table)->Subset(before);

    if (from || to) {
        Y_ABORT_UNLESS(!subset->Frozen, "Got subset with frozens, cannot shrink it");
        Y_ABORT_UNLESS(!subset->ColdParts, "Got subset with cold parts, cannot shrink it");

        TShrink shrink(Env, subset->Scheme->Keys);

        if (shrink.Put(subset->Flatten, from, to).Skipped) {
            return nullptr; /* Cannot shrink due to lack of some pages */
        } else {
            subset->Flatten = std::move(shrink.PartView);
        }
    }

    return subset;
}

TAutoPtr<TSubset> TDatabase::ScanSnapshot(ui32 table, TRowVersion snapshot)
{
    return Require(table)->ScanSnapshot(snapshot);
}

bool TDatabase::HasBorrowed(ui32 table, ui64 selfTabletId) const
{
    return Require(table)->HasBorrowed(selfTabletId);
}

TBundleSlicesMap TDatabase::LookupSlices(ui32 table, TArrayRef<const TLogoBlobID> bundles) const
{
    return Require(table)->LookupSlices(bundles);
}

void TDatabase::ReplaceSlices(ui32 table, TBundleSlicesMap slices)
{
    return DatabaseImpl->ReplaceSlices(table, std::move(slices));
}

void TDatabase::Replace(ui32 table, TArrayRef<const TPartView> partViews, const TSubset &subset)
{
    return DatabaseImpl->Replace(table, partViews, subset);
}

void TDatabase::ReplaceTxStatus(ui32 table, TArrayRef<const TIntrusiveConstPtr<TTxStatusPart>> txStatus, const TSubset &subset)
{
    return DatabaseImpl->ReplaceTxStatus(table, txStatus, subset);
}

void TDatabase::Merge(ui32 table, TPartView partView)
{
    return DatabaseImpl->Merge(table, std::move(partView));
}

void TDatabase::Merge(ui32 table, TIntrusiveConstPtr<TColdPart> part)
{
    return DatabaseImpl->Merge(table, std::move(part));
}

void TDatabase::Merge(ui32 table, TIntrusiveConstPtr<TTxStatusPart> txStatus)
{
    return DatabaseImpl->Merge(table, std::move(txStatus));
}

TAlter& TDatabase::Alter()
{
    Y_ABORT_UNLESS(Redo, "Scheme change must be done within a transaction");

    return *(Alter_ ? Alter_ : (Alter_ = new TAlter(DatabaseImpl.Get())));
}

void TDatabase::DebugDumpTable(ui32 table, IOutputStream& str, const NScheme::TTypeRegistry& typeRegistry) const {
    str << "Table " << table << Endl;
    if (auto &wrap = DatabaseImpl->Get(table, false))
        wrap->DebugDump(str, Env, typeRegistry);
    else
        str << "unknown" << Endl;
}

void TDatabase::DebugDump(IOutputStream& str, const NScheme::TTypeRegistry& typeRegistry) const {
    for (const auto& it: DatabaseImpl->Scheme->Tables) {
        if (DatabaseImpl->Get(it.first, false)) {
            str << "======= " << it.second.Name << " ======\n";
            DebugDumpTable(it.first, str, typeRegistry);
        }
    }
}

TKeyRangeCache* TDatabase::DebugGetTableErasedKeysCache(ui32 table) const {
    if (auto &wrap = DatabaseImpl->Get(table, false)) {
        return wrap->GetErasedKeysCache();
    } else {
        return nullptr;
    }
}

size_t TDatabase::GetCommitRedoBytes() const
{
    Y_ABORT_UNLESS(Redo, "Transaction is not in progress");
    return Redo->Bytes();
}

bool TDatabase::HasChanges() const
{
    Y_ABORT_UNLESS(Redo, "Transaction is not in progress");

    return *Redo || (Alter_ && *Alter_) || Change->Snapshots || Change->RemovedRowVersions;
}

TDatabase::TProd TDatabase::Commit(TTxStamp stamp, bool commit, TCookieAllocator *cookieAllocator)
{
    TVector<std::function<void()>> onPersistent;

    if (commit) {
        for (auto& callback : OnCommit_) {
            callback();
        }
        onPersistent = std::move(OnPersistent_);
    } else {
        auto it = OnRollback_.rbegin();
        auto end = OnRollback_.rend();
        while (it != end) {
            (*it)();
            ++it;
        }
    }

    OnCommit_.clear();
    OnRollback_.clear();
    OnPersistent_.clear();

    TempIterators.clear();

    if (commit && HasChanges()) {
        Y_ABORT_UNLESS(stamp >= Change->Stamp);
        Y_ABORT_UNLESS(DatabaseImpl->Serial() == Change->Serial);

        // FIXME: Temporary hack for using up to date change stamp when scan
        //        is queued inside a transaction. In practice we just need to
        //        stop using empty commits for scans. See KIKIMR-5366 for
        //        details.
        const_cast<TTxStamp&>(Change->Stamp) = stamp;

        NRedo::TWriter prefix{ };

        {
            const ui32 head = ui32(ECompatibility::Head);
            const ui32 edge = ui32(ECompatibility::Edge);

            prefix.EvBegin(head, edge, Change->Serial, Change->Stamp);
        }

        const auto offset = prefix.Bytes(); /* useful payload starts here */

        auto annex = Annex->Unwrap();
        if (annex) {
            Y_ABORT_UNLESS(cookieAllocator, "Have to provide TCookieAllocator with enabled annex");

            TVector<NPageCollection::TGlobId> blobs;

            blobs.reserve(annex.size());

            for (auto &one: annex) {
                auto glob = cookieAllocator->Do(one.GId.Logo.Channel(), one.Data.size());

                blobs.emplace_back(one.GId = glob);

                Y_ABORT_UNLESS(glob.Logo.BlobSize(), "Blob cannot have zero bytes");
            }

            prefix.EvAnnex(blobs);
        }

        DatabaseImpl->CommitTransaction(Change->Stamp, annex, prefix);

        if (Alter_ && *Alter_) {
            auto delta = Alter_->Flush();
            Y_PROTOBUF_SUPPRESS_NODISCARD delta->SerializeToString(&Change->Scheme);
        }

        prefix.Join(std::move(*Redo));

        Change->Redo = std::move(prefix).Finish();

        for (const auto& xpair : Change->RemovedRowVersions) {
            if (auto& wrap = DatabaseImpl->Get(xpair.first, false)) {
                for (const auto& range : xpair.second) {
                    wrap->RemoveRowVersions(range.Lower, range.Upper);
                }
            }
        }

        Change->Garbage = std::move(DatabaseImpl->Garbage);
        Change->Deleted = std::move(DatabaseImpl->Deleted);
        Change->Affects = DatabaseImpl->GrabAffects();
        Change->Annex = std::move(annex);

        if (Change->Redo.size() == offset && !Change->Affects) {
            std::exchange(Change->Redo, { }); /* omit complete NOOP redo */
        }

        if (Change->Redo.size() > offset && !Change->Affects) {
            Y_Fail(
                NFmt::Do(*Change) << " produced " << (Change->Redo.size() - offset)
                << "b of non technical redo without leaving effects on data");
        } else if (Change->Redo && Change->Serial != DatabaseImpl->Serial()) {
            Y_Fail(
                NFmt::Do(*Change) << " serial diverged from current db "
                << DatabaseImpl->Serial() << " after rolling up redo log");
        } else if (Change->Deleted.size() != Change->Garbage.size()) {
            Y_Fail(NFmt::Do(*Change) << " has inconsistent garbage data");
        }
    } else {
        DatabaseImpl->RollbackTransaction();
    }

    DatabaseImpl->RunGC();

    Redo = nullptr;
    Annex = nullptr;
    Alter_ = nullptr;
    Env = nullptr;

    return { std::move(Change), std::move(onPersistent) };
}

TTable* TDatabase::Require(ui32 table) const noexcept
{
    return DatabaseImpl->Get(table, true).Self.Get();
}

TTable* TDatabase::RequireForUpdate(ui32 table) const noexcept
{
    return DatabaseImpl->GetForUpdate(table).Self.Get();
}

TGarbage TDatabase::RollUp(TTxStamp stamp, TArrayRef<const char> delta, TArrayRef<const char> redo,
                                TMemGlobs annex)
{
    Y_ABORT_UNLESS(!annex || redo, "Annex have to be rolled up with redo log");

    DatabaseImpl->Switch(stamp);

    if (delta) {
        TSchemeChanges changes;
        bool parseOk = ParseFromStringNoSizeLimit(changes, delta);
        Y_ABORT_UNLESS(parseOk);

        DatabaseImpl->ApplySchema(changes);
    }

    if (redo) {
        DatabaseImpl->Assign(std::move(annex));
        DatabaseImpl->ApplyRedo(redo);
        DatabaseImpl->GrabAnnex();
    }

    return std::move(DatabaseImpl->Garbage);
}

void TDatabase::RollUpRemoveRowVersions(ui32 table, const TRowVersion& lower, const TRowVersion& upper)
{
    if (auto& wrap = DatabaseImpl->Get(table, false)) {
        wrap->RemoveRowVersions(lower, upper);
    }
}

TCompactionStats TDatabase::GetCompactionStats(ui32 table) const
{
    return Require(table)->GetCompactionStats();
}

// NOTE: This helper should be used only to dump local DB contents in GDB
void DebugDumpDb(const TDatabase &db) {
    NScheme::TTypeRegistry typeRegistry;
    db.DebugDump(Cout, typeRegistry);
}

}}

Y_DECLARE_OUT_SPEC(, NKikimr::NTable::TDatabase::TChangeCounter, stream, value) {
    stream << "TChangeCounter{serial=";
    stream << value.Serial;
    stream << ", epoch=";
    stream << value.Epoch;
    stream << "}";
}
