#pragma once
#include "flat_row_eggs.h"
#include "flat_row_versions.h"
#include "flat_update_op.h"
#include "flat_dbase_scheme.h"
#include "flat_dbase_change.h"
#include "flat_dbase_misc.h"
#include "flat_iterator.h"
#include "flat_table_observer.h"
#include "util_basics.h"

namespace NKikimr {

namespace NPageCollection {
    class TCookieAllocator;
    struct TMemGlob;
}

namespace NTable {

    struct TStats;
    struct TSizeEnv;
    class TRowState;
    struct TChange;
    class TDatabaseImpl;
    class TAnnex;
    class TTable;
    class TKeyRangeCache;

    namespace NRedo {
        class TWriter;
    }

struct TKeyRange {
    TRawVals MinKey;
    TRawVals MaxKey;
    bool MinInclusive = true;
    bool MaxInclusive = true;
};

class TDatabase {
public:
    using TMemGlobs = TVector<NPageCollection::TMemGlob>;
    using TCookieAllocator = NPageCollection::TCookieAllocator;
    using TCounters = TDbStats;

    struct TProd {
        THolder<TChange> Change;
        TVector<std::function<void()>> OnPersistent;
    };

    struct TChangeCounter {
        /**
         * Monotonic change counter for a table or an entire database. Serial
         * is incremented and persisted on each successful Commit() that has
         * data changes (i.e. not empty). Note: this may or may not be zero
         * when table has no changes, or when all changes have been compacted.
         */
        ui64 Serial = 0;

        /**
         * Monotonic epoch of a table's current memtable. This is incremented
         * each time a memtable is flushed and a new one is started. The
         * current memtable may or may not have additional changes.
         */
        TEpoch Epoch = TEpoch::Zero();

        TChangeCounter() = default;

        TChangeCounter(ui64 serial, TEpoch epoch)
            : Serial(serial)
            , Epoch(epoch)
        {}

        bool operator==(const TChangeCounter& rhs) const = default;
        bool operator!=(const TChangeCounter& rhs) const = default;

        /**
         * Compares two change counters, such that when a < b then b either
         * has more changes than a, or it's impossible to determine.
         */
        bool operator<(const TChangeCounter& rhs) const;
    };

    TDatabase(const TDatabase&) = delete;
    TDatabase(TDatabaseImpl *databaseImpl = nullptr) noexcept;
    ~TDatabase();

    void SetTableObserver(ui32 table, TIntrusivePtr<ITableObserver> ptr) noexcept;

    /**
     * Returns durable monotonic change counter for a table (or a database when
     * table = Max<ui32>() by default).
     */
    TChangeCounter Head(ui32 table = Max<ui32>()) const noexcept;

    /*_ Call Next() before accessing each row including the 1st row. */

    TAutoPtr<TTableIter> Iterate(ui32 table, TRawVals key, TTagsRef tags, ELookup) const noexcept;
    TAutoPtr<TTableIter> IterateExact(ui32 table, TRawVals key, TTagsRef tags,
            TRowVersion snapshot = TRowVersion::Max(),
            const ITransactionMapPtr& visible = nullptr,
            const ITransactionObserverPtr& observer = nullptr) const noexcept;
    TAutoPtr<TTableIter> IterateRange(ui32 table, const TKeyRange& range, TTagsRef tags,
            TRowVersion snapshot = TRowVersion::Max(),
            const ITransactionMapPtr& visible = nullptr,
            const ITransactionObserverPtr& observer = nullptr) const noexcept;
    TAutoPtr<TTableReverseIter> IterateRangeReverse(ui32 table, const TKeyRange& range, TTagsRef tags,
            TRowVersion snapshot = TRowVersion::Max(),
            const ITransactionMapPtr& visible = nullptr,
            const ITransactionObserverPtr& observer = nullptr) const noexcept;

    template<class TIteratorType>
    TAutoPtr<TIteratorType> IterateRangeGeneric(ui32 table, const TKeyRange& range, TTagsRef tags,
            TRowVersion snapshot = TRowVersion::Max(),
            const ITransactionMapPtr& visible = nullptr,
            const ITransactionObserverPtr& observer = nullptr) const noexcept;

    // NOTE: the row refeneces data in some internal buffers that get invalidated on the next Select() or Commit() call
    EReady Select(ui32 table, TRawVals key, TTagsRef tags, TRowState& row,
                  ui64 readFlags = 0, TRowVersion snapshot = TRowVersion::Max(),
                  const ITransactionMapPtr& visible = nullptr,
                  const ITransactionObserverPtr& observer = nullptr) const noexcept;

    EReady Select(ui32 table, TRawVals key, TTagsRef tags, TRowState& row, TSelectStats& stats,
                  ui64 readFlags = 0, TRowVersion snapshot = TRowVersion::Max(),
                  const ITransactionMapPtr& visible = nullptr,
                  const ITransactionObserverPtr& observer = nullptr) const noexcept;

    TSelectRowVersionResult SelectRowVersion(
            ui32 table, TRawVals key, ui64 readFlags = 0,
            const ITransactionMapPtr& visible = nullptr,
            const ITransactionObserverPtr& observer = nullptr) const noexcept;
    TSelectRowVersionResult SelectRowVersion(
            ui32 table, TArrayRef<const TCell> key, ui64 readFlags = 0,
            const ITransactionMapPtr& visible = nullptr,
            const ITransactionObserverPtr& observer = nullptr) const noexcept;

    bool Precharge(ui32 table, TRawVals minKey, TRawVals maxKey,
                        TTagsRef tags, ui64 readFlags, ui64 itemsLimit, ui64 bytesLimit,
                        EDirection direction = EDirection::Forward,
                        TRowVersion snapshot = TRowVersion::Max());

    TSizeEnv CreateSizeEnv();
    void CalculateReadSize(TSizeEnv& env, ui32 table, TRawVals minKey, TRawVals maxKey,
                        TTagsRef tags, ui64 readFlags, ui64 itemsLimit, ui64 bytesLimit,
                        EDirection direction = EDirection::Forward,
                        TRowVersion snapshot = TRowVersion::Max());

    void Update(ui32 table, ERowOp, TRawVals key, TArrayRef<const TUpdateOp>, TRowVersion rowVersion = TRowVersion::Min());

    void UpdateTx(ui32 table, ERowOp, TRawVals key, TArrayRef<const TUpdateOp>, ui64 txId);
    void RemoveTx(ui32 table, ui64 txId);
    void CommitTx(ui32 table, ui64 txId, TRowVersion rowVersion = TRowVersion::Min());

    /**
     * Returns true when table has an open transaction that is not committed or removed yet
     */
    bool HasOpenTx(ui32 table, ui64 txId) const;
    bool HasTxData(ui32 table, ui64 txId) const;
    bool HasCommittedTx(ui32 table, ui64 txId) const;
    bool HasRemovedTx(ui32 table, ui64 txId) const;

    /**
     * Returns a set of open transactions in the provided table. This only
     * includes transactions with changes that are neither committed nor
     * removed.
     */
    const absl::flat_hash_set<ui64>& GetOpenTxs(ui32 table) const;

    /**
     * Returns a number of open transactions in the provided table. This only
     * includes transactions with changes that are neither committed nor
     * removed.
     */
    size_t GetOpenTxCount(ui32 table) const;

    /**
     * Remove row versions [lower, upper) from the given table
     *
     * Once committed this cannot be undone. This is a hint to the underlying
     * storage that row versions in the given range would no longer be accessed
     * and may be compacted and garbage collected. Using row versions from the
     * given range in future or even ongoing transactions/scans may or may not
     * produce inconsistent results.
     */
    void RemoveRowVersions(ui32 table, const TRowVersion& lower, const TRowVersion& upper);

    /**
     * Returns currently committed ranges of removed row versions
     */
    const TRowVersionRanges& GetRemovedRowVersions(ui32 table) const;

    void NoMoreReadsForTx();

    TAlter& Alter(); /* Begin DDL ALTER script */

    TEpoch TxSnapTable(ui32 table);

    const TScheme& GetScheme() const noexcept;

    TIntrusiveConstPtr<TRowScheme> GetRowScheme(ui32 table) const noexcept;

    TPartView GetPartView(ui32 table, const TLogoBlobID &bundle) const;
    TVector<TPartView> GetTableParts(ui32 table) const;
    TVector<TIntrusiveConstPtr<TColdPart>> GetTableColdParts(ui32 table) const;
    void EnumerateTableParts(ui32 table, const std::function<void(const TPartView&)>& callback) const;
    void EnumerateTableColdParts(ui32 table, const std::function<void(const TIntrusiveConstPtr<TColdPart>&)>& callback) const;
    void EnumerateTableTxStatusParts(ui32 table, const std::function<void(const TIntrusiveConstPtr<TTxStatusPart>&)>& callback) const;
    void EnumerateTxStatusParts(const std::function<void(const TIntrusiveConstPtr<TTxStatusPart>&)>& callback) const;
    ui64 GetTableMemSize(ui32 table, TEpoch epoch = TEpoch::Max()) const;
    ui64 GetTableMemRowCount(ui32 tableId) const;
    ui64 GetTableMemOpsCount(ui32 tableId) const;
    ui64 GetTableIndexSize(ui32 table) const;
    ui64 GetTableSearchHeight(ui32 table) const;
    ui64 EstimateRowSize(ui32 table) const;
    const TCounters& Counters() const noexcept;
    TString SnapshotToLog(ui32 table, TTxStamp);

    TAutoPtr<TSubset> Subset(ui32 table, TArrayRef<const TLogoBlobID> bundle, TEpoch before) const;
    TAutoPtr<TSubset> Subset(ui32 table, TEpoch before, TRawVals from, TRawVals to) const;
    TAutoPtr<TSubset> ScanSnapshot(ui32 table, TRowVersion snapshot = TRowVersion::Max());

    bool HasBorrowed(ui32 table, ui64 selfTabletId) const;

    TBundleSlicesMap LookupSlices(ui32 table, TArrayRef<const TLogoBlobID> bundles) const;
    void ReplaceSlices(ui32 table, TBundleSlicesMap slices);

    void Replace(ui32 table, TArrayRef<const TPartView>, const TSubset&);
    void ReplaceTxStatus(ui32 table, TArrayRef<const TIntrusiveConstPtr<TTxStatusPart>>, const TSubset&);
    void Merge(ui32 table, TPartView);
    void Merge(ui32 table, TIntrusiveConstPtr<TColdPart>);
    void Merge(ui32 table, TIntrusiveConstPtr<TTxStatusPart>);

    void DebugDumpTable(ui32 table, IOutputStream& str, const NScheme::TTypeRegistry& typeRegistry) const;
    void DebugDump(IOutputStream& str, const NScheme::TTypeRegistry& typeRegistry) const;

    TKeyRangeCache* DebugGetTableErasedKeysCache(ui32 table) const;

    /**
     * Returns true when current transaction has changes to commit
     */
    bool HasChanges() const;

    /**
     * Rollback all current transaction changes
     *
     * Similar to aborting transaction and then starting a new one
     */
    void RollbackChanges();

    // executor interface
    void Begin(TTxStamp, IPages& env);
    TProd Commit(TTxStamp, bool commit, TCookieAllocator* = nullptr);
    TGarbage RollUp(TTxStamp, TArrayRef<const char> delta, TArrayRef<const char> redo, TMemGlobs annex);

    void RollUpRemoveRowVersions(ui32 table, const TRowVersion& lower, const TRowVersion& upper);

    size_t GetCommitRedoBytes() const;

    TCompactionStats GetCompactionStats(ui32 table) const;

    /**
     * Adds a callback, which is called when database changes are committed
     */
    template<class TCallback>
    void OnCommit(TCallback&& callback) {
        OnCommit_.emplace_back(std::forward<TCallback>(callback));
    }

    /**
     * Adds a callback, which is called when database changes are rolled back
     * 
     * @param callback 
     */
    template<class TCallback>
    void OnRollback(TCallback&& callback) {
        OnRollback_.emplace_back(std::forward<TCallback>(callback));
    }

    /**
     * Adds a callback, which is called when database changes are persistent
     */
    template<class TCallback>
    void OnPersistent(TCallback&& callback) {
        OnPersistent_.emplace_back(std::forward<TCallback>(callback));
    }

private:
    TTable* Require(ui32 tableId) const noexcept;
    TTable* RequireForUpdate(ui32 tableId) const noexcept;

private:
    const THolder<TDatabaseImpl> DatabaseImpl;

    bool NoMoreReadsFlag;
    IPages* Env = nullptr;
    THolder<TChange> Change;
    TAutoPtr<TAlter> Alter_;
    TAutoPtr<TAnnex> Annex;
    TAutoPtr<NRedo::TWriter> Redo;

    TVector<ui32> ModifiedRefs;
    TVector<TUpdateOp> ModifiedOps;

    mutable TDeque<TPartIter> TempIterators; // Keeps the last result of Select() valid
    mutable THashSet<ui32> IteratedTables;

    TVector<std::function<void()>> OnCommit_;
    TVector<std::function<void()>> OnRollback_;
    TVector<std::function<void()>> OnPersistent_;
};


}}
