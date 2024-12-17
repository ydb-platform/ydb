#pragma once
#include "defs.h"
#include "flat_update_op.h"
#include "flat_mem_eggs.h"
#include "flat_mem_blobs.h"
#include "flat_row_scheme.h"
#include "flat_row_nulls.h"
#include "flat_row_celled.h"
#include "flat_page_blobs.h"
#include "flat_sausage_solid.h"
#include "flat_table_committed.h"
#include "util_pool.h"
#include <ydb/core/scheme/scheme_tablecell.h>
#include <ydb/core/scheme/scheme_type_id.h>
#include <ydb/core/util/btree_cow.h>
#include <ydb/library/yverify_stream/yverify_stream.h>

#include <library/cpp/containers/absl_flat_hash/flat_hash_map.h>
#include <library/cpp/containers/absl_flat_hash/flat_hash_set.h>
#include <library/cpp/containers/stack_vector/stack_vec.h>

#include <util/generic/vector.h>
#include <util/memory/pool.h>

namespace NKikimr {
namespace NTable {
namespace NMem {

    struct TPoint {
        TArrayRef<const TCell> Key;
        const TKeyCellDefaults &KeyCellDefaults;
    };

    struct TCandidate {
        const TCell* Key;
    };

    struct TKeyCmp {
        const NScheme::TTypeInfoOrder* Types;
        const ui32 Count;

        explicit TKeyCmp(const TRowScheme& rowScheme)
            : Types(&rowScheme.Keys->Types[0])
            , Count(rowScheme.Keys->Types.size())
        {

        }

        bool operator()(const TTreeKey& a, const TTreeKey& b) const {
            return CompareTypedCellVectors(a.KeyCells, b.KeyCells, Types, Count) < 0;
        }

        bool operator()(const TTreeKey& a, const TCandidate& b) const {
            return CompareTypedCellVectors(a.KeyCells, b.Key, Types, Count) < 0;
        }

        bool operator()(const TCandidate& a, const TTreeKey& b) const {
            return CompareTypedCellVectors(a.Key, b.KeyCells, Types, Count) < 0;
        }

        int DoCompare(const TCell* a, const TPoint& b) const {
            const size_t bSize = Min(b.Key.size(), b.KeyCellDefaults->size());

            for (size_t i = 0; i < bSize; ++i) {
                const TCell& left = i < Count ? a[i] : b.KeyCellDefaults[i];

                if (int cmp = CompareTypedCells(left, b.Key[i], b.KeyCellDefaults.Types[i]))
                    return cmp;
            }

            // Missing point cells are filled with a virtual +inf
            return b.Key.size() < b.KeyCellDefaults->size() ? -1 : 0;
        }

        bool operator()(const TTreeKey& a, const TPoint& b) const {
            return DoCompare(a.KeyCells, b) < 0;
        }

        bool operator()(const TPoint& a, const TTreeKey& b) const {
            return DoCompare(b.KeyCells, a) > 0;
        }
    };

    class TTreeAllocatorState {
        struct TFreeItem {
            TFreeItem* Next;
        };

    public:
        explicit TTreeAllocatorState(size_t pageSize)
            : PageSize(pageSize)
        {
            Y_ABORT_UNLESS(PageSize >= sizeof(TFreeItem));
        }

        ~TTreeAllocatorState() noexcept {
            TFreeItem* head = Head;
            while (head) {
                TFreeItem* next = head->Next;
                ::operator delete(head, PageSize);
                head = next;
            }
        }

        [[nodiscard]] void* Allocate(size_t size) {
            if (Y_LIKELY(size == PageSize)) {
                if (Head) {
                    TFreeItem* item = Head;
                    Head = item->Next;
                    NSan::Poison(item, PageSize);
                    return item;
                }
            }
            return ::operator new(size);
        }

        void Deallocate(void* p, size_t size) noexcept {
            if (Y_LIKELY(size == PageSize)) {
                NSan::Poison(p, size);
                TFreeItem* item = reinterpret_cast<TFreeItem*>(p);
                item->Next = Head;
                Head = item;
            } else {
                ::operator delete(p, size);
            }
        }

    private:
        const size_t PageSize;
        TFreeItem* Head = nullptr;
    };

    template<class T>
    class TTreeAllocator {
    public:
        template<class U>
        friend class TTreeAllocator;

        using value_type = T;

        TTreeAllocator(TTreeAllocatorState* state) noexcept
            : State(state)
        { }

        template<class U>
        TTreeAllocator(const TTreeAllocator<U>& other) noexcept
            : State(other.State)
        { }

        [[nodiscard]] T* allocate(size_t n) {
            return static_cast<T*>(State->Allocate(sizeof(T) * n));
        }

        void deallocate(T* p, size_t n) noexcept {
            State->Deallocate(p, sizeof(T) * n);
        }

        template<class U>
        bool operator==(const TTreeAllocator<U>& rhs) const noexcept {
            return State == rhs.State;
        }

    private:
        TTreeAllocatorState* State = nullptr;
    };

    using TTree = TCowBTree<TTreeKey, TTreeValue, TKeyCmp, TTreeAllocator<TTreeValue>, 512>;

    class TTreeIterator;
    class TTreeSnapshot;

} // namespace NMem

    class TMemIter;

    struct TMemTableSnapshot;

    struct TMemTableRollbackState;

    class TMemTable : public TThrRefBase {
        friend class TMemIter;

    public:
        struct TTxIdStat {
            ui64 OpsCount = 0;
        };

        using TTxIdStats = absl::flat_hash_map<ui64, TTxIdStat>;

    public:
        using TTree = NMem::TTree;
        using TOpsRef = TArrayRef<const TUpdateOp>;
        using TMemGlob = NPageCollection::TMemGlob;

        TMemTable(TIntrusiveConstPtr<TRowScheme> scheme, TEpoch epoch, ui64 annex, ui64 chunk = 4032)
            : Epoch(epoch)
            , Scheme(scheme)
            , Blobs(annex)
            , Comparator(*Scheme)
            , Pool(chunk)
            , TreeAllocatorState(TTree::PageSize)
            , Tree(Comparator, &TreeAllocatorState)
        {}

        void Update(ERowOp rop, TRawVals key_, TOpsRef ops, TArrayRef<const TMemGlob> pages, TRowVersion rowVersion,
                    NTable::ITransactionMapSimplePtr committed)
        {
            Y_DEBUG_ABORT_UNLESS(
                rop == ERowOp::Upsert || rop == ERowOp::Erase || rop == ERowOp::Reset,
                "Unexpected row operation");

            Y_ABORT_UNLESS(ops.size() < Max<ui16>(), "Too large update ops array");

            // Filter legacy empty values and re-order them in tag order
            ScratchUpdateTags.clear();
            for (ui32 it = 0; it < ops.size(); ++it) {
                if (Y_UNLIKELY(ops[it].Op == ECellOp::Empty)) {
                    // Filter possible empty updates
                    continue;
                }

                ScratchUpdateTags.emplace_back(ops[it].Tag, it);
            }
            std::sort(ScratchUpdateTags.begin(), ScratchUpdateTags.end());

            // Filter (very unlikely) duplicate updates for the same tag
            {
                const auto equalTag = [](const TTagWithPos& a, const TTagWithPos& b) -> bool {
                    return a.first == b.first;
                };
                auto firstDup = std::adjacent_find(ScratchUpdateTags.begin(), ScratchUpdateTags.end(), equalTag);
                if (Y_UNLIKELY(firstDup != ScratchUpdateTags.end())) {
                    // We remove duplicates, keeping the first for every tag,
                    // matching the way Apply used to work in the past.
                    ScratchUpdateTags.erase(
                        std::unique(firstDup, ScratchUpdateTags.end(), equalTag),
                        ScratchUpdateTags.end());
                }
            }

            // Find possible existing update for the same key
            const TCelled key(key_, *Scheme->Keys, true);
            const NMem::TTreeValue* const current = Tree.Find(NMem::TCandidate{ key.Cells });
            const NMem::TUpdate* next = current ? current->GetFirst() : nullptr;

            ScratchMergeTags.clear();
            ScratchMergeTagsLast.clear();

            // When writing a committed row we need to create a fully merged row state
            if (rowVersion.Step != Max<ui64>()) {
                // Search for the first committed row version we would need to merge from
                while (next) {
                    TRowVersion nextVersion = next->RowVersion;
                    if (nextVersion.Step == Max<ui64>()) {
                        auto* commitVersion = committed.Find(nextVersion.TxId);
                        if (!commitVersion) {
                            next = next->Next;
                            continue;
                        }
                        nextVersion = *commitVersion;
                    }
                    Y_VERIFY_DEBUG_S(nextVersion <= rowVersion,
                        "Out of order write to key detected:"
                        << " Old# " << nextVersion
                        << " New# " << rowVersion);
                    break;
                }

                const NMem::TUpdate* mergeFrom = next;

                // Search for the first committed row version not shadowed by the new update
                while (next) {
                    TRowVersion nextVersion = next->RowVersion;
                    if (nextVersion.Step == Max<ui64>()) {
                        auto* commitVersion = committed.Find(nextVersion.TxId);
                        if (!commitVersion) {
                            next = next->Next;
                            continue;
                        }
                        nextVersion = *commitVersion;
                    }
                    if (nextVersion < rowVersion) {
                        break;
                    }
                    next = next->Next;
                }

                // See which tags we need to merge from earlier row versions
                while (mergeFrom && rop == ERowOp::Upsert) {
                    if (mergeFrom->RowVersion.Step == Max<ui64>()) {
                        if (!committed.Find(mergeFrom->RowVersion.TxId)) {
                            // this item is not committed, skip
                            mergeFrom = mergeFrom->Next;
                            continue;
                        }
                    }

                    if (mergeFrom->Rop != ERowOp::Upsert) {
                        rop = ERowOp::Reset;
                    }

                    ScratchMergeTags.swap(ScratchMergeTagsLast);
                    ScratchMergeTags.clear();

                    auto have = ScratchUpdateTags.begin();
                    auto haveLast = ScratchMergeTagsLast.begin();
                    const auto* op = mergeFrom->Ops();
                    for (ui32 it = 0; it < mergeFrom->Items; ++it, ++op) {
                        TTag tag = op->Tag;
                        while (have != ScratchUpdateTags.end() && have->first < tag) {
                            ++have;
                        }
                        if (have == ScratchUpdateTags.end() || have->first != tag) {
                            while (haveLast != ScratchMergeTagsLast.end() && (*haveLast)->Tag < tag) {
                                ScratchMergeTags.emplace_back(*haveLast);
                                ++haveLast;
                            }
                            if (haveLast == ScratchMergeTagsLast.end() || (*haveLast)->Tag != tag) {
                                ScratchMergeTags.emplace_back(op);
                            }
                        }
                    }
                    while (haveLast != ScratchMergeTagsLast.end()) {
                        ScratchMergeTags.emplace_back(*haveLast);
                        ++haveLast;
                    }

                    if (mergeFrom->RowVersion.Step != Max<ui64>()) {
                        break;
                    }
                    mergeFrom = mergeFrom->Next;
                }
            }

            const size_t mergedSize = ScratchUpdateTags.size() + ScratchMergeTags.size();
            Y_ABORT_UNLESS(mergedSize < Max<ui16>(), "Merged row update is too large");

            auto *update = NewUpdate(mergedSize);

            update->Next = next;
            update->RowVersion = rowVersion;
            update->Items = mergedSize;
            update->Rop = rop;

            ui32 dstIndex = 0;

            auto missing = ScratchMergeTags.begin();
            for (const TTagWithPos& src : ScratchUpdateTags) {
                const TTag tag = src.first;
                while (missing != ScratchMergeTags.end() && (*missing)->Tag < tag) {
                    update->Ops()[dstIndex++] = **missing;
                    ++missing;
                }

                const ui32 it = src.second;
                const auto *info = Scheme->ColInfo(tag);

                if (info == nullptr) {
                    /* Redo log rolling on bootstap happens with the last
                        actual row scheme that may have some columns already
                        be deleted. So cannot differ here error and booting.
                     */
                } else if (TCellOp::HaveNoPayload(ops[it].NormalizedCellOp())) {
                    /* Payloadless ECellOp types may have zero type value */
                } else if (info->TypeInfo.GetTypeId() != ops[it].Value.Type()) {
                    Y_ABORT("Got an unexpected column type %" PRIu16 " in cell update for tag %" PRIu32 " (expected %" PRIu16 ")",
                        ops[it].Value.Type(), ops[it].Tag, info->TypeInfo.GetTypeId());
                }

                auto cell = ops[it].AsCell();

                if (ops[it].Op == ELargeObj::Extern) {
                    /* Transformation REDO ELargeObj to TBlobs reference */

                    const auto ref = Blobs.Push(pages.at(cell.AsValue<ui32>()));
                    if (RollbackState) {
                        RollbackState->AddedBlobs++;
                    }

                    cell = TCell::Make<ui64>(ref);

                } else if (ops[it].Op != ELargeObj::Inline) {
                    Y_ABORT("Got an unexpected ELargeObj reference in update ops");
                } else if (!cell.IsInline()) {
                    cell = Clone(cell.Data(), cell.Size());
                }

                update->Ops()[dstIndex++] = { ops[it].Tag, ops[it].Op, cell };
            }

            while (missing != ScratchMergeTags.end()) {
                update->Ops()[dstIndex++] = **missing;
                ++missing;
            }

            if (current) {
                Tree.UpdateUnsafe()->Chain = update;
            } else {
                Tree.EmplaceUnsafe(NewKey(key.Cells), update);
                ++RowCount;
            }

            ++OpsCount;

            if (rowVersion.Step == Max<ui64>()) {
                MinRowVersion = TRowVersion::Min();
                MaxRowVersion = TRowVersion::Max();
                if (RollbackState) {
                    auto it = TxIdStats.find(rowVersion.TxId);
                    if (it != TxIdStats.end()) {
                        UndoBuffer.push_back(TUndoOpUpdateTxIdStats{ rowVersion.TxId, it->second });
                    } else {
                        UndoBuffer.push_back(TUndoOpEraseTxIdStats{ rowVersion.TxId });
                    }
                }
                ++TxIdStats[rowVersion.TxId].OpsCount;
            } else {
                MinRowVersion = Min(MinRowVersion, rowVersion);
                MaxRowVersion = Max(MaxRowVersion, rowVersion);
            }
        }

        size_t GetUsedMem() const noexcept
        {
            return
                Pool.Used()
                + (Tree.AllocatedPages() - Tree.DroppedPages()) * TTree::PageSize
                + Blobs.GetBytes();
        }

        size_t GetWastedMem() const noexcept
        {
            return Pool.Wasted();
        }

        ui64 GetOpsCount() const noexcept { return OpsCount; }

        ui64 GetRowCount() const noexcept { return RowCount; }

        TRowVersion GetMinRowVersion() const noexcept { return MinRowVersion; }
        TRowVersion GetMaxRowVersion() const noexcept { return MaxRowVersion; }

        static TIntrusiveConstPtr<NPage::TExtBlobs> MakeBlobsPage(TArrayRef<const TMemTableSnapshot>);
        void DebugDump(IOutputStream&, const NScheme::TTypeRegistry&) const;

        NMem::TTreeSnapshot Snapshot();

        NMem::TTreeSnapshot Immediate() const;

        const NMem::TBlobs* GetBlobs() const {
            return &Blobs;
        }

        void PrepareRollback();
        void RollbackChanges();
        void CommitChanges(TArrayRef<const TMemGlob> blobs);
        void CommitBlobs(TArrayRef<const TMemGlob> blobs);

        const TTxIdStats& GetTxIdStats() const {
            return TxIdStats;
        }

        void CommitTx(ui64 txId, TRowVersion rowVersion) {
            auto it = Committed.find(txId);
            bool toInsert = (it == Committed.end());

            if (toInsert || it->second > rowVersion) {
                if (RollbackState) {
                    if (it != Committed.end()) {
                        UndoBuffer.push_back(TUndoOpUpdateCommitted{ txId, it->second });
                    } else {
                        UndoBuffer.push_back(TUndoOpEraseCommitted{ txId });
                    }
                }
                Committed[txId] = rowVersion;
                if (toInsert) {
                    auto itRemoved = Removed.find(txId);
                    if (itRemoved != Removed.end()) {
                        if (RollbackState) {
                            UndoBuffer.push_back(TUndoOpInsertRemoved{ txId });
                        }
                        Removed.erase(itRemoved);
                    }
                }
            }
        }

        void RemoveTx(ui64 txId) {
            auto it = Committed.find(txId);
            if (it == Committed.end()) {
                auto itRemoved = Removed.find(txId);
                if (itRemoved == Removed.end()) {
                    if (RollbackState) {
                        UndoBuffer.push_back(TUndoOpEraseRemoved{ txId });
                    }
                    Removed.insert(txId);
                }
            }
        }

        const absl::flat_hash_map<ui64, TRowVersion>& GetCommittedTransactions() const {
            return Committed;
        }

        const absl::flat_hash_set<ui64>& GetRemovedTransactions() const {
            return Removed;
        }

    private:
        NMem::TTreeKey NewKey(const TCell* src) noexcept {
            const size_t items = Scheme->Keys->Size();
            const size_t bytes = sizeof(TCell) * items;

            void* base = Pool.Allocate(bytes);
            auto* key = reinterpret_cast<TCell*>(base);

            TCell* dst = key;
            for (size_t i = 0; i < items; ++i, ++src, ++dst) {
                new (dst) TCell(Clone(src->Data(), src->Size()));
            }

            return NMem::TTreeKey(key);
        }

        NMem::TUpdate* NewUpdate(ui32 cols) noexcept
        {
            const size_t bytes = sizeof(NMem::TUpdate) + cols * sizeof(NMem::TColumnUpdate);

            return (NMem::TUpdate*)Pool.Allocate(bytes);
        }

        TCell Clone(const char *data, ui32 size) noexcept
        {
            const bool small = TCell::CanInline(size);

            return { small ? data : (const char*)Pool.Append(data, size), size };
        }

        void DebugDump() const;

    public:
        const TEpoch Epoch;
        const TIntrusiveConstPtr<TRowScheme> Scheme;

    private:
        NMem::TBlobs Blobs;
        const NMem::TKeyCmp Comparator;
        NUtil::TMemoryPool Pool;
        NMem::TTreeAllocatorState TreeAllocatorState;
        TTree Tree;
        ui64 OpsCount = 0;
        ui64 RowCount = 0;
        TRowVersion MinRowVersion = TRowVersion::Max();
        TRowVersion MaxRowVersion = TRowVersion::Min();
        TTxIdStats TxIdStats;
        absl::flat_hash_map<ui64, TRowVersion> Committed;
        absl::flat_hash_set<ui64> Removed;

    private:
        struct TRollbackState {
            TTree::TSnapshot Snapshot;
            ui64 OpsCount;
            ui64 RowCount;
            TRowVersion MinRowVersion;
            TRowVersion MaxRowVersion;
            size_t AddedBlobs = 0;
        };

        std::optional<TRollbackState> RollbackState;

    private:
        struct TUndoOpUpdateCommitted {
            ui64 TxId;
            TRowVersion Value;
        };
        struct TUndoOpEraseCommitted {
            ui64 TxId;
        };
        struct TUndoOpInsertRemoved {
            ui64 TxId;
        };
        struct TUndoOpEraseRemoved {
            ui64 TxId;
        };
        struct TUndoOpUpdateTxIdStats {
            ui64 TxId;
            TTxIdStat Value;
        };
        struct TUndoOpEraseTxIdStats {
            ui64 TxId;
        };

        using TUndoOp = std::variant<
            TUndoOpUpdateCommitted,
            TUndoOpEraseCommitted,
            TUndoOpInsertRemoved,
            TUndoOpEraseRemoved,
            TUndoOpUpdateTxIdStats,
            TUndoOpEraseTxIdStats>;

        // This buffer is applied in reverse on rollback
        // Memory is reused to avoid hot path allocations
        std::vector<TUndoOp> UndoBuffer;

    private:
        // Temporary buffers to avoid hot path allocations
        using TTagWithPos = std::pair<TTag, ui32>;
        TSmallVec<TTagWithPos> ScratchUpdateTags;
        std::vector<const NMem::TColumnUpdate*> ScratchMergeTags;
        std::vector<const NMem::TColumnUpdate*> ScratchMergeTagsLast;
    };

}
}
