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
#include <ydb/core/scheme/scheme_tablecell.h>
#include <ydb/core/scheme/scheme_type_id.h>
#include <ydb/core/util/btree_cow.h>
#include <ydb/core/util/yverify_stream.h>

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
        const NScheme::TTypeIdOrder* Types;
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

    using TTree = TCowBTree<TTreeKey, TTreeValue, TKeyCmp>;

    class TTreeIterator;
    class TTreeSnapshot;

} // namespace NMem

    class TMemIt;

    struct TMemTableSnapshot;

    class TMemTable : public TThrRefBase {
        friend class TMemIt;

        template <size_t SizeCap = 512*1024, size_t Overhead = 64>
        class TMyPolicy : public TMemoryPool::IGrowPolicy {
        public:
            size_t Next(size_t prev) const noexcept override
            {
                if (prev >= SizeCap - Overhead)
                    return SizeCap - Overhead;

                // Use same buckets as LF-alloc (4KB, 6KB, 8KB, 12KB, 16KB ...)
                size_t size = FastClp2(prev);

                if (size < prev + prev/3)
                    size += size/2;

                return size - Overhead;
            }
        };

    public:
        struct TTxIdStat {
            ui64 OpsCount = 0;
        };

        using TTxIdStats = THashMap<ui64, TTxIdStat>;

    public:
        using TTree = NMem::TTree;
        using TOpsRef = TArrayRef<const TUpdateOp>;
        using TMemGlob = NPageCollection::TMemGlob;

        TMemTable(TIntrusiveConstPtr<TRowScheme> scheme, TEpoch epoch, ui64 annex, ui64 chunk = 4032)
            : Epoch(epoch)
            , Scheme(scheme)
            , Blobs(annex)
            , Comparator(*Scheme)
            , Pool(chunk, &Policy)
            , Tree(Comparator) // TODO: support TMemoryPool with caching
        {}

        void Update(ERowOp rop, TRawVals key_, TOpsRef ops, TArrayRef<TMemGlob> pages, TRowVersion rowVersion,
                    NTable::ITransactionMapSimplePtr committed)
        {
            Y_VERIFY_DEBUG(
                rop == ERowOp::Upsert || rop == ERowOp::Erase || rop == ERowOp::Reset,
                "Unexpected row operation");

            Y_VERIFY(ops.size() < Max<ui16>(), "Too large update ops array");

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

                    ScratchMergeTagsLast.swap(ScratchMergeTags);
                    ScratchMergeTagsLast.clear();

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
            Y_VERIFY(mergedSize < Max<ui16>(), "Merged row update is too large");

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
                } else if (info->TypeId != ops[it].Value.Type()) {
                    Y_FAIL("Got an unexpected column type %" PRIu16 " in cell update for tag %" PRIu32 " (expected %" PRIu16 ")",
                        ops[it].Value.Type(), ops[it].Tag, info->TypeId);
                }

                auto cell = ops[it].AsCell();

                if (ops[it].Op == ELargeObj::Extern) {
                    /* Transformation REDO ELargeObj to TBlobs reference */

                    const auto ref = Blobs.Push(pages.at(cell.AsValue<ui32>()));

                    cell = TCell::Make<ui64>(ref);

                } else if (ops[it].Op != ELargeObj::Inline) {
                    Y_FAIL("Got an unexpected ELargeObj reference in update ops");
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
                ++TxIdStats[rowVersion.TxId].OpsCount;
            } else {
                MinRowVersion = Min(MinRowVersion, rowVersion);
                MaxRowVersion = Max(MaxRowVersion, rowVersion);
            }
        }

        size_t GetUsedMem() const noexcept
        {
            return
                Pool.MemoryAllocated()
                + (Tree.AllocatedPages() - Tree.DroppedPages()) * TTree::PageSize
                + Blobs.GetBytes();
        }

        size_t GetWastedMem() const noexcept
        {
            return Pool.MemoryWaste();
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

        const TTxIdStats& GetTxIdStats() const {
            return TxIdStats;
        }

        void CommitTx(ui64 txId, TRowVersion rowVersion) {
            auto it = Committed.find(txId);
            if (it == Committed.end() || it->second > rowVersion) {
                Committed[txId] = rowVersion;
                if (it == Committed.end()) {
                    Removed.erase(txId);
                }
            }
        }

        void RemoveTx(ui64 txId) {
            auto it = Committed.find(txId);
            if (it == Committed.end()) {
                Removed.insert(txId);
            }
        }

        const THashMap<ui64, TRowVersion>& GetCommittedTransactions() const {
            return Committed;
        }

        const THashSet<ui64>& GetRemovedTransactions() const {
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

            return { small ? data : Pool.Append(data, size), size };
        }

        void DebugDump() const;

    public:
        const TEpoch Epoch;
        const TIntrusiveConstPtr<TRowScheme> Scheme;

    private:
        NMem::TBlobs Blobs;
        const NMem::TKeyCmp Comparator;
        TMyPolicy<> Policy;
        TMemoryPool Pool;
        TTree Tree;
        ui64 OpsCount = 0;
        ui64 RowCount = 0;
        TRowVersion MinRowVersion = TRowVersion::Max();
        TRowVersion MaxRowVersion = TRowVersion::Min();
        TTxIdStats TxIdStats;
        THashMap<ui64, TRowVersion> Committed;
        THashSet<ui64> Removed;

    private:
        // Temporary buffers to avoid hot path allocations
        using TTagWithPos = std::pair<TTag, ui32>;
        TSmallVec<TTagWithPos> ScratchUpdateTags;
        std::vector<const NMem::TColumnUpdate*> ScratchMergeTags;
        std::vector<const NMem::TColumnUpdate*> ScratchMergeTagsLast;
    };

}
}
