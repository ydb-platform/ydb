#include "flat_mem_warm.h"
#include "flat_mem_snapshot.h"
#include "flat_page_other.h"

namespace NKikimr {
namespace NTable {

TString PrintRow(const TDbTupleRef& row, const NScheme::TTypeRegistry& typeRegistry) {
    return DbgPrintTuple(row, typeRegistry);
}

TIntrusiveConstPtr<NPage::TExtBlobs> TMemTable::MakeBlobsPage(TArrayRef<const TMemTableSnapshot> list)
{
    NPage::TExtBlobsWriter writer;

    for (auto &one: list) {
        for (auto it = one->GetBlobs()->Iterator(); it.IsValid(); it.Next()) {
            writer.Put(it->GId);
        }
    }

    return new NPage::TExtBlobs(writer.Make(true), { });
}

NMem::TTreeSnapshot TMemTable::Snapshot() {
    return NMem::TTreeSnapshot(Tree.Snapshot());
}

NMem::TTreeSnapshot TMemTable::Immediate() const {
    // Immediate snapshots are used in two distinct cases:
    // 1) When taking a snapshot of a frozen mem table, in that case we know
    //    the tree is frozen and will not be modified, so using an unsafe
    //    snapshot is ok.
    // 2) When taking a snapshot of a mutable mem table, but we can guarantee
    //    mem table will not be changed while iterator is still valid, for
    //    example during point reads.
    return NMem::TTreeSnapshot(Tree.UnsafeSnapshot());
}

void TMemTable::PrepareRollback() {
    Y_ABORT_UNLESS(!RollbackState);
    auto& state = RollbackState.emplace();
    state.Snapshot = Tree.Snapshot();
    state.OpsCount = OpsCount;
    state.RowCount = RowCount;
    state.MinRowVersion = MinRowVersion;
    state.MaxRowVersion = MaxRowVersion;
    Pool.BeginTransaction();
}

void TMemTable::RollbackChanges() {
    Y_ABORT_UNLESS(RollbackState);
    auto& state = *RollbackState;
    Tree.RollbackTo(state.Snapshot);
    state.Snapshot = { };
    Tree.CollectGarbage();
    Pool.RollbackTransaction();
    Blobs.Rollback(state.AddedBlobs);
    OpsCount = state.OpsCount;
    RowCount = state.RowCount;
    MinRowVersion = state.MinRowVersion;
    MaxRowVersion = state.MaxRowVersion;

    struct TApplyUndoOp {
        TMemTable* Self;

        void operator()(const TUndoOpUpdateCommitted& op) const {
            Self->Committed[op.TxId] = op.Value;
        }

        void operator()(const TUndoOpEraseCommitted& op) const {
            Self->Committed.erase(op.TxId);
        }

        void operator()(const TUndoOpInsertRemoved& op) const {
            Self->Removed.insert(op.TxId);
        }

        void operator()(const TUndoOpEraseRemoved& op) const {
            Self->Removed.erase(op.TxId);
        }

        void operator()(const TUndoOpUpdateTxIdStats& op) const {
            Self->TxIdStats[op.TxId] = op.Value;
        }

        void operator()(const TUndoOpEraseTxIdStats& op) const {
            Self->TxIdStats.erase(op.TxId);
        }
    };

    while (!UndoBuffer.empty()) {
        std::visit(TApplyUndoOp{ this }, UndoBuffer.back());
        UndoBuffer.pop_back();
    }

    RollbackState.reset();
}

void TMemTable::CommitChanges(TArrayRef<const TMemGlob> blobs) {
    Y_ABORT_UNLESS(RollbackState);
    auto& state = *RollbackState;
    state.Snapshot = { };
    Tree.CollectGarbage();
    Pool.CommitTransaction();
    Blobs.Commit(state.AddedBlobs, blobs);
    UndoBuffer.clear();
    RollbackState.reset();
}

void TMemTable::CommitBlobs(TArrayRef<const TMemGlob> blobs) {
    Y_ABORT_UNLESS(!RollbackState);
    Blobs.Commit(Blobs.Size(), blobs);
}

void TMemTable::DebugDump(IOutputStream& str, const NScheme::TTypeRegistry& typeRegistry) const {
    auto it = Immediate().Iterator();
    auto types = Scheme->Keys->BasicTypes();
    for (it.SeekFirst(); it.IsValid(); it.Next()) {
        TDbTupleRef key(types.data(), it.GetKey(), types.size());

        TString keyStr = PrintRow(key, typeRegistry) + " -> ";
        const auto *row = it.GetValue();
        while (row) {
            str << keyStr
                << "ERowOp " << int(row->Rop)
                << " {";
            for (ui32 i = 0; i < row->Items; ++i) {
                TTag colId = row->Ops()[i].Tag;
                if (Scheme->ColInfo(colId)) {
                    auto typeInfo = Scheme->ColInfo(colId)->TypeInfo;
                    auto &op = row->Ops()[i];

                    str << EOpToStr(ECellOp(op.Op)) << " " << op.Tag << " " << DbgPrintCell(op.Value, typeInfo, typeRegistry);
                } else {
                    str << "unknown column " << colId;
                }
                if (i+1 < row->Items)
                    str << ", ";
            }
            str << "}" << Endl;
            row = row->Next;
        }
    }
}

}}
